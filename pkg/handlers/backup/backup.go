/* Copyright 2022 Zinc Labs Inc. and Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
 */

// Package backup serves the endpoints that create and fetch node backups.
package backup

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zincsearch/zincsearch/pkg/backup"
	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/streaming"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

const defaultDrainTimeout = 10 * time.Minute

type response struct {
	Message string        `json:"message,omitempty"`
	Error   string        `json:"error,omitempty"`
	Backup  *backup.Info  `json:"backup,omitempty"`
	Backups []backup.Info `json:"backups,omitempty"`
}

func dir(c *gin.Context) (string, bool) {
	d := config.Global.BackupPath
	if d == "" {
		zutils.GinRenderJSON(c, http.StatusBadRequest, response{Error: "backups are not configured (zinc_backup_path)"})
		return "", false
	}
	return d, true
}

// Create takes a backup of the whole node.
//
// When the node consumes a stream, the consumer is paused and drained first and
// resumed afterwards, so the backup is a state the stream passed through. The
// archive is kept in zinc_backup_path; fetch it with Get. Query parameter
// "timeout" (a Go duration, default 10m) bounds waiting for the consumer to drain.
//
// @Id CreateBackup
// @Summary Create a backup of the node
// @security BasicAuth
// @Tags    Backup
// @Produce json
// @Param   timeout query string false "Maximum wait for the node to become quiescent, e.g. 5m"
// @Success 200 {object} response
// @Failure 400 {object} response
// @Failure 409 {object} response
// @Failure 500 {object} response
// @Failure 504 {object} response
// @Router /api/backup [post]
func Create(c *gin.Context) {
	d, ok := dir(c)
	if !ok {
		return
	}
	timeout := defaultDrainTimeout
	if raw := c.Query("timeout"); raw != "" {
		v, err := time.ParseDuration(raw)
		if err != nil || v <= 0 {
			zutils.GinRenderJSON(c, http.StatusBadRequest, response{Error: "timeout must be a positive duration such as 5m"})
			return
		}
		timeout = v
	}

	if cons := streaming.Current(); cons != nil {
		wasPaused := cons.Status().Paused
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		err := cons.PauseAndDrain(ctx)
		cancel()
		// Continue consuming however this request ends, unless someone else
		// paused the consumer before and expects it to stay paused.
		if !wasPaused {
			defer cons.Resume()
		}
		if err != nil {
			zutils.GinRenderJSON(c, http.StatusGatewayTimeout, response{Error: err.Error()})
			return
		}
	}

	info, err := backup.Create(c.Request.Context(), d)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, backup.ErrNotQuiescent) {
			code = http.StatusConflict
		}
		zutils.GinRenderJSON(c, code, response{Error: err.Error()})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, response{Message: "ok", Backup: info})
}

// List returns the stored backups, oldest first.
//
// @Id ListBackups
// @Summary List backups
// @security BasicAuth
// @Tags    Backup
// @Produce json
// @Success 200 {object} response
// @Router /api/backup [get]
func List(c *gin.Context) {
	d, ok := dir(c)
	if !ok {
		return
	}
	infos, err := backup.List(d)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, response{Error: err.Error()})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, response{Backups: infos})
}

// Get downloads a backup. Ranges are supported, so an interrupted download can
// continue. The checksum of the whole file is in the X-Zinc-Backup-Sha256 header.
//
// @Id GetBackup
// @Summary Download a backup
// @security BasicAuth
// @Tags    Backup
// @Produce application/gzip
// @Param   name path string true "Backup name"
// @Success 200 {file} binary
// @Failure 404 {object} response
// @Router /api/backup/{name} [get]
func Get(c *gin.Context) {
	d, ok := dir(c)
	if !ok {
		return
	}
	f, info, err := backup.Open(d, c.Param("name"))
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusNotFound, response{Error: err.Error()})
		return
	}
	defer f.Close()
	c.Header("Content-Type", "application/gzip")
	c.Header("X-Zinc-Backup-Sha256", info.SHA256)
	c.Header("X-Zinc-Backup-Last-Applied", strconv.FormatUint(info.LastApplied, 10))
	http.ServeContent(c.Writer, c.Request, info.Name, info.CreatedAt, f)
}

// Delete removes a backup.
//
// @Id DeleteBackup
// @Summary Delete a backup
// @security BasicAuth
// @Tags    Backup
// @Produce json
// @Param   name path string true "Backup name"
// @Success 200 {object} response
// @Failure 404 {object} response
// @Router /api/backup/{name} [delete]
func Delete(c *gin.Context) {
	d, ok := dir(c)
	if !ok {
		return
	}
	if err := backup.Delete(d, c.Param("name")); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, backup.ErrNotFound) {
			code = http.StatusNotFound
		}
		zutils.GinRenderJSON(c, code, response{Error: err.Error()})
		return
	}
	zutils.GinRenderJSON(c, http.StatusOK, response{Message: "ok"})
}
