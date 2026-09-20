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

// Package stream serves the endpoints that control the stream consumer of a node.
package stream

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/zincsearch/zincsearch/pkg/streaming"
	"github.com/zincsearch/zincsearch/pkg/zutils"
)

const defaultPauseTimeout = time.Minute

type response struct {
	Message    string            `json:"message,omitempty"`
	Error      string            `json:"error,omitempty"`
	Status     *streaming.Status `json:"status,omitempty"`
	WALPending *uint64           `json:"wal_pending,omitempty"`
}

func consumer(c *gin.Context) *streaming.Consumer {
	cons := streaming.Current()
	if cons == nil {
		zutils.GinRenderJSON(c, http.StatusNotFound, response{Error: "stream consumer is not enabled (zinc_stream_enable)"})
	}
	return cons
}

// Status returns the state of the stream consumer.
//
// @Id StreamStatus
// @Summary Get stream consumer status
// @security BasicAuth
// @Tags    Stream
// @Produce json
// @Success 200 {object} response
// @Failure 404 {object} response
// @Router /api/stream/status [get]
func Status(c *gin.Context) {
	cons := consumer(c)
	if cons == nil {
		return
	}
	st := cons.Status()
	zutils.GinRenderJSON(c, http.StatusOK, response{Status: &st})
}

// Pause stops consuming and waits until the data of this node is quiescent.
//
// It returns 200 only when the batch in flight is finished and every accepted
// document is applied, so the data files can be copied while the node is paused.
// Query parameter "timeout" (a Go duration, default 1m) bounds the wait.
//
// @Id StreamPause
// @Summary Pause the stream consumer and wait until the node is quiescent
// @security BasicAuth
// @Tags    Stream
// @Produce json
// @Param   timeout query string false "Maximum wait, e.g. 30s"
// @Success 200 {object} response
// @Failure 404 {object} response
// @Failure 504 {object} response
// @Router /api/stream/pause [post]
func Pause(c *gin.Context) {
	cons := consumer(c)
	if cons == nil {
		return
	}
	timeout := defaultPauseTimeout
	if raw := c.Query("timeout"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			zutils.GinRenderJSON(c, http.StatusBadRequest, response{Error: "timeout must be a positive duration such as 30s"})
			return
		}
		timeout = d
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()
	if err := cons.PauseAndDrain(ctx); err != nil {
		// The consumer stays paused so the caller can retry; Resume undoes it.
		st := cons.Status()
		zutils.GinRenderJSON(c, http.StatusGatewayTimeout, response{Error: err.Error(), Status: &st})
		return
	}
	pending, _ := streaming.WALPending()
	st := cons.Status()
	zutils.GinRenderJSON(c, http.StatusOK, response{Message: "ok", Status: &st, WALPending: &pending})
}

// Resume continues consuming after Pause.
//
// @Id StreamResume
// @Summary Resume the stream consumer
// @security BasicAuth
// @Tags    Stream
// @Produce json
// @Success 200 {object} response
// @Failure 404 {object} response
// @Router /api/stream/resume [post]
func Resume(c *gin.Context) {
	cons := consumer(c)
	if cons == nil {
		return
	}
	cons.Resume()
	st := cons.Status()
	zutils.GinRenderJSON(c, http.StatusOK, response{Message: "ok", Status: &st})
}
