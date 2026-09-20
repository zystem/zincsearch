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

package backup

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/streaming"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
	"github.com/zincsearch/zincsearch/test/utils"
)

const index = "backup_handler_index"

func call(t *testing.T, h gin.HandlerFunc, query string, params map[string]string) (*gin.Context, int, map[string]interface{}, []byte) {
	t.Helper()
	c, w := utils.NewGinContext()
	c.Request.URL.RawQuery = query
	if params != nil {
		utils.SetGinRequestParams(c, params)
	}
	h(c)
	body := map[string]interface{}{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	c.Writer.WriteHeaderNow()
	return c, w.Code, body, w.Body.Bytes()
}

func quiescentNode(t *testing.T) {
	t.Helper()
	idx, err := core.NewIndex(index, "disk", 2)
	require.NoError(t, err)
	require.NoError(t, core.StoreIndex(idx))
	t.Cleanup(func() { _ = core.DeleteIndex(index) })
	for i := 0; i < 10; i++ {
		require.NoError(t, idx.CreateDocument(fmt.Sprintf("d%d", i), map[string]interface{}{"n": i}, false))
	}
	require.Eventually(t, func() bool {
		n, err := streaming.WALPending()
		return err == nil && n == 0
	}, 20*time.Second, 50*time.Millisecond)
}

func TestNotConfigured(t *testing.T) {
	config.Global.BackupPath = ""
	for name, h := range map[string]gin.HandlerFunc{"create": Create, "list": List, "get": Get, "delete": Delete} {
		_, code, body, _ := call(t, h, "", map[string]string{"name": "x"})
		assert.Equal(t, http.StatusBadRequest, code, name)
		assert.Contains(t, body["error"], "zinc_backup_path", name)
	}
}

func TestCreateListGetDelete(t *testing.T) {
	quiescentNode(t)
	config.Global.BackupPath = t.TempDir()
	t.Cleanup(func() { config.Global.BackupPath = "" })

	_, code, body, _ := call(t, Create, "timeout=soon", nil)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body["error"], "timeout")

	_, code, body, _ = call(t, Create, "", nil)
	require.Equal(t, http.StatusOK, code, "%v", body)
	info := body["backup"].(map[string]interface{})
	name := info["name"].(string)
	assert.Equal(t, []interface{}{index}, info["indexes"])

	_, code, body, _ = call(t, List, "", nil)
	assert.Equal(t, http.StatusOK, code)
	assert.Len(t, body["backups"], 1)

	c, code, _, raw := call(t, Get, "", map[string]string{"name": name})
	assert.Equal(t, http.StatusOK, code)
	onDisk, err := os.ReadFile(filepath.Join(config.Global.BackupPath, name))
	require.NoError(t, err)
	assert.Equal(t, onDisk, raw, "the download is the stored archive")
	assert.Equal(t, info["sha256"], c.Writer.Header().Get("X-Zinc-Backup-Sha256"))

	// A name must not lead out of the backup directory.
	for _, bad := range []string{"../" + name, "..%2F" + name, "other.tgz"} {
		_, code, _, _ = call(t, Get, "", map[string]string{"name": bad})
		assert.Equal(t, http.StatusNotFound, code, bad)
		_, code, _, _ = call(t, Delete, "", map[string]string{"name": bad})
		assert.Equal(t, http.StatusNotFound, code, bad)
	}

	_, code, _, _ = call(t, Delete, "", map[string]string{"name": name})
	assert.Equal(t, http.StatusOK, code)
	_, code, _, _ = call(t, Get, "", map[string]string{"name": name})
	assert.Equal(t, http.StatusNotFound, code)
}

func TestCreateReportsAConflictWhenTheNodeIsBusy(t *testing.T) {
	quiescentNode(t)
	config.Global.BackupPath = t.TempDir()
	t.Cleanup(func() { config.Global.BackupPath = "" })

	// Without a stream consumer nothing pauses writers: an accepted document
	// that is not applied yet makes the node non-quiescent.
	idx, _ := core.GetIndex(index)
	require.NoError(t, idx.CreateDocument("late", map[string]interface{}{"n": 1}, false))
	_, code, body, _ := call(t, Create, "", nil)
	assert.Equal(t, http.StatusConflict, code, "%v", body)
	assert.Contains(t, body["error"], "not quiescent")
}
