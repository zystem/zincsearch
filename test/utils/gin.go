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

package utils

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
)

func NewGinContext() (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.ReleaseMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := &http.Request{
		URL:    &url.URL{},
		Header: make(http.Header),
	}
	c.Request = req
	return c, w
}

func SetGinRequestData(c *gin.Context, data interface{}) {
	c.Request.Header.Set("Content-Type", "application/json;charset=utf-8")
	switch v := data.(type) {
	case string:
		c.Request.Body = io.NopCloser(bytes.NewBufferString(v))
	case map[string]interface{}:
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return
		}
		buf := bytes.NewBuffer(jsonBytes)
		c.Request.Body = io.NopCloser(buf)
	default:
	}
}

func SetGinRequestURL(c *gin.Context, path string, params map[string]string) {
	q := c.Request.URL.Query()
	for k, v := range params {
		q.Add(k, v)
	}
	c.Request.URL.Path = path
	c.Request.URL.RawQuery = q.Encode()
}

func SetGinRequestParams(c *gin.Context, params map[string]string) {
	p := gin.Params{}
	for k, v := range params {
		p = append(p, gin.Param{Key: k, Value: v})
	}
	c.Params = p
}

// WaitWAL blocks until every document accepted by the index is applied, which
// is when it becomes searchable. It replaces sleeping for the WAL interval.
func WaitWAL(t testing.TB, index *core.Index) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		pending, err := index.WALPending()
		if err == nil && pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("index %s still has %d pending WAL entries (err %v)", index.GetName(), pending, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WaitWALByName is WaitWAL for an index that is looked up by name.
func WaitWALByName(t testing.TB, name string) {
	t.Helper()
	index, ok := core.GetIndex(name)
	if !ok {
		t.Fatalf("index %s does not exist", name)
	}
	WaitWAL(t, index)
}
