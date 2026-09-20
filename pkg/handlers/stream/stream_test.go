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

package stream

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/streaming"
	"github.com/zincsearch/zincsearch/pkg/zutils/json"
	"github.com/zincsearch/zincsearch/test/utils"
)

func call(handler func(*gin.Context), query string) (int, map[string]interface{}) {
	c, w := utils.NewGinContext()
	c.Request.URL.RawQuery = query
	handler(c)
	body := map[string]interface{}{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body
}

func TestEndpointsWithoutConsumer(t *testing.T) {
	require.Nil(t, streaming.Current())
	for name, h := range map[string]func(*gin.Context){"status": Status, "pause": Pause, "resume": Resume} {
		code, body := call(h, "")
		assert.Equal(t, http.StatusNotFound, code, name)
		assert.Contains(t, body["error"], "not enabled", name)
	}
}

func TestPauseResumeAndHealth(t *testing.T) {
	const index = "stream_handler_index"
	t.Cleanup(func() { _ = core.DeleteIndex(index) })

	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(15*time.Second))
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	_, err = js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "zinc", Subjects: []string{"zinc.>"}, Storage: jetstream.MemoryStorage})
	require.NoError(t, err)
	for i := 1; i <= 30; i++ {
		raw, err := streaming.NewDoc(index, fmt.Sprintf("d%d", i), map[string]interface{}{"n": i}).Encode()
		require.NoError(t, err)
		_, err = js.Publish(context.Background(), "zinc.logs", raw)
		require.NoError(t, err)
	}

	// The saved offset lives in the node metadata, which outlives test runs.
	require.NoError(t, metadata.KV.Delete(streaming.OffsetKey))
	t.Cleanup(func() { _ = metadata.KV.Delete(streaming.OffsetKey) })
	config.Global.Stream.URL = ns.ClientURL()
	config.Global.Stream.Name = "zinc"
	config.Global.Stream.Consumer = "node-handler-test"
	config.Global.Stream.Batch = 4
	_, err = streaming.StartFromConfig()
	require.NoError(t, err)
	t.Cleanup(streaming.Shutdown)
	require.Eventually(t, func() bool { return streaming.Current().Status().LastApplied > 0 }, 20*time.Second, 5*time.Millisecond)

	// A bad timeout is refused and does not touch the consumer.
	code, _ := call(Pause, "timeout=soon")
	assert.Equal(t, http.StatusBadRequest, code)
	assert.False(t, streaming.Current().Status().Paused)

	// Pause answers only once the node is quiescent.
	code, body := call(Pause, "timeout=30s")
	require.Equal(t, http.StatusOK, code, "%v", body)
	assert.Equal(t, float64(0), body["wal_pending"])
	status := body["status"].(map[string]interface{})
	assert.Equal(t, true, status["paused"])
	assert.Equal(t, true, status["drained"])
	applied := status["last_applied"]

	// /healthz exposes the same state for coordinators.
	c, w := utils.NewGinContext()
	meta.GetHealthz(c)
	health := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &health))
	assert.Equal(t, "ok", health["status"])
	details := health["details"].(map[string]interface{})["stream"].(map[string]interface{})
	assert.Equal(t, true, details["drained"])
	assert.Equal(t, applied, details["last_applied"])

	code, body = call(Status, "")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, true, body["status"].(map[string]interface{})["paused"])

	code, body = call(Resume, "")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, false, body["status"].(map[string]interface{})["paused"])
	require.Eventually(t, func() bool { return streaming.Current().Status().LastApplied == 30 }, 20*time.Second, 10*time.Millisecond)
}
