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

package coordinator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/streaming/wire"
)

const testToken = "s3cret"

// apiEnv is a coordinator with fake nodes, a publisher on an embedded NATS, and
// its HTTP API.
type apiEnv struct {
	cl   *cluster
	nats *natsServer
	pub  *Publisher
	c    *Coordinator
	srv  *httptest.Server
}

func newAPIEnv(t *testing.T, cfg APIConfig, bufferBytes uint64) *apiEnv {
	t.Helper()
	cl := newCluster(t)
	n := newNATSServer(t)
	n.createStream()
	pub, _ := testPublisher(t, n, bufferBytes)
	c, err := New(Config{
		ID: "api", Nodes: []NodeConfig{cl.nodes["a"].config("a"), cl.nodes["b"].config("b")},
		FailThreshold: 2, MaxLag: 50, PromoteCooldown: time.Minute, BackupInterval: time.Hour, BackupRetry: time.Minute,
		BackupRetention: 3, VerifyInterval: 24 * time.Hour, Now: cl.clk.now,
	}, Deps{State: cl.st, Blobs: cl.blobs, Publisher: pub})
	require.NoError(t, err)
	c.PollOnce(ctx)
	c.refreshCluster(ctx)
	c.tryLease(ctx)
	cfg.Token = testToken
	srv := httptest.NewServer(c.Handler(cfg))
	t.Cleanup(srv.Close)
	cl.c = c
	return &apiEnv{cl: cl, nats: n, pub: pub, c: c, srv: srv}
}

func (e *apiEnv) do(method, path, contentType string, body string, header ...string) (*http.Response, []byte) {
	e.cl.t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	require.NoError(e.cl.t, err)
	req.Header.Set("Authorization", "Bearer "+testToken)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.cl.t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(e.cl.t, err)
	return resp, raw
}

func asMap(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()
	m := map[string]interface{}{}
	require.NoError(t, json.Unmarshal(raw, &m), string(raw))
	return m
}

func TestAPIRequiresTheToken(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1<<20)
	for _, path := range []string{"/v1/status", "/v1/admit", "/v1/master", "/v1/backups"} {
		req, _ := http.NewRequest(http.MethodGet, e.srv.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, path)

		req.Header.Set("Authorization", "Bearer wrong")
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, path)
	}
	resp, err := http.Get(e.srv.URL + "/healthz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "the liveness probe needs no token")
}

func TestIngestAcceptsThreeBodyFormats(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1<<20)

	resp, raw := e.do("POST", "/v1/ingest/logs", "application/x-ndjson", "{\"line\":\"one\"}\n\n{\"line\":\"two\",\"_id\":\"mine\"}\n")
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	assert.Equal(t, float64(2), asMap(t, raw)["accepted_documents"])

	resp, raw = e.do("POST", "/v1/ingest/logs", "application/json", `[{"line":"three"},{"line":"four"}]`)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))

	resp, raw = e.do("POST", "/v1/ingest/logs", "application/json", `{"line":"five"}`)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))

	msgs := e.nats.readAll()
	require.Len(t, msgs, 3)
	assert.Equal(t, wire.KindDocs, msgs[0].Kind)
	assert.Equal(t, "logs", msgs[0].Index)
	require.Len(t, msgs[0].Docs, 2)
	assert.Equal(t, "mine", msgs[0].Docs[1].ID, "the _id field names the document")
	assert.NotContains(t, msgs[0].Docs[1].Doc, "_id", "and is not part of it")
	assert.Equal(t, "one", msgs[0].Docs[0].Doc["line"])
	assert.NotEmpty(t, msgs[0].Docs[0].ID, "the publisher names the others")
}

func TestIngestSplitsBigRequestsIntoMessages(t *testing.T) {
	e := newAPIEnv(t, APIConfig{MaxDocsPerMessage: 2, MaxMessageBytes: 300}, 1<<20)

	var body strings.Builder
	for i := 0; i < 5; i++ {
		fmt.Fprintf(&body, "{\"n\":%d}\n", i)
	}
	resp, raw := e.do("POST", "/v1/ingest/logs", "application/x-ndjson", body.String())
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	assert.Equal(t, float64(3), asMap(t, raw)["messages"], "2 + 2 + 1 documents")

	// By size: each document is ~100 bytes, so two fit in 300 bytes at most.
	pad := strings.Repeat("x", 90)
	body.Reset()
	for i := 0; i < 4; i++ {
		fmt.Fprintf(&body, "{\"pad\":\"%s\"}\n", pad)
	}
	resp, raw = e.do("POST", "/v1/ingest/other", "application/x-ndjson", body.String())
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	assert.Greater(t, asMap(t, raw)["messages"], float64(1))

	var total int
	for _, m := range e.nats.readAll() {
		total += len(m.Docs)
	}
	assert.Equal(t, 9, total, "every document arrives exactly once")
}

func TestIngestRefusesBadInputWithoutPublishingAnything(t *testing.T) {
	e := newAPIEnv(t, APIConfig{MaxMessageBytes: 200, MaxBody: 1 << 10}, 1<<20)
	cases := map[string]struct {
		path, ct, body string
		header         []string
		want           int
	}{
		"broken json":           {"/v1/ingest/logs", "application/json", `{"a":`, nil, 400},
		"an array of numbers":   {"/v1/ingest/logs", "application/json", `[1,2]`, nil, 400},
		"broken ndjson line":    {"/v1/ingest/logs", "application/x-ndjson", "{\"a\":1}\nnot json\n", nil, 400},
		"empty body":            {"/v1/ingest/logs", "application/json", ``, nil, 400},
		"an empty array":        {"/v1/ingest/logs", "application/json", `[]`, nil, 400},
		"a reserved index name": {"/v1/ingest/_meta", "application/json", `{"a":1}`, nil, 400},
		"a bad idempotency key": {"/v1/ingest/logs", "application/json", `{"a":1}`, []string{"Idempotency-Key", "no spaces allowed"}, 400},
		"a document too large":  {"/v1/ingest/logs", "application/json", `{"a":"` + strings.Repeat("x", 400) + `"}`, nil, 413},
		"a body too large":      {"/v1/ingest/logs", "application/json", `[` + strings.Repeat(`{"a":1},`, 400) + `{"a":1}]`, nil, 413},
		// The last document of an otherwise good request is too large: nothing of it is published.
		"one bad among good": {"/v1/ingest/logs", "application/x-ndjson", "{\"a\":1}\n{\"a\":\"" + strings.Repeat("x", 300) + "\"}\n", nil, 413},
	}
	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			resp, raw := e.do("POST", tt.path, tt.ct, tt.body, tt.header...)
			assert.Equal(t, tt.want, resp.StatusCode, string(raw))
			assert.Contains(t, asMap(t, raw), "error")
		})
	}
	assert.Empty(t, e.nats.readAll(), "nothing reached the stream")
}

// A producer that does not know whether its request went through sends it again;
// with an Idempotency-Key the second copy replaces the first one.
func TestIngestIdempotencyKeyGivesDocumentsStableIDs(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1<<20)
	body := "{\"n\":1}\n{\"n\":2}\n{\"_id\":\"named\",\"n\":3}\n"
	for i := 0; i < 2; i++ {
		resp, raw := e.do("POST", "/v1/ingest/logs", "application/x-ndjson", body, "Idempotency-Key", "build-42-part-1")
		require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	}
	msgs := e.nats.readAll()
	require.Len(t, msgs, 2, "the stream holds both attempts")
	for i, d := range msgs[0].Docs {
		assert.Equal(t, d.ID, msgs[1].Docs[i].ID, "document %d has the same ID in both attempts", i)
	}
	assert.Equal(t, "build-42-part-1-0", msgs[0].Docs[0].ID)
	assert.Equal(t, "named", msgs[0].Docs[2].ID)
}

func TestIngestPushesBackWhenTheStreamIsDownAndTheBufferFull(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1500)
	e.nats.stop()

	accepted, refused := 0, 0
	var refusal map[string]interface{}
	var header http.Header
	for i := 0; i < 50 && refused == 0; i++ {
		resp, raw := e.do("POST", "/v1/ingest/logs", "application/json", fmt.Sprintf(`{"n":%d}`, i))
		switch resp.StatusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusServiceUnavailable:
			refused++
			refusal, header = asMap(t, raw), resp.Header
		default:
			t.Fatalf("unexpected %d: %s", resp.StatusCode, raw)
		}
	}
	require.Equal(t, 1, refused)
	require.Greater(t, accepted, 2)
	assert.Equal(t, "5", header.Get("Retry-After"))
	assert.Equal(t, float64(0), refusal["accepted_documents"])
	assert.Contains(t, refusal["error"], "buffer is full")

	// The producers are told to stop, and builds are not admitted either.
	resp, raw := e.do("GET", "/v1/admit", "", "")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, string(raw))

	e.nats.start()
	require.Eventually(t, func() bool { return e.pub.Health().Buffered == 0 }, 20*time.Second, 20*time.Millisecond)
	total := 0
	for _, m := range e.nats.readAll() {
		total += len(m.Docs)
	}
	assert.Equal(t, accepted, total, "everything that was accepted arrived")
}

func TestAdminOperationsBecomeStreamMessages(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1<<20)

	resp, raw := e.do("POST", "/v1/admin/logs/create_index", "application/json", `{"mappings":{"properties":{"line":{"type":"text"}}}}`)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	resp, raw = e.do("POST", "/v1/admin/logs/set_mapping", "application/json", `{"properties":{"level":{"type":"keyword"}}}`)
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))
	resp, raw = e.do("POST", "/v1/admin/logs/delete_index", "", "")
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(raw))

	msgs := e.nats.readAll()
	require.Len(t, msgs, 3)
	assert.Equal(t, wire.OpCreateIndex, msgs[0].Op)
	assert.JSONEq(t, `{"mappings":{"properties":{"line":{"type":"text"}}}}`, string(msgs[0].Data))
	assert.Equal(t, wire.OpSetMapping, msgs[1].Op)
	assert.Equal(t, wire.OpDeleteIndex, msgs[2].Op)
	assert.Empty(t, msgs[2].Data)

	resp, _ = e.do("POST", "/v1/admin/logs/explode", "", "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp, _ = e.do("POST", "/v1/admin/logs/create_index", "application/json", `{not json`)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Len(t, e.nats.readAll(), 3, "refused operations are not published")
}

func TestAdmitMasterAndStatus(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1<<20)

	resp, raw := e.do("GET", "/v1/admit", "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	assert.Equal(t, true, asMap(t, raw)["allowed"])

	resp, raw = e.do("GET", "/v1/master", "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	m := asMap(t, raw)
	assert.Equal(t, "a", m["name"])
	assert.Equal(t, e.cl.nodes["a"].srv.URL, m["url"])

	resp, raw = e.do("GET", "/v1/status", "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	st := asMap(t, raw)
	assert.Equal(t, "a", st["master"])
	assert.Equal(t, true, st["leader"])
	assert.Len(t, st["nodes"], 2)
	assert.Contains(t, st, "publisher")
	assert.Contains(t, st, "admission")

	e.cl.nodes["a"].set(func(f *fakeNode) { f.down = true })
	e.cl.nodes["b"].set(func(f *fakeNode) { f.down = true })
	e.c.PollOnce(ctx)
	e.c.PollOnce(ctx)
	resp, raw = e.do("GET", "/v1/admit", "", "")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, fmt.Sprint(asMap(t, raw)["reasons"]), "no ZincSearch node")
}

func TestBackupEndpointsAndLeaderGate(t *testing.T) {
	e := newAPIEnv(t, APIConfig{}, 1<<20)

	resp, raw := e.do("POST", "/v1/backups", "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	name := asMap(t, raw)["name"].(string)

	resp, raw = e.do("GET", "/v1/backups", "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, asMap(t, raw)["backups"], 1)

	resp, raw = e.do("GET", "/v1/backups/"+name, "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "archive number 1 of the fake node, offset 100", string(raw))
	assert.NotEmpty(t, resp.Header.Get("X-Zinc-Backup-Sha256"))

	resp, _ = e.do("GET", "/v1/backups/zincsearch-nothing.tgz", "", "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, raw = e.do("POST", "/v1/verify", "", "")
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))

	// A coordinator that is not the leader does not start jobs or failovers.
	e.c.leader.Store(false)
	for _, path := range []string{"/v1/backups", "/v1/verify", "/v1/failover"} {
		resp, _ = e.do("POST", path, "application/json", `{"to":"b"}`)
		assert.Equal(t, http.StatusConflict, resp.StatusCode, path)
	}
	e.c.leader.Store(true)

	resp, raw = e.do("POST", "/v1/failover", "application/json", `{"to":"b"}`)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	assert.Equal(t, "b", asMap(t, raw)["master"])
	resp, _ = e.do("POST", "/v1/failover", "application/json", `{"to":"nobody"}`)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	resp, _ = e.do("POST", "/v1/failover", "application/json", `{}`)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
