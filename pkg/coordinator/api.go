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
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/zincsearch/zincsearch/pkg/coordinator/blob"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
	"github.com/zincsearch/zincsearch/pkg/streaming/wire"
)

// APIConfig configures the HTTP API of a Coordinator.
type APIConfig struct {
	// Token protects every endpoint except /healthz. Requests carry it as
	// "Authorization: Bearer <token>". Empty means no protection.
	Token string
	// MaxBody bounds the size of a request body. Default 32 MiB.
	MaxBody int64
	// MaxDocsPerMessage bounds how many documents go into one stream message.
	// Default 500.
	MaxDocsPerMessage int
	// MaxMessageBytes bounds the size of the documents in one stream message.
	// Default 512 KiB, which fits the default limit of NATS of 1 MiB.
	MaxMessageBytes int
}

func (a *APIConfig) defaults() {
	if a.MaxBody <= 0 {
		a.MaxBody = 32 << 20
	}
	if a.MaxDocsPerMessage <= 0 {
		a.MaxDocsPerMessage = 500
	}
	if a.MaxMessageBytes <= 0 {
		a.MaxMessageBytes = 512 << 10
	}
}

type api struct {
	c   *Coordinator
	cfg APIConfig
}

// Handler returns the HTTP API:
//
//	GET  /healthz                     liveness, no token needed
//	GET  /v1/status                   everything the coordinator knows
//	GET  /v1/admit                    200 when new builds may start, 503 when not
//	GET  /v1/master                   the node that serves reads
//	POST /v1/ingest/{index}           publish documents (JSON object/array, or NDJSON)
//	POST /v1/admin/{index}/{op}       publish an administrative operation
//	GET  /v1/backups                  list stored backups
//	POST /v1/backups                  take a backup now (leader only)
//	GET  /v1/backups/{name}           download a stored backup
//	POST /v1/verify                   verify the stored backups now (leader only)
//	POST /v1/failover                 {"to":"node"}: promote by hand (leader only)
func (c *Coordinator) Handler(cfg APIConfig) http.Handler {
	cfg.defaults()
	a := &api{c: c, cfg: cfg}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /v1/status", a.auth(a.status))
	mux.HandleFunc("GET /v1/admit", a.auth(a.admit))
	mux.HandleFunc("GET /v1/master", a.auth(a.master))
	mux.HandleFunc("POST /v1/ingest/{index}", a.auth(a.ingest))
	mux.HandleFunc("POST /v1/admin/{index}/{op}", a.auth(a.admin))
	mux.HandleFunc("GET /v1/backups", a.auth(a.listBackups))
	mux.HandleFunc("POST /v1/backups", a.auth(a.leaderOnly(a.createBackup)))
	mux.HandleFunc("GET /v1/backups/{name}", a.auth(a.downloadBackup))
	mux.HandleFunc("POST /v1/verify", a.auth(a.leaderOnly(a.verify)))
	mux.HandleFunc("POST /v1/failover", a.auth(a.leaderOnly(a.failover)))
	return mux
}

func reply(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, format string, args ...interface{}) {
	reply(w, code, map[string]string{"error": fmt.Sprintf(format, args...)})
}

func (a *api) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.Token != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(a.cfg.Token)) != 1 {
				fail(w, http.StatusUnauthorized, "missing or wrong token")
				return
			}
		}
		next(w, r)
	}
}

func (a *api) leaderOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !a.c.IsLeader() {
			fail(w, http.StatusConflict, "this coordinator instance is not the leader; ask the leader")
			return
		}
		next(w, r)
	}
}

func (a *api) healthz(w http.ResponseWriter, _ *http.Request) {
	reply(w, http.StatusOK, map[string]interface{}{"status": "ok", "leader": a.c.IsLeader()})
}

// Status is the answer of GET /v1/status.
type Status struct {
	ID        string           `json:"id"`
	Leader    bool             `json:"leader"`
	Master    string           `json:"master"`
	Epoch     uint64           `json:"epoch"`
	Nodes     []NodeStatus     `json:"nodes"`
	Publisher *PublisherHealth `json:"publisher,omitempty"`
	Admission Admission        `json:"admission"`
	Backup    Schedule         `json:"backup_schedule"`
	Verify    Schedule         `json:"verify_schedule"`
	Backups   []BackupRecord   `json:"backups"`
}

// Status returns everything the coordinator knows about the cluster.
func (c *Coordinator) Status(ctx context.Context) (Status, error) {
	cl := c.ClusterState()
	st := Status{
		ID: c.cfg.ID, Leader: c.IsLeader(), Master: cl.Master, Epoch: cl.Epoch,
		Nodes: c.NodeStatuses(), Admission: c.Admit(),
	}
	if c.pub != nil {
		h := c.pub.Health()
		st.Publisher = &h
	}
	st.Backup, st.Verify = c.Schedules(ctx)
	if c.blobs != nil {
		recs, err := c.Backups(ctx)
		if err != nil {
			return st, err
		}
		st.Backups = recs
	}
	return st, nil
}

func (a *api) status(w http.ResponseWriter, r *http.Request) {
	st, err := a.c.Status(r.Context())
	if err != nil {
		fail(w, http.StatusInternalServerError, "%v", err)
		return
	}
	reply(w, http.StatusOK, st)
}

func (a *api) admit(w http.ResponseWriter, _ *http.Request) {
	adm := a.c.Admit()
	code := http.StatusOK
	if !adm.Allowed {
		code = http.StatusServiceUnavailable
	}
	reply(w, code, adm)
}

func (a *api) master(w http.ResponseWriter, _ *http.Request) {
	m, ok := a.c.Master()
	if !ok {
		fail(w, http.StatusServiceUnavailable, "the master is not known yet")
		return
	}
	reply(w, http.StatusOK, map[string]interface{}{"name": m.Name, "url": m.URL, "epoch": a.c.ClusterState().Epoch})
}

// docLine is one parsed document with its size, so requests can be split into
// messages without parsing them twice.
type docLine struct {
	doc  wire.Doc
	size int
}

// parseDocs reads documents from a request body: newline delimited JSON, a JSON
// array of objects, or one JSON object. A string field "_id" names the document
// and is not stored in it. idempotencyKey, when set, names the documents that
// have no ID after their position in the request, so a retried request replaces
// what the first attempt wrote instead of adding a second copy.
func parseDocs(body io.Reader, contentType, idempotencyKey string) ([]docLine, error) {
	var objects []map[string]interface{}
	ndjson := strings.Contains(contentType, "ndjson") || strings.Contains(contentType, "jsonl")
	if ndjson {
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 64<<10), 8<<20)
		for n := 1; sc.Scan(); n++ {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var obj map[string]interface{}
			if err := json.Unmarshal(line, &obj); err != nil {
				return nil, fmt.Errorf("line %d is not a JSON object: %v", n, err)
			}
			objects = append(objects, obj)
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	} else {
		raw, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			return nil, errors.New("empty body")
		}
		if raw[0] == '[' {
			if err := json.Unmarshal(raw, &objects); err != nil {
				return nil, fmt.Errorf("body is not an array of JSON objects: %v", err)
			}
		} else {
			var obj map[string]interface{}
			if err := json.Unmarshal(raw, &obj); err != nil {
				return nil, fmt.Errorf("body is not a JSON object: %v", err)
			}
			objects = []map[string]interface{}{obj}
		}
	}
	if len(objects) == 0 {
		return nil, errors.New("no documents in the body")
	}

	docs := make([]docLine, 0, len(objects))
	for i, obj := range objects {
		if obj == nil {
			return nil, fmt.Errorf("document %d is null", i+1)
		}
		id := ""
		if v, ok := obj["_id"].(string); ok && v != "" {
			id = v
			delete(obj, "_id")
		}
		if id == "" && idempotencyKey != "" {
			id = fmt.Sprintf("%s-%d", idempotencyKey, i)
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			return nil, err
		}
		docs = append(docs, docLine{doc: wire.Doc{ID: id, Doc: obj}, size: len(raw) + len(id) + 32})
	}
	return docs, nil
}

func (a *api) ingest(w http.ResponseWriter, r *http.Request) {
	if a.c.pub == nil {
		fail(w, http.StatusNotImplemented, "this coordinator has no publisher")
		return
	}
	index := r.PathValue("index")
	if index == "" || strings.HasPrefix(index, "_") || strings.ContainsAny(index, "/ \\") {
		fail(w, http.StatusBadRequest, "invalid index name %q", index)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key != "" && !state.ValidKey(key) {
		fail(w, http.StatusBadRequest, "Idempotency-Key must be made of letters, digits and \"/_=.-\"")
		return
	}
	docs, err := parseDocs(http.MaxBytesReader(w, r.Body, a.cfg.MaxBody), r.Header.Get("Content-Type"), key)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			fail(w, http.StatusRequestEntityTooLarge, "the body is larger than %d bytes", a.cfg.MaxBody)
			return
		}
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}

	// Split into messages before publishing anything, so a document that can
	// never fit is refused with nothing accepted.
	var batches [][]wire.Doc
	var current []wire.Doc
	currentSize := 0
	for _, d := range docs {
		if d.size > a.cfg.MaxMessageBytes {
			fail(w, http.StatusRequestEntityTooLarge, "a document is larger than %d bytes", a.cfg.MaxMessageBytes)
			return
		}
		if len(current) > 0 && (len(current) >= a.cfg.MaxDocsPerMessage || currentSize+d.size > a.cfg.MaxMessageBytes) {
			batches, current, currentSize = append(batches, current), nil, 0
		}
		current, currentSize = append(current, d.doc), currentSize+d.size
	}
	batches = append(batches, current)

	accepted := 0
	for i, batch := range batches {
		if err := a.c.pub.PublishDocs(r.Context(), index, batch); err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, ErrBackpressure) {
				code = http.StatusServiceUnavailable
				w.Header().Set("Retry-After", "5")
			}
			if errors.Is(err, ErrMessageTooLarge) {
				code = http.StatusRequestEntityTooLarge
			}
			reply(w, code, map[string]interface{}{"error": err.Error(), "accepted_documents": accepted, "accepted_messages": i})
			return
		}
		accepted += len(batch)
	}
	reply(w, http.StatusAccepted, map[string]interface{}{"accepted_documents": accepted, "messages": len(batches)})
}

func (a *api) admin(w http.ResponseWriter, r *http.Request) {
	if a.c.pub == nil {
		fail(w, http.StatusNotImplemented, "this coordinator has no publisher")
		return
	}
	op := wire.Op(r.PathValue("op"))
	switch op {
	case wire.OpCreateIndex, wire.OpDeleteIndex, wire.OpSetMapping:
	default:
		fail(w, http.StatusBadRequest, "unknown operation %q", op)
		return
	}
	var data json.RawMessage
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, a.cfg.MaxBody))
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if !json.Valid(body) {
			fail(w, http.StatusBadRequest, "the body is not valid JSON")
			return
		}
		data = body
	}
	var payload interface{}
	if data != nil {
		payload = data
	}
	msg, err := wire.NewAdmin(r.PathValue("index"), op, payload)
	if err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}
	if _, err := wire.Decode(mustEncode(msg)); err != nil {
		fail(w, http.StatusBadRequest, "%v", err)
		return
	}
	if err := a.c.pub.Publish(r.Context(), msg); err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ErrBackpressure) {
			code = http.StatusServiceUnavailable
		}
		fail(w, code, "%v", err)
		return
	}
	reply(w, http.StatusAccepted, map[string]string{"message": "accepted"})
}

func mustEncode(m *wire.Message) []byte {
	raw, _ := m.Encode()
	return raw
}

func (a *api) listBackups(w http.ResponseWriter, r *http.Request) {
	if a.c.blobs == nil {
		fail(w, http.StatusNotImplemented, "this coordinator has no blob store")
		return
	}
	recs, err := a.c.Backups(r.Context())
	if err != nil {
		fail(w, http.StatusInternalServerError, "%v", err)
		return
	}
	reply(w, http.StatusOK, map[string]interface{}{"backups": recs})
}

func (a *api) createBackup(w http.ResponseWriter, r *http.Request) {
	if !a.c.jobMu.TryLock() {
		fail(w, http.StatusConflict, "a backup or verification is already running")
		return
	}
	defer a.c.jobMu.Unlock()
	rec, err := a.c.BackupOnce(r.Context())
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ErrNoBackupNode) {
			code = http.StatusServiceUnavailable
		}
		fail(w, code, "%v", err)
		return
	}
	reply(w, http.StatusOK, rec)
}

func (a *api) verify(w http.ResponseWriter, r *http.Request) {
	if !a.c.jobMu.TryLock() {
		fail(w, http.StatusConflict, "a backup or verification is already running")
		return
	}
	defer a.c.jobMu.Unlock()
	if err := a.c.VerifyOnce(r.Context()); err != nil {
		fail(w, http.StatusInternalServerError, "%v", err)
		return
	}
	reply(w, http.StatusOK, map[string]string{"message": "all stored backups are good"})
}

func (a *api) downloadBackup(w http.ResponseWriter, r *http.Request) {
	if a.c.blobs == nil {
		fail(w, http.StatusNotImplemented, "this coordinator has no blob store")
		return
	}
	name := r.PathValue("name")
	var rec BackupRecord
	if _, err := state.GetJSON(r.Context(), a.c.st, backupPrefix+name, &rec); err != nil {
		if errors.Is(err, state.ErrNotFound) {
			fail(w, http.StatusNotFound, "no backup %q", name)
			return
		}
		fail(w, http.StatusInternalServerError, "%v", err)
		return
	}
	rc, err := a.c.blobs.Get(r.Context(), name)
	if errors.Is(err, blob.ErrNotFound) {
		fail(w, http.StatusNotFound, "no backup %q", name)
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "%v", err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("X-Zinc-Backup-Sha256", rec.SHA256)
	w.Header().Set("Content-Length", fmt.Sprint(rec.Size))
	_, _ = io.Copy(w, rc)
}

func (a *api) failover(w http.ResponseWriter, r *http.Request) {
	var req struct {
		To string `json:"to"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil || req.To == "" {
		fail(w, http.StatusBadRequest, `body must be {"to":"<node name>"}`)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := a.c.Promote(ctx, req.To); err != nil {
		fail(w, http.StatusConflict, "%v", err)
		return
	}
	m, _ := a.c.Master()
	reply(w, http.StatusOK, map[string]interface{}{"master": m.Name, "epoch": a.c.ClusterState().Epoch})
}
