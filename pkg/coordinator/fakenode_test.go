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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeNode is a stand-in for a ZincSearch node: it speaks the HTTP API the
// coordinator uses, and its behaviour can be changed from the test.
type fakeNode struct {
	srv *httptest.Server

	mu         sync.Mutex
	stream     StreamStatus
	down       bool
	backups    map[string][]byte
	infos      map[string]BackupInfo
	backupErr  string
	tamper     bool // serve other bytes than the announced checksum belongs to
	resumeHits int
	created    int
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	f := &fakeNode{
		stream:  StreamStatus{Connected: true, Stream: "zinc", LastApplied: 100, StreamLast: 100},
		backups: map[string][]byte{},
		infos:   map[string]BackupInfo{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", f.healthz)
	mux.HandleFunc("POST /api/stream/resume", f.resume)
	mux.HandleFunc("POST /api/backup", f.createBackup)
	mux.HandleFunc("GET /api/backup/{name}", f.getBackup)
	mux.HandleFunc("DELETE /api/backup/{name}", f.deleteBackup)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		down := f.down
		f.mu.Unlock()
		if down {
			http.Error(w, "the node is down", http.StatusServiceUnavailable)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeNode) config(name string) NodeConfig {
	return NodeConfig{Name: name, URL: f.srv.URL, User: "admin", Password: "secret"}
}

func (f *fakeNode) set(fn func(f *fakeNode)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeNode) healthz(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	st := f.stream
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "details": map[string]interface{}{"stream": st}})
}

func (f *fakeNode) resume(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := r.BasicAuth(); !ok {
		http.Error(w, "no credentials", http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	f.resumeHits++
	f.stream.Paused = false
	f.mu.Unlock()
	_, _ = w.Write([]byte(`{"message":"ok"}`))
}

func (f *fakeNode) createBackup(w http.ResponseWriter, _ *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.backupErr != "" {
		http.Error(w, f.backupErr, http.StatusConflict)
		return
	}
	f.created++
	content := []byte(fmt.Sprintf("archive number %d of the fake node, offset %d", f.created, f.stream.LastApplied))
	sum := sha256.Sum256(content)
	info := BackupInfo{
		Name:        fmt.Sprintf("zincsearch-2026010%dT000000Z-%d.tgz", f.created, f.stream.LastApplied),
		Size:        int64(len(content)),
		SHA256:      hex.EncodeToString(sum[:]),
		CreatedAt:   time.Date(2026, 1, f.created, 0, 0, 0, 0, time.UTC),
		LastApplied: f.stream.LastApplied,
		Indexes:     []string{"logs"},
	}
	f.backups[info.Name] = content
	f.infos[info.Name] = info
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"message": "ok", "backup": info})
}

func (f *fakeNode) getBackup(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := r.PathValue("name")
	content, ok := f.backups[name]
	if !ok {
		http.Error(w, "no such backup", http.StatusNotFound)
		return
	}
	w.Header().Set("X-Zinc-Backup-Sha256", f.infos[name].SHA256)
	if f.tamper {
		content = []byte(strings.ToUpper(string(content)))
	}
	_, _ = w.Write(content)
}

func (f *fakeNode) deleteBackup(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := r.PathValue("name")
	if _, ok := f.backups[name]; !ok {
		http.Error(w, "no such backup", http.StatusNotFound)
		return
	}
	delete(f.backups, name)
	delete(f.infos, name)
	_, _ = w.Write([]byte(`{"message":"ok"}`))
}

func (f *fakeNode) backupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.backups)
}

// clock is a clock the test moves.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}
