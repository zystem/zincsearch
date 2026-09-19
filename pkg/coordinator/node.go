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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// NodeConfig describes one ZincSearch node.
type NodeConfig struct {
	// Name identifies the node in the state and in status output. It has to be
	// stable and unique, e.g. the name of the pod.
	Name string `json:"name"`
	// URL is the base URL of the node's HTTP API, e.g. "http://zinc-0:4080".
	URL string `json:"url"`
	// User and Password are the credentials for the endpoints that need them.
	User     string `json:"user,omitempty"`
	Password string `json:"-"`
}

// StreamStatus is the replication state a node reports on /healthz.
type StreamStatus struct {
	Connected     bool   `json:"connected"`
	Paused        bool   `json:"paused"`
	Drained       bool   `json:"drained"`
	Stream        string `json:"stream"`
	Consumer      string `json:"consumer"`
	LastApplied   uint64 `json:"last_applied"`
	StreamLast    uint64 `json:"stream_last"`
	Lag           uint64 `json:"lag"`
	AppliedTotal  uint64 `json:"applied_total"`
	SkippedTotal  uint64 `json:"skipped_total"`
	LastError     string `json:"last_error,omitempty"`
	LastErrorTime string `json:"last_error_time,omitempty"`
}

// BackupInfo describes a backup a node created.
type BackupInfo struct {
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	CreatedAt   time.Time `json:"created_at"`
	LastApplied uint64    `json:"last_applied"`
	Indexes     []string  `json:"indexes"`
}

// NodeError is an error answer of a node.
type NodeError struct {
	Node   string
	Status int
	Body   string
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("node %s answered %d: %s", e.Node, e.Status, strings.TrimSpace(e.Body))
}

// Node talks to one ZincSearch node over HTTP.
type Node struct {
	cfg  NodeConfig
	http *http.Client
}

// NewNode returns a client for the node. The HTTP client has no overall timeout:
// creating a backup takes as long as it takes, so callers bound calls by context.
func NewNode(cfg NodeConfig) *Node {
	return &Node{cfg: cfg, http: &http.Client{}}
}

// Config returns the configuration of the node.
func (n *Node) Config() NodeConfig { return n.cfg }

func (n *Node) do(ctx context.Context, method, path string, query url.Values, auth bool) (*http.Response, error) {
	u := strings.TrimRight(n.cfg.URL, "/") + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	if auth {
		req.SetBasicAuth(n.cfg.User, n.cfg.Password)
	}
	resp, err := n.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &NodeError{Node: n.cfg.Name, Status: resp.StatusCode, Body: string(body)}
	}
	return resp, nil
}

func (n *Node) doJSON(ctx context.Context, method, path string, query url.Values, out interface{}) error {
	resp, err := n.do(ctx, method, path, query, true)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Health returns the replication state of the node. It fails when the node does
// not answer or does not consume a stream.
func (n *Node) Health(ctx context.Context) (StreamStatus, error) {
	resp, err := n.do(ctx, http.MethodGet, "/healthz", nil, false)
	if err != nil {
		return StreamStatus{}, err
	}
	defer resp.Body.Close()
	var body struct {
		Details struct {
			Stream *StreamStatus `json:"stream"`
		} `json:"details"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return StreamStatus{}, fmt.Errorf("node %s: unreadable /healthz: %w", n.cfg.Name, err)
	}
	if body.Details.Stream == nil {
		return StreamStatus{}, fmt.Errorf("node %s does not consume a stream (zinc_stream_enable)", n.cfg.Name)
	}
	return *body.Details.Stream, nil
}

// Resume tells the node to continue consuming.
func (n *Node) Resume(ctx context.Context) error {
	return n.doJSON(ctx, http.MethodPost, "/api/stream/resume", nil, nil)
}

// CreateBackup makes the node take a backup. The node pauses and drains its
// consumer for it, waiting up to drainTimeout, and resumes afterwards. The call
// returns when the archive is complete.
func (n *Node) CreateBackup(ctx context.Context, drainTimeout time.Duration) (BackupInfo, error) {
	q := url.Values{"timeout": {drainTimeout.String()}}
	var out struct {
		Backup BackupInfo `json:"backup"`
	}
	if err := n.doJSON(ctx, http.MethodPost, "/api/backup", q, &out); err != nil {
		return BackupInfo{}, err
	}
	if out.Backup.Name == "" {
		return BackupInfo{}, fmt.Errorf("node %s returned no backup", n.cfg.Name)
	}
	return out.Backup, nil
}

// Download is an open backup download.
type Download struct {
	io.ReadCloser
	// Size is the length of the archive as the node reports it.
	Size int64
	// SHA256 is the checksum of the whole archive as the node reports it.
	SHA256 string
}

// DownloadBackup opens the archive of a backup for reading.
func (n *Node) DownloadBackup(ctx context.Context, name string) (*Download, error) {
	resp, err := n.do(ctx, http.MethodGet, "/api/backup/"+url.PathEscape(name), nil, true)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		size = -1
	}
	return &Download{ReadCloser: resp.Body, Size: size, SHA256: resp.Header.Get("X-Zinc-Backup-Sha256")}, nil
}

// DeleteBackup removes a backup from the node's disk.
func (n *Node) DeleteBackup(ctx context.Context, name string) error {
	return n.doJSON(ctx, http.MethodDelete, "/api/backup/"+url.PathEscape(name), nil, nil)
}
