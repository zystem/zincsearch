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
	"strings"
	"sync"
	"time"
)

// NodeState is what the coordinator makes of the last answers of a node.
type NodeState string

const (
	// NodeUnknown: the node was not asked yet.
	NodeUnknown NodeState = "unknown"
	// NodeUp: reachable, consuming the stream and not far behind. Only such a
	// node can be promoted.
	NodeUp NodeState = "up"
	// NodeLagging: consuming, but further behind than allowed.
	NodeLagging NodeState = "lagging"
	// NodeDegraded: reachable, but its consumer is not connected to the stream.
	NodeDegraded NodeState = "degraded"
	// NodeSuspect: the last answers failed, but not yet often enough to call it down.
	NodeSuspect NodeState = "suspect"
	// NodeDown: did not answer for FailThreshold polls in a row.
	NodeDown NodeState = "down"
	// NodeNeedsRestore: reachable, but it cannot continue the stream because its
	// data is behind what the stream still holds. It has to be restored from a backup.
	NodeNeedsRestore NodeState = "needs_restore"
)

// Messages a node reports when it cannot consume; see package streaming.
const (
	gapMessage   = "does not reach back"
	aheadMessage = "is ahead of the stream"
)

// NodeStatus is the coordinator's view of one node.
type NodeStatus struct {
	Name     string       `json:"name"`
	URL      string       `json:"url"`
	State    NodeState    `json:"state"`
	Failures int          `json:"consecutive_failures"`
	LastSeen time.Time    `json:"last_seen,omitempty"`
	Stream   StreamStatus `json:"stream"`
	Error    string       `json:"error,omitempty"`
}

func classify(s NodeStatus, cfg Config) NodeState {
	switch {
	case s.Failures >= cfg.FailThreshold:
		return NodeDown
	case s.Failures > 0:
		return NodeSuspect
	case s.LastSeen.IsZero():
		return NodeUnknown
	case strings.Contains(s.Stream.LastError, gapMessage) || strings.Contains(s.Stream.LastError, aheadMessage):
		return NodeNeedsRestore
	case !s.Stream.Connected:
		return NodeDegraded
	case s.Stream.Lag > cfg.MaxLag:
		return NodeLagging
	default:
		return NodeUp
	}
}

// PollOnce asks every node for its state, concurrently, and updates the
// coordinator's view.
func (c *Coordinator) PollOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, name := range c.order {
		node := c.nodes[name]
		wg.Add(1)
		go func() {
			defer wg.Done()
			askCtx, cancel := context.WithTimeout(ctx, c.cfg.PollTimeout)
			defer cancel()
			stream, err := node.Health(askCtx)

			c.mu.Lock()
			defer c.mu.Unlock()
			s := c.status[node.cfg.Name]
			if err != nil {
				s.Failures++
				s.Error = err.Error()
			} else {
				s.Failures = 0
				s.Error = ""
				s.LastSeen = c.cfg.Now()
				s.Stream = stream
			}
			s.State = classify(s, c.cfg)
			c.status[node.cfg.Name] = s
		}()
	}
	wg.Wait()
}

// NodeStatuses returns the current view of all nodes, in configuration order.
func (c *Coordinator) NodeStatuses() []NodeStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]NodeStatus, 0, len(c.order))
	for _, name := range c.order {
		out = append(out, c.status[name])
	}
	return out
}

func (c *Coordinator) nodeStatus(name string) NodeStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status[name]
}
