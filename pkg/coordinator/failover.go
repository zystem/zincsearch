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
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
)

const clusterKey = "cluster"

// Cluster is the shared decision about who is master. Every coordinator
// instance reads it; only the leader changes it, with a compare-and-set.
type Cluster struct {
	Master    string    `json:"master"`
	Epoch     uint64    `json:"epoch"`
	ChangedAt time.Time `json:"changed_at"`
	Reason    string    `json:"reason,omitempty"`
}

// Promotion is one entry of the history of who was master.
type Promotion struct {
	Epoch  uint64    `json:"epoch"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

// Master returns the node that currently serves reads.
func (c *Coordinator) Master() (NodeConfig, bool) {
	c.mu.RLock()
	name := c.cluster.Master
	c.mu.RUnlock()
	n, ok := c.nodes[name]
	if !ok {
		return NodeConfig{}, false
	}
	return n.cfg, true
}

// ClusterState returns the last decision about the master that this instance saw.
func (c *Coordinator) ClusterState() Cluster {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cluster
}

// refreshCluster reads the shared decision, creating the initial one.
func (c *Coordinator) refreshCluster(ctx context.Context) {
	cl, _, err := c.loadCluster(ctx)
	if err != nil {
		log.Error().Err(err).Msg("coordinator: read the cluster state")
		return
	}
	c.mu.Lock()
	c.cluster = cl
	c.mu.Unlock()
}

// loadCluster reads the cluster state, creating it with the first configured node
// as master when it does not exist. It also returns the revision to update against.
func (c *Coordinator) loadCluster(ctx context.Context) (Cluster, uint64, error) {
	var cl Cluster
	rev, err := state.GetJSON(ctx, c.st, clusterKey, &cl)
	if err == nil {
		if _, known := c.nodes[cl.Master]; !known {
			return Cluster{}, 0, fmt.Errorf("the state names master %q, which is not a configured node", cl.Master)
		}
		return cl, rev, nil
	}
	if !errors.Is(err, state.ErrNotFound) {
		return Cluster{}, 0, err
	}
	cl = Cluster{Master: c.order[0], Epoch: 1, ChangedAt: c.cfg.Now(), Reason: "initial"}
	rev, err = state.CreateJSON(ctx, c.st, clusterKey, cl)
	if errors.Is(err, state.ErrConflict) { // another instance was first
		return c.loadCluster(ctx)
	}
	return cl, rev, err
}

// FailoverOnce promotes a replica when the master is gone. It returns the name of
// the new master, or "" when nothing changed. Only the leader should call it.
//
// Promotion moves no data: every node consumes the same stream, so a replica
// that is up and not far behind already holds what the master holds. What
// changes is which node the readers are sent to.
func (c *Coordinator) FailoverOnce(ctx context.Context) (string, error) {
	cl, rev, err := c.loadCluster(ctx)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.cluster = cl
	c.mu.Unlock()

	master := c.nodeStatus(cl.Master)
	if master.State != NodeDown && master.State != NodeNeedsRestore {
		return "", nil
	}
	if c.cfg.Now().Sub(cl.ChangedAt) < c.cfg.PromoteCooldown {
		return "", nil
	}

	candidate := ""
	var freshest uint64
	for _, s := range c.NodeStatuses() {
		if s.Name == cl.Master || s.State != NodeUp {
			continue
		}
		if candidate == "" || s.Stream.LastApplied > freshest {
			candidate, freshest = s.Name, s.Stream.LastApplied
		}
	}
	if candidate == "" {
		log.Warn().Str("master", cl.Master).Str("state", string(master.State)).Msg("coordinator: the master is unavailable and no replica can take over")
		return "", nil
	}
	reason := fmt.Sprintf("master %s is %s", cl.Master, master.State)
	if err := c.promote(ctx, cl, rev, candidate, reason); err != nil {
		if errors.Is(err, state.ErrConflict) {
			c.refreshCluster(ctx) // another instance decided first: adopt its decision
			return "", nil
		}
		return "", err
	}
	return candidate, nil
}

// Promote makes a node the master by hand. The node must be reachable.
func (c *Coordinator) Promote(ctx context.Context, name string) error {
	if _, ok := c.nodes[name]; !ok {
		return fmt.Errorf("unknown node %q", name)
	}
	switch c.nodeStatus(name).State {
	case NodeDown, NodeSuspect, NodeUnknown, NodeNeedsRestore:
		return fmt.Errorf("node %s is %s and cannot become master", name, c.nodeStatus(name).State)
	}
	cl, rev, err := c.loadCluster(ctx)
	if err != nil {
		return err
	}
	if cl.Master == name {
		return nil
	}
	return c.promote(ctx, cl, rev, name, "promoted by hand")
}

func (c *Coordinator) promote(ctx context.Context, cl Cluster, rev uint64, to, reason string) error {
	next := Cluster{Master: to, Epoch: cl.Epoch + 1, ChangedAt: c.cfg.Now(), Reason: reason}
	if _, err := state.UpdateJSON(ctx, c.st, clusterKey, next, rev); err != nil {
		return err
	}
	c.mu.Lock()
	c.cluster = next
	c.mu.Unlock()
	log.Warn().Str("from", cl.Master).Str("to", to).Uint64("epoch", next.Epoch).Str("reason", reason).Msg("coordinator: promoted")

	entry := Promotion{Epoch: next.Epoch, From: cl.Master, To: to, At: next.ChangedAt, Reason: reason}
	if _, err := state.CreateJSON(ctx, c.st, fmt.Sprintf("history/%020d", next.Epoch), entry); err != nil {
		log.Error().Err(err).Msg("coordinator: write the promotion history")
	}

	// A node that is paused, e.g. because it was interrupted while it made a
	// backup, would answer with data that gets older; make sure it consumes.
	if s := c.nodeStatus(to); s.Stream.Paused {
		if err := c.nodes[to].Resume(ctx); err != nil {
			log.Error().Err(err).Str("node", to).Msg("coordinator: resume the new master")
		}
	}
	return nil
}

// History returns the promotions, oldest first.
func (c *Coordinator) History(ctx context.Context) ([]Promotion, error) {
	entries, err := c.st.List(ctx, "history/")
	if err != nil {
		return nil, err
	}
	out := make([]Promotion, 0, len(entries))
	for _, e := range entries {
		var p Promotion
		if _, err := state.GetJSON(ctx, c.st, e.Key, &p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}
