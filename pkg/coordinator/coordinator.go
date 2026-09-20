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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/coordinator/blob"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
)

// Config configures a Coordinator. Zero values are replaced by defaults.
type Config struct {
	// ID identifies this coordinator instance in the leader election.
	// Default: host name and process ID.
	ID string
	// Nodes are the ZincSearch nodes. The first one is master until the state says otherwise.
	Nodes []NodeConfig

	// PollInterval is how often nodes are asked. Default 2s.
	PollInterval time.Duration
	// PollTimeout bounds one question to a node. Default 3s.
	PollTimeout time.Duration
	// FailThreshold is the number of failed polls in a row after which a node is down. Default 3.
	FailThreshold int
	// MaxLag is how many stream messages a node may be behind and still count as up. Default 1000.
	MaxLag uint64
	// PromoteCooldown is the minimum time between two promotions. Default 1m.
	PromoteCooldown time.Duration
	// LeaseTTL is how long a leader stays leader without renewing. Default 15s.
	LeaseTTL time.Duration

	// BackupInterval is the time between backups. Default 1h; a negative value
	// switches scheduled backups off.
	BackupInterval time.Duration
	// BackupRetry is how long to wait after a failed backup before trying again. Default 5m.
	BackupRetry time.Duration
	// BackupRetention is how many backups are kept. Default 3.
	BackupRetention int
	// OrphanGrace is how old an object of the blob store that the state does not
	// know has to be before Prune deletes it (an upload whose record was never
	// written). Default 24h; a negative value only reports orphans.
	OrphanGrace time.Duration
	// DrainTimeout bounds waiting for a node to pause and drain before a backup. Default 10m.
	DrainTimeout time.Duration
	// VerifyInterval is the time between verifications of the stored backups.
	// Default 24h; a negative value switches them off.
	VerifyInterval time.Duration

	// Now is the clock. Default time.Now.
	Now func() time.Time
}

func (c *Config) defaults() error {
	if len(c.Nodes) == 0 {
		return errors.New("coordinator: no nodes configured")
	}
	seen := map[string]bool{}
	for _, n := range c.Nodes {
		if n.Name == "" || n.URL == "" {
			return errors.New("coordinator: every node needs a name and a URL")
		}
		if !state.ValidKey(n.Name) {
			return fmt.Errorf("coordinator: node name %q is not usable as a key", n.Name)
		}
		if seen[n.Name] {
			return fmt.Errorf("coordinator: node %q is configured twice", n.Name)
		}
		seen[n.Name] = true
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&c.PollInterval, 2*time.Second)
	def(&c.PollTimeout, 3*time.Second)
	def(&c.PromoteCooldown, time.Minute)
	def(&c.LeaseTTL, 15*time.Second)
	def(&c.BackupInterval, time.Hour)
	def(&c.BackupRetry, 5*time.Minute)
	def(&c.DrainTimeout, 10*time.Minute)
	def(&c.VerifyInterval, 24*time.Hour)
	if c.FailThreshold == 0 {
		c.FailThreshold = 3
	}
	if c.MaxLag == 0 {
		c.MaxLag = 1000
	}
	if c.BackupRetention == 0 {
		c.BackupRetention = 3
	}
	if c.OrphanGrace == 0 {
		c.OrphanGrace = 24 * time.Hour
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.ID == "" {
		host, _ := os.Hostname()
		c.ID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return nil
}

// Deps are what a Coordinator works with. Publisher and Verifier are optional.
type Deps struct {
	State state.Store
	// Blobs is where backups are stored.
	Blobs blob.Store
	// Publisher is used for ingest and to judge whether new builds may start.
	Publisher *Publisher
	// Verifier proves that stored backups can be restored.
	Verifier Verifier
}

// Coordinator is the control plane of a replicated ZincSearch cluster.
type Coordinator struct {
	cfg      Config
	st       state.Store
	blobs    blob.Store
	pub      *Publisher
	verifier Verifier

	nodes map[string]*Node
	order []string

	lease  *state.Lease
	leader atomic.Bool

	mu      sync.RWMutex
	status  map[string]NodeStatus
	cluster Cluster

	jobMu sync.Mutex // one backup or verification at a time
}

// New creates a coordinator. Call Run to start it.
func New(cfg Config, deps Deps) (*Coordinator, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	if deps.State == nil {
		return nil, errors.New("coordinator: a state store is required")
	}
	c := &Coordinator{
		cfg: cfg, st: deps.State, blobs: deps.Blobs, pub: deps.Publisher, verifier: deps.Verifier,
		nodes:  make(map[string]*Node),
		status: make(map[string]NodeStatus),
	}
	for _, n := range cfg.Nodes {
		c.nodes[n.Name] = NewNode(n)
		c.order = append(c.order, n.Name)
		c.status[n.Name] = NodeStatus{Name: n.Name, URL: n.URL, State: NodeUnknown}
	}
	c.lease = state.NewLease(deps.State, "leader", cfg.ID, cfg.LeaseTTL, cfg.Now)
	return c, nil
}

// IsLeader reports whether this instance runs failover and backups.
func (c *Coordinator) IsLeader() bool { return c.leader.Load() }

// Run runs the coordinator until ctx ends: it watches the nodes, takes part in
// the leader election and, as the leader, promotes replicas and runs the
// scheduled jobs. It gives up the lease when it stops.
func (c *Coordinator) Run(ctx context.Context) error {
	poll := time.NewTicker(c.cfg.PollInterval)
	defer poll.Stop()
	lease := time.NewTicker(c.cfg.LeaseTTL / 3)
	defer lease.Stop()

	c.tryLease(ctx)
	c.PollOnce(ctx)
	c.refreshCluster(ctx)
	for {
		select {
		case <-ctx.Done():
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.lease.Release(releaseCtx)
			cancel()
			c.leader.Store(false)
			return nil
		case <-lease.C:
			c.tryLease(ctx)
		case <-poll.C:
			c.PollOnce(ctx)
			c.refreshCluster(ctx)
			if c.IsLeader() {
				if _, err := c.FailoverOnce(ctx); err != nil {
					log.Error().Err(err).Msg("coordinator: failover check")
				}
				c.startDueJobs(ctx)
			}
		}
	}
}

func (c *Coordinator) tryLease(ctx context.Context) {
	ok, err := c.lease.Acquire(ctx)
	if err != nil {
		// Not knowing whether the lease is held is treated as not holding it.
		log.Error().Err(err).Msg("coordinator: leader election")
		ok = false
	}
	if was := c.leader.Swap(ok); was != ok {
		log.Info().Bool("leader", ok).Str("id", c.cfg.ID).Msg("coordinator: leadership changed")
	}
}

// Admission says whether the CI system may start new builds.
type Admission struct {
	Allowed bool     `json:"allowed"`
	Reasons []string `json:"reasons,omitempty"`
}

// Admit tells whether new builds may start: logs must have somewhere to go,
// and there must be a node that can serve them. Builds that already run are not
// affected; their logs are buffered or flushed as the situation allows.
func (c *Coordinator) Admit() Admission {
	var reasons []string
	if c.pub != nil {
		h := c.pub.Health()
		switch {
		case !h.Connected:
			reasons = append(reasons, "the replication stream is not connected")
		case h.Failing:
			reasons = append(reasons, "publishing to the replication stream fails")
		case h.Congested:
			reasons = append(reasons, "the local log buffer is filling up")
		}
	}
	usable := false
	for _, s := range c.NodeStatuses() {
		switch s.State {
		case NodeUp, NodeLagging, NodeDegraded, NodeSuspect:
			usable = true
		}
	}
	if !usable {
		reasons = append(reasons, "no ZincSearch node is available")
	}
	return Admission{Allowed: len(reasons) == 0, Reasons: reasons}
}
