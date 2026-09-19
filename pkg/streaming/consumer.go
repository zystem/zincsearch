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

package streaming

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
)

var (
	// ErrGap means the stream no longer holds messages this node still needs.
	// The node must be restored from a backup before it can consume.
	ErrGap = errors.New("stream does not reach back to the node's offset")
	// ErrAhead means the node claims to have applied messages the stream does
	// not have, e.g. because the stream was recreated.
	ErrAhead = errors.New("node offset is ahead of the stream")
)

// Config configures a Consumer.
type Config struct {
	// URL of the NATS server(s), comma separated.
	URL string
	// Stream is the JetStream stream to consume.
	Stream string
	// Subject optionally restricts the consumed subject of the stream.
	Subject string
	// Consumer names the JetStream consumer of this node. It must be unique per node.
	Consumer string
	// Batch is the maximum number of messages applied between two flushes.
	Batch int
	// FetchWait is how long one fetch waits for messages.
	FetchWait time.Duration
	// Retry is the delay before retrying after an error.
	Retry time.Duration
	// AllowGap lets the node continue at the first available message when the
	// stream was already trimmed past its offset. Data in between is lost.
	AllowGap bool
	// Options are extra NATS connection options, e.g. credentials.
	Options []nats.Option
}

func (c *Config) prepare() error {
	if c.URL == "" || c.Stream == "" || c.Consumer == "" {
		return errors.New("streaming: URL, Stream and Consumer are required")
	}
	if c.Batch <= 0 {
		c.Batch = 256
	}
	if c.FetchWait <= 0 {
		c.FetchWait = time.Second
	}
	if c.Retry <= 0 {
		c.Retry = time.Second
	}
	return nil
}

// Status is a snapshot of a Consumer for health checks and coordinators.
type Status struct {
	Connected bool   `json:"connected"`
	Paused    bool   `json:"paused"`
	Drained   bool   `json:"drained"`
	Stream    string `json:"stream"`
	Consumer  string `json:"consumer"`
	// LastApplied is the sequence of the last message applied and durable here.
	LastApplied uint64 `json:"last_applied"`
	// StreamLast is the last sequence known to exist in the stream.
	StreamLast uint64 `json:"stream_last"`
	// Lag is how many messages the node is behind.
	Lag           uint64 `json:"lag"`
	AppliedTotal  uint64 `json:"applied_total"`
	SkippedTotal  uint64 `json:"skipped_total"`
	LastError     string `json:"last_error,omitempty"`
	LastErrorTime string `json:"last_error_time,omitempty"`
}

// Consumer applies a JetStream stream to this node, strictly in stream order.
//
// The node's own offset (see OffsetStore) decides where consuming resumes; the
// JetStream consumer is only a cursor that is recreated at that offset whenever
// anything goes wrong. Per batch the order is: apply, flush to disk, save the
// offset, acknowledge. A crash anywhere in between only causes messages to be
// applied again, which the Applier tolerates.
type Consumer struct {
	cfg   Config
	app   Applier
	store OffsetStore

	applied    atomic.Uint64
	streamLast atomic.Uint64
	total      atomic.Uint64
	skipped    atomic.Uint64

	mu      sync.Mutex
	wake    chan struct{} // closed and replaced on every state change
	paused  bool
	parked  bool
	running bool
	lastErr string
	lastAt  time.Time

	nc     *nats.Conn
	cancel context.CancelFunc
	done   chan struct{}
}

// New creates a Consumer. Nothing is contacted until Start.
func New(cfg Config, app Applier, store OffsetStore) (*Consumer, error) {
	if err := cfg.prepare(); err != nil {
		return nil, err
	}
	return &Consumer{cfg: cfg, app: app, store: store, wake: make(chan struct{})}, nil
}

// Start connects to NATS in the background and starts consuming. It does not
// wait for the server to be reachable: a node keeps serving searches while the
// stream is unavailable and catches up once it returns.
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return errors.New("streaming: consumer already started")
	}
	offset, err := c.store.Load()
	if err != nil {
		return fmt.Errorf("load stream offset: %w", err)
	}
	c.applied.Store(offset)

	opts := append([]nats.Option{
		nats.Name("zincsearch-" + c.cfg.Consumer),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}, c.cfg.Options...)
	nc, err := nats.Connect(c.cfg.URL, opts...)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("jetstream: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.nc, c.cancel, c.done, c.running = nc, cancel, make(chan struct{}), true
	go c.run(ctx, js)
	return nil
}

// Stop stops consuming after the current batch is durable and disconnects.
func (c *Consumer) Stop() {
	c.mu.Lock()
	cancel, done, nc := c.cancel, c.done, c.nc
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
	nc.Close()
}

// Pause stops taking new messages and returns once the batch in flight is
// applied, flushed, saved and acknowledged. While paused the node's data does
// not change, which makes it safe to back up. Pause on a stopped consumer
// returns immediately.
func (c *Consumer) Pause(ctx context.Context) error {
	c.mu.Lock()
	c.paused = true
	c.signalLocked()
	for c.running && !c.parked {
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.mu.Lock()
	}
	c.mu.Unlock()
	return nil
}

// Resume continues consuming after Pause.
func (c *Consumer) Resume() {
	c.mu.Lock()
	c.paused = false
	c.signalLocked()
	c.mu.Unlock()
}

// Status returns the current state.
func (c *Consumer) Status() Status {
	c.mu.Lock()
	s := Status{
		Paused:    c.paused,
		Drained:   c.paused && c.parked,
		Stream:    c.cfg.Stream,
		Consumer:  c.cfg.Consumer,
		LastError: c.lastErr,
	}
	if !c.lastAt.IsZero() {
		s.LastErrorTime = c.lastAt.UTC().Format(time.RFC3339)
	}
	nc := c.nc
	c.mu.Unlock()

	s.Connected = nc != nil && nc.IsConnected()
	s.LastApplied = c.applied.Load()
	s.StreamLast = c.streamLast.Load()
	if s.StreamLast > s.LastApplied {
		s.Lag = s.StreamLast - s.LastApplied
	}
	s.AppliedTotal = c.total.Load()
	s.SkippedTotal = c.skipped.Load()
	return s
}

func (c *Consumer) signalLocked() {
	close(c.wake)
	c.wake = make(chan struct{})
}

func (c *Consumer) fail(err error) {
	log.Error().Err(err).Str("stream", c.cfg.Stream).Str("consumer", c.cfg.Consumer).Msg("stream consumer")
	c.mu.Lock()
	c.lastErr, c.lastAt = err.Error(), time.Now()
	c.mu.Unlock()
}

func (c *Consumer) run(ctx context.Context, js jetstream.JetStream) {
	defer func() {
		c.mu.Lock()
		c.running, c.parked = false, false
		c.signalLocked()
		c.mu.Unlock()
		close(c.done)
	}()

	var stream jetstream.Stream
	var cons jetstream.Consumer
	lastLag := time.Time{}
	for ctx.Err() == nil {
		if !c.parkIfPaused(ctx) {
			return
		}
		if cons == nil {
			var err error
			stream, cons, err = c.prepare(ctx, js)
			if err != nil {
				c.fail(err)
				sleep(ctx, c.cfg.Retry)
				continue
			}
		}
		n, err := c.step(ctx, cons)
		if err != nil {
			// Recreate the cursor at the saved offset; whatever was not saved is
			// delivered and applied again.
			cons = nil
			if ctx.Err() == nil {
				c.fail(err)
				sleep(ctx, c.cfg.Retry)
			}
			continue
		}
		if n == 0 || time.Since(lastLag) > time.Second {
			if info, err := stream.Info(ctx); err == nil {
				c.streamLast.Store(info.State.LastSeq)
			}
			lastLag = time.Now()
		}
	}
}

// parkIfPaused blocks while the consumer is paused. It returns false when the
// consumer was stopped meanwhile.
func (c *Consumer) parkIfPaused(ctx context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.paused {
		if !c.parked {
			c.parked = true
			c.signalLocked()
		}
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			c.mu.Lock()
			return false
		}
		c.mu.Lock()
	}
	c.parked = false
	return ctx.Err() == nil
}

// prepare (re)creates the JetStream cursor at the node's saved offset.
func (c *Consumer) prepare(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, jetstream.Consumer, error) {
	stream, err := js.Stream(ctx, c.cfg.Stream)
	if err != nil {
		return nil, nil, fmt.Errorf("stream %s: %w", c.cfg.Stream, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("stream %s info: %w", c.cfg.Stream, err)
	}
	c.streamLast.Store(info.State.LastSeq)

	need := c.applied.Load() + 1
	first := info.State.FirstSeq
	if info.State.Msgs == 0 {
		first = info.State.LastSeq + 1
	}
	if need < first {
		if !c.cfg.AllowGap {
			return nil, nil, fmt.Errorf("%w: node needs message %d, stream starts at %d", ErrGap, need, first)
		}
		log.Warn().Uint64("needed", need).Uint64("first", first).Msg("stream gap accepted, messages are lost")
		need = first
	}
	if need > info.State.LastSeq+1 {
		return nil, nil, fmt.Errorf("%w: node needs message %d, stream ends at %d", ErrAhead, need, info.State.LastSeq)
	}

	// Delivery position cannot be changed on an existing consumer.
	if err := stream.DeleteConsumer(ctx, c.cfg.Consumer); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, nil, fmt.Errorf("delete consumer %s: %w", c.cfg.Consumer, err)
	}
	cfg := jetstream.ConsumerConfig{
		Name:          c.cfg.Consumer,
		Durable:       c.cfg.Consumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       10 * time.Minute,
		MaxAckPending: 2 * c.cfg.Batch,
		FilterSubject: c.cfg.Subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}
	if need > 1 {
		cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		cfg.OptStartSeq = need
	}
	cons, err := stream.CreateConsumer(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create consumer %s: %w", c.cfg.Consumer, err)
	}
	return stream, cons, nil
}

// step fetches one batch and makes it durable. It returns how many messages
// were delivered.
func (c *Consumer) step(ctx context.Context, cons jetstream.Consumer) (int, error) {
	batch, err := cons.Fetch(c.cfg.Batch, jetstream.FetchMaxWait(c.cfg.FetchWait))
	if err != nil {
		return 0, fmt.Errorf("fetch: %w", err)
	}
	var msgs []jetstream.Msg
	for m := range batch.Messages() {
		msgs = append(msgs, m)
	}
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
		return len(msgs), fmt.Errorf("fetch: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	applied := c.applied.Load()
	last := applied
	var applyErr error
	for _, m := range msgs {
		md, err := m.Metadata()
		if err != nil {
			applyErr = fmt.Errorf("message metadata: %w", err)
			break
		}
		seq := md.Sequence.Stream
		if seq <= last {
			continue // delivered again after it was applied
		}
		if err := c.applyOne(ctx, seq, m.Data()); err != nil {
			applyErr = err
			break
		}
		last = seq
	}

	// Make everything applied so far durable before moving the offset, even when
	// the batch stopped early.
	if last > applied {
		if err := c.app.Flush(); err != nil {
			return len(msgs), fmt.Errorf("flush: %w", err)
		}
		if err := c.store.Save(last); err != nil {
			return len(msgs), fmt.Errorf("save offset: %w", err)
		}
		c.applied.Store(last)
		for _, m := range msgs {
			if md, err := m.Metadata(); err == nil && md.Sequence.Stream <= last {
				_ = m.Ack()
			}
		}
	}
	return len(msgs), applyErr
}

// applyOne applies a message, retrying transient failures on the same message
// so that order is never broken. A message that can never succeed is skipped.
func (c *Consumer) applyOne(ctx context.Context, seq uint64, data []byte) error {
	msg, err := Decode(data)
	for {
		if err == nil {
			err = c.app.Apply(seq, msg)
		}
		if err == nil {
			c.total.Add(1)
			return nil
		}
		if IsPermanent(err) {
			c.skipped.Add(1)
			c.fail(fmt.Errorf("skipping message %d: %w", seq, err))
			return nil
		}
		c.fail(fmt.Errorf("apply message %d: %w", seq, err))
		if !sleep(ctx, c.cfg.Retry) {
			return ctx.Err()
		}
		err = nil
	}
}

// sleep waits for d and reports whether the context is still alive.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
