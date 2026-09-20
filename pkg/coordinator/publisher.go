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

// Package coordinator runs a replicated ZincSearch cluster: it publishes logs to
// the replication stream, watches the nodes, promotes a replica when the master
// fails, backs a replica up on a schedule, and tells the CI system whether it may
// start new builds.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/zincsearch/zincsearch/pkg/coordinator/buffer"
	"github.com/zincsearch/zincsearch/pkg/streaming/wire"
)

var errNotConnected = errors.New("not connected to NATS")

var (
	// ErrBackpressure means the stream is unreachable and the local buffer is
	// full: the message was not accepted and the caller has to slow down or retry.
	ErrBackpressure = errors.New("coordinator: the stream is unavailable and the local buffer is full")
	// ErrMessageTooLarge means the message cannot be sent in one piece.
	ErrMessageTooLarge = errors.New("coordinator: message is larger than the stream accepts")
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	// Subject the messages are published to. It has to be one of the subjects of
	// the stream the nodes consume.
	Subject string
	// Timeout bounds waiting for the acknowledgement of one message. Default 5s.
	Timeout time.Duration
	// Retry is the pause between attempts to drain the buffer. Default 1s.
	Retry time.Duration
	// CongestedAt is the fill level of the buffer, in percent, from which the
	// publisher reports itself congested. Default 50.
	CongestedAt uint64
}

// Publisher puts messages on the replication stream in the order it is given
// them, and never loses one it accepted.
//
// While the stream is reachable a message is acknowledged by JetStream before
// Publish returns. While it is not, messages are parked in a bounded buffer on
// disk and sent in order once the stream is back. Once the buffer holds anything,
// every new message goes through it, so order is kept. When the buffer is full,
// Publish refuses the message with ErrBackpressure.
type Publisher struct {
	js  jetstream.JetStream
	nc  *nats.Conn
	cfg PublisherConfig
	buf *buffer.Buffer

	id  string
	seq atomic.Uint64

	mu   sync.Mutex // decides between the direct path and the buffer
	wake chan struct{}

	failing   atomic.Bool
	published atomic.Uint64

	errMu   sync.Mutex
	lastErr string
	lastAt  time.Time

	cancel context.CancelFunc
	done   chan struct{}
}

// NewPublisher starts a publisher on an existing connection. The connection
// should reconnect forever (nats.RetryOnFailedConnect, nats.MaxReconnects(-1)),
// see ConnectNATS. Close stops it; the buffer stays open and belongs to the caller.
func NewPublisher(nc *nats.Conn, cfg PublisherConfig, buf *buffer.Buffer) (*Publisher, error) {
	if cfg.Subject == "" {
		return nil, errors.New("coordinator: publisher needs a subject")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Retry <= 0 {
		cfg.Retry = time.Second
	}
	if cfg.CongestedAt == 0 || cfg.CongestedAt > 100 {
		cfg.CongestedAt = 50
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	idBytes := make([]byte, 5)
	_, _ = rand.Read(idBytes)

	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{
		js: js, nc: nc, cfg: cfg, buf: buf,
		id:     hex.EncodeToString(idBytes),
		wake:   make(chan struct{}, 1),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go p.flushLoop(ctx)
	return p, nil
}

// ConnectNATS connects with the options a long running publisher needs: it does
// not fail when the server is down at start, and it reconnects forever.
func ConnectNATS(url, name string, opts ...nats.Option) (*nats.Conn, error) {
	base := []nats.Option{
		nats.Name(name),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}
	return nats.Connect(url, append(base, opts...)...)
}

// Close stops draining the buffer. Messages still in it stay there and are sent
// by the next publisher that opens the same buffer.
func (p *Publisher) Close() {
	p.cancel()
	<-p.done
}

// Publish accepts one message. When it returns nil the message is either
// acknowledged by the stream or safe in the local buffer; on an error nothing of
// it was accepted.
func (p *Publisher) Publish(ctx context.Context, m *wire.Message) error {
	data, err := m.Encode()
	if err != nil {
		return err
	}
	if err := p.checkSize(data); err != nil {
		return err
	}
	return p.publish(ctx, p.nextID(), data)
}

// PublishDocs publishes documents of one index as one message. Documents that
// have no ID get one that is derived from the message, so a message that is sent
// twice, e.g. after a lost acknowledgement, replaces its documents instead of
// duplicating them.
func (p *Publisher) PublishDocs(ctx context.Context, index string, docs []wire.Doc) error {
	id := p.nextID()
	stamped := make([]wire.Doc, len(docs))
	for i, d := range docs {
		if d.ID == "" {
			d.ID = fmt.Sprintf("%s-%d", id, i)
		}
		stamped[i] = d
	}
	data, err := wire.NewDocs(index, stamped).Encode()
	if err != nil {
		return err
	}
	if err := p.checkSize(data); err != nil {
		return err
	}
	return p.publish(ctx, id, data)
}

// checkSize enforces the server's payload limit. Before the first connection
// the limit is unknown (zero); the message is then accepted into the buffer.
func (p *Publisher) checkSize(data []byte) error {
	limit := p.nc.MaxPayload()
	if limit > 0 && int64(len(data)) > limit {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrMessageTooLarge, len(data), limit)
	}
	return nil
}

func (p *Publisher) nextID() string { return fmt.Sprintf("%s-%d", p.id, p.seq.Add(1)) }

func (p *Publisher) publish(ctx context.Context, id string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Only when nothing waits in the buffer may a message overtake it. There is
	// no point in waiting for an acknowledgement over a connection that is known
	// to be down: the message is parked at once.
	if p.buf.Stats().Count == 0 {
		if !p.nc.IsConnected() {
			p.noteFailure(errNotConnected)
		} else if err := p.send(ctx, id, data); err == nil {
			p.published.Add(1)
			return nil
		} else {
			p.noteFailure(err)
		}
	}
	if err := p.buf.Append(buffer.Entry{ID: id, Data: data}); err != nil {
		if errors.Is(err, buffer.ErrFull) {
			return ErrBackpressure
		}
		return err
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return nil
}

func (p *Publisher) send(ctx context.Context, id string, data []byte) error {
	sendCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	_, err := p.js.Publish(sendCtx, p.cfg.Subject, data, jetstream.WithMsgID(id))
	if err == nil {
		p.failing.Store(false)
	}
	return err
}

func (p *Publisher) noteFailure(err error) {
	p.failing.Store(true)
	p.errMu.Lock()
	p.lastErr, p.lastAt = err.Error(), time.Now()
	p.errMu.Unlock()
}

// flushLoop drains the buffer into the stream, oldest first.
func (p *Publisher) flushLoop(ctx context.Context) {
	defer close(p.done)
	ticker := time.NewTicker(p.cfg.Retry)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-ticker.C:
		}
		p.drain(ctx)
	}
}

func (p *Publisher) drain(ctx context.Context) {
	for ctx.Err() == nil {
		items, err := p.buf.Peek(64)
		if err != nil {
			p.noteFailure(err)
			return
		}
		if len(items) == 0 {
			return
		}
		var last uint64
		for _, it := range items {
			if err := p.send(ctx, it.ID, it.Data); err != nil {
				p.noteFailure(err)
				if last > 0 {
					_ = p.buf.Remove(last)
				}
				return
			}
			p.published.Add(1)
			last = it.Seq
		}
		if err := p.buf.Remove(last); err != nil {
			p.noteFailure(err)
			return
		}
	}
}

// PublisherHealth is the state of a Publisher.
type PublisherHealth struct {
	// Connected tells whether the NATS connection is up.
	Connected bool `json:"connected"`
	// Failing is true from a failed send until the next successful one.
	Failing bool `json:"failing"`
	// Congested is true when the buffer is filled beyond the configured level.
	Congested     bool   `json:"congested"`
	Buffered      uint64 `json:"buffered"`
	BufferedBytes uint64 `json:"buffered_bytes"`
	BufferMax     uint64 `json:"buffer_max_bytes"`
	Published     uint64 `json:"published"`
	LastError     string `json:"last_error,omitempty"`
	LastErrorTime string `json:"last_error_time,omitempty"`
}

// Health reports the state of the publisher.
func (p *Publisher) Health() PublisherHealth {
	st := p.buf.Stats()
	h := PublisherHealth{
		Connected:     p.nc.IsConnected(),
		Failing:       p.failing.Load(),
		Congested:     st.Bytes*100 >= st.MaxBytes*p.cfg.CongestedAt,
		Buffered:      st.Count,
		BufferedBytes: st.Bytes,
		BufferMax:     st.MaxBytes,
		Published:     p.published.Load(),
	}
	p.errMu.Lock()
	if p.lastErr != "" && (h.Failing || h.Buffered > 0) {
		h.LastError = p.lastErr
		h.LastErrorTime = p.lastAt.UTC().Format(time.RFC3339)
	}
	p.errMu.Unlock()
	return h
}
