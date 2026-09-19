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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/core"
)

const testStream = "LOGS"

type memStore struct {
	mu  sync.Mutex
	seq uint64
}

func (m *memStore) Load() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.seq, nil
}

func (m *memStore) Save(seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq = seq
	return nil
}

// env is an embedded JetStream server with a client connection.
type env struct {
	t   *testing.T
	url string
	js  jetstream.JetStream
}

func startNATS(t *testing.T) *env {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(15*time.Second), "nats did not start")
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	nc, err := nats.Connect(ns.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return &env{t: t, url: ns.ClientURL(), js: js}
}

func (e *env) createStream(cfg jetstream.StreamConfig) {
	e.t.Helper()
	cfg.Name = testStream
	cfg.Subjects = []string{"logs.>"}
	cfg.Storage = jetstream.MemoryStorage
	_, err := e.js.CreateStream(context.Background(), cfg)
	require.NoError(e.t, err)
}

func (e *env) publishRaw(data []byte) {
	e.t.Helper()
	_, err := e.js.Publish(context.Background(), "logs.ci", data)
	require.NoError(e.t, err)
}

func (e *env) publish(m *Message) {
	e.t.Helper()
	raw, err := m.Encode()
	require.NoError(e.t, err)
	e.publishRaw(raw)
}

func (e *env) publishDocs(index string, from, to int) {
	e.t.Helper()
	for i := from; i <= to; i++ {
		e.publish(NewDoc(index, fmt.Sprintf("doc-%d", i), map[string]interface{}{"line": fmt.Sprintf("line %d", i)}))
	}
}

func (e *env) consumer(store OffsetStore, mutate ...func(*Config)) *Consumer {
	e.t.Helper()
	cfg := Config{
		URL:       e.url,
		Stream:    testStream,
		Consumer:  "node-a",
		Batch:     16,
		FetchWait: 100 * time.Millisecond,
		Retry:     50 * time.Millisecond,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	c, err := New(cfg, NewCoreApplier(), store)
	require.NoError(e.t, err)
	return c
}

func start(t *testing.T, c *Consumer) {
	t.Helper()
	require.NoError(t, c.Start())
	t.Cleanup(c.Stop)
}

func waitApplied(t *testing.T, c *Consumer, seq uint64) {
	t.Helper()
	assert.Eventually(t, func() bool { return c.Status().LastApplied >= seq }, 60*time.Second, 20*time.Millisecond,
		"consumer should reach sequence %d, status %+v", seq, c.Status())
}

// waitWALDrained waits until every accepted document of the index is searchable.
func waitWALDrained(t *testing.T, name string) {
	t.Helper()
	assert.Eventually(t, func() bool {
		idx, ok := core.GetIndex(name)
		if !ok {
			return false
		}
		n, err := idx.WALPending()
		return err == nil && n == 0
	}, 60*time.Second, 50*time.Millisecond)
}

func TestConsumer_AppliesDocumentsAndAdminOpsInOrder(t *testing.T) {
	const name = "streaming_consumer_order"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})

	// The mapping is created first and a field is added between documents; every
	// node must see the same order.
	create, err := NewAdmin(name, OpCreateIndex, map[string]interface{}{
		"mappings": map[string]interface{}{"properties": map[string]interface{}{"line": map[string]interface{}{"type": "text"}}},
	})
	require.NoError(t, err)
	e.publish(create)
	e.publishDocs(name, 1, 5)
	addField, err := NewAdmin(name, OpSetMapping, map[string]interface{}{
		"properties": map[string]interface{}{"level": map[string]interface{}{"type": "keyword"}},
	})
	require.NoError(t, err)
	e.publish(addField)
	e.publishDocs(name, 6, 10)

	c := e.consumer(&memStore{})
	start(t, c)
	waitApplied(t, c, 12)

	waitDocs(t, name, 10)
	idx, ok := core.GetIndex(name)
	require.True(t, ok)
	_, ok = idx.GetMappings().GetProperty("level")
	assert.True(t, ok, "the mapping added in the middle of the stream is applied")

	st := c.Status()
	assert.True(t, st.Connected)
	assert.Equal(t, uint64(12), st.LastApplied)
	assert.Equal(t, uint64(12), st.AppliedTotal)
	assert.Zero(t, st.Lag)
}

func TestConsumer_PauseHoldsTheNodeStill(t *testing.T) {
	const name = "streaming_consumer_pause"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})
	e.publishDocs(name, 1, 3)

	c := e.consumer(&memStore{})
	start(t, c)
	waitApplied(t, c, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.Pause(ctx))
	st := c.Status()
	assert.True(t, st.Paused)
	assert.True(t, st.Drained, "pause returns only once the batch in flight is finished")

	// Messages published while paused are not consumed.
	e.publishDocs(name, 4, 6)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, uint64(3), c.Status().LastApplied)

	// Once the WAL is drained nothing changes on disk until Resume.
	waitWALDrained(t, name)
	waitDocs(t, name, 3)

	c.Resume()
	waitApplied(t, c, 6)
	waitDocs(t, name, 6)
	assert.False(t, c.Status().Paused)
}

func TestConsumer_ResumesFromTheSavedOffset(t *testing.T) {
	const name = "streaming_consumer_resume"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})
	store := &memStore{}

	e.publishDocs(name, 1, 5)
	first := e.consumer(store)
	require.NoError(t, first.Start())
	waitApplied(t, first, 5)
	first.Stop()
	saved, _ := store.Load()
	assert.Equal(t, uint64(5), saved)

	// A restarted node continues after its offset instead of starting over.
	e.publishDocs(name, 6, 10)
	second := e.consumer(store)
	start(t, second)
	waitApplied(t, second, 10)
	assert.Equal(t, uint64(5), second.Status().AppliedTotal, "only the new messages are applied")
	waitDocs(t, name, 10)
}

func TestConsumer_RedeliveryAfterALostOffsetDoesNotDuplicate(t *testing.T) {
	const name = "streaming_consumer_redelivery"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})
	store := &memStore{}

	e.publishDocs(name, 1, 5)
	first := e.consumer(store)
	require.NoError(t, first.Start())
	waitApplied(t, first, 5)
	first.Stop()
	waitWALDrained(t, name)
	waitDocs(t, name, 5)

	// The crash happened after the documents were written but before the offset
	// was saved, so all five are delivered and applied a second time.
	require.NoError(t, store.Save(0))
	second := e.consumer(store)
	start(t, second)
	waitApplied(t, second, 5)
	assert.Equal(t, uint64(5), second.Status().AppliedTotal)

	waitWALDrained(t, name)
	waitDocs(t, name, 5)
}

func TestConsumer_SkipsAMessageThatCanNeverBeApplied(t *testing.T) {
	const name = "streaming_consumer_poison"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})

	e.publishDocs(name, 1, 2)
	e.publishRaw([]byte("this is not a message"))
	e.publish(&Message{Kind: KindAdmin, Index: name, Op: Op("explode")})
	e.publishDocs(name, 3, 4)

	c := e.consumer(&memStore{})
	start(t, c)
	waitApplied(t, c, 6)
	waitDocs(t, name, 4)

	st := c.Status()
	assert.Equal(t, uint64(2), st.SkippedTotal)
	assert.Equal(t, uint64(4), st.AppliedTotal)
	assert.Contains(t, st.LastError, "skipping message")
}

func TestConsumer_RefusesToStartAcrossAGap(t *testing.T) {
	const name = "streaming_consumer_gap"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	// The stream only keeps the newest three messages.
	e.createStream(jetstream.StreamConfig{MaxMsgs: 3})
	e.publishDocs(name, 1, 6)

	t.Run("fails loudly instead of losing data", func(t *testing.T) {
		c := e.consumer(&memStore{})
		start(t, c)
		assert.Eventually(t, func() bool { return c.Status().LastError != "" }, 10*time.Second, 20*time.Millisecond)
		st := c.Status()
		assert.Contains(t, st.LastError, ErrGap.Error())
		assert.Zero(t, st.LastApplied)
		assert.Zero(t, st.AppliedTotal)
	})

	t.Run("continues past the gap only when told to", func(t *testing.T) {
		c := e.consumer(&memStore{}, func(cfg *Config) { cfg.AllowGap = true; cfg.Consumer = "node-b" })
		start(t, c)
		waitApplied(t, c, 6)
		assert.Equal(t, uint64(3), c.Status().AppliedTotal)
		waitDocs(t, name, 3)
	})
}

func TestConsumer_RefusesAnOffsetAheadOfTheStream(t *testing.T) {
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})
	e.publishDocs("streaming_consumer_ahead", 1, 3)

	c := e.consumer(&memStore{seq: 100})
	start(t, c)
	assert.Eventually(t, func() bool { return c.Status().LastError != "" }, 10*time.Second, 20*time.Millisecond)
	assert.Contains(t, c.Status().LastError, ErrAhead.Error())
}

func TestConsumer_WaitsForTheStreamToExist(t *testing.T) {
	const name = "streaming_consumer_late"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)

	c := e.consumer(&memStore{})
	start(t, c)
	assert.Eventually(t, func() bool { return c.Status().LastError != "" }, 10*time.Second, 20*time.Millisecond)
	assert.Zero(t, c.Status().LastApplied)

	e.createStream(jetstream.StreamConfig{})
	e.publishDocs(name, 1, 3)
	waitApplied(t, c, 3)
	waitDocs(t, name, 3)
}

func TestConsumer_PauseAndDrainMakesTheIndexMatchTheOffset(t *testing.T) {
	const name = "streaming_consumer_drain"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{})
	e.publishDocs(name, 1, 200)

	c := e.consumer(&memStore{}, func(cfg *Config) { cfg.Batch = 8 })
	start(t, c)

	// Pause while the consumer is still in the middle of the stream.
	require.Eventually(t, func() bool { return c.Status().LastApplied > 0 }, 20*time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, c.PauseAndDrain(ctx))

	// The moment it returns, the node's data is exactly the messages up to its
	// offset: nothing is waiting in the WAL and nothing more will arrive.
	pending, err := WALPending()
	require.NoError(t, err)
	assert.Zero(t, pending)
	offset := c.Status().LastApplied
	require.Greater(t, offset, uint64(0))
	assert.Equal(t, int(offset), countDocs(t, name), "one document per applied message")

	e.publishDocs(name, 201, 210)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, offset, c.Status().LastApplied)
	assert.Equal(t, int(offset), countDocs(t, name))

	c.Resume()
	waitApplied(t, c, 210)
	waitDocs(t, name, 210)
}

func TestConsumer_DetectsAGapCreatedWhileItWasPaused(t *testing.T) {
	const name = "streaming_consumer_pause_gap"
	t.Cleanup(func() { _ = core.DeleteIndex(name) })
	e := startNATS(t)
	e.createStream(jetstream.StreamConfig{MaxMsgs: 3})
	e.publishDocs(name, 1, 2)

	c := e.consumer(&memStore{})
	start(t, c)
	waitApplied(t, c, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.Pause(ctx))

	// Retention drops messages 3..5 that the node has not read.
	e.publishDocs(name, 3, 8)
	c.Resume()

	assert.Eventually(t, func() bool { return strings.Contains(c.Status().LastError, ErrGap.Error()) },
		10*time.Second, 20*time.Millisecond, "status %+v", c.Status())
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, uint64(2), c.Status().LastApplied, "the node must not skip over the lost messages")
}
