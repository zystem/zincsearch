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
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/coordinator/buffer"
	"github.com/zincsearch/zincsearch/pkg/streaming/wire"
)

const testStream = "LOGS"

// natsServer is an embedded JetStream server that can be stopped and started
// again on the same port with the same data, like a restarting NATS node.
type natsServer struct {
	t    *testing.T
	port int
	dir  string
	srv  *server.Server
}

func newNATSServer(t *testing.T) *natsServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	n := &natsServer{t: t, port: port, dir: t.TempDir()}
	n.start()
	t.Cleanup(n.stop)
	return n
}

func (n *natsServer) start() {
	n.t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: n.port, JetStream: true, StoreDir: n.dir, NoLog: true, NoSigs: true})
	require.NoError(n.t, err)
	go srv.Start()
	require.True(n.t, srv.ReadyForConnections(15*time.Second))
	n.srv = srv
}

func (n *natsServer) stop() {
	if n.srv != nil {
		n.srv.Shutdown()
		n.srv.WaitForShutdown()
		n.srv = nil
	}
}

func (n *natsServer) url() string { return fmt.Sprintf("nats://127.0.0.1:%d", n.port) }

// jetstreamOn connects a plain client and makes sure the test stream exists.
func (n *natsServer) createStream() {
	n.t.Helper()
	nc, err := nats.Connect(n.url())
	require.NoError(n.t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(n.t, err)
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name: testStream, Subjects: []string{"logs.>"}, Storage: jetstream.FileStorage, Duplicates: time.Hour,
	})
	require.NoError(n.t, err)
}

// readAll returns every message of the stream, in stream order.
func (n *natsServer) readAll() []*wire.Message {
	n.t.Helper()
	nc, err := nats.Connect(n.url())
	require.NoError(n.t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(n.t, err)
	stream, err := js.Stream(context.Background(), testStream)
	require.NoError(n.t, err)
	info, err := stream.Info(context.Background())
	require.NoError(n.t, err)
	if info.State.Msgs == 0 {
		return nil
	}
	cons, err := stream.OrderedConsumer(context.Background(), jetstream.OrderedConsumerConfig{})
	require.NoError(n.t, err)
	batch, err := cons.Fetch(int(info.State.Msgs), jetstream.FetchMaxWait(5*time.Second))
	require.NoError(n.t, err)
	var out []*wire.Message
	for msg := range batch.Messages() {
		m, err := wire.Decode(msg.Data())
		require.NoError(n.t, err)
		out = append(out, m)
	}
	require.NoError(n.t, batch.Error())
	return out
}

func testPublisher(t *testing.T, n *natsServer, maxBuffer uint64) (*Publisher, *buffer.Buffer) {
	t.Helper()
	buf, err := buffer.Open(filepath.Join(t.TempDir(), "buffer.db"), maxBuffer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = buf.Close() })
	nc, err := ConnectNATS(n.url(), "test-publisher")
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	p, err := NewPublisher(nc, PublisherConfig{Subject: "logs.ci", Timeout: 300 * time.Millisecond, Retry: 50 * time.Millisecond}, buf)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p, buf
}

func line(i int) *wire.Message {
	return wire.NewDoc("logs", fmt.Sprintf("d%d", i), map[string]interface{}{"n": i})
}

func lines(msgs []*wire.Message) string {
	var ids []string
	for _, m := range msgs {
		switch m.Kind {
		case wire.KindDoc:
			ids = append(ids, m.ID)
		case wire.KindDocs:
			ids = append(ids, fmt.Sprintf("batch%d", len(m.Docs)))
		default:
			ids = append(ids, string(m.Op))
		}
	}
	return strings.Join(ids, ",")
}

func TestPublisherSendsInOrderWhenTheStreamIsUp(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	p, buf := testPublisher(t, n, 1<<20)
	ctx := context.Background()

	require.NoError(t, p.Publish(ctx, line(1)))
	admin, err := wire.NewAdmin("logs", wire.OpSetMapping, map[string]interface{}{"properties": map[string]interface{}{}})
	require.NoError(t, err)
	require.NoError(t, p.Publish(ctx, admin))
	require.NoError(t, p.PublishDocs(ctx, "logs", []wire.Doc{{Doc: map[string]interface{}{"n": 3}}, {Doc: map[string]interface{}{"n": 4}}}))
	require.NoError(t, p.Publish(ctx, line(5)))

	assert.Equal(t, "d1,set_mapping,batch2,d5", lines(n.readAll()))
	assert.Zero(t, buf.Stats().Count, "nothing needed the buffer")
	h := p.Health()
	assert.True(t, h.Connected)
	assert.False(t, h.Failing)
	assert.Equal(t, uint64(4), h.Published)
}

func TestPublisherParksMessagesDuringAnOutageAndKeepsTheOrder(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	p, buf := testPublisher(t, n, 1<<20)
	ctx := context.Background()

	require.NoError(t, p.Publish(ctx, line(1)))
	n.stop()

	// The outage does not fail the producers.
	require.NoError(t, p.Publish(ctx, line(2)))
	require.NoError(t, p.Publish(ctx, line(3)))
	assert.Equal(t, uint64(2), buf.Stats().Count)
	h := p.Health()
	assert.True(t, h.Failing)
	assert.NotEmpty(t, h.LastError)

	n.start()
	// A message that arrives while the buffer still holds older ones queues up
	// behind them instead of overtaking.
	require.NoError(t, p.Publish(ctx, line(4)))
	require.Eventually(t, func() bool { return buf.Stats().Count == 0 }, 20*time.Second, 20*time.Millisecond)
	require.NoError(t, p.Publish(ctx, line(5)))

	assert.Equal(t, "d1,d2,d3,d4,d5", lines(n.readAll()), "every message exactly once, in order")
	assert.False(t, p.Health().Failing)
}

func TestPublisherPushesBackWhenTheBufferIsFull(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	p, buf := testPublisher(t, n, 2000)
	ctx := context.Background()
	n.stop()

	accepted := 0
	var err error
	for i := 1; i <= 100 && err == nil; i++ {
		if err = p.Publish(ctx, line(i)); err == nil {
			accepted++
		}
	}
	require.ErrorIs(t, err, ErrBackpressure)
	require.Greater(t, accepted, 3)
	assert.Equal(t, uint64(accepted), buf.Stats().Count, "a refused message is not stored")
	assert.True(t, p.Health().Congested)

	// The stream returns: everything that was accepted arrives, and new
	// messages are accepted again.
	n.start()
	require.Eventually(t, func() bool { return buf.Stats().Count == 0 }, 20*time.Second, 20*time.Millisecond)
	require.NoError(t, p.Publish(ctx, line(1000)))
	got := n.readAll()
	require.Len(t, got, accepted+1)
	assert.Equal(t, "d1", got[0].ID)
	assert.Equal(t, "d1000", got[len(got)-1].ID)
	assert.False(t, p.Health().Congested)
}

func TestPublisherRefusesAMessageThatCanNeverBeSent(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	p, buf := testPublisher(t, n, 1<<30)

	huge := wire.NewDoc("logs", "big", map[string]interface{}{"blob": strings.Repeat("x", 2<<20)})
	err := p.Publish(context.Background(), huge)
	require.ErrorIs(t, err, ErrMessageTooLarge)
	assert.Zero(t, buf.Stats().Count)
}

func TestPublishDocsGivesDocumentsStableIDs(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	p, _ := testPublisher(t, n, 1<<20)

	require.NoError(t, p.PublishDocs(context.Background(), "logs", []wire.Doc{
		{Doc: map[string]interface{}{"n": 1}},
		{ID: "mine", Doc: map[string]interface{}{"n": 2}},
		{Doc: map[string]interface{}{"n": 3}},
	}))
	msgs := n.readAll()
	require.Len(t, msgs, 1)
	ids := []string{msgs[0].Docs[0].ID, msgs[0].Docs[1].ID, msgs[0].Docs[2].ID}
	assert.Equal(t, "mine", ids[1])
	assert.NotEmpty(t, ids[0])
	assert.True(t, strings.HasSuffix(ids[0], "-0"), ids[0])
	assert.True(t, strings.HasSuffix(ids[2], "-2"), ids[2])
	assert.Equal(t, strings.TrimSuffix(ids[0], "-0"), strings.TrimSuffix(ids[2], "-2"), "derived from one message ID")
}

// A message that arrives while older ones wait in the buffer must go behind
// them, even when the stream is reachable right now.
func TestPublisherNeverOvertakesTheBuffer(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	buf, err := buffer.Open(filepath.Join(t.TempDir(), "buffer.db"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = buf.Close() })
	nc, err := ConnectNATS(n.url(), "test-publisher")
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	// The timer that drains the buffer never fires during the test; only a
	// Publish wakes the drain.
	p, err := NewPublisher(nc, PublisherConfig{Subject: "logs.ci", Retry: time.Hour}, buf)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	ctx := context.Background()

	require.NoError(t, p.Publish(ctx, line(1)))
	for i := 2; i <= 3; i++ {
		data, err := line(i).Encode()
		require.NoError(t, err)
		require.NoError(t, buf.Append(buffer.Entry{ID: fmt.Sprintf("left-over-%d", i), Data: data}))
	}
	require.NoError(t, p.Publish(ctx, line(4)))

	require.Eventually(t, func() bool { return buf.Stats().Count == 0 }, 20*time.Second, 20*time.Millisecond)
	assert.Equal(t, "d1,d2,d3,d4", lines(n.readAll()))
}

// While NATS is known to be down a producer must not wait for an
// acknowledgement that cannot come: the message is parked at once.
func TestPublisherParksAtOnceWhileTheConnectionIsDown(t *testing.T) {
	n := newNATSServer(t)
	n.createStream()
	buf, err := buffer.Open(filepath.Join(t.TempDir(), "buffer.db"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { _ = buf.Close() })
	nc, err := ConnectNATS(n.url(), "test-publisher")
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	p, err := NewPublisher(nc, PublisherConfig{Subject: "logs.ci", Timeout: 30 * time.Second, Retry: time.Hour}, buf)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	require.NoError(t, p.Publish(context.Background(), line(1)))

	n.stop()
	require.Eventually(t, func() bool { return !nc.IsConnected() }, 10*time.Second, 10*time.Millisecond)

	started := time.Now()
	require.NoError(t, p.Publish(context.Background(), line(2)))
	assert.Less(t, time.Since(started), time.Second, "the 30s acknowledgement timeout is not waited for")
	assert.Equal(t, uint64(1), buf.Stats().Count)
}
