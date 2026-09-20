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

// Package statetest is the behaviour every state.Store has to show. Each
// implementation runs the same suite, so they stay interchangeable.
package statetest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
)

// Run runs the suite against stores made by newStore. The stores of different
// calls may share their storage: every test keeps to its own key prefix.
func Run(t *testing.T, newStore func(t *testing.T) state.Store) {
	suite := map[string]func(t *testing.T, s state.Store, p string){
		"get of a missing key":                  getMissing,
		"create and get":                        createAndGet,
		"create refuses an existing key":        createExisting,
		"update needs the current revision":     updateRevision,
		"update of a missing key":               updateMissing,
		"delete and create again":               deleteAndRecreate,
		"a revision is never reused":            revisionsNeverRepeat,
		"list by prefix":                        listPrefix,
		"binary values":                         binaryValues,
		"invalid keys":                          invalidKeys,
		"concurrent updates never lose a write": concurrentCounter,
		"only one concurrent create wins":       concurrentCreate,
		"lease has one holder":                  leaseOneHolder,
		"lease is taken over after it expired":  leaseExpiry,
		"lease can be released":                 leaseRelease,
		"json helpers":                          jsonHelpers,
	}
	for name, fn := range suite {
		t.Run(name, func(t *testing.T) {
			s := newStore(t)
			t.Cleanup(func() { _ = s.Close() })
			fn(t, s, prefix())
		})
	}
}

func prefix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "t" + hex.EncodeToString(b)
}

var ctx = context.Background()

func getMissing(t *testing.T, s state.Store, p string) {
	_, err := s.Get(ctx, p+"/missing")
	assert.ErrorIs(t, err, state.ErrNotFound)
}

func createAndGet(t *testing.T, s state.Store, p string) {
	rev, err := s.Create(ctx, p+"/a", []byte("one"))
	require.NoError(t, err)
	assert.Greater(t, rev, uint64(0))

	e, err := s.Get(ctx, p+"/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), e.Value)
	assert.Equal(t, rev, e.Revision)
	assert.Equal(t, p+"/a", e.Key)
}

func createExisting(t *testing.T, s state.Store, p string) {
	_, err := s.Create(ctx, p+"/a", []byte("one"))
	require.NoError(t, err)
	_, err = s.Create(ctx, p+"/a", []byte("two"))
	require.ErrorIs(t, err, state.ErrConflict)
	e, err := s.Get(ctx, p+"/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), e.Value, "the first value stays")
}

func updateRevision(t *testing.T, s state.Store, p string) {
	rev1, err := s.Create(ctx, p+"/a", []byte("one"))
	require.NoError(t, err)

	rev2, err := s.Update(ctx, p+"/a", []byte("two"), rev1)
	require.NoError(t, err)
	assert.NotEqual(t, rev1, rev2)
	e, err := s.Get(ctx, p+"/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("two"), e.Value)
	assert.Equal(t, rev2, e.Revision)

	// A writer that still has the old revision loses.
	_, err = s.Update(ctx, p+"/a", []byte("stale"), rev1)
	require.ErrorIs(t, err, state.ErrConflict)
	e, err = s.Get(ctx, p+"/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("two"), e.Value)
}

func updateMissing(t *testing.T, s state.Store, p string) {
	_, err := s.Update(ctx, p+"/nothing", []byte("x"), 1)
	assert.ErrorIs(t, err, state.ErrNotFound)
}

func deleteAndRecreate(t *testing.T, s state.Store, p string) {
	require.NoError(t, s.Delete(ctx, p+"/none"), "deleting a missing key is fine")

	_, err := s.Create(ctx, p+"/a", []byte("one"))
	require.NoError(t, err)
	require.NoError(t, s.Delete(ctx, p+"/a"))
	_, err = s.Get(ctx, p+"/a")
	require.ErrorIs(t, err, state.ErrNotFound)

	rev, err := s.Create(ctx, p+"/a", []byte("again"))
	require.NoError(t, err)
	e, err := s.Get(ctx, p+"/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("again"), e.Value)
	assert.Equal(t, rev, e.Revision)
}

// A writer that read a key before it was deleted and created again must not be
// able to overwrite the new value with its old revision.
func revisionsNeverRepeat(t *testing.T, s state.Store, p string) {
	rev1, err := s.Create(ctx, p+"/a", []byte("one"))
	require.NoError(t, err)
	require.NoError(t, s.Delete(ctx, p+"/a"))
	rev2, err := s.Create(ctx, p+"/a", []byte("two"))
	require.NoError(t, err)
	assert.NotEqual(t, rev1, rev2)

	_, err = s.Update(ctx, p+"/a", []byte("from the past"), rev1)
	require.ErrorIs(t, err, state.ErrConflict)
	e, err := s.Get(ctx, p+"/a")
	require.NoError(t, err)
	assert.Equal(t, []byte("two"), e.Value)
}

func listPrefix(t *testing.T, s state.Store, p string) {
	for _, k := range []string{"nodes/b", "nodes/a", "nodes/c", "backups/x", "nodesx"} {
		_, err := s.Create(ctx, p+"/"+k, []byte(k))
		require.NoError(t, err)
	}
	require.NoError(t, s.Delete(ctx, p+"/nodes/c"))

	got, err := s.List(ctx, p+"/nodes/")
	require.NoError(t, err)
	require.Len(t, got, 2, "only live keys under the prefix")
	assert.Equal(t, p+"/nodes/a", got[0].Key)
	assert.Equal(t, []byte("nodes/a"), got[0].Value)
	assert.Equal(t, p+"/nodes/b", got[1].Key)

	all, err := s.List(ctx, p+"/")
	require.NoError(t, err)
	assert.Len(t, all, 4)

	none, err := s.List(ctx, p+"/nothing/")
	require.NoError(t, err)
	assert.Empty(t, none)
}

func binaryValues(t *testing.T, s state.Store, p string) {
	value := []byte{0x00, 0xff, 0xfe, '\n', '"', '\\', 0x80, 0x00}
	_, err := s.Create(ctx, p+"/bin", value)
	require.NoError(t, err)
	e, err := s.Get(ctx, p+"/bin")
	require.NoError(t, err)
	assert.Equal(t, value, e.Value)

	_, err = s.Create(ctx, p+"/empty", nil)
	require.NoError(t, err)
	e, err = s.Get(ctx, p+"/empty")
	require.NoError(t, err)
	assert.Empty(t, e.Value)
}

func invalidKeys(t *testing.T, s state.Store, p string) {
	for _, key := range []string{"", ".hidden", "/lead", "has space", "a*b", "a>b", strings.Repeat("k", 300)} {
		_, err := s.Create(ctx, key, []byte("x"))
		assert.ErrorIs(t, err, state.ErrInvalidKey, "%q", key)
	}
	assert.True(t, state.ValidKey(p+"/backups/zincsearch-20260101T000000Z-7.tgz"))
}

func concurrentCounter(t *testing.T, s state.Store, p string) {
	key := p + "/counter"
	_, err := s.Create(ctx, key, []byte("0"))
	require.NoError(t, err)

	const workers, perWorker = 5, 8
	var wg sync.WaitGroup
	var conflicts atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				for {
					e, err := s.Get(ctx, key)
					if err != nil {
						t.Error(err)
						return
					}
					var n int
					_, _ = fmt.Sscanf(string(e.Value), "%d", &n)
					_, err = s.Update(ctx, key, []byte(fmt.Sprint(n+1)), e.Revision)
					if err == nil {
						break
					}
					if err != state.ErrConflict {
						t.Error(err)
						return
					}
					conflicts.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	e, err := s.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, fmt.Sprint(workers*perWorker), string(e.Value), "no increment was lost (%d conflicts retried)", conflicts.Load())
}

func concurrentCreate(t *testing.T, s state.Store, p string) {
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Create(ctx, p+"/one", []byte(fmt.Sprint(i))); err == nil {
				wins.Add(1)
			} else if err != state.ErrConflict {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(1), wins.Load())
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

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

func leaseOneHolder(t *testing.T, s state.Store, p string) {
	clk := &clock{t: time.Now()}
	a := state.NewLease(s, p+"/leader", "a", 30*time.Second, clk.now)
	b := state.NewLease(s, p+"/leader", "b", 30*time.Second, clk.now)

	ok, err := a.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	ok, err = b.Acquire(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "the lease is taken")

	// The holder renews, so it stays the holder past the original expiry.
	clk.advance(20 * time.Second)
	ok, err = a.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	clk.advance(20 * time.Second)
	ok, err = b.Acquire(ctx)
	require.NoError(t, err)
	assert.False(t, ok)
}

func leaseExpiry(t *testing.T, s state.Store, p string) {
	clk := &clock{t: time.Now()}
	a := state.NewLease(s, p+"/leader", "a", 30*time.Second, clk.now)
	b := state.NewLease(s, p+"/leader", "b", 30*time.Second, clk.now)

	ok, err := a.Acquire(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	clk.advance(31 * time.Second) // a stopped renewing
	ok, err = b.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, ok, "b takes over")

	ok, err = a.Acquire(ctx)
	require.NoError(t, err)
	assert.False(t, ok, "a is no longer the holder")
}

func leaseRelease(t *testing.T, s state.Store, p string) {
	clk := &clock{t: time.Now()}
	a := state.NewLease(s, p+"/leader", "a", time.Minute, clk.now)
	b := state.NewLease(s, p+"/leader", "b", time.Minute, clk.now)

	ok, err := a.Acquire(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, b.Release(ctx), "releasing what somebody else holds does nothing")
	ok, _ = b.Acquire(ctx)
	assert.False(t, ok)

	require.NoError(t, a.Release(ctx))
	ok, err = b.Acquire(ctx)
	require.NoError(t, err)
	assert.True(t, ok, "released at once, no waiting for the expiry")
}

func jsonHelpers(t *testing.T, s state.Store, p string) {
	type doc struct {
		Master string `json:"master"`
		Epoch  int    `json:"epoch"`
	}
	rev, err := state.CreateJSON(ctx, s, p+"/cluster", doc{Master: "a", Epoch: 1})
	require.NoError(t, err)

	var got doc
	gotRev, err := state.GetJSON(ctx, s, p+"/cluster", &got)
	require.NoError(t, err)
	assert.Equal(t, doc{Master: "a", Epoch: 1}, got)
	assert.Equal(t, rev, gotRev)

	_, err = state.UpdateJSON(ctx, s, p+"/cluster", doc{Master: "b", Epoch: 2}, rev)
	require.NoError(t, err)
	_, err = state.GetJSON(ctx, s, p+"/cluster", &got)
	require.NoError(t, err)
	assert.Equal(t, doc{Master: "b", Epoch: 2}, got)

	_, err = state.GetJSON(ctx, s, p+"/nothing", &got)
	assert.ErrorIs(t, err, state.ErrNotFound)
}
