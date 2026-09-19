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

package buffer

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func open(t *testing.T, max uint64) (*Buffer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "buffer.db")
	b, err := Open(path, max)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b, path
}

func TestFIFOOrderAndRemove(t *testing.T) {
	b, _ := open(t, 1<<20)
	for i := 1; i <= 5; i++ {
		require.NoError(t, b.Append(Entry{ID: fmt.Sprintf("id-%d", i), Data: []byte(fmt.Sprintf("data-%d", i))}))
	}
	assert.Equal(t, uint64(5), b.Stats().Count)

	items, err := b.Peek(3)
	require.NoError(t, err)
	require.Len(t, items, 3)
	for i, it := range items {
		assert.Equal(t, fmt.Sprintf("id-%d", i+1), it.ID)
		assert.Equal(t, fmt.Sprintf("data-%d", i+1), string(it.Data))
	}
	assert.Equal(t, uint64(5), b.Stats().Count, "peek removes nothing")

	require.NoError(t, b.Remove(items[1].Seq)) // the first two
	rest, err := b.Peek(10)
	require.NoError(t, err)
	require.Len(t, rest, 3)
	assert.Equal(t, "id-3", rest[0].ID)
	assert.Equal(t, uint64(3), b.Stats().Count)

	require.NoError(t, b.Remove(rest[2].Seq))
	assert.Equal(t, Stats{MaxBytes: 1 << 20}, b.Stats())
	require.NoError(t, b.Remove(1000), "removing from an empty buffer is fine")
}

func TestBinaryPayloadAndEmptyID(t *testing.T) {
	b, _ := open(t, 1<<20)
	payload := []byte{0, 1, 2, 0, 255, '\n', 0}
	require.NoError(t, b.Append(Entry{ID: "", Data: payload}, Entry{ID: "x", Data: nil}))
	items, err := b.Peek(2)
	require.NoError(t, err)
	require.Len(t, items, 2)
	assert.Equal(t, payload, items[0].Data)
	assert.Equal(t, "x", items[1].ID)
	assert.Empty(t, items[1].Data)
}

func TestBoundAndAllOrNothing(t *testing.T) {
	b, _ := open(t, 100)
	require.NoError(t, b.Append(Entry{ID: "a", Data: make([]byte, 59)}))
	assert.Equal(t, uint64(60), b.Stats().Bytes)

	// Two entries of which only one would fit: none is stored.
	err := b.Append(Entry{ID: "b", Data: make([]byte, 19)}, Entry{ID: "c", Data: make([]byte, 30)})
	require.ErrorIs(t, err, ErrFull)
	assert.Equal(t, uint64(1), b.Stats().Count)
	assert.Equal(t, uint64(60), b.Stats().Bytes)

	require.ErrorIs(t, b.Append(Entry{ID: "big", Data: make([]byte, 101)}), ErrTooLarge)

	// Space returns when items are removed.
	items, _ := b.Peek(1)
	require.NoError(t, b.Remove(items[0].Seq))
	require.NoError(t, b.Append(Entry{ID: "b", Data: make([]byte, 90)}))
}

func TestSurvivesReopen(t *testing.T) {
	b, path := open(t, 1<<20)
	require.NoError(t, b.Append(Entry{ID: "1", Data: []byte("one")}, Entry{ID: "2", Data: []byte("two")}))
	first, _ := b.Peek(1)
	require.NoError(t, b.Remove(first[0].Seq))
	require.NoError(t, b.Close())

	again, err := Open(path, 1<<20)
	require.NoError(t, err)
	defer again.Close()
	items, err := again.Peek(10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "2", items[0].ID)
	assert.Equal(t, Stats{Count: 1, Bytes: 4, MaxBytes: 1 << 20}, again.Stats())

	// Sequence numbers keep growing after a reopen, so order is kept.
	require.NoError(t, again.Append(Entry{ID: "3"}))
	items, _ = again.Peek(10)
	assert.Equal(t, []string{"2", "3"}, []string{items[0].ID, items[1].ID})
	assert.Greater(t, items[1].Seq, items[0].Seq)
}

func TestConcurrentAppendKeepsCountsExact(t *testing.T) {
	b, _ := open(t, 1<<20)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				assert.NoError(t, b.Append(Entry{ID: fmt.Sprintf("%d-%d", w, i), Data: []byte("xy")}))
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, uint64(100), b.Stats().Count)
	items, err := b.Peek(1000)
	require.NoError(t, err)
	assert.Len(t, items, 100)
}

func TestRejectsNULInID(t *testing.T) {
	b, _ := open(t, 1<<20)
	assert.Error(t, b.Append(Entry{ID: "a\x00b"}))
	_, err := Open("", 0)
	assert.Error(t, err)
}
