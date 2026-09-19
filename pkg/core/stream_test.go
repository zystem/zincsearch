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

package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex_WALPendingAndSync(t *testing.T) {
	const name = "TestIndex_WALPendingAndSync"
	index, err := NewIndex(name, "disk", 2)
	require.NoError(t, err)
	require.NoError(t, StoreIndex(index))
	t.Cleanup(func() { assert.NoError(t, DeleteIndex(name)) })

	pending, err := index.WALPending()
	require.NoError(t, err)
	assert.Zero(t, pending, "a fresh index has nothing to apply")

	for _, id := range []string{"a", "b", "c", "d"} {
		require.NoError(t, index.CreateDocument(id, map[string]interface{}{"name": id}, false))
	}
	assert.NoError(t, index.SyncWAL())

	// The background consumer applies accepted entries and the counter drains.
	assert.Eventually(t, func() bool {
		pending, err := index.WALPending()
		return err == nil && pending == 0
	}, 10*time.Second, 50*time.Millisecond)

	// Applied documents are visible by ID.
	for _, id := range []string{"a", "b", "c", "d"} {
		hit, err := index.GetDocument(id)
		require.NoError(t, err)
		assert.Equal(t, id, hit.ID)
	}
}

func TestIndex_DocCountIsExactBeyondTheSearchLimit(t *testing.T) {
	const name = "TestIndex_DocCount"
	index, err := NewIndex(name, "disk", 3)
	require.NoError(t, err)
	require.NoError(t, StoreIndex(index))
	t.Cleanup(func() { assert.NoError(t, DeleteIndex(name)) })

	n, err := index.DocCount()
	require.NoError(t, err)
	assert.Zero(t, n)

	// More than the 10000 hits a search reports.
	const docs = 10_250
	for i := 0; i < docs; i++ {
		require.NoError(t, index.CreateDocument(fmt.Sprintf("d%d", i), map[string]interface{}{"i": i}, false))
	}
	// Replacing a document must not count it twice.
	require.NoError(t, index.CreateDocument("d0", map[string]interface{}{"i": -1}, true))
	require.Eventually(t, func() bool {
		pending, err := index.WALPending()
		return err == nil && pending == 0
	}, 60*time.Second, 100*time.Millisecond)

	n, err = index.DocCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(docs), n)
}
