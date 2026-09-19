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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

// countDocs returns how many documents of the index are searchable.
func countDocs(t *testing.T, name string) int {
	t.Helper()
	idx, ok := core.GetIndex(name)
	if !ok {
		return -1
	}
	res, err := idx.Search(&meta.ZincQuery{Query: &meta.Query{MatchAll: &meta.MatchAllQuery{}}, Size: 1})
	require.NoError(t, err)
	return res.Hits.Total.Value
}

func waitDocs(t *testing.T, name string, want int) {
	t.Helper()
	assert.Eventually(t, func() bool { return countDocs(t, name) == want }, 20*time.Second, 100*time.Millisecond,
		"index %s should hold %d documents", name, want)
}

func mustAdmin(t *testing.T, index string, op Op, data interface{}) *Message {
	t.Helper()
	m, err := NewAdmin(index, op, data)
	require.NoError(t, err)
	return m
}

func TestCoreApplier_DocumentsAreIdempotent(t *testing.T) {
	const name = "streaming_apply_docs"
	a := NewCoreApplier()
	t.Cleanup(func() { _ = core.DeleteIndex(name) })

	// The first document creates the index, like the bulk API does.
	require.NoError(t, a.Apply(1, NewDoc(name, "d1", map[string]interface{}{"line": "one"})))
	require.NoError(t, a.Apply(2, NewDoc(name, "d2", map[string]interface{}{"line": "two"})))
	// A document without ID is named after its sequence.
	require.NoError(t, a.Apply(3, NewDoc(name, "", map[string]interface{}{"line": "three"})))
	require.NoError(t, a.Flush())
	waitDocs(t, name, 3)

	// Delivery is at-least-once: applying everything again changes nothing.
	require.NoError(t, a.Apply(1, NewDoc(name, "d1", map[string]interface{}{"line": "one"})))
	require.NoError(t, a.Apply(2, NewDoc(name, "d2", map[string]interface{}{"line": "two"})))
	require.NoError(t, a.Apply(3, NewDoc(name, "", map[string]interface{}{"line": "three"})))
	require.NoError(t, a.Flush())
	time.Sleep(2500 * time.Millisecond) // let the WAL consumer apply the repeats
	waitDocs(t, name, 3)

	idx, _ := core.GetIndex(name)
	hit, err := idx.GetDocument("seq-3")
	require.NoError(t, err)
	source, ok := hit.Source.(map[string]interface{})
	require.True(t, ok, "unexpected source type %T", hit.Source)
	assert.Equal(t, "three", source["line"])
}

func TestCoreApplier_AdminOperations(t *testing.T) {
	const name = "streaming_apply_admin"
	a := NewCoreApplier()
	t.Cleanup(func() { _ = core.DeleteIndex(name) })

	create := mustAdmin(t, name, OpCreateIndex, map[string]interface{}{
		"mappings": map[string]interface{}{"properties": map[string]interface{}{"host": map[string]interface{}{"type": "keyword"}}},
	})
	require.NoError(t, a.Apply(1, create))
	idx, ok := core.GetIndex(name)
	require.True(t, ok)
	prop, ok := idx.GetMappings().GetProperty("host")
	require.True(t, ok)
	assert.Equal(t, "keyword", prop.Type)

	// Repeating a create does not fail and does not reset anything.
	require.NoError(t, a.Apply(2, create))

	// Adding a field works, and adding it again is a no-op.
	setMapping := mustAdmin(t, name, OpSetMapping, map[string]interface{}{
		"properties": map[string]interface{}{"level": map[string]interface{}{"type": "keyword"}},
	})
	require.NoError(t, a.Apply(3, setMapping))
	require.NoError(t, a.Apply(4, setMapping))
	idx, _ = core.GetIndex(name)
	_, ok = idx.GetMappings().GetProperty("level")
	assert.True(t, ok)

	// Redefining a field differently can never succeed, so it is permanent.
	conflict := mustAdmin(t, name, OpSetMapping, map[string]interface{}{
		"properties": map[string]interface{}{"level": map[string]interface{}{"type": "text"}},
	})
	err := a.Apply(5, conflict)
	require.Error(t, err)
	assert.True(t, IsPermanent(err))

	// Delete, and deleting again is a no-op.
	del := mustAdmin(t, name, OpDeleteIndex, nil)
	require.NoError(t, a.Apply(6, del))
	_, ok = core.GetIndex(name)
	assert.False(t, ok)
	require.NoError(t, a.Apply(7, del))
}

func TestCoreApplier_PermanentErrors(t *testing.T) {
	a := NewCoreApplier()
	for name, m := range map[string]*Message{
		"bad index name for a doc":    NewDoc("_forbidden", "x", map[string]interface{}{"a": 1}),
		"bad index name on create":    mustAdmin(t, "bad name!", OpCreateIndex, nil),
		"unknown admin op":            mustAdmin(t, "streaming_perm", Op("explode"), nil),
		"malformed create_index data": {Kind: KindAdmin, Index: "streaming_perm", Op: OpCreateIndex, Data: []byte(`{`)},
		"malformed set_mapping data":  {Kind: KindAdmin, Index: "streaming_perm", Op: OpSetMapping, Data: []byte(`[`)},
	} {
		t.Run(name, func(t *testing.T) {
			err := a.Apply(1, m)
			require.Error(t, err)
			assert.True(t, IsPermanent(err), "got %v", err)
		})
	}
}

func TestCoreApplier_DocsBatchIsIdempotent(t *testing.T) {
	const name = "streaming_apply_batch"
	a := NewCoreApplier()
	t.Cleanup(func() { _ = core.DeleteIndex(name) })

	batch := NewDocs(name, []Doc{
		{ID: "b1", Doc: map[string]interface{}{"line": "one"}},
		{Doc: map[string]interface{}{"line": "two"}},
		{Doc: map[string]interface{}{"line": "three"}},
	})
	require.NoError(t, a.Apply(10, batch))
	require.NoError(t, a.Flush())
	waitDocs(t, name, 3)

	// The same message again, as after a crash before the offset was saved.
	require.NoError(t, a.Apply(10, batch))
	require.NoError(t, a.Flush())
	time.Sleep(2500 * time.Millisecond)
	waitDocs(t, name, 3)

	idx, _ := core.GetIndex(name)
	for _, id := range []string{"b1", "seq-10-1", "seq-10-2"} {
		_, err := idx.GetDocument(id)
		assert.NoError(t, err, id)
	}
}

func TestCoreApplier_InvalidDocumentIsPermanentAndDoesNotBlockTheBatch(t *testing.T) {
	const name = "streaming_apply_invalid"
	a := NewCoreApplier()
	t.Cleanup(func() { _ = core.DeleteIndex(name) })

	require.NoError(t, a.Apply(1, mustAdmin(t, name, OpCreateIndex, map[string]interface{}{
		"mappings": map[string]interface{}{"properties": map[string]interface{}{"n": map[string]interface{}{"type": "numeric"}}},
	})))

	err := a.Apply(2, NewDoc(name, "bad", map[string]interface{}{"n": "abc"}))
	require.Error(t, err)
	assert.True(t, IsPermanent(err), "a document that violates the mapping can never succeed: %v", err)

	batch := NewDocs(name, []Doc{
		{ID: "ok1", Doc: map[string]interface{}{"n": 1}},
		{ID: "bad", Doc: map[string]interface{}{"n": "abc"}},
		{ID: "ok2", Doc: map[string]interface{}{"n": 2}},
	})
	require.NoError(t, a.Apply(3, batch))
	require.NoError(t, a.Flush())
	waitDocs(t, name, 2)
}

func TestCoreApplier_CreateIndexRejectsAMismatchingName(t *testing.T) {
	const name = "streaming_apply_mismatch"
	a := NewCoreApplier()
	t.Cleanup(func() { _ = core.DeleteIndex(name); _ = core.DeleteIndex(name + "_other") })

	m := mustAdmin(t, name, OpCreateIndex, map[string]interface{}{"name": name + "_other"})
	for i := 0; i < 2; i++ { // a redelivery must give the same answer
		err := a.Apply(1, m)
		require.Error(t, err)
		assert.True(t, IsPermanent(err), "got %v", err)
	}
	_, ok := core.GetIndex(name + "_other")
	assert.False(t, ok, "nothing is created under the other name")

	require.NoError(t, a.Apply(2, mustAdmin(t, name, OpCreateIndex, map[string]interface{}{"name": name})))
	require.NoError(t, a.Apply(2, mustAdmin(t, name, OpCreateIndex, map[string]interface{}{"name": name})))
}
