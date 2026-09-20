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

package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/streaming"
)

const (
	indexA = "backup_test_a"
	indexB = "backup_test_b"
)

// emptyNode removes every index so the node looks freshly installed.
func emptyNode(t *testing.T) {
	t.Helper()
	for _, idx := range core.ZINC_INDEX_LIST.List() {
		require.NoError(t, core.DeleteIndex(idx.GetName()))
	}
	require.NoError(t, metadata.KV.Delete(streaming.OffsetKey))
}

func fillIndex(t *testing.T, name string, shards int64, docs int) {
	t.Helper()
	idx, err := core.NewIndex(name, "disk", shards)
	require.NoError(t, err)
	require.NoError(t, core.StoreIndex(idx))
	for i := 1; i <= docs; i++ {
		require.NoError(t, idx.CreateDocument(fmt.Sprintf("%s-%d", name, i), map[string]interface{}{"line": fmt.Sprintf("line %d", i)}, false))
	}
}

func waitDrained(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		n, err := streaming.WALPending()
		return err == nil && n == 0
	}, 30*time.Second, 50*time.Millisecond, "the WAL should drain")
}

func docCount(t *testing.T, name string) int {
	t.Helper()
	idx, ok := core.GetIndex(name)
	require.True(t, ok, "index %s should be loaded", name)
	res, err := idx.Search(&meta.ZincQuery{Query: &meta.Query{MatchAll: &meta.MatchAllQuery{}}, Size: 1})
	require.NoError(t, err)
	return res.Hits.Total.Value
}

// node prepares a quiescent node with two indexes and a stream offset.
func node(t *testing.T) {
	t.Helper()
	emptyNode(t)
	t.Cleanup(func() { emptyNode(t) })
	fillIndex(t, indexA, 3, 50)
	fillIndex(t, indexB, 1, 20)
	require.NoError(t, streaming.KVOffsetStore{}.Save(7))
	waitDrained(t)
}

func TestCreateRestoreRoundTrip(t *testing.T) {
	node(t)
	dir := t.TempDir()

	info, err := Create(context.Background(), dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), info.LastApplied, "the manifest carries the stream offset")
	assert.Equal(t, []string{indexA, indexB}, info.Indexes)
	assert.True(t, ValidName(info.Name), info.Name)
	assert.Greater(t, info.Size, int64(0))
	assert.Len(t, info.SHA256, 64)

	// The archive is private: it holds the password hashes of the users.
	st, err := os.Stat(filepath.Join(dir, info.Name))
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), st.Mode().Perm())

	infos, err := List(dir)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	assert.Equal(t, *info, infos[0])

	manifest, err := Verify(context.Background(), filepath.Join(dir, info.Name))
	require.NoError(t, err)
	assert.Equal(t, uint64(7), manifest.LastApplied)
	assert.Equal(t, map[string]uint64{indexA: 50, indexB: 20}, manifest.Docs, "the manifest counts the documents")

	// Lose everything, then restore.
	emptyNode(t)
	require.Zero(t, core.ZINC_INDEX_LIST.Len())
	restored, err := Restore(context.Background(), filepath.Join(dir, info.Name))
	require.NoError(t, err)
	assert.Equal(t, manifest.Files, restored.Files)

	// The restored node resumes the stream after the last message of the backup.
	offset, err := streaming.KVOffsetStore{}.Load()
	require.NoError(t, err)
	assert.Equal(t, uint64(7), offset)

	// The indexes load from the restored metadata and hold the same documents.
	require.NoError(t, core.LoadZincIndexesFromMetadata(meta.Version))
	assert.Equal(t, 50, docCount(t, indexA))
	assert.Equal(t, 20, docCount(t, indexB))
	idxA, _ := core.GetIndex(indexA)
	hit, err := idxA.GetDocument(indexA + "-42")
	require.NoError(t, err)
	assert.Equal(t, indexA+"-42", hit.ID)

	// And they accept new documents.
	require.NoError(t, idxA.CreateDocument("after-restore", map[string]interface{}{"line": "new"}, false))
	waitDrained(t)
	assert.Equal(t, 51, docCount(t, indexA))
}

func TestCreateRefusesANodeThatIsNotQuiescent(t *testing.T) {
	node(t)
	idx, _ := core.GetIndex(indexA)
	// Accepted but not applied yet: the WAL consumer runs once a second.
	require.NoError(t, idx.CreateDocument("late", map[string]interface{}{"line": "late"}, false))

	_, err := Create(context.Background(), t.TempDir())
	require.ErrorIs(t, err, ErrNotQuiescent)

	// Nothing is left behind.
	dir := t.TempDir()
	waitDrained(t)
	_, err = Create(context.Background(), dir)
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, strings.HasPrefix(e.Name(), "."), "leftover %s", e.Name())
	}
}

func TestCreateRefusesADirectoryInsideTheDataPath(t *testing.T) {
	node(t)
	_, err := Create(context.Background(), filepath.Join(config.Global.DataPath, "backups"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "outside the data path")

	_, err = Create(context.Background(), "")
	require.Error(t, err)
}

// entry is one file of an archive.
type entry struct {
	name string
	data []byte
}

func readEntries(t *testing.T, archive string) []entry {
	t.Helper()
	f, err := os.Open(archive)
	require.NoError(t, err)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	tr := tar.NewReader(gz)
	var entries []entry
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data, err := io.ReadAll(tr)
		require.NoError(t, err)
		entries = append(entries, entry{hdr.Name, data})
	}
	return entries
}

func writeEntries(t *testing.T, dst string, entries []entry) {
	t.Helper()
	f, err := os.Create(dst)
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		require.NoError(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: e.name, Mode: 0o640, Size: int64(len(e.data))}))
		_, err := tw.Write(e.data)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
}

func TestVerifyAndRestoreRejectDamagedArchives(t *testing.T) {
	node(t)
	dir := t.TempDir()
	info, err := Create(context.Background(), dir)
	require.NoError(t, err)
	good := filepath.Join(dir, info.Name)
	entries := readEntries(t, good)
	require.Greater(t, len(entries), 3)

	// Entries as they were read, so each case changes exactly one thing.
	clone := func() []entry {
		out := make([]entry, len(entries))
		for i, e := range entries {
			out[i] = entry{e.name, append([]byte(nil), e.data...)}
		}
		return out
	}
	firstData := func(es []entry) int {
		for i, e := range es {
			if strings.HasPrefix(e.name, "data/") && len(e.data) > 0 {
				return i
			}
		}
		t.Fatal("no data file")
		return -1
	}
	rawGood, err := os.ReadFile(good)
	require.NoError(t, err)

	cases := map[string]func(t *testing.T, dst string){
		"a flipped byte in a data file": func(t *testing.T, dst string) {
			es := clone()
			i := firstData(es)
			es[i].data[len(es[i].data)/2] ^= 0xff
			writeEntries(t, dst, es)
		},
		"a data file that is shorter": func(t *testing.T, dst string) {
			es := clone()
			i := firstData(es)
			es[i].data = es[i].data[:len(es[i].data)-1]
			writeEntries(t, dst, es)
		},
		"a file that the manifest lists but the archive lacks": func(t *testing.T, dst string) {
			es := clone()
			i := firstData(es)
			writeEntries(t, dst, append(es[:i], es[i+1:]...))
		},
		"a file that is not in the manifest": func(t *testing.T, dst string) {
			writeEntries(t, dst, append([]entry{{"data/" + indexA + "/extra", []byte("x")}}, clone()...))
		},
		"a missing manifest": func(t *testing.T, dst string) {
			var es []entry
			for _, e := range clone() {
				if e.name != manifestName {
					es = append(es, e)
				}
			}
			writeEntries(t, dst, es)
		},
		"a path that leaves the data directory": func(t *testing.T, dst string) {
			writeEntries(t, dst, append([]entry{{"../evil", []byte("x")}}, clone()...))
		},
		"an absolute path": func(t *testing.T, dst string) {
			writeEntries(t, dst, append([]entry{{"/etc/evil", []byte("x")}}, clone()...))
		},
		"an unexpected top level entry": func(t *testing.T, dst string) {
			writeEntries(t, dst, append([]entry{{"other/file", []byte("x")}}, clone()...))
		},
		"a truncated archive": func(t *testing.T, dst string) {
			require.NoError(t, os.WriteFile(dst, rawGood[:len(rawGood)/2], 0o600))
		},
		"a damaged compressed stream": func(t *testing.T, dst string) {
			bad := append([]byte(nil), rawGood...)
			bad[len(bad)/2] ^= 0xff
			require.NoError(t, os.WriteFile(dst, bad, 0o600))
		},
		"something that is not an archive": func(t *testing.T, dst string) {
			require.NoError(t, os.WriteFile(dst, []byte("hello"), 0o600))
		},
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)

	emptyNode(t)
	before, err := os.ReadDir(config.Global.DataPath)
	require.NoError(t, err)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			bad := filepath.Join(t.TempDir(), "bad.tgz")
			cases[name](t, bad)

			_, err := Verify(context.Background(), bad)
			require.ErrorIs(t, err, ErrCorrupt)

			// A damaged archive changes nothing on the node.
			_, err = Restore(context.Background(), bad)
			require.ErrorIs(t, err, ErrCorrupt)
			after, err := os.ReadDir(config.Global.DataPath)
			require.NoError(t, err)
			assert.Equal(t, len(before), len(after), "the data path is untouched")
			assert.Zero(t, core.ZINC_INDEX_LIST.Len())
		})
	}
}

func TestRestoreRefusesANodeWithData(t *testing.T) {
	node(t)
	dir := t.TempDir()
	info, err := Create(context.Background(), dir)
	require.NoError(t, err)

	_, err = Restore(context.Background(), filepath.Join(dir, info.Name))
	require.ErrorIs(t, err, ErrNotEmpty)
	assert.Equal(t, 50, docCount(t, indexA), "the node is untouched")
}

func TestStoreNamesAndDelete(t *testing.T) {
	for _, bad := range []string{"", "../x.tgz", "zincsearch-x.tgz", "zincsearch-20260101T000000Z-1.tgz/../../etc/passwd", "a/zincsearch-20260101T000000Z-1.tgz"} {
		assert.False(t, ValidName(bad), bad)
	}
	assert.True(t, ValidName("zincsearch-20260101T000000Z-1234.tgz"))

	node(t)
	dir := t.TempDir()
	info, err := Create(context.Background(), dir)
	require.NoError(t, err)

	f, got, err := Open(dir, info.Name)
	require.NoError(t, err)
	_ = f.Close()
	assert.Equal(t, info.Name, got.Name)
	_, _, err = Open(dir, "../"+info.Name)
	assert.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, Delete(dir, info.Name))
	infos, err := List(dir)
	require.NoError(t, err)
	assert.Empty(t, infos)
	_, err = os.Stat(filepath.Join(dir, info.Name))
	assert.True(t, os.IsNotExist(err))
	assert.ErrorIs(t, Delete(dir, info.Name), ErrNotFound)
}

func secondLevelShards(t *testing.T, name string) int {
	t.Helper()
	idx, ok := core.GetIndex(name)
	require.True(t, ok)
	total := 0
	for _, shard := range idx.GetIndex().Shards {
		total += int(shard.ShardNum)
	}
	return total
}

func TestCreateRestoreKeepsEverySecondLevelShard(t *testing.T) {
	// A tiny size limit makes every shard start a new second-level shard after
	// the documents written so far were applied.
	limit := config.Global.Shard.MaxSize
	config.Global.Shard.MaxSize = 1
	t.Cleanup(func() { config.Global.Shard.MaxSize = limit })

	emptyNode(t)
	t.Cleanup(func() { emptyNode(t) })
	idx, err := core.NewIndex(indexA, "disk", 2)
	require.NoError(t, err)
	require.NoError(t, core.StoreIndex(idx))
	for wave := 0; wave < 3; wave++ {
		for i := 0; i < 20; i++ {
			require.NoError(t, idx.CreateDocument(fmt.Sprintf("w%d-%d", wave, i), map[string]interface{}{"wave": wave}, false))
		}
		waitDrained(t)
		want := 2 + wave + 1
		require.Eventually(t, func() bool { return secondLevelShards(t, indexA) >= want }, 20*time.Second, 50*time.Millisecond,
			"wave %d should have started a new second-level shard", wave)
	}
	require.Greater(t, secondLevelShards(t, indexA), 2*1, "the index spans several second-level shards")
	waitDrained(t)

	dir := t.TempDir()
	info, err := Create(context.Background(), dir)
	require.NoError(t, err)

	emptyNode(t)
	_, err = Restore(context.Background(), filepath.Join(dir, info.Name))
	require.NoError(t, err)
	require.NoError(t, core.LoadZincIndexesFromMetadata(meta.Version))
	assert.Equal(t, 60, docCount(t, indexA), "documents of every second-level shard are restored")
}

func TestVerifyDeepRestoresAndCounts(t *testing.T) {
	node(t)
	dir := t.TempDir()
	info, err := Create(context.Background(), dir)
	require.NoError(t, err)
	good := filepath.Join(dir, info.Name)

	// A manifest that claims other numbers than the data holds. The manifest is
	// not covered by its own checksums, so only counting the restored documents
	// can notice.
	entries := readEntries(t, good)
	for i, e := range entries {
		if e.name == manifestName {
			entries[i].data = []byte(strings.Replace(string(e.data), `"`+indexA+`": 50`, `"`+indexA+`": 51`, 1))
		}
	}
	lying := filepath.Join(t.TempDir(), "lying.tgz")
	writeEntries(t, lying, entries)
	_, err = Verify(context.Background(), lying)
	require.NoError(t, err, "checksums alone cannot see it")

	emptyNode(t)
	_, err = VerifyDeep(context.Background(), lying)
	require.ErrorIs(t, err, ErrCorrupt)
	assert.Contains(t, err.Error(), "holds 50 documents after the restore, the backup says 51")

	// The honest archive passes, and the node is left restored.
	emptyNode(t)
	manifest, err := VerifyDeep(context.Background(), good)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), manifest.LastApplied)
	assert.Equal(t, 50, docCount(t, indexA))
}
