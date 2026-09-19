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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/metadata"
)

const restoreTmpPrefix = ".restore-"

// VerifyDeep proves that an archive can be restored and holds what it claims:
// it restores the archive into this node, loads the indexes, opens every shard
// and compares the number of documents of each index with the manifest.
//
// It needs a node without indexes, so run it with a scratch ZINC_DATA_PATH that
// is thrown away afterwards. It goes beyond Verify, which only compares
// checksums: it finds an archive that is intact but that the engine cannot use.
func VerifyDeep(ctx context.Context, archive string) (*Manifest, error) {
	manifest, err := Restore(ctx, archive)
	if err != nil {
		return nil, err
	}
	if err := core.LoadZincIndexesFromMetadata(meta.Version); err != nil {
		return nil, fmt.Errorf("%w: load the restored indexes: %v", ErrCorrupt, err)
	}
	for _, name := range manifest.Indexes {
		idx, ok := core.GetIndex(name)
		if !ok {
			return nil, corrupt("index %s was restored but is not loaded", name)
		}
		got, err := idx.DocCount()
		if err != nil {
			return nil, corrupt("count the documents of index %s: %v", name, err)
		}
		if want := manifest.Docs[name]; got != want {
			return nil, corrupt("index %s holds %d documents after the restore, the backup says %d", name, got, want)
		}
	}
	return manifest, nil
}

// Restore puts the contents of an archive into this node, which must not have
// any index yet. The archive is extracted and verified completely against its
// manifest before anything is put in place, so a damaged archive changes nothing.
//
// The indexes are loaded when the node starts. Restore right before that: the
// restored metadata carries the stream offset, so the stream consumer continues
// exactly after the last message the backup contains.
func Restore(ctx context.Context, archive string) (*Manifest, error) {
	dataPath := config.Global.DataPath
	if err := checkEmpty(dataPath); err != nil {
		return nil, err
	}

	tmp, err := os.MkdirTemp(dataPath, restoreTmpPrefix)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)

	manifest, _, err := readArchive(ctx, archive, tmp)
	if err != nil {
		return nil, err
	}
	entries, err := readMetadata(filepath.Join(tmp, metadataName))
	if err != nil {
		return nil, err
	}

	moved, err := moveIndexes(filepath.Join(tmp, "data"), dataPath)
	if err == nil {
		err = metadata.Load(entries)
	}
	if err != nil {
		for _, p := range moved {
			_ = os.RemoveAll(p)
		}
		return nil, err
	}
	return manifest, nil
}

// checkEmpty refuses to restore over existing data.
func checkEmpty(dataPath string) error {
	if n := core.ZINC_INDEX_LIST.Len(); n > 0 {
		return fmt.Errorf("%w: %d indexes are loaded", ErrNotEmpty, n)
	}
	entries, err := os.ReadDir(dataPath)
	if err != nil {
		return err
	}
	for _, e := range entries {
		// Files and directories that start with "_" or "." belong to the node
		// itself: the metadata database and scratch space.
		if name := e.Name(); strings.HasPrefix(name, "_") || strings.HasPrefix(name, ".") {
			continue
		}
		return fmt.Errorf("%w: %s already exists in %s", ErrNotEmpty, e.Name(), dataPath)
	}
	return nil
}

func readMetadata(p string) (map[string][]byte, error) {
	raw, err := os.ReadFile(p)
	if err != nil {
		return nil, corrupt("read %s: %v", metadataName, err)
	}
	entries := make(map[string][]byte)
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, corrupt("%s: %v", metadataName, err)
	}
	return entries, nil
}

// moveIndexes renames every index directory of src into dst. It returns what it
// moved, also on failure, so the caller can undo it.
func moveIndexes(src, dst string) ([]string, error) {
	entries, err := os.ReadDir(src)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // a backup of a node without indexes
	}
	if err != nil {
		return nil, err
	}
	var moved []string
	for _, e := range entries {
		target := filepath.Join(dst, e.Name())
		if _, err := os.Lstat(target); err == nil {
			return moved, fmt.Errorf("%w: %s already exists", ErrNotEmpty, target)
		}
		if err := os.Rename(filepath.Join(src, e.Name()), target); err != nil {
			return moved, err
		}
		moved = append(moved, target)
	}
	return moved, nil
}
