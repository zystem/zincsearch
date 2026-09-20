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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/metadata"
	"github.com/zincsearch/zincsearch/pkg/streaming"
)

var createMu sync.Mutex

// nodeState is what must not change while a backup is taken.
type nodeState struct {
	walPending uint64
	offset     uint64
}

func currentState() (nodeState, error) {
	pending, err := streaming.WALPending()
	if err != nil {
		return nodeState{}, err
	}
	offset, err := streaming.KVOffsetStore{}.Load()
	if err != nil {
		return nodeState{}, err
	}
	return nodeState{walPending: pending, offset: offset}, nil
}

// Create writes a backup of the whole node into dir and returns its description.
//
// The node has to be quiescent: no document may be applied while Create runs.
// Create verifies that before and after copying, and returns ErrNotQuiescent
// instead of a backup that might mix two states. dir must be outside the data
// path.
func Create(ctx context.Context, dir string) (*Info, error) {
	if !createMu.TryLock() {
		return nil, errors.New("backup: another backup is running")
	}
	defer createMu.Unlock()

	dir, err := prepareDir(dir)
	if err != nil {
		return nil, err
	}
	before, err := currentState()
	if err != nil {
		return nil, err
	}
	if before.walPending != 0 {
		return nil, fmt.Errorf("%w: %d accepted documents are not applied yet", ErrNotQuiescent, before.walPending)
	}

	staging, err := os.MkdirTemp(dir, ".staging-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(staging)

	indexes := core.ZINC_INDEX_LIST.List()
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].GetName() < indexes[j].GetName() })
	names := make([]string, 0, len(indexes))
	docs := make(map[string]uint64, len(indexes))
	for _, idx := range indexes {
		names = append(names, idx.GetName())
		if err := idx.BackupShards(ctx, filepath.Join(staging, "data", idx.GetName())); err != nil {
			return nil, err
		}
		n, err := idx.DocCount()
		if err != nil {
			return nil, fmt.Errorf("count documents of index %s: %w", idx.GetName(), err)
		}
		docs[idx.GetName()] = n
	}
	if err := writeMetadata(staging); err != nil {
		return nil, err
	}

	after, err := currentState()
	if err != nil {
		return nil, err
	}
	if after != before {
		return nil, fmt.Errorf("%w: it moved from offset %d (%d pending) to offset %d (%d pending)",
			ErrNotQuiescent, before.offset, before.walPending, after.offset, after.walPending)
	}

	manifest := &Manifest{
		Version:     FormatVersion,
		CreatedAt:   time.Now().UTC().Truncate(time.Second),
		LastApplied: before.offset,
		Indexes:     names,
		Docs:        docs,
		Files:       make(map[string]File),
	}
	return writeArchive(ctx, dir, staging, manifest)
}

// prepareDir validates the backup directory and creates it.
func prepareDir(dir string) (string, error) {
	if dir == "" {
		return "", errors.New("backup: the backup directory is not configured (zinc_backup_path)")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	dataPath, err := filepath.Abs(config.Global.DataPath)
	if err != nil {
		return "", err
	}
	if inside(dataPath, abs) {
		return "", fmt.Errorf("backup: the backup directory %s must be outside the data path %s", abs, dataPath)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return "", err
	}
	// A symlink may still lead into the data path.
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		if resolvedData, err := filepath.EvalSymlinks(dataPath); err == nil && inside(resolvedData, resolved) {
			return "", fmt.Errorf("backup: the backup directory %s resolves into the data path", abs)
		}
	}
	return abs, nil
}

func inside(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func writeMetadata(staging string) error {
	entries, err := metadata.Dump()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(entries) // values are []byte, encoded as base64
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(staging, metadataName), raw, 0o600)
}

// writeArchive packs staging into a .tgz in dir. Every file is hashed while it
// is packed, so the manifest describes exactly the bytes in the archive.
func writeArchive(ctx context.Context, dir, staging string, manifest *Manifest) (*Info, error) {
	tmp, err := os.CreateTemp(dir, ".archive-*.tmp")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename

	archiveHash := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(tmp, archiveHash))
	tw := tar.NewWriter(gz)

	err = filepath.WalkDir(staging, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("backup: %s is not a regular file", p)
		}
		rel, err := filepath.Rel(staging, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		file, err := addFile(tw, p, name)
		if err != nil {
			return err
		}
		manifest.Files[name] = file
		return nil
	})
	if err == nil {
		err = addBytes(tw, manifestName, mustJSON(manifest))
	}
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	if syncErr := tmp.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	size, err := fileSize(tmp.Name())
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(tmp.Name(), 0o600); err != nil {
		return nil, err
	}

	name := archiveName(manifest)
	final := filepath.Join(dir, name)
	for i := 2; ; i++ {
		if _, err := os.Lstat(final); errors.Is(err, fs.ErrNotExist) {
			break
		}
		name = fmt.Sprintf("%s-%d.tgz", strings.TrimSuffix(archiveName(manifest), ".tgz"), i)
		final = filepath.Join(dir, name)
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return nil, err
	}

	info := &Info{
		Name:        name,
		Size:        size,
		SHA256:      hex.EncodeToString(archiveHash.Sum(nil)),
		CreatedAt:   manifest.CreatedAt,
		LastApplied: manifest.LastApplied,
		Indexes:     manifest.Indexes,
	}
	if err := writeSidecar(dir, info); err != nil {
		_ = os.Remove(final)
		return nil, err
	}
	syncDir(dir)
	return info, nil
}

func archiveName(m *Manifest) string {
	return fmt.Sprintf("zincsearch-%s-%d.tgz", m.CreatedAt.UTC().Format("20060102T150405Z"), m.LastApplied)
}

func addFile(tw *tar.Writer, p, name string) (File, error) {
	f, err := os.Open(p)
	if err != nil {
		return File{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return File{}, err
	}
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o640, Size: st.Size(), ModTime: st.ModTime()}); err != nil {
		return File{}, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tw, h), f)
	if err != nil {
		return File{}, err
	}
	if n != st.Size() {
		return File{}, fmt.Errorf("backup: %s changed while it was copied", p)
	}
	return File{Size: n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func addBytes(tw *tar.Writer, name string, data []byte) error {
	if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o640, Size: int64(len(data)), ModTime: time.Now()}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

func mustJSON(v interface{}) []byte {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err) // the manifest only holds strings and numbers
	}
	return raw
}

func fileSize(p string) (int64, error) {
	st, err := os.Stat(p)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
