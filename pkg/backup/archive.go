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

// Package backup creates, verifies and restores backups of a whole node.
//
// A backup is a single .tgz that holds the index data, the node metadata and a
// manifest with the checksum of every file. The manifest also records the stream
// sequence the data reaches, so a node restored from it resumes consuming the
// stream exactly where the backup ended.
//
// A backup is only consistent while the node does not change. Pause the stream
// consumer and wait until the WAL is drained (see streaming.Consumer.PauseAndDrain)
// before calling Create; Create refuses to run and fails if the node changed.
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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// FormatVersion is the version of the archive layout.
const FormatVersion = 1

const (
	manifestName = "manifest.json"
	metadataName = "metadata.json"
	dataPrefix   = "data/"
	// maxManifestSize bounds how much of a corrupt archive is read as a manifest.
	maxManifestSize = 64 << 20
)

var (
	// ErrNotQuiescent means the node was changing while it was backed up.
	ErrNotQuiescent = errors.New("backup: the node is not quiescent, pause the stream consumer and let the WAL drain first")
	// ErrCorrupt means an archive does not match its manifest or is malformed.
	ErrCorrupt = errors.New("backup: the archive is corrupt")
	// ErrNotEmpty means restore was asked to write into a node that has data.
	ErrNotEmpty = errors.New("backup: restore needs a node without indexes")
)

// File describes one file of an archive.
type File struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Manifest is the table of contents of an archive. It is the last entry of the
// archive and lists every other entry.
type Manifest struct {
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	// LastApplied is the stream sequence of the last message the data contains.
	// It is 0 when the node does not consume a stream.
	LastApplied uint64   `json:"last_applied"`
	Indexes     []string `json:"indexes"`
	// Docs is the number of documents of every index at the time of the backup.
	// VerifyDeep checks a restored node against it.
	Docs  map[string]uint64 `json:"docs"`
	Files map[string]File   `json:"files"`
}

// Info describes a stored archive.
type Info struct {
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	SHA256      string    `json:"sha256"`
	CreatedAt   time.Time `json:"created_at"`
	LastApplied uint64    `json:"last_applied"`
	Indexes     []string  `json:"indexes"`
}

func corrupt(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// Verify reads the whole archive and checks it against its manifest without
// extracting anything. It detects truncation, bit flips, missing and extra files.
func Verify(ctx context.Context, archive string) (*Manifest, error) {
	m, _, err := readArchive(ctx, archive, "")
	return m, err
}

// readArchive streams the archive, hashing every file. When dest is not empty
// the files are also written below it. The manifest is checked against what was
// read, so a successful return means the archive is intact.
func readArchive(ctx context.Context, archive, dest string) (*Manifest, map[string]File, error) {
	f, err := os.Open(archive)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, nil, corrupt("not a gzip stream: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)

	var manifest *Manifest
	got := make(map[string]File)
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, corrupt("read tar: %v", err)
		}
		if hdr.Typeflag == tar.TypeDir {
			continue
		}
		if hdr.Typeflag != tar.TypeReg {
			return nil, nil, corrupt("entry %q is not a regular file", hdr.Name)
		}
		name, err := safeName(hdr.Name)
		if err != nil {
			return nil, nil, err
		}
		if _, dup := got[name]; dup || (name == manifestName && manifest != nil) {
			return nil, nil, corrupt("duplicate entry %q", name)
		}

		if name == manifestName {
			raw, err := io.ReadAll(io.LimitReader(tr, maxManifestSize))
			if err != nil {
				return nil, nil, corrupt("read manifest: %v", err)
			}
			manifest = new(Manifest)
			if err := json.Unmarshal(raw, manifest); err != nil {
				return nil, nil, corrupt("manifest: %v", err)
			}
			continue
		}

		file, err := readEntry(tr, dest, name)
		if err != nil {
			return nil, nil, err
		}
		got[name] = file
	}
	// Reading to the end verifies the gzip checksum and length.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return nil, nil, corrupt("gzip: %v", err)
	}

	if manifest == nil {
		return nil, nil, corrupt("no %s", manifestName)
	}
	if manifest.Version != FormatVersion {
		return nil, nil, corrupt("unsupported format version %d", manifest.Version)
	}
	if err := matchManifest(manifest, got); err != nil {
		return nil, nil, err
	}
	return manifest, got, nil
}

func readEntry(r io.Reader, dest, name string) (File, error) {
	h := sha256.New()
	var w io.Writer = h
	var out *os.File
	if dest != "" {
		target := filepath.Join(dest, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return File{}, err
		}
		var err error
		out, err = os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
		if err != nil {
			return File{}, err
		}
		w = io.MultiWriter(out, h)
	}
	n, err := io.Copy(w, r)
	if out != nil {
		if syncErr := out.Sync(); err == nil {
			err = syncErr
		}
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil {
		return File{}, corrupt("read %q: %v", name, err)
	}
	return File{Size: n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// matchManifest requires the files that were read to be exactly the files the
// manifest lists, byte for byte.
func matchManifest(m *Manifest, got map[string]File) error {
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		read, ok := got[name]
		if !ok {
			return corrupt("%q is listed in the manifest but missing", name)
		}
		if want := m.Files[name]; read != want {
			return corrupt("%q differs from the manifest: size %d sha256 %s, want size %d sha256 %s",
				name, read.Size, read.SHA256, want.Size, want.SHA256)
		}
	}
	for name := range got {
		if _, ok := m.Files[name]; !ok {
			return corrupt("%q is in the archive but not in the manifest", name)
		}
	}
	return nil
}

// safeName validates an archive entry name and returns it in clean form. Only
// the layout this package writes is accepted, so a crafted archive cannot place
// files anywhere else.
func safeName(name string) (string, error) {
	clean := path.Clean(name)
	if clean != name || path.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains(clean, "\\") {
		return "", corrupt("unsafe entry name %q", name)
	}
	if clean != manifestName && clean != metadataName && !strings.HasPrefix(clean, dataPrefix) {
		return "", corrupt("unexpected entry %q", name)
	}
	return clean, nil
}
