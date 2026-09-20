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

package blob

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FS is a Store on a directory. It is atomic per object: a reader sees an object
// completely or not at all.
type FS struct {
	root string
}

// NewFS returns a store in root, which is created when needed.
func NewFS(root string) (*FS, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, err
	}
	return &FS{root: abs}, nil
}

func (f *FS) path(name string) string { return filepath.Join(f.root, filepath.FromSlash(name)) }

func (f *FS) Put(ctx context.Context, name string, r io.Reader, _ int64) error {
	if !ValidName(name) {
		return ErrInvalidName
	}
	target := f.path(name)
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".put-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, err = io.Copy(tmp, &ctxReader{ctx: ctx, r: r})
	if syncErr := tmp.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp.Name(), target)
}

func (f *FS) Get(_ context.Context, name string) (io.ReadCloser, error) {
	if !ValidName(name) {
		return nil, ErrInvalidName
	}
	file, err := os.Open(f.path(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return file, err
}

func (f *FS) List(ctx context.Context, prefix string) ([]Info, error) {
	var infos []Info
	err := filepath.WalkDir(f.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".put-") {
			return nil
		}
		rel, err := filepath.Rel(f.root, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if !strings.HasPrefix(name, prefix) {
			return nil
		}
		st, err := d.Info()
		if err != nil {
			return err
		}
		infos = append(infos, Info{Name: name, Size: st.Size(), ModTime: st.ModTime()})
		return nil
	})
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, err
}

func (f *FS) Delete(_ context.Context, name string) error {
	if !ValidName(name) {
		return ErrInvalidName
	}
	if err := os.Remove(f.path(name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// ctxReader stops a copy when the context ends.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
