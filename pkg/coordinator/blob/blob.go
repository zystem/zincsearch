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

// Package blob is where the coordinator keeps backups: an S3 compatible bucket,
// or a directory for small setups and tests.
package blob

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
)

// ErrNotFound means there is no object of that name.
var ErrNotFound = errors.New("blob: not found")

// ErrInvalidName means the name is not usable as an object name.
var ErrInvalidName = errors.New("blob: invalid name")

// Info describes a stored object.
type Info struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// Store keeps named blobs.
type Store interface {
	// Put stores the reader's content under name, replacing an object of the same
	// name. size is the number of bytes, or -1 when unknown.
	Put(ctx context.Context, name string, r io.Reader, size int64) error
	// Get opens an object, or returns ErrNotFound.
	Get(ctx context.Context, name string) (io.ReadCloser, error)
	// List returns the objects whose name starts with prefix, ordered by name.
	List(ctx context.Context, prefix string) ([]Info, error)
	// Delete removes an object. Deleting a missing object is not an error.
	Delete(ctx context.Context, name string) error
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._=/-]{0,511}$`)

// ValidName reports whether name can be stored: letters, digits and "._=/-",
// starting with a letter or digit, without empty or dot-dot path elements.
func ValidName(name string) bool {
	if !nameRe.MatchString(name) {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
