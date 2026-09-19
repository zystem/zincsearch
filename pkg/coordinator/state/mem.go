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

package state

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// Mem is a Store in memory. It keeps nothing across restarts; it is meant for
// tests and for embedding the coordinator where losing its state is acceptable.
type Mem struct {
	mu      sync.Mutex
	entries map[string]Entry
	rev     uint64
}

// NewMem returns an empty in-memory store.
func NewMem() *Mem { return &Mem{entries: make(map[string]Entry)} }

func copyBytes(b []byte) []byte { return append([]byte(nil), b...) }

func (m *Mem) Get(_ context.Context, key string) (Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return Entry{}, ErrNotFound
	}
	e.Value = copyBytes(e.Value)
	return e, nil
}

func (m *Mem) Create(_ context.Context, key string, value []byte) (uint64, error) {
	if !ValidKey(key) {
		return 0, ErrInvalidKey
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.entries[key]; ok {
		return 0, ErrConflict
	}
	m.rev++
	m.entries[key] = Entry{Key: key, Value: copyBytes(value), Revision: m.rev}
	return m.rev, nil
}

func (m *Mem) Update(_ context.Context, key string, value []byte, expected uint64) (uint64, error) {
	if !ValidKey(key) {
		return 0, ErrInvalidKey
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return 0, ErrNotFound
	}
	if e.Revision != expected {
		return 0, ErrConflict
	}
	m.rev++
	m.entries[key] = Entry{Key: key, Value: copyBytes(value), Revision: m.rev}
	return m.rev, nil
}

func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
	return nil
}

func (m *Mem) List(_ context.Context, prefix string) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entry
	for key, e := range m.entries {
		if strings.HasPrefix(key, prefix) {
			e.Value = copyBytes(e.Value)
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *Mem) Close() error { return nil }
