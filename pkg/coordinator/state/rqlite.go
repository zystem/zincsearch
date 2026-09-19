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
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/rqlite/gorqlite"
)

// RQLite is a Store on an rqlite cluster: a failure domain of its own, so the
// coordinator can keep deciding while JetStream is down, and any number of
// coordinator instances can share it.
type RQLite struct {
	conn *gorqlite.Connection
}

// A deleted key stays as a tombstone, so its revision keeps growing: a writer
// that still holds the revision from before a delete and re-create can never
// succeed by accident.
const rqliteSchema = `CREATE TABLE IF NOT EXISTS coordinator_state (
	key TEXT PRIMARY KEY,
	value TEXT NOT NULL,
	revision INTEGER NOT NULL,
	deleted INTEGER NOT NULL DEFAULT 0
)`

// OpenRQLite connects to rqlite, e.g. "http://rqlite:4001", and creates the
// table it needs. Reads use the "strong" consistency level, so a value that
// was written is the value that is read, also on another node.
func OpenRQLite(ctx context.Context, url string) (*RQLite, error) {
	conn, err := gorqlite.Open(url)
	if err != nil {
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	if err := conn.SetConsistencyLevel(gorqlite.ConsistencyLevelStrong); err != nil {
		conn.Close()
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	r := &RQLite{conn: conn}
	if _, err := r.exec(ctx, rqliteSchema); err != nil {
		conn.Close()
		return nil, err
	}
	return r, nil
}

func (r *RQLite) exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	wr, err := r.conn.WriteOneParameterizedContext(ctx, gorqlite.ParameterizedStatement{Query: query, Arguments: args})
	if err != nil {
		return 0, fmt.Errorf("rqlite: %w", err)
	}
	return wr.RowsAffected, nil
}

// Values are stored base64 encoded: rqlite speaks JSON, which cannot carry
// arbitrary bytes.
func encode(value []byte) string { return base64.StdEncoding.EncodeToString(value) }

func decode(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

func (r *RQLite) Get(ctx context.Context, key string) (Entry, error) {
	if !ValidKey(key) {
		return Entry{}, ErrInvalidKey
	}
	entries, err := r.query(ctx, "SELECT key, value, revision FROM coordinator_state WHERE key = ? AND deleted = 0", key)
	if err != nil {
		return Entry{}, err
	}
	if len(entries) == 0 {
		return Entry{}, ErrNotFound
	}
	return entries[0], nil
}

func (r *RQLite) query(ctx context.Context, query string, args ...interface{}) ([]Entry, error) {
	qr, err := r.conn.QueryOneParameterizedContext(ctx, gorqlite.ParameterizedStatement{Query: query, Arguments: args})
	if err != nil {
		return nil, fmt.Errorf("rqlite: %w", err)
	}
	var entries []Entry
	for qr.Next() {
		var key, value string
		var revision int64
		if err := qr.Scan(&key, &value, &revision); err != nil {
			return nil, fmt.Errorf("rqlite: %w", err)
		}
		raw, err := decode(value)
		if err != nil {
			return nil, fmt.Errorf("rqlite: value of %s is not base64: %w", key, err)
		}
		entries = append(entries, Entry{Key: key, Value: raw, Revision: uint64(revision)})
	}
	return entries, nil
}

func (r *RQLite) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if !ValidKey(key) {
		return 0, ErrInvalidKey
	}
	n, err := r.exec(ctx, "INSERT INTO coordinator_state (key, value, revision, deleted) VALUES (?, ?, 1, 0) ON CONFLICT(key) DO NOTHING", key, encode(value))
	if err != nil {
		return 0, err
	}
	if n == 1 {
		return 1, nil
	}

	// The key has a row: either it is live, or it is a tombstone to revive.
	tombstone, err := r.query(ctx, "SELECT key, value, revision FROM coordinator_state WHERE key = ? AND deleted = 1", key)
	if err != nil {
		return 0, err
	}
	if len(tombstone) == 0 {
		return 0, ErrConflict
	}
	next := tombstone[0].Revision + 1
	n, err = r.exec(ctx, "UPDATE coordinator_state SET value = ?, revision = ?, deleted = 0 WHERE key = ? AND deleted = 1 AND revision = ?",
		encode(value), int64(next), key, int64(tombstone[0].Revision))
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, ErrConflict // somebody revived it first
	}
	return next, nil
}

func (r *RQLite) Update(ctx context.Context, key string, value []byte, expected uint64) (uint64, error) {
	if !ValidKey(key) {
		return 0, ErrInvalidKey
	}
	n, err := r.exec(ctx, "UPDATE coordinator_state SET value = ?, revision = revision + 1 WHERE key = ? AND revision = ? AND deleted = 0", encode(value), key, int64(expected))
	if err != nil {
		return 0, err
	}
	if n == 1 {
		return expected + 1, nil
	}
	if _, err := r.Get(ctx, key); errors.Is(err, ErrNotFound) {
		return 0, ErrNotFound
	}
	return 0, ErrConflict
}

func (r *RQLite) Delete(ctx context.Context, key string) error {
	if !ValidKey(key) {
		return ErrInvalidKey
	}
	_, err := r.exec(ctx, "UPDATE coordinator_state SET value = '', revision = revision + 1, deleted = 1 WHERE key = ? AND deleted = 0", key)
	return err
}

func (r *RQLite) List(ctx context.Context, prefix string) ([]Entry, error) {
	return r.query(ctx, "SELECT key, value, revision FROM coordinator_state WHERE deleted = 0 AND substr(key, 1, ?) = ? ORDER BY key", len(prefix), prefix)
}

func (r *RQLite) Close() error {
	r.conn.Close()
	return nil
}
