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

// Package buffer is a small durable FIFO on disk. The coordinator parks
// messages in it while the stream is unreachable, so a short outage of NATS does
// not lose logs and does not stop builds.
package buffer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"go.etcd.io/bbolt"
)

var (
	// ErrFull means the entries do not fit within the configured size limit.
	ErrFull = errors.New("buffer: full")
	// ErrTooLarge means one entry is larger than the whole buffer.
	ErrTooLarge = errors.New("buffer: entry larger than the buffer")
)

var (
	itemsBucket = []byte("items")
	metaBucket  = []byte("meta")
	countKey    = []byte("count")
	bytesKey    = []byte("bytes")
)

// Entry is what is stored: an identifier and the payload. The identifier lets a
// consumer make redelivery harmless, e.g. as a JetStream message ID.
type Entry struct {
	ID   string
	Data []byte
}

// Item is a stored entry with its position. Sequence numbers only grow.
type Item struct {
	Seq uint64
	Entry
}

// Stats describes how full the buffer is.
type Stats struct {
	Count    uint64
	Bytes    uint64
	MaxBytes uint64
}

// Buffer is a durable FIFO bounded in bytes. Every change reaches the disk
// before the call returns.
type Buffer struct {
	db  *bbolt.DB
	max uint64
}

// Open opens or creates the buffer file. maxBytes bounds the payload it holds.
func Open(path string, maxBytes uint64) (*Buffer, error) {
	if maxBytes == 0 {
		return nil, errors.New("buffer: maxBytes must be positive")
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 5_000_000_000})
	if err != nil {
		return nil, fmt.Errorf("buffer: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(itemsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("buffer: %w", err)
	}
	return &Buffer{db: db, max: maxBytes}, nil
}

func encodeKey(seq uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, seq)
	return k
}

func encodeValue(e Entry) []byte { return []byte(e.ID + "\x00" + string(e.Data)) }

func decodeValue(v []byte) Entry {
	id, data, _ := strings.Cut(string(v), "\x00")
	return Entry{ID: id, Data: []byte(data)}
}

func size(e Entry) uint64 { return uint64(len(e.ID) + len(e.Data)) }

func getUint(b *bbolt.Bucket, key []byte) uint64 {
	if v := b.Get(key); len(v) == 8 {
		return binary.BigEndian.Uint64(v)
	}
	return 0
}

func putUint(b *bbolt.Bucket, key []byte, n uint64) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, n)
	return b.Put(key, v)
}

// Append adds the entries in order, all or none. It returns ErrFull, and stores
// nothing, when they would not fit.
func (b *Buffer) Append(entries ...Entry) error {
	var total uint64
	for _, e := range entries {
		if strings.Contains(e.ID, "\x00") {
			return errors.New("buffer: an ID must not contain a NUL byte")
		}
		if size(e) > b.max {
			return ErrTooLarge
		}
		total += size(e)
	}
	return b.db.Update(func(tx *bbolt.Tx) error {
		items, meta := tx.Bucket(itemsBucket), tx.Bucket(metaBucket)
		used := getUint(meta, bytesKey)
		if used+total > b.max {
			return ErrFull
		}
		for _, e := range entries {
			seq, err := items.NextSequence()
			if err != nil {
				return err
			}
			if err := items.Put(encodeKey(seq), encodeValue(e)); err != nil {
				return err
			}
		}
		if err := putUint(meta, bytesKey, used+total); err != nil {
			return err
		}
		return putUint(meta, countKey, getUint(meta, countKey)+uint64(len(entries)))
	})
}

// Peek returns up to n of the oldest items without removing them.
func (b *Buffer) Peek(n int) ([]Item, error) {
	var out []Item
	err := b.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(itemsBucket).Cursor()
		for k, v := c.First(); k != nil && len(out) < n; k, v = c.Next() {
			out = append(out, Item{Seq: binary.BigEndian.Uint64(k), Entry: decodeValue(v)})
		}
		return nil
	})
	return out, err
}

// Remove deletes every item with a sequence number up to and including upTo.
func (b *Buffer) Remove(upTo uint64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		items, meta := tx.Bucket(itemsBucket), tx.Bucket(metaBucket)
		var count, bytes uint64
		c := items.Cursor()
		for k, v := c.First(); k != nil && binary.BigEndian.Uint64(k) <= upTo; k, v = c.First() {
			count++
			bytes += size(decodeValue(v))
			if err := c.Delete(); err != nil {
				return err
			}
		}
		if count == 0 {
			return nil
		}
		if err := putUint(meta, bytesKey, getUint(meta, bytesKey)-bytes); err != nil {
			return err
		}
		return putUint(meta, countKey, getUint(meta, countKey)-count)
	})
}

// Stats reports how much the buffer holds.
func (b *Buffer) Stats() Stats {
	s := Stats{MaxBytes: b.max}
	_ = b.db.View(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(metaBucket)
		s.Count, s.Bytes = getUint(meta, countKey), getUint(meta, bytesKey)
		return nil
	})
	return s
}

// Close closes the file.
func (b *Buffer) Close() error { return b.db.Close() }
