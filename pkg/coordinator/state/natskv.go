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
	"errors"
	"sort"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// NATSKV is a Store on a NATS JetStream key/value bucket. It needs nothing but
// the NATS cluster that the replication stream runs on anyway.
//
// The state then lives in the same failure domain as the stream: while JetStream
// is down, the coordinator cannot change its state either. That is acceptable for
// one coordinator instance; several instances that must keep deciding while
// JetStream is down should use rqlite instead.
type NATSKV struct {
	kv jetstream.KeyValue
}

// OpenNATSKV opens the bucket, creating it when it does not exist. replicas is
// the number of copies (1, 3 or 5); use as many as the stream has.
func OpenNATSKV(ctx context.Context, js jetstream.JetStream, bucket string, replicas int) (*NATSKV, error) {
	if replicas < 1 {
		replicas = 1
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:   bucket,
		Replicas: replicas,
		History:  1,
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		return nil, err
	}
	return &NATSKV{kv: kv}, nil
}

func (n *NATSKV) Get(ctx context.Context, key string) (Entry, error) {
	if !ValidKey(key) {
		return Entry{}, ErrInvalidKey
	}
	e, err := n.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Entry{}, ErrNotFound
	}
	if err != nil {
		return Entry{}, err
	}
	return Entry{Key: key, Value: e.Value(), Revision: e.Revision()}, nil
}

func (n *NATSKV) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	if !ValidKey(key) {
		return 0, ErrInvalidKey
	}
	rev, err := n.kv.Create(ctx, key, value)
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return 0, ErrConflict
	}
	return rev, err
}

func (n *NATSKV) Update(ctx context.Context, key string, value []byte, expected uint64) (uint64, error) {
	if !ValidKey(key) {
		return 0, ErrInvalidKey
	}
	rev, err := n.kv.Update(ctx, key, value, expected)
	if errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.Is(err, jetstream.ErrKeyExists) {
		// A revision that matches nothing is either a lost race or a missing key.
		if _, getErr := n.Get(ctx, key); errors.Is(getErr, ErrNotFound) {
			return 0, ErrNotFound
		}
		return 0, ErrConflict
	}
	return rev, err
}

func (n *NATSKV) Delete(ctx context.Context, key string) error {
	if !ValidKey(key) {
		return ErrInvalidKey
	}
	err := n.kv.Delete(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (n *NATSKV) List(ctx context.Context, prefix string) ([]Entry, error) {
	lister, err := n.kv.ListKeys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var keys []string
	for key := range lister.Keys() {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.Strings(keys)
	entries := make([]Entry, 0, len(keys))
	for _, key := range keys {
		e, err := n.Get(ctx, key)
		if errors.Is(err, ErrNotFound) {
			continue // deleted while listing
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// Close does nothing: the connection belongs to the caller.
func (n *NATSKV) Close() error { return nil }
