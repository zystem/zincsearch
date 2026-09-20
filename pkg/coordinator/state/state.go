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

// Package state is the small key/value store the coordinator keeps its state in:
// which node is master, the promotion history, the list of backups.
//
// The interface is deliberately tiny and every write is a compare-and-set on a
// revision, so it maps onto NATS JetStream key/value buckets (the default, no
// extra service) as well as onto rqlite (an independent failure domain).
package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	// ErrNotFound means the key does not exist.
	ErrNotFound = errors.New("state: not found")
	// ErrConflict means the key exists already (Create) or its revision is not the
	// expected one (Update): somebody else wrote in between.
	ErrConflict = errors.New("state: revision conflict")
	// ErrInvalidKey means the key uses characters the stores cannot all handle.
	ErrInvalidKey = errors.New("state: invalid key")
)

var keyRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9/_=.-]{0,254}$`)

// ValidKey reports whether key can be stored. Keys are made of letters, digits
// and "/_=.-", start with a letter or digit, and are at most 255 bytes long.
func ValidKey(key string) bool { return keyRe.MatchString(key) }

// Entry is a stored value with its revision. Revisions grow with every write of
// the key; they are only compared for equality.
type Entry struct {
	Key      string
	Value    []byte
	Revision uint64
}

// Store is the coordinator's state store.
type Store interface {
	// Get returns the entry, or ErrNotFound.
	Get(ctx context.Context, key string) (Entry, error)
	// Create stores value under a key that does not exist yet; ErrConflict otherwise.
	Create(ctx context.Context, key string, value []byte) (revision uint64, err error)
	// Update replaces the value when the stored revision is expected. It returns
	// ErrConflict on another revision and ErrNotFound on a missing key.
	Update(ctx context.Context, key string, value []byte, expected uint64) (revision uint64, err error)
	// Delete removes the key. Deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// List returns the entries whose key starts with prefix, ordered by key.
	List(ctx context.Context, prefix string) ([]Entry, error)
	// Close releases the connection.
	Close() error
}

// GetJSON reads a JSON value into v and returns its revision.
func GetJSON(ctx context.Context, s Store, key string, v interface{}) (uint64, error) {
	e, err := s.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(e.Value, v); err != nil {
		return 0, fmt.Errorf("state: %s holds invalid JSON: %w", key, err)
	}
	return e.Revision, nil
}

// CreateJSON stores v as JSON under a new key.
func CreateJSON(ctx context.Context, s Store, key string, v interface{}) (uint64, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return s.Create(ctx, key, raw)
}

// UpdateJSON replaces the value with v as JSON when the revision is expected.
func UpdateJSON(ctx context.Context, s Store, key string, v interface{}, expected uint64) (uint64, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return s.Update(ctx, key, raw, expected)
}

// Lease is a lock with an expiry that one holder at a time owns. The
// coordinator uses it to elect the instance that runs failover and backups.
//
// Expiry is judged by the local clock of whoever tries to take the lease over, so
// the clocks of coordinator instances need to agree to within a fraction of the TTL.
type Lease struct {
	store  Store
	key    string
	holder string
	ttl    time.Duration
	now    func() time.Time
}

type leaseRecord struct {
	Holder  string    `json:"holder"`
	Expires time.Time `json:"expires"`
}

// NewLease returns a lease on key for holder. The clock is injectable for tests
// and defaults to time.Now.
func NewLease(store Store, key, holder string, ttl time.Duration, now func() time.Time) *Lease {
	if now == nil {
		now = time.Now
	}
	return &Lease{store: store, key: key, holder: holder, ttl: ttl, now: now}
}

// Acquire takes the lease, or renews it when it is already held by this holder,
// and reports whether this holder owns the lease now. Call it more often than the
// TTL, e.g. every third of it.
func (l *Lease) Acquire(ctx context.Context) (bool, error) {
	mine := leaseRecord{Holder: l.holder, Expires: l.now().Add(l.ttl)}
	var current leaseRecord
	rev, err := GetJSON(ctx, l.store, l.key, &current)
	if errors.Is(err, ErrNotFound) {
		if _, err := CreateJSON(ctx, l.store, l.key, mine); err != nil {
			if errors.Is(err, ErrConflict) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if current.Holder != l.holder && l.now().Before(current.Expires) {
		return false, nil
	}
	if _, err := UpdateJSON(ctx, l.store, l.key, mine, rev); err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Release gives the lease up early, so another instance takes over at once. It
// does nothing when this holder does not own the lease.
func (l *Lease) Release(ctx context.Context) error {
	var current leaseRecord
	rev, err := GetJSON(ctx, l.store, l.key, &current)
	if errors.Is(err, ErrNotFound) || (err == nil && current.Holder != l.holder) {
		return nil
	}
	if err != nil {
		return err
	}
	current.Expires = time.Time{}
	if _, err := UpdateJSON(ctx, l.store, l.key, current, rev); err != nil && !errors.Is(err, ErrConflict) {
		return err
	}
	return nil
}
