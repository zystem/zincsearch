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

package streaming

import (
	"errors"
	"fmt"
	"strconv"

	zerrors "github.com/zincsearch/zincsearch/pkg/errors"
	"github.com/zincsearch/zincsearch/pkg/metadata"
)

// OffsetStore keeps the sequence of the last stream message that is applied and
// durable on this node. It is the single source of truth about how far the
// node's data reaches, so a node restored from a backup resumes exactly where
// the backup ended.
type OffsetStore interface {
	// Load returns the saved sequence, or 0 when nothing was applied yet.
	Load() (uint64, error)
	// Save records that every message up to and including seq is applied.
	Save(seq uint64) error
}

// OffsetKey is the metadata key that holds the offset. Because it lives in the
// node metadata, it travels with every metadata backup.
const OffsetKey = "stream_offset"

// KVOffsetStore keeps the offset in the node metadata.
type KVOffsetStore struct{}

// Load implements OffsetStore.
func (KVOffsetStore) Load() (uint64, error) {
	raw, err := metadata.KV.Get(OffsetKey)
	if errors.Is(err, zerrors.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	seq, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid saved stream offset %q: %w", raw, err)
	}
	return seq, nil
}

// Save implements OffsetStore.
func (KVOffsetStore) Save(seq uint64) error {
	return metadata.KV.Set(OffsetKey, []byte(strconv.FormatUint(seq, 10)))
}
