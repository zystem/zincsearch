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

package core

import (
	"errors"

	"github.com/zincsearch/zincsearch/pkg/wal/redo"
)

// SyncWAL flushes the write-ahead log of every opened shard to disk, so every
// document accepted so far survives a crash. Shards whose WAL was never opened
// have nothing to flush.
func (index *Index) SyncWAL() error {
	var errs []error
	for _, shard := range index.shards {
		shard.lock.RLock()
		w := shard.wal
		shard.lock.RUnlock()
		if w == nil {
			continue
		}
		if err := w.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// WALPending returns how many WAL entries were accepted but are not yet applied
// to the shard indexes. It is zero when every accepted document is searchable
// and no shard file is going to change until the next write.
func (index *Index) WALPending() (uint64, error) {
	var pending uint64
	for _, shard := range index.shards {
		if err := shard.OpenWAL(); err != nil {
			return 0, err
		}
		last, err := shard.wal.LastIndex()
		if err != nil {
			return 0, err
		}
		// The redo log holds the last committed entry; it is absent until the
		// first entry of the shard was applied. The WAL consumer writes it from
		// another goroutine, so it must be read as a copy.
		var committed uint64
		raw, err := shard.wal.Redo.ReadCopy(RedoActionWrite)
		if err == nil {
			_, committed, err = parseRedoLog(raw)
		}
		if err != nil && !errors.Is(err, redo.ErrNotFound) {
			return 0, err
		}
		if last > committed {
			pending += last - committed
		}
	}
	return pending, nil
}

// DocCount returns the number of live documents of the index, counted in the
// shards themselves, so it is exact whatever the size of the index.
func (index *Index) DocCount() (uint64, error) {
	var total uint64
	for _, shard := range index.shards {
		for second := int64(0); second < shard.GetShardNum(); second++ {
			writer, err := shard.GetWriter(second)
			if err != nil {
				return 0, err
			}
			reader, err := writer.Reader()
			if err != nil {
				return 0, err
			}
			n, err := reader.Count()
			_ = reader.Close()
			if err != nil {
				return 0, err
			}
			total += n
		}
	}
	return total, nil
}
