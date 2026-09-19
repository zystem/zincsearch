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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// BackupShards copies a consistent snapshot of every second-level shard of the
// index below dst, in the layout of the data directory: dst/<shard>/<second>.
//
// Every shard is copied as one consistent snapshot, but the shards are copied
// one after another, so the result is only consistent across shards when no
// document is applied meanwhile. The caller has to make sure of that, e.g. by
// pausing the stream consumer and waiting until WALPending is zero.
func (index *Index) BackupShards(ctx context.Context, dst string) error {
	if index.GetStorageType() != "disk" {
		return fmt.Errorf("index %s: only disk indexes can be backed up, storage type is %q", index.GetName(), index.GetStorageType())
	}
	ids := make([]string, 0, len(index.shards))
	for id := range index.shards {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		shard := index.shards[id]
		for second := int64(0); second < shard.GetShardNum(); second++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			target := filepath.Join(dst, id, fmt.Sprintf("%06x", second))
			if err := shard.backupSecondShard(ctx, second, target); err != nil {
				return fmt.Errorf("index %s shard %s/%06x: %w", index.GetName(), id, second, err)
			}
		}
	}
	return nil
}

func (s *IndexShard) backupSecondShard(ctx context.Context, second int64, dst string) error {
	writer, err := s.GetWriter(second)
	if err != nil {
		return err
	}
	// The reader pins the segments it needs, so a background merge cannot remove
	// a file while it is copied.
	reader, err := writer.Reader()
	if err != nil {
		return err
	}
	defer reader.Close()
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return err
	}
	cancel := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { close(cancel) })
	defer stop()
	if err := reader.Backup(dst, cancel); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}
