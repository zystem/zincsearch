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
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zincsearch/zincsearch/pkg/config"
	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/meta"
)

var current atomic.Pointer[Consumer]

// Current returns the consumer of this process, or nil when streaming is off.
func Current() *Consumer { return current.Load() }

// StartFromConfig starts the consumer described by the zinc_stream_* settings
// and publishes its state on /healthz.
func StartFromConfig() (*Consumer, error) {
	sc := config.Global.Stream
	c, err := New(Config{
		URL:      sc.URL,
		Stream:   sc.Name,
		Subject:  sc.Subject,
		Consumer: sc.Consumer,
		Batch:    sc.Batch,
		AllowGap: sc.AllowGap,
	}, NewCoreApplier(), KVOffsetStore{})
	if err != nil {
		return nil, fmt.Errorf("zinc_stream_url, zinc_stream_name and zinc_stream_consumer are required when zinc_stream_enable is true: %w", err)
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	current.Store(c)
	meta.RegisterHealthDetail("stream", func() interface{} { return c.Status() })
	return c, nil
}

// Shutdown stops the consumer of this process, if any. The batch in flight is
// made durable first.
func Shutdown() {
	if c := current.Swap(nil); c != nil {
		c.Stop()
	}
}

// PauseAndDrain pauses the consumer and waits until every document it accepted
// is applied to the indexes. When it returns, the data files of this node do
// not change until Resume, so they can be copied as they are.
func (c *Consumer) PauseAndDrain(ctx context.Context) error {
	if err := c.Pause(ctx); err != nil {
		return err
	}
	for {
		pending, err := walPending()
		if err == nil && pending == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return fmt.Errorf("waiting for the WAL to drain: %w", errors.Join(ctx.Err(), err))
			}
			return fmt.Errorf("waiting for the WAL to drain, %d entries left: %w", pending, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// WALPending returns how many accepted documents are not applied yet, over all
// indexes of this process.
func WALPending() (uint64, error) { return walPending() }

func walPending() (uint64, error) {
	var total uint64
	for _, idx := range core.ZINC_INDEX_LIST.List() {
		n, err := idx.WALPending()
		if err != nil {
			return 0, fmt.Errorf("index %s: %w", idx.GetName(), err)
		}
		total += n
	}
	return total, nil
}
