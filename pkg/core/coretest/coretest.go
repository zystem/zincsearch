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

// Package coretest holds test helpers that need the core package, so they
// cannot live in test/utils (core's own dependencies import that package).
package coretest

import (
	"testing"
	"time"

	"github.com/zincsearch/zincsearch/pkg/core"
)

// WaitWAL blocks until every document accepted by the index is applied, which
// is when it becomes searchable. It replaces sleeping for the WAL interval.
func WaitWAL(t testing.TB, index *core.Index) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		pending, err := index.WALPending()
		if err == nil && pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("index %s still has %d pending WAL entries (err %v)", index.GetName(), pending, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WaitWALByName is WaitWAL for an index that is looked up by name.
func WaitWALByName(t testing.TB, name string) {
	t.Helper()
	index, ok := core.GetIndex(name)
	if !ok {
		t.Fatalf("index %s does not exist", name)
	}
	WaitWAL(t, index)
}
