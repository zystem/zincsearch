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

package metadata

import (
	"errors"

	"github.com/zincsearch/zincsearch/pkg/metadata/storage"
)

// ErrDumpUnsupported is returned when the configured metadata storage cannot
// list its keys, which a backup needs. The etcd storage of cluster mode is one.
var ErrDumpUnsupported = errors.New("the metadata storage does not support backup")

// Dump returns every metadata entry (indexes, users, roles, templates, aliases
// and key/value entries) keyed by its full storage key.
func Dump() (map[string][]byte, error) {
	d, ok := db.(storage.Dumper)
	if !ok {
		return nil, ErrDumpUnsupported
	}
	return d.Dump()
}

// Load makes the metadata equal to entries: keys that are not in entries are
// deleted and the others are written. It is meant for restoring a backup into a
// node that has not started serving yet.
func Load(entries map[string][]byte) error {
	existing, err := Dump()
	if err != nil {
		return err
	}
	for key := range existing {
		if _, keep := entries[key]; !keep {
			if err := db.Delete(key); err != nil {
				return err
			}
		}
	}
	for key, value := range entries {
		if err := db.Set(key, value); err != nil {
			return err
		}
	}
	return nil
}
