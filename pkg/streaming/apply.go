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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/core"
	"github.com/zincsearch/zincsearch/pkg/handlers/index"
	"github.com/zincsearch/zincsearch/pkg/meta"
	zincanalysis "github.com/zincsearch/zincsearch/pkg/uquery/analysis"
	"github.com/zincsearch/zincsearch/pkg/uquery/mappings"
)

// Applier applies stream messages to a node.
type Applier interface {
	// Apply applies one message. seq is the stream sequence of the message.
	// Applying a message more than once must have the same effect as applying it
	// once, because delivery is at-least-once.
	Apply(seq uint64, m *Message) error
	// Flush makes everything applied since the previous Flush durable.
	Flush() error
}

// CoreApplier applies messages to the indexes of this process.
type CoreApplier struct {
	mu      sync.Mutex
	touched map[string]struct{}
}

// NewCoreApplier returns an Applier backed by pkg/core.
func NewCoreApplier() *CoreApplier {
	return &CoreApplier{touched: make(map[string]struct{})}
}

// Apply implements Applier.
func (a *CoreApplier) Apply(seq uint64, m *Message) error {
	switch m.Kind {
	case KindDoc:
		return a.applyDoc(seq, m)
	case KindDocs:
		return a.applyDocs(seq, m)
	case KindAdmin:
		return applyAdmin(m)
	default:
		return permanent("unknown message kind %q", m.Kind)
	}
}

// Flush syncs the WAL of every index that received documents, so an offset
// saved after Flush never points past data that a crash could still lose.
func (a *CoreApplier) Flush() error {
	a.mu.Lock()
	names := make([]string, 0, len(a.touched))
	for name := range a.touched {
		names = append(names, name)
	}
	a.touched = make(map[string]struct{})
	a.mu.Unlock()

	var errs []error
	for _, name := range names {
		idx, ok := core.GetIndex(name)
		if !ok {
			continue // deleted after the document arrived
		}
		if err := idx.SyncWAL(); err != nil {
			errs = append(errs, fmt.Errorf("sync wal of index %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func (a *CoreApplier) applyDoc(seq uint64, m *Message) error {
	if err := core.CheckIndexName(m.Index); err != nil {
		return &PermanentError{Err: err}
	}
	idx, _, err := core.GetOrCreateIndex(m.Index, "", 0)
	if err != nil {
		return err
	}
	id := m.ID
	if id == "" {
		id = fmt.Sprintf("seq-%d", seq)
	}
	// update=true replaces a document that already carries this ID, which makes
	// a redelivered message harmless.
	if err := idx.CreateDocument(id, m.Doc, true); err != nil {
		return classifyDocError(err)
	}
	a.mu.Lock()
	a.touched[m.Index] = struct{}{}
	a.mu.Unlock()
	return nil
}

func (a *CoreApplier) applyDocs(seq uint64, m *Message) error {
	if err := core.CheckIndexName(m.Index); err != nil {
		return &PermanentError{Err: err}
	}
	idx, _, err := core.GetOrCreateIndex(m.Index, "", 0)
	if err != nil {
		return err
	}
	// A batch is applied as a whole again after a failure in the middle: every
	// document is replaced by ID, so the ones already written do no harm.
	for i, d := range m.Docs {
		id := d.ID
		if id == "" {
			id = fmt.Sprintf("seq-%d-%d", seq, i)
		}
		if err := idx.CreateDocument(id, d.Doc, true); err != nil {
			var invalid *core.InvalidDocumentError
			if errors.As(err, &invalid) {
				// Only this document is bad; the others of the batch still apply.
				log.Warn().Err(err).Str("index", m.Index).Uint64("seq", seq).Int("pos", i).Msg("streaming: skipping invalid document")
				continue
			}
			return err
		}
	}
	a.mu.Lock()
	a.touched[m.Index] = struct{}{}
	a.mu.Unlock()
	return nil
}

// classifyDocError marks content errors permanent so they are skipped instead
// of retried forever; storage errors stay transient.
func classifyDocError(err error) error {
	var invalid *core.InvalidDocumentError
	if errors.As(err, &invalid) {
		return &PermanentError{Err: err}
	}
	return err
}

func applyAdmin(m *Message) error {
	switch m.Op {
	case OpCreateIndex:
		return applyCreateIndex(m)
	case OpDeleteIndex:
		return applyDeleteIndex(m)
	case OpSetMapping:
		return applySetMapping(m)
	default:
		return permanent("unknown admin op %q", m.Op)
	}
}

func applyCreateIndex(m *Message) error {
	if err := core.CheckIndexName(m.Index); err != nil {
		return &PermanentError{Err: err}
	}
	if _, ok := core.GetIndex(m.Index); ok {
		return nil
	}
	simple := new(meta.IndexSimple)
	if len(m.Data) > 0 {
		if err := json.Unmarshal(m.Data, simple); err != nil {
			return permanent("create_index data: %v", err)
		}
	}
	// The envelope names the index; a body that names another one would make the
	// existence check above look at a different index than the one created.
	if simple.Name != "" && simple.Name != m.Index {
		return permanent("create_index: index %q does not match data.name %q", m.Index, simple.Name)
	}
	// Reject input that the worker would refuse, so a bad message is skipped and
	// only genuine I/O failures are retried.
	settings := simple.Settings
	if settings == nil {
		settings = new(meta.IndexSettings)
	}
	analyzers, err := zincanalysis.RequestAnalyzer(settings.Analysis)
	if err != nil {
		return &PermanentError{Err: err}
	}
	if _, err := mappings.Request(analyzers, simple.Mappings); err != nil {
		return &PermanentError{Err: err}
	}
	return index.CreateIndexWorker(simple, m.Index)
}

func applyDeleteIndex(m *Message) error {
	if _, ok := core.GetIndex(m.Index); !ok {
		return nil
	}
	return core.DeleteIndex(m.Index)
}

func applySetMapping(m *Message) error {
	if err := core.CheckIndexName(m.Index); err != nil {
		return &PermanentError{Err: err}
	}
	var req map[string]interface{}
	if err := json.Unmarshal(m.Data, &req); err != nil {
		return permanent("set_mapping data: %v", err)
	}
	requested, err := mappings.Request(nil, req)
	if err != nil {
		return &PermanentError{Err: err}
	}

	idx, exists, err := core.GetOrCreateIndex(m.Index, "", 0)
	if err != nil {
		return err
	}

	// Same rules as the set mapping API, except that repeating a field with an
	// identical definition succeeds, so the operation can be redelivered.
	if exists {
		current := idx.GetMappings()
		if current != nil && current.Len() > 0 {
			changed := false
			for field, prop := range requested.ListProperty() {
				if old, ok := current.GetProperty(field); ok {
					if !reflect.DeepEqual(old, prop) {
						return permanent("index [%s] already maps field [%s] differently", m.Index, field)
					}
					continue
				}
				current.SetProperty(field, prop)
				changed = true
			}
			if !changed {
				return nil
			}
			requested = current
		}
	}

	for k, v := range requested.Properties {
		if v.Fields == nil {
			continue
		}
		update := false
		for kField, field := range v.Fields {
			if field.Fields != nil {
				field.Fields = nil
				v.Fields[kField] = field
				update = true
			}
		}
		if update {
			requested.Properties[k] = v
		}
	}
	_ = idx.SetMappings(requested)
	return core.StoreIndex(idx)
}
