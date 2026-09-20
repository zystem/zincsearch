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

// Package wire defines the messages of the replication stream. It depends on
// nothing but the standard library, so producers such as the coordinator can use
// it without pulling in the search engine.
package wire

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Kind tells what a stream message carries.
type Kind string

const (
	// KindDoc is a document that is created or replaced in an index.
	KindDoc Kind = "doc"
	// KindDocs is a batch of documents for one index, applied in order. It exists
	// so a producer can send many log lines in one message.
	KindDocs Kind = "docs"
	// KindAdmin is an administrative operation on an index.
	KindAdmin Kind = "admin"
)

// Op is an administrative operation.
type Op string

const (
	// OpCreateIndex creates an index. Data is the body of the create index API
	// (meta.IndexSimple). Creating an existing index is a no-op.
	OpCreateIndex Op = "create_index"
	// OpDeleteIndex deletes an index. Deleting a missing index is a no-op.
	OpDeleteIndex Op = "delete_index"
	// OpSetMapping adds field mappings to an index. Data is the body of the set
	// mapping API. Re-adding an identical field is a no-op.
	OpSetMapping Op = "set_mapping"
)

// Doc is one document of a KindDocs message.
type Doc struct {
	// ID names the document. When empty it is derived from the stream sequence
	// and the position in the batch, so a redelivered message replaces its copy.
	ID  string                 `json:"id,omitempty"`
	Doc map[string]interface{} `json:"doc"`
}

// Message is the envelope published to the stream.
type Message struct {
	Kind  Kind   `json:"kind"`
	Index string `json:"index"`

	// ID and Doc describe a KindDoc message. When ID is empty it is derived from
	// the stream sequence so a redelivered message replaces its earlier copy.
	ID  string                 `json:"id,omitempty"`
	Doc map[string]interface{} `json:"doc,omitempty"`

	// Docs describes a KindDocs message.
	Docs []Doc `json:"docs,omitempty"`

	// Op and Data describe a KindAdmin message.
	Op   Op              `json:"op,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// PermanentError marks a message that can never be applied, however often it is
// retried. The consumer skips such a message instead of blocking the stream.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent returns a PermanentError with a formatted reason.
func Permanent(format string, args ...interface{}) error {
	return &PermanentError{Err: fmt.Errorf(format, args...)}
}

// IsPermanent reports whether err marks a message that must be skipped.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// NewDoc builds a document message.
func NewDoc(index, id string, doc map[string]interface{}) *Message {
	return &Message{Kind: KindDoc, Index: index, ID: id, Doc: doc}
}

// NewDocs builds a message that carries several documents of one index.
func NewDocs(index string, docs []Doc) *Message {
	return &Message{Kind: KindDocs, Index: index, Docs: docs}
}

// NewAdmin builds an administrative message. data is marshalled to JSON.
func NewAdmin(index string, op Op, data interface{}) (*Message, error) {
	m := &Message{Kind: KindAdmin, Index: index, Op: op}
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		m.Data = raw
	}
	return m, nil
}

// Encode serialises the message for publishing.
func (m *Message) Encode() ([]byte, error) { return json.Marshal(m) }

// Decode parses a stream message body. A body that is not a valid message is a
// permanent error, because redelivery cannot fix it.
func Decode(data []byte) (*Message, error) {
	m := new(Message)
	if err := json.Unmarshal(data, m); err != nil {
		return nil, Permanent("decode message: %v", err)
	}
	switch m.Kind {
	case KindDoc:
		if m.Index == "" {
			return nil, Permanent("document message without index")
		}
		if m.Doc == nil {
			return nil, Permanent("document message without doc")
		}
	case KindDocs:
		if m.Index == "" {
			return nil, Permanent("docs message without index")
		}
		if len(m.Docs) == 0 {
			return nil, Permanent("docs message without documents")
		}
		for i, d := range m.Docs {
			if d.Doc == nil {
				return nil, Permanent("docs message: document %d is empty", i)
			}
		}
	case KindAdmin:
		if m.Index == "" {
			return nil, Permanent("admin message without index")
		}
		if m.Op == "" {
			return nil, Permanent("admin message without op")
		}
	default:
		return nil, Permanent("unknown message kind %q", m.Kind)
	}
	return m, nil
}
