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

// Package streaming replicates a ZincSearch node from a NATS JetStream stream.
//
// Every message of the stream is either a document or an administrative
// operation. All nodes consume the same stream in the same order, so nodes
// never talk to each other to stay identical.
//
// The message format itself lives in package wire.
package streaming

import "github.com/zincsearch/zincsearch/pkg/streaming/wire"

// The message types are defined in package wire; they are repeated here so the
// engine side of the code reads naturally.
type (
	Kind           = wire.Kind
	Op             = wire.Op
	Message        = wire.Message
	Doc            = wire.Doc
	PermanentError = wire.PermanentError
)

const (
	KindDoc       = wire.KindDoc
	KindDocs      = wire.KindDocs
	KindAdmin     = wire.KindAdmin
	OpCreateIndex = wire.OpCreateIndex
	OpDeleteIndex = wire.OpDeleteIndex
	OpSetMapping  = wire.OpSetMapping
)

var (
	NewDoc      = wire.NewDoc
	NewDocs     = wire.NewDocs
	NewAdmin    = wire.NewAdmin
	Decode      = wire.Decode
	IsPermanent = wire.IsPermanent
)

func permanent(format string, args ...interface{}) error { return wire.Permanent(format, args...) }
