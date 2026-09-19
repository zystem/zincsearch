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

package wire

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageRoundTrip(t *testing.T) {
	doc := NewDoc("logs", "id-1", map[string]interface{}{"line": "hello"})
	raw, err := doc.Encode()
	require.NoError(t, err)
	got, err := Decode(raw)
	require.NoError(t, err)
	assert.Equal(t, KindDoc, got.Kind)
	assert.Equal(t, "logs", got.Index)
	assert.Equal(t, "id-1", got.ID)
	assert.Equal(t, "hello", got.Doc["line"])

	admin, err := NewAdmin("logs", OpSetMapping, map[string]interface{}{"properties": map[string]interface{}{}})
	require.NoError(t, err)
	raw, err = admin.Encode()
	require.NoError(t, err)
	got, err = Decode(raw)
	require.NoError(t, err)
	assert.Equal(t, KindAdmin, got.Kind)
	assert.Equal(t, OpSetMapping, got.Op)
	assert.JSONEq(t, `{"properties":{}}`, string(got.Data))
}

func TestDecodeRejectsInvalidMessages(t *testing.T) {
	for name, body := range map[string]string{
		"not json":          `not json`,
		"unknown kind":      `{"kind":"other","index":"a"}`,
		"doc without index": `{"kind":"doc","doc":{"a":1}}`,
		"doc without doc":   `{"kind":"doc","index":"a"}`,
		"admin without op":  `{"kind":"admin","index":"a"}`,
		"admin no index":    `{"kind":"admin","op":"delete_index"}`,
		"docs no index":     `{"kind":"docs","docs":[{"doc":{"a":1}}]}`,
		"docs empty":        `{"kind":"docs","index":"a","docs":[]}`,
		"docs with a null":  `{"kind":"docs","index":"a","docs":[{"id":"x"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Decode([]byte(body))
			require.Error(t, err)
			assert.True(t, IsPermanent(err), "redelivery cannot fix %q", name)
		})
	}
}

func TestDocsRoundTrip(t *testing.T) {
	m := NewDocs("logs", []Doc{
		{ID: "a", Doc: map[string]interface{}{"line": "one"}},
		{Doc: map[string]interface{}{"line": "two"}},
	})
	raw, err := m.Encode()
	require.NoError(t, err)
	got, err := Decode(raw)
	require.NoError(t, err)
	assert.Equal(t, KindDocs, got.Kind)
	require.Len(t, got.Docs, 2)
	assert.Equal(t, "a", got.Docs[0].ID)
	assert.Equal(t, "two", got.Docs[1].Doc["line"])
}
