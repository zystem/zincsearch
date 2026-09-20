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

package blob_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zincsearch/zincsearch/pkg/coordinator/blob"
)

func read(t *testing.T, s blob.Store, name string) []byte {
	t.Helper()
	r, err := s.Get(context.Background(), name)
	require.NoError(t, err)
	defer r.Close()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

// run is what every blob.Store has to do. multipart adds the uploads that a real
// S3 server needs several requests for: objects of unknown size and large ones.
func run(t *testing.T, s blob.Store, multipart bool) {
	ctx := context.Background()

	t.Run("put, get and replace", func(t *testing.T) {
		require.NoError(t, s.Put(ctx, "a/one.tgz", strings.NewReader("first"), 5))
		assert.Equal(t, []byte("first"), read(t, s, "a/one.tgz"))
		require.NoError(t, s.Put(ctx, "a/one.tgz", strings.NewReader("second!"), 7))
		assert.Equal(t, []byte("second!"), read(t, s, "a/one.tgz"))
	})

	t.Run("unknown size and a large object", func(t *testing.T) {
		if !multipart {
			t.Skip("this server cannot take streaming uploads")
		}
		big := make([]byte, 17<<20) // several parts of a multipart upload
		_, err := rand.Read(big)
		require.NoError(t, err)
		require.NoError(t, s.Put(ctx, "big.bin", bytes.NewReader(big), -1))
		assert.True(t, bytes.Equal(big, read(t, s, "big.bin")))
	})

	t.Run("a missing object", func(t *testing.T) {
		_, err := s.Get(ctx, "nothing/here")
		assert.ErrorIs(t, err, blob.ErrNotFound)
		assert.NoError(t, s.Delete(ctx, "nothing/here"), "deleting a missing object is fine")
	})

	t.Run("list and delete", func(t *testing.T) {
		for _, name := range []string{"l/b", "l/a", "l/c", "other"} {
			require.NoError(t, s.Put(ctx, name, strings.NewReader(name), int64(len(name))))
		}
		infos, err := s.List(ctx, "l/")
		require.NoError(t, err)
		require.Len(t, infos, 3)
		assert.Equal(t, []string{"l/a", "l/b", "l/c"}, []string{infos[0].Name, infos[1].Name, infos[2].Name})
		assert.Equal(t, int64(3), infos[0].Size)

		require.NoError(t, s.Delete(ctx, "l/b"))
		infos, err = s.List(ctx, "l/")
		require.NoError(t, err)
		assert.Len(t, infos, 2)
		_, err = s.Get(ctx, "l/b")
		assert.ErrorIs(t, err, blob.ErrNotFound)
	})

	t.Run("names that could leave the store", func(t *testing.T) {
		for _, name := range []string{"", "../x", "a/../../x", "/abs", "a//b", ".hidden", "a b"} {
			assert.ErrorIs(t, s.Put(ctx, name, strings.NewReader("x"), 1), blob.ErrInvalidName, "%q", name)
			_, err := s.Get(ctx, name)
			assert.ErrorIs(t, err, blob.ErrInvalidName, "%q", name)
			assert.ErrorIs(t, s.Delete(ctx, name), blob.ErrInvalidName, "%q", name)
		}
	})

	t.Run("a cancelled upload stores nothing", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		err := s.Put(cctx, "cancelled.bin", strings.NewReader("data"), 4)
		assert.Error(t, err)
		_, err = s.Get(ctx, "cancelled.bin")
		assert.ErrorIs(t, err, blob.ErrNotFound)
	})
}

func TestFS(t *testing.T) {
	s, err := blob.NewFS(t.TempDir())
	require.NoError(t, err)
	run(t, s, true)
}

func TestS3(t *testing.T) {
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket("backups"))
	ts := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(ts.Close)

	s, err := blob.NewS3(context.Background(), blob.S3Config{
		Endpoint:  strings.TrimPrefix(ts.URL, "http://"),
		Bucket:    "backups",
		Prefix:    "zincsearch/",
		AccessKey: "key", SecretKey: "secret",
		PathStyle: true,
	})
	require.NoError(t, err)
	// gofakes3 stores the body of uploads with a streaming signature (objects of
	// unknown size, multipart parts) still wrapped in its aws-chunked framing.
	// Real S3 servers unwrap it, see TestS3RealServer.
	run(t, s, false)
}

// TestS3RealServer runs the whole suite against a real S3 compatible service, e.g.
//
//	ZINC_TEST_S3_ENDPOINT=127.0.0.1:3900 ZINC_TEST_S3_BUCKET=backups \
//	ZINC_TEST_S3_KEY=... ZINC_TEST_S3_SECRET=... go test ./pkg/coordinator/blob -run RealServer
//
// Set ZINC_TEST_S3_SECURE=true for https, and ZINC_TEST_S3_REGION when the server
// answers the region lookup badly (S3 Ninja: us-east-1). The bucket has to exist; the test only
// touches objects below the prefix "zinc-test/".
func TestS3RealServer(t *testing.T) {
	endpoint := os.Getenv("ZINC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ZINC_TEST_S3_ENDPOINT, ZINC_TEST_S3_BUCKET, ZINC_TEST_S3_KEY and ZINC_TEST_S3_SECRET to run")
	}
	s, err := blob.NewS3(context.Background(), blob.S3Config{
		Endpoint:  endpoint,
		Bucket:    os.Getenv("ZINC_TEST_S3_BUCKET"),
		Prefix:    "zinc-test/",
		Region:    os.Getenv("ZINC_TEST_S3_REGION"),
		AccessKey: os.Getenv("ZINC_TEST_S3_KEY"),
		SecretKey: os.Getenv("ZINC_TEST_S3_SECRET"),
		Secure:    os.Getenv("ZINC_TEST_S3_SECURE") == "true",
		PathStyle: true,
	})
	require.NoError(t, err)
	run(t, s, true)
}

func TestS3NeedsAnExistingBucket(t *testing.T) {
	ts := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(ts.Close)
	_, err := blob.NewS3(context.Background(), blob.S3Config{
		Endpoint: strings.TrimPrefix(ts.URL, "http://"), Bucket: "missing", AccessKey: "k", SecretKey: "s", PathStyle: true,
	})
	require.Error(t, err)
}
