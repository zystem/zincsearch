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

package blob

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config configures an S3 store.
type S3Config struct {
	// Endpoint is host:port of the S3 service, e.g. "s3.amazonaws.com" or "garage:3900".
	Endpoint string
	Bucket   string
	// Prefix is put in front of every object name, e.g. "zincsearch/backups/".
	Prefix    string
	AccessKey string
	SecretKey string
	Region    string
	// Secure selects https.
	Secure bool
	// PathStyle addresses buckets as endpoint/bucket instead of bucket.endpoint,
	// which most self-hosted S3 services (MinIO, Garage) need.
	PathStyle bool
}

// S3 is a Store on an S3 compatible bucket.
type S3 struct {
	client *minio.Client
	bucket string
	prefix string
}

// NewS3 connects to the bucket, which has to exist.
func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	lookup := minio.BucketLookupAuto
	if cfg.PathStyle {
		lookup = minio.BucketLookupPath
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       cfg.Secure,
		Region:       cfg.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("blob: bucket " + cfg.Bucket + " does not exist")
	}
	return &S3{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

func (s *S3) key(name string) string { return s.prefix + name }

func (s *S3) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if !ValidName(name) {
		return ErrInvalidName
	}
	_, err := s.client.PutObject(ctx, s.bucket, s.key(name), r, size, minio.PutObjectOptions{ContentType: "application/gzip"})
	return err
}

func (s *S3) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if !ValidName(name) {
		return nil, ErrInvalidName
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(name), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	// GetObject is lazy; Stat is what notices a missing object.
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return obj, nil
}

func (s *S3) List(ctx context.Context, prefix string) ([]Info, error) {
	var infos []Info
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.key(prefix), Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		infos = append(infos, Info{Name: strings.TrimPrefix(obj.Key, s.prefix), Size: obj.Size, ModTime: obj.LastModified})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

func (s *S3) Delete(ctx context.Context, name string) error {
	if !ValidName(name) {
		return ErrInvalidName
	}
	return s.client.RemoveObject(ctx, s.bucket, s.key(name), minio.RemoveObjectOptions{})
}
