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

// Command zinccoordinator runs the coordinator of a replicated ZincSearch
// cluster: it accepts logs over HTTP and publishes them to the replication
// stream, watches the nodes, promotes a replica when the master fails, backs a
// replica up on a schedule and tells the CI system whether to start builds.
//
// Everything is configured through environment variables, see docs/coordinator.md.
//
//	zinccoordinator                         run the coordinator
//	zinccoordinator list-backups            list the stored backups
//	zinccoordinator fetch-backup -out FILE  download the newest good backup (or -name NAME)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docker/go-units"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/zincsearch/zincsearch/pkg/coordinator"
	"github.com/zincsearch/zincsearch/pkg/coordinator/blob"
	"github.com/zincsearch/zincsearch/pkg/coordinator/buffer"
	"github.com/zincsearch/zincsearch/pkg/coordinator/state"
)

func main() {
	zerolog.TimeFieldFormat = time.RFC3339
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "list-backups":
		err = listBackups()
	case len(os.Args) > 1 && os.Args[1] == "fetch-backup":
		err = fetchBackup(os.Args[2:])
	case len(os.Args) > 1 && os.Args[1] == "help", len(os.Args) > 1 && os.Args[1] == "-h":
		fmt.Fprintln(os.Stderr, "usage: zinccoordinator [list-backups | fetch-backup -out FILE [-name NAME]]\nsee docs/coordinator.md for the environment variables")
		return
	default:
		err = run()
	}
	if err != nil {
		log.Fatal().Err(err).Msg("zinccoordinator")
	}
}

// ---- environment ----

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envDuration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return d, nil
}

func envInt(name string, def int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}

func envBytes(name string, def int64) (int64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := units.RAMInBytes(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}

func envBool(name string) bool { return strings.EqualFold(os.Getenv(name), "true") }

// retry runs fn until it succeeds or ctx ends. The coordinator has to start
// even when NATS or rqlite are not up yet.
func retry(ctx context.Context, what string, fn func(context.Context) error) error {
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		log.Warn().Err(err).Msgf("waiting for %s", what)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, err)
		case <-time.After(2 * time.Second):
		}
	}
}

// ---- the parts ----

// connectNATS connects when NATS is needed: for the state, the publisher, or both.
func connectNATS() (*nats.Conn, error) {
	url := os.Getenv("COORD_NATS_URL")
	if url == "" {
		return nil, nil
	}
	var opts []nats.Option
	if creds := os.Getenv("COORD_NATS_CREDS"); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	if user := os.Getenv("COORD_NATS_USER"); user != "" {
		opts = append(opts, nats.UserInfo(user, os.Getenv("COORD_NATS_PASSWORD")))
	}
	return coordinator.ConnectNATS(url, "zinccoordinator", opts...)
}

func openState(ctx context.Context, nc *nats.Conn) (state.Store, error) {
	switch kind := env("COORD_STATE", "natskv"); kind {
	case "natskv":
		if nc == nil {
			return nil, errors.New("COORD_STATE=natskv needs COORD_NATS_URL")
		}
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, err
		}
		replicas, err := envInt("COORD_STATE_REPLICAS", 1)
		if err != nil {
			return nil, err
		}
		var s *state.NATSKV
		err = retry(ctx, "the NATS key/value bucket", func(ctx context.Context) error {
			var err error
			s, err = state.OpenNATSKV(ctx, js, env("COORD_STATE_BUCKET", "zinc_coordinator"), replicas)
			return err
		})
		return s, err
	case "rqlite":
		url := os.Getenv("COORD_RQLITE_URL")
		if url == "" {
			return nil, errors.New("COORD_STATE=rqlite needs COORD_RQLITE_URL")
		}
		var s *state.RQLite
		err := retry(ctx, "rqlite", func(ctx context.Context) error {
			var err error
			s, err = state.OpenRQLite(ctx, url)
			return err
		})
		return s, err
	case "memory":
		log.Warn().Msg("COORD_STATE=memory keeps nothing across restarts and cannot be shared: for tests only")
		return state.NewMem(), nil
	default:
		return nil, fmt.Errorf("COORD_STATE must be natskv, rqlite or memory, not %q", kind)
	}
}

func openBlobs(ctx context.Context) (blob.Store, error) {
	switch kind := env("COORD_BLOB", "none"); kind {
	case "none":
		return nil, nil
	case "fs":
		path := os.Getenv("COORD_FS_PATH")
		if path == "" {
			return nil, errors.New("COORD_BLOB=fs needs COORD_FS_PATH")
		}
		return blob.NewFS(path)
	case "s3":
		cfg := blob.S3Config{
			Endpoint:  os.Getenv("COORD_S3_ENDPOINT"),
			Bucket:    os.Getenv("COORD_S3_BUCKET"),
			Prefix:    os.Getenv("COORD_S3_PREFIX"),
			AccessKey: os.Getenv("COORD_S3_KEY"),
			SecretKey: os.Getenv("COORD_S3_SECRET"),
			Region:    os.Getenv("COORD_S3_REGION"),
			Secure:    envBool("COORD_S3_SECURE"),
			PathStyle: envBool("COORD_S3_PATH_STYLE"),
		}
		if cfg.Endpoint == "" || cfg.Bucket == "" {
			return nil, errors.New("COORD_BLOB=s3 needs COORD_S3_ENDPOINT and COORD_S3_BUCKET")
		}
		var s *blob.S3
		err := retry(ctx, "the S3 bucket", func(ctx context.Context) error {
			var err error
			s, err = blob.NewS3(ctx, cfg)
			return err
		})
		return s, err
	default:
		return nil, fmt.Errorf("COORD_BLOB must be s3, fs or none, not %q", kind)
	}
}

func parseNodes() ([]coordinator.NodeConfig, error) {
	raw := os.Getenv("COORD_NODES")
	if raw == "" {
		return nil, errors.New("COORD_NODES is required, e.g. a=http://zinc-a:4080,b=http://zinc-b:4080")
	}
	var nodes []coordinator.NodeConfig
	for _, part := range strings.Split(raw, ",") {
		name, url, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" || url == "" {
			return nil, fmt.Errorf("COORD_NODES entry %q is not name=url", part)
		}
		nodes = append(nodes, coordinator.NodeConfig{
			Name: name, URL: url,
			User: env("COORD_NODE_USER", "admin"), Password: os.Getenv("COORD_NODE_PASSWORD"),
		})
	}
	return nodes, nil
}

func ensureStream(ctx context.Context, nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	name := env("COORD_STREAM", "zinc")
	maxAge, err := envDuration("COORD_STREAM_MAX_AGE", 2*time.Hour)
	if err != nil {
		return err
	}
	replicas, err := envInt("COORD_STREAM_REPLICAS", 1)
	if err != nil {
		return err
	}
	return retry(ctx, "the replication stream", func(ctx context.Context) error {
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     name,
			Subjects: []string{name + ".>"},
			Storage:  jetstream.FileStorage,
			Replicas: replicas,
			MaxAge:   maxAge,
			// A publisher that repeats a message after a lost acknowledgement must
			// not be able to add it twice, even when it repeats it much later.
			Duplicates: time.Hour,
		})
		return err
	})
}

// ---- run ----

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nc, err := connectNATS()
	if err != nil {
		return err
	}
	if nc != nil {
		defer nc.Close()
	}
	if nc != nil && envBool("COORD_ENSURE_STREAM") {
		if err := ensureStream(ctx, nc); err != nil {
			return err
		}
	}
	st, err := openState(ctx, nc)
	if err != nil {
		return err
	}
	defer st.Close()
	blobs, err := openBlobs(ctx)
	if err != nil {
		return err
	}
	nodes, err := parseNodes()
	if err != nil {
		return err
	}

	var pub *coordinator.Publisher
	if nc != nil {
		bufMax, err := envBytes("COORD_BUFFER_MAX", 1<<30)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(env("COORD_BUFFER_DIR", "/var/lib/zinc-coordinator"), 0o700); err != nil {
			return err
		}
		buf, err := buffer.Open(env("COORD_BUFFER_DIR", "/var/lib/zinc-coordinator")+"/buffer.db", uint64(bufMax))
		if err != nil {
			return err
		}
		defer buf.Close()
		pub, err = coordinator.NewPublisher(nc, coordinator.PublisherConfig{Subject: env("COORD_STREAM", "zinc") + "." + env("COORD_SUBJECT", "logs")}, buf)
		if err != nil {
			return err
		}
		defer pub.Close()
	}

	cfg := coordinator.Config{ID: os.Getenv("COORD_ID"), Nodes: nodes}
	if cfg.PollInterval, err = envDuration("COORD_POLL_INTERVAL", 0); err != nil {
		return err
	}
	if cfg.PromoteCooldown, err = envDuration("COORD_PROMOTE_COOLDOWN", 0); err != nil {
		return err
	}
	if cfg.BackupInterval, err = envDuration("COORD_BACKUP_INTERVAL", 0); err != nil {
		return err
	}
	if cfg.VerifyInterval, err = envDuration("COORD_VERIFY_INTERVAL", 0); err != nil {
		return err
	}
	if cfg.DrainTimeout, err = envDuration("COORD_DRAIN_TIMEOUT", 0); err != nil {
		return err
	}
	if cfg.FailThreshold, err = envInt("COORD_FAIL_THRESHOLD", 0); err != nil {
		return err
	}
	if cfg.OrphanGrace, err = envDuration("COORD_ORPHAN_GRACE", 0); err != nil {
		return err
	}
	if cfg.BackupRetention, err = envInt("COORD_BACKUP_RETENTION", 0); err != nil {
		return err
	}
	maxLag, err := envInt("COORD_MAX_LAG", 0)
	if err != nil {
		return err
	}
	cfg.MaxLag = uint64(maxLag)

	var verifier coordinator.Verifier
	if bin := os.Getenv("COORD_ZINC_BIN"); bin != "" {
		verifier = coordinator.ExecVerifier{Binary: bin, WorkDir: os.Getenv("COORD_WORK_DIR")}
	}
	c, err := coordinator.New(cfg, coordinator.Deps{State: st, Blobs: blobs, Publisher: pub, Verifier: verifier})
	if err != nil {
		return err
	}

	maxBody, err := envBytes("COORD_MAX_BODY", 32<<20)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              env("COORD_LISTEN", ":8080"),
		Handler:           c.Handler(coordinator.APIConfig{Token: os.Getenv("COORD_TOKEN"), MaxBody: maxBody}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	if os.Getenv("COORD_TOKEN") == "" {
		log.Warn().Msg("COORD_TOKEN is not set: the API is open to everybody who can reach it")
	}

	errc := make(chan error, 2)
	go func() { errc <- c.Run(ctx) }()
	go func() {
		log.Info().Str("listen", srv.Addr).Int("nodes", len(nodes)).Msg("coordinator started")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errc:
		stop()
		return err
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	log.Info().Msg("coordinator stopping")
	return srv.Shutdown(shutdown)
}

// ---- helper commands ----

func stateAndBlobs(ctx context.Context) (state.Store, blob.Store, func(), error) {
	nc, err := connectNATS()
	if err != nil {
		return nil, nil, nil, err
	}
	st, err := openState(ctx, nc)
	if err != nil {
		return nil, nil, nil, err
	}
	blobs, err := openBlobs(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	if blobs == nil {
		return nil, nil, nil, errors.New("COORD_BLOB is not configured")
	}
	closeAll := func() {
		_ = st.Close()
		if nc != nil {
			nc.Close()
		}
	}
	return st, blobs, closeAll, nil
}

func listBackups() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, _, closeAll, err := stateAndBlobs(ctx)
	if err != nil {
		return err
	}
	defer closeAll()
	recs, err := coordinator.ListBackups(ctx, st)
	if err != nil {
		return err
	}
	for _, r := range recs {
		fmt.Printf("%s  %-8s  offset %-10d  %d bytes  node %s\n", r.Name, r.Status, r.LastApplied, r.Size, r.Node)
	}
	return nil
}

// fetchBackup downloads the newest backup that is good, or the named one, and
// verifies it against the recorded checksum on the way. It is the first step of
// recovering a node; the second is `zincsearch restore FILE` before it starts.
func fetchBackup(args []string) error {
	fs := flag.NewFlagSet("fetch-backup", flag.ExitOnError)
	out := fs.String("out", "", "file to write the backup to (required)")
	name := fs.String("name", "", "backup name; default: the newest verified one")
	_ = fs.Parse(args)
	if *out == "" {
		return errors.New("fetch-backup needs -out FILE")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	st, blobs, closeAll, err := stateAndBlobs(ctx)
	if err != nil {
		return err
	}
	defer closeAll()

	tmp := *out + ".part"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	rec, err := coordinator.FetchBackup(ctx, st, blobs, *name, f)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, *out); err != nil {
		return err
	}
	fmt.Printf("%s (%s, offset %d) written to %s\n", rec.Name, rec.Status, rec.LastApplied, *out)
	return nil
}
