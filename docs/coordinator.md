# The coordinator

`zinccoordinator` is the control plane of a replicated ZincSearch cluster (see
[streaming.md](streaming.md) for how the nodes replicate). It is a single
program that you run next to the nodes, as a container or as a Go library
(`pkg/coordinator`). It does five things:

* **Ingest.** It accepts logs and administrative operations over HTTP and
  publishes them to the replication stream. Nothing writes to a ZincSearch node
  directly. While the stream is unreachable it parks messages in a bounded buffer
  on disk and sends them in order when the stream returns.
* **Failover.** It watches the nodes and, when the master is down, promotes the
  replica that is up and least behind. Promotion moves no data: every node
  consumes the same stream, so only the node that readers are sent to changes.
* **Backups.** Every hour it makes the replica take a backup, streams it into an
  S3 bucket, checks its checksum, and keeps the newest three.
* **Backup verification.** Every day it reads the stored backups back, compares
  their checksums, and restores the newest one in a scratch ZincSearch to prove
  that it works, counting the documents.
* **Admission.** It tells the CI system whether new builds may start.

The state (who is master, the promotion history, the list of backups, when jobs
ran) is kept in NATS JetStream (default) or in rqlite. Several coordinator
instances can run against the same state: one of them is elected leader and runs
failover and the scheduled jobs; all of them can ingest.

```
CI runners ──▶ coordinator ──▶ JetStream stream ──▶ ZincSearch master   (serves reads)
                   │                            └─▶ ZincSearch replica  (backed up hourly)
                   ├──▶ state: NATS KV or rqlite
                   └──▶ S3: backups
```

## Running it

```shell
COORD_NODES=zinc-0=http://zinc-0:4080,zinc-1=http://zinc-1:4080 \
COORD_NODE_USER=admin COORD_NODE_PASSWORD=... \
COORD_NATS_URL=nats://nats:4222 COORD_ENSURE_STREAM=true \
COORD_BLOB=s3 COORD_S3_ENDPOINT=s3.example.com COORD_S3_BUCKET=zinc-backups \
COORD_S3_KEY=... COORD_S3_SECRET=... \
COORD_ZINC_BIN=/usr/local/bin/zincsearch \
COORD_TOKEN=... \
zinccoordinator
```

The first node in `COORD_NODES` is the master until the state says otherwise.
Every node has to run with `zinc_stream_enable`, `zinc_backup_path` and the same
stream name.

### Environment

| Variable                    | Meaning                                                                 |
|-----------------------------|-------------------------------------------------------------------------|
| `COORD_LISTEN`              | HTTP listen address. Default `:8080`.                                   |
| `COORD_TOKEN`               | Bearer token for the API. Without it the API is open; a warning is logged. |
| `COORD_ID`                  | Name of this instance in the leader election. Default host and pid.     |
| `COORD_NODES`               | `name=url,name=url`. Required.                                          |
| `COORD_NODE_USER`, `COORD_NODE_PASSWORD` | Credentials of the nodes' API (backup endpoints).           |
| `COORD_NATS_URL`            | NATS servers. Needed to publish, and for the NATS state.                |
| `COORD_NATS_CREDS`, `COORD_NATS_USER`, `COORD_NATS_PASSWORD` | NATS authentication.             |
| `COORD_STREAM`              | Stream name. Default `zinc`. Messages go to `<stream>.<COORD_SUBJECT>`, subject default `logs`. |
| `COORD_ENSURE_STREAM`       | `true` creates the stream if it is missing (file storage, subjects `<stream>.>`). |
| `COORD_STREAM_MAX_AGE`      | Retention of the stream. Default `2h`. See the sizing rule below.       |
| `COORD_STREAM_REPLICAS`     | Copies of the stream: 1, 3 or 5. Default 1.                             |
| `COORD_STATE`               | `natskv` (default), `rqlite` or `memory` (tests only).                  |
| `COORD_STATE_BUCKET`, `COORD_STATE_REPLICAS` | NATS bucket (default `zinc_coordinator`) and its copies.    |
| `COORD_RQLITE_URL`          | rqlite address, for `COORD_STATE=rqlite`.                               |
| `COORD_BLOB`                | `s3`, `fs` or `none`. Backups need one.                                 |
| `COORD_S3_ENDPOINT`, `COORD_S3_BUCKET`, `COORD_S3_PREFIX`, `COORD_S3_KEY`, `COORD_S3_SECRET`, `COORD_S3_REGION`, `COORD_S3_SECURE`, `COORD_S3_PATH_STYLE` | The bucket has to exist. Self-hosted S3 (MinIO, Garage) needs `COORD_S3_PATH_STYLE=true`. |
| `COORD_FS_PATH`             | Directory, for `COORD_BLOB=fs`.                                         |
| `COORD_BUFFER_DIR`          | Where the buffer file lives. Default `/var/lib/zinc-coordinator`.       |
| `COORD_BUFFER_MAX`          | Size limit of the buffer. Default `1GiB`.                               |
| `COORD_POLL_INTERVAL`       | How often nodes are asked. Default `2s`.                                |
| `COORD_FAIL_THRESHOLD`      | Failed polls in a row until a node is down. Default `3`.                |
| `COORD_MAX_LAG`             | Messages a node may be behind and still be promoted. Default `1000`.    |
| `COORD_PROMOTE_COOLDOWN`    | Minimum time between promotions. Default `1m`.                          |
| `COORD_BACKUP_INTERVAL`     | Default `1h`; negative switches scheduled backups off.                  |
| `COORD_BACKUP_RETENTION`    | Backups kept. Default `3`.                                              |
| `COORD_DRAIN_TIMEOUT`       | How long a node may take to pause before a backup. Default `10m`.       |
| `COORD_VERIFY_INTERVAL`     | Default `24h`; negative switches verification off.                      |
| `COORD_ZINC_BIN`            | Path of `zincsearch`. Turns on the restore-and-count verification.      |
| `COORD_WORK_DIR`            | Scratch space for verification. Default the system temp directory.      |

## Choosing the state store

`natskv` needs nothing but the NATS cluster that you run anyway, which makes it
the default for a single coordinator. Its state lives in the same failure domain
as the stream: while JetStream is down the coordinator cannot change its state.
Give the bucket as many replicas as the stream has.

`rqlite` is a failure domain of its own, so several coordinator instances can
keep deciding while JetStream is down. Use it when the coordinator is embedded in
a system that runs rqlite already, or when you run more than one instance.

## HTTP API

Every endpoint but `/healthz` needs `Authorization: Bearer <COORD_TOKEN>`.

| Endpoint                           | Meaning                                                          |
|------------------------------------|------------------------------------------------------------------|
| `GET /healthz`                     | Liveness, and whether this instance is the leader.               |
| `GET /v1/admit`                    | `200` when new builds may start, `503` with the reasons when not. |
| `GET /v1/master`                   | `{name, url, epoch}` of the node that serves reads.              |
| `GET /v1/status`                   | Everything: nodes, publisher, admission, schedules, backups.     |
| `POST /v1/ingest/{index}`          | Publish documents: NDJSON (`application/x-ndjson`), a JSON array, or one object. |
| `POST /v1/admin/{index}/{op}`      | Publish `create_index`, `set_mapping` or `delete_index`; the body is the body of the matching ZincSearch API. |
| `GET /v1/backups`                  | The stored backups with their status.                            |
| `GET /v1/backups/{name}`           | Download one.                                                    |
| `POST /v1/backups`                 | Take a backup now. Leader only.                                  |
| `POST /v1/verify`                  | Verify the stored backups now. Leader only.                      |
| `POST /v1/failover`                | `{"to":"name"}` promote by hand. Leader only.                    |

### Ingest

`202` means the documents are safe: acknowledged by the stream or in the buffer
on disk. A `_id` string in a document names it and is not stored. When the
stream is unreachable and the buffer is full the answer is `503` with
`Retry-After`, and the body says how much was accepted.

A producer that does not know whether a request went through should send it
again with an `Idempotency-Key` header: documents without `_id` are then named
after the key and their position, so the second copy replaces the first instead
of adding to it.

### Admission

Builds are admitted when logs have somewhere to go and there is a node to serve
them: the stream is connected and accepts messages, the buffer is not filling
up, and at least one node answers. A CI system checks `GET /v1/admit` before it
starts a build. Builds that already run are not affected; their logs are
buffered or flushed as the situation allows.

## Backups

The coordinator asks a replica (never the master) to create a backup. The replica
pauses its consumer, drains, writes the archive and resumes; the coordinator
streams the archive into S3 while computing its SHA-256 and keeps it only when the
checksum equals the one the node announced. The archive on the node's disk is a
staging copy and is removed.

Each stored backup has a status:

* `uploaded`: stored, and the checksum matched at upload.
* `verified`: restored in a scratch node, documents counted.
* `bad`: damaged or unusable. It is never used to recover a node.

**Retention** keeps the newest three usable backups and always also the newest
verified one. Bad backups are removed only when a usable one exists.

**Verification** runs daily: every backup that is not bad is read back and its
checksum compared with the recorded one (this finds storage that rots). The
newest is then restored by `zincsearch verify-backup --deep` in a throwaway data
directory and its documents are counted against the manifest (this finds an
archive that is intact but that ZincSearch cannot use). A failure marks the backup
`bad` and logs an `ALERT` line; watch for it.

### Sizing the stream against the backup interval

A node restored from a backup catches up from the stream, so the stream has to
reach back to the backup's position. With a backup every hour keep at least two
hours of retention (`COORD_STREAM_MAX_AGE=2h`): one missed backup then does not
open a gap, two in a row would.

## Recovering a node

```shell
zinccoordinator fetch-backup -out /restore/backup.tgz   # newest verified backup, checked on the way
zincsearch restore /restore/backup.tgz                  # into the empty ZINC_DATA_PATH
zincsearch                                              # resumes the stream after the backup
```

`fetch-backup` needs the same `COORD_STATE` and `COORD_BLOB` variables as the
coordinator, so in Kubernetes it fits an init container in front of the node.
`zinccoordinator list-backups` shows what is stored.

A node whose data is older than the stream still holds reports
`stream does not reach back to the node's offset`; the coordinator sees that as
`needs_restore` and does not promote it, and replaces the master when the master
is in that state.

## Failover rules

* A node is `down` after `COORD_FAIL_THRESHOLD` failed polls in a row.
* Only a master that is `down` or `needs_restore` is replaced. A master that lost
  the stream still answers reads and stays master.
* The replacement is a node that is `up` (reachable, consuming, not more than
  `COORD_MAX_LAG` messages behind), the one furthest along if there are several.
  With none, nothing changes and a warning is logged.
* After a promotion there is a cool-down; the old master, when it returns, is a
  replica and nothing flips back.
* Only the leader promotes, with a compare-and-set on the shared state, so two
  coordinators never promote twice.
