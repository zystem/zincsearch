# Replication through a NATS JetStream stream

A ZincSearch node can build its indexes from a NATS JetStream stream instead of
(or besides) the HTTP write API. Every node of a cluster consumes the **same
stream in the same order**, so nodes stay identical without talking to each
other: there is no leader election, no write forwarding and nothing to fence on
the write path. A producer publishes once; each node applies the messages on its
own.

```
producer ──▶ JetStream stream ──▶ node A   (own consumer, serves reads)
                              └─▶ node B   (own consumer, backed up hourly)
```

## Configuration

| Key                     | Meaning                                                                     |
|-------------------------|-----------------------------------------------------------------------------|
| `zinc_stream_enable`    | Turn the consumer on. Default `false`.                                      |
| `zinc_stream_url`       | NATS server URL(s), comma separated. Required.                              |
| `zinc_stream_name`      | JetStream stream to consume. Default `zinc`.                                |
| `zinc_stream_subject`   | Optional subject filter inside the stream.                                  |
| `zinc_stream_consumer`  | Name of this node's consumer. **Required and unique per node.**             |
| `zinc_stream_batch`     | Messages applied between two flushes. Default `256`.                        |
| `zinc_stream_allow_gap` | Continue when the stream was trimmed past this node's position. Default `false`; messages in between are lost. |
| `zinc_backup_path`      | Directory of backups, outside `zinc_data_path`. See [Backup](#backup-and-restore). |

All keys can also be set through the environment (`ZINC_STREAM_URL`, ...).

The node does not create the stream. Create it with the retention you need, for
example a file stream with `MaxAge` of a few hours (see the recovery section for
how to size it against the backup interval).

## Messages

A message body is JSON.

```json
{"kind":"doc","index":"logs","id":"build-7-line-12","doc":{"line":"compiled ok"}}
{"kind":"docs","index":"logs","docs":[{"id":"a","doc":{"line":"one"}},{"doc":{"line":"two"}}]}
{"kind":"admin","index":"logs","op":"create_index","data":{"mappings":{"properties":{"line":{"type":"text"}}}}}
{"kind":"admin","index":"logs","op":"set_mapping","data":{"properties":{"level":{"type":"keyword"}}}}
{"kind":"admin","index":"logs","op":"delete_index"}
```

* `doc` creates a document, or replaces the one with the same `id`. Without `id`
  the ID is derived from the stream sequence. A document for a missing index
  creates the index, like the bulk API does.
* `docs` is a batch of documents of one index, applied in order. It exists so a
  producer can send many log lines in one message. A document without `id` is
  named after the stream sequence and its position in the batch.
* `admin` operations take the same `data` as the matching HTTP API
  (`create_index`: the create index body, `set_mapping`: the mapping body).

Administrative operations and documents travel in one stream, so a mapping is
applied before the documents published after it, on every node.

Delivery is at-least-once, so applying a message twice must equal applying it
once: documents are replaced by ID, `create_index` on an existing index and
`delete_index` on a missing one do nothing, and `set_mapping` with an identical
field does nothing.

A message that can never succeed (invalid JSON, unknown operation, invalid index
name, a mapping that contradicts an existing one) is **skipped** and counted in
`skipped_total`, so it cannot block the stream. Transient failures (for example
a full disk) are retried on the same message, which keeps the order.

## How a node consumes

The node keeps its own position, the sequence of the last message that is applied
and durable, in its metadata (key `stream_offset`). Per batch it applies the
messages, flushes the WAL to disk, saves the position and only then acknowledges.
After a crash it resumes at the saved position; messages that were applied but
not saved are applied again, which is harmless.

Because the position lives in the node metadata, it travels with a backup: a node
restored from a backup resumes exactly after the last message the backup holds.

The node refuses to consume when the stream no longer holds the messages it still
needs (it reports `stream does not reach back to the node's offset`), or when its
position is beyond the end of the stream. In both cases restore the node from a
backup instead of letting it silently miss data.

## State and control

`GET /healthz` stays `200` and gets a `details.stream` section:

```json
{"status":"ok","details":{"stream":{
  "connected":true,"paused":false,"drained":false,
  "stream":"zinc","consumer":"node-a",
  "last_applied":2001,"stream_last":2001,"lag":0,
  "applied_total":2001,"skipped_total":0}}}
```

`lag` is how many messages the node is behind. `last_error` and
`last_error_time` appear while something is wrong.

| Endpoint                    | Purpose                                                            |
|-----------------------------|--------------------------------------------------------------------|
| `GET /api/stream/status`    | The same state, authenticated.                                     |
| `POST /api/stream/pause`    | Stop consuming and answer once the node is quiescent: the batch in flight is durable and every accepted document is applied. `?timeout=30s` bounds the wait. |
| `POST /api/stream/resume`   | Continue.                                                          |

While paused the data files of the node do not change.

## Backup and restore

A backup is one `.tgz` with the index data, the node metadata and a manifest that
holds the size and SHA-256 of every file and the stream position of the data.
It contains the password hashes of the users, so it is written with mode `0600`;
treat it as a secret.

Set `zinc_backup_path` and call:

```shell
curl -u admin:... -X POST 'http://node:4080/api/backup?timeout=5m'
```

When the node consumes a stream, the endpoint pauses and drains the consumer,
takes the backup and resumes, so a backup is a state the stream really passed
through. It returns the description of the archive (`name`, `size`, `sha256`,
`last_applied`, `indexes`). Without a consumer nothing stops writers; the call
fails with `409` when it notices that the node changed.

| Endpoint                       | Purpose                                                        |
|--------------------------------|----------------------------------------------------------------|
| `POST /api/backup`             | Create a backup.                                               |
| `GET /api/backup`              | List backups, oldest first.                                    |
| `GET /api/backup/{name}`       | Download. Ranges work; the checksum is in `X-Zinc-Backup-Sha256`. |
| `DELETE /api/backup/{name}`    | Delete.                                                        |

Take backups from a node that does not serve queries, since the node is paused
for as long as the backup takes.

Check an archive without restoring it, and restore it into a new node:

```shell
zincsearch verify-backup backup.tgz          # checksums only
ZINC_DATA_PATH=/tmp/scratch zincsearch verify-backup --deep backup.tgz   # also restores it and counts the documents
zincsearch restore backup.tgz      # before the first start, ZINC_DATA_PATH must hold no index
zincsearch                         # starts and resumes the stream after the backup
```

`restore` extracts the whole archive, verifies every file against the manifest
and only then puts anything in place, so a damaged archive changes nothing. It
refuses a node that already has indexes.

### Recovering a lost node

1. Fetch the newest backup and `zincsearch restore` it into the new node's data
   directory.
2. Start the node. It resumes at the position stored in the backup and catches up
   from the stream.

The stream has to reach back to the backup's position: with a backup every hour
keep at least two hours of stream retention, so one missed backup does not leave a
gap.
