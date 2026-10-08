<p align="center"><img src="tranquila.png" width="300px"></p>

# tranquila

A distributed S3 bucket synchronization tool. Tranquila copies objects from source S3 buckets to a destination S3-compatible endpoint, tracking state in Redis so syncs are resumable across runs.

## How It Works

Tranquila pipelines discovery and sync per bucket:

1. **Discovery** — Lists a source bucket in configurable batches (default 100 000 objects). For each object, it checks Redis state and marks new or modified objects as pending.
2. **Sync** — A shared worker pool starts transferring pending objects immediately as each batch is ready. Discovery of the next batch begins only after the current batch has been fully synced, keeping memory usage bounded.
3. **Concurrency** — Multiple buckets are discovered and synced concurrently (bounded by `--workers`).

State is persisted in Redis using object-level keys, so interrupted or failed transfers are automatically retried on the next run.

Key properties:

- **Resumable** — Redis tracks every object, so an interrupted run picks up where it left off.
- **Provider-agnostic** — works against AWS S3 or any S3-compatible endpoint (MinIO, Ceph, …) on either side, with Redis or Valkey for state.
- **Survives flaky endpoints** — transient failures are retried with backoff, and a struggling endpoint is automatically paced down and recovered, without the process exiting.
- **Continuous or one-shot** — run once as a `Job`, or `--watch` continuously via polling, MinIO notifications or SQS.
- **Observable** — Prometheus/OTLP metrics, a management API, and Kubernetes probes.

For the internals — data flow, the Redis key design, the resilience machinery and the concurrency model — see **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Commands

### `tranquila sync`

Runs discovery and sync against all configured buckets.

```shell
tranquila sync [flags]
tranquila -c tranquila.yaml sync
```

### `tranquila status [bucket...]`

Prints per-bucket statistics from the management API (requires a running `tranquila sync` process).

```shell
tranquila status
tranquila status bucket1 bucket2
tranquila status --server https://tranquila.example.svc:8080 bucket1
```

`--server` (env `MGMT_ADDR`) takes the management API's base URL. A bare
`host:port` is assumed `http://` for local/dev use; in a cluster where the
API is fronted by TLS, pass the full `https://...` URL.

Output columns: `BUCKET | LAST COLLECTED | TOTAL | SYNCED | PENDING | FAILED | PARKED` (plus
`RATE | ETA` while a bucket is actively syncing). `PARKED` is how many sharded-discovery prefixes
resumed onto a checkpointed page the backend still could not answer, as of that bucket's most
recently completed cycle — `-` means the server has no source client wired to report it (an older
`tranquila`), distinct from a real `0` (genuinely nothing parked). A nonzero, persistent value here
is the visible symptom of a bucket discovery cannot make progress on; see
[Prefix-Sharded Discovery](#prefix-sharded-discovery) for what it means and
`--discovery-prefix-budget`/`--list-retry-budget` for the knobs that affect it.

When a sync is actively running, two additional columns are shown: `RATE | ETA`.

### `tranquila completion <bash|zsh|fish>`

Prints a shell completion script to stdout, generated from the live CLI grammar — every flag,
subcommand and enum is picked up automatically, with nothing to keep in sync by hand.

```shell
# Load once per shell session
source <(tranquila completion bash)
source <(tranquila completion zsh)
tranquila completion fish | source

# Or install permanently
tranquila completion bash > /etc/bash_completion.d/tranquila
tranquila completion zsh > "${fpath[1]}/_tranquila"
tranquila completion fish > ~/.config/fish/completions/tranquila.fish
```

## Configuration

Priority order (highest wins):

1. CLI flags
2. Environment variables
3. YAML config file
4. Built-in defaults

### Config File

Load with `--configFile <path>` (short: `-c`) or place a `tranquila.yaml` in the working directory. Tranquila also checks `~/.config/tranquila.yaml`.

All sync settings live under a `sync:` top-level key. Multi-word keys use **hyphens** (e.g. `access-key`, `rate-limit`). Nested keys are equivalent to their hyphen-joined flat form — `source: { access-key: foo }` resolves to the same flag as `source-access-key: foo`.

```yaml
sync:
  source:
    endpoint: ""          # leave empty for AWS; set for MinIO/S3-compatible
    region: "us-east-1"
    access-key: ""
    secret-key: ""
    rate-limit: 0         # max S3 API calls/sec for source endpoint (0 = unlimited)

  dest:
    endpoint: ""
    region: "us-east-1"
    access-key: ""
    secret-key: ""
    rate-limit: 0         # max S3 API calls/sec for destination endpoint (0 = unlimited)
    bucket-prefix: ""     # prepended to auto-discovered destination bucket names

  # Structured bucket mappings (preferred for multiple buckets)
  buckets:
    - source:
        bucket: "my-bucket"
        prefix: "optional/prefix/"   # optional
      destination:
        bucket: "backup-my-bucket"
        prefix: "optional/dest/prefix/"  # optional
    - source:
        bucket: "other-bucket"
      destination:
        bucket: "other-bucket-backup"

  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    pool-size: 0          # 0 = go-redis default (10 * GOMAXPROCS)

  workers: 10
  check-sizes: false        # re-sync if destination size differs from source
  discovery-batch-size: 100000  # objects per batch; sync drains before next batch starts
  list-attempt-timeout: 0s              # starting ListObjectsV2 attempt timeout (0 = default 60s)
  sharded-discovery-concurrency: 0      # concurrent prefix listings in sharded mode (0 = default 4)
  discovery-prefix-budget: 0s           # per-prefix listing budget per cycle (0 = default 10m, negative = unbounded)
  discovery-checkpoints: true           # resume a failed prefix where it stopped, not from page 1
  discovery-checkpoint-ttl: 24h         # how long an unrefreshed resume point survives
  list-retry-budget: 0s                 # per-page retry budget, including escalation (0 = default 10m, negative = unbounded)

  # Continuous watch mode
  watch: false
  watch-mode: poll          # poll | minio | sqs
  watch-interval: 60s       # inter-cycle sleep (poll mode only)
  sqs-queue-url: ""         # SQS queue URL (sqs mode only)

  # Retry pacing after a failed cycle in watch mode (exponential, jittered)
  cycle-backoff: 5s
  cycle-backoff-max: 10m
  endpoint-fail-threshold: 5  # transient failures before an endpoint's rate is halved

  # Cadence for propagate-deletes reconciliation (poll mode / initial catch-up
  # sync only; see "Propagate Deletes"). 0 = every cycle.
  delete-reconcile-interval: 0s

  # Burn-after-reading minimum age (see "Burn-After-Reading"). 0 = delete
  # immediately, today's original behavior.
  burn-after-reading-min-age: 0s
  burn-after-reading-reconcile-interval: 30m

  telemetry:
    exporter: "prometheus"  # prometheus | otlp | none
    addr: ":8081"
    otlp-endpoint: ""       # gRPC endpoint, e.g. localhost:4317

  mgmt-addr: ":8080"        # management API listen address
```

**Note:** underscores in YAML keys (e.g. `access_key`) are not equivalent to hyphens — use hyphens to match flag names.

#### Burn-After-Reading

Set `burn-after-reading: true` on any bucket mapping to delete source objects after they are successfully synced and verified:

```yaml
sync:
  buckets:
    - source:
        bucket: staging-uploads
      destination:
        bucket: archive-uploads
      burn-after-reading: true
```

**Verification.** Deletion is irreversible, so it only happens behind a content check, tried cheapest-first:

1. **CRC32 metadata** — for an object uploaded in this run, tranquila compares the CRC32 returned by the upload response with the CRC32 stored by S3 (via `HeadObject`), having already confirmed the destination size matches the source. Not every S3-compatible endpoint echoes flexible checksums, though, so this tier is skipped (not treated as a mismatch) when either value is empty — falling through to the next tier instead of refusing outright.
2. **ETag** — identical single-part uploads produce identical ETags (S3's MD5-of-content convention) on any S3-compatible backend, at zero extra read cost: the source's ETag comes from discovery listing (or, for an object just uploaded, a fresh `HeadObject`), the destination's from the `HeadObject` call the size check already makes. This catches a destination overwritten or corrupted with same-size content — something a size check alone would miss — **without downloading either object**. A multipart ETag (contains a `-partCount` suffix, since S3 computes it as a checksum-of-part-checksums rather than a checksum of the content) is never comparable this way and falls through to the next tier.
3. **Content hash** — downloads and CRC32-hashes both objects directly. This is the fallback of last resort: the cost of verifying an irreversible delete when neither side carries a stored checksum usable for comparison (destination doesn't support flexible checksums *and* the object is multipart, or has no ETag at all).

A mismatch at any tier — or an inconclusive comparison with no further tier to fall back to — refuses the delete and marks the job failed for retry.

**Dry-run mode:** pass `--dry-run` (or set `TRANQUILA_DRY_RUN=true`) to log what would be deleted without actually removing anything:

```shell
tranquila sync --dry-run -c tranquila.yaml
```

Dry-run logs include the object key, CRC32 comparison result, and the planned deletion for every object that would be removed.

**Minimum age.** By default the source object is deleted immediately once verified. Set `burn-after-reading-min-age` (globally, and/or per bucket) to defer deletion until the source object is at least that old (by its S3 last-modified time). An object younger than the threshold is still synced normally — just not deleted yet — and picked up for deletion once it ages past the threshold.

```yaml
sync:
  burn-after-reading-min-age: 7d   # global default: applies to every burn-after-reading bucket
  burn-after-reading-reconcile-interval: 30m   # how often the background sweep checks for objects that just became old enough

  buckets:
    - source:
        bucket: staging-uploads
      destination:
        bucket: archive-uploads
      burn-after-reading: true
      # inherits the 7d global default

    - source:
        bucket: audit-logs
      destination:
        bucket: audit-logs-archive
      burn-after-reading: true
      burn-after-reading-min-age: 30d   # overrides the global default for this bucket
```

Accepts Go duration syntax (`12h`, `90m`) plus `d`/`w` suffixes (`7d`, `2w`) for single-unit day/week values — `1d12h` is not supported; use `36h` instead. `0` (the default) means immediate deletion, today's original behavior.

**How deferred objects get swept.** An independent background reconciler (`--burn-after-reading-reconcile-interval`, default `30m`) periodically scans for synced objects that have now aged past the threshold and deletes them, using the same tiered verification as above. This is deliberately **not** tied to `--watch-interval` or to the regular sync/discovery cycle: checking for month-old objects doesn't need sync-cycle cadence, and in pure event-driven watch mode (`--watch-mode=minio`/`sqs`) there's no other periodic re-scan to piggyback on — the reconciler runs on its own schedule in all three watch modes, and once at the end of a one-shot (non-`--watch`) run.

#### Propagate Deletes

Set `propagate-deletes: true` on a bucket mapping to delete the destination object when the corresponding source object is deleted:

```yaml
sync:
  buckets:
    - source:
        bucket: my-bucket
      destination:
        bucket: backup-my-bucket
      propagate-deletes: true
```

Like `burn-after-reading`, this is only available via structured `buckets:` config — the legacy `--bucket-mappings`/`--bucket-mapping-file`/`--prefix-mappings` flags cannot set it.

**How deletions are detected** depends on `--watch-mode`:

- **`minio` / `sqs`** — the watcher's own delete notification (`s3:ObjectRemoved:*` / `ObjectRemoved:*`) drives the deletion directly, as soon as it arrives. If a bucket doesn't have `propagate-deletes` set, a delete notification for it is simply ignored.
- **`poll`, and the initial catch-up sync in `minio`/`sqs` mode** — there is no delete notification to rely on, so tranquila periodically reconciles: every discovered object's "last seen" timestamp is recorded, and any previously-synced object not seen in the most recent listing is a deletion candidate. Before actually deleting the destination object, tranquila re-checks the candidate against the **source** with `HeadObject`; only a confirmed 404/`NoSuchKey` triggers the delete. A candidate that still exists in source (a listing gap, not a real deletion) or a check that fails for any other reason (network error, permission error, etc.) is left alone and re-evaluated on the next pass — propagate-deletes never deletes on an inconclusive answer.

**Cost tradeoff:** the reconciliation scan is `SCAN ... MATCH tranquila:obj:{bucket}:*`, and Redis's `SCAN` filters `MATCH` *after* walking the whole keyspace — so this costs one full-keyspace pass (in a production deployment with ~1M+ tracked objects across all buckets, that's ~1M+ keys examined) every time it runs, regardless of how large the reconciled bucket actually is. Use `--delete-reconcile-interval` to decouple how often this runs from `--watch-interval`:

```yaml
sync:
  delete-reconcile-interval: 30m   # 0 (default) = reconcile at the end of every sync cycle
```

A one-shot run (no `--watch`) always reconciles once, since there's no repeated cycle to throttle.

#### Prefix-Sharded Discovery

By default tranquila lists a bucket flat — one bucket-wide `ListObjectsV2` scan. Some backends (observed against a MinIO deployment with ~295K objects) cannot answer that in one request: the call hangs with no response at all, even bypassing every proxy in front of it. Since the underlying data is fine — the same bucket browses instantly folder-by-folder in the MinIO/S3 console — the fix is to discover the same way: a recursive, `/`-delimited listing that walks the bucket's own key structure (e.g. `2026/08/26/...`) instead of asking for everything at once.

This kicks in two ways:

- **Automatically** — if a flat listing exhausts all its retries with a transient/gateway-timeout-class error, tranquila falls back to prefix-sharded discovery for that bucket for the rest of the cycle, and logs a warning telling you to set the flag below. This costs the full exhausted retry budget (worst case a few minutes) the first time it happens, once per bucket, until you set the flag.
- **Explicitly** — set `sharded-discovery: true` on a bucket mapping already known to need it, to skip the doomed flat attempt entirely:

```yaml
sync:
  buckets:
    - source:
        bucket: keycloak-audit-events
      destination:
        bucket: archive
      sharded-discovery: true
```

Like `burn-after-reading`/`propagate-deletes`, this is only available via structured `buckets:` config, not the legacy `--bucket-mappings` flags.

**Tradeoffs:**

- `--discovery-batch-size`'s pause-while-a-batch-drains pacing doesn't apply to sharded discovery — there's no single continuation token for a tree walk to pause on. Backpressure instead comes from the same per-bucket worker cap (`--max-workers-per-bucket`) that already throttles flat discovery, which bounds it identically in practice.
- A bucket with no `/`-delimited key structure gains nothing from sharding (there's nothing to shard by) but isn't harmed either — it just becomes one listing call at the root, same shape as flat.
- Every `ListObjectsV2` attempt (flat or sharded) is individually bounded by `--list-attempt-timeout` (default 60s). A healthy call finishes in well under that; a hanging one fails and retries instead of blocking a discovery goroutine forever.
- The flag sets the timeout for the **first** attempt only. Each attempt that times out doubles the deadline for the next one, up to 4x the configured value, because retrying a deadline with the same deadline cannot succeed — a prefix that genuinely needs 90s to list would otherwise fail identically on every attempt. Failures that are not timeouts (a 504, say) do not extend the deadline. The whole retry loop for one page is capped by `--list-retry-budget` (default 10m, negative = unbounded) regardless of how far the deadline escalated — see below, since raising `--list-attempt-timeout` alone does not help once this budget is what's actually binding.
- A timed-out attempt counts as endpoint congestion, so it feeds the rate-limit degradation described above. Listings timing out under load will therefore slow tranquila down, which is usually what relieves the pressure causing them.

**Tuning for a slow backend.** Sharding narrows *what* each listing call covers, but if the backend itself is slow per-call — regardless of how narrow the prefix is — narrower scope alone may not be enough once several listings run concurrently. Two knobs:

```shell
# Fewer simultaneous prefix listings per bucket during a sharded walk (default 4).
# Lower this first if individual LIST calls are slow even in isolation.
tranquila sync --sharded-discovery-concurrency=1

# More headroom before giving up on one ListObjectsV2 attempt (default 60s).
tranquila sync --list-attempt-timeout=3m
```

Both apply to the source endpoint only (nothing lists the destination). If you also have `--source-rate-limit` set, make sure it's sized for what the backend can actually sustain — a limit far above real capacity does not prevent the contention that pushes individual calls past the attempt timeout.

**Raising `--list-attempt-timeout` alone can stop helping once a budget binds first.** Two separate budgets bound how long tranquila keeps retrying, and both default to 10 minutes: `--list-retry-budget` bounds one *page*'s retry loop (attempt deadlines are `min(escalated timeout, budget remaining)`), and `--discovery-prefix-budget` bounds a whole *prefix*'s multi-page walk in sharded discovery, nested around the page budget. Whichever is smaller in wall-clock terms at any moment wins — so an escalated attempt timeout can get silently truncated to far less than configured once either budget's remaining time runs low, producing a `context deadline exceeded` that looks like a misconfigured `--list-attempt-timeout` but is really the budget expiring. If you deliberately raise `--list-attempt-timeout` past a couple of minutes, raise the relevant budget(s) to match (or set them negative for unbounded, accepting that an unanswerable prefix then holds its slot longer):

```shell
tranquila sync --list-attempt-timeout=2m --list-retry-budget=20m
tranquila sync --list-attempt-timeout=2m --discovery-prefix-budget=-1   # unbounded, sharded discovery only
```

tranquila logs a one-time `Warn` at startup ("this budget is lower than the escalated list-attempt-timeout ceiling and will bind first") when a bounded budget sits below the escalation ceiling (`list-attempt-timeout × 4`), naming which flag is the actual constraint.

**Progress survives a failed cycle.** A prefix that dies partway through its pages used to be
re-listed from its *first* page on the next cycle, so on a bucket whose pages are already at the
limit of what the backend can answer, the walk could repeat forever with zero net progress.
Discovery now records a resume point per prefix in Redis (`tranquila:ckpt:{bucket}:{prefix}`) and
continues from there:

```shell
tranquila sync --discovery-checkpoints=false      # opt out (default: enabled)
tranquila sync --discovery-checkpoint-ttl=6h      # expire an abandoned resume point sooner (default 24h)
```

Details worth knowing:

- A checkpoint exists only while a prefix is **incomplete**. Completing one clears it, so the next
  cycle lists that prefix in full — which is what gives an object whose *transfer* failed a chance
  to be rediscovered.
- Only leaf prefixes are checkpointed. A prefix that contains subfolders is never resumed, because
  a delimited listing interleaves objects and subfolders in one paginated stream and resuming past
  a page would skip the subfolders it carried.
- An abandoned resume point expires after the TTL and the prefix is listed from the start again.
  That is also the uninstall path: disable the flag and the keys self-delete within one TTL, with
  no cleanup step.
- Checkpoint store errors are never fatal — they degrade that prefix to the previous behaviour.
- This does not make listings faster. It stops the work being thrown away. If nearly every page
  times out you will still want `--sharded-discovery-concurrency` lowered; checkpointing is what
  makes lowering it safe, since a slower walk that also restarts every cycle is worse.
- Each prefix gets a bounded turn (`--discovery-prefix-budget`, default 10m) before it yields its
  worker slot. Without that bound a handful of pathological prefixes could hold every slot for
  hours and the rest of the bucket was never listed at all in that cycle. Being cut off is cheap
  because the checkpoint is already banked — the prefix resumes next cycle from the page it
  stopped on. Set it negative to restore the old unbounded behaviour.
- Watch for `sharded discovery: resuming prefix from stored checkpoint` at info level. An `Error`
  line naming a prefix that resumed and listed **zero** pages before failing means that prefix is
  pinned on a page the backend cannot answer at all; it will stay there until the TTL expires.
- **This count doesn't require grepping logs.** Every such prefix — one that resumed onto its
  checkpoint and still delivered zero pages — is counted for the cycle that just ran, exposed as
  the `tranquila.s3.discovery.parked_prefixes` gauge (per bucket) and the `PARKED` column in
  `tranquila status`. A bucket where this number stays high and steady across cycles has most of
  its prefixes stuck, not just a slow backend — on a bucket that large, checkpointing alone can no
  longer make forward progress, and driving the initial backfill from an out-of-band key list
  instead of sharded discovery is the more honest fix.

**A prefix that keeps failing no longer takes the bucket down with it.** Each prefix is listed independently: one that exhausts its retries is logged (`sharded discovery: prefix listing failed after retries, continuing with other prefixes`), skipped, and retried on the next cycle, while every other prefix still completes and syncs. Discovery reports those failures at the end of the cycle, so the bucket's cycle is still marked failed and retried — but the objects it *could* reach are already synced rather than discarded. On a bucket with hundreds of prefixes, only the first few failures are named in the cycle error, followed by a count of the rest.

**A bucket that can never finish discovery no longer blocks live events.** In `minio`/`sqs` watch mode the initial catch-up sync runs *concurrently* with the event stream, so a bucket whose listing keeps timing out no longer stops every other bucket's live events from being consumed. The catch-up keeps retrying in the background with the usual cycle backoff; only a genuine misconfiguration (all-permanent failures) terminates the process.

#### Bucket mappings via CLI / file

Legacy string-based mappings are also supported and additive with structured config. CLI flags win on conflict (same source bucket):

```shell
# Comma-separated
tranquila sync --bucket-mappings "src=dst,other"

# From file (one mapping per line; "src=dst" or bare "name"; "#" comments)
tranquila sync --bucket-mapping-file mappings.txt

# Prefix mappings
tranquila sync --prefix-mappings "bucket/src-prefix=dst-prefix"
```

### Environment Variables

| Variable                          | Default          | Description                                          |
| --------------------------------- | ---------------- | ---------------------------------------------------- |
| `SOURCE_ENDPOINT`                 | _(AWS)_          | S3-compatible source endpoint                        |
| `SOURCE_REGION`                   | `us-east-1`      | Source AWS region                                    |
| `SOURCE_ACCESS_KEY`               |                  | Source access key ID                                 |
| `SOURCE_SECRET_KEY`               |                  | Source secret access key                             |
| `SOURCE_RATE_LIMIT`               | `0`              | Max S3 API calls/sec for source endpoint             |
| `DEST_ENDPOINT`                   | _(AWS)_          | S3-compatible destination endpoint                   |
| `DEST_REGION`                     | `us-east-1`      | Destination AWS region                               |
| `DEST_ACCESS_KEY`                 |                  | Destination access key ID                            |
| `DEST_SECRET_KEY`                 |                  | Destination secret access key                        |
| `DEST_RATE_LIMIT`                 | `0`              | Max S3 API calls/sec for destination endpoint        |
| `DEST_BUCKET_PREFIX`              |                  | Prefix prepended to destination bucket names         |
| `BUCKET_MAPPINGS`                 |                  | Comma-separated bucket mappings (`src=dst` or `name`)|
| `BUCKET_MAPPING_FILE`             |                  | Path to bucket mapping file                          |
| `PREFIX_MAPPINGS`                 |                  | Comma-separated prefix mappings                      |
| `REDIS_ADDR`                      | `localhost:6379` | Redis address                                        |
| `REDIS_PASSWORD`                  |                  | Redis password                                       |
| `REDIS_DB`                        | `0`              | Redis database number                                |
| `REDIS_POOL_SIZE`                 | `0`              | Redis connection pool size (0 = go-redis default)    |
| `TRANQUILA_WORKERS`               | `10`             | Number of concurrent sync workers                    |
| `TRANQUILA_CHECK_SIZES`           | `false`          | Re-sync objects whose destination size differs       |
| `TRANQUILA_DRY_RUN`               | `false`          | Log planned burn-after-reading deletions, no delete  |
| `TRANQUILA_DISCOVERY_BATCH_SIZE`  | `100000`         | Objects per discovery batch (0 = use default)        |
| `TRANQUILA_LIST_ATTEMPT_TIMEOUT`  | `0`              | Per-`ListObjectsV2`-attempt timeout (0 = default 60s) |
| `TRANQUILA_SHARDED_DISCOVERY_CONCURRENCY` | `0`      | Concurrent prefix listings in sharded mode (0 = default 4) |
| `TRANQUILA_WATCH`                 | `false`          | Enable continuous watch mode                         |
| `TRANQUILA_WATCH_MODE`            | `poll`           | Watch backend: `poll`, `minio`, or `sqs`             |
| `TRANQUILA_WATCH_INTERVAL`        | `60s`            | Idle time between poll cycles                        |
| `TRANQUILA_SQS_QUEUE_URL`         |                  | SQS queue URL (sqs watch mode)                       |
| `TRANQUILA_CYCLE_BACKOFF`         | `5s`             | Base retry delay after a failed cycle (watch mode)   |
| `TRANQUILA_CYCLE_BACKOFF_MAX`     | `10m`            | Maximum retry delay after a failed cycle             |
| `TRANQUILA_ENDPOINT_FAIL_THRESHOLD` | `5`            | Transient failures before an endpoint's rate is halved |
| `TRANQUILA_DELETE_RECONCILE_INTERVAL` | `0s`         | Cadence for propagate-deletes reconciliation (0 = every cycle) |
| `TRANQUILA_BURN_AFTER_READING_MIN_AGE` | `0s`        | Global default minimum object age before burn-after-reading deletes it (0 = immediate); supports `d`/`w` units |
| `TRANQUILA_BURN_AFTER_READING_RECONCILE_INTERVAL` | `30m` | Cadence for the independent sweep that burns objects once old enough |
| `TELEMETRY_EXPORTER`              | `prometheus`     | Metrics exporter: `prometheus`, `otlp`, or `none`    |
| `TELEMETRY_ADDR`                  | `:8081`          | Prometheus metrics listen address                    |
| `TELEMETRY_OTLP_ENDPOINT`         |                  | OTLP gRPC endpoint                                   |
| `MGMT_ADDR`                       | `:8080`          | Management API listen address                        |
| `TRANQUILA_LOG_LEVEL`             | `info`           | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `TRANQUILA_LOG_JSON`              | `false`          | Emit logs as JSON                                    |

## Continuous Watch Mode

Enable with `--watch`. Three backends are available:

| Mode    | Flag                               | Mechanism                                                                                               |
| ------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `poll`  | `--watch-interval` (default `60s`) | Repeats the full sync cycle with a configurable sleep. Works with any S3-compatible endpoint.           |
| `minio` | —                                  | Subscribes to MinIO bucket notifications via SSE. Reuses source credentials.                            |
| `sqs`   | `--sqs-queue-url`                  | Long-polls an SQS queue for S3 event notifications. Configure the S3->SQS notification externally.      |

Event-driven backends (`minio`, `sqs`) run a full initial sync on startup to catch changes missed while the process was down, then switch to event-driven.

```shell
# Poll every 5 minutes
tranquila sync --watch --watch-interval=5m

# MinIO native events
tranquila sync --watch --watch-mode=minio --source-endpoint=http://minio:9000

# SQS
tranquila sync --watch --watch-mode=sqs \
  --sqs-queue-url=https://sqs.eu-west-1.amazonaws.com/123/my-queue
```

### Failure handling in watch mode

Watch mode is a long-lived service, so a transient endpoint fault must not become
a pod restart. Failed cycles are retried with exponential backoff plus jitter
(`--cycle-backoff`, `--cycle-backoff-max`) and the process stays alive, keeping
`/healthz` answering.

| Failure | Watch mode | One-shot |
| --- | --- | --- |
| Transient (504, 502, 500, timeouts, dropped connections) | Retried forever with backoff | Exits non-zero |
| Throttle (503, `SlowDown`, 429) | Retried forever with backoff | Exits non-zero |
| Permanent (`AccessDenied`, `NoSuchBucket`, bad credentials) | Exits non-zero | Exits non-zero |

Misconfiguration stays loud: only a cycle whose failures are *all* permanent
terminates. A cycle mixing permanent and transient failures is treated as
transient, so a flaky endpoint can never be misread as misconfiguration.

One-shot runs (no `--watch`) are unchanged and still exit non-zero on any
failure, so a Kubernetes `Job` or CI invocation reports it.

### Adaptive rate limiting

Each endpoint's rate limit is governed by additive-increase/multiplicative-decrease
congestion control, fed by the outcome of every S3 API call:

- After `--endpoint-fail-threshold` consecutive transient failures (default `5`)
  the endpoint's rate limit is **halved**, down to a floor of 1 call/sec.
- An explicit throttle (`503`, `SlowDown`, `429`) is unambiguous back-pressure
  and halves the rate on the **first** signal, without waiting for the threshold.
- After 20 consecutive healthy calls the limit climbs back by 10% of the
  configured base, until it is fully restored. Decrease fast, recover slowly.
- A permanent error counts as *healthy* for pacing purposes: the endpoint
  answered, so it is not congested.

Source and destination are paced independently and symmetrically: a source-side
`504` throttles source reads only, and a destination-side `504` throttles
destination writes only. Every S3 operation feeds the controller, including the
destination's `PutObject`, `HeadObject`, `DeleteObject` and bucket creation.

> **Requires a configured rate limit.** Only endpoints with an explicit
> `--source-rate-limit` / `--dest-rate-limit` are degraded. An endpoint left
> unlimited (the default, `0`) has no ceiling to reduce, and inventing one would
> throttle a healthy endpoint — so it keeps running unlimited and gets only the
> cycle backoff above. To protect the destination, set `--dest-rate-limit`.

Note that the limiter paces *operations*, not HTTP requests: an upload above the
transfer manager's 16 MiB multipart threshold issues several requests but spends
one token, and a failure anywhere in it is one congestion signal. Pacing is
therefore approximate for workloads dominated by large objects.

Current pacing is exposed on `GET /api/v1/sync` as `source` and `destination`
(`rate_limit`, `base_rate_limit`, `degraded`, `degraded_since`) and as the
metrics `tranquila_s3_rate_limit`, `tranquila_s3_rate_limit_degraded`,
`tranquila_s3_rate_limit_changes` and `tranquila_s3_errors`.

`/readyz` stays green while degraded: degraded operation is the designed correct
response to a flaky endpoint, and reporting it as unready would stall rolling
updates without shedding any load. Alert on
`tranquila_s3_rate_limit_degraded == 1` and `tranquila_sync_cycle_failures`
instead.

## Required IAM Permissions

**Source account:**

- `s3:ListBucket`
- `s3:GetObject`
- `s3:DeleteObject` — only required if using `burn-after-reading`

**Destination account:**

- `s3:PutObject`
- `s3:HeadBucket`
- `s3:CreateBucket`
- `s3:DeleteObject` — only required if using `propagate-deletes`

## Observability

### Prometheus

Metrics are exposed at `http://localhost:8081/metrics` by default.

```shell
tranquila sync --telemetry-addr=:9090   # change listen address
```

### OTLP

```shell
tranquila sync --telemetry-exporter=otlp --telemetry-otlp-endpoint=localhost:4317
```

### Metrics

| Metric                             | Type            | Attributes            | Description                                        |
| ---------------------------------- | --------------- | --------------------- | -------------------------------------------------- |
| `tranquila.objects.synced`         | Counter         | `bucket`              | Objects successfully copied                        |
| `tranquila.objects.failed`         | Counter         | `bucket`              | Objects that failed to copy                        |
| `tranquila.bytes.transferred`      | Counter         | `bucket`              | Bytes transferred                                  |
| `tranquila.transfer.duration`      | Histogram (s)   | `bucket`              | Per-object transfer duration                       |
| `tranquila.workers.active`         | UpDownCounter   | —                     | Workers currently executing a transfer             |
| `tranquila.sync.cycle.failures`    | Counter         | —                     | Watch cycles that failed and were retried          |
| `tranquila.s3.operation.duration`  | Histogram (ms)  | `operation`, `bucket`, `status` | Duration of individual S3 API calls      |
| `tranquila.s3.errors`              | Counter         | `endpoint`, `class`   | S3 failures by class (transient/throttle/permanent) |
| `tranquila.s3.rate_limit`          | Gauge ({call}/s)| `endpoint`            | Effective rate limit; 0 when unlimited             |
| `tranquila.s3.rate_limit.degraded` | Gauge           | `endpoint`            | 1 while congestion control has reduced the limit   |
| `tranquila.s3.rate_limit.changes`  | Counter         | `endpoint`, `direction` | Rate-limit adjustments; detects oscillation      |
| `tranquila.s3.discovery.parked_prefixes` | Gauge     | `endpoint`, `bucket`  | Sharded-discovery prefixes stuck on an unanswerable checkpointed page, as of the last completed cycle |

Useful alerts:

```promql
# An endpoint has been throttled by congestion control for a sustained period
tranquila_s3_rate_limit_degraded == 1

# Watch cycles are failing: the process is alive but not making progress
rate(tranquila_sync_cycle_failures_total[15m]) > 0

# A bucket has prefixes it cannot make progress on
tranquila_s3_discovery_parked_prefixes > 0
```

### Grafana dashboard

[`deploy/grafana/tranquila-dashboard.json`](deploy/grafana/tranquila-dashboard.json) is a
ready-to-import dashboard covering sync progress, failures, endpoint health and per-bucket
detail. Import it through Dashboards → New → Import, provision it from a file, or apply it as a
grafana-operator `GrafanaDashboard` with `kubectl apply -k deploy/grafana`; see
[deploy/grafana/README.md](deploy/grafana/README.md) for all three, and for the blind spots the
dashboard cannot cover — most importantly that `tranquila.s3.operation.duration` uses the
OpenTelemetry default bucket boundaries, whose largest finite bucket is 10 s, so no panel shows
a latency quantile above that.

![Tranquila dashboard](deploy/grafana/dashboard.png)

### Management API

A lightweight HTTP API is available at `http://localhost:8080` while sync is running.

| Endpoint                     | Description                                   |
| ---------------------------- | --------------------------------------------- |
| `GET /api/v1/buckets`        | List all buckets with Redis state statistics  |
| `GET /api/v1/buckets/{name}` | Per-bucket statistics with live progress      |
| `GET /api/v1/sync`           | Overall sync run progress                     |
| `GET /healthz`               | Liveness probe (always 200 while serving)     |
| `GET /readyz`                | Readiness probe (200 ready / 503 Redis down)  |

`/healthz` is a **liveness** probe: it returns `200 {"status":"ok"}` whenever the process
is serving HTTP and checks no dependencies — so a temporarily unreachable Redis will not
cause a pod restart. `/readyz` is a **readiness** probe: it pings Redis and returns
`200 {"status":"ok"}` when reachable or `503 {"status":"unavailable","error":...}` when
not, so traffic is only routed to pods that can serve requests.

#### Redis and Valkey

Sync state is held in Redis. **Valkey works as a drop-in replacement** — the
state layer maintains its counters with Lua scripts, and the end-to-end suite
runs them against Redis 7, Valkey 8 and Valkey 9 on every CI run, so fork
compatibility is verified rather than assumed. Point `--redis-addr` at either.

Other Redis-compatible engines (KeyDB, Dragonfly, ElastiCache, MemoryDB) are
untested; Dragonfly in particular implements Lua differently. See
[e2e/README.md](e2e/README.md#key-value-engines) for how to verify one.

#### Bucket statistics

The per-bucket counts are maintained incrementally in Redis and read with a single
lookup, so the endpoints respond in milliseconds regardless of how many objects are
tracked. They are updated atomically with each object's status, in the same operation.

The counters are seeded automatically the first time they are read on a keyspace that
predates them: that one request recomputes them from the object records and can take a
few seconds on a large keyspace. Every request afterwards is served from the counters.

If the counters ever drift from reality, force a reconcile by deleting the marker key —
the next request recomputes everything from the object records:

```shell
redis-cli DEL tranquila:statsbuilt
```

#### Kubernetes probes

Point both probes at the management API port (from `--mgmt-addr`, default `8080`):

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

## Build

```bash
go build -v
```

Requires Go 1.25 or later, as declared in `go.mod`.

## Helm chart

The chart lives in this repository at [`charts/tranquila`](charts/tranquila) and is published
as an OCI artifact:

```shell
helm install tranquila oci://ghcr.io/pflege-de-labs/charts/tranquila \
  --version 0.5.1 \
  --values my-values.yaml
```

It optionally bundles Valkey for the state store, or points at an external Redis/Valkey. See
[charts/tranquila/README.md](charts/tranquila/README.md) for values and the supported shapes.

The chart is versioned independently of the application: a push to `main` that raises `version`
in `charts/tranquila/Chart.yaml` publishes it, anything else is a no-op. It previously lived in
`pflege-de/helm-charts`.

## Container images

CI publishes images to `ghcr.io/pflege-de-labs/tranquila`:

| Tag | Points at |
| --- | --- |
| `latest` | the newest stable release |
| `1.2.3`, `1.2`, `1` | that release |
| `<short-sha>` | the build of that commit, never moves |
| `main` | the newest build of `main` |
| `pr-<n>` | the newest build of that pull request |

Images are built for `linux/amd64` and `linux/arm64`. A release rebuilds from its tag, so the
release tags share one digest of their own and `tranquila --version` reports the version rather
than a commit sha. Use a `<short-sha>` tag to deploy an exact CI build.

A branch with no open pull request produces no image; run the CI workflow manually
(`workflow_dispatch`) to get a `:<branch>` image for one.

### Verifying a release

Images are signed with cosign using the identity of the workflow that built them — there is no
public key to distribute. They are built by the shared
[pflege-de-labs/github-workflows](https://github.com/pflege-de-labs/github-workflows), so the
certificate names that workflow, and the workflow-repository claim names tranquila:

```bash
cosign verify \
  --certificate-identity-regexp '^https://github\.com/pflege-de-labs/github-workflows/\.github/workflows/image-release\.yml@' \
  --certificate-github-workflow-repository pflege-de-labs/tranquila \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/pflege-de-labs/tranquila:1.2.3
```

> Releases built before the move to the shared workflows were signed by this repository's own
> workflow. Verify those with `--certificate-identity-regexp '^https://github.com/pflege-de-labs/tranquila/'`
> in place of the two certificate options above ([ADR 0008](docs/adr/0008-shared-workflows.md)).

> Signing and attestations start with the first release built by this pipeline. Images for
> `0.4.3` and earlier were built before it existed and carry **no** signature — `cosign verify`
> against them returns `no signatures found`, whichever identity you pass. Those versions were
> also published as `linux/amd64` only; multi-arch starts with the same release.

Release images carry an SPDX SBOM and SLSA provenance as attestations:

```bash
docker buildx imagetools inspect ghcr.io/pflege-de-labs/tranquila:1.2.3 --format '{{ json .SBOM }}'
docker buildx imagetools inspect ghcr.io/pflege-de-labs/tranquila:1.2.3 --format '{{ json .Provenance }}'
```

CI images (`main`, `pr-<n>`, `<short-sha>`) are signed and carry an SBOM too, but no provenance —
the provenance command above returns nothing for them, which is expected; only the release build
sets `provenance: mode=max`.

### Vulnerability scanning

Every image pushed from `main` or a release is scanned with [Trivy](https://aquasecurity.github.io/trivy)
and reported to [SecObserve](https://github.com/SecObserve/SecObserve), the org's vulnerability
management system, along with its SBOM. `pr-<n>` images are built and signed but not scanned —
see [ADR 0006](docs/adr/0006-secobserve-image-scanning.md) for why. Findings do not fail the
build; they land in SecObserve for triage.

Release binaries come with an SPDX SBOM each and a signed `checksums.txt`, which covers the
binaries and their SBOMs:

```bash
cosign verify-blob \
  --bundle checksums.txt.bundle \
  --certificate-identity-regexp '^https://github\.com/pflege-de-labs/github-workflows/\.github/workflows/go-binaries\.yml@' \
  --certificate-github-workflow-repository pflege-de-labs/tranquila \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
sha256sum -c checksums.txt
```

Binaries are published for `linux` and `darwin` on `amd64` and `arm64`.

The full rationale for this pipeline is in
[docs/adr/0001-ci-cd-pipeline.md](docs/adr/0001-ci-cd-pipeline.md), and how it runs on the shared
workflows in [docs/adr/0008-shared-workflows.md](docs/adr/0008-shared-workflows.md).

## Tests

```bash
go test ./...             # unit tests, no containers
cd e2e && go test ./...    # end-to-end, container-backed (~3-4 min)
```

The end-to-end suite drives the resilience behaviour above against a real MinIO,
injecting HTTP faults (504/503/500) and TCP faults (connection resets) to prove
that transient failures are absorbed, watch mode survives an outage, and rate
limits degrade and recover. It is a separate Go module, so it neither slows the
unit suite nor adds container dependencies to the production module.

It needs a container runtime. Docker works as-is; on macOS, Podman works without
Docker Desktop, but the machine must be **rootful**:

```bash
brew install podman
podman machine init --rootful && podman machine start
cd e2e && go test ./...
```

No environment variables are needed — the suite configures the runtime itself,
and skips with an explanation if none is reachable. Apple's native `container`
CLI is **not** supported (it exposes no Docker-compatible API). Full setup,
configuration reference and troubleshooting: **[e2e/README.md](e2e/README.md)**.

## Usage Examples

Sync with environment variables:

```shell
export SOURCE_REGION=us-west-2
export DEST_BUCKET_PREFIX=backup-
export REDIS_ADDR=redis.example.com:6379
./tranquila sync
```

Sync with a config file:

```shell
./tranquila -c tranquila.yaml sync
```

Large bucket — reduce batch size to start syncing sooner:

```shell
./tranquila sync --discovery-batch-size=50000
```

Check sync status:

```shell
./tranquila status my-bucket-1 my-bucket-2
```

Resume after interruption — rerun the same command. Pending and failed objects are retried automatically from Redis state.

## Graceful Shutdown

On `SIGTERM` or `SIGINT`, Tranquila stops accepting new jobs and waits for in-flight transfers to complete before exiting.
