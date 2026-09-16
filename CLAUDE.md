# Tranquila — Project Context

@AGENTS.md

Human-facing docs: [README.md](./README.md) for usage,
[docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md) for internals,
[e2e/README.md](./e2e/README.md) for the container-backed test suite. Keep this
file for agent-specific context and the decisions behind the code; do not
duplicate the reference material there.

## Architecture

| Layer | Package | Notes |
| --- | --- | --- |
| CLI | `main.go`, `cmd_sync.go`, `cmd_status.go` | kong + kong-yaml. Config bound via `kctx.Bind(cfg)` after parse. |
| Config | `config/` | YAML loaded in `main.go`, passed to `SyncCmd.Run(*config.Config)`. |
| Sync engine | `internal/sync/` | `Syncer`: discover → mark pending → transfer via worker pool. Redis state. |
| Watchers | `internal/watcher/` | `Watcher` interface + poll/MinIO/SQS implementations. |
| Storage | `internal/storage/` | `aws-sdk-go-v2` + transfermanager. Works with any S3-compatible endpoint. |
| State | `internal/state/` | Redis. Keys: `tranquila:obj:{bucket}:{key}`, `tranquila:collection:{bucket}`, `tranquila:stats:{bucket}`, `tranquila:buckets`, `tranquila:statsbuilt`. |
| API | `internal/api/` | Management HTTP API. `/api/v1/buckets`, `/api/v1/sync`. K8s probes: `/healthz` (liveness), `/readyz` (readiness, pings Redis). |

## Implemented Features

### Continuous Watch (`--watch`)

Three backends selected via `--watch-mode`:

| Mode | Flag | Mechanism |
| --- | --- | --- |
| `poll` (default) | `--watch-interval` (default 60s) | Loops `Run()` with sleep. Universal fallback. |
| `minio` | — | `minio-go/v7` `ListenBucketNotification` SSE stream. Reuses source credentials. |
| `sqs` | `--sqs-queue-url` | `aws-sdk-go-v2/service/sqs` long-poll. User pre-configures S3→SQS notification. |

Event-driven modes (`minio`, `sqs`) run an initial full `Run()` on startup to catch missed changes, then switch to event-driven.

Key design: provider-agnostic — polling works everywhere; push modes are opt-in per backend. No SQS/SNS assumption for non-AWS endpoints.

### Structured Bucket Config (YAML)

Replaces awkward `--bucket-mappings "src=dst"` strings for large deployments.

```yaml
buckets:
  - source:
      bucket: sourceBucket
      prefix: foo        # optional
    destination:
      bucket: dstBucket
      prefix: bar        # optional
```

Processed in `cmd_sync.go:resolveBuckets()`. Structured config loaded first; CLI flags (`--bucket-mappings`, `--prefix-mappings`, `--bucket-mapping-file`) are additive and override on conflict (same source bucket).

## Key Design Decisions

- **No S3 event notification config management** — tranquila does not configure SNS/SQS targets on buckets. Users set that up externally. Tranquila only consumes events.
- **MinIO watcher uses `minioNotifier` interface** — `*minio.Client` never referenced directly in `MinIOWatcher`; allows test injection without real server.
- **SQS watcher always deletes messages** — even unparseable ones. Sync is idempotent via Redis; stuck messages in SQS are worse than a missed event.
- **Initial full sync before event loop** — `RunWatcher` calls `Run()` first so objects changed while the program was down are not missed.
- **`runWatch`/`runWatcher` private helpers** — public methods delegate to injectable private versions; enables unit tests without real S3/Redis.
- **`ListObjectsPage` streams via an `onPage` callback, not a return slice** — the old signature accumulated up to `DiscoveryBatchSize` (default 100k) objects in memory across many S3 pages before returning, so `discoverAndSyncBucket` submitted nothing to the worker pool until the *entire* batch was listed. A bucket that's large or hitting transient S3 errors (e.g. 504s) mid-listing looked completely stalled — zero transfers — for the whole batch. The callback fires per underlying S3 page so objects start transferring as soon as they're discovered; the outer per-batch `batchDone.Wait()` still bounds memory/pending count.
- **Per-bucket transfer concurrency cap (`--max-workers-per-bucket`, default: half of `--workers`)** — the worker pool is shared across all buckets in a cycle. Without a cap, a bucket with many (often small) objects can occupy the whole pool via continuous submission, starving other buckets' transfers even though their discovery goroutines are actively queuing jobs. Enforced with a per-bucket semaphore acquired in `discoverAndSyncBucket`'s `onPage` callback and released in `Job.OnComplete`.
- **Delete propagation (`propagate-deletes`) uses two different mechanisms per watch mode** — `minio`/`sqs` already receive an `ObjectRemoved` notification, so `ObjectEvent.IsDelete` (set from `EventName` in `minio.go`/`sqs.go`) drives an immediate destination delete via `runWatcher`'s `eventDispatch`. `poll` mode has no such notification, so it relies on reconciliation: `discoverAndSyncBucket` calls `state.TouchSeen` for every object seen in a listing (only for `PropagateDeletes` buckets, to avoid the extra write for everyone else), and `Syncer.reconcileDeletes` (called at the end of `Run`) finds synced objects whose `seen_at` predates the cycle via `state.ScanStaleObjects`.
- **Reconciliation never deletes on an inconclusive answer** — before deleting the destination, `reconcileDeletes` re-checks the candidate against the *source* with `HeadObject`, classified through `classifyHeadErr`/`storage.Classify`. Only a confirmed `ClassOK` 404 (`NoSuchKey`) counts as "genuinely deleted"; any other error class (transient, throttled, permanent-but-not-404) is treated as inconclusive and retried next pass — a listing gap or a flaky `HeadObject` must never cause a live destination object to be deleted.
- **`--delete-reconcile-interval` is decoupled from `--watch-interval`** — `ScanStaleObjects` is `SCAN`-based (see Redis Key Design), so it pays the same O(whole-keyspace) cost as `RebuildStats` every time it runs, independent of how small the reconciled bucket is. `Syncer.reconcileDue`/`dueForReconcile` throttle it per-bucket; `0` (default) means "every `Run()` call" for consistency with this codebase's other `0 = no throttling` flags, not "disabled".
- **A failed destination delete does not call `state.MarkFailed`** — unlike a failed upload, `needsSync` would then retry a full sync of a source object that no longer exists. `processResults` leaves the record's status untouched on a delete failure so it is picked up again by the next reconcile pass (or the next watcher event, for event-driven modes) instead of looping on a doomed re-upload.
- **`performBurnAfterReading` falls back to ETag, then content hash, when S3-provided CRC32 metadata is unavailable** — the original implementation trusted only the upload response's/`HeadObject`'s CRC32 and refused to delete (permanently, every cycle) when either was empty. Not every S3-compatible destination echoes flexible checksums, so this made burn-after-reading unusable against such a destination. It now shares the same cheapest-first tier order `performVerifyAndDelete` already used for pre-existing objects: CRC32 metadata → single-part ETag → full content hash, only refusing once every applicable tier has been tried.
- **Verification mismatches are a typed `*verifyMismatchError{Bucket, Key, Method, Source, Destination}`, not values interpolated into the error string** — `processResults` extracts it via `errors.As` and attaches `verify_method`/`source_value`/`dest_value` as their own structured log fields on the same `Error()`-level event, rather than relying on a separate `Info`/`Warn` log line (whose fields would be lost if the configured log level filtered it out) or on parsing them back out of free text.
- **Every `ListObjectsV2` attempt gets a per-attempt deadline (`Client.listAttemptTimeout`, configurable via `--list-attempt-timeout`, default 60s)** — root-caused against a real MinIO deployment: a flat listing over a ~295K-object bucket hung with *zero response*, even bypassing every proxy in front of it. Nothing in `storage.Client` previously imposed any per-call deadline (only the long-lived watch-loop `ctx`, cancelled on SIGTERM), so `listPageWithRetry`'s retry loop never even reached attempt 2 — attempt 1 never returned. `storage.Classify` deliberately treats `context.DeadlineExceeded` as `ClassOK` ("cancellation is our own doing") — correct for the caller's own ctx, wrong for a timeout the retry loop imposes on itself, which must be retried like any transient fault. `listAttemptTimedOut(outerCtx, attemptCtx, err)` distinguishes the two cases explicitly rather than relying on `storage.Classify`/`isTransientErr` for this one.
- **A timed-out `ListObjectsV2` attempt escalates the *next* attempt's deadline, and counts as congestion** — two defects found together in production logs where four sharded-discovery prefixes each failed all 8 attempts over ~10 minutes while `GetObject`/`HeadObject`/`DeleteObject` against the same endpoint succeeded continuously. (1) `listPageWithRetry` built every attempt's context with the same `c.listAttemptTimeout`, so a prefix genuinely needing >60s could never succeed no matter the retry count — the loop just burned `listMaxRetries × timeout`. `escalateListTimeout` now doubles the deadline after a *timeout* only (a 504 needs another try, not a longer one), capped at `listAttemptTimeoutMaxFactor` (4x); `listRetryBudget` (10m) bounds the loop, since escalating deadlines can outrun the attempt count. `listMaxRetries` stays 8 so flaky-gateway resilience is unchanged. (2) `recordOp` passed the raw error to `Classify`, which maps `context.DeadlineExceeded` to `ClassOK` — correct for the caller's ctx, inverted for a deadline the retry loop imposed on itself. Each self-inflicted timeout therefore fed `aimd.onHealthy()`, which *raises* the rate limit and resets `consecFail`; with several discovery workers timing out each minute, `consecFail` was reset faster than it could ever reach `--endpoint-fail-threshold`, so the endpoint stayed pinned at "healthy" and tranquila accelerated against a backend that was already too slow. `listErrClass` + `recordOpClass` now report those as `ClassTransient`. This is the same misclassification `listAttemptTimedOut` already fixed for the *retry* decision one line below — it had not been applied to the metrics/AIMD call above it.
- **Prefix-sharded discovery (`sharded-discovery`) walks a bucket the way the MinIO/S3 web console does** — folder-by-folder via a `/`-delimited `ListObjectsV2` (`CommonPrefixes`), instead of one flat, bucket-wide scan a struggling backend may never be able to answer. `storage.listObjectsTree` is the S3-independent orchestration core (tested against a fake `listDelimitedFn`, no real client needed): a bounded-concurrency (`Client.shardedDiscoveryConcurrency`, configurable via `--sharded-discovery-concurrency`, default 4) fan-out over prefixes via a `sync.WaitGroup`-tracked task queue, first-error capture + cancellation, and — critically — **`onPage` is invoked from a single consumer goroutine only** (workers push pages onto a channel rather than calling `onPage` directly), so its existing contract in `discoverAndSyncBucket` (mutates closed-over counters and `bucketSem` with no locking) holds unchanged even though the listing calls producing those pages run concurrently.
- **`--list-attempt-timeout`/`--sharded-discovery-concurrency` exist because narrower prefix scope alone didn't fix a real deployment** — a ~3.4M-object bucket (`year/month/day` structure) kept timing out even with sharding active (confirmed via interleaved concurrent-attempt log lines). Direct isolated testing (bypassing tranquila) found root/year/month-level delimited listings fast (<1s) but a single leaf (day) folder's first page took ~9s in complete isolation — with `shardedDiscoveryConcurrency=4` sharded-discovery workers *and* the transfer worker pool all hitting the same backend concurrently (and `--source-rate-limit` set far above what the backend could actually sustain), real per-call latency plausibly exceeded the previously-hardcoded 60s. Both values were consts; `storage.Config.ListAttemptTimeout`/`ShardedDiscoveryConcurrency` (0 = the same defaults) make them tunable per-deployment without a code change — deliberately source-only (`cmd_sync.go` only threads them into the source `storage.Config`), since nothing lists the destination.
- **Sharded discovery triggers via automatic fallback, not opt-in alone** — a flat listing that exhausts all retries with a transient/throttle error (checked via `isShardableListErr`, which requires the error to be a `*storage.ListError` specifically — see below) automatically retries as a sharded walk for the rest of that cycle, logging a `Warn` recommending the flag for next time. This protects buckets nobody has flagged yet (the whole point — "there might be buckets with more objects" was the ask); the `sharded-discovery: true` flag exists purely as an efficiency escape hatch to skip the now-known-doomed flat attempt (up to `listMaxRetries × listMaxDelay` ≈ several minutes) on buckets already identified as needing it.
- **`storage.ListError{Bucket, Err}` distinguishes "the listing call itself failed" from "onPage failed"** — `ListObjectsPage` wraps only its own `listPageWithRetry` failures in `ListError`; an error returned by the `onPage` callback (e.g. a Redis mark-pending failure) passes through unwrapped. `isShardableListErr` requires `errors.As` to find a `*ListError` before even checking `storage.Classify` — otherwise a transient Redis error (which also classifies `ClassTransient` under `Classify`'s "unknown error" fallback) would incorrectly trigger a sharded-discovery fallback that can't fix a Redis problem.
- **`discoverObject` is shared between `discoverFlat` and `discoverSharded`** — both extract the identical per-object `needsSync`/`TouchSeen`/`MarkPending`/`bucketSem`/`pool.submit` logic from a single method, parameterized by whichever `*sync.WaitGroup` the caller uses for its own batching semantics (`discoverFlat`: per-`DiscoveryBatchSize`-batch, unchanged; `discoverSharded`: one `WaitGroup` for the whole tree walk, waited on once at the end). Job-submission behavior can never drift between the two discovery strategies as a result.
- **`DiscoveryBatchSize`'s pause-while-a-batch-drains pacing does not apply to sharded discovery** — a tree walk has no single linear continuation token to pause on. Backpressure instead comes from the same per-bucket `bucketSem` (`--max-workers-per-bucket`) that already throttles flat discovery, which bounds it identically in practice; documented as an accepted scope limitation rather than replicated with tree-walk-specific batching.
- **`SetCollectionTime` is called right after `EnsureBucket` succeeds, not at the end of `discoverAndSyncBucket`** — it was originally only called on a fully clean discovery cycle, but `discoverFlat`/`discoverSharded` stream and transfer objects (including burn-after-reading deletes) incrementally as they're listed, so a large bucket can make substantial real progress and still return an error from a *later* listing page. That meant the bucket's `tranquila:buckets` index entry / `tranquila:collection:{bucket}` timestamp — which is *all* `ListBuckets` and `internal/api`'s `getBucket` (its 404-vs-200 check) key off — was never written, so an actively-syncing bucket could be permanently invisible in `tranquila status`. Nothing else in this codebase reads this timestamp for sync decisions (confirmed: it's purely a display/existence check), so moving the write earlier is safe. `RunWatcher`'s `initialSync` still runs a full `Run()` (and therefore this call, once per configured bucket) before switching to event-driven mode, so this also closes the gap for minio/sqs watch modes even though `runWatcher`'s own event dispatch never touches this key.
- **`config.Duration` supports `d`/`w` suffixes via `encoding.TextUnmarshaler`, with no custom `kong.MapperValue`/`Decode` needed** — confirmed by reading the vendored kong source (`mapper.go`) that kong's default mapper already checks for `TextUnmarshaler` on any custom scalar type, and `gopkg.in/yaml.v3` does the same for scalar nodes. One `UnmarshalText` implementation (`ParseDuration`: try `time.ParseDuration` first, then a single `d`/`w`-suffixed number) covers both the CLI/env flag and the YAML config field — see `TestKongParsesDuration`/`TestDurationYAML` for the exact behavior this relies on.
- **Burn-after-reading's minimum age is enforced once, at job-submission time (`discoverObject`/`burnEligible`), not inside `transfer`/`performBurnAfterReading`** — `processResults` already decides `RemoveObject` vs `MarkSynced` purely from `Job.BurnAfterReading`, so as long as that flag is set to whether *this job* should actually attempt a delete (`cfg.BurnAfterReading && burnEligible(...)`, not `cfg.BurnAfterReading` alone), nothing downstream needed to change. The alternative (gating the delete itself inside `performBurnAfterReading`/`performVerifyAndDelete`) would have needed a way to report back "delete was skipped" so `processResults` didn't wrongly `RemoveObject` a record whose source is still there — avoided entirely by deciding once, upfront. `runWatcher`'s live-event path uses the identical substitution via the extracted `burnNowForEvent` (mirrors `eventDispatch`'s testability-by-extraction pattern), using `event.ModifiedAt` instead of a listed object's.
- **An unknown/zero `ModifiedAt` refuses burn eligibility rather than assuming "old enough"** (`burnEligible`) — matches this codebase's established "never delete on an inconclusive answer" posture (`classifyHeadErr`/`reconcileDeletes`). Relevant in practice for watch-mode events, since `ObjectEvent.ModifiedAt` is documented best-effort and may be empty.
- **The burn-after-reading min-age reconciler (`Syncer.RunBurnAfterReadingReconciler`) is a genuinely independent polling loop, not reused from propagate-deletes' `reconcileDeletes`** — `reconcileDeletes` only runs at the end of `Run()`, which for event-driven (`minio`/`sqs`) watch modes means exactly once at `RunWatcher` startup (`initialSync`), never again. An object whose burn got deferred for age would never get swept in that mode. `cmd_sync.go`'s `Run()` spawns `RunBurnAfterReadingReconciler` (default `--burn-after-reading-reconcile-interval` 30m) concurrently with whichever watch backend is selected via `sync.WaitGroup.Go`, so it runs on its own schedule regardless of `--watch-mode`/`--watch-interval`. One-shot (non-`--watch`) mode instead calls the single-pass `ReconcileBurnAfterReading` once, directly, alongside `Run()`.
- **`ScanAgedSyncedObjects` reuses the `modified_at` field every object record already carries** (written by `MarkPending`/`UpsertObject`) rather than adding a new per-object write the way `propagate-deletes`' `TouchSeen` needed to — burn-after-reading's age check only ever needs the *source's* last-modified time, which is already tracked for unrelated reasons (`needsSync`'s modification-time comparison). Mirrors `ScanStaleObjects`'s SCAN + pipelined-HMGet shape and the same documented O(keyspace) cost, but — unlike `ScanStaleObjects`, which treats a *missing* `seen_at` as stale (never confirmed present) — a missing/unparseable `modified_at` is excluded, not included: burn-after-reading must never delete a source object whose age it cannot actually confirm.
- **The top-level `runErr != context.Canceled` check became `!errors.Is(runErr, context.Canceled)`** — `cmd_sync.go`'s `Run()` now `errors.Join`s the watch-mode result with the (separately goroutine'd) reconciler's result; a joined value wrapping a bare `context.Canceled` is never `==` to it, so the old direct comparison would have misreported a clean SIGTERM shutdown as a real error.

- **The catch-up sync runs concurrently with the event loop, not before it** (`Syncer.runWatcherWithCatchUp`) — `RunWatcher` used to call `initialSync` sequentially first, and `initialSync` only returns once a whole `Run()` cycle succeeds. A ~3.4M-object bucket whose day-level listings kept timing out therefore meant **no live events were consumed for any bucket at all** (observed: `attempt=179` and climbing, while `orders`/805K and `keycloak-audit-events` completed fine). Both halves now run under `sync.WaitGroup.Go` against a derived context, so a fatal (all-permanent) catch-up still cancels the event loop rather than leaving it running until SIGTERM. Accepted trade-off: while they overlap, one object can get a job from both paths — plain sync is idempotent, and with burn-after-reading the worst case is a spurious `failed` record for an object already verified at the destination, since the destination copy is always verified before any source delete. `TestRunWatcherWithCatchUpDoesNotBlockEventLoop` is the regression test.
- **A prefix whose listing fails no longer aborts the whole tree walk** (`storage.listObjectsTree`) — `setErr` used to `cancel()` the walk on the first error, so one pathological day prefix discarded every other prefix's progress, every cycle, forever. Listing failures are now collected per prefix (capped at `maxReportedPrefixErrs`, plus an "and N more" summary, since a bucket where every prefix is slow would otherwise log hundreds of them each cycle) and returned as `errors.Join` at the end; only an `onPage` failure or ctx cancellation still aborts everything, because those are caller-side and continuing to list would be pointless. Joining `*storage.ListError` values keeps `errors.As` working for `isShardableListErr`.
- **`isShardableListErr` special-cases `context.DeadlineExceeded`** — the per-attempt timeout is the *most common* real trigger for wanting sharded discovery, but `storage.Classify` deliberately maps `DeadlineExceeded` to `ClassOK` ("cancellation is our own doing"), so the automatic flat→sharded fallback could never fire for it. Only `DeadlineExceeded`, never `context.Canceled` — the latter is SIGTERM, not a struggling backend.
- **Sharded discovery persists a per-prefix resume point, not a completion memo** (`tranquila:ckpt:{bucket}:{prefix}`, TTL'd) — this is the deferred "cross-cycle memory" item, unblocked by reframing it. `processOne`'s continuation token used to be a function-local dropped on the error path, so a prefix that died on page 9 of 18 re-listed from page 1 next cycle and died on 9 again: **forward progress per cycle was exactly zero**, which is why no amount of retry/escalation/timeout tuning ever fixed the 3.4M-object bucket. A *completion* memo was what made this hard to invalidate (today's partition grows; burn-after-reading deletes keys under a "done" prefix) — a *resume point* has no completion state to invalidate, and clearing it on completion is load-bearing rather than tidiness: the full re-list next cycle is what rediscovers objects whose **transfer** failed, which resuming permanently past them would strand. Three invariants, each with a named regression test: (1) **only leaf prefixes are checkpointed** — a delimited listing interleaves `Contents` and `CommonPrefixes` in one paginated stream, so resuming past page k also skips the sub-prefixes page k carried, losing whole subtrees *silently*; a prefix that yields a sub-prefix drops its checkpoint and opts out for the rest of the walk (`TestListObjectsTreeBranchingPrefixDropsCheckpoint`). (2) **The token is written by the single consumer goroutine, after `onPage` accepted the page** — `pages` is unbuffered, so page k+1 cannot be sent until page k's `onPage` *and* its checkpoint write finish, making per-prefix ordering total with no extra locking; `listObjectsTree`'s single-consumer property is now required for *correctness*, not only for `discoverAndSyncBucket`'s unlocked counters (`TestListObjectsTreeCheckpointSavedAfterOnPage`). (3) **Checkpoint errors are never fatal** — they degrade that prefix to the pre-checkpointing behaviour. `internal/sync` is untouched: the checkpointer is a property of the source `storage.Client` like `ListAttemptTimeout`, so the prefix never has to be threaded through `onPage`. New failure mode, accepted and bounded: a page the backend can *never* answer now parks its prefix (we resume onto it every cycle instead of re-listing the pages before it); detected within one cycle via `resumed && pagesThisCycle == 0`, logged at `Error`, and bounded by the TTL — auto-skip is impossible since continuation tokens are opaque, and discarding the checkpoint is strictly worse than keeping it. See [docs/adr/0002-discovery-checkpointing.md](docs/adr/0002-discovery-checkpointing.md).
- **A parked prefix is now visible without grepping logs** (`tranquila.s3.discovery.parked_prefixes` gauge, `tranquila status`'s `PARKED` column) — a real deployment's `total` count sat at 525 for a 13M-object bucket, and diagnosing it meant tracing `setStatusScript`'s Lua atomicity, `RebuildStats`'s once-only guard, `discoverObject`'s `MarkPending` gating, and the propagate-deletes/burn-after-reading removal paths by hand to rule out a counter bug before concluding the low count was *genuine* — every one of those paths was already correct, and the actual cause was the "parks" failure mode the bullet above already named and logs at `Error`, just with no queryable signal outside that log line. `listObjectsTree` (the free function, unit-tested directly) now returns a `parked int` alongside its error — counted per walk via the same `resumed && pagesThisCycle == 0` condition already driving the `Error` log, not carried as state across walks: every cycle re-attempts every prefix from the root, so a prefix the backend can answer again simply stops being counted on its next cycle, with no separate "un-parking" logic needed. `Client.ListObjectsTree` (the public wrapper) stores it per bucket (`map[string]int64`, mutex-guarded — several buckets can be walked concurrently on one `Client`) rather than widening the public method's signature, read back via `Client.ParkedPrefixes(bucket)` — the same "operational state the Client tracks, exposed via a getter + a metrics callback" pattern already used for `c.aimd.state()`. The API layer follows the `Endpoints func() (...)` precedent already in `api.Config` rather than importing `storage.Client` directly: `ParkedPrefixes func(bucket string) int64`, wired from `cmd_sync.go` as `src.ParkedPrefixes`, `nil` by default so older callers see the field simply absent. `BucketStatus.ParkedPrefixes` is a `*int64`, not a plain `int64`, so `tranquila status` can print `-` (no source client wired to report it) instead of a misleading `0` — genuinely healthy and unknown are different facts. `TestListObjectsTreeReportsParkedPrefixes` drives the exact two-cycle sequence (first failure → not parked; resumed and still failing → parked; recovered → not parked again) that distinguishes "parked" from "just failed once." The Grafana dashboard's **Discovery** row plots the gauge per bucket, colored by Grafana's own name-hash like the dashboard's other open-set per-bucket panels (see ADR 0004) rather than the validated categorical palette; an absent line means no sharded-discovery cycle has completed yet, not a real zero — mirrored in the panel description too, since that distinction needs to reach whoever is actually staring at the graph.
- **A prefix gets a bounded turn (`--discovery-prefix-budget`, default 10m), because `listRetryBudget` bounds a *page*, not a prefix** — `processOne` paginates serially, so a prefix with 18 slow pages could hold a worker slot for ~3h while every other prefix waited; with `shardedDiscoveryConcurrency=4` on a bucket with ~190 day-prefixes, four pathological ones monopolised every slot and the rest went completely unlisted for the whole cycle. Implemented as a `context.WithTimeout` derived from the walk ctx in `processOne`, so cancelling the walk still cancels the prefix, and a budget-exhausted `list` returns `context.DeadlineExceeded` — which `isTransientErr` reports false for (`Classify` maps it to `ClassOK`), so `listPageWithRetry` returns immediately instead of retrying into the dead budget. **This is only safe because checkpointing landed first**: the prefix resumes from the page it stopped on rather than restarting, so a bounded turn costs nothing and buys whole-bucket coverage per cycle. `0` = default, **negative = unbounded** (not zero, which would silently mean "no listing at all" under the codebase's usual 0-is-default convention). The early-stop log distinguishes `budget_exhausted` (a scheduling decision, logged `Info` when pages were banked) from a real backend failure.
- **`aimd`'s failure counter is a leaky bucket (`failScore`), not a consecutive-failure count** — `consecFail` was zeroed by *any* healthy call, and one `storage.Client` multiplexes discovery listings and the entire transfer pool, so a single successful `GetObject` erased an arbitrarily long run of `ListObjectsV2` timeouts. Observed: four discovery workers timing out ~1/min each while burn-after-reading kept a steady stream of healthy Get/Delete calls flowing — the counter was reset faster than it could reach `--endpoint-fail-threshold`, so the endpoint stayed pinned at "healthy" while visibly failing. "Consecutive" is not a meaningful property of interleaved concurrent calls; outnumbering is. Now `+1` per failure, `-1` per healthy call (floor 0), halve at `failN`. Still event-counted and deterministic — no clock, no sleeps, per the existing constraint. `healthyOps` is deliberately left a true consecutive count that any failure resets: back off readily, ramp up only from a genuinely quiet endpoint. Note this is still inert unless `--source-rate-limit` is finite (`onCongestion` returns early on `base == rate.Inf`).
- **`listPageWithRetry` must never return a nil page with a nil error, and the limiter wait must not share the retry loop's `err`** — the deployed build crashed a pod with `SIGSEGV` in `listDelimitedPage` (`objectsFromContents(bucket, page.Contents)` on a nil page) while sharded discovery walked prefix `2026/4/1/`. `if err = c.wait(ctx); err != nil` assigned the *shared* loop variable, so a successful rate-limiter wait set `err = nil` and erased the previous attempt's failure; the very next statement found `listRetryBudget` spent and `break`ed into `return nil, err` — `(nil, nil)`. Escalating per-attempt deadlines (60s→120s→240s→480s, visible as `next_timeout` in the log) are what push elapsed time past the budget, so this needed a slow backend *and* PR #49's escalation to trigger. Every other `c.wait` call site in `s3.go` already scoped the error with `if err := ...`. The wait error is now scoped, the loop exit wraps the last attempt error (`%w`, so `isShardableListErr`'s `context.DeadlineExceeded` case still fires) and synthesizes one if there is none, the backoff sleep is capped at the remaining budget (it used to sleep up to 30s only to break on the next iteration), and both call sites keep an `errNilListPage` guard — a panic in a discovery goroutine takes down the transfer pool and `/healthz` with it. `listRetryBudget` moved from a const to a `Client` field purely as a test seam (no `Config` field, no flag at the time); `TestListPageWithRetryBudgetExhaustedReturnsError` is the regression test.
- **`listRetryBudget` gained a `--list-retry-budget` flag (0 = default 10m, negative = unbounded), because it silently truncated an escalated `--list-attempt-timeout` with no way to raise it** — a user who raised `--list-attempt-timeout` past what the fixed 10-minute `listRetryBudget` had room left for kept seeing `context deadline exceeded` at *less* than their configured value, because `remaining := c.listRetryBudget - time.Since(began)` becomes the binding constraint on `attemptCtx`'s deadline as cumulative retry+backoff time eats the fixed budget, regardless of how far `escalateListTimeout` had raised `timeout`. Threaded like `DiscoveryPrefixBudget` (`Config.ListRetryBudget` → `NewClient` → `Client.listRetryBudget`, identical 0/negative convention) — but the 0/negative resolution alone was not sufficient: the loop's own `remaining <= 0` break fired immediately on attempt 1 for a *negative* budget, unlike `processOne`'s `prefixBudget > 0` gate around its own `context.WithTimeout`, so the loop needed the same gating, not just the resolved value copied in. Extracted into a small pure function, `budgetRemaining(listRetryBudget, began) (remaining time.Duration, exhausted bool)`, returning a sentinel `unboundedRemaining` (`math.MaxInt64` nanoseconds) rather than a zero-value placeholder needing its own `if bounded` branch at every call site — the same no-nil-pointer, no-branch convention this codebase already uses for `rate.Inf`. Both `min(timeout, remaining)` (the attempt deadline) and `min(delay, remaining)` (the backoff-sleep clamp) then work unconditionally for the unbounded case, and `budgetRemaining` is unit-tested directly with no sleeps (`TestBudgetRemaining`), unlike the loop's own regression test, which does need one real HTTP round trip to prove the loop proceeds past attempt 1 (`TestListPageWithRetryNegativeBudgetNeverBreaksEarly`, bounded by an outer `ctx` timeout rather than run to full completion — the real, unclamped exponential backoff this loop uses, 1s→2s→4s→…→30s per attempt with no test seam to fake that clock, would otherwise make an 8-attempt regression test take minutes). `--discovery-prefix-budget`'s help text and this flag's now cross-reference each other, since the two compound (a page's budget nested inside a prefix's, in sharded discovery — `processOne`'s `prefixCtx` wraps `ctx` before it reaches `listPageWithRetry`) and tuning one without knowing about the other is exactly what produced the original report. A one-time startup `Warn` (`cmd_sync.go`'s `warnIfListBudgetsBindFirst`, source config only) fires when either bounded budget sits below the escalated `--list-attempt-timeout` ceiling (`ListAttemptTimeout × listAttemptTimeoutMaxFactor`), turning a confusing mid-run truncation into a config lint at startup instead — its defaults are duplicated local consts with a cross-reference comment rather than exported package-level consts, since exporting internal tuning constants across the package boundary for one warning is a bigger surface than the warning itself. The two `Dur()` log fields this loop already emits, `retry_in`/`next_timeout`, also moved to `.Str(...)` in the same change: zerolog's default `Dur()` rendering is raw milliseconds with no unit override anywhere in this repo, so a value like `retry_in=2853.70869` reads like seconds but is 2.85s — scoped to just these two fields, not the four other `Dur()` sites in `internal/sync/syncer.go`, since a repo-wide `DurationFieldInteger`/`DurationFieldUnit` change is a separate, independently-reviewable decision that risks changing output for anyone already parsing the current numeric-ms format.
- **The AWS SDK's own unwired stderr logger — "Response has no supported checksum..." — is suppressed, not bridged into structured logging** — `NewClient` never called `awsconfig.WithLogger(...)`, so the checksum-validation middleware's default-on warning (`s3.Options.DisableLogOutputChecksumValidationSkipped`, default `false`) went straight to the SDK's own `os.Stderr` logger (`smithy-go`'s `logging.NewStandardLogger`, prefix `"SDK "`), bypassing zerolog entirely with no bucket/key/request context — expected/benign against non-AWS S3-compatible backends, tranquila's primary target, which commonly don't echo an SDK-recognized checksum header. Tranquila already does its own multi-tier verification elsewhere (`PutObject`'s CRC32, `HeadObject`'s `ChecksumMode`, `performVerifyAndDelete`'s tiered CRC32→ETag→content-hash fallback), so this SDK-level line was never load-bearing for correctness — just noise with nothing to correlate it against. Set via `o.DisableLogOutputChecksumValidationSkipped = true` in `NewClient`'s `clientOpts`, applied unconditionally (both AWS and custom endpoints, both source and destination clients, since they share `NewClient`).
- **This does not make listings faster, and the deployment tuning is part of the fix.** The bucket that motivated it was running at *defaults*: `--sharded-discovery-concurrency=4` under a `--workers=10` discovery semaphore (so up to 40 concurrent LISTs) plus the transfer pool, against a backend measured at ~9s for a single leaf page *in isolation*. Note `--source-rate-limit` defaults to `0` = unlimited, which makes AIMD entirely inert (`onCongestion` returns early on `base == rate.Inf`) — so PR #49's "list timeouts count as congestion" change is a no-op unless a finite limit is configured. Even with one set it would rarely fire: `onHealthy` resets `consecFail` unconditionally and the counter is shared across every op on the client, so the steady stream of healthy burn-after-reading `GetObject`/`DeleteObject` calls keeps clearing the streak before 4 list workers can accumulate 5 *consecutive* failures. Both are real defects, both deliberately left out of the checkpointing change.
- **Superseded limitation (kept for the arithmetic): the tree walk used to have no cross-cycle memory.** Every failed cycle re-lists from scratch. Measured in isolation, a day-prefix page in the problem bucket costs ~8–9s *even at `MaxKeys=1`* (already the S3 API maximum, so there is no page-size lever), ~18 pages/day × ~190 day prefixes — a full walk is hours. Per-prefix checkpointing in Redis was considered and deliberately deferred until we can see how much of the bucket now completes; it needs careful invalidation (the current day's partition keeps growing, and burn-after-reading deletes keys underneath an already-"completed" prefix).

- **The Grafana dashboard (`deploy/grafana/`) shows no latency quantile, deliberately** — `tranquila.s3.operation.duration` was instrumented without explicit histogram boundaries, so it uses the OpenTelemetry defaults whose largest finite bucket is 10 000 ms. Every list attempt in the pathology this project has spent its history on lives above that line (per-attempt deadline 60s escalating to 240s), so a `histogram_quantile` there interpolates between `le=10000` and `+Inf` and reports whatever the estimator's tail assumption says, not a measurement. The panels use `rate(sum)/rate(count)` (an exact mean) and `rate(count) - rate(bucket{le=10000})` (an exact count of calls past the ceiling) instead. Two related gaps are recorded rather than papered over: that histogram carries no `endpoint` attribute, so source and destination latency are pooled; and there are no discovery metrics at all, so a bucket stuck in a tree walk is visible only indirectly. Prometheus 3 normalises bucket bounds to `10000.0` while Prometheus 2 keeps `10000`, so `le` is matched with a regex — an exact match silently returns no data on one of them, which is indistinguishable from "no slow calls". Exported metric names were read off a live `/metrics` endpoint, not derived from the OTel naming rules (`{call}/s` becomes `_per_second`, which is not guessable). See [docs/adr/0004-grafana-dashboard.md](docs/adr/0004-grafana-dashboard.md).

- **Every pushed and released image is scanned with Trivy and reported to SecObserve, via `SecObserve/secobserve_actions_templates` — but `SO_API_BASE_URL` is a public gateway, not the backend's own hostname.** `api.secobserve.p4e.io`, the naive guess, resolves to `192.168.3.6` — unreachable from the `ubuntu-latest` GitHub-hosted runners every job in `ci.yml`/`release.yml` already runs on. `https://webhooks.management.p4e.io/secobserve` is the public route actually used, confirmed reachable with a live request (not assumed) before it went into the workflow. The requested package name was double-checked the same way earlier in this session for an unrelated CLI-completion task and turned out to be right where a lazy grep-for-"not found" said it was wrong — the API-URL check here applied that same standard, and this time the naive guess really was wrong. **`SO_API_TOKEN` does not exist as a repository secret yet** (`gh secret list` returns nothing) — both workflows' new steps fail loudly until a SecObserve user with the `Upload` role creates one. The SBOM uploaded is the same attestation `docker/build-push-action`'s `sbom: true` already produces (extracted via the identical `docker buildx imagetools inspect ... .SBOM` command `release.yml` already used to assert its presence), not a second SBOM generated by Trivy or anything else — one image, one SBOM. `ci.yml`'s new steps are gated on `github.event_name != 'pull_request'`, stricter than the existing fork-only push gate: SecObserve auto-creates a "branch" per `SO_BRANCH_NAME` on first import, and a transient PR branch has no reason to leave a permanent trace in a findings tool the way `main` does. See [docs/adr/0006-secobserve-image-scanning.md](docs/adr/0006-secobserve-image-scanning.md), superseded by [docs/adr/0007-secobserve-upload-correctness.md](docs/adr/0007-secobserve-upload-correctness.md).
- **`docker buildx imagetools inspect --format '{{ json .SBOM }}'` does not print an SBOM for a multi-platform image — it prints a platform-keyed map of them**, `map[string]sbomStub` from buildx's `tplInputs.SBOM()` (`util/imagetools/printers.go`), i.e. `{"linux/amd64":{"SPDX":{…}},"linux/arm64":{"SPDX":{…}}}`. SecObserve identifies an upload by trying every parser's `check_format`, and the SPDX one requires `SPDXID`/`spdxVersion` at the *top* level, so every upload since #71 came back `400 {"message":"No suitable parser found"}` — on both `main` CI runs and on both attempts of the `v0.6.0` release. Both workflows now unwrap with `jq` into `sbom-linux-amd64.json`/`sbom-linux-arm64.json` (two uploads: SecObserve keys a `Vulnerability_Check` on `(product, branch, service, filename)`, so they do not overwrite each other) and **assert `SPDXID`/`spdxVersion` before uploading** — verified against the real published `v0.6.0` attestations pulled from GHCR, where the old command's output fails the guard and both unwrapped documents pass it.
- **`actions/upload_sbom` cannot fail a job, so that 400 was invisible for two weeks** — its entrypoint `file_upload_observations.sh` is an `sh` script with no `set -e` whose last command is `deactivate`, discarding the Python uploader's `exit(1)`. `actions/SCA/trivy_image` reports failures only because *its* entrypoint (`entrypoint_trivy_image.sh`) sets `-e` and `source`s the same script — the asymmetry is upstream's, not ours, and is present in `dev/actions/upload_sbom` too. The fix is not a reimplementation: the pinned scanner image already contains `/entrypoints/entrypoint_upload_sbom.sh` (`set -e` + `source file_upload_sbom.sh`, posting to the dedicated `…/file_upload_sbom_by_name/` endpoint), so both workflows `docker run` that image by digest with `--entrypoint` set to it. Renovate's `github-actions` manager cannot see an image reference inside a `run:` block, hence the `customManagers` regex on `SCANNER_IMAGE:` in `.github/renovate.json`.
- **Both SecObserve steps are `continue-on-error: true`, reported through a `::warning` and a job-summary note** — ADR 0006 deliberately made them blocking and flagged the coupling as "revisit if too tight"; it was too tight on the first release. A `500` from SecObserve's import endpoint left `ghcr.io/pflege-de-labs/tranquila:0.6.0` pushed, signed and verified while `binaries` (`needs: [verify, image]`) never ran, so there was **no `v0.6.0` GitHub release and no binaries**. The two failure modes 0006 conflated are different: a *scan* that skips itself reports a false clean bill of health, an *upload* that fails reports nothing and has a signed, verified image behind it already. The gates that are real gates — version check, `cosign sign`/`verify`, the attestation presence check, and the new SPDX guard — stay blocking.
- **That `500` is not a payload problem and is not fixable in this repository.** Auth, product lookup and branch creation all succeeded (the SBOM request in the same job reached *parser detection*, which in `api/views.py` runs after `user_has_permission_or_403` and `Branch.objects.create`), and `so_suppress_licenses` is not the differentiator against the passing `main` run because the upstream action **defaults it to `true`**. The body is Django's default `Server Error (500)` HTML, so the exception escaped DRF's handler and the traceback lives under the `django.request` logger, not the `secobserve.*` lines. The deployment's own logs show a `psycopg.OperationalError: consuming input failed: SSL error: unexpected eof while reading` the day before — an intermittent DB connection drop is the leading candidate.
- **`tranquila completion` uses `king.Completer.Out()`, never `.Write()`** — every `king.Completer` implementation (`Bash`/`Zsh`/`Fish`) writes a file into the current working directory (`tranquila.bash`/`_tranquila`/`tranquila.fish`) as an *unconditional* side effect of `Write()`, in addition to any `io.Writer` passed in. A `completion` subcommand's job is to print a script for the caller to `source`/redirect themselves — silently dropping a file into whatever directory the user happened to run the command from would be a surprising, unwanted side effect. `Out()` returns the generated bytes with no such effect; the subcommand writes them to `kctx.Stdout` itself.
- **The completion script is generated from `kctx.Model.Node`, not a static/duplicated flag list** — king's `Completion(k *kong.Node, altname string)` walks the live, already-built kong grammar, so a new flag or subcommand is picked up automatically the next time completion is regenerated; nothing about the CLI surface is repeated by hand. Verified end-to-end, not just "it builds": the generated bash script was sourced in a real bash process and driven through `_tranquila_completions` directly — `sync --list<TAB>` narrows to the one real flag matching (`--list-attempt-timeout`), and zsh/fish outputs were syntax-checked with `zsh -n`/`fish -n`.

## Dependency Updates

Renovate groups Go dependencies by **what must move together**, not by manager: `aws sdk` (~20
modules sharing internal version constraints — a partial bump does not compile), `opentelemetry`
(one release train), `e2e test dependencies`, and the `go` toolchain directive alone (never
automerged). Everything else gets its own PR, and `separateMajorMinor` is on.

Two traps worth not rediscovering:

- **`e2e/go.mod`'s entries are all `// indirect`**, because the module has a `replace` to `../` —
  so every *direct* dependency of the root module is an *indirect* one of e2e's. Renovate does not
  manage indirect Go dependencies by default, so a root bump used to leave `e2e/go.mod` pinned to
  the old versions; the module graph then disagreed with itself and every command that loaded it
  failed with an opaque `go: updates to go.mod needed` (PR #50). A packageRule enables them
  explicitly, and `go-checks.yml` checks both modules are tidy by name so the next occurrence says
  so directly.
- **`ignoreTests: true` used to be set globally** while docker and github-actions automerged, so
  those merged without CI passing — including the actions that push to GHCR and sign with this
  repo's OIDC identity. Removed; automerge now waits for CI.

Rationale in [docs/adr/0003-dependency-update-policy.md](docs/adr/0003-dependency-update-policy.md).

- **The Helm chart lives in this repo at `charts/tranquila`, and its two "bring your own" values were dead on arrival** — the chart moved out of `pflege-de/helm-charts` during the org migration. `values.yaml` documents top-level `existingSecret` and `existingConfig`, but the templates read `config.existingSecret` and `config.existing` (the latter a key that exists nowhere in `values.yaml`), so the documented spelling was unreachable — and `existingSecret` did not merely no-op, the render *failed* on the `required` calls guarding the credential keys. The helpers `tranquila.existingSecret`/`tranquila.existingConfig` now prefer the top-level key and fall back to the nested one, because reading only the documented key would break every values file that had worked around the bug using the spelling the chart's own notes recommended. The chart is versioned independently of the application (`appVersion` tracks the app), so a push to `main` raising `version` in `Chart.yaml` publishes it and nothing else does. CI's `chart` job lints and renders every combination under `charts/tranquila/ci/` and then asserts what came out — rendering proves a chart is not broken, it says nothing about a chart that is quietly wrong, which is exactly what this bug was. See [docs/adr/0005-repository-and-chart-consolidation.md](docs/adr/0005-repository-and-chart-consolidation.md).

## Configuration Reference (YAML)

All of the below nests under a top-level `sync:` key — `Source`/`Destination`/`Buckets`/etc.
are `embed:""` fields flattened onto the `sync` subcommand, and kong-yaml's resolver
builds its lookup path from the command tree, so a top-level `source:`/`redis:`/etc.
(no `sync:` wrapper) is silently ignored. Likewise, flag names use hyphens
(`access-key`, not `access_key`) — the underscore variant is silently ignored too.

```yaml
sync:
  source:
    endpoint: ""           # empty = AWS; set for MinIO/compatible
    region: us-east-1
    access-key: ""
    secret-key: ""

  destination:
    endpoint: ""
    region: us-east-1
    access-key: ""
    secret-key: ""

  # Structured bucket mappings (preferred for multiple buckets)
  buckets:
    - source:
        bucket: src-bucket
        prefix: optional/prefix
      destination:
        bucket: dst-bucket
        prefix: optional/prefix

  # Legacy string mappings (still supported)
  bucket-mappings: []        # "name" or "src=dst"
  bucket-mapping-file: ""
  dest-bucket-prefix: ""

  redis:
    addr: localhost:6379
    password: ""
    db: 0

  workers: 10

  # Resilience (see "Failure Handling" below)
  cycle-backoff: 5s
  cycle-backoff-max: 10m
  endpoint-fail-threshold: 5

  # Cadence for propagate-deletes reconciliation (poll mode + initial
  # catch-up sync; see "Key Design Decisions"). 0 = every Run() call.
  delete-reconcile-interval: 0s

  telemetry:
    exporter: prometheus     # prometheus | otlp | none
    addr: :8081
    otlp-endpoint: ""
```

## Testing

| Scope | Command | Notes |
| --- | --- | --- |
| Unit | `go test ./...` | Stdlib `testing`, table-driven. No containers, no sleeps. |
| End-to-end | `cd e2e && go test ./...` | Separate module (`github.com/pflege-de-labs/tranquila/e2e`) with a `replace` to `../`. Root `go test ./...` does not descend into it. |

The e2e module is separate on purpose: testcontainers pulls ~89 transitive
dependencies (moby, containerd) that must not enter the production module graph
or `govulncheck` scope. A submodule can still import `internal/...` because the
internal rule is lexical on import paths, not module-scoped.

Two fault injectors, because they cover different layers:

- **Toxiproxy** (container) is L4 only — `latency`, `down`, `bandwidth`,
  `slow_close`, `timeout`, `reset_peer`, `slicer`, `limit_data`. It has no HTTP
  parsing and **cannot emit 504/503/500**.
- **`faultproxy_test.go`** is an in-process L7 `httputil.ReverseProxy` that
  injects HTTP statuses, which is what the production 504 incident needed. It
  emits both XML bodies (SDK decodes an `APIError`) and non-XML gateway pages
  (no `APIError` at all — the case that forces status-first classification).

Gotchas worth not rediscovering: assertions use `HeadObject` because
`listPageWithRetry`'s 8 jittered attempts make a failing list take 2+ minutes;
podman needs `DOCKER_HOST` pointed at a path containing `podman.sock` or Ryuk
dies on the missing `bridge` network; Apple's `container` has no Docker API and
cannot run testcontainers at all. Details in `e2e/README.md`.

## Redis Key Design

`SCAN ... MATCH` filters **server-side after iterating**: `COUNT` bounds keys
examined per call, not keys returned. A pattern scan therefore costs
O(whole keyspace) no matter how few keys match — and this keyspace is dominated
by `tranquila:obj:*` records (~1.1M in production). Anything on a request path
must avoid scanning.

| Key | Purpose |
| --- | --- |
| `tranquila:obj:{bucket}:{key}` | Per-object record. Bucket names cannot contain `:`, object keys can — split on the first `:` after the prefix. |
| `tranquila:collection:{bucket}` | Last discovery timestamp. |
| `tranquila:stats:{bucket}` | Maintained counters (`total`/`synced`/`pending`/`failed`), so `BucketStats` is one HGETALL. |
| `tranquila:buckets` | Set indexing discovered buckets, so `ListBuckets` is one SMEMBERS. |
| `tranquila:statsbuilt` | Marker that counters have been seeded. |

Counters are updated **atomically with the status write** by the Lua scripts in
`state.go` (`setStatusScript`, `deleteObjectScript`), which read the previous
status to decrement the right field. Every write path must go through
`setStatus` / `deleteObjectScript` or counters will drift.

Because those scripts run via `EVALSHA`, engine compatibility is tested rather
than assumed: `e2e/harness_test.go` defines `kvEngines`, and every state test
runs against Redis 7, Valkey 8 and Valkey 9 (all green, including recovery from
`SCRIPT FLUSH` via the `NOSCRIPT` fallback). Add an engine there to check it.

`RebuildStats` recomputes everything from the object records in a **single**
pass (not one per bucket) and reconciles both the counters and the bucket index.
It runs automatically when `tranquila:statsbuilt` is absent, which seeds an
upgraded keyspace on first read. **To force a reconcile, delete that marker** —
that is the operator escape hatch for drift.

`ScanPending` still scans per bucket, but currently has no callers.

`propagate-deletes` reconciliation adds a `seen_at` field to `tranquila:obj:{bucket}:{key}` (written by `TouchSeen`, a plain `HSET` — no counter mutation, not a status transition) and a second per-bucket scan, `ScanStaleObjects`, that pays the same keyspace-wide `SCAN` cost as `ScanPending`/`RebuildStats`. Both `TouchSeen` and `ScanStaleObjects` only run for buckets with `PropagateDeletes` enabled, so buckets that don't opt in pay neither the extra write nor the scan.

### Redis connection pool recovery

A reported "pool never recovers after a Redis/Valkey outage, only a tranquila
restart fixes it" turned out, on investigation, not to be a bug in tranquila's
own code: only one `*redis.Client`/`*state.Store` is created for the process
lifetime (`cmd_sync.go` `Run()`), it's never closed/recreated mid-run, Redis
errors classify as `ClassTransient` (not fatal — `isFatalCycleErr` doesn't
terminate the watch loop over them), and no Redis call in the sync path can
deadlock or leak a goroutine/semaphore slot on failure.

go-redis's own pool (`internal/pool/pool.go`) trips into a degraded mode once
`dialErrorsNum` reaches `PoolSize` consecutive dial failures: further callers
get a cached error immediately, and a single background `tryDial()` goroutine
probes once/sec, resetting the counter on the first successful dial. This is
confirmed **by design and self-healing** by the go-redis maintainers —
[redis/go-redis#3062](https://github.com/redis/go-redis/issues/3062), closed
as "application related, not client related." `MinIdleConns`'s replenishment
(`checkMinIdleConns`) goes through the *same* `dialConn`/`dialErrorsNum` gate,
so setting it does **not** provide an independent recovery path — deliberately
not configured for that reason.

Root cause of the original report was never conclusively identified (address
was a stable Kubernetes Service ClusterIP, ruling out stale pod-IP caching).
Two changes were made anyway, both independently justified regardless of root
cause: `processResults` previously discarded every `MarkFailed`/`MarkSynced`/
`RemoveObject` error (`_ = s.cfg.State.MarkX(...)`) — during an outage this
left zero tranquila-level log evidence beyond go-redis's own low-level pool
spam; `logStateWriteErr` now logs them at `Warn`. `RedisConfig.PoolSize`
(`--redis-pool-size`, default `0` = go-redis's `10 * GOMAXPROCS`) makes the
dial-error trip threshold explicit and predictable rather than tied to the
container's CPU limit, and gives an operator escape hatch to size it smaller
(reaches the steady single-prober recovery state sooner, trading redial log
noise for a small window with no dial attempts at all) or larger.

## Failure Handling

Watch mode must never exit on a transient endpoint fault — `os.Exit` also kills
the in-process mgmt server, so `/healthz` stops answering and K8s restarts the pod.

| Layer | Mechanism |
| --- | --- |
| Error classification | `storage.Classify` → `ClassOK`/`ClassTransient`/`ClassThrottle`/`ClassPermanent`. HTTP status first (`*awshttp.ResponseError`), then smithy error code. Unknown → transient. |
| Per-call retry | AWS SDK retryer at `s3MaxAttempts` (5); `listPageWithRetry` at 8 attempts, 30s delay cap, jittered. |
| Per-cycle retry | `runWatch`/`initialSync` back off with `cycleBackoff` (jittered exponential) and never return on transient errors. |
| Rate degradation | `internal/storage/aimd.go`, fed one signal per S3 call from `recordOp`. Halve after N transient failures (immediately on throttle), floor 1/s, additive recovery of 10% of base per 20 healthy calls. |

Key decisions:

- **Only permanent errors terminate**, and only when *every* failure in the cycle
  is permanent (`isFatalCycleErr` fans out one level of `errors.Join`). A mixed
  cycle counts as transient so a flaky endpoint cannot look like misconfiguration.
- **One-shot mode is unchanged** — still exits non-zero, so a K8s Job reports failure.
  The asymmetry falls out of the retry loop living inside `runWatch`.
- **`Run` returns `errors.Join`**, not the first error, so a partial-failure cycle
  keeps every bucket's failure visible.
- **Only endpoints with a configured `rate-limit` degrade.** Unlimited has no
  ceiling to halve, and inventing one would throttle a healthy endpoint.
- **The limiter is always constructed** (`rate.Inf` when unlimited) so the pointer
  is never nil and never swapped — only `SetLimit` mutates. Note `rate.Inf` is
  `Limit(math.MaxFloat64)`, *not* IEEE infinity: compare with `== rate.Inf`.
- **AIMD is event-counted, never time-based**, so the control loop is deterministic
  under test — no clock injection, no sleeps.
- **`/readyz` stays green while degraded** — degraded is the designed correct
  response; marking the pod NotReady sheds no load and stalls rollouts.
- **Detached transfers are time-bounded** (`transferGrace`) so a degraded limiter
  cannot pace an uncancellable transfer past `terminationGracePeriodSeconds`.

## CLI Flags Added This Session

```text
--watch                  enable continuous sync mode
--watch-mode             poll|minio|sqs (default: poll)
--watch-interval         inter-cycle sleep for poll mode (default: 60s)
--sqs-queue-url          SQS queue URL (sqs mode only)
--cycle-backoff          base retry delay after a failed cycle (default: 5s)
--cycle-backoff-max      cap for the retry delay (default: 10m)
--endpoint-fail-threshold transient failures before halving the rate (default: 5)
```

## Dependencies Added

- `github.com/minio/minio-go/v7` — MinIO native event subscription
- `github.com/aws/aws-sdk-go-v2/service/sqs` — SQS long-poll
- `github.com/miekg/king` — bash/zsh/fish completion generation for `tranquila completion`, driven off
  the live kong grammar rather than a hand-maintained script. Pulls in `gomarkdown/markdown` and
  `mmarkdown/mmark/v2` transitively (king's man-page generator, in the same package as the
  completion generators, unused here) — small enough (4 indirect modules total) not to warrant the
  separate-module treatment `e2e`'s testcontainers dependency needed for ~89.
