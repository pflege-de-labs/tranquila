# 0009. Recover panics per unit of work, not per process

* Status: Accepted
* Date: 2026-10-19

## Context

No production code called `recover()`, so a panic in any goroutine killed the whole process: the
transfer pool, discovery, and the management server (`/healthz`, `/readyz`) with it. #55 was such
a crash: one `(nil, nil)` return in one listing goroutine took the pod down.

Crash-and-restart is simple, and all state lives in Redis. But a *deterministic* panic on one
object or prefix becomes a crash loop, and every restart pays a full catch-up sync of every bucket
before live events resume.

`recover()` only catches panics on its own goroutine, so a recover must sit on the goroutine
where the work runs.

## Decision

We will recover at three choke points, each turning a panic into the error its unit of work
already reports:

| Site (`site` label) | Where | A panic becomes |
| --- | --- | --- |
| `transfer` | `runJob` in `internal/sync/worker.go`, per job | `Result.Err`, so `processResults` marks the object failed and still calls `OnComplete`, releasing `bucketSem` and the batch `WaitGroup` |
| `list_prefix` | `processOne` in `internal/storage/s3.go`, per sharded-discovery prefix | a per-prefix error, so the walk continues with the other prefixes (#55's case) |
| `discovery` | the per-bucket goroutine in `Syncer.Run` | that bucket's cycle error. This also covers `onPage`, which both discovery strategies call on this goroutine. `listObjectsTree`'s deferred `cancel()` still runs and unblocks its producers |

`internal/recovery.Handle` logs each one at `Error` with its stack and bucket/key/prefix, and
counts it in `tranquila.panics.recovered{site}`.

The top-level loops (watch loop, event loop, results processing, burn-after-reading reconciler,
watcher goroutines) stay **crash-and-restart**. A recovered top-level loop is just dead: the
process would keep answering `/healthz` while doing nothing, which is worse than a restart.

Considered and rejected: keeping the status quo (crash loop on a deterministic bad input), and a
single process-wide recover (impossible in Go across goroutines).

## Consequences

* A panicking object is marked `failed` and retried each cycle instead of crash-looping the pod.
  A recovered discovery panic counts as a transient cycle failure (`storage.Classify` treats it
  as an unknown error), so watch mode backs off and retries.
* Panics no longer show up as pod restarts. Alert on `tranquila.panics.recovered` instead.
* A panic between acquiring a resource and handing it on (e.g. `bucketSem` in `discoverObject`)
  can leak that resource for the rest of the bucket's cycle. Those are per-cycle objects and are
  rebuilt on the next one.
* New goroutines doing per-unit work must add their own recover, or they stay crash-and-restart.
