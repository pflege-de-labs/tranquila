# End-to-end resilience tests

Container-backed tests that exercise tranquila's failure handling against a real
S3-compatible server ([Silo](https://github.com/pgsty/silo), a maintained MinIO
fork), with real fault injection — the behaviour the unit tests can only
approximate with synthetic errors.

- [Quick start](#quick-start)
- [Setup: macOS with Podman](#setup-macos-with-podman-no-docker-desktop)
- [Setup: Linux](#setup-linux)
- [Setup: CI](#setup-ci)
- [Configuration reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [What is covered](#what-is-covered)
- [Design notes](#design-notes)

## Quick start

Given a working container runtime:

```shell
cd e2e
go test ./...
```

Expect roughly 3–4 minutes, and about 230 MB of image pulls on the first run
(Silo 153 MB, Redis 40 MB, Toxiproxy 18 MB, Ryuk 2 MB). No environment
variables need to be set: `TestMain` configures the runtime itself.

```shell
go test -v ./...                              # per-test progress
go test -run TestHarnessSmoke -v ./...        # fixture only, fastest useful check
go test -run TestWatchSurvivesSustainedOutage -v ./...
go test -short ./...                          # skip all container tests
```

Requirements: Go ≥ 1.25 (as declared in `e2e/go.mod`), a container runtime
(below), and outbound access to `ghcr.io` and `docker.io`. Four
containers run concurrently — Silo, Redis, Toxiproxy and Ryuk — so a podman
machine at the 2048 MiB default is tight; 4096 MiB is comfortable.

If no runtime is reachable the tests **skip** with an explanation rather than
failing, so they are harmless on a machine that cannot run them.

## Setup: macOS with Podman (no Docker Desktop)

Verified on macOS 26.6 (Tahoe) / Apple Silicon with Podman 5.8.3.

```shell
brew install podman

# Rootful is the configuration this suite is verified against: Ryuk, the
# testcontainers reaper, wants a privileged container. Rootless is untested
# here and is documented upstream as problematic on macOS.
podman machine init --rootful --cpus 4 --memory 4096   # memory is MiB
podman machine start
```

Verify the Docker-compatible API answers before running the suite:

```shell
podman machine list                     # STATE should be "Currently running"
podman machine inspect --format '{{.Rootful}}'   # must print true
curl -s -o /dev/null -w '%{http_code}\n' \
  --unix-socket ~/.local/share/containers/podman/machine/podman.sock http://d/_ping
# expect: 200
```

Then `cd e2e && go test ./...`. Nothing else is required — `TestMain` finds the
podman socket and sets `DOCKER_HOST` and the Ryuk variables for you.

### If you have an existing rootless machine

Ryuk will fail. Convert it:

```shell
podman machine stop
podman machine set --rootful
podman machine start
```

### Apple's native `container` CLI is not supported

Apple's Containerization framework and `container` CLI (macOS 26) expose **no
Docker-compatible API** — they use their own CLI/XPC interface with one VM per
container, so testcontainers cannot talk to them at all.
[apple/container#131](https://github.com/apple/container/issues/131) ("Support
testcontainers?") was closed without a compatibility layer. Third-party shims
such as [socktainer](https://socktainer.github.io/) exist but describe
themselves as under heavy development and do not support privileged containers,
which Ryuk needs. **Use Podman.**

## Setup: Linux

Docker needs no setup. For Podman, either start the rootful system service:

```shell
sudo systemctl enable --now podman.socket
export DOCKER_HOST=unix:///run/podman/podman.sock
```

or use the rootless user service, which `TestMain` detects via
`$XDG_RUNTIME_DIR/podman/podman.sock`:

```shell
systemctl --user enable --now podman.socket
```

Rootless Podman is untested here; if Ryuk misbehaves, see
[Troubleshooting](#troubleshooting).

## Setup: CI

GitHub Actions `ubuntu-latest` needs nothing: Docker is preinstalled at
`/var/run/docker.sock` and the podman handling is inert. The `e2e` job in
`.github/workflows/e2e.yml`, which CI and the release both call, is simply:

```yaml
- name: Run end-to-end tests
  working-directory: e2e
  run: go test -v -timeout 30m ./...
```

The images are multi-arch, so `ubuntu-24.04-arm` runners work too.

## Configuration reference

`TestMain` sets these only when unset, so an explicit value always wins — CI and
unusual local setups are never overridden.

| Variable | Set to | Why |
| --- | --- | --- |
| `DOCKER_HOST` | `unix://<podman.sock>` | Selects the runtime **and** the testcontainers provider — see [the `bridge` failure](#troubleshooting) for why the path matters. |
| `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` | `/var/run/docker.sock` | Ryuk is bind-mounted the socket by its path *inside* the podman VM, not the host path. |
| `TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED` | `true` | Ryuk needs a privileged container on a podman machine. Note the name — the frequently cited `TESTCONTAINERS_RYUK_PRIVILEGED` does not exist. |

To force a specific runtime, set `DOCKER_HOST` yourself:

```shell
DOCKER_HOST=unix:///var/run/docker.sock go test ./...
```

Useful escape hatches:

| Variable | Effect |
| --- | --- |
| `TESTCONTAINERS_RYUK_DISABLED=true` | Skip the reaper entirely. Cleanup is also registered explicitly via `t.Cleanup`, so the suite does not depend on Ryuk. |
| `TESTCONTAINERS_HUB_IMAGE_NAME_PREFIX` | Pull Docker Hub images through a mirror. Affects Silo, Redis and Ryuk — Toxiproxy comes from `ghcr.io`. |

These four, plus `TESTCONTAINERS_SESSION_ID`, are the complete set testcontainers-go
v0.44.0 reads.

## Troubleshooting

**`unable to find network with name or ID bridge: network not found`**

The signature failure of podman-behind-a-Docker-socket. testcontainers selects
its podman provider by matching the literal substring `podman.sock` in
`DOCKER_HOST`; a podman machine symlinks `/var/run/docker.sock` to its own
socket, so podman looks exactly like Docker, the Docker provider is chosen, and
Ryuk is created on a network named `bridge` that podman does not have and
**refuses to create** (`podman network create bridge` → *"conflicts with a valid
network mode"*).

`TestMain` prevents this by pointing `DOCKER_HOST` at podman's stable
`podman.sock` path. If you set `DOCKER_HOST` manually, make sure the path
contains `podman.sock`. In particular, do **not** use

```shell
podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'
```

as-is: it returns a temp path named `podman-machine-default-api.sock`, which does
not contain that substring and so silently leaves the Docker provider selected.

**`no image found in image index for architecture "arm64"`**

An amd64-only image tag on Apple Silicon. Silo publishes per-arch tags
(`RELEASE.*-amd64`, `RELEASE.*-arm64`) next to the multi-arch `RELEASE.*` one;
pin the plain `RELEASE.*` tag. Keep the pinned tags in `harness_test.go`.

**`start silo: ... unauthorized: access to the requested resource is not authorized`**

A stale pin to `quay.io/minio/minio`. MinIO ended community image distribution
and that repository now answers 401 to anonymous pulls — which is what broke this
suite in CI and prompted the switch to Silo.

**Tests skip with "no container runtime"**

The machine is not running: `podman machine start` (or start Docker). The skip
message names the path it tried.

**Ryuk fails to start and you just need results**

`TESTCONTAINERS_RYUK_DISABLED=true go test ./...`. Containers are still removed
by the explicit `t.Cleanup` calls; you only lose the safety net for a hard crash.
Note that a stale `ryuk.disabled=true` in `~/.testcontainers.properties` applies
to *every* project on the machine and can mask breakage — prefer the env var.

**Leftover containers after an interrupted run**

Each test starts its own Silo + Redis + Toxiproxy stack and removes it via
`t.Cleanup`. A run killed with `SIGKILL` (or `kill -9`) skips those cleanups and
leaves the stack behind — Ryuk reaps such orphans by session label, but not
instantly. Orphans are easy to mistake for a hung run, since `podman ps` shows
long-lived containers while a fresh test is starting new ones. Check what is
actually running before concluding anything:

```shell
podman ps --format '{{.Names}} {{.Image}} {{.Status}}'
```

To clear them by hand:

```shell
podman ps -q --filter ancestor=docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z \
  | xargs -r podman rm -f
```

Prefer `Ctrl-C` over `kill -9` so cleanup runs. Port conflicts are unlikely
regardless: every container binds an ephemeral host port.

**The suite is slow**

Most of the time is the production retry budget, not the containers, which start
in about 8 seconds. See [Design notes](#design-notes).

## What is covered

| Test | Guarantee |
| --- | --- |
| `TestBucketStats*`, `TestListBuckets*`, `TestRebuildStats*`, `TestScriptsUseEvalsha` | The Redis state layer, **run against Redis and Valkey** — see [Key-value engines](#key-value-engines). |
| `TestHarnessSmoke` | The fixture itself: SigV4 survives the proxy, and real SDK errors for 504/502/500/503/403 land in the intended `storage.ErrClass`. |
| `TestSyncCompletesDespiteTransient504` | Regression for the production incident: a 504 burst during discovery is absorbed and every object still syncs. |
| `TestWatchSurvivesSustainedOutage` | Watch mode does not exit while an endpoint is hard down, and syncs once it recovers. |
| `TestWatchTerminatesOnPermanentError` | Misconfiguration (403) still surfaces instead of retrying forever. |
| `TestRateLimitDegradesAndRecovers` | Congestion control halves a configured limit, recovers additively, and never degrades an unlimited endpoint. |
| `TestDestinationDegradesIndependently` | A sick destination throttles writes without slowing source reads. |
| `TestL4FaultsAreTransient` | A TCP-level connection reset classifies as transient. |
| `TestBurnAfterReadingVerifiesContentNotJustSize` | The verify-and-delete path checksums content, not just size: a matching single-part object is verified by ETag alone (no download), a same-size tampered destination is caught by ETag mismatch (no download), and a multipart object correctly falls back to downloading and hashing both sides. |

## Design notes

### Key-value engines

The state layer maintains its per-bucket counters with **Lua scripts**, executed
via `EVALSHA`. Scripting is the most likely place for a Redis-compatible fork to
diverge, so compatibility is tested rather than assumed: every state test runs
against each engine in `kvEngines` (`harness_test.go`).

Verified green:

| Engine | Image | Reported version |
| --- | --- | --- |
| Redis | `redis:7-alpine` | `redis_version=7.4.11` |
| Valkey 8 | `valkey/valkey:8-alpine` | `valkey_version=8.1.9` (`redis_version=7.2.4`) |
| Valkey 9 | `valkey/valkey:9-alpine` | `valkey_version=9.1.1` (`redis_version=7.2.4`) |

Valkey reports a compatibility `redis_version` alongside its own `valkey_version`;
the tests log both, so a passing run names the engine it actually proved.

`TestScriptsUseEvalsha` covers the mechanism rather than just the results: it
exercises the initial script load, a second call served from the cached SHA, and
recovery through the `NOSCRIPT` fallback after a `SCRIPT FLUSH`. An engine with a
subtly different script cache fails there rather than in production.

Not tested, so not claimed: KeyDB, Dragonfly (whose Lua support differs
materially), and managed services such as ElastiCache or MemoryDB. To check
another engine, add it to `kvEngines` and run `go test -run TestBucketStats ./...`.

### A separate Go module

`go test ./...` at the repository root does not descend into nested modules, so
these tests never slow the ordinary unit suite — and testcontainers' ~90
transitive dependencies (moby, containerd, gopsutil) stay out of the production
module's graph and out of `govulncheck`'s scope. A submodule can still import
`internal/...`, because that restriction is lexical on import paths rather than
module-scoped.

### Two fault injectors

**Toxiproxy is L4 only.** Its toxics — `latency`, `down`, `bandwidth`,
`slow_close`, `timeout`, `reset_peer`, `slicer`, `limit_data` — operate on the
TCP byte stream. It has no HTTP parsing, so **it cannot emit a 504, 503 or 500**.

The incident these tests exist to reproduce was an HTTP `504 GatewayTimeout` from
a gateway in front of MinIO, so `faultproxy_test.go` provides an in-process
`httputil.ReverseProxy` that returns a chosen status on demand. It needs no
container and is precisely controllable from the test.

The injector deliberately supports two body shapes, because they exercise
different classification paths:

- **XML body** — the SDK decodes it into a `smithy.APIError`, so classification
  can key on the error code.
- **Non-XML body** (an nginx-style HTML page) — XML decoding fails and the SDK
  surfaces *no* `APIError` at all, leaving only the HTTP status. This is why
  `storage.Classify` must check status before error code.

Toxiproxy still earns its place for the orthogonal L4 faults an L7 proxy cannot
produce, such as a mid-stream connection reset.

The proxy leaves the `Host` header exactly as the client sent it: SigV4 signs
that header, so rewriting it would invalidate every signature. Silo, like MinIO,
accepts a foreign `Host` under path-style addressing.

### Why assertions use HeadObject

`listPageWithRetry` retries 8 times with jittered backoff capped at 30s, so a
*fully* failing list call takes over two minutes. `HeadObject` has no repo-level
retry wrapper, so a failing call costs only the SDK's own attempts — seconds.
Tests that need a failure to surface therefore use `HeadObject`.

For the same reason, `TestWatchSurvivesSustainedOutage` faults the
**destination**: `discoverAndSyncBucket` calls `EnsureBucket` before it lists, so
a faulted destination fails a cycle in seconds instead of minutes.

Raising `listMaxRetries` or the delay cap in `internal/storage/s3.go` will make
this suite noticeably slower.

### Images

Pinned in `harness_test.go`, all multi-arch (`linux/amd64` + `linux/arm64`) so
the same tags work on an Apple Silicon laptop and an x86 CI runner. Renovate bumps
Silo and Toxiproxy (a regex manager in `.github/renovate.json`); Redis and Valkey
float within their major line on purpose and are not tracked:

| Image | Note |
| --- | --- |
| `docker.io/pgsty/silo:RELEASE.2026-09-16T00-00-00Z` | Maintained MinIO fork; keeps `MINIO_*` env vars and `/minio/*` routes. Plain `RELEASE.*` tag is multi-arch; `-amd64`/`-arm64`/`-distroless` suffixes are not what you want here. |
| `ghcr.io/shopify/toxiproxy:2.12.0` | Use ghcr, not Docker Hub: `docker.io/shopify/toxiproxy` is stale at 2.1.4 and amd64-only. |
| `redis:7-alpine` | |
