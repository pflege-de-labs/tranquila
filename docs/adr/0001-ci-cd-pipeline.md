# 0001. Images are tagged by metadata-action, releases rebuild and are signed

* Status: Accepted, amended by [0008](0008-shared-workflows.md)
* Date: 2026-09-11

## Context

The pipeline this record replaces had three defects.

**Image tags were hand-rolled shell.** The image build lived in its own `docker-build.yml`,
triggered by `workflow_run: [CI]` so that no image was produced for a commit whose tests failed.
A `workflow_run` event carries no branch or pull-request context, so `docker/metadata-action`'s
`type=ref,event=branch` and `type=ref,event=pr` autodetection could not work and the workflow
computed tags with `tr` and `${sha:0:7}` instead. Pull-request builds got no `pr-<n>` tag at all.
Because CI ran on both `push: branches: ["**"]` and `pull_request`, a branch with an open PR
triggered everything twice, and two separate `should-run` jobs existed purely to suppress the
duplicate.

**There was no supply-chain evidence.** Nothing in the repository used cosign, syft, SBOMs,
provenance or attestations. A consumer could not tell that an image came from this repository,
nor what was inside it.

**`main.version` was never injected.** `main.go` declares `var version = "dev"` and hands it to
kong for the `--version` flag, but the Dockerfile built with `-ldflags="-s -w"` only. Every
published image reported `dev`.

Separately, the release was a retag: it pulled the `<short-sha>` image built earlier by CI,
applied semver tags and pushed them. That fixed the release's metadata at whatever CI happened to
record, and made the release fail outright if that image had since been garbage-collected.

## Decision

**The image build moves into `ci.yml`**, gated by `needs: checks` on a reusable `go-checks.yml`
workflow. This keeps the "no image for a failing commit" guarantee that `workflow_run` provided,
while restoring the real event context that `metadata-action` needs. The triggers become
`pull_request` (unfiltered, so stacked PRs onto a non-`main` base are covered too),
`push: branches: [main]`, and `workflow_dispatch`. With no `push: branches: ["**"]` there is no
duplicate run, so both `should-run` jobs are deleted.

**The Go checks live in one reusable workflow.** `ci.yml` and `release.yml` both call
`go-checks.yml`, which carries a `working-directory: e2e` vet step, the container-backed suite and
`govulncheck`. Duplicating those across two files would let the e2e invocation drift, and there is
no Makefile to hide the details behind. The e2e job gains a `docker info` precondition, because the
suite *skips* rather than fails when no container runtime is reachable — without it, a runner-image
change that dropped Docker would silently turn the gate into a green no-op.

**Releases rebuild rather than promote.** A tag runs `guard` → `verify` → `image` → `binaries`.
Rebuilding means the CI run of the commit no longer gates the release, so the gate moves into the
release itself — including the e2e suite, which at three to four minutes is the only test that
exercises real S3, real TCP faults and real Redis. `VERSION` is the git tag, so `tranquila
--version` reports `v1.2.3` and the OCI labels describe a release rather than a branch build.

**A release must be cut from `main`.** The `guard` job fails when `git merge-base --is-ancestor`
says the tagged commit is not reachable from `origin/main`. It is a separate job so a misplaced tag
fails in seconds, before the test suite and a two-platform build have run.

**`:latest` follows the highest stable tag**, replacing the previous "is this the tip of `main`"
check. That check had a live race here: `.github/renovate.json` sets `automerge: true` with
`ignoreTests: true`, so a dependency bump can land between the tag push and the workflow start,
`origin/main` moves, and `:latest` is silently not applied — no error, discovered days later. The
highest-stable-tag rule is deterministic, and additionally declines `:latest` for a pre-release and
for a re-run of an older tag. Under `guard` every released tag is on `main` by construction, so the
only remaining question is which tag is newest.

**The image is cross-compiled.** The Dockerfile builds `FROM --platform=$BUILDPLATFORM` and sets
`GOOS`/`GOARCH` from `TARGETOS`/`TARGETARCH`, so `linux/amd64,linux/arm64` costs two native
compiles instead of one native and one under QEMU. The Go build cache mount is keyed per target so
the two legs do not evict each other.

Supply-chain evidence is produced for both artifact kinds:

* The release image build passes `sbom: true` and `provenance: mode=max`, so buildx attaches an
  SPDX SBOM and SLSA provenance as in-toto attestations on the index. The workflow asserts both are
  present rather than trusting that they were produced.
* Every pushed image — release, `main`, and same-repository pull request — is signed with cosign
  **by digest**, which covers every tag pointing at that build at once. Pull requests from forks get
  no registry credentials and no OIDC token, so they build both platforms without pushing and are
  not signed; building without pushing is what validates a Dockerfile change in a fork PR.
* Each released binary gets an SPDX SBOM from syft, read out of the Go build information the linker
  embeds. `-s -w` strips the symbol table and DWARF but leaves `runtime/debug.BuildInfo` intact.
* `checksums.txt` covers the binaries and their SBOMs, and that one file is signed with
  `cosign sign-blob`, so a single signature authenticates every release asset.

Signing is keyless. The identity is the workflow's OIDC token, so there is no private key to
generate, store or rotate, and `id-token: write` is the only prerequisite.

**CI images set `provenance: false`.** This is a deliberate divergence from the release path.
`docker/build-push-action` otherwise attaches `mode=min` provenance to every registry push, and each
attestation becomes an extra manifest that GHCR renders as an `unknown/unknown` platform row. Min-mode
provenance asserts which workflow built the image — which the cosign signature already proves
cryptographically. Doing it on every branch and every PR push buys nothing and makes the package
listing unreadable. Do not "fix" this back.

`sbom: true` is likewise release-only: it runs a syft scan per platform on every build, and nobody
audits the SBOM of `pr-42`.

Release binaries ship for `linux/amd64`, `linux/arm64`, `darwin/amd64` and `darwin/arm64`. Darwin is
included because `main.go` already carries a `runtime.GOOS == "darwin"` branch for the memory-limit
provider, and `tranquila status` is an operator command someone runs from a laptop against a
port-forwarded Redis. Windows is not shipped: no code path acknowledges it and the memory-limit
handling is cgroup-centric.

## Consequences

* **A branch with no open pull request no longer gets an image.** `AGENTS.md` requires a branch and
  a PR for every change, so such a branch is normally short-lived; `workflow_dispatch` is the escape
  hatch when one genuinely needs a `:<branch>` image. Restoring `push: branches: ["**"]` would mean
  restoring both dedup jobs, which is the thing this record removes.
* **Pull-request images are tagged `pr-<n>`, not `<branch>`.** The `<short-sha>` tag is unchanged, so
  anything pinned to a digest or a sha is unaffected.
* **A release image is not bit-identical to the `<short-sha>` image of the same commit.** The
  `<short-sha>` tag remains the way to deploy an exact CI build; the semver tags are the release.
* Dependencies, the Go toolchain and the base image are re-resolved at release time, which is why
  `verify` exists. A release can fail on code that passed CI earlier — that is the point.
* The release binaries and the binary inside the image are built by different Go toolchains:
  `go-version: stable` on the runner versus the digest-pinned `golang` image in the Dockerfile. They
  agree today and drift independently. Reading the Dockerfile's version from the workflow is more
  machinery than the problem deserves.
* Keyless signing records the repository, workflow and commit in Sigstore's public transparency log,
  for pull-request builds as well as releases. For a repository that must not disclose those,
  key-based signing with a `COSIGN_PRIVATE_KEY` secret is the alternative, at the cost of key
  custody.
* Verification requires cosign and network access to Sigstore. The commands are in the README.
* cosign signatures and buildx attestations live in the registry as untagged manifests. A GHCR
  cleanup workflow that prunes untagged manifests — a common reaction to a large package — would
  destroy them. None exists today; if one is added it must exclude them.
* Renovate now manages three further actions, two of which push to the registry and sign with this
  repository's OIDC identity, while `.github/renovate.json` still sets `ignoreTests: true` with
  `automerge: true` for the github-actions manager. Tightening that is worthwhile follow-up work and
  is out of scope here.
