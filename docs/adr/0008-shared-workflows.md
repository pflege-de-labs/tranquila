# 0008. CI and release run on the shared pflege-de-labs workflows

* Status: Accepted
* Date: 2026-10-08

## Context

The pipeline of [ADR 0001](0001-ci-cd-pipeline.md) and the SecObserve upload of
[ADR 0007](0007-secobserve-upload-correctness.md) were written here and copied into compactor,
teamster and nats-auth-callout, where the copies drifted. They now live once, as reusable
workflows in [pflege-de-labs/github-workflows](https://github.com/pflege-de-labs/github-workflows).

## Decision

`ci.yml`, `release.yml` and `release_helm_chart.yml` call the shared workflows, pinned to a commit
sha with the release in a comment: `go-checks`, `helm-lint`, `image-build`, `secobserve-image`,
`release-guard`, `image-release`, `go-binaries` and `helm-release`. What ADR 0001 and ADR 0007
decided carries over unchanged: tags, `:latest` ownership, the release rebuild, signing,
attestations, the version check, SecObserve not gating a release.

What stays here is what the shared workflows cannot do:

* The end-to-end suite, in `e2e.yml`, called from CI and release as before. It lives in a nested
  module and needs a container runtime. It also vets the e2e module.
* The chart render assertions, in `scripts/check-chart-render.sh`, run by `helm-lint`.

The alternative, keeping our own copies, lost because every fix to a pin or a step would have to
be made four times.

## Consequences

* **Signing identity.** A keyless signature names the workflow that made it, which is now the
  shared one. Images and checksums from the first release on these workflows verify with
  `--certificate-identity-regexp '^https://github\.com/pflege-de-labs/github-workflows/'` and
  `--certificate-github-workflow-repository pflege-de-labs/tranquila`. Earlier releases keep
  verifying with `^https://github.com/pflege-de-labs/tranquila/`.
* **The SPDX guard no longer gates.** ADR 0007 kept the check that each extracted SBOM is a bare
  SPDX document as a blocking step. The shared workflow runs SecObserve in a job of its own after
  the image, and every step there, the guard included, reports a failure as a warning. The image
  is already pushed, signed and verified when it runs, so failing it withheld nothing.
* **New checks.** CI now also runs `go vet` on the root module, `gofmt`, and reports statement
  coverage in the job summary without gating on it.
* **gosec is off.** The shared workflow runs it by default, and it reports findings that predate
  this change. It is enabled once they are fixed.
* **Check names.** Status checks are named `<job> / <shared job>`, e.g. `checks / Build & Test`.
  No ruleset requires a check by name today.
* Pin updates arrive as Renovate pull requests against the github-workflows release.
