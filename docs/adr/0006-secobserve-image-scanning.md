# 0006. Images are scanned with Trivy and reported to SecObserve

* Status: Superseded by [0007](0007-secobserve-upload-correctness.md)
* Date: 2026-09-15

## Context

`ci.yml` and `release.yml` build, sign, and (for releases) attest every image, but nothing checks
what is actually inside one. `go-checks.yml`'s `Vulnerability Check` job runs `govulncheck` — Go
source and module vulnerabilities only — and has no visibility into the base image's OS packages
or anything baked in by the multi-stage `Dockerfile`. Findings had no destination even if they
were produced: there was no vulnerability-management system wired into either workflow.

The org already runs [SecObserve](https://github.com/SecObserve/SecObserve), an open-source
vulnerability and license management system, reachable at `secobserve.p4e.io`, with a
`tranquila` product already provisioned there. SecObserve publishes
[`secobserve_actions_templates`](https://github.com/SecObserve/secobserve_actions_templates) — a
repository of pinned, docker-based GitHub Actions that run a scanner and upload its findings in
one step, including `actions/SCA/trivy_image` (Trivy against a container image) and
`actions/upload_sbom` (a standalone SBOM upload). No workflow-authoring was needed on the scanner
or upload-protocol side, only wiring these two actions into the existing build jobs.

Two things had to be resolved before that wiring was safe:

* **The API is not on a public address.** `api.secobserve.p4e.io` resolves to `192.168.3.6`, an
  RFC1918 address unreachable from GitHub's hosted `ubuntu-latest` runners, which is what every
  job in both workflows already runs on. A gateway route,
  `https://webhooks.management.p4e.io/secobserve`, is public and was confirmed reachable
  (`curl -I` returns a real HTTP response, not a connection failure) — this is the URL used as
  `SO_API_BASE_URL`, not the backend host itself.
* **No `SO_API_TOKEN` secret exists yet** (`gh secret list` on this repository returns nothing).
  This ADR does not create it — the token has to come from a SecObserve user with at least the
  `Upload` role, added as a repository secret named `SO_API_TOKEN` before either workflow's new
  steps can succeed. Until it exists, the scan/upload steps fail with an authentication error, not
  silently skip.

## Decision

**Two new steps land in each `image` job's existing flow**, right after the image is built,
signed, and (for releases) verified: extract the SBOM attestation `docker/build-push-action`
already produces to a file, upload it via `actions/upload_sbom`, then scan the same digest with
`actions/SCA/trivy_image`, which uploads its CycloneDX vulnerability report in the same call. Both
actions are pinned to a commit SHA (`0a0f7d0981982a589f2a11c3f05c8e1c628fbd79`, tag `v2026_08`),
matching how every other third-party action in this repo is pinned.

**The SBOM is extracted, not regenerated.** `release.yml` already builds with `sbom: true` and
already asserts the attestation's presence (`docker buildx imagetools inspect ... .SBOM`); the new
step reuses that exact command to capture it to a file instead of running a second SBOM generator
against the same image. `ci.yml`'s per-commit build did not previously set `sbom: true` — it does
now, for the same reason: one SBOM per image, generated once, by the tool that built it.

**`ci.yml`'s new steps are skipped for `pull_request` events entirely** (not only fork PRs, which
were already excluded from pushing at all). SecObserve auto-creates a "branch" per
`SO_BRANCH_NAME` on first import; a transient PR branch has no reason to leave a permanent-ish
trace in a findings tool the way `main` and `workflow_dispatch` builds do. `release.yml` has no PR
trigger to guard against — every run there is a real tag.

**Scanning targets the digest, not a tag.** `target: ${IMAGE}@${DIGEST}` in both workflows, so
Trivy scans the exact bits that were just signed (and, in `release.yml`, already verified) —
never a tag that could be repointed between the build step and the scan step.

**Binary SBOMs (`binaries` job, syft-generated `*.spdx.json` per platform) are not uploaded here.**
The ask was image scanning; the four cross-compiled binaries are the same source built four ways,
and uploading all four to one SecObserve product needs a decision about origin/service naming this
change does not make. Left as a deliberate follow-up, not a silent omission.

## Consequences

* Every image pushed from `main` (or `workflow_dispatch`) and every release image gets scanned and
  its findings land in SecObserve, without a human running anything by hand.
* **`SO_API_TOKEN` must be added as a repository secret before this works.** Until then, both
  workflows' new steps fail — visibly, in the Actions log, not silently — which is the intended
  failure mode: a security-scanning step that "passes" by skipping itself is worse than one that
  fails loudly.
* A second external dependency now sits on the critical path of every push to `main` and every
  release: if `webhooks.management.p4e.io` is unreachable or SecObserve is down, the `image` job
  fails and no image is published for that push, even though the image itself built, signed, and
  (for releases) verified correctly. Accepted for now; revisit if this proves too tight a
  coupling — the scan/upload steps could be split into a job that reports failure without
  blocking the image from existing.
* Trivy's `--exit-code 0` (baked into the action's entrypoint) means a found vulnerability never
  fails the build on its own. SecObserve has a separate `actions/check_security_gate` action for
  turning findings into a blocking check; not wired in here, since enforcing a gate was not asked
  for and is a separate, reviewable decision with its own failure-mode tradeoffs.
