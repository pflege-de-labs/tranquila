# 0007. Upload a bare SPDX document per platform, and stop letting SecObserve gate a release

* Status: Accepted
* Date: 2026-09-16
* Supersedes [0006](0006-secobserve-image-scanning.md)

## Context

[0006](0006-secobserve-image-scanning.md) wired two SecObserve steps into each `image` job. Both
were broken, in different ways, and one of them had been broken since it merged without anyone
noticing.

**The SBOM upload was rejected on every run.** Both workflows extracted the attestation with:

```sh
docker buildx imagetools inspect "${IMAGE}@${DIGEST}" --format '{{ json .SBOM }}' > sbom.json
```

For a *multi-platform* image, buildx renders `tplInputs.SBOM()`, which returns
`map[string]sbomStub` keyed by platform (`docker/buildx`, `util/imagetools/printers.go`,
`util/imagetools/loader.go`). The file is therefore a wrapper, not a document:

```json
{"linux/amd64": {"SPDX": {…}}, "linux/arm64": {"SPDX": {…}}}
```

SecObserve identifies an uploaded file by trying every parser's `check_format`; the SPDX parser
requires `SPDXID` and `spdxVersion` at the *top level*
(`backend/application/import_observations/parsers/spdx/parser.py`), and CycloneDX likewise. Nothing
matched, so every upload came back `400 {"message":"No suitable parser found"}` — on the two `main`
CI runs after the merge, and on both attempts of the `v0.6.0` release.

**Nobody noticed, because that step cannot fail.** `actions/upload_sbom` runs entrypoint
`file_upload_observations.sh`, an `sh` script with no `set -e` whose last command is `deactivate`;
the uploader's `exit(1)` is discarded and the step exits 0. The Trivy action reports failures only
because *its* entrypoint (`entrypoint_trivy_image.sh`) sets `-e` and `source`s the same script.
Upstream already ships the right entrypoint for an SBOM upload —
`/entrypoints/entrypoint_upload_sbom.sh`, which is `set -e` plus `source file_upload_sbom.sh` and
posts to the dedicated `…/api/import/file_upload_sbom_by_name/` endpoint — but its published action
does not point at it. This is an upstream defect; it is present in `dev/actions/upload_sbom` too.

**The Trivy upload failed with a server-side 500, and that withheld a release.** The
`v0.6.0` run got `500 Server Error` from `…/api/import/file_upload_observations_by_name/` on both
attempts, while the same commit's `main` CI run had uploaded the same report successfully half an
hour earlier. It is not a payload problem: the SBOM request in the same job reached *parser
detection*, which happens after `user_has_permission_or_403` and `Branch.objects.create`, so auth,
product lookup and branch creation all worked; and `so_suppress_licenses` is not the differentiator
because the upstream action defaults it to `true`, so CI sent it too. The response body is Django's
default `Server Error (500)` HTML, meaning the exception escaped DRF's handler. The consequence in
this repository is what matters: `binaries` is `needs: [verify, image]`, so a failed upload left
`ghcr.io/pflege-de-labs/tranquila:0.6.0` pushed, signed and verified but with **no `v0.6.0` GitHub
release and no release binaries**.

ADR 0006 anticipated exactly this and accepted it ("revisit if this proves too tight a coupling").
It proved too tight on the first release.

## Decision

**We will extract one bare SPDX document per platform, and assert that it is one.** Both workflows
unwrap buildx's platform map with `jq` into `sbom-linux-amd64.json` and `sbom-linux-arm64.json`, and
then fail the step unless each file has `SPDXID` and `spdxVersion` at the top level. The guard is
the point: the class of bug that shipped here is "the file is not what the API expects", and it is
cheap to catch before it reaches the API instead of as a 400 nobody reads. Verified against the
published `v0.6.0` attestations pulled from GHCR: the old command's output fails the guard, and both
unwrapped documents pass it (SPDX-2.3, 87 packages each).

Both platforms upload, as two calls with different `SO_FILE_NAME`. SecObserve keys a
`Vulnerability_Check` on `(product, branch, service, filename)`, so they do not overwrite each
other, and arm64-only base-layer packages stay visible. Uploading only `linux/amd64` was the
simpler option and was rejected for that reason — the release ships both architectures.

**We will call the scanner image directly instead of using `actions/upload_sbom`.** A step runs
`docker run … --entrypoint /entrypoints/entrypoint_upload_sbom.sh` against the *same* pinned image
the SecObserve actions use (`ghcr.io/secobserve/secobserve-scanners:2026_08`, by digest). This is
not a reimplementation of the integration: it is upstream's own code, reached through the entrypoint
upstream's own action forgot to name. The alternative — keep the action and verify the upload
afterwards through the SecObserve API — needs a second API contract to hand-maintain to check the
first one, for no gain. An issue is filed upstream; if the action is fixed, this step goes back to
`uses:`.

The cost is that Renovate's `github-actions` manager does not see an image reference inside a
`run:` block, so the pin would rot silently. A `customManagers` regex entry in
`.github/renovate.json` matches `SCANNER_IMAGE:` in `.github/workflows/*.yml` with the `docker`
datasource, which puts it under the same automerging rule as every other pinned image.

**We will make both SecObserve steps non-blocking, and loud.** Each gets `continue-on-error: true`
and an `id`, and a following step turns a `failure` outcome into a `::warning` plus a job-summary
note. SecObserve is a findings tool, not a release gate: an outage there must not withhold an image
that already built, signed and verified, nor the GitHub release and binaries that depend on it.
Everything that *is* a gate — the version check, `cosign sign`, `cosign verify`, the attestation
presence check, the SPDX guard above — stays blocking and unchanged.

This reverses 0006's "fails visibly rather than silently" stance only for the *upload* half. The
reason that stance was right and is now wrong is the difference between the two failure modes 0006
conflated: a scan step that skips itself reports a false clean bill of health, while an upload step
that fails reports nothing at all and already has a signed, verified image behind it. The warning
and the job-summary note preserve the visibility; only the gating goes.

Everything else in 0006 stands: the gateway URL (`https://webhooks.management.p4e.io/secobserve`,
not the RFC1918 backend host), extracting rather than regenerating the SBOM, skipping `pull_request`
events in `ci.yml`, scanning the digest rather than a tag, the SHA-pinned actions, and leaving the
`binaries` job's syft SBOMs out of scope.

## Consequences

* A SecObserve outage, or a repeat of the 500, no longer costs a release. It costs a warning on an
  otherwise green run, and an import that has to be redone.
* Two SBOMs per image now exist in SecObserve per branch instead of zero. License components are
  imported for both, through the dedicated SBOM endpoint rather than the observations endpoint.
* A failed SBOM upload is now a real failure signal for the first time. Expect the first runs after
  this merges to surface upload problems that were previously invisible.
* The scanner image pin lives in a `run:` block and depends on the new `customManagers` entry to
  stay fresh. If that regex stops matching (for example if the variable is renamed), the pin goes
  stale silently — the same failure mode this ADR is otherwise closing.
* **The 500 is not fixed here, and is not fixable here.** It needs the SecObserve backend traceback
  for 2026-09-16 13:27–13:34 UTC from the `django.request` logger. The deployment's own logs already
  show a `psycopg.OperationalError: consuming input failed: SSL error: unexpected eof while reading`
  the day before, so an intermittent database connection drop is the leading candidate; that would
  produce precisely an unhandled 500 with Django's default HTML body.
* `v0.6.0` still has no GitHub release. Re-running the failed run replays the *tag's* workflow,
  which is the broken one, so the fix has to land on `main` first and a new tag has to be cut.
