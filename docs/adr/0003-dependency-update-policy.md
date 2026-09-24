# 0003. Dependency updates are grouped by what must move together

* Status: Accepted
* Date: 2026-09-14

## Context

Every Go dependency update went into a single `go dependencies` group, with `separateMajorMinor`
and `separateMinorPatch` both off. One PR therefore carried whatever had moved since the last
one, regardless of kind or blast radius.

PR #50 is the worked example: it bundled the `go` directive 1.25 → 1.26, roughly twenty AWS SDK
modules, and `golang.org/x/time` into one change. It failed CI, and nothing about the failure
said which of those was responsible — the answer turned out to be none of them (see below), but
establishing that meant reproducing the whole bundle locally.

A second, quieter problem surfaced with it. `e2e` is a separate module with a `replace` to `../`,
so every *direct* dependency of the root module is an *indirect* dependency of e2e's. Renovate
does not manage indirect Go dependencies by default, so it bumped and tidied the root module and
left `e2e/go.mod` pinned to the old versions. The module graph then disagreed with itself and
every command that loaded it failed with `go: updates to go.mod needed`.

Separately, `ignoreTests: true` was set globally while `automerge: true` was set for the docker
and github-actions managers. Those updates merged without CI passing. Since ADR 0001 the actions
in question push images to GHCR and sign them with this repository's OIDC identity.

## Decision

**Group by what must move together, not by manager.** Three groups exist because a partial update
genuinely does not work:

* `aws sdk` — the SDK is ~20 modules sharing internal version constraints; a partial bump does not
  compile.
* `opentelemetry` — the API, metric, SDK and exporter packages are one release train.
* `e2e test dependencies` — testcontainers and toxiproxy, which cannot reach the production binary
  and drag ~89 transitive modules of their own.

The `go` toolchain directive gets its own group and never automerges: it changes codegen and
runtime behaviour for everything built with it, so it is not a routine bump and must not arrive
bundled with a library update.

Everything else is ungrouped, so each dependency gets its own pull request. `separateMajorMinor`
is now on, so a breaking major never rides along with a patch.

**Renovate manages e2e's indirect dependencies explicitly**, so the module it cannot otherwise see
is bumped and tidied in the same PR as the root change that requires it.
Major updates of indirect dependencies are disabled: a Go major is a new module path, so only the
module importing it can adopt it, and Renovate's added requirement is removed again by `go mod
tidy` (PR #78).

**CI checks both modules are tidy**, by name. The cause is subtle enough that discovering it from
an opaque `updates to go.mod needed` cost real time once; a step that says so directly, and that
catches drift from any source rather than only from Renovate, is worth sixteen lines.

**`ignoreTests` is removed.** Automerge still applies to docker digests and non-major action
bumps, but now waits for CI.

## Consequences

* More pull requests: roughly five to eight a month rather than one or two. That is the cost being
  paid for a failing update naming its own cause and being revertable on its own.
* A broken bump no longer blocks unrelated ones. Under the single group, one bad dependency held
  every other update behind it until someone intervened.
* Automerged updates are slower by the length of a CI run, which is the point.
* Renovate now opens updates for e2e's indirect dependencies. These are not independent choices —
  they are consequences of the root module's — so they should be reviewed as part of whatever root
  change produced them, not on their own merits.
* The grouping is a judgement about today's dependencies. A new ecosystem whose modules must move
  in lockstep needs a rule of its own; without one it will be proposed module by module and the
  individual PRs will each fail to build.
