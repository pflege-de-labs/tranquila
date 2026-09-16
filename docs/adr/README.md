# Architecture Decision Records

Architectural decisions are recorded here, one file per decision, in
[ADR](https://adr.github.io/) format.

## Rules

* Copy [0000-template.md](0000-template.md) to `NNNN-short-title.md`, numbered sequentially.
* An accepted ADR is immutable. To change a decision, write a new ADR and set the old one to
  `Superseded by …`; only the status line of the old record may be edited.
* Any change to how components are structured, how they communicate, or which external
  dependency is used needs an ADR before the PR is merged.

## Index

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-ci-cd-pipeline.md) | Images are tagged by metadata-action, releases rebuild and are signed | Accepted |
| [0002](0002-discovery-checkpointing.md) | Sharded discovery persists a per-prefix resume point | Accepted |
| [0003](0003-dependency-update-policy.md) | Dependency updates are grouped by what must move together | Accepted |
| [0004](0004-grafana-dashboard.md) | Ship a Grafana dashboard built from the metrics that exist | Accepted |
| [0005](0005-repository-and-chart-consolidation.md) | Move to pflege-de-labs and bring the Helm chart with it | Accepted |
| [0006](0006-secobserve-image-scanning.md) | Images are scanned with Trivy and reported to SecObserve | Superseded by [0007](0007-secobserve-upload-correctness.md) |
| [0007](0007-secobserve-upload-correctness.md) | Upload a bare SPDX document per platform, and stop letting SecObserve gate a release | Accepted |
