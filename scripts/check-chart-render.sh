#!/usr/bin/env bash
# Asserts what the chart renders; rendering alone proves only that it is not broken.
set -euo pipefail

chart=${1:-charts/tranquila}
fail() { echo "::error::$1"; exit 1; }

# The credentials/config a user manages out of band must suppress the
# chart's own objects. Both keys are documented at the top level and
# were previously read from under `config`, so the documented form
# rendered a Secret anyway and failed on its `required` calls.
out=$(helm template tranquila "$chart" \
        --values "$chart/ci/existing-secret-values.yaml")
echo "$out" | grep -q "name: tranquila-s3-credentials" \
  || fail "existingSecret is not mounted"
echo "$out" | grep -q "name: tranquila-sync-config" \
  || fail "existingConfig is not mounted"
if echo "$out" | grep -qE "^  name: tranquila-tranquila-config$"; then
  fail "chart rendered its own config objects despite existingSecret/existingConfig"
fi

# The nested form predates the documented one and must keep working.
helm template tranquila "$chart" \
  --set config.existingSecret=legacy-secret \
  --set config.existing=legacy-cm >/dev/null \
  || fail "nested config.existingSecret/config.existing no longer renders"

# Disabling the subchart must actually drop it.
if helm template tranquila "$chart" \
     --values "$chart/ci/external-redis-values.yaml" \
     | grep -q "valkey"; then
  fail "valkey rendered while valkey.enabled=false"
fi

# The image default must track this repository, not the old owner.
helm template tranquila "$chart" \
  --values "$chart/ci/default-values.yaml" \
  | grep -q '"ghcr.io/pflege-de-labs/tranquila:' \
  || fail "image default does not point at ghcr.io/pflege-de-labs/tranquila"

echo "chart render assertions passed"
