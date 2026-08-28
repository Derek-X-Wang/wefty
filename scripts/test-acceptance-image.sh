#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/acceptance-image.yml
realtiming=.github/workflows/service-acceptance-realtiming.yml
dockerfile=examples/oci-echo-service/Dockerfile
runbook=docs/runbooks/oci-node.md
attended=docs/acceptance/m3-lima-transport.md

fail() {
  printf 'acceptance image contract: %s\n' "$1" >&2
  exit 1
}

[[ -f $workflow && -f $dockerfile ]] || fail 'workflow and illustrative Dockerfile are required'
bash -n scripts/verify-acceptance-image.sh
grep -Fq 'pull_request:' "$workflow" || fail 'PR build trigger is missing'
grep -Fq 'branches: [main]' "$workflow" || fail 'main-only trigger is missing'
grep -Fq 'github.event_name == '\''push'\''' "$workflow" || fail 'main publication guard is missing'
grep -Fq 'packages: write' "$workflow" || fail 'publish job cannot write GHCR'
if grep -Fq 'secrets.' "$workflow"; then
  fail 'PR-facing workflow must not depend on repository secrets'
fi
grep -Fq 'linux/amd64' "$workflow" || fail 'amd64 build is missing'
grep -Fq 'linux/arm64' "$workflow" || fail 'arm64 build is missing'
grep -Fq 'provenance: false' "$workflow" || fail 'reproducible build must disable provenance manifests'
grep -Fq 'sbom: false' "$workflow" || fail 'reproducible build must disable SBOM manifests'
grep -Eq '^# syntax=.*@sha256:[0-9a-f]{64}$' "$dockerfile" || fail 'Dockerfile frontend is not digest-pinned'
grep -Eq '^ARG GO_IMAGE=.*@sha256:[0-9a-f]{64}$' "$dockerfile" || fail 'Go build root is not digest-pinned'
grep -Eq '^ARG BUSYBOX_IMAGE=.*@sha256:[0-9a-f]{64}$' "$dockerfile" || fail 'runtime root is not digest-pinned'
grep -Fq 'org.opencontainers.image.source="https://github.com/Derek-X-Wang/wefty"' "$dockerfile" || fail 'GHCR package is not linked to the public repository'
grep -Fq 'ENTRYPOINT ["/usr/local/bin/wefty-echo-service"]' "$dockerfile" || fail 'image does not run the asserted echo program'
grep -Fq 'workflows: [acceptance-image]' "$realtiming" || fail 'realtiming is not causally downstream of image publication'
grep -Fq 'ghcr.io/derek-x-wang/wefty-echo-service' "$realtiming" || fail 'realtiming does not consume the public image'
grep -Fq 'wefty node load-image' "$runbook" || fail 'node runbook omits offline import'
grep -Fq 'acceptance-image-index-digest.txt' "$runbook" || fail 'node runbook omits the stable digest receipt'
grep -Fq 'acceptance-image-index-digest.txt' "$attended" || fail 'attended Mac runbook omits the canonical digest receipt'
