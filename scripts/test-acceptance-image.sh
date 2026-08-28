#!/usr/bin/env bash
set -euo pipefail

bash -n scripts/build-oci-install-manifest.sh
bash -n scripts/verify-acceptance-image.sh
go test ./scripts -run '^TestAcceptanceImageWorkflowContract$' -count=1

temporary_root="$(mktemp -d)"
trap 'rm -rf -- "$temporary_root"' EXIT
cp scripts/test-acceptance-image.sh "$temporary_root/wefty-echo-service.oci.tar"
synthetic_digest="sha256:$(printf 'a%.0s' {1..64})"
bash scripts/build-oci-install-manifest.sh \
  --helper scripts/test-acceptance-image.sh \
  --probe-reference ghcr.io/derek-x-wang/wefty-echo-service \
  --probe-digest "$synthetic_digest" \
  --probe-archive "$temporary_root/wefty-echo-service.oci.tar" \
  --output "$temporary_root/manifest.json"
jq -e --arg digest "$synthetic_digest" '
  .version == 1 and
  .probe_reference == "ghcr.io/derek-x-wang/wefty-echo-service" and
  .probe_digest == $digest and
  .probe_archive_path == "wefty-echo-service.oci.tar"
' "$temporary_root/manifest.json" >/dev/null
