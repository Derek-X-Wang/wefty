#!/usr/bin/env bash
set -euo pipefail

bash -n scripts/build-oci-install-manifest.sh
bash -n scripts/verify-acceptance-image.sh
go test ./scripts -run '^TestAcceptanceImageWorkflowContract$' -count=1

temporary_root="$(mktemp -d)"
trap 'rm -rf -- "$temporary_root"' EXIT
probe_archive="$temporary_root/wefty-echo-service.oci.tar"
cp scripts/test-acceptance-image.sh "$probe_archive"
synthetic_digest="sha256:$(printf 'a%.0s' {1..64})"

build_manifest() {
  local reference=$1 archive=$2 output=$3
  bash scripts/build-oci-install-manifest.sh \
    --helper scripts/test-acceptance-image.sh \
    --probe-reference "$reference" \
    --probe-digest "$synthetic_digest" \
    --probe-archive "$archive" \
    --output "$output"
}

expect_invalid_reference() {
  local label=$1 reference=$2 stderr="$temporary_root/$1-reference.stderr" status
  set +e
  build_manifest "$reference" "$probe_archive" "$temporary_root/$label-reference.json" >/dev/null 2>"$stderr"
  status=$?
  set -e
  [[ $status == 64 ]] || { printf 'invalid reference row %s exited %s, want 64\n' "$label" "$status" >&2; exit 1; }
  grep -Fxq 'invalid probe reference (repository name only; no tag or digest)' "$stderr"
}

expect_invalid_archive_name() {
  local label=$1 archive="$temporary_root/$2" stderr="$temporary_root/$1-archive.stderr" status
  cp scripts/test-acceptance-image.sh "$archive"
  set +e
  build_manifest ghcr.io/derek-x-wang/wefty-echo-service "$archive" "$temporary_root/$label-archive.json" >/dev/null 2>"$stderr"
  status=$?
  set -e
  [[ $status == 64 ]] || { printf 'invalid archive row %s exited %s, want 64\n' "$label" "$status" >&2; exit 1; }
  grep -Fxq 'invalid probe archive name' "$stderr"
}

expect_valid_reference() {
  local label=$1 reference=$2 output="$temporary_root/$1-manifest.json"
  build_manifest "$reference" "$probe_archive" "$output" >/dev/null
  jq -e --arg reference "$reference" --arg digest "$synthetic_digest" '
    .version == 1 and
    .probe_reference == $reference and
    .probe_digest == $digest and
    .probe_archive_path == "wefty-echo-service.oci.tar"
  ' "$output" >/dev/null
}

expect_valid_reference real_ghcr ghcr.io/derek-x-wang/wefty-echo-service
expect_valid_reference registry_port registry.example.com:5000/team/probe

expect_invalid_reference backslash 'ghcr.io/derek-x-wang/wefty\echo-service'
expect_invalid_reference quote 'ghcr.io/derek-x-wang/wefty"echo-service'
expect_invalid_reference newline $'ghcr.io/derek-x-wang/wefty\necho-service'
expect_invalid_reference carriage_return $'ghcr.io/derek-x-wang/wefty\recho-service'
expect_invalid_reference space 'ghcr.io/derek-x-wang/wefty echo-service'
expect_invalid_reference tag 'ghcr.io/derek-x-wang/wefty-echo-service:latest'
expect_invalid_reference digest 'ghcr.io/derek-x-wang/wefty-echo-service@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
expect_invalid_archive_name backslash 'wefty\echo-service.oci.tar'
expect_invalid_archive_name quote 'wefty"echo-service.oci.tar'
expect_invalid_archive_name newline $'wefty\necho-service.oci.tar'
expect_invalid_archive_name carriage_return $'wefty\recho-service.oci.tar'
expect_invalid_archive_name space 'wefty echo-service.oci.tar'
expect_invalid_archive_name trailing_lf $'wefty-echo-service.oci.tar\n'
