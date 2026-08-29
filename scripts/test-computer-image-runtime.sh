#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper for the #234 acceptance lane. The product checker owns
# all runtime assertions; this script only maps the older evidence arguments.
image=
arch=
evidence=
edge_process_pattern='wefty-rfb-websocket --port'
while (($# > 0)); do
  case "$1" in
    --image) image="${2:-}"; shift ;;
    --arch) arch="${2:-}"; shift ;;
    --evidence) evidence="${2:-}"; shift ;;
    --edge-process-pattern) edge_process_pattern="${2:-}"; shift ;;
    *) printf '%s\n' 'usage: scripts/test-computer-image-runtime.sh --image REF@sha256:DIGEST --arch amd64|arm64 --evidence DIR [--edge-process-pattern TEXT]' >&2; exit 64 ;;
  esac
  shift
done
[[ $image == *@sha256:* && $arch =~ ^(amd64|arm64)$ && -n $evidence && -n $edge_process_pattern ]] || exit 64
mkdir -p "$evidence"

checker="$evidence/wefty-computer-conformance"
go build -o "$checker" ./cmd/wefty-computer-conformance

"$checker" \
  --image "$image" \
  --platform "linux/$arch" \
  --input-oracle-path /tmp/wefty-computer/input-oracle.json \
  --driver-oracle-path /tmp/wefty-computer/driver-state.json \
  --edge-process-pattern "$edge_process_pattern" \
  --receipt "$evidence/${arch}-runtime.json"

mutations="$evidence/mutations"
mkdir -p "$mutations"
executed_rows=0
run_mutation() {
  local mutation=$1 cell=$2 detail=$3
  local tag="wefty-computer-broken-${mutation}:local"
  local fixture_dockerfile=examples/computer/fixtures/Dockerfile
  if [[ $mutation == text-frames-accepted && $edge_process_pattern == 'wayvnc -w' ]]; then
    fixture_dockerfile=examples/computer/fixtures/Dockerfile.wayland-text
  fi
  docker build --platform "linux/$arch" --file "$fixture_dockerfile" \
    --build-arg "BASE_IMAGE=$image" --build-arg "MUTATION=$mutation" --tag "$tag" .
  set +e
  "$checker" --image "$tag" --platform "linux/$arch" \
    --input-oracle-path /tmp/wefty-computer/input-oracle.json \
    --driver-oracle-path /tmp/wefty-computer/driver-state.json \
    --edge-process-pattern "$edge_process_pattern" \
    --mutation-profile "$mutation" --receipt "$mutations/$mutation.json"
  local checker_status=$?
  set -e
  if ((checker_status == 64)); then
    printf 'checker usage failure for mutation %s\n' "$mutation" >&2
    exit 1
  fi
  jq -e --arg cell "$cell" --arg detail "$detail" '
    [.checks[] | select(.status == "FAIL")] as $failed |
    ($failed | length) == 1 and $failed[0].id == $cell and $failed[0].detail == $detail
  ' "$mutations/$mutation.json" >/dev/null
  executed_rows=$((executed_rows + 1))
}

run_mutation missing-control-endpoint transport.control-ready 'control never completed rfb-websocket-v1'
run_mutation missing-view-endpoint transport.view-ready 'view never completed rfb-websocket-v1'
run_mutation duplicate-endpoint endpoints.distinct 'view and control received the same attempt-local port'
run_mutation plain-tcp-control transport.plain-tcp-rejected 'endpoint accepted TCP but did not complete the required WebSocket upgrade'
run_mutation view-accepts-input input.view-isolated 'view pointer or key input reached the guest before the control sentinel'
run_mutation text-frames-accepted transport.text-frame-rejected 'one endpoint violated the negative wire assertion'
run_mutation driver-json-ignored driver.true-consumed 'tenant ignored the true driver generation'
run_mutation malformed-driver-accepted driver.malformed-fails-closed 'a malformed generation was accepted or not observed'
run_mutation unknown-driver-version-accepted driver.unknown-version-fails-closed 'unknown-version generation was accepted or not observed'
run_mutation writable-driver driver.read-only 'tenant can write driver.json'
run_mutation reserved-env-shadowed environment.view-port 'reserved environment did not match the authoritative value'
run_mutation forbidden-privilege harness.forbidden-privilege 'forbidden SYS_ADMIN capability was added'
run_mutation readiness-over-60s readiness.before-deadline 'endpoint pair was not ready before t0 + 60s'
run_mutation shm-too-small harness.shm-size '/dev/shm ceiling is not 1 GiB'
run_mutation writable-rootfs harness.rootfs-read-only 'write failed with EACCES, not EROFS'
run_mutation view-wildcard-bind endpoints.view-loopback 'endpoint was missing or wildcard-bound'
run_mutation control-wildcard-bind endpoints.control-loopback 'endpoint was missing or wildcard-bound'
run_mutation profile-state-lost persistence.profile-survives 'profile marker under HOME was lost'
run_mutation sign-in-state-lost persistence.sign-in-survives 'sign-in marker under HOME was lost'
run_mutation edge-does-not-recover persistence.edge-recovers 'both endpoints did not recover after edge withdrawal'
jq -n --arg platform "linux/$arch" --argjson executed_rows "$executed_rows" \
  '{version:1,platform:$platform,executed_rows:$executed_rows}' > "$mutations/summary.json"
jq -e --arg platform "linux/$arch" '.platform == $platform and .executed_rows == 20' "$mutations/summary.json" >/dev/null
