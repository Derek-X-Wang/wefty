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
stage_error() {
  printf '::error title=computer runtime conformance::%s: %s\n' "$1" "$2" >&2
}

if ! mkdir -p "$evidence"; then
  stage_error evidence-directory "could not create $evidence"
  exit 1
fi

checker="$evidence/wefty-computer-conformance"
diagnostics=scripts/check-computer-image-runtime-evidence.sh
if ! go build -o "$checker" ./cmd/wefty-computer-conformance; then
  stage_error checker-build 'could not build wefty-computer-conformance'
  exit 1
fi

set +e
positive_stderr="$evidence/${arch}-runtime.stderr"
"$checker" \
  --image "$image" \
  --repair-image "$image" \
  --platform "linux/$arch" \
  --input-oracle-path /tmp/wefty-computer/input-oracle.json \
  --driver-oracle-path /tmp/wefty-computer/driver-state.json \
  --edge-process-pattern "$edge_process_pattern" \
  --receipt "$evidence/${arch}-runtime.json" 2> "$positive_stderr"
positive_status=$?
set -e
sed -n '1,400p' "$positive_stderr" >&2
if grep -Fq 'runtime teardown failed' "$positive_stderr"; then
  stage_error positive-runtime-teardown "checker reported container or temporary-root cleanup failure; see $positive_stderr"
  exit 1
fi
if ((positive_status != 0)); then
  stage_error positive-runtime "checker exited $positive_status; receipt=$evidence/${arch}-runtime.json"
  exit 1
fi
"$diagnostics" positive "$evidence/${arch}-runtime.json"

repair_probe_stderr="$evidence/${arch}-teardown-repair.stderr"
set +e
"$checker" \
  --image "$image" \
  --repair-image "$image" \
  --platform "linux/$arch" \
  --mutation-profile teardown-permission-repair \
  --receipt "$evidence/${arch}-teardown-repair.json" 2> "$repair_probe_stderr"
repair_probe_status=$?
set -e
sed -n '1,400p' "$repair_probe_stderr" >&2
"$diagnostics" teardown-repair "$evidence/${arch}-teardown-repair.json" "$repair_probe_status"

mutations="$evidence/mutations"
if ! mkdir -p "$mutations"; then
  stage_error mutation-directory "could not create $mutations"
  exit 1
fi
fixture_tags=()
cleanup_fixture_images() {
  local status=$?
  trap - EXIT
  if ((${#fixture_tags[@]} > 0)) && ! docker rmi "${fixture_tags[@]}"; then
    stage_error fixture-image-cleanup "docker rmi failed for ${#fixture_tags[@]} broken fixture images"
    status=1
  fi
  exit "$status"
}
trap cleanup_fixture_images EXIT
executed_rows=0
run_mutation() {
  local mutation=$1 cell=$2 detail=$3
  local tag="wefty-computer-broken-${mutation}:local"
  local fixture_dockerfile=examples/computer/fixtures/Dockerfile
  if [[ $mutation == text-frames-accepted && $edge_process_pattern == 'wayvnc -w' ]]; then
    fixture_dockerfile=examples/computer/fixtures/Dockerfile.wayland-text
  fi
  if ! docker build --platform "linux/$arch" --file "$fixture_dockerfile" \
    --build-arg "BASE_IMAGE=$image" --build-arg "MUTATION=$mutation" --tag "$tag" .; then
    stage_error "fixture-build/$mutation" "docker build failed for $tag"
    exit 1
  fi
  fixture_tags+=("$tag")
  local checker_started=$SECONDS
  local checker_stderr="$mutations/$mutation.stderr"
  set +e
  "$checker" --image "$tag" --repair-image "$image" --platform "linux/$arch" \
    --input-oracle-path /tmp/wefty-computer/input-oracle.json \
    --driver-oracle-path /tmp/wefty-computer/driver-state.json \
    --edge-process-pattern "$edge_process_pattern" \
    --mutation-profile "$mutation" --receipt "$mutations/$mutation.json" 2> "$checker_stderr"
  local checker_status=$?
  set -e
  sed -n '1,400p' "$checker_stderr" >&2
  if grep -Fq 'runtime teardown failed' "$checker_stderr"; then
    stage_error "runtime-teardown/$mutation" "checker reported container or temporary-root cleanup failure; see $checker_stderr"
    exit 1
  fi
  local checker_wall_seconds=$((SECONDS - checker_started))
  "$diagnostics" mutation "$mutations/$mutation.json" "$mutation" "$cell" "$detail" \
    "$checker_status" "$checker_wall_seconds"
  executed_rows=$((executed_rows + 1))
}

run_mutation missing-control-endpoint transport.control-ready 'control never completed rfb-websocket-v1'
run_mutation missing-view-endpoint transport.view-ready 'view never completed rfb-websocket-v1'
run_mutation duplicate-endpoint endpoints.distinct 'view and control received the same attempt-local port'
run_mutation plain-tcp-control transport.plain-tcp-rejected 'endpoint accepted TCP but did not complete the required WebSocket upgrade'
run_mutation plain-rfb-control transport.plain-tcp-rejected 'endpoint accepted TCP but did not complete the required WebSocket upgrade'
run_mutation view-accepts-input input.view-isolated 'view pointer or key input reached the guest before the control sentinel'
run_mutation text-frames-accepted transport.text-frame-rejected 'one endpoint violated the negative wire assertion'
run_mutation driver-json-ignored driver.true-consumed 'tenant ignored the true driver generation'
run_mutation malformed-driver-accepted driver.malformed-fails-closed 'a malformed generation was accepted'
run_mutation unknown-driver-version-accepted driver.unknown-version-fails-closed 'unknown-version generation was accepted'
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
  '{version:1,platform:$platform,executed_rows:$executed_rows}' > "$mutations/summary.json" || {
    stage_error summary-write 'could not write mutation summary receipt'
    exit 1
  }
"$diagnostics" summary "$mutations/summary.json" "linux/$arch" 21
