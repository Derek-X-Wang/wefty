#!/usr/bin/env bash
set -euo pipefail

module='github.com/google/go-containerregistry@v0.22.0'
module_cache="$(go env GOMODCACHE)"
# Download outside the checkout's module so its go.mod and go.sum stay untouched.
outside="$(mktemp -d "${TMPDIR:-/tmp}/wefty-crane-modules.XXXXXX")"
trap 'rmdir -- "$outside"' EXIT

download() {
  go -C "$outside" mod download "$module" &&
    # A versioned download alone does not fetch crane's dependencies.
    go -C "$module_cache/$module" mod download
}

for attempt in 1 2 3; do
  if output="$(download 2>&1)"; then
    printf '%s\n' "$output"
    exit 0
  else
    status=$?
  fi
  printf '%s\n' "$output" >&2
  # Never retry an integrity failure, even alongside a transient error.
  if grep -Eiq 'checksum mismatch|SECURITY ERROR|verifying .*checksum' <<<"$output"; then
    exit "$status"
  fi
  if [[ $attempt == 3 ]] || ! grep -Eq 'stream error:.*INTERNAL_ERROR' <<<"$output"; then
    exit "$status"
  fi
  printf 'Crane module download attempt %s/3 failed; retrying in 3 seconds\n' "$attempt" >&2
  sleep 3
done
