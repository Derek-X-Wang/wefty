#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper for the #234 acceptance lane. The product checker owns
# all runtime assertions; this script only maps the older evidence arguments.
image=
arch=
evidence=
while (($# > 0)); do
  case "$1" in
    --image) image="${2:-}"; shift ;;
    --arch) arch="${2:-}"; shift ;;
    --evidence) evidence="${2:-}"; shift ;;
    *) printf '%s\n' 'usage: scripts/test-computer-image-runtime.sh --image REF@sha256:DIGEST --arch amd64|arm64 --evidence DIR' >&2; exit 64 ;;
  esac
  shift
done
[[ $image == *@sha256:* && $arch =~ ^(amd64|arm64)$ && -n $evidence ]] || exit 64
mkdir -p "$evidence"

go run ./cmd/wefty-computer-conformance \
  --image "$image" \
  --platform "linux/$arch" \
  --input-oracle-path /tmp/wefty-computer/input-oracle.json \
  --driver-oracle-path /tmp/wefty-computer/driver-state.json \
  --receipt "$evidence/${arch}-runtime.json"
