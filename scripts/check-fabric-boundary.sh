#!/usr/bin/env bash
set -euo pipefail

if violations=$(git grep -n '"tailscale.com/' -- '*.go' ':(exclude)fabric/tsnet/**'); then
  echo "tailscale.com imports must stay inside fabric/tsnet:" >&2
  echo "${violations}" >&2
  exit 1
fi

echo "Fabric import boundary is clean."
