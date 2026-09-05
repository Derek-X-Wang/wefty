#!/usr/bin/env bash
set -euo pipefail

if violations=$(git grep -n '"tailscale.com/' -- '*.go' ':(exclude)fabric/tsnet/**'); then
  echo "tailscale.com imports must stay inside fabric/tsnet:" >&2
  echo "${violations}" >&2
  exit 1
fi

if violations=$(git grep -n -E 'MagicDNS|\.ts\.net|svc:' -- '*.go' '*.json' \
  ':(exclude)fabric/tsnet/**' ':(exclude)scripts/fabric_identity_receipt_contract_test.go'); then
  echo "provider-specific DNS and service-name shapes must stay behind fabric/tsnet:" >&2
  echo "${violations}" >&2
  exit 1
fi

echo "Fabric import and public naming boundaries are clean."
