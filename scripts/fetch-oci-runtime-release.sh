#!/usr/bin/env bash
# fetch-oci-runtime-release.sh ARCH CONTAINERD_VERSION CONTAINERD_SHA256 RUNC_VERSION RUNC_SHA256 DIR
#
# Leaves the pinned upstream containerd archive and runc binary in DIR for a
# CI lane to install. CI restores DIR with actions/cache, so a short outage of
# the GitHub release downloads does not fail the lane (#1567). Files already in
# DIR are not downloaded again. Every run checks both files against the SHA-256
# values pinned in the repository (#1569), from the cache or not: a mismatch
# fails the run. The release's own .sha256sum files are not used. A fresh
# download that fails the check is removed, so DIR never keeps it.
set -euo pipefail

arch=${1:?pass the architecture (amd64 or arm64)}
containerd_version=${2:?pass the containerd version}
containerd_sha256=${3:?pass the pinned containerd archive SHA-256}
runc_version=${4:?pass the runc version}
runc_sha256=${5:?pass the pinned runc binary SHA-256}
dir=${6:?pass the asset directory}

say() { printf 'fetch-oci-runtime-release: %s\n' "$*"; }

for sum in "$containerd_sha256" "$runc_sha256"; do
  [[ $sum =~ ^[0-9a-f]{64}$ ]] || { say "not a SHA-256: '$sum'" >&2; exit 2; }
done

containerd_archive="containerd-$containerd_version-linux-$arch.tar.gz"
containerd_url="https://github.com/containerd/containerd/releases/download/v$containerd_version/$containerd_archive"
runc_binary="runc.$arch"
runc_url="https://github.com/opencontainers/runc/releases/download/v$runc_version/$runc_binary"
assets=("$containerd_archive" "$runc_binary")

mkdir -p "$dir"
cd "$dir"
missing=()
for asset in "${assets[@]}"; do
  [ -s "$asset" ] || missing+=("$asset")
done

if [ "${#missing[@]}" -eq 0 ]; then
  say "using cached containerd $containerd_version and runc $runc_version ($arch); no download"
else
  # A partial set is not trusted: fetch both from the pinned releases.
  rm -f -- "${assets[@]}"
  say "downloading containerd $containerd_version and runc $runc_version ($arch)"
  # The window rides out a short burst of 5xx answers from the release host;
  # --max-time ends a stalled attempt so that it is retried.
  fetch() { curl -fLsS --connect-timeout 20 --max-time 120 --retry 7 --retry-all-errors --retry-max-time 180 -o "$1" "$2"; }
  fetch "$containerd_archive" "$containerd_url"
  fetch "$runc_binary" "$runc_url"
fi

say "checking $containerd_archive against the in-repo SHA-256 $containerd_sha256"
say "checking $runc_binary against the in-repo SHA-256 $runc_sha256"
if ! printf '%s  %s\n%s  %s\n' "$containerd_sha256" "$containerd_archive" "$runc_sha256" "$runc_binary" | sha256sum --check -; then
  [ "${#missing[@]}" -eq 0 ] || rm -f -- "${assets[@]}"
  say "checksum mismatch against the in-repo SHA-256" >&2
  exit 1
fi
