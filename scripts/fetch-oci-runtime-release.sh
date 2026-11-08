#!/usr/bin/env bash
# fetch-oci-runtime-release.sh ARCH CONTAINERD_VERSION RUNC_VERSION DIR
#
# Leaves the pinned upstream containerd archive and runc binary in DIR for a
# CI lane to install. CI restores DIR with actions/cache, so a short outage of
# the GitHub release downloads does not fail the lane (#1567). Files already in
# DIR are not downloaded again. Every run checks them against the upstream
# SHA-256 files kept beside them, from the cache or not: a mismatch fails the
# run. A fresh download that fails the check is removed, so DIR never keeps it.
set -euo pipefail

arch=${1:?pass the architecture (amd64 or arm64)}
containerd_version=${2:?pass the containerd version}
runc_version=${3:?pass the runc version}
dir=${4:?pass the asset directory}

containerd_archive="containerd-$containerd_version-linux-$arch.tar.gz"
containerd_base="https://github.com/containerd/containerd/releases/download/v$containerd_version"
runc_binary="runc.$arch"
runc_base="https://github.com/opencontainers/runc/releases/download/v$runc_version"
assets=("$containerd_archive" "$containerd_archive.sha256sum" "$runc_binary" runc.sha256sum)

say() { printf 'fetch-oci-runtime-release: %s\n' "$*"; }

mkdir -p "$dir"
cd "$dir"
missing=()
for asset in "${assets[@]}"; do
  [ -s "$asset" ] || missing+=("$asset")
done

if [ "${#missing[@]}" -eq 0 ]; then
  say "using cached containerd $containerd_version and runc $runc_version ($arch); no download"
else
  # A partial set is not trusted: fetch all four from the same release.
  rm -f -- "${assets[@]}"
  say "downloading containerd $containerd_version and runc $runc_version ($arch)"
  # The window rides out a short burst of 5xx answers from the release host;
  # --max-time ends a stalled attempt so that it is retried.
  fetch() { curl -fLsS --connect-timeout 20 --max-time 120 --retry 7 --retry-all-errors --retry-max-time 180 -o "$1" "$2"; }
  fetch "$containerd_archive" "$containerd_base/$containerd_archive"
  fetch "$containerd_archive.sha256sum" "$containerd_base/$containerd_archive.sha256sum"
  fetch "$runc_binary" "$runc_base/$runc_binary"
  fetch runc.sha256sum "$runc_base/runc.sha256sum"
fi

if ! sha256sum --check "$containerd_archive.sha256sum" ||
  ! grep " $runc_binary\$" runc.sha256sum | sha256sum --check -; then
  [ "${#missing[@]}" -eq 0 ] || rm -f -- "${assets[@]}"
  say "checksum mismatch" >&2
  exit 1
fi
