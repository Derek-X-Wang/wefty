#!/usr/bin/env bash
# fetch-mise-release.sh pin ARCH
# fetch-mise-release.sh fetch ARCH DIR
#
# The Wayland reference Computer image (examples/computer-wayland/Dockerfile)
# installs a pinned mise binary. CI fetches it here and passes DIR into the
# build with --build-context mise-release=DIR. CI restores DIR with
# actions/cache, so a short outage of the GitHub release download does not
# fail the build (#1575).
#
# pin   writes the pinned version and the ARCH SHA-256, read from the
#       Dockerfile, as version=... and sha256=... lines to $GITHUB_OUTPUT
#       (or to stdout when it is unset).
# fetch leaves DIR/mise. A file already in DIR is not downloaded again. Every
#       run checks the file against the pinned SHA-256, from the cache or not:
#       a mismatch fails the run. A fresh download that fails the check is
#       removed, so DIR never keeps it. The build checks it again.
set -euo pipefail

mode=${1:?pass pin or fetch}
arch=${2:?pass the architecture (amd64 or arm64)}
dockerfile="$(cd "$(dirname "$0")/.." && pwd)/examples/computer-wayland/Dockerfile"

say() { printf 'fetch-mise-release: %s\n' "$*"; }

case "$arch" in
  amd64) mise_arch=x64; sha_arg=MISE_SHA256_AMD64 ;;
  arm64) mise_arch=arm64; sha_arg=MISE_SHA256_ARM64 ;;
  *) say "unsupported architecture '$arch'" >&2; exit 2 ;;
esac

pinned_arg() {
  local value
  value="$(sed -n "s/^ARG $1=//p" "$dockerfile")"
  if [ -z "$value" ] || [ "$(printf '%s\n' "$value" | wc -l)" -ne 1 ]; then
    say "expected exactly one 'ARG $1=...' line in $dockerfile" >&2
    exit 2
  fi
  printf '%s' "$value"
}

version="$(pinned_arg MISE_VERSION)"
sha256="$(pinned_arg "$sha_arg")"
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { say "not a mise version: '$version'" >&2; exit 2; }
[[ $sha256 =~ ^[0-9a-f]{64}$ ]] || { say "not a SHA-256: '$sha256'" >&2; exit 2; }

case "$mode" in
  pin)
    printf 'version=%s\nsha256=%s\n' "$version" "$sha256" >> "${GITHUB_OUTPUT:-/dev/stdout}"
    ;;
  fetch)
    dir=${3:?pass the asset directory}
    mkdir -p "$dir"
    cd "$dir"
    cached=false
    if [ -s mise ]; then
      cached=true
      say "using the cached mise $version ($arch); no download"
    else
      rm -f mise
      say "downloading mise $version ($arch)"
      # The window rides out a short burst of 5xx answers from the release host;
      # --max-time ends a stalled attempt so that it is retried.
      # A failed transfer may leave partial bytes; remove them so a later run does not treat them as cached.
      if ! curl -fLsS --connect-timeout 20 --max-time 120 --retry 7 --retry-all-errors --retry-max-time 180 -o mise \
        "https://github.com/jdx/mise/releases/download/v$version/mise-v$version-linux-$mise_arch"; then
        rm -f mise
        say "download failed" >&2
        exit 1
      fi
    fi
    say "checking mise against the in-repo SHA-256 $sha256"
    if ! printf '%s  %s\n' "$sha256" mise | sha256sum --check -; then
      [ "$cached" = true ] || rm -f mise
      say "checksum mismatch against the in-repo SHA-256" >&2
      exit 1
    fi
    ;;
  *)
    say "unknown mode '$mode'; pass pin or fetch" >&2
    exit 2
    ;;
esac
