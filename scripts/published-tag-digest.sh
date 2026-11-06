#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' 'usage: published-tag-digest.sh index-digest REPO | read-tag CRANE TAG EXPECTED_DIGEST' >&2
}

case "${1:-}" in
  index-digest)
    [[ $# == 2 ]] || { usage; exit 64; }
    repo=$2
    # crane v0.22.0 cmd/crane/cmd/index.go appendRemote prints exactly
    # targetRef.Context().Digest(digest.String()) followed by a newline.
    # Preserve trailing newlines so extra output cannot be silently discarded.
    output="$(cat; printf '.')"
    reference=${output%.}
    reference=${reference%$'\n'}
    digest=${reference#"$repo@"}
    if [[ $reference != "$repo@$digest" || ! $digest =~ ^sha256:[0-9a-fA-F]{64}$ ]]; then
      printf 'invalid crane index append output: expected exactly one %s@sha256:<64 hex> reference\n' "$repo" >&2
      exit 1
    fi
    printf '%s\n' "$digest"
    ;;
  read-tag)
    [[ $# == 4 ]] || { usage; exit 64; }
    crane=$2 tag=$3 expected=$4
    error_file="$(mktemp)"
    trap 'rm -f -- "$error_file"' EXIT
    for ((attempt = 1; attempt <= 5; attempt++)); do
      if actual="$("$crane" digest "$tag" 2>"$error_file")"; then
        if [[ $actual != "$expected" ]]; then
          printf 'published tag %s digest mismatch: expected %s, got %s\n' "$tag" "$expected" "$actual" >&2
          exit 1
        fi
        printf '%s\n' "$actual"
        exit 0
      fi
      if ((attempt < 5)); then sleep 2; fi
    done
    printf 'published tag %s did not resolve to expected %s after 5 attempts; last error: %s\n' \
      "$tag" "$expected" "$(cat "$error_file")" >&2
    exit 1
    ;;
  *)
    usage
    exit 64
    ;;
esac
