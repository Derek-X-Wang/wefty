#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  printf 'usage: %s RECEIPT EVIDENCE_SOURCE\n' "$0" >&2
  exit 64
fi

receipt=$1
evidence_source=$2

require_line() {
  if ! grep -Fqx "$1" "$receipt"; then
    printf 'native Linux OCI receipt omitted %s\n' "$1" >&2
    exit 1
  fi
}

reject_prefix() {
  if grep -Fq "$1" "$receipt"; then
    printf 'native Linux OCI receipt unexpectedly contains %s\n' "$1" >&2
    exit 1
  fi
}

case "$evidence_source" in
  pr-build)
    require_line 'pull_from_empty=NOT-RUN'
    require_line 'pull_from_empty_reason=pr-build: image not published'
    require_line 'pull_import_digest_equal=NOT-RUN'
    require_line 'pull_import_digest_equal_reason=pr-build: image not published'
    ;;
  published-artifact)
    require_line 'pull_from_empty=true'
    require_line 'pull_import_digest_equal=true'
    reject_prefix 'pull_from_empty_reason='
    reject_prefix 'pull_import_digest_equal_reason='
    ;;
  *)
    printf 'unsupported native Linux OCI evidence source %s\n' "$evidence_source" >&2
    exit 64
    ;;
esac
