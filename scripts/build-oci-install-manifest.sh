#!/usr/bin/env bash
set -euo pipefail

helper=
probe_reference=
probe_digest=
probe_archive=
output=

usage() {
  printf '%s\n' 'usage: scripts/build-oci-install-manifest.sh --helper FILE --probe-reference REF --probe-digest sha256:HEX --probe-archive FILE --output FILE'
}

while (($# > 0)); do
  case "$1" in
    --helper) helper="${2:-}"; shift ;;
    --probe-reference) probe_reference="${2:-}"; shift ;;
    --probe-digest) probe_digest="${2:-}"; shift ;;
    --probe-archive) probe_archive="${2:-}"; shift ;;
    --output) output="${2:-}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 64 ;;
  esac
  shift
done

[[ -f $helper && -f $probe_archive && -n $output ]] || { usage >&2; exit 64; }
[[ $probe_digest =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'invalid probe digest\n' >&2; exit 64; }
[[ -n $probe_reference && $probe_reference != *'"'* && $probe_reference != *$'\r'* && $probe_reference != *$'\n'* ]] || { printf 'invalid probe reference\n' >&2; exit 64; }

if command -v sha256sum >/dev/null 2>&1; then
  helper_digest="$(sha256sum "$helper")"
  helper_digest="${helper_digest%% *}"
elif command -v shasum >/dev/null 2>&1; then
  helper_digest="$(shasum -a 256 "$helper")"
  helper_digest="${helper_digest%% *}"
else
  printf 'sha256sum or shasum is required\n' >&2
  exit 64
fi

output_dir="$(dirname -- "$output")"
archive_name="$(basename -- "$probe_archive")"
[[ $archive_name != *'"'* && $archive_name != *$'\r'* && $archive_name != *$'\n'* ]] || { printf 'invalid probe archive name\n' >&2; exit 64; }
mkdir -p -- "$output_dir"
if [[ $(cd -- "$(dirname -- "$probe_archive")" && pwd)/$archive_name != $(cd -- "$output_dir" && pwd)/$archive_name ]]; then
  install -m 0644 "$probe_archive" "$output_dir/$archive_name"
fi
temporary="$(mktemp "$output_dir/.manifest.json.XXXXXX")"
trap 'rm -f -- "$temporary"' EXIT
printf '%s\n' \
  '{' \
  '  "version": 1,' \
  "  \"helper_checksum\": \"sha256:$helper_digest\"," \
  "  \"probe_reference\": \"$probe_reference\"," \
  "  \"probe_digest\": \"$probe_digest\"," \
  "  \"probe_archive_path\": \"$archive_name\"" \
  '}' >"$temporary"
chmod 0644 "$temporary"
mv -f -- "$temporary" "$output"
printf 'wrote %s and %s\n' "$output" "$output_dir/$archive_name"
