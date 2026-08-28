#!/usr/bin/env bash
set -euo pipefail

archive=
expected_top_digest=
platform_digest_output=
receipt_output=
declare -a expected_platforms=()
declare -a expected_platform_digests=()

usage() {
  printf '%s\n' 'usage: scripts/verify-acceptance-image.sh --archive FILE [--expected-top-digest sha256:HEX] [--expected-platform OS/ARCH]... [--expected-platform-digest OS/ARCH=sha256:HEX]... [--platform-digest-output FILE] [--receipt-output FILE]'
}

while (($# > 0)); do
  case "$1" in
    --archive) archive="${2:-}"; shift ;;
    --expected-top-digest) expected_top_digest="${2:-}"; shift ;;
    --expected-platform) expected_platforms+=("${2:-}"); shift ;;
    --expected-platform-digest) expected_platform_digests+=("${2:-}"); shift ;;
    --platform-digest-output) platform_digest_output="${2:-}"; shift ;;
    --receipt-output) receipt_output="${2:-}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 64 ;;
  esac
  shift
done

[[ -f $archive ]] || { usage >&2; exit 64; }
[[ -z $expected_top_digest || $expected_top_digest =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'invalid expected top-level digest\n' >&2; exit 64; }
if ((${#expected_platforms[@]} == 0)); then
  printf 'at least one expected platform is required\n' >&2
  exit 64
fi

temporary_root="$(mktemp -d)"
trap 'rm -rf -- "$temporary_root"' EXIT

archive_member() {
  local member=$1 destination=$2
  if tar -xOf "$archive" "$member" >"$destination" 2>/dev/null; then
    return
  fi
  tar -xOf "$archive" "./$member" >"$destination"
}

hash_file() {
  local path=$1 value
  if command -v sha256sum >/dev/null 2>&1; then
    value="$(sha256sum "$path")"
  else
    value="$(shasum -a 256 "$path")"
  fi
  printf 'sha256:%s\n' "${value%% *}"
}

descriptor_blob() {
  local digest=$1 size=$2 output=$3
  [[ $digest =~ ^sha256:[0-9a-f]{64}$ && $size =~ ^[0-9]+$ ]] || { printf 'invalid OCI descriptor\n' >&2; exit 1; }
  archive_member "blobs/sha256/${digest#sha256:}" "$output"
  [[ $(wc -c <"$output" | tr -d ' ') == "$size" ]] || { printf 'OCI descriptor size mismatch for %s\n' "$digest" >&2; exit 1; }
  [[ $(hash_file "$output") == "$digest" ]] || { printf 'OCI descriptor digest mismatch for %s\n' "$digest" >&2; exit 1; }
}

root_index="$temporary_root/index.json"
archive_member index.json "$root_index"
[[ $(jq -er '.schemaVersion == 2 and (.manifests | length) == 1' "$root_index") == true ]] || { printf 'OCI archive must contain one top-level image\n' >&2; exit 1; }
top_digest="$(jq -er '.manifests[0].digest' "$root_index")"
top_size="$(jq -er '.manifests[0].size' "$root_index")"
top_media_type="$(jq -er '.manifests[0].mediaType' "$root_index")"
top_blob="$temporary_root/top.json"
descriptor_blob "$top_digest" "$top_size" "$top_blob"
if [[ -n $expected_top_digest && $top_digest != "$expected_top_digest" ]]; then
  printf 'archive top-level digest %s does not match published %s\n' "$top_digest" "$expected_top_digest" >&2
  exit 1
fi

platform_rows="$temporary_root/platforms.tsv"
: >"$platform_rows"
case "$top_media_type" in
  application/vnd.oci.image.index.v1+json|application/vnd.docker.distribution.manifest.list.v2+json)
    jq -er '.schemaVersion == 2' "$top_blob" >/dev/null
    jq -r '.manifests[] | select(.platform.os != null and .platform.architecture != null) | [.platform.os + "/" + .platform.architecture, .digest, (.size|tostring)] | @tsv' "$top_blob" >"$platform_rows"
    ;;
  application/vnd.oci.image.manifest.v1+json|application/vnd.docker.distribution.manifest.v2+json)
    config_digest="$(jq -er '.config.digest' "$top_blob")"
    config_size="$(jq -er '.config.size' "$top_blob")"
    config_blob="$temporary_root/config.json"
    descriptor_blob "$config_digest" "$config_size" "$config_blob"
    platform="$(jq -er '.os + "/" + .architecture' "$config_blob")"
    printf '%s\t%s\t%s\n' "$platform" "$top_digest" "$top_size" >"$platform_rows"
    ;;
  *) printf 'unsupported top-level OCI media type %s\n' "$top_media_type" >&2; exit 1 ;;
esac

for expected_platform in "${expected_platforms[@]}"; do
  [[ $expected_platform =~ ^linux/(amd64|arm64)$ ]] || { printf 'invalid expected platform %s\n' "$expected_platform" >&2; exit 64; }
  matches="$(awk -F '\t' -v platform="$expected_platform" '$1 == platform { count++ } END { print count+0 }' "$platform_rows")"
  [[ $matches == 1 ]] || { printf 'platform %s appears %s times, want exactly once\n' "$expected_platform" "$matches" >&2; exit 1; }
done

if [[ $(wc -l <"$platform_rows" | tr -d ' ') != "${#expected_platforms[@]}" ]]; then
  printf 'OCI image contains an unexpected platform set\n' >&2
  exit 1
fi

for expected in "${expected_platform_digests[@]}"; do
  platform="${expected%%=*}"
  digest="${expected#*=}"
  [[ $platform != "$expected" && $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { printf 'invalid expected platform digest %s\n' "$expected" >&2; exit 64; }
  actual="$(awk -F '\t' -v platform="$platform" '$1 == platform { print $2 }' "$platform_rows")"
  [[ $actual == "$digest" ]] || { printf '%s digest %s does not match reproducible build %s\n' "$platform" "$actual" "$digest" >&2; exit 1; }
done

if [[ -n $platform_digest_output ]]; then
  [[ ${#expected_platforms[@]} == 1 ]] || { printf 'platform digest output requires exactly one expected platform\n' >&2; exit 64; }
  awk -F '\t' -v platform="${expected_platforms[0]}" '$1 == platform { print $2 }' "$platform_rows" >"$platform_digest_output"
fi

receipt="$(jq -n --arg top_level_digest "$top_digest" --rawfile rows "$platform_rows" '
  {top_level_digest: $top_level_digest, platforms: ($rows | split("\n") | map(select(length > 0) | split("\t") | {platform: .[0], digest: .[1], size: (.[2] | tonumber)}))}')"
if [[ -n $receipt_output ]]; then
  printf '%s\n' "$receipt" >"$receipt_output"
else
  printf '%s\n' "$receipt"
fi
