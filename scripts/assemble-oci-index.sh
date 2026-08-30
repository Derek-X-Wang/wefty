#!/usr/bin/env bash
set -euo pipefail

reference=
output=
index_output=
digest_output=
declare -a image_specs=()

usage() {
  printf '%s\n' 'usage: scripts/assemble-oci-index.sh --reference NAME --output FILE --index-output FILE --digest-output FILE --image OS/ARCH=ARCHIVE [--image OS/ARCH=ARCHIVE]...'
}

while (($# > 0)); do
  case "$1" in
    --reference) reference="${2:-}"; shift ;;
    --output) output="${2:-}"; shift ;;
    --index-output) index_output="${2:-}"; shift ;;
    --digest-output) digest_output="${2:-}"; shift ;;
    --image) image_specs+=("${2:-}"); shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 64 ;;
  esac
  shift
done

[[ -n $reference && -n $output && -n $index_output && -n $digest_output ]] || { usage >&2; exit 64; }
((${#image_specs[@]} > 0)) || { usage >&2; exit 64; }

temporary_root="$(mktemp -d)"
trap 'rm -rf -- "$temporary_root"' EXIT
layout="$temporary_root/layout"
mkdir -p "$layout/blobs/sha256"
printf '{"imageLayoutVersion":"1.0.0"}\n' > "$layout/oci-layout"
descriptors="$temporary_root/descriptors.jsonl"
: > "$descriptors"

hash_file() {
  local path=$1 value
  if command -v sha256sum >/dev/null 2>&1; then
    value="$(sha256sum "$path")"
  else
    value="$(shasum -a 256 "$path")"
  fi
  printf 'sha256:%s\n' "${value%% *}"
}

copy_blob() {
  local source=$1 destination=$2
  if [[ -e $destination ]]; then
    cmp "$source" "$destination"
  else
    cp "$source" "$destination"
  fi
}

for index in "${!image_specs[@]}"; do
  spec="${image_specs[$index]}"
  platform="${spec%%=*}"
  archive="${spec#*=}"
  [[ $platform != "$spec" && $platform =~ ^linux/(amd64|arm64)$ && -f $archive ]] || { printf 'invalid image input %s\n' "$spec" >&2; exit 64; }
  source_layout="$temporary_root/source-$index"
  mkdir -p "$source_layout"
  tar -xf "$archive" -C "$source_layout"
  jq -e '.schemaVersion == 2 and (.manifests | length) == 1' "$source_layout/index.json" >/dev/null
  top_digest="$(jq -er '.manifests[0].digest' "$source_layout/index.json")"
  top_blob="$source_layout/blobs/sha256/${top_digest#sha256:}"
  [[ -f $top_blob && $(hash_file "$top_blob") == "$top_digest" ]] || { printf 'invalid top-level descriptor in %s\n' "$archive" >&2; exit 1; }
  top_media_type="$(jq -er '.manifests[0].mediaType' "$source_layout/index.json")"
  case "$top_media_type" in
    application/vnd.oci.image.index.v1+json|application/vnd.docker.distribution.manifest.list.v2+json)
      descriptor="$(jq -cer --arg platform "$platform" '
        ($platform | split("/")) as $parts |
        [.manifests[] | select(.platform.os == $parts[0] and .platform.architecture == $parts[1])] |
        if length == 1 then .[0] else error("platform descriptor must appear exactly once") end
      ' "$top_blob")"
      ;;
    application/vnd.oci.image.manifest.v1+json|application/vnd.docker.distribution.manifest.v2+json)
      descriptor="$(jq -cer --arg platform "$platform" '
        ($platform | split("/")) as $parts |
        .manifests[0] + {platform: {os: $parts[0], architecture: $parts[1]}}
      ' "$source_layout/index.json")"
      ;;
    *) printf 'unsupported top-level OCI media type %s\n' "$top_media_type" >&2; exit 1 ;;
  esac
  printf '%s\n' "$descriptor" >> "$descriptors"
  while IFS= read -r blob; do
    copy_blob "$blob" "$layout/blobs/sha256/${blob##*/}"
  done < <(find "$source_layout/blobs/sha256" -type f -print)
done

jq -sc '{schemaVersion: 2, mediaType: "application/vnd.oci.image.index.v1+json", manifests: .}' "$descriptors" > "$index_output"
index_digest="$(hash_file "$index_output")"
index_size="$(wc -c < "$index_output" | tr -d ' ')"
copy_blob "$index_output" "$layout/blobs/sha256/${index_digest#sha256:}"
jq -cn --arg digest "$index_digest" --argjson size "$index_size" --arg reference "$reference" '
  {schemaVersion: 2, manifests: [{mediaType: "application/vnd.oci.image.index.v1+json", digest: $digest, size: $size, annotations: {"org.opencontainers.image.ref.name": $reference}}]}
' > "$layout/index.json"
printf '%s\n' "$index_digest" > "$digest_output"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner -C "$layout" -cf "$output" .
