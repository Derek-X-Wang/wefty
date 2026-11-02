#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
root="$(mktemp -d "${TMPDIR:-/tmp}/wefty-crane-retry.XXXXXX")"
trap 'rm -rf -- "$root"' EXIT
mkdir -p "$root/bin"
cat > "$root/bin/go" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ $# == 2 && $1 == env && $2 == GOMODCACHE ]]; then
  printf '%s\n' "$TEST_ROOT/module cache"
  exit 0
fi
if [[ $# == 5 && $1 == -C && -d $2 && $2 != "$TEST_ROOT/module cache"* && $3 == mod && $4 == download &&
  $5 == github.com/google/go-containerregistry@v0.22.0 ]]; then
  phase=module
elif [[ $# == 4 && $1 == -C && $2 == "$TEST_ROOT/module cache/github.com/google/go-containerregistry@v0.22.0" &&
  $3 == mod && $4 == download ]]; then
  phase=deps
else
  printf 'unexpected go arguments:' >&2; printf ' [%s]' "$@" >&2; printf '\n' >&2; exit 99
fi
count=0
if [[ -f $TEST_ROOT/$phase ]]; then read -r count < "$TEST_ROOT/$phase"; fi
count=$((count + 1))
printf '%s\n' "$count" > "$TEST_ROOT/$phase"
[[ $phase == "$TEST_PHASE" ]] || exit 0
case "$TEST_MODE" in
  recover) [[ $count -lt 3 ]] || exit 0 ;;
  checksum) printf 'verifying module: checksum mismatch\n' >&2 ;;
  security) printf 'SECURITY ERROR\n' >&2 ;;
  verifying) printf 'verifying module: checksum missing from sumdb response\n' >&2 ;;
  permanent) printf 'unknown revision v0.22.0\n' >&2; exit 17 ;;
  exhaust) ;;
  *) exit 99 ;;
esac
# Integrity cases also include a transient failure: integrity must win.
printf 'read https://proxy.golang.org/module.zip: stream error: stream ID 1; INTERNAL_ERROR; received from peer\n' >&2
exit 17
SH
cat > "$root/bin/sleep" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
[[ $* == 3 ]]
printf '%s\n' "$*" >> "$TEST_ROOT/sleeps"
SH
chmod +x "$root/bin/go" "$root/bin/sleep"

for phase in module deps; do
  for mode in recover checksum security verifying permanent exhaust; do
    case_root="$root/$phase-$mode"
    mkdir -p "$case_root"
    status=0
    PATH="$root/bin:$PATH" TEST_ROOT="$case_root" TEST_PHASE="$phase" TEST_MODE="$mode" \
      bash "$script_dir/download-crane-modules.sh" > "$case_root/output" 2>&1 || status=$?
    expected_status=17 attempts=1 sleeps=0
    case "$mode" in
      recover) expected_status=0 attempts=3 sleeps=2 ;;
      exhaust) attempts=3 sleeps=2 ;;
    esac
    actual_sleeps=0
    if [[ -f $case_root/sleeps ]]; then actual_sleeps="$(wc -l < "$case_root/sleeps" | tr -d ' ')"; fi
    if [[ $status != "$expected_status" || $(cat "$case_root/$phase") != "$attempts" || $actual_sleeps != "$sleeps" ]]; then
      cat "$case_root/output" >&2
      printf 'FAIL: %s/%s status=%s sleeps=%s\n' "$phase" "$mode" "$status" "$actual_sleeps" >&2
      exit 1
    fi
    if [[ $phase == module && $mode != recover && -f $case_root/deps ]]; then
      printf 'FAIL: dependencies attempted after module failure\n' >&2
      exit 1
    fi
    printf 'PASS: %s/%s status=%s attempts=%s sleeps=%s\n' "$phase" "$mode" "$status" "$attempts" "$actual_sleeps"
  done
done
