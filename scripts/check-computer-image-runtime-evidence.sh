#!/usr/bin/env bash
set -euo pipefail

error() {
  local stage=$1 message=$2
  printf '::error title=computer runtime conformance::%s: %s\n' "$stage" "$message" >&2
}

require_receipt() {
  local receipt=$1 row=$2
  if [[ ! -f $receipt ]]; then
    error "receipt/$row" "missing receipt $receipt"
    return 1
  fi
  if ! jq -e 'type == "object" and (.checks | type == "array")' "$receipt" >/dev/null 2>&1; then
    error "receipt/$row" "malformed receipt $receipt"
    return 1
  fi
}

case ${1:-} in
  mutation)
    [[ $# == 7 ]] || { error diagnostics-usage 'mutation requires RECEIPT MUTATION CELL DETAIL CHECKER_STATUS CHECKER_WALL_SECONDS'; exit 64; }
    receipt=$2 mutation=$3 cell=$4 detail=$5 checker_status=$6 checker_wall_seconds=$7
    require_receipt "$receipt" "$mutation" || exit 1
    if [[ $checker_status == 64 ]]; then
      error "checker-exit/$mutation" 'checker reported a usage failure'
      exit 1
    fi
    if [[ $checker_status != 1 ]]; then
      error "checker-exit/$mutation" "checker exited $checker_status; expected 1 for the intentional mutation"
      exit 1
    fi
    if ! failed_count=$(jq -er '[.checks[] | select(.status == "FAIL")] | length' "$receipt"); then
      error "receipt-jq/$mutation" 'could not aggregate FAIL rows'
      exit 1
    fi
    if ((failed_count == 0)); then
      error "fail-set/$mutation" "zero FAIL rows after ${checker_wall_seconds}s checker wall time; expected exactly $cell"
      exit 1
    fi
    if ! jq -e --arg cell "$cell" --arg detail "$detail" '
      [.checks[] | select(.status == "FAIL")] as $failed |
      ($failed | length) == 1 and $failed[0].id == $cell and $failed[0].detail == $detail
    ' "$receipt" >/dev/null; then
      error "fail-set/$mutation" "unexpected FAIL rows after ${checker_wall_seconds}s checker wall time; expected exactly $cell"
      jq -c '[.checks[] | select(.status == "FAIL") | {id,detail}]' "$receipt" >&2 || \
        error "receipt-jq/$mutation" 'could not render unexpected FAIL rows'
      exit 1
    fi
    ;;
  summary)
    [[ $# == 4 ]] || { error diagnostics-usage 'summary requires RECEIPT PLATFORM EXPECTED_ROWS'; exit 64; }
    receipt=$2 platform=$3 expected_rows=$4
    if [[ ! -f $receipt ]]; then
      error summary-receipt "missing summary receipt $receipt"
      exit 1
    fi
    if ! jq -e --arg platform "$platform" --argjson rows "$expected_rows" '
      type == "object" and .version == 1 and .platform == $platform and .executed_rows == $rows
    ' "$receipt" >/dev/null 2>&1; then
      error row-count "summary did not prove platform=$platform executed_rows=$expected_rows"
      exit 1
    fi
    ;;
  *)
    error diagnostics-usage 'expected mutation or summary'
    exit 64
    ;;
esac
