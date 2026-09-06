#!/usr/bin/env bash
set -euo pipefail

readonly readiness_pairings='[
  {"check":"transport.view-ready","events":["view_endpoint_ready","first_rfb_frame"]},
  {"check":"transport.control-ready","events":["control_endpoint_ready","first_rfb_frame"]},
  {"check":"input.view-isolated","events":["input_oracle_ready","key_observer_advanced"]},
  {"check":"input.view-isolated-during-tenure","events":["key_observer_advanced"]}
]'

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
  if ! jq -e --argjson readiness_pairings "$readiness_pairings" '
    def valid_readiness_pair:
      . as $check |
      any($readiness_pairings[];
        .check == $check.id and any(.events[]; . == $check.readiness_event));
    type == "object" and .version == 2 and
    (.checks | type == "array" and all(.[];
      type == "object" and (.id | type == "string") and
      (.status == "PASS" or .status == "FAIL" or .status == "NOT-RUN") and
      ((.detail // "") | type == "string") and
      if .status == "FAIL" then
        (.failure_reason == "assertion_failed" or .failure_reason == "mutation_detected" or .failure_reason == "readiness_timeout") and
        if .failure_reason == "readiness_timeout" then
          valid_readiness_pair and
          (.readiness_observation_window_seconds | type == "number" and . > 0) and
          (.readiness_observation_elapsed_seconds | type == "number") and
          (.readiness_observation_elapsed_seconds >= .readiness_observation_window_seconds) and
          ((.readiness_observed_later // false) | type == "boolean")
        else
          (has("readiness_event") | not) and
          (has("readiness_observed_later") | not) and
          if .failure_reason == "assertion_failed" then
            (((has("readiness_observation_window_seconds") | not) and
              (has("readiness_observation_elapsed_seconds") | not)) or
             ((.readiness_observation_window_seconds | type == "number" and . > 0) and
              (.readiness_observation_elapsed_seconds | type == "number") and
              (.readiness_observation_elapsed_seconds >= .readiness_observation_window_seconds)))
          else
            (has("readiness_observation_window_seconds") | not) and
            (has("readiness_observation_elapsed_seconds") | not)
          end
        end
      else
        (has("failure_reason") | not) and (has("readiness_event") | not) and
        (has("readiness_observation_window_seconds") | not) and
        (has("readiness_observation_elapsed_seconds") | not) and
        (has("readiness_observed_later") | not)
      end
    )) and
    (.teardown | type == "object") and
    (.teardown.retries_used | type == "number" and . >= 0 and floor == .) and
    (.teardown.permission_repair_performed | type == "boolean") and
    ((.teardown.permission_repair_seconds // 0) | type == "number" and . >= 0) and
    (.teardown.observations | type == "array" and all(.[]; type == "object" and (.reason | type == "string") and ((.detail // "") | type == "string"))) and
    (.teardown.leftovers | type == "array" and all(.[]; type == "string")) and
    (.teardown.leftovers | length) == 0
  ' "$receipt" >/dev/null 2>&1; then
    error "receipt/$row" "malformed receipt $receipt"
    return 1
  fi
}

case ${1:-} in
  positive)
    [[ $# == 2 ]] || { error diagnostics-usage 'positive requires RECEIPT'; exit 64; }
    receipt=$2
    require_receipt "$receipt" positive-runtime || exit 1
    if ! jq -e '.status == "PASS"' "$receipt" >/dev/null 2>&1; then
      error receipt/positive-runtime "non-PASS receipt $receipt"
      exit 1
    fi
    ;;
  teardown-repair)
    [[ $# == 3 ]] || { error diagnostics-usage 'teardown-repair requires RECEIPT CHECKER_STATUS'; exit 64; }
    receipt=$2 checker_status=$3
    require_receipt "$receipt" teardown-repair || exit 1
    if [[ $checker_status != 2 ]]; then
      error teardown-repair "checker exited $checker_status; expected 2 for the teardown-only fixture"
      exit 1
    fi
    if ! jq -e '
      .teardown.permission_repair_performed == true and
      (.teardown.permission_repair_seconds | type == "number") and
      .teardown.permission_repair_seconds > 0 and
      .teardown.permission_repair_seconds <= 15 and
      (.teardown.leftovers | length) == 0
    ' "$receipt" >/dev/null; then
      error teardown-repair 'receipt did not prove bounded permission repair without leftovers'
      exit 1
    fi
    ;;
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
      [$failed[] | select(.failure_reason == "mutation_detected" and .id == $cell and .detail == $detail)] as $mutation |
      [.checks[] | select(.id == $cell)] as $expected_id |
      [$failed[] | select(.failure_reason == "readiness_timeout" and (.id | startswith("input.")))] as $input_timeouts |
      [.checks[] | select((.id == "input.view-isolated" or .id == "input.view-isolated-during-tenure") and .status != "NOT-RUN")] as $input_started |
      ($mutation | length) == 1 and
      ($expected_id | length) == 1 and
      all($failed[];
        (.failure_reason == "mutation_detected" and .id == $cell and .detail == $detail) or
        .failure_reason == "readiness_timeout"
      ) and
      (($input_timeouts | length) == 0 or
        (all($input_timeouts[]; .readiness_observed_later == true) and
         ([.checks[] | select(.id == "input.control-accepted" and .status == "PASS")] | length) == 1)) and
      (($input_started | length) == 0 or $cell == "input.view-isolated" or
        ([.checks[] | select(.id == "input.control-accepted" and .status != "NOT-RUN")] | length) == 1)
    ' "$receipt" >/dev/null; then
      error "fail-set/$mutation" "unexpected FAIL rows after ${checker_wall_seconds}s checker wall time; expected exactly $cell"
      jq -c '[.checks[] | select(.status == "FAIL") | {id,detail,failure_reason,readiness_event}]' "$receipt" >&2 || \
        error "receipt-jq/$mutation" 'could not render unexpected FAIL rows'
      exit 1
    fi
    readiness_timeouts=$(jq -c '[.checks[] | select(.status == "FAIL" and .failure_reason == "readiness_timeout") | {id,readiness_event}]' "$receipt") || {
      error "receipt-jq/$mutation" 'could not render readiness timeout rows'
      exit 1
    }
    if [[ $readiness_timeouts != '[]' ]]; then
      printf '::notice title=computer runtime conformance::readiness-timeout/%s: mutation detected with unrelated typed readiness rows %s\n' \
        "$mutation" "$readiness_timeouts" >&2
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
    error diagnostics-usage 'expected positive, teardown-repair, mutation, or summary'
    exit 64
    ;;
esac
