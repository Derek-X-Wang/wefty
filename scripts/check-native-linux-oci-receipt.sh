#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
  printf 'usage: %s NATIVE_RECEIPT EVIDENCE_SOURCE SERVICE_PUBLICATION_RECEIPT L1_AGENT_RECEIPT\n' "$0" >&2
  exit 64
fi

receipt=$1
evidence_source=$2
service_receipt=$3
l1_agent_receipt=$4

require_unique_value() {
  file=$1
  key=$2
  expected=$3
  count=$(awk -v prefix="$key=" 'index($0, prefix) == 1 { count++ } END { print count + 0 }' "$file")
  if [ "$count" -ne 1 ] || ! grep -Fqx "$key=$expected" "$file"; then
    printf 'receipt must contain exactly one %s=%s\n' "$key" "$expected" >&2
    exit 1
  fi
}

require_unique_boolean() {
  file=$1
  key=$2
  count=$(awk -v prefix="$key=" 'index($0, prefix) == 1 { count++ } END { print count + 0 }' "$file")
  if [ "$count" -ne 1 ] || ! grep -Eq "^${key}=(true|false)$" "$file"; then
    printf 'receipt must contain exactly one boolean %s\n' "$key" >&2
    exit 1
  fi
}

duration_within_bound() {
  duration_remaining=$1
  duration_limit_seconds=$2
  duration_trace_parsed_ns=UNPARSED
  duration_trace_bound_ns=UNPARSED
  case "$duration_limit_seconds" in
    ''|*[!0-9]*) return 1 ;;
  esac
  while [ "${duration_limit_seconds#0}" != "$duration_limit_seconds" ]; do
    duration_limit_seconds=${duration_limit_seconds#0}
  done
  [ -n "$duration_limit_seconds" ] || duration_limit_seconds=0
  [ "${#duration_limit_seconds}" -le 9 ] || return 1
  duration_limit_ns=$((duration_limit_seconds * 1000000000))
  duration_trace_bound_ns=$duration_limit_ns
  duration_total_ns=0
  duration_components=0
  duration_max_ns=9223372036854775807

  while [ -n "$duration_remaining" ]; do
    duration_number=
    while [ -n "$duration_remaining" ]; do
      duration_first=${duration_remaining%"${duration_remaining#?}"}
      case "$duration_first" in
        [0-9]|.)
          duration_number=$duration_number$duration_first
          duration_remaining=${duration_remaining#?}
          ;;
        *) break ;;
      esac
    done
    case "$duration_number" in
      ''|.*|*.|*.*.*|*[!0-9.]*) return 1 ;;
    esac

    case "$duration_number" in
      *.*)
        duration_integer=${duration_number%%.*}
        duration_fraction=${duration_number#*.}
        ;;
      *)
        duration_integer=$duration_number
        duration_fraction=
        ;;
    esac
    while [ "${duration_integer#0}" != "$duration_integer" ]; do
      duration_integer=${duration_integer#0}
    done
    [ -n "$duration_integer" ] || duration_integer=0
    [ "${#duration_integer}" -le 18 ] || return 1

    case "$duration_remaining" in
      ns*) duration_multiplier=1; duration_remaining=${duration_remaining#ns} ;;
      us*) duration_multiplier=1000; duration_remaining=${duration_remaining#us} ;;
      µs*) duration_multiplier=1000; duration_remaining=${duration_remaining#µs} ;;
      ms*) duration_multiplier=1000000; duration_remaining=${duration_remaining#ms} ;;
      s*) duration_multiplier=1000000000; duration_remaining=${duration_remaining#s} ;;
      m*) duration_multiplier=60000000000; duration_remaining=${duration_remaining#m} ;;
      h*) duration_multiplier=3600000000000; duration_remaining=${duration_remaining#h} ;;
      *) return 1 ;;
    esac

    duration_max_integer=$((duration_max_ns / duration_multiplier))
    [ "$duration_integer" -le "$duration_max_integer" ] || return 1
    duration_component_ns=$((duration_integer * duration_multiplier))
    duration_fraction_scale=$duration_multiplier
    while [ -n "$duration_fraction" ]; do
      duration_first=${duration_fraction%"${duration_fraction#?}"}
      duration_fraction=${duration_fraction#?}
      duration_fraction_scale=$((duration_fraction_scale / 10))
      [ "$duration_fraction_scale" -gt 0 ] || return 1
      duration_fraction_ns=$((duration_first * duration_fraction_scale))
      [ "$duration_fraction_ns" -le $((duration_max_ns - duration_component_ns)) ] || return 1
      duration_component_ns=$((duration_component_ns + duration_fraction_ns))
    done
    [ "$duration_component_ns" -le $((duration_max_ns - duration_total_ns)) ] || return 1
    duration_total_ns=$((duration_total_ns + duration_component_ns))
    duration_components=$((duration_components + 1))
  done

  duration_trace_parsed_ns=$duration_total_ns
  # A 1ns literal is not credible elapsed-time evidence from this harness.
  # One microsecond is the smallest accepted measurement quantum.
  [ "$duration_components" -gt 0 ] && [ "$duration_total_ns" -ge 1000 ] && [ "$duration_total_ns" -le "$duration_limit_ns" ]
}

trace_duration_result() {
  [ "${WEFTY_RECEIPT_GATE_TRACE:-0}" = 1 ] || return 0
  printf 'receipt_duration key=%s value=%s parsed_ns=%s bound_ns=%s result=%s\n' \
    "$key" "$value" "${duration_trace_parsed_ns:-UNPARSED}" "${duration_trace_bound_ns:-UNPARSED}" "$1" >&2
}

require_duration_within() {
  file=$1
  key=$2
  max_seconds=$3
  count=$(grep -c "^${key}=" "$file" || true)
  value=$(sed -n "s/^${key}=//p" "$file")
  duration_trace_parsed_ns=UNPARSED
  duration_trace_bound_ns=UNPARSED
  if [ "$count" -eq 1 ] && duration_within_bound "$value" "$max_seconds"; then
    trace_duration_result accepted
    return
  fi
  trace_duration_result rejected
  printf 'receipt must contain exactly one measured %s duration within %ss\n' "$key" "$max_seconds" >&2
  exit 1
}

duration_within_receipt_bound() {
  measured=$1
  bound=$2
  # Reuse the checked Go-duration parser with a large finite ceiling, then
  # compare the two parsed values. The caller restores the measured/bound
  # trace fields so diagnostics name the receipt-derived authority.
  if ! duration_within_bound "$measured" 999999999; then
    return 1
  fi
  measured_ns=$duration_trace_parsed_ns
  if ! duration_within_bound "$bound" 999999999; then
    return 1
  fi
  bound_ns=$duration_trace_parsed_ns
  duration_trace_parsed_ns=$measured_ns
  duration_trace_bound_ns=$bound_ns
  [ "$measured_ns" -le "$bound_ns" ]
}

require_duration_within_receipt_bound() {
  file=$1
  key=$2
  bound_key=$3
  count=$(grep -c "^${key}=" "$file" || true)
  bound_count=$(grep -c "^${bound_key}=" "$file" || true)
  value=$(sed -n "s/^${key}=//p" "$file")
  bound=$(sed -n "s/^${bound_key}=//p" "$file")
  duration_trace_parsed_ns=UNPARSED
  duration_trace_bound_ns=UNPARSED
  if [ "$count" -eq 1 ] && [ "$bound_count" -eq 1 ] && duration_within_receipt_bound "$value" "$bound"; then
    trace_duration_result accepted
    return
  fi
  trace_duration_result rejected
  printf 'receipt must contain exactly one measured %s duration within its exactly one %s bound\n' "$key" "$bound_key" >&2
  exit 1
}

case "$evidence_source" in
  pr-build)
    require_unique_value "$receipt" pull_from_empty NOT-RUN
    require_unique_value "$receipt" pull_from_empty_reason 'pr-build: image not published'
    require_unique_value "$receipt" pull_import_digest_equal NOT-RUN
    require_unique_value "$receipt" pull_import_digest_equal_reason 'pr-build: image not published'
    require_unique_value "$receipt" binding_repull_reconciliation NOT-RUN
    require_unique_value "$receipt" binding_repull_reconciliation_reason 'pr-build: image not published'
    ;;
  published-artifact)
    require_unique_value "$receipt" pull_from_empty true
    require_unique_value "$receipt" pull_import_digest_equal true
    require_unique_value "$receipt" binding_repull_reconciliation true
    if grep -Eq '^(pull_from_empty|pull_import_digest_equal|binding_repull_reconciliation)_reason=' "$receipt"; then
      printf 'published native Linux OCI receipt unexpectedly contains a NOT-RUN reason\n' >&2
      exit 1
    fi
    ;;
  *)
    printf 'unsupported native Linux OCI evidence source %s\n' "$evidence_source" >&2
    exit 64
    ;;
esac

require_unique_value "$l1_agent_receipt" service_fresh_attempt_readmission true
require_unique_value "$l1_agent_receipt" service_helper_loss_injected true
require_unique_value "$l1_agent_receipt" service_barrier_advertised_reap_timeout 10s
require_unique_value "$l1_agent_receipt" service_barrier_takeover_bound 20s
require_unique_value "$l1_agent_receipt" service_barrier_verified_ready_bound 30s
require_unique_value "$l1_agent_receipt" service_fresh_attempt_admission_bound 30s
require_duration_within_receipt_bound "$l1_agent_receipt" service_fresh_attempt_admission_elapsed service_fresh_attempt_admission_bound
require_duration_within_receipt_bound "$l1_agent_receipt" service_barrier_handshake_elapsed service_barrier_takeover_bound
require_duration_within_receipt_bound "$l1_agent_receipt" service_barrier_session_admission_elapsed service_barrier_takeover_bound
require_duration_within_receipt_bound "$l1_agent_receipt" service_barrier_sweep_elapsed service_barrier_advertised_reap_timeout
require_duration_within_receipt_bound "$l1_agent_receipt" service_barrier_verify_elapsed service_barrier_advertised_reap_timeout
require_duration_within_receipt_bound "$l1_agent_receipt" service_barrier_verified_ready_elapsed service_barrier_verified_ready_bound
require_unique_value "$l1_agent_receipt" service_lost_log_typed true
if ! grep -Eq '^service_lost_log_disposition=(swept|retained):[a-z_]+$' "$l1_agent_receipt"; then
  printf 'receipt must contain one typed lost-attempt log disposition\n' >&2
  exit 1
fi
require_unique_value "$service_receipt" term_kill_escalation true
require_unique_boolean "$service_receipt" term_kill_log_evidence_incomplete
require_unique_value "$service_receipt" term_kill_log_seal_pairing true
require_unique_value "$service_receipt" term_kill_stdout_log true
require_unique_value "$service_receipt" term_kill_stderr_log true
require_unique_value "$service_receipt" withdrawal true
require_duration_within "$service_receipt" withdrawal_elapsed 5
require_unique_value "$service_receipt" republication true
require_duration_within_receipt_bound "$service_receipt" republication_elapsed republication_observation_deadline
