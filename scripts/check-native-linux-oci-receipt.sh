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

require_duration_within() {
  file=$1
  key=$2
  max_seconds=$3
  count=$(awk -v prefix="$key=" 'index($0, prefix) == 1 { count++ } END { print count + 0 }' "$file")
  value=$(awk -v prefix="$key=" 'index($0, prefix) == 1 { print substr($0, length(prefix) + 1) }' "$file")
  if [ "$count" -ne 1 ] || ! awk -v value="$value" -v limit="$max_seconds" '
    BEGIN {
      remaining = value
      total_ns = 0
      components = 0
      while (length(remaining) > 0) {
        if (match(remaining, /^[0-9]+([.][0-9]+)?/) != 1) exit 1
        number = substr(remaining, 1, RLENGTH) + 0
        remaining = substr(remaining, RLENGTH + 1)
        if (match(remaining, /^(ns|us|µs|ms|s|m|h)/) != 1) exit 1
        unit = substr(remaining, 1, RLENGTH)
        remaining = substr(remaining, RLENGTH + 1)
        multiplier = 1
        if (unit == "us" || unit == "µs") multiplier = 1000
        else if (unit == "ms") multiplier = 1000000
        else if (unit == "s") multiplier = 1000000000
        else if (unit == "m") multiplier = 60000000000
        else if (unit == "h") multiplier = 3600000000000
        total_ns += number * multiplier
        components++
      }
      limit_ns = (limit + 0) * 1000000000
      # A 1ns literal is not credible elapsed-time evidence from this harness.
      # One microsecond is the smallest accepted measurement quantum.
      if (components == 0 || total_ns < 1000 || total_ns > limit_ns) exit 1
      exit 0
    }
  '; then
    printf 'receipt must contain exactly one measured %s duration within %ss\n' "$key" "$max_seconds" >&2
    exit 1
  fi
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
require_duration_within "$l1_agent_receipt" service_recovery_elapsed 15
require_unique_value "$service_receipt" term_kill_escalation true
require_unique_boolean "$service_receipt" term_kill_log_evidence_incomplete
require_unique_value "$service_receipt" term_kill_log_seal_pairing true
require_unique_value "$service_receipt" term_kill_stdout_log true
require_unique_value "$service_receipt" term_kill_stderr_log true
require_unique_value "$service_receipt" withdrawal true
require_duration_within "$service_receipt" withdrawal_elapsed 5
require_unique_value "$service_receipt" republication true
require_duration_within "$service_receipt" republication_elapsed 5
