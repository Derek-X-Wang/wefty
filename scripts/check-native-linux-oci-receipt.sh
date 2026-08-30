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
      if (value !~ /^[0-9]+([.][0-9]+)?(ns|us|µs|ms|s|m|h)$/) exit 1
      unit = value
      sub(/^[0-9]+([.][0-9]+)?/, "", unit)
      number = value
      sub(/(ns|us|µs|ms|s|m|h)$/, "", number)
      multiplier = 1
      if (unit == "ns") multiplier = 0.000000001
      else if (unit == "us" || unit == "µs") multiplier = 0.000001
      else if (unit == "ms") multiplier = 0.001
      else if (unit == "m") multiplier = 60
      else if (unit == "h") multiplier = 3600
      seconds = number * multiplier
      exit !(seconds > 0 && seconds <= limit)
    }
  '; then
    printf 'receipt must contain exactly one positive %s duration within %ss\n' "$key" "$max_seconds" >&2
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
