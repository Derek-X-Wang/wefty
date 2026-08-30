#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
  printf 'usage: %s NATIVE_RECEIPT EVIDENCE_SOURCE SERVICE_PUBLICATION_RECEIPT\n' "$0" >&2
  exit 64
fi

receipt=$1
evidence_source=$2
service_receipt=$3

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

require_unique_measurement() {
  file=$1
  key=$2
  count=$(awk -v prefix="$key=" 'index($0, prefix) == 1 { count++ } END { print count + 0 }' "$file")
  if [ "$count" -ne 1 ] || ! grep -Eq "^${key}=.+$" "$file" || grep -Fqx "$key=0s" "$file"; then
    printf 'receipt must contain exactly one non-zero %s measurement\n' "$key" >&2
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

require_unique_value "$receipt" service_fresh_attempt_readmission true
require_unique_measurement "$receipt" service_recovery_elapsed
require_unique_value "$service_receipt" term_kill_escalation true
require_unique_boolean "$service_receipt" term_kill_log_evidence_incomplete
require_unique_value "$service_receipt" term_kill_log_seal_pairing true
require_unique_value "$service_receipt" term_kill_stdout_log true
require_unique_value "$service_receipt" term_kill_stderr_log true
require_unique_value "$service_receipt" withdrawal true
require_unique_measurement "$service_receipt" withdrawal_elapsed
require_unique_value "$service_receipt" republication true
require_unique_measurement "$service_receipt" republication_elapsed
