#!/bin/sh
set -eu

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  printf '%s\n' 'usage: check-linux-computer-receipt.sh RECEIPT CANDIDATE_SHA xfce|wayland [MUTATED_ROW]' >&2
  exit 64
fi

receipt=$1
candidate_sha=$2
expected_image=$3
mutated_row=${4:-}
case "$expected_image" in xfce|wayland) ;; *) exit 64 ;; esac
test -s "$receipt"

jq -e --arg candidate "$candidate_sha" --arg image "$expected_image" --arg mutated "$mutated_row" '
  def required_rows: [
    "linux.create_boot",
    "linux.remote_takeover",
    "linux.restart_survival",
    "linux.reconfiguration",
    "linux.storage_provenance",
    "linux.guest_authority",
    "linux.removal"
  ];
  . as $root |
  .version == 2 and
  .platform == "linux/amd64" and
  .candidate_sha == $candidate and
  .image.variant == $image and
  (.candidate_sha | test("^[0-9a-f]{40}$")) and
  (if $mutated == "" then .status == "NOT-RUN" and .not_run_issue == 286 else .status == "FAIL" end) and
  (.image.index_digest | test("^sha256:[0-9a-f]{64}$")) and
  (.image.platform_digest | test("^sha256:[0-9a-f]{64}$")) and
  (.fabric_identities | length >= 2) and
  ([.fabric_identities[] | select(.fabric_id == "" or .user_id == "" or .device_id == "")] | length == 0) and
  (.authority_generations | length >= 1) and
  ([.authority_generations[] | select(. <= 0)] | length == 0) and
  (.computer_ids | length >= 4) and
  (.job_ids | length >= 4) and
  (.attempt_ids | length >= 4) and
  (.storage_ids | length >= 4) and
  (.resource_caps.memory_bytes > 0 and .resource_caps.disk_bytes > 0 and .resource_caps.backup_cap == 4 and .resource_caps.submit_max_inflight == 20) and
  (.timings.l1_lease != "" and .timings.l1_node_dead != "" and .timings.l3_reconcile == "production-default") and
  (.deviations | any(.id == "dev.plain_fabric_identity" and .status == "DEVIATION")) and
  ((.rows | keys) == (required_rows | sort)) and
  ([.rows[] | select(.status == "MISSING")] | length == 0) and
  ([.rows[] | select(.started_at == null or .completed_at == null or (.assertions | length) == 0)] | length == 0) and
  (if $mutated == "" then
    ([.rows[] | select(.status == "FAIL")] | length == 0) and
    ([.rows[] | .assertions[] | select(. != true)] | length == 0)
   else
    (required_rows | index($mutated)) != null and
    ([.rows[] | select(.status == "FAIL")] | length == 1) and
    .rows[$mutated].status == "FAIL" and
    ([.rows[$mutated].assertions[] | select(. != true)] | length == 1)
   end) and
  ([required_rows[] | select(. != "linux.storage_provenance" and . != "linux.guest_authority" and . != $mutated) as $id | $root.rows[$id].status == "PASS"] | all) and
  (if $mutated == "linux.storage_provenance" then
    .rows["linux.storage_provenance"].status == "FAIL"
   else
    .rows["linux.storage_provenance"].status == "NOT-RUN" and
    .rows["linux.storage_provenance"].not_run_issue == 286 and
    (.rows["linux.storage_provenance"].not_run_reason | contains("restore publishes no receipt-derived")) and
    (.rows["linux.storage_provenance"].evidence.restore_session_revocation_evidence | contains("unavailable"))
   end) and
  (if $mutated == "linux.guest_authority" then
    .rows["linux.guest_authority"].status == "FAIL"
   else
    .rows["linux.guest_authority"].status == "NOT-RUN" and
    .rows["linux.guest_authority"].not_run_issue == 157 and
    (.rows["linux.guest_authority"].not_run_reason | contains("complete M3 OCI matrix")) and
    (.rows["linux.guest_authority"].evidence.blocked_assertion | contains("root Run"))
   end) and
  (.rows["linux.removal"].evidence.inventory_source == "helper VerifyNamespace route") and
  (.residue_inventories.post_removal_helper_namespace | type == "object")
' "$receipt" >/dev/null
