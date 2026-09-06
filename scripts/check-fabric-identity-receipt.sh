#!/bin/sh
set -eu

if [ "$#" -ne 7 ]; then
  printf '%s\n' 'usage: check-fabric-identity-receipt.sh RECEIPT CANDIDATE_SHA trusted|pull_request|workflow_dispatch MACHINE_RESULT PERSON_RESULT MACHINE_ARMED PERSON_ARMED' >&2
  exit 64
fi

receipt=$1
candidate_sha=$2
trust_domain=$3
machine_result=$4
person_result=$5
machine_armed=$6
person_armed=$7
test -s "$receipt"
case "$trust_domain" in trusted|pull_request|workflow_dispatch) ;; *) exit 64 ;; esac

jq -e \
  --arg candidate "$candidate_sha" \
  --arg trust_domain "$trust_domain" \
  --arg machine_result "$machine_result" \
  --arg person_result "$person_result" \
  --argjson machine_armed "$machine_armed" \
  --argjson person_armed "$person_armed" '
  def required_rows: [
    "fabric.machine_dns_acl",
    "fabric.machine_second_peer_reachability",
    "fabric.person_whoami"
  ];
  def pass_row($assertion_keys; $evidence_keys):
    .status == "PASS" and .reason == null and
    (.assertions | type) == "object" and
    (.assertions | keys) == $assertion_keys and
    ([.assertions[] | select(. != true)] | length == 0) and
    (.evidence | type) == "object" and
    (.evidence | keys) == $evidence_keys and
    .deviations == [];
  def not_run_row($reason):
    .status == "NOT-RUN" and .reason == $reason and
    (.assertions | length) == 0 and (.evidence | length) == 0 and
    (.deviations | any(.id == "dev.plain_fabric_identity" and .status == "DEVIATION"));
  . as $root |
  .version == 1 and
  .candidate_sha == $candidate and
  (.candidate_sha | test("^[0-9a-f]{40}$")) and
  .trust_domain == $trust_domain and
  .job_results == {machine:$machine_result,person:$person_result} and
  (.rows | keys) == (required_rows | sort) and
  ([paths(scalars) as $path |
    select(($path[-1] | tostring | test("tailscale|magicdns|svc"; "i")))] | length) == 0 and
  ([paths(strings) as $path |
    select(getpath($path) | test("\\.ts\\.net|svc:|magicdns|tailscale"; "i"))] | length) == 0 and
  ([paths(scalars) as $path |
    select($path[-1] | tostring | endswith("connect_host")) |
    getpath($path) as $value |
    select(($value | type) != "string" or
      (($value | test("^(control-plane|run-ledger|node-[0-9a-f]{16})$")) | not))] | length) == 0 and
  if $trust_domain == "pull_request" then
    $machine_result == "skipped" and $person_result == "skipped" and
    .status == "NOT-RUN" and
    ([.rows[] | select(not_run_row("pull_request_secretless") | not)] | length) == 0
  elif $trust_domain == "workflow_dispatch" then
    $machine_result == "skipped" and $person_result == "skipped" and
    .status == "NOT-RUN" and
    ([.rows[] | select(not_run_row("manual_dispatch_secretless") | not)] | length) == 0
  else
    (if $machine_armed then
       $machine_result == "success" and
       (.rows["fabric.machine_dns_acl"] |
         pass_row(
           ["dns_resolved", "machine_identity_authenticated", "shared_tagged_key_dial_succeeded"];
           ["listener_connect_host", "peer_connect_host"]
         )) and
       (.rows["fabric.machine_second_peer_reachability"] |
         pass_row(
           ["echo_round_trip", "peer_address_distinct"];
           ["listener_connect_host", "peer_connect_host"]
         ))
     else
       $machine_result == "skipped" and
       (.rows["fabric.machine_dns_acl"] | not_run_row("secret_unarmed")) and
       (.rows["fabric.machine_second_peer_reachability"] | not_run_row("secret_unarmed"))
     end) and
    (if $person_armed then
       $person_result == "success" and .status == "PASS" and
       (.rows["fabric.person_whoami"] |
         pass_row(
           ["person_identity_complete", "whoami_authenticated"];
           ["listener_connect_host", "peer_connect_host"]
         ))
     else
       $person_result == "skipped" and .status == "NOT-RUN" and
       (.rows["fabric.person_whoami"] | not_run_row("secret_unarmed"))
     end)
  end
' "$receipt" >/dev/null
