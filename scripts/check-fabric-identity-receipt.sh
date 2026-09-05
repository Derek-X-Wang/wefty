#!/bin/sh
set -eu

if [ "$#" -ne 7 ]; then
  printf '%s\n' 'usage: check-fabric-identity-receipt.sh RECEIPT CANDIDATE_SHA trusted|pull_request MACHINE_RESULT PERSON_RESULT MACHINE_ARMED PERSON_ARMED' >&2
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
  def pass_row:
    .status == "PASS" and .reason == null and
    ([.assertions[] | select(. != true)] | length == 0) and
    (.assertions | length) > 0 and
    (.deviations | length) == 0;
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
    select((getpath($path) | test("\\.ts\\.net|^svc:"; "i")) and
      (($path[-1] | tostring | endswith("connect_host")) | not))] | length) == 0 and
  if $trust_domain == "pull_request" then
    $machine_result == "skipped" and $person_result == "skipped" and
    .status == "NOT-RUN" and
    ([.rows[] | select(not_run_row("pull_request_secretless") | not)] | length) == 0
  else
    $machine_armed and $machine_result == "success" and
    (.rows["fabric.machine_dns_acl"] | pass_row) and
    (.rows["fabric.machine_second_peer_reachability"] | pass_row) and
    (if $person_armed then
       $person_result == "success" and .status == "PASS" and
       (.rows["fabric.person_whoami"] | pass_row)
     else
       $person_result == "skipped" and .status == "NOT-RUN" and
       (.rows["fabric.person_whoami"] | not_run_row("secret_unarmed"))
     end)
  end
' "$receipt" >/dev/null
