#!/bin/sh
set -eu

if [ "$#" -ne 9 ]; then
  printf '%s\n' 'usage: assemble-fabric-identity-receipt.sh OUTPUT CANDIDATE_SHA trusted|pull_request|workflow_dispatch MACHINE_RESULT PERSON_RESULT MACHINE_ARMED PERSON_ARMED MACHINE_RECEIPT PERSON_RECEIPT' >&2
  exit 64
fi

output=$1
candidate_sha=$2
trust_domain=$3
machine_result=$4
person_result=$5
machine_armed=$6
person_armed=$7
machine_receipt=$8
person_receipt=$9

case "$candidate_sha" in
  *[!0-9a-f]*|'') exit 64 ;;
esac
test "${#candidate_sha}" -eq 40
case "$trust_domain" in trusted|pull_request|workflow_dispatch) ;; *) exit 64 ;; esac
case "$machine_result" in success|failure|cancelled|skipped) ;; *) exit 64 ;; esac
case "$person_result" in success|failure|cancelled|skipped) ;; *) exit 64 ;; esac
case "$machine_armed" in true|false) ;; *) exit 64 ;; esac
case "$person_armed" in true|false) ;; *) exit 64 ;; esac

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/wefty-fabric-receipt.XXXXXX")
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
rows="$work_directory/rows.json"

not_run_row() {
  reason=$1
  jq -n --arg reason "$reason" '{
    status:"NOT-RUN",
    reason:$reason,
    assertions:{},
    evidence:{},
    deviations:[{
      id:"dev.plain_fabric_identity",
      status:"DEVIATION",
      reason:"production Fabric identity evidence did not execute"
    }]
  }'
}

failed_row() {
  reason=$1
  jq -n --arg reason "$reason" '{
    status:"FAIL",
    reason:$reason,
    assertions:{},
    evidence:{},
    deviations:[{
      id:"dev.plain_fabric_identity",
      status:"DEVIATION",
      reason:"production Fabric identity evidence did not complete successfully"
    }]
  }'
}

if [ "$trust_domain" != trusted ]; then
  test "$machine_result" = skipped
  test "$person_result" = skipped
  if [ "$trust_domain" = pull_request ]; then
    skip_reason=pull_request_secretless
  else
    skip_reason=manual_dispatch_secretless
  fi
  machine_dns_acl=$(not_run_row "$skip_reason")
  machine_second_peer=$(not_run_row "$skip_reason")
  person_whoami=$(not_run_row "$skip_reason")
else
  if [ "$machine_result" = success ]; then
    test "$machine_armed" = true
    test -s "$machine_receipt"
    jq -e --arg candidate "$candidate_sha" '
      .version == 1 and .candidate_sha == $candidate and
      (.rows | keys | sort) == ["fabric.machine_dns_acl", "fabric.machine_second_peer_reachability"] and
      ([.rows[] | select(.status != "PASS")] | length == 0)
    ' "$machine_receipt" >/dev/null
    machine_dns_acl=$(jq -c '.rows["fabric.machine_dns_acl"]' "$machine_receipt")
    machine_second_peer=$(jq -c '.rows["fabric.machine_second_peer_reachability"]' "$machine_receipt")
  elif [ "$machine_result" = skipped ] && [ "$machine_armed" = false ]; then
    machine_dns_acl=$(not_run_row secret_unarmed)
    machine_second_peer=$(not_run_row secret_unarmed)
  else
    machine_dns_acl=$(failed_row "machine_job_$machine_result")
    machine_second_peer=$(failed_row "machine_job_$machine_result")
  fi

  if [ "$person_result" = success ]; then
    test "$person_armed" = true
    test -s "$person_receipt"
    jq -e --arg candidate "$candidate_sha" '
      .version == 1 and .candidate_sha == $candidate and
      (.rows | keys) == ["fabric.person_whoami"] and
      .rows["fabric.person_whoami"].status == "PASS"
    ' "$person_receipt" >/dev/null
    person_whoami=$(jq -c '.rows["fabric.person_whoami"]' "$person_receipt")
  elif [ "$person_result" = skipped ] && [ "$person_armed" = false ]; then
    person_whoami=$(not_run_row secret_unarmed)
  else
    person_whoami=$(failed_row "person_job_$person_result")
  fi
fi

jq -n \
  --arg candidate "$candidate_sha" \
  --arg trust_domain "$trust_domain" \
  --arg machine_result "$machine_result" \
  --arg person_result "$person_result" \
  --argjson machine_dns_acl "$machine_dns_acl" \
  --argjson machine_second_peer "$machine_second_peer" \
  --argjson person_whoami "$person_whoami" \
  '{
    version:1,
    candidate_sha:$candidate,
    trust_domain:$trust_domain,
    job_results:{machine:$machine_result,person:$person_result},
    rows:{
      "fabric.machine_dns_acl":$machine_dns_acl,
      "fabric.machine_second_peer_reachability":$machine_second_peer,
      "fabric.person_whoami":$person_whoami
    }
  } |
  .status = (if ([.rows[] | select(.status == "FAIL")] | length) > 0 then "FAIL"
             elif ([.rows[] | select(.status == "NOT-RUN")] | length) > 0 then "NOT-RUN"
             else "PASS" end)' > "$rows"

mv "$rows" "$output"
