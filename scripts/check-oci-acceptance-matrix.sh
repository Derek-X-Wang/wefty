#!/bin/sh
set -eu

# Fail-closed gate for the M3 OCI acceptance matrix (spec section 9), assembled by
# scripts/assemble-oci-acceptance-matrix.sh. Rejects a failing row, a missing or
# unknown row, an untyped skip, an unearned PASS, a commit mismatch on either
# half, and any Mac row claimed without the attended owner-hardware artifact.
#
# A typed skip is an honest "not proven yet" and passes. A FAIL is a proof that
# ran and came back red, so it fails the gate.

if [ "$#" -ne 4 ]; then
  printf '%s\n' 'usage: check-oci-acceptance-matrix.sh MATRIX CANDIDATE_SHA EVIDENCE_SOURCE RUNNER_ENVIRONMENT' >&2
  exit 64
fi

matrix=$1
candidate_sha=$2
evidence_source=$3
runner_environment=$4

case "$candidate_sha" in
  *[!0-9a-f]*|'') exit 64 ;;
esac
test "${#candidate_sha}" -eq 40
case "$evidence_source" in published-artifact|pr-build) ;; *) exit 64 ;; esac
case "$runner_environment" in github-hosted|self-hosted|owner-hardware) ;; *) exit 64 ;; esac

test -f "$matrix"

# The frozen row inventory. Nine class rows under both platform prefixes, one
# capability row each, the Linux-only list and the Mac-only list.
required_rows='[
  "linux.oneshot.image_identity","linux.oneshot.delivery","linux.oneshot.engine_loss",
  "linux.service.publication","linux.service.restart","linux.service.stop_start",
  "linux.service.data","linux.service.crash_recovery","linux.service.removal",
  "linux.node.capability_claims",
  "linux.only.unprivileged_agent","linux.only.socket_activated_helper",
  "linux.only.cgroup_v2_limits","linux.only.no_raw_containerd",
  "mac.oneshot.image_identity","mac.oneshot.delivery","mac.oneshot.engine_loss",
  "mac.service.publication","mac.service.restart","mac.service.stop_start",
  "mac.service.data","mac.service.crash_recovery","mac.service.removal",
  "mac.node.capability_claims",
  "mac.only.stopped_vm_autostart","mac.only.helper_socket_authorization",
  "mac.only.raw_containerd_denied","mac.only.dynamic_forwarding_disabled",
  "mac.only.dial_attempt_port","mac.only.template_convergence",
  "mac.only.launch_topology","mac.only.headless_reboot"
]'

verdict=$(jq -r --argjson required "$required_rows" --arg candidate "$candidate_sha" \
  --arg source "$evidence_source" --arg environment "$runner_environment" '
  def names(predicate): [.rows // {} | .[] | select(predicate) | .id] | join(", ");

  (.rows // {}) as $rows
  | [$rows[] | select((.id // "") | startswith("mac."))] as $mac
  | [$mac[] | select(.status != "PASS")] as $mac_open
  | ([$rows[] | select(.status == "PASS")] | length) as $passed
  | ([$rows[] | select(.status == "FAIL")] | length) as $broken
  | ([$rows[] | select(.status == "NOT-RUN")] | length) as $skipped
  | (.mac_source // "") as $mac_source
  | ([
      (if .version != 1 then "unsupported matrix version \(.version)" else empty end),
      (if .candidate_sha != $candidate then "candidate_sha \(.candidate_sha) does not bind to \($candidate)" else empty end),
      (if .evidence_source != $source then "evidence_source \(.evidence_source) does not bind to \($source)" else empty end),
      (if .runner_environment != $environment then "runner_environment \(.runner_environment) does not bind to \($environment)" else empty end),
      (if .linux_evidence.commit != $candidate
        then "the Linux realtiming evidence is bound to \(.linux_evidence.commit // "nothing"), not \($candidate)" else empty end),

      ([$required[] | . as $id | select(($rows | has($id)) | not)] | if length > 0 then "required rows missing: \(join(", "))" else empty end),
      ([$required[] | . as $id | select($rows | has($id)) | select($rows[$id].id != $id)] | if length > 0 then "rows do not carry their own stable id: \(join(", "))" else empty end),
      (if ($rows | length) != ($required | length) then "the matrix carries \($rows | length) rows, want \($required | length)" else empty end),

      (names(.status | IN("PASS", "FAIL", "NOT-RUN") | not)
        | if length > 0 then "rows carry an unknown status: \(.)" else empty end),
      (names(.status == "FAIL")
        | if length > 0 then "rows failed: \(.)" else empty end),
      (names(.status == "FAIL" and ((.reason // "") | length) == 0)
        | if length > 0 then "rows failed without a reason: \(.)" else empty end),
      (names(.status == "NOT-RUN" and ((.not_run_issue // 0) <= 0 or ((.reason // "") | length) == 0))
        | if length > 0 then "untyped skips without an owning ticket and reason: \(.)" else empty end),
      (names(.status == "PASS" and (((.assertions | length) + (.attested | length)) == 0 or ([.assertions[] | select(. == false)] | length) > 0))
        | if length > 0 then "rows claim PASS without earning it: \(.)" else empty end),
      (names(.status == "PASS" and (.gaps | length) > 0)
        | if length > 0 then "rows claim PASS while declaring a gap: \(.)" else empty end),

      (if ($mac_source | IN("absent", "attended-owner-hardware") | not)
        then "mac_source \($mac_source) is not a recognised Mac evidence source" else empty end),
      (if .mac_evidence.source != $mac_source then "mac_source disagrees with mac_evidence.source" else empty end),

      (if $mac_source == "absent"
        then (names(((.id // "") | startswith("mac.")) and .status != "NOT-RUN")
          | if length > 0 then "Mac rows claim a verdict without the attended artifact: \(.)" else empty end)
        else empty end),
      (if $mac_source == "attended-owner-hardware" and $environment == "github-hosted"
        then "a GitHub-hosted runner claimed attended Mac evidence" else empty end),
      (if $mac_source == "attended-owner-hardware" and .mac_evidence.commit != $candidate
        then "the attended Mac artifact is bound to \(.mac_evidence.commit // "nothing"), not \($candidate)" else empty end),
      (if $mac_source == "attended-owner-hardware" and ((.mac_evidence.session_id // "") | length) == 0
        then "the attended Mac artifact carries no session id" else empty end),
      (if $mac_source == "attended-owner-hardware"
        then (names(((.id // "") | startswith("mac.")) and .source != "attended-owner-hardware")
          | if length > 0 then "Mac rows were not sourced from the attended artifact: \(.)" else empty end)
        else empty end),

      (if .complete != (($mac_open | length) == 0) then "complete disagrees with the Mac rows" else empty end),
      (if .status != (if $broken > 0 then "FAIL" elif $skipped > 0 then "NOT-RUN" else "PASS" end)
        then "the aggregate status \(.status) disagrees with the rows" else empty end),
      (if .status == "FAIL" then "the matrix aggregate is FAIL" else empty end)
    ]) as $violations
  | "violations\t\($violations | length)",
    ($violations[] | "violation\t\(.)"),
    "summary\t\($mac_open | length)\t\($mac_source)\t\($passed)\t\($broken)\t\($skipped)\t\($rows | length)\t\(.status)"
' "$matrix")

printf '%s\n' "$verdict" | while IFS="$(printf '\t')" read -r kind detail; do
  [ "$kind" = violation ] || continue
  printf 'oci acceptance matrix: %s\n' "$detail" >&2
done

violation_count=$(printf '%s\n' "$verdict" | awk -F'\t' '$1 == "violations" { print $2; exit }')
summary=$(printf '%s\n' "$verdict" | awk -F'\t' '$1 == "summary" { print; exit }')
mac_open=$(printf '%s' "$summary" | cut -f2)
mac_source=$(printf '%s' "$summary" | cut -f3)
passed=$(printf '%s' "$summary" | cut -f4)
broken=$(printf '%s' "$summary" | cut -f5)
skipped=$(printf '%s' "$summary" | cut -f6)
total=$(printf '%s' "$summary" | cut -f7)
aggregate=$(printf '%s' "$summary" | cut -f8)

if [ "${violation_count:-1}" -ne 0 ]; then
  exit 1
fi

if [ "$mac_open" -gt 0 ]; then
  printf 'matrix incomplete: %s Mac rows not run (mac_source=%s) — %s PASS / %s FAIL / %s NOT-RUN of %s rows, aggregate %s\n' \
    "$mac_open" "$mac_source" "$passed" "$broken" "$skipped" "$total" "$aggregate"
else
  printf 'matrix complete: %s PASS / %s FAIL / %s NOT-RUN of %s rows, aggregate %s\n' \
    "$passed" "$broken" "$skipped" "$total" "$aggregate"
fi
