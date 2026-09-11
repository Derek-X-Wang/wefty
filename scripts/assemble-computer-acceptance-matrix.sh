#!/bin/sh
set -eu

# Assemble the agent-computer acceptance matrix (agent-computer spec section 10)
# from the receipts the two lanes already produce. The Linux half is copied
# verbatim from the typed linux-computer-matrix.json the realtiming lane writes;
# the Mac half comes from the attended owner-hardware fragment, or is typed
# NOT-RUN when that fragment is absent.
# scripts/check-computer-acceptance-matrix.sh validates the result and is the
# fail-closed gate.

if [ "$#" -ne 6 ]; then
  printf '%s\n' 'usage: assemble-computer-acceptance-matrix.sh OUTPUT CANDIDATE_SHA EVIDENCE_SOURCE LINUX_EVIDENCE_DIR MAC_FRAGMENT|none RUNNER_ENVIRONMENT' >&2
  exit 64
fi

output=$1
candidate_sha=$2
evidence_source=$3
linux_directory=$4
mac_fragment=$5
runner_environment=$6

# The attended Mac lane is owner hardware. GitHub-hosted macOS cannot boot nested
# Lima vz (agent-computer spec section 10.1), so a hosted run always reports the
# Mac half absent and every Mac row is owned by the owner-hardware ticket.
mac_absent_issue=128
mac_absent_reason='no attended owner-hardware session ran; GitHub-hosted macOS cannot boot nested Lima vz'
# The dominant product blocker for a booted Computer on a Mac. It was #394, the
# Lima vz bridge-bind defect, until that was fixed; the attempts now die one step
# later on #408, where agent and helper disagree on the arm64 platform variant
# and every OCI payload fails image_platform_unsupported with no container
# created. Until it is fixed the rows that need a booted Computer stay unproven
# even with owner hardware in hand. Named in the gap, never used to launder the
# skip.
mac_blocker_issue=408
mac_blocker_reason='blocked by #408: agent and helper disagree on the arm64 image platform, so no Computer payload starts on a Mac'
mac_setup_reason='the owner-hardware setup in docs/acceptance/m3.5-mac-computer.md has not been performed for this candidate'

case "$candidate_sha" in
  *[!0-9a-f]*|'') exit 64 ;;
esac
test "${#candidate_sha}" -eq 40
case "$evidence_source" in published-artifact|pr-build) ;; *) exit 64 ;; esac
case "$runner_environment" in github-hosted|self-hosted|owner-hardware) ;; *) exit 64 ;; esac

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/wefty-computer-matrix.XXXXXX")
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
rows="$work_directory/rows.json"
: > "$rows"

linux_receipt="$linux_directory/linux-computer-matrix.json"
test -f "$linux_receipt"
linux_receipt_version=$(jq -r '.version // 0' "$linux_receipt")
linux_variant=$(jq -r '.image.variant // ""' "$linux_receipt")

provenance="$linux_directory/provenance-receipt.json"
linux_commit=""
linux_artifact_run_id=""
if [ -f "$provenance" ]; then
  linux_commit=$(jq -r '.commit // ""' "$provenance")
  linux_artifact_run_id=$(jq -r '.artifact_run_id // ""' "$provenance")
fi

# id|proof|spec_refs. The nine Linux rows are linuxComputerMatrixRows, unchanged.
linux_row_specification() {
  cat <<'SPECIFICATION'
linux.create_boot|Create and boot|10.2 Create and boot
linux.network_egress|Private network outbound|10.2 Create and boot
linux.screen_crossover_refused|Screen crossover refused|10.2 Remote take-over
linux.remote_takeover|Remote take-over|10.2 Remote take-over
linux.restart_survival|Restart survival|10.2 Restart survival
linux.reconfiguration|Reconfiguration|10.2 Reconfiguration
linux.storage_provenance|Storage provenance|10.2 Storage provenance
linux.guest_authority|Guest authority|10.2 Guest authority
linux.removal|Removal|10.2 Removal
SPECIFICATION
}

# id|proof|spec_refs|blocked_by. A blocked_by of 408 marks a row that cannot be
# proven on a Mac until the arm64 image-platform defect is fixed; an empty
# blocked_by marks a row whose only obstacle is that the owner-hardware setup has
# not run.
mac_row_specification() {
  cat <<'SPECIFICATION'
mac.create_boot|Create and boot through the M3 helper tunnel in one shared Lima VM|10.2 Create and boot|
mac.network_egress|Private network outbound, and neither the macOS LAN nor the host outside the sanctioned bridge|10.2 Create and boot|408
mac.screen_crossover_refused|Screen crossover between two Computers in the one VM refused|10.2 Remote take-over|408
mac.remote_takeover|A second physical tailnet device watches and controls; viewer input and unauthorized peer denied server-side|10.2 Remote take-over|408
mac.restart_survival|Runtime, agent and VM stop/start preserve the disk; screen withdraws on loss and returns only after sweep and policy readiness|10.2 Restart survival|408
mac.reconfiguration|Reimage, reset, grow, detachment and abort, plus loop/ext4/Lima guest residue and cap ceiling facts|10.2 Reconfiguration|408
mac.storage_provenance|Source-node copy contract with guest-native backup files and managed-root facts|10.2 Storage provenance|408
mac.guest_authority|Gateway-primary and forced DialHostBridge fallback, never LAN|10.2 Guest authority|408
mac.removal|Disk, profile, container, task, loop, mount, log, control and publication residue absent; attended inventory retained|10.2 Removal|408
mac.reference_image_narrowness|Computer reimage and the optional reference image remain narrow exceptions|11 item 4|
SPECIFICATION
}

linux_row_specification | while IFS='|' read -r id proof spec_refs; do
  [ -n "$id" ] || continue
  jq --arg id "$id" --arg proof "$proof" --arg spec_refs "$spec_refs" \
    '(.rows[$id] // {status:"MISSING"})
     | {id:$id, proof:$proof, spec_refs:$spec_refs, status:.status,
        assertions:(.assertions // {}), attested:{},
        evidence:(.evidence // {}), gaps:{},
        not_run_issue:(.not_run_issue // 0), reason:(.not_run_reason // ""),
        source:"realtiming-linux"}' "$linux_receipt" >> "$rows"
done

if [ "$mac_fragment" = none ]; then
  mac_row_specification | while IFS='|' read -r id proof spec_refs blocked_by; do
    [ -n "$id" ] || continue
    if [ -n "$blocked_by" ]; then
      row_reason="$mac_absent_reason; $mac_blocker_reason"
      row_gaps=$(jq -n --arg attended "$mac_absent_reason" --arg defect "$mac_blocker_reason" \
        '{attended_session:$attended, product_defect:$defect}')
    else
      row_reason="$mac_absent_reason; $mac_setup_reason"
      row_gaps=$(jq -n --arg attended "$mac_absent_reason" --arg setup "$mac_setup_reason" \
        '{attended_session:$attended, owner_hardware_setup:$setup}')
    fi
    jq -n --arg id "$id" --arg proof "$proof" --arg spec_refs "$spec_refs" \
      --argjson issue "$mac_absent_issue" --arg reason "$row_reason" --argjson gaps "$row_gaps" \
      '{id:$id, proof:$proof, spec_refs:$spec_refs, status:"NOT-RUN",
        assertions:{}, attested:{}, evidence:{}, gaps:$gaps,
        not_run_issue:$issue, reason:$reason, source:"attended-absent"}' >> "$rows"
  done
  mac_evidence=$(jq -n --argjson issue "$mac_absent_issue" --arg reason "$mac_absent_reason" \
    --argjson blocker "$mac_blocker_issue" \
    '{source:"absent", session_id:"", commit:"", artifact_sha256:"",
      attended_row_counts:{}, destination_asserted:false, plain_fabric_deviation:false,
      not_run_issue:$issue, blocker_issue:$blocker, reason:$reason}')
else
  test -f "$mac_fragment"
  jq -e '.rows | type == "object"' "$mac_fragment" > /dev/null
  mac_row_specification | while IFS='|' read -r id proof spec_refs blocked_by; do
    [ -n "$id" ] || continue
    jq --arg id "$id" --arg proof "$proof" --arg spec_refs "$spec_refs" \
      '(.rows[$id] // {status:"MISSING"})
       | {id:$id, proof:$proof, spec_refs:$spec_refs, status:.status,
          assertions:(.assertions // {}), attested:(.attested // {}),
          evidence:(.evidence // {}), gaps:(.gaps // {}),
          not_run_issue:(.not_run_issue // 0), reason:(.reason // ""),
          source:"attended-owner-hardware"}' "$mac_fragment" >> "$rows"
  done
  mac_evidence=$(jq --argjson blocker "$mac_blocker_issue" \
    '{source:"attended-owner-hardware", session_id:(.session_id // ""),
      commit:(.commit // ""), artifact_sha256:(.artifact_sha256 // ""),
      attended_row_counts:(.attended_row_counts // {}),
      destination_asserted:(.destination.asserted // false),
      plain_fabric_deviation:([(.deviations // [])[] | select(.id == "dev.plain_fabric_identity")] | length > 0),
      not_run_issue:0, blocker_issue:$blocker, reason:""}' "$mac_fragment")
fi

jq -s --arg candidate "$candidate_sha" --arg source "$evidence_source" \
  --arg environment "$runner_environment" --argjson mac "$mac_evidence" \
  --arg variant "$linux_variant" --arg linux_commit "$linux_commit" \
  --arg artifact_run_id "$linux_artifact_run_id" \
  --argjson receipt_version "$linux_receipt_version" \
  --arg stamped "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  '(reduce .[] as $row ({}; . + {($row.id): $row})) as $rows
   | ([$rows[] | select(.id | startswith("mac.")) | select(.status != "PASS")] | length) as $mac_open
   | ([$rows[] | select(.status == "MISSING" or .status == "FAIL")] | length) as $broken
   | ([$rows[] | select(.status == "NOT-RUN")] | length) as $skipped
   | {version: 1,
      status: (if $broken > 0 then "FAIL" elif $skipped > 0 then "NOT-RUN" else "PASS" end),
      complete: ($mac_open == 0),
      candidate_sha: $candidate,
      evidence_source: $source,
      runner_environment: $environment,
      mac_source: $mac.source,
      linux_evidence: {source: "realtiming", variant: $variant,
                       commit: $linux_commit, artifact_run_id: $artifact_run_id,
                       receipt_version: $receipt_version},
      mac_evidence: $mac,
      rows: $rows,
      started_at: $stamped,
      completed_at: $stamped}' "$rows" > "$output"

test -s "$output"
