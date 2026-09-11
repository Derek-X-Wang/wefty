#!/bin/sh
set -eu

# Assemble the M3 OCI acceptance matrix (spec section 9) from receipts the lanes
# already produce. Linux rows come from the realtiming evidence directory; Mac
# rows come from the attended owner-hardware fragment, or are typed NOT-RUN when
# that fragment is absent. scripts/check-oci-acceptance-matrix.sh validates the
# result and is the fail-closed gate.

if [ "$#" -ne 6 ]; then
  printf '%s\n' 'usage: assemble-oci-acceptance-matrix.sh OUTPUT CANDIDATE_SHA EVIDENCE_SOURCE LINUX_EVIDENCE_DIR MAC_FRAGMENT|none RUNNER_ENVIRONMENT' >&2
  exit 64
fi

output=$1
candidate_sha=$2
evidence_source=$3
linux_directory=$4
mac_fragment=$5
runner_environment=$6

# A Linux cell this repository cannot prove live yet. #402 owns the missing
# evidence and names the row.
gap_issue=402
# A lane that recorded its own NOT-RUN in its receipt — a pull-request build with
# no published image, say. That is a lane condition, not a missing capability, so
# it stays with the matrix ticket rather than the evidence-gap ticket.
lane_skip_issue=157
# The attended Mac lane is owner hardware. GitHub-hosted macOS cannot boot nested
# Lima vz (spec section 9.1), so a hosted run always reports the Mac half absent.
mac_absent_issue=128
mac_absent_reason='GitHub-hosted macOS cannot boot nested Lima vz, so the Mac half needs the attended owner-hardware lane'

case "$candidate_sha" in
  *[!0-9a-f]*|'') exit 64 ;;
esac
test "${#candidate_sha}" -eq 40
case "$evidence_source" in published-artifact|pr-build) ;; *) exit 64 ;; esac
case "$runner_environment" in github-hosted|self-hosted|owner-hardware) ;; *) exit 64 ;; esac

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/wefty-oci-matrix.XXXXXX")
trap 'rm -rf "$work_directory"' EXIT HUP INT TERM
facts="$work_directory/facts.txt"
rows="$work_directory/rows.json"

: > "$facts"
for receipt in native-linux-oci.txt oci-service-publication-linux.txt \
  oci-service-l1-agent-linux.txt helper-restart-timeline.txt lost-attempt-sweep.txt; do
  if [ -f "$linux_directory/$receipt" ]; then
    cat "$linux_directory/$receipt" >> "$facts"
  fi
done

fact() {
  awk -v prefix="$1=" 'index($0, prefix) == 1 { print substr($0, length(prefix) + 1); exit }' "$facts"
}

has_fact() {
  awk -v prefix="$1=" 'index($0, prefix) == 1 { found = 1; exit } END { exit !found }' "$facts"
}

# id|proof|spec_refs|required facts|declared gaps
#   required fact: NAME (the value must be exactly "true"), NAME=VALUE (exact
#   match), or the sentinel agent_uid_nonzero. "-" means the row has no live fact.
#   declared gap: NAME:REASON, several joined by ";". Reasons carry no ";".
row_specification() {
  cat <<'SPECIFICATION'
linux.oneshot.image_identity|Public digest pull and offline tar import with identical top-level and platform digests, tag movement leaves every retry on the original job digest, cache repull after wipe|table Linux/one-shot + bullets 2,3,4|pull_from_empty pull_import_digest_equal registry_disabled_pull_rejected registry_disabled_import import_run node_load_image archive_platform_filtered public_acceptance_image tag_refloat_resolved_once binding_repull_reconciliation|cache_intact_after_reboot:no Linux reboot harness exists, and the only reboot test writes a permanent systemd_reboot_harness NOT-RUN receipt;rerun_under_tag_movement:frozen rerun identity is proven, but never with the tag floated between run and rerun
linux.oneshot.delivery|Handoff write, one authenticated bridge request, split stdout and stderr markers, exit 0|table Linux/one-shot|oneshot_handoff_marker_bytes oneshot_bridge_once oneshot_split_streams oneshot_digest_evidence ordinary_l3_oci_submission ordinary_l3_frozen_rerun live_log_delivery stdout_log stderr_log|
linux.oneshot.engine_loss|Pre-start engine loss requeues on the pinned digest, mid-run loss is terminal|table Linux/one-shot + bullet 5|prestart_requeue_pinned wait_before_start shim_loss=runtime_failure containerd_stop=runtime_failure|
linux.service.publication|Publish health and echo through Fabric on a helper-allocated loopback port|table Linux/service|service_echo_health service_echo_body health echo helper_tunnel portless_started port_collision_avoided startup_timeout|
linux.service.restart|Payload restart with cooperative TERM, KILL escalation, and paired log seals|table Linux/service|fresh_restart fresh_restart_authority term_cooperative_stop term_grace_stop term_kill_escalation term_kill_log_seal_pairing term_kill_stdout_log term_kill_stderr_log|
linux.service.stop_start|Stop and start reacquires capacity, withdraws and republishes on the retained binding digest|table Linux/service|stop_start slot_saturation retained_binding_digest withdrawal republication|
linux.service.data|Persistent service data across restart and stop/start, writable rootfs discarded on every restart|table Linux/service|service_data_root_user service_data_numeric_user service_data_named_user service_data_restart_persistent service_data_stop_start_persistent service_rootfs_discarded service_data_same_digest_replacement_fresh|
linux.service.crash_recovery|Agent, helper and containerd crash, helper loss, stale residue sweep with a reused boot ID|table Linux/service + bullets 6,7,8|service_helper_loss_injected service_helper_loss_observed service_fresh_attempt_readmission service_barrier_prefaced_during_startup service_lost_log_typed control_loss_reaped socket_and_service_active_after_recovery removal_prior_boot_oci_sweep|agent_sigkill_kind_oci:every agent SIGKILL arm submits kind=process, and the OCI lane only tears the agent down in-process;heartbeat_blackhole_live:heartbeat blackhole is proven only against the fake engine harness
linux.service.removal|Full removal and residue proof from every state, crash injection at every create and delete phase, bind sources untouched and image still cached|table Linux/service + bullets 9,10,11|removal_manifest_complete removal_pending removal_every_attempt removal_service_data_volume removal_service_data_owner_record removal_post_delete_attestation removal_delete_attest_crash_injected removal_completed service_residue_verified_absent service_retained_binding_verified namespace_absent|removal_from_stopped_kind_oci:the stopped-removal arm submits kind=process;removal_from_offline_kind_oci:the offline and force-forget arms submit kind=process;bind_sources_untouched_kind_oci:no live OCI test mounts an operator bind source, and operator_bind_source_untouched is Computer-only;crash_injection_create_phases:create-boundary crash injection exists only against the fake engine;crash_injection_after_quiescence:runtimeRemovalCheckpointAfterQuiescence is never injected in the live lane;delete_attest_restart:removal_delete_attest_restart is NOT-RUN_hosted_lane because a real agent process restart needs owner hardware
linux.node.capability_claims|Capable and incapable claim pairs for every required capability|bullet 1|-|capable_incapable_claim_pairs:claim pairs exist only in untagged fake-engine tests, no receipt key carries them, and the doctor snapshot in the lane is hand-built
linux.only.unprivileged_agent|The agent runs unprivileged|Linux-only list|agent_uid_nonzero|
linux.only.socket_activated_helper|A root socket-activated helper owns every containerd call|Linux-only list|helper_uid=0 helper_socket_root_owned socket_and_service_active_after_recovery|cold_socket_activation:nothing proves a cold inactive unit was started by the first socket connect
linux.only.cgroup_v2_limits|Opt-in cgroup-v2 memory and CPU limits for an ordinary OCI job|Linux-only list|oom_kill plain_137_exit|memory_max_readback:the memory limit is proven only by an OOM kill, never by reading memory.max back;cpu_millicores_enforcement:CPUMillicores appears only in runtime-spec golden fixtures and no capped container is run
linux.only.no_raw_containerd|Zero raw containerd access from the agent|Linux-only list|raw_socket_denied|
SPECIFICATION
}

# id|proof|spec_refs
mac_row_specification() {
  cat <<'SPECIFICATION'
mac.oneshot.image_identity|Same image identity contract through helper-owned image operations|table Mac/one-shot + bullets 2,3,4
mac.oneshot.delivery|Same delivery contract through the guest handoff mount, helper logs and the discovered gateway|table Mac/one-shot
mac.oneshot.engine_loss|Pre-start and mid-run loss including VM-loss classification|table Mac/one-shot + bullet 5
mac.service.publication|Publish health and echo through the fenced helper tunnel|table Mac/service
mac.service.restart|Payload restart through the fenced helper tunnel|table Mac/service
mac.service.stop_start|Stop and start reacquires capacity and republishes after the boot barrier|table Mac/service
mac.service.data|Guest-native service data persists and the writable rootfs is discarded|table Mac/service
mac.service.crash_recovery|VM, helper and hostagent loss withdraws publication, reaps or sweeps, then resumes after the boot barrier|table Mac/service + bullets 6,7,8
mac.service.removal|Full guest-native removal and residue proof|table Mac/service + bullets 9,10,11
mac.node.capability_claims|Capable and incapable claim pairs for every required capability|bullet 1
mac.only.stopped_vm_autostart|Stopped-VM auto-start only when OCI intent is enabled|Mac-only list
mac.only.helper_socket_authorization|Helper socket authorization|Mac-only list
mac.only.raw_containerd_denied|Raw containerd denial|Mac-only list
mac.only.dynamic_forwarding_disabled|Dynamic forwarding disabled|Mac-only list
mac.only.dial_attempt_port|DialAttemptPort reachability|Mac-only list
mac.only.template_convergence|Template convergence classes|Mac-only list
mac.only.launch_topology|The operator-user agent LaunchDaemon is the sole Lima supervisor|Mac-only list
mac.only.headless_reboot|Headless reboot evidence from ticket 128|Mac-only list
SPECIFICATION
}

pairs_to_object() {
  # stdin: NAME<TAB>VALUE lines. stdout: a JSON object of the given value type.
  jq -R -n --arg kind "$1" '[inputs | select(length > 0) | split("\t")
    | {key: .[0], value: (if $kind == "boolean" then (.[1] == "true") else .[1] end)}] | from_entries'
}

: > "$rows"

row_specification | while IFS='|' read -r id proof spec_refs required gaps; do
  [ -n "$id" ] || continue
  status=PASS
  not_run_reason=
  skip_source=lane
  assertion_pairs="$work_directory/assertions.tsv"
  evidence_pairs="$work_directory/evidence.tsv"
  : > "$assertion_pairs"
  : > "$evidence_pairs"

  for requirement in $required; do
    [ "$requirement" != '-' ] || continue
    passed=true
    case "$requirement" in
      agent_uid_nonzero)
        name=agent_uid
        if ! has_fact "$name"; then
          status=MISSING
          not_run_reason="missing fact $name"
          continue
        fi
        value=$(fact "$name")
        printf '%s\t%s\n' "$name" "$value" >> "$evidence_pairs"
        case "$value" in ''|*[!0-9]*|0) passed=false ;; esac
        printf '%s\t%s\n' agent_uid_nonzero "$passed" >> "$assertion_pairs"
        [ "$passed" = true ] || status=FAIL
        continue
        ;;
      *=*)
        name=${requirement%%=*}
        expected=${requirement#*=}
        ;;
      *)
        name=$requirement
        expected=true
        ;;
    esac

    if ! has_fact "$name"; then
      status=MISSING
      not_run_reason="missing fact $name"
      continue
    fi
    value=$(fact "$name")
    printf '%s\t%s\n' "$name" "$value" >> "$evidence_pairs"
    case "$value" in
      NOT-RUN|NOT-RUN_*)
        reason=$(fact "${name}_reason")
        [ -n "$reason" ] || reason="the lane recorded $value"
        if [ "$status" = PASS ]; then
          status='NOT-RUN'
          skip_source=lane
          not_run_reason="$name: $reason"
        fi
        ;;
      "$expected")
        printf '%s\t%s\n' "$name" true >> "$assertion_pairs"
        ;;
      *)
        printf '%s\t%s\n' "$name" false >> "$assertion_pairs"
        status=FAIL
        ;;
    esac
  done

  gap_pairs="$work_directory/gaps.tsv"
  : > "$gap_pairs"
  if [ -n "$gaps" ]; then
    printf '%s\n' "$gaps" | tr ';' '\n' | while IFS= read -r gap; do
      [ -n "$gap" ] || continue
      printf '%s\t%s\n' "${gap%%:*}" "${gap#*:}" >> "$gap_pairs"
    done
    if [ "$status" = PASS ]; then
      first_gap=$(printf '%s' "$gaps" | cut -d';' -f1)
      status='NOT-RUN'
      skip_source=gap
      not_run_reason="${first_gap%%:*}: ${first_gap#*:}"
    fi
  fi

  not_run_issue=0
  if [ "$status" = 'NOT-RUN' ]; then
    if [ "$skip_source" = gap ]; then
      not_run_issue=$gap_issue
    else
      not_run_issue=$lane_skip_issue
    fi
  fi

  jq -n --arg id "$id" --arg proof "$proof" --arg spec_refs "$spec_refs" \
    --arg status "$status" --arg reason "$not_run_reason" --argjson issue "$not_run_issue" \
    --argjson assertions "$(pairs_to_object boolean < "$assertion_pairs")" \
    --argjson evidence "$(pairs_to_object string < "$evidence_pairs")" \
    --argjson gaps "$(pairs_to_object string < "$gap_pairs")" \
    '{id:$id, proof:$proof, spec_refs:$spec_refs, status:$status,
      assertions:$assertions, evidence:$evidence, gaps:$gaps,
      not_run_issue:$issue, not_run_reason:$reason, source:"realtiming-linux"}' >> "$rows"
done

if [ "$mac_fragment" = none ]; then
  mac_row_specification | while IFS='|' read -r id proof spec_refs; do
    [ -n "$id" ] || continue
    jq -n --arg id "$id" --arg proof "$proof" --arg spec_refs "$spec_refs" \
      --argjson issue "$mac_absent_issue" --arg reason "$mac_absent_reason" \
      '{id:$id, proof:$proof, spec_refs:$spec_refs, status:"NOT-RUN",
        assertions:{}, evidence:{}, gaps:{},
        not_run_issue:$issue, not_run_reason:$reason, source:"attended-absent"}' >> "$rows"
  done
  mac_evidence=$(jq -n --argjson issue "$mac_absent_issue" --arg reason "$mac_absent_reason" \
    '{source:"absent", session_id:"", commit:"", artifact_sha256:"",
      attended_row_counts:{}, not_run_issue:$issue, not_run_reason:$reason}')
else
  test -f "$mac_fragment"
  jq -e '.rows | type == "object"' "$mac_fragment" > /dev/null
  mac_row_specification | while IFS='|' read -r id proof spec_refs; do
    [ -n "$id" ] || continue
    jq --arg id "$id" --arg proof "$proof" --arg spec_refs "$spec_refs" \
      '(.rows[$id] // {status:"MISSING"})
       | {id:$id, proof:$proof, spec_refs:$spec_refs, status:.status,
          assertions:(.assertions // {}), evidence:(.evidence // {}), gaps:(.gaps // {}),
          not_run_issue:(.not_run_issue // 0), not_run_reason:(.not_run_reason // ""),
          source:"attended-owner-hardware"}' "$mac_fragment" >> "$rows"
  done
  mac_evidence=$(jq '{source:"attended-owner-hardware", session_id:(.session_id // ""),
    commit:(.commit // ""), artifact_sha256:(.artifact_sha256 // ""),
    attended_row_counts:(.attended_row_counts // {}), not_run_issue:0, not_run_reason:""}' "$mac_fragment")
fi

jq -s --arg candidate "$candidate_sha" --arg source "$evidence_source" \
  --arg environment "$runner_environment" --argjson mac "$mac_evidence" \
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
      mac_open_rows: $mac_open,
      linux_evidence: {source: "realtiming"},
      mac_evidence: $mac,
      rows: $rows,
      started_at: $stamped,
      completed_at: $stamped}' "$rows" > "$output"

test -s "$output"
