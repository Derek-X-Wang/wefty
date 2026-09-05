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
    "linux.network_egress",
    "linux.screen_crossover_refused",
    "linux.remote_takeover",
    "linux.restart_survival",
    "linux.reconfiguration",
    "linux.storage_provenance",
    "linux.guest_authority",
    "linux.removal"
  ];
  . as $root |
  .version == 5 and
  .platform == "linux/amd64" and
  .candidate_sha == $candidate and
  .image.variant == $image and
  (.candidate_sha | test("^[0-9a-f]{40}$")) and
  (if $mutated == "" then .status == "NOT-RUN" and .not_run_issue == 157 else .status == "FAIL" end) and
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
  ([required_rows[] | select(. != "linux.guest_authority" and . != $mutated) as $id | $root.rows[$id].status == "PASS"] | all) and
  (if $mutated == "linux.network_egress" then
    .rows["linux.network_egress"].status == "FAIL"
   else
    .rows["linux.network_egress"].status == "PASS" and
    .rows["linux.network_egress"].assertions.private_veth_address_present and
    .rows["linux.network_egress"].assertions.mounted_resolver_recorded and
    .rows["linux.network_egress"].assertions.loopback_proxy_listening and
    .rows["linux.network_egress"].assertions.proxy_upstream_reachable and
    .rows["linux.network_egress"].assertions.default_route_present and
    .rows["linux.network_egress"].assertions.public_ipv4_connected and
    .rows["linux.network_egress"].assertions.resolver_reachable and
    .rows["linux.network_egress"].assertions.helper_http_through_veth and
    .rows["linux.network_egress"].assertions.node_listener_ipv4_refused and
    .rows["linux.network_egress"].assertions.node_listener_ipv6_refused and
    (.rows["linux.network_egress"].evidence.computer_id | length > 0) and
    (.rows["linux.network_egress"].evidence.attempt_id | length > 0) and
    (.rows["linux.network_egress"].evidence.veth_address | test("^198\\.1[89]\\.[0-9]+\\.[0-9]+$")) and
    (.rows["linux.network_egress"].evidence.veth_gateway | test("^198\\.1[89]\\.[0-9]+\\.[0-9]+$")) and
    .rows["linux.network_egress"].evidence.veth_address != .rows["linux.network_egress"].evidence.veth_gateway and
    (.rows["linux.network_egress"].evidence.resolver_snapshot | contains("nameserver")) and
    (.rows["linux.network_egress"].evidence.resolver_address | test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")) and
    (if (.rows["linux.network_egress"].evidence.resolver_address | startswith("127.")) then
      .rows["linux.network_egress"].evidence.proxy_udp_listening == "true" and
      .rows["linux.network_egress"].evidence.proxy_tcp_listening == "true" and
      (.rows["linux.network_egress"].evidence.proxy_upstream_address | test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")) and
      (.rows["linux.network_egress"].evidence.proxy_upstream_source == "systemd_uplink" or .rows["linux.network_egress"].evidence.proxy_upstream_source == "node_stub") and
      .rows["linux.network_egress"].evidence.proxy_upstream_reachable == "true"
     else true end) and
    .rows["linux.network_egress"].evidence.default_route_interface == "eth0" and
    .rows["linux.network_egress"].evidence.default_route_gateway == .rows["linux.network_egress"].evidence.veth_gateway and
    .rows["linux.network_egress"].evidence.public_ipv4_address == "1.1.1.1:443" and
    .rows["linux.network_egress"].evidence.public_ipv4_outcome == "connected" and
    .rows["linux.network_egress"].evidence.dns_outcome == "resolved" and
    .rows["linux.network_egress"].evidence.resolved_name == "example.com" and
    (.rows["linux.network_egress"].evidence.resolved_address | test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$")) and
    .rows["linux.network_egress"].evidence.helper_http_status == "200" and
    .rows["linux.network_egress"].evidence.helper_http_body == "wefty-computer-egress-v1" and
    (.rows["linux.network_egress"].evidence.node_listener_ipv4_address | test("^198\\.1[89]\\.[0-9]+\\.[0-9]+:[0-9]+$")) and
    .rows["linux.network_egress"].evidence.node_listener_ipv4_address == (.rows["linux.network_egress"].evidence.veth_gateway + ":" + (.rows["linux.network_egress"].evidence.node_listener_ipv4_address | split(":")[-1])) and
    .rows["linux.network_egress"].evidence.node_listener_ipv4_outcome == "refused" and
    .rows["linux.network_egress"].evidence.node_listener_ipv4_errno == "ECONNREFUSED" and
    (.rows["linux.network_egress"].evidence.node_listener_ipv6_address | test("^\\[fe80:.*%eth0\\]:[0-9]+$")) and
    .rows["linux.network_egress"].evidence.node_listener_ipv6_outcome == "refused" and
    (["EADDRNOTAVAIL", "ENETUNREACH", "EHOSTUNREACH", "ECONNREFUSED"] | index($root.rows["linux.network_egress"].evidence.node_listener_ipv6_errno)) != null
   end) and
  (if $mutated == "linux.screen_crossover_refused" then
    .rows["linux.screen_crossover_refused"].status == "FAIL"
   else
    .rows["linux.screen_crossover_refused"].status == "PASS" and
    .rows["linux.screen_crossover_refused"].assertions.crossover_refused and
    .rows["linux.screen_crossover_refused"].assertions.target_alive_at_refusal_edge and
    (.rows["linux.screen_crossover_refused"].evidence.source_computer_id | length > 0) and
    (.rows["linux.screen_crossover_refused"].evidence.source_attempt_id | length > 0) and
    (.rows["linux.screen_crossover_refused"].evidence.target_computer_id | length > 0) and
    (.rows["linux.screen_crossover_refused"].evidence.target_attempt_id | length > 0) and
    .rows["linux.screen_crossover_refused"].evidence.source_computer_id != .rows["linux.screen_crossover_refused"].evidence.target_computer_id and
    .rows["linux.screen_crossover_refused"].evidence.source_attempt_id != .rows["linux.screen_crossover_refused"].evidence.target_attempt_id and
    .rows["linux.screen_crossover_refused"].evidence.source_computer_id == .rows["linux.network_egress"].evidence.computer_id and
    .rows["linux.screen_crossover_refused"].evidence.source_attempt_id == .rows["linux.network_egress"].evidence.attempt_id and
    (.rows["linux.screen_crossover_refused"].evidence.target_egress_address | test("^198\\.1[89]\\.[0-9]+\\.[0-9]+$")) and
    (.rows["linux.screen_crossover_refused"].evidence.target_veth_gateway | test("^198\\.1[89]\\.[0-9]+\\.[0-9]+$")) and
    .rows["linux.screen_crossover_refused"].evidence.target_egress_address != .rows["linux.screen_crossover_refused"].evidence.target_veth_gateway and
    (.rows["linux.screen_crossover_refused"].evidence.target_view_port | test("^[0-9]+$")) and
    (.rows["linux.screen_crossover_refused"].evidence.target_control_port | test("^[0-9]+$")) and
    (.rows["linux.screen_crossover_refused"].evidence.target_egress_port | test("^[0-9]+$")) and
    .rows["linux.screen_crossover_refused"].evidence.view_read_address == (.rows["linux.screen_crossover_refused"].evidence.target_egress_address + ":" + .rows["linux.screen_crossover_refused"].evidence.target_view_port) and
    .rows["linux.screen_crossover_refused"].evidence.control_inject_address == (.rows["linux.screen_crossover_refused"].evidence.target_egress_address + ":" + .rows["linux.screen_crossover_refused"].evidence.target_control_port) and
    .rows["linux.screen_crossover_refused"].evidence.egress_address_target == (.rows["linux.screen_crossover_refused"].evidence.target_egress_address + ":" + .rows["linux.screen_crossover_refused"].evidence.target_egress_port) and
    .rows["linux.screen_crossover_refused"].evidence.target_liveness_view == "read_succeeded" and
    .rows["linux.screen_crossover_refused"].evidence.target_liveness_control == "inject_succeeded" and
    .rows["linux.screen_crossover_refused"].evidence.target_liveness_egress == "connected" and
    .rows["linux.screen_crossover_refused"].evidence.view_read_outcome == "refused" and
    .rows["linux.screen_crossover_refused"].evidence.control_inject_outcome == "refused" and
    .rows["linux.screen_crossover_refused"].evidence.view_read_errno == "ECONNREFUSED" and
    .rows["linux.screen_crossover_refused"].evidence.control_inject_errno == "ECONNREFUSED" and
    .rows["linux.screen_crossover_refused"].evidence.egress_address_outcome == "refused" and
    .rows["linux.screen_crossover_refused"].evidence.egress_address_errno == "ECONNREFUSED" and
    (.rows["linux.screen_crossover_refused"].evidence.node_listener_ipv6_address | test("^\\[fe80:.*%eth0\\]:[0-9]+$")) and
    .rows["linux.screen_crossover_refused"].evidence.node_listener_ipv6_outcome == "refused" and
    (["EADDRNOTAVAIL", "ENETUNREACH", "EHOSTUNREACH", "ECONNREFUSED"] | index($root.rows["linux.screen_crossover_refused"].evidence.node_listener_ipv6_errno)) != null and
    (if .image.variant == "xfce" then
      .rows["linux.screen_crossover_refused"].evidence.abstract_socket_visible == "false" and
      .rows["linux.screen_crossover_refused"].evidence.abstract_socket_outcome == "refused" and
      (["ENOENT", "ECONNREFUSED"] | index($root.rows["linux.screen_crossover_refused"].evidence.abstract_socket_errno)) != null and
      .rows["linux.screen_crossover_refused"].evidence.derived_display_outcome == "transport_refused" and
      .rows["linux.screen_crossover_refused"].evidence.derived_display_class == "x_transport" and
      .rows["linux.screen_crossover_refused"].evidence.target_liveness_x == "read_succeeded"
     else
      .rows["linux.screen_crossover_refused"].evidence.abstract_socket_outcome == "not_applicable" and
      .rows["linux.screen_crossover_refused"].evidence.derived_display_outcome == "not_applicable"
     end)
   end) and
  (if $mutated == "linux.storage_provenance" then
    .rows["linux.storage_provenance"].status == "FAIL"
   else
    .rows["linux.storage_provenance"].status == "PASS" and
    (.rows["linux.storage_provenance"].evidence.restore_token_revocation_receipt | contains("revoke_all=true"))
   end) and
  (if $mutated == "linux.guest_authority" then
    .rows["linux.guest_authority"].status == "FAIL"
   else
    .rows["linux.guest_authority"].status == "NOT-RUN" and
    .rows["linux.guest_authority"].not_run_issue == 157 and
    (.rows["linux.guest_authority"].not_run_reason | contains("complete M3 OCI matrix")) and
    (.rows["linux.guest_authority"].evidence.blocked_assertion | contains("root Run"))
   end) and
  (.rows["linux.removal"].evidence.inventory_source == "helper VerifyNamespaceReadOnly route") and
  (.residue_inventories.post_removal_helper_namespace | type == "object")
' "$receipt" >/dev/null

jq -r '
  (.rows["linux.network_egress"].evidence.node_listener_ipv6_errno // "unavailable") as $egress |
  (.rows["linux.screen_crossover_refused"].evidence.node_listener_ipv6_errno // "unavailable") as $crossover |
  "linux-computer-ipv6-refusal image=\(.image.variant) egress=\($egress) crossover=\($crossover)"
' "$receipt"
