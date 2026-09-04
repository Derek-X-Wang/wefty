//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
)

var linuxComputerIPv6RefusalErrnos = []string{"EADDRNOTAVAIL", "ENETUNREACH", "EHOSTUNREACH", "ECONNREFUSED"}

func TestLinuxNativeComputerCLIMatrixAtProductionTimings(t *testing.T) {
	receipt := newLinuxComputerMatrixReceipt()
	evidence := newRealTimingEvidence(t)
	t.Cleanup(func() {
		receipt.finish()
		evidence.recordJSON("linux-computer-matrix.json", receipt)
	})
	if runtime.GOOS != "linux" {
		for _, required := range linuxComputerMatrixRows {
			receipt.begin(required.ID)
			if err := receipt.notRun(required.ID, 128,
				"Linux-native Computer acceptance requires the Ubuntu containerd runner; hosted Darwin does not claim Lima",
				map[string]bool{"darwin_typed_skip_recorded": true}, nil); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	t.Setenv("WEFTY_DEV_PLAIN_FABRIC_ID", "plain-linux-computer-acceptance")

	reference := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_REFERENCE")
	digest := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_DIGEST")
	archive := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_ARCHIVE")
	variant := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_VARIANT")
	reimagePrefix := "WEFTY_OCI_WAYLAND_COMPUTER_"
	if variant == "wayland" {
		reimagePrefix = "WEFTY_OCI_XFCE_COMPUTER_"
	} else if variant != "xfce" {
		t.Fatalf("unknown Computer matrix variant %q", variant)
	}
	reimageReference := requiredComputerRealtimeEnvironment(t, reimagePrefix+"REFERENCE")
	reimageDigest := requiredComputerRealtimeEnvironment(t, reimagePrefix+"DIGEST")
	reimageArchive := requiredComputerRealtimeEnvironment(t, reimagePrefix+"ARCHIVE")
	if reimageDigest == digest {
		t.Fatalf("Computer reimage artifact aliases the current %s image digest %s", variant, digest)
	}
	imageRuntime := readPublishedComputerRuntimeReceipt(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_COMPUTER_RUNTIME_RECEIPT"))
	receipt.Image = linuxComputerImageEvidence{Variant: variant, Reference: reference, IndexDigest: digest,
		PlatformDigest: imageRuntime.Digest, Archive: filepath.Base(archive)}
	candidate, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(candidate)) == "" {
		t.Fatalf("resolve candidate SHA: %v", err)
	}
	receipt.CandidateSHA = strings.TrimSpace(string(candidate))
	if published := requiredComputerRealtimeEnvironment(t, "CANDIDATE_SHA"); published != receipt.CandidateSHA {
		t.Fatalf("candidate SHA %s does not match published artifact SHA %s", receipt.CandidateSHA, published)
	}
	receipt.ResourceCaps = linuxComputerResourceCaps{MemoryBytes: 1 << 30, DiskBytes: 128 << 20, BackupCap: 4, SubmitMaxInflight: l1.DefaultComputerSubmitMaxInflight}
	receipt.Timings = map[string]string{
		"l1_lease":     l1.DefaultLeaseDuration.String(),
		"l1_node_dead": l1.DefaultNodeDeadAfter.String(),
		"l3_reconcile": "production-default",
	}
	receipt.Deviations = []linuxComputerDeviation{{ID: "dev.plain_fabric_identity", Status: "DEVIATION",
		Reason: "secretless Linux CI uses the shipped DEVELOPMENT ONLY self-asserted plain person identity route; attended Fabric identity remains #128"}}

	helperSocket := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_HELPER_CHECKSUM")
	probeReference := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_REFERENCE")
	probeDigest := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_DIGEST")
	recordBarrierResidue := func(residue *ocihelper.NamespaceResidueError) {
		receipt.ResidueInventories["between_package_runtime_residue"] = residue.RuntimeResidue
		receipt.ResidueInventories["between_package_durable_retained"] = residue.DurableRetained
		receipt.ResidueInventories["between_package_observed_inventory"] = residue.Observed
		receipt.ResidueAssertions["between_package_absence_blocked_by_runtime_residue"] = !ocihelper.InventoryEmpty(residue.RuntimeResidue)
		receipt.ResidueAssertions["between_package_observed_classified"] = reflect.DeepEqual(
			residue.Observed, mergeAcceptanceInventory(residue.RuntimeResidue, residue.DurableRetained))
	}
	importRealtimeProbeImage(t, requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_PROBE_ARCHIVE"),
		helperSocket, helperChecksum, probeReference, probeDigest, recordBarrierResidue)
	importRealtimeProbeImage(t, archive, helperSocket, helperChecksum, reference, digest, recordBarrierResidue)
	importRealtimeProbeImage(t, reimageArchive, helperSocket, helperChecksum, reimageReference, reimageDigest, recordBarrierResidue)
	removalBaseline := inspectHelperNamespaceInventory(t, helperSocket, helperChecksum)
	receipt.ResidueInventories["pre_matrix_observed_inventory"] = removalBaseline.Inventory
	receipt.ResidueInventories["pre_matrix_runtime_residue"] = removalBaseline.RuntimeResidue
	receipt.ResidueInventories["pre_matrix_durable_retained"] = removalBaseline.DurableRetained
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration: l1.DefaultLeaseDuration, productionTimings: true, computerLane: true,
		agentArguments: []string{
			"--oci-helper-socket=" + helperSocket,
			"--oci-helper-checksum=" + helperChecksum,
			"--oci-probe-image=" + probeReference,
			"--oci-probe-digest=" + probeDigest,
			"--oci-intent-file=" + intentPath,
		},
	})
	t.Cleanup(func() {
		evidence.recordProcessOutput("computer-control-plane.log", harness.controlPlane)
		evidence.recordProcessOutput("computer-run-ledger.log", harness.runLedger)
		for index, process := range harness.agents {
			evidence.recordProcessOutput(fmt.Sprintf("computer-agent-%02d.log", index+1), process)
		}
	})

	receipt.begin("linux.create_boot")
	traitRefusal := runComputerCLIExpectError(t, harness, "", "", "services", "create", "--name", "trait-refusal",
		"--image", reference+"@"+digest, "--node", "acceptance-node", "--disk-bytes", fmt.Sprint(64<<20))
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer",
		"--name", "linux-native-acceptance", "--image", reference+"@"+digest, "--node", "acceptance-node",
		"--memory-bytes", fmt.Sprint(1<<30), "--disk-bytes", fmt.Sprint(128<<20), "--backup-cap", "4",
		"--idempotency-key", "linux-native-computer-create")
	ready := waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return computerDisplayPublished(current) && current.AppliedRevision == current.IntentRevision
	})
	recordComputerAuthority(receipt, ready)
	diskEvidence := inspectLiveComputerDisk(t, ready)
	policy := bootstrapComputerAcceptanceAdmin(t, harness)
	adminView := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-admin", "linux-admin-device-a", takeoverViewRetryStalePolicy)
	receipt.TakeoverRetryStderr = append(receipt.TakeoverRetryStderr, adminView.toleratedStderr...)
	adminTake := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", adminView.tokenFile)
	adminRelease := contract.ComputerControlReceipt{}
	if !mutatingLinuxComputerRow("linux.create_boot") {
		adminRelease = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-a",
			"services", "takeover", "release", ready.ComputerID, "--session-token-file", adminView.tokenFile)
	}
	adminView.stop(t)
	completeLinuxComputerRow(t, receipt, "linux.create_boot", map[string]bool{
		"trait_only_refusal_observed":     strings.Contains(traitRefusal, "require --computer"),
		"trait_only_publication":          ready.CurrentJob.Spec.PublishedPort == nil,
		"fully_allocated_disk_on_disk":    diskEvidence.FullyAllocated,
		"view_named_endpoint_dialed":      adminView.admitted,
		"control_named_endpoint_dialed":   adminTake.TenureState == contract.ComputerControlTenureHeld,
		"control_named_endpoint_released": adminRelease.TenureState == contract.ComputerControlTenureFree,
		"helper_admitted_real_payload":    ready.CurrentJob.CurrentAttemptID != "",
		"cross_process_plain_authority":   len(policy.Admins) == 1 && policy.Admins[0].FabricID == "plain-linux-computer-acceptance" && adminView.admitted,
	}, map[string]string{"computer_id": ready.ComputerID, "job_id": ready.CurrentJobID,
		"attempt_id": ready.CurrentJob.CurrentAttemptID, "storage_id": ready.StorageID,
		"storage_generation": fmt.Sprint(ready.StorageGeneration), "intent_revision": fmt.Sprint(ready.IntentRevision),
		"disk_path": diskEvidence.Path, "disk_blocks_bytes": fmt.Sprint(diskEvidence.BlocksBytes)})

	receipt.begin("linux.network_egress")
	readyEndpoints := readLiveComputerEndpointEnvironment(t, ready)
	nodeListenerPort, stopNodeListener := startDualStackNodeBoundaryListener(t)
	defer stopNodeListener()
	nodeGatewayIPv6 := readComputerHostLinkIPv6(t, readyEndpoints.ViewPort)
	egress := probeComputerNetworkEgress(t, ready, readyEndpoints, nodeGatewayIPv6, nodeListenerPort)
	completeLinuxComputerRow(t, receipt, "linux.network_egress", map[string]bool{
		"private_veth_address_present": egress.Address != "" && egress.Gateway != "" && egress.Address != egress.Gateway,
		"mounted_resolver_recorded":    egress.ResolverSnapshot != "" && egress.ResolverAddress != "",
		"loopback_proxy_listening":     !net.ParseIP(egress.ResolverAddress).IsLoopback() || egress.ProxyUDPListening && egress.ProxyTCPListening,
		"proxy_upstream_reachable":     !net.ParseIP(egress.ResolverAddress).IsLoopback() || egress.ProxyUpstreamReachable,
		"default_route_present":        egress.DefaultRouteInterface == "eth0" && egress.DefaultRouteGateway == egress.Gateway,
		"public_ipv4_connected":        egress.PublicIPv4.Outcome == "connected",
		"resolver_reachable":           egress.DNSOutcome == "resolved" && egress.ResolvedName == "example.com" && egress.ResolvedAddress != "",
		"helper_http_through_veth":     egress.HelperHTTPStatus == 200 && egress.HelperHTTPBody == "wefty-computer-egress-v1" && !mutatingLinuxComputerRow("linux.network_egress"),
		"node_listener_ipv4_refused":   egress.NodeListenerIPv4.Outcome == "refused" && egress.NodeListenerIPv4.ErrnoName == "ECONNREFUSED",
		"node_listener_ipv6_refused":   egress.NodeListenerIPv6.Outcome == "refused" && slices.Contains(linuxComputerIPv6RefusalErrnos, egress.NodeListenerIPv6.ErrnoName),
	}, map[string]string{
		"computer_id": ready.ComputerID, "attempt_id": ready.CurrentJob.CurrentAttemptID,
		"veth_address": egress.Address, "veth_gateway": egress.Gateway,
		"resolver_snapshot": egress.ResolverSnapshot, "resolver_address": egress.ResolverAddress,
		"proxy_udp_listening": strconv.FormatBool(egress.ProxyUDPListening), "proxy_tcp_listening": strconv.FormatBool(egress.ProxyTCPListening),
		"proxy_upstream_address": egress.ProxyUpstreamAddress, "proxy_upstream_source": egress.ProxyUpstreamSource,
		"proxy_upstream_reachable": strconv.FormatBool(egress.ProxyUpstreamReachable),
		"default_route_interface":  egress.DefaultRouteInterface, "default_route_gateway": egress.DefaultRouteGateway,
		"public_ipv4_address": egress.PublicIPv4.Address, "public_ipv4_outcome": egress.PublicIPv4.Outcome,
		"public_ipv4_errno": egress.PublicIPv4.ErrnoName, "dns_outcome": egress.DNSOutcome,
		"resolved_name": egress.ResolvedName, "resolved_address": egress.ResolvedAddress,
		"helper_http_status": fmt.Sprint(egress.HelperHTTPStatus), "helper_http_body": egress.HelperHTTPBody,
		"node_listener_ipv4_address": egress.NodeListenerIPv4.Address, "node_listener_ipv4_outcome": egress.NodeListenerIPv4.Outcome,
		"node_listener_ipv4_errno":   egress.NodeListenerIPv4.ErrnoName,
		"node_listener_ipv6_address": egress.NodeListenerIPv6.Address, "node_listener_ipv6_outcome": egress.NodeListenerIPv6.Outcome,
		"node_listener_ipv6_errno": egress.NodeListenerIPv6.ErrnoName,
	})

	receipt.begin("linux.screen_crossover_refused")
	neighbour := createReadyComputer(t, harness, reference, digest, "linux-native-crossover-neighbour", "linux-native-crossover-neighbour-create")
	recordComputerAuthority(receipt, neighbour)
	probeTarget := neighbour
	if mutatingLinuxComputerRow("linux.screen_crossover_refused") {
		// The negative lane targets the source Computer's own endpoints. Those
		// must remain readable/controllable, proving the refusal assertions are
		// sensitive to a real reachable screen rather than fixed receipt values.
		probeTarget = ready
	}
	targetEndpoints := readLiveComputerEndpointEnvironment(t, probeTarget)
	stopEgressListener := startLiveComputerEgressListener(t, probeTarget, targetEndpoints.Address)
	egressAlive := probeLiveComputerEgressListener(t, probeTarget, targetEndpoints.Address)
	crossover := probeComputerScreenCrossover(t, ready, probeTarget, variant, targetEndpoints, nodeGatewayIPv6, nodeListenerPort)
	// The target-local success immediately after A's refusals proves the target
	// screen and test listener were alive at the same boundary edge.
	liveness := probeComputerScreenCrossover(t, probeTarget, probeTarget, variant, targetEndpoints, nodeGatewayIPv6, nodeListenerPort)
	stopEgressListener()
	removedNeighbour := removeAndWaitComputer(t, harness, neighbour, 4*time.Minute)
	crossoverRefused := crossover.ViewRead.Outcome == "refused" && crossover.ViewRead.ErrnoName == "ECONNREFUSED" &&
		crossover.ControlInject.Outcome == "refused" && crossover.ControlInject.ErrnoName == "ECONNREFUSED" &&
		crossover.EgressAddress.Outcome == "refused" && crossover.EgressAddress.ErrnoName == "ECONNREFUSED" &&
		crossover.NodeListenerIPv6.Outcome == "refused" && slices.Contains(linuxComputerIPv6RefusalErrnos, crossover.NodeListenerIPv6.ErrnoName)
	targetAlive := egressAlive && liveness.ViewRead.Outcome == "read_succeeded" && liveness.ControlInject.Outcome == "inject_succeeded" && liveness.EgressAddress.Outcome == "connected"
	if variant == "xfce" {
		crossoverRefused = crossoverRefused && !crossover.AbstractSocketVisible &&
			crossover.AbstractSocket.Outcome == "refused" && crossover.AbstractSocket.ErrnoName == "ENOENT" &&
			crossover.DerivedDisplay.Outcome == "transport_refused" && crossover.DerivedDisplay.Class == "x_transport"
		targetAlive = targetAlive && liveness.AbstractSocketVisible && liveness.AbstractSocket.Outcome == "connected" && liveness.DerivedDisplay.Outcome == "read_succeeded"
	}
	completeLinuxComputerRow(t, receipt, "linux.screen_crossover_refused", map[string]bool{
		"two_colocated_computers_live": ready.CurrentJob.CurrentAttemptID != "" && neighbour.CurrentJob.CurrentAttemptID != "" && ready.ComputerID != neighbour.ComputerID,
		"target_alive_at_refusal_edge": targetAlive,
		"crossover_refused":            crossoverRefused,
		"neighbour_removed_verified":   removedNeighbour.RemovalOutcome == "removed_verified",
	}, map[string]string{
		"source_computer_id":         ready.ComputerID,
		"source_attempt_id":          ready.CurrentJob.CurrentAttemptID,
		"target_computer_id":         probeTarget.ComputerID,
		"target_attempt_id":          probeTarget.CurrentJob.CurrentAttemptID,
		"target_view_port":           fmt.Sprint(targetEndpoints.ViewPort),
		"target_control_port":        fmt.Sprint(targetEndpoints.ControlPort),
		"target_egress_address":      targetEndpoints.Address,
		"target_veth_gateway":        targetEndpoints.Gateway,
		"target_egress_port":         fmt.Sprint(liveComputerEgressListenerPort),
		"target_liveness_view":       liveness.ViewRead.Outcome,
		"target_liveness_control":    liveness.ControlInject.Outcome,
		"target_liveness_x":          liveness.DerivedDisplay.Outcome,
		"target_liveness_egress":     liveness.EgressAddress.Outcome,
		"abstract_socket":            crossover.AbstractSocketName,
		"abstract_socket_visible":    strconv.FormatBool(crossover.AbstractSocketVisible),
		"abstract_socket_outcome":    crossover.AbstractSocket.Outcome,
		"abstract_socket_errno":      crossover.AbstractSocket.ErrnoName,
		"derived_display":            crossover.DerivedDisplay.Address,
		"derived_display_outcome":    crossover.DerivedDisplay.Outcome,
		"derived_display_class":      crossover.DerivedDisplay.Class,
		"view_read_outcome":          crossover.ViewRead.Outcome,
		"view_read_address":          crossover.ViewRead.Address,
		"view_read_errno":            crossover.ViewRead.ErrnoName,
		"control_inject_outcome":     crossover.ControlInject.Outcome,
		"control_inject_address":     crossover.ControlInject.Address,
		"control_inject_errno":       crossover.ControlInject.ErrnoName,
		"egress_address_outcome":     crossover.EgressAddress.Outcome,
		"egress_address_target":      crossover.EgressAddress.Address,
		"egress_address_errno":       crossover.EgressAddress.ErrnoName,
		"node_listener_ipv6_address": crossover.NodeListenerIPv6.Address,
		"node_listener_ipv6_outcome": crossover.NodeListenerIPv6.Outcome,
		"node_listener_ipv6_errno":   crossover.NodeListenerIPv6.ErrnoName,
		"neighbour_removal_outcome":  removedNeighbour.RemovalOutcome,
	})

	receipt.begin("linux.remote_takeover")
	viewerIdentity := runComputerCLIPersonWithEvidence[l1.AuthenticatedPerson](t, evidence, "whoami-cli-viewer.json", harness, "linux-viewer", "linux-viewer-device", "whoami")
	if viewerIdentity.FabricID != policy.Admins[0].FabricID || viewerIdentity.UserID != "linux-viewer" || viewerIdentity.DeviceID != "linux-viewer-device" {
		t.Fatalf("viewer whoami observation = %#v", viewerIdentity)
	}
	viewGrant := runComputerCLIPersonWithEvidence[l1.ComputerGrantMutationResult](t, evidence, "grant-cli-view.json", harness, "linux-admin", "linux-admin-device-a",
		"services", "grant", ready.ComputerID, "linux-viewer", "--permission", "view",
		"--policy-revision", fmt.Sprint(policy.Revision), "--idempotency-key", "linux-native-view-grant")
	if !viewGrant.MutationApplied || viewGrant.Grant.Permission != l1.ComputerGrantView {
		t.Fatalf("Computer view grant = %#v", viewGrant)
	}
	receipt.FabricIdentities = append(receipt.FabricIdentities,
		linuxComputerFabricIdentity{Role: "administrator", FabricID: policy.Admins[0].FabricID, UserID: "linux-admin", DeviceID: "linux-admin-device-a"},
		linuxComputerFabricIdentity{Role: "viewer", FabricID: viewGrant.Grant.FabricID, UserID: "linux-viewer", DeviceID: "linux-viewer-device"})
	// L1 publishes the durable grant before the hosting agent can acknowledge
	// and install that policy revision. Establish live viewer admission through
	// the typed stale-policy retry path before the direct RFB isolation probe.
	viewerView := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-viewer", "linux-viewer-device", takeoverViewRetryStalePolicy)
	receipt.TakeoverRetryStderr = append(receipt.TakeoverRetryStderr, viewerView.toleratedStderr...)
	inputIsolation := proveLiveViewInputIsolation(t, harness, ready, "linux-viewer", "linux-viewer-device")
	viewerTakeDenied := runComputerCLIPersonExpectError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerView.tokenFile)
	controlGrant := runComputerCLIPersonWithEvidence[l1.ComputerGrantMutationResult](t, evidence, "grant-cli-control.json", harness, "linux-admin", "linux-admin-device-a",
		"services", "grant", ready.ComputerID, "linux-viewer", "--permission", "control",
		"--policy-revision", fmt.Sprint(viewGrant.Grant.PolicyRevision), "--idempotency-key", "linux-native-control-grant")
	viewerView.stop(t)
	viewerControl := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-viewer", "linux-viewer-device", takeoverViewRetryStalePolicy)
	receipt.TakeoverRetryStderr = append(receipt.TakeoverRetryStderr, viewerControl.toleratedStderr...)
	viewerTake := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	viewerRelease := contract.ComputerControlReceipt{}
	if !mutatingLinuxComputerRow("linux.remote_takeover") {
		viewerRelease = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
			"services", "takeover", "release", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	}
	if viewerTake.TenureState != contract.ComputerControlTenureHeld {
		t.Fatalf("viewer take receipt = %#v", viewerTake)
	}
	viewerTake = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	adminOverrideView := startTakeoverViewCLI(t, evidence, harness, ready.ComputerID, "linux-admin", "linux-admin-device-b", takeoverViewRetryNone)
	adminOverride := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-b",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", adminOverrideView.tokenFile)
	adminOverrideRelease := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-admin-device-b",
		"services", "takeover", "release", ready.ComputerID, "--session-token-file", adminOverrideView.tokenFile)
	viewerTake = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	viewerDrivingBeforeRevoke := viewerTake.TenureState == contract.ComputerControlTenureHeld && viewerTake.HumanDriving &&
		readLiveComputerHumanDriving(t, ready.CurrentJobID)
	revoked := runComputerCLIPerson[l1.ComputerGrantMutationResult](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "revoke", ready.ComputerID, "linux-viewer", "--policy-revision", fmt.Sprint(controlGrant.Grant.PolicyRevision),
		"--idempotency-key", "linux-native-control-revoke", "--wait", "--wait-timeout", "3m")
	viewerControl.waitClosed(t, 30*time.Second)
	staleSessionDenied := runComputerCLIPersonExpectControlError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", ready.ComputerID, "--session-token-file", viewerControl.tokenFile)
	if staleSessionDenied.Receipt == nil {
		t.Fatalf("revoked take-over session omitted its typed terminal receipt: %#v", staleSessionDenied)
	}
	viewerDrivingAfterRevoke := waitLiveComputerHumanDriving(t, ready.CurrentJobID, false, 10*time.Second)
	adminOverrideView.stop(t)
	audit := runComputerCLIPerson[l1.ComputerTakeoverAuditList](t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "audit", "tail", ready.ComputerID, "--limit", "100")
	auditKinds, auditAuthority := takeoverAuditEvidence(audit)
	revokedControlBeforeSession := takeoverAuditReleasePrecedesClose(audit, viewerTake.HolderSessionID, l1.ComputerTakeoverRevoked)
	receipt.AuthorityGenerations = appendUniqueInt64(receipt.AuthorityGenerations, auditAuthority)
	completeLinuxComputerRow(t, receipt, "linux.remote_takeover", map[string]bool{
		"cli_view_grant_cas":           viewGrant.MutationApplied,
		"view_admission_live":          viewerView.admitted,
		"view_only_take_refused":       strings.Contains(viewerTakeDenied, string(contract.ErrorControlNotAuthorized)),
		"view_pointer_isolation_live":  inputIsolation,
		"cli_control_grant_cas":        controlGrant.MutationApplied,
		"cli_take_live":                viewerTake.TenureState == contract.ComputerControlTenureHeld,
		"cli_release_live":             viewerRelease.TenureState == contract.ComputerControlTenureFree,
		"admin_override_live":          adminOverride.OverrideDisplacedSessionID != "" && adminOverride.SignalStayedTrue && adminOverrideRelease.TenureState == contract.ComputerControlTenureFree,
		"cli_revoke_installed":         revoked.ObservationState == "completed",
		"revoked_while_driving":        viewerDrivingBeforeRevoke,
		"revocation_closed_session":    staleSessionDenied.Error.Code == contract.ErrorTakeoverSessionEnded && staleSessionDenied.Receipt.SessionEndReason == string(l1.ComputerTakeoverRevoked),
		"revocation_cleared_driver":    !viewerDrivingAfterRevoke,
		"revoked_release_before_close": revokedControlBeforeSession,
		"audit_tail_session_open":      auditKinds[l1.ComputerTakeoverSessionOpen],
		"audit_tail_session_close":     auditKinds[l1.ComputerTakeoverSessionClose],
		"audit_tail_control_acquired":  auditKinds[l1.ComputerTakeoverControlAcquired],
		"audit_tail_control_released":  auditKinds[l1.ComputerTakeoverControlReleased],
		"audit_tail_admin_overrode":    auditKinds[l1.ComputerTakeoverAdminOverrode],
	}, map[string]string{
		"policy_revision":      fmt.Sprint(revoked.Grant.PolicyRevision),
		"authority_generation": fmt.Sprint(auditAuthority),
		"viewer_fabric_id":     viewGrant.Grant.FabricID,
		"revoked_session_id":   viewerTake.HolderSessionID,
		"session_end_reason":   staleSessionDenied.Receipt.SessionEndReason,
	})

	receipt.begin("linux.restart_survival")
	oldAttempt, oldStorage, oldGeneration := ready.CurrentJob.CurrentAttemptID, ready.StorageID, ready.StorageGeneration
	profileMarker := plantLiveProfileMarker(t, ready, "restart-survival-marker")
	lossAttempts := map[string]string{}
	var helperLossTerminal *l1.Attempt
	for _, action := range []string{"kill-payload", "kill-shim", "kill-helper", "stop-containerd"} {
		before := ready.CurrentJob.CurrentAttemptID
		faultAction := action
		if action == "kill-payload" || action == "kill-shim" {
			faultAction += ":" + ready.CurrentJobID
		} else if action == "kill-helper" {
			faultAction += ":service-restart-survival"
		}
		triggerLinuxComputerFault(t, harness, faultAction)
		if action == "stop-containerd" {
			triggerLinuxComputerFault(t, harness, "start-containerd")
		}
		if harness.agent.exited() {
			harness.restartAgent(t)
		}
		ready = waitForComputerCLI(t, harness, ready.ComputerID, 5*time.Minute, func(current l1.Computer) bool {
			return computerDisplayPublished(current) &&
				current.CurrentJob.CurrentAttemptID != "" && current.CurrentJob.CurrentAttemptID != before
		})
		lossAttempts[action] = ready.CurrentJob.CurrentAttemptID
		if action == "kill-helper" {
			terminal := waitForComputerAttemptTerminal(t, harness, ready.CurrentJobID, before, 30*time.Second)
			helperLossTerminal = &terminal
		}
		assertLiveProfileMarker(t, ready, profileMarker)
	}
	beforeAgentLoss := ready.CurrentJob.CurrentAttemptID
	restarted := ready
	if !mutatingLinuxComputerRow("linux.restart_survival") {
		harness.agent.kill(t)
		harness.restartAgent(t)
		restarted = waitForComputerCLI(t, harness, ready.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
			return computerDisplayPublished(current) &&
				current.CurrentJob.CurrentAttemptID != "" && current.CurrentJob.CurrentAttemptID != beforeAgentLoss
		})
	}
	assertLiveProfileMarker(t, restarted, profileMarker)
	// The live-session file retains the old door's ephemeral endpoint. Present
	// the unchanged bearer to the republished door so this row tests Node-lineage
	// terminality rather than a TCP refusal from the dead listener.
	oldSessionEndpoint, currentSessionEndpoint := retargetTakeoverSessionCapability(t, viewerControl.tokenFile, restarted.DisplayEndpoint)
	oldAuthorityRejected := runComputerCLIPersonExpectControlError(t, harness, "linux-viewer", "linux-viewer-device",
		"services", "takeover", "take", restarted.ComputerID, "--session-token-file", viewerControl.tokenFile)
	oldSessionEndReason := ""
	if oldAuthorityRejected.Receipt != nil {
		oldSessionEndReason = oldAuthorityRejected.Receipt.SessionEndReason
	}
	recordComputerAuthority(receipt, restarted)
	helperLossTerminalCode, helperLossTerminalID := "", ""
	if helperLossTerminal != nil {
		helperLossTerminalID = helperLossTerminal.AttemptID
		if helperLossTerminal.Result != nil && helperLossTerminal.Result.RuntimeFailure != nil {
			helperLossTerminalCode = string(helperLossTerminal.Result.RuntimeFailure.Code)
		}
	}
	completeLinuxComputerRow(t, receipt, "linux.restart_survival", map[string]bool{
		"payload_loss_fresh_attempt": lossAttempts["kill-payload"] != "" && lossAttempts["kill-payload"] != oldAttempt,
		"shim_loss_fresh_attempt":    lossAttempts["kill-shim"] != "" && lossAttempts["kill-shim"] != lossAttempts["kill-payload"],
		"helper_loss_fresh_attempt":  lossAttempts["kill-helper"] != "" && lossAttempts["kill-helper"] != lossAttempts["kill-shim"],
		"helper_loss_prior_attempt_typed_terminal": helperLossTerminal != nil && helperLossTerminal.AttemptID == lossAttempts["kill-shim"] &&
			helperLossTerminal.State == contract.AttemptFailed && helperLossTerminal.Result != nil && helperLossTerminal.Result.RuntimeFailure != nil &&
			helperLossTerminal.Result.RuntimeFailure.Code == contract.RuntimeFailureUnavailable,
		"runtime_loss_fresh_attempt":     lossAttempts["stop-containerd"] != "" && lossAttempts["stop-containerd"] != lossAttempts["kill-helper"],
		"agent_loss_fresh_attempt":       restarted.CurrentJob.CurrentAttemptID != beforeAgentLoss,
		"same_storage_generation":        restarted.StorageID == oldStorage && restarted.StorageGeneration == oldGeneration,
		"profile_marker_survived_losses": true,
		"readiness_republished":          oldSessionEndpoint != currentSessionEndpoint,
		"old_session_authority_rejected": oldAuthorityRejected.Error.Code == contract.ErrorTakeoverSessionEnded &&
			oldAuthorityRejected.Receipt != nil && oldAuthorityRejected.Receipt.SessionEndReason == string(l1.ComputerTakeoverAttemptAuthorityLost),
	}, map[string]string{"old_attempt_id": oldAttempt, "new_attempt_id": restarted.CurrentJob.CurrentAttemptID,
		"storage_id": restarted.StorageID, "storage_generation": fmt.Sprint(restarted.StorageGeneration),
		"profile_marker": filepath.Base(profileMarker), "helper_loss_terminal_attempt_id": helperLossTerminalID,
		"helper_loss_terminal_code": helperLossTerminalCode, "old_session_endpoint": oldSessionEndpoint,
		"current_session_endpoint": currentSessionEndpoint, "old_session_end_reason": oldSessionEndReason})

	receipt.begin("linux.reconfiguration")
	resized := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", restarted.ComputerID,
		"--disk-bytes", fmt.Sprint(160<<20), "--expect-current", "--idempotency-key", "linux-native-grow")
	resized = waitForComputerCLI(t, harness, resized.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.DesiredDiskBytes == 160<<20
	})
	reset := runComputerCLI[l1.Computer](t, harness, false, "services", "reset", resized.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-reset", "--terminate-sessions")
	resetIntent := reset.IntentRevision
	resetCrashObserved := false
	if !mutatingLinuxComputerRow("linux.reconfiguration") {
		triggerLinuxComputerFault(t, harness, "kill-helper:service-reconfiguration-reset")
		resetCrashObserved = true
		if harness.agent.exited() {
			harness.restartAgent(t)
		}
	}
	reset = waitForComputerCLI(t, harness, reset.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision && current.StorageGeneration > resized.StorageGeneration
	})
	// Reset can publish its verified empty successor before the first attempt
	// initializes that filesystem root for the image user. Reimage therefore
	// carries the explicit one-shot ownership authority instead of depending on
	// an incidental attach winning this race.
	reimageStarted := time.Now()
	reimaged := runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", reset.ComputerID,
		"--image", reimageReference+"@"+reimageDigest, "--expect-current", "--idempotency-key", "linux-native-reimage", "--terminate-sessions", "--chown")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		failOnTypedReimagePreflight(t, current, reset.CurrentSpecRevision)
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > reset.CurrentSpecRevision && computerDisplayPublished(current)
	})
	chownReimaged := reimaged
	chownReimageElapsed := time.Since(reimageStarted)
	ownershipMatchStarted := time.Now()
	reimaged = runComputerCLI[l1.Computer](t, harness, false, "services", "reimage", reimaged.ComputerID,
		"--image", reference+"@"+digest, "--expect-current", "--idempotency-key", "linux-native-reimage-ownership-match", "--terminate-sessions")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 4*time.Minute, func(current l1.Computer) bool {
		failOnTypedReimagePreflight(t, current, chownReimaged.CurrentSpecRevision)
		return current.ReconfigurationPhase == l1.ComputerReconfigurationStable && current.AppliedRevision == current.IntentRevision &&
			current.CurrentSpecRevision > chownReimaged.CurrentSpecRevision && computerDisplayPublished(current)
	})
	ownershipMatchElapsed := time.Since(ownershipMatchStarted)
	abortEvidence := exerciseLiveReconfigurationAbort(t, harness, reference, digest)
	detachment := inspectLiveComputerReimageDetachment(t, harness, reimaged)
	recordComputerAuthority(receipt, reimaged)
	completeLinuxComputerRow(t, receipt, "linux.reconfiguration", map[string]bool{
		"grow_applied_live":            resized.DesiredDiskBytes == 160<<20,
		"reset_crash_phase_live":       resetCrashObserved && reset.IntentRevision >= resetIntent,
		"reset_fresh_generation_live":  reset.StorageGeneration > resized.StorageGeneration,
		"reimage_new_projection_live":  reimaged.CurrentSpecRevision > chownReimaged.CurrentSpecRevision && chownReimaged.CurrentSpecRevision > reset.CurrentSpecRevision,
		"reimage_ownership_match_live": ownershipMatchElapsed > 0,
		"detachment_receipt_live":      detachment,
		"abort_after_dead_node_live":   abortEvidence.Aborted,
		"stale_cas_rejected_live":      abortEvidence.StaleCASRejected,
		"no_automatic_rollback_live":   abortEvidence.NoAutoRollback,
	}, map[string]string{"intent_revision": fmt.Sprint(reimaged.IntentRevision), "spec_revision": fmt.Sprint(reimaged.CurrentSpecRevision),
		"storage_generation": fmt.Sprint(reimaged.StorageGeneration), "reimage_reference": reimageReference,
		"reimage_digest": reimageDigest, "reimage_chown_elapsed": chownReimageElapsed.String(),
		"reimage_ownership_match_elapsed": ownershipMatchElapsed.String(), "aborted_computer_id": abortEvidence.ComputerID})

	receipt.begin("linux.storage_provenance")
	backupOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "create", reimaged.ComputerID,
		"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-backup", "--wait", "4m")
	if backupOutput.Backups == nil || len(backupOutput.Backups.Backups) != 1 || backupOutput.Backups.Backups[0].Status != "available" {
		t.Fatalf("Computer Backup result = %#v", backupOutput)
	}
	backup := backupOutput.Backups.Backups[0]
	cloneOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "clone", reimaged.ComputerID, backup.BackupID,
		"--name", "linux-native-clone", "--disk-bytes", fmt.Sprint(160<<20), "--expect-current",
		"--idempotency-key", "linux-native-clone", "--wait", "4m")
	if cloneOutput.Computer == nil || cloneOutput.Computer.ComputerID == "" {
		t.Fatalf("Computer clone result = %#v", cloneOutput)
	}
	cloneReceipt := requiredStorageCopyReceipt(t, cloneOutput.StorageProvenance, "clone", cloneOutput.Computer.ComputerID)
	if cloneReceipt.MachineIDBeforeDigest == cloneReceipt.MachineIDAfterDigest || !cloneReceipt.SourceUnchanged ||
		!cloneReceipt.DestinationPrepared || cloneReceipt.PreparationReceipt || cloneReceipt.DestinationChown {
		t.Fatalf("Computer clone receipt = %#v", cloneReceipt)
	}
	startedClone := runComputerCLI[l1.Computer](t, harness, false, "services", "start", cloneOutput.Computer.ComputerID, "--expect-current")
	startedClone = waitForComputerCLI(t, harness, startedClone.ComputerID, 3*time.Minute, computerDisplayPublished)
	cloneOutput.Computer = &startedClone
	recordComputerAuthority(receipt, *cloneOutput.Computer)
	exportDirectory := filepath.Join(t.TempDir(), "custody-export")
	if err := os.MkdirAll(exportDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	exportOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "export", reimaged.ComputerID, backup.BackupID,
		"--path", exportDirectory, "--expect-current", "--idempotency-key", "linux-native-export", "--wait", "4m")
	if exportOutput.CustodyExport == nil || exportOutput.CustodyExport.Status != "available" || exportOutput.CustodyExport.ManifestDigest == "" {
		t.Fatalf("Computer custody export result = %#v", exportOutput)
	}
	manifestPath := filepath.Join(exportDirectory, "custody.json")
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := sha256.Sum256(manifestBytes)
	manifestDigest := "sha256:" + hex.EncodeToString(manifestHash[:])
	if manifestDigest != exportOutput.CustodyExport.ManifestDigest {
		t.Fatalf("custody manifest digest = %s, want %s", manifestDigest, exportOutput.CustodyExport.ManifestDigest)
	}
	importOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "import", exportOutput.CustodyExport.ExportID,
		"--name", "linux-native-import", "--disk-bytes", fmt.Sprint(160<<20), "--node", "acceptance-node",
		"--path", exportDirectory, "--manifest", manifestPath, "--manifest-digest", manifestDigest,
		"--idempotency-key", "linux-native-import", "--wait", "4m")
	if importOutput.Computer == nil || importOutput.StorageProvenance == nil || !importOutput.StorageProvenance.CustodyTainted {
		t.Fatalf("Computer custody import result = %#v", importOutput)
	}
	importReceipt := requiredStorageCopyReceipt(t, importOutput.StorageProvenance, "import", importOutput.Computer.ComputerID)
	if importReceipt.MachineIDBeforeDigest == importReceipt.MachineIDAfterDigest || !importReceipt.SourceUnchanged ||
		!importReceipt.DestinationPrepared || importReceipt.PreparationReceipt || importReceipt.DestinationChown {
		t.Fatalf("Computer import receipt = %#v", importReceipt)
	}
	recordComputerAuthority(receipt, *importOutput.Computer)
	if err := os.RemoveAll(exportDirectory); err != nil {
		t.Fatal(err)
	}
	attestOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "custody", "attest",
		exportOutput.CustodyExport.ExportID, "--idempotency-key", "linux-native-export-attested")
	if attestOutput.CustodyExport == nil || !attestOutput.CustodyExport.OperatorAttestedDeleted {
		t.Fatalf("Computer custody attestation result = %#v", attestOutput)
	}
	staleCredential := startTakeoverViewCLI(t, evidence, harness, reimaged.ComputerID, "linux-admin", "linux-admin-device-a", takeoverViewRetryNone)
	stopping := runComputerCLI[l1.Computer](t, harness, false, "services", "stop", reimaged.ComputerID, "--expect-current")
	stopped := waitForComputerCLI(t, harness, stopping.ComputerID, 3*time.Minute, computerStoppedAfterExplicitStop)
	restoreAdmissionState := stopped.CurrentJob.State
	restoreBaseline := stopped.StorageGeneration
	restoreOutput := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "restore", stopped.ComputerID, backup.BackupID,
		"--keep-old-as-backup", "--expect-current", "--idempotency-key", "linux-native-restore", "--wait", "4m")
	if restoreOutput.Computer == nil || restoreOutput.Computer.StorageGeneration != restoreBaseline+1 {
		t.Fatalf("Computer restore result = %#v", restoreOutput)
	}
	restoreReceipt := requiredStorageCopyReceipt(t, restoreOutput.StorageProvenance, "restore", restoreOutput.Computer.ComputerID)
	if restoreReceipt.OSIdentityRekeyed || restoreReceipt.MachineIDBeforeDigest != "" || restoreReceipt.MachineIDAfterDigest != "" ||
		!restoreReceipt.SourceUnchanged || !restoreReceipt.DestinationPrepared || restoreReceipt.PreparationReceipt || restoreReceipt.DestinationChown {
		t.Fatalf("Computer restore receipt = %#v", restoreReceipt)
	}
	restoreRevocation := restoreOutput.Computer.LastRestoreRevocation
	restoreTokenRevoked := restoreRevocation != nil &&
		restoreRevocation.OperationRevision == restoreOutput.Computer.IntentRevision && restoreRevocation.RevokeAll &&
		restoreRevocation.TokenRevocation.ComputerID == reimaged.ComputerID &&
		!restoreRevocation.TokenRevocation.CommittedAt.IsZero()
	restoreTokenRevocationDetail := "absent"
	if restoreRevocation != nil {
		restoreTokenRevocationDetail = fmt.Sprintf("operation_revision=%d revoke_all=%t computer_id=%s committed_at=%s",
			restoreRevocation.OperationRevision, restoreRevocation.RevokeAll,
			restoreRevocation.TokenRevocation.ComputerID, restoreRevocation.TokenRevocation.CommittedAt.Format(time.RFC3339Nano))
	}
	staleCredential.waitClosed(t, 30*time.Second)
	reimaged = *restoreOutput.Computer
	backupInventory := runComputerCLI[computerBackupInventoryReceipt](t, harness, false, "services", "backup", "list", reimaged.ComputerID)
	capSet := runComputerCLI[storageCLIMutationReceipt](t, harness, false, "services", "backup", "set-cap", reimaged.ComputerID,
		"--cap", fmt.Sprint(len(backupInventory.Backups)), "--expect-current")
	capPressure := ""
	if !mutatingLinuxComputerRow("linux.storage_provenance") {
		capPressure = runComputerCLIExpectError(t, harness, "", "", "services", "backup", "create", reimaged.ComputerID,
			"--expect-current", "--allow-power-off", "--idempotency-key", "linux-native-cap-pressure", "--wait", "4m")
	}
	enospc := exerciseLiveComputerENOSPC(t, harness, reference, digest)
	reimaged = runComputerCLI[l1.Computer](t, harness, false, "services", "start", reimaged.ComputerID, "--expect-current")
	reimaged = waitForComputerCLI(t, harness, reimaged.ComputerID, 3*time.Minute, computerDisplayPublished)
	restoreOldEndpoint, restoreCurrentEndpoint := retargetTakeoverSessionCapability(t, staleCredential.tokenFile, reimaged.DisplayEndpoint)
	preStopSessionRejectedAfterRestore := runComputerCLIPersonExpectControlError(t, harness, "linux-admin", "linux-admin-device-a",
		"services", "takeover", "take", reimaged.ComputerID, "--session-token-file", staleCredential.tokenFile)
	recordComputerAuthority(receipt, reimaged)
	storageAssertions := map[string]bool{
		"cold_backup_available_live":         backup.Status == "available",
		"clone_fork_created_live":            cloneOutput.Computer.ComputerID != reimaged.ComputerID,
		"clone_machine_id_rekeyed_live":      cloneReceipt.MachineIDBeforeDigest != cloneReceipt.MachineIDAfterDigest,
		"clone_source_unchanged_live":        cloneReceipt.SourceUnchanged,
		"clone_first_attach_nonfresh_live":   cloneReceipt.DestinationPrepared && !cloneReceipt.PreparationReceipt && !cloneReceipt.DestinationChown && computerDisplayPublished(*cloneOutput.Computer),
		"custody_export_manifest_bound_live": manifestDigest == exportOutput.CustodyExport.ManifestDigest,
		"custody_import_tainted_live":        importOutput.StorageProvenance.CustodyTainted,
		"import_machine_id_rekeyed_live":     importReceipt.MachineIDBeforeDigest != importReceipt.MachineIDAfterDigest,
		"import_source_unchanged_live":       importReceipt.SourceUnchanged,
		"custody_delete_attested_live":       attestOutput.CustodyExport.OperatorAttestedDeleted,
		"keep_old_restore_live":              len(backupInventory.Backups) >= 2,
		"restore_fresh_generation_live":      restoreOutput.Computer.StorageGeneration == restoreBaseline+1,
		"restore_preserved_machine_id_live":  !restoreReceipt.OSIdentityRekeyed && restoreReceipt.MachineIDBeforeDigest == "" && restoreReceipt.MachineIDAfterDigest == "",
		"restore_first_attach_nonfresh_live": restoreReceipt.DestinationPrepared && !restoreReceipt.PreparationReceipt && !restoreReceipt.DestinationChown && computerDisplayPublished(reimaged),
		"restore_token_revocation_live":      restoreTokenRevoked,
		"prestop_session_lineage_rejected_after_restore_live": preStopSessionRejectedAfterRestore.Error.Code == contract.ErrorTakeoverSessionEnded &&
			preStopSessionRejectedAfterRestore.Receipt != nil && preStopSessionRejectedAfterRestore.Receipt.SessionEndReason == string(l1.ComputerTakeoverAttemptAuthorityLost),
		"backup_cap_pressure_live": capSet.Computer != nil && strings.Contains(capPressure, string(contract.ErrorConflict)),
		"real_disk_enospc_live":    enospc.Observed,
	}
	storageEvidence := map[string]string{"backup_id": backup.BackupID, "clone_computer_id": cloneOutput.Computer.ComputerID,
		"clone_machine_id_before": cloneReceipt.MachineIDBeforeDigest, "clone_machine_id_after": cloneReceipt.MachineIDAfterDigest,
		"export_id": exportOutput.CustodyExport.ExportID, "import_computer_id": importOutput.Computer.ComputerID,
		"retained_backups": fmt.Sprint(len(backupInventory.Backups)), "enospc_observation": enospc.Detail,
		"restore_old_endpoint": restoreOldEndpoint, "restore_current_endpoint": restoreCurrentEndpoint,
		"restore_admission_job_state":      string(restoreAdmissionState),
		"restore_token_revocation_receipt": restoreTokenRevocationDetail}
	completeLinuxComputerRow(t, receipt, "linux.storage_provenance", storageAssertions, storageEvidence)

	receipt.begin("linux.guest_authority")
	defaultOff := !reimaged.SubmitEnabled && reimaged.SubmitMaxInflight == l1.DefaultComputerSubmitMaxInflight
	submission := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "enable", reimaged.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-submission-enable")
	if !submission.MutationApplied || !submission.SubmitEnabled {
		t.Fatalf("Computer submission enable = %#v", submission)
	}
	selfResult := waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	var self l3.ComputerSelf
	if selfResult.Status != http.StatusOK || json.Unmarshal([]byte(selfResult.Body), &self) != nil {
		t.Fatalf("live Computer self status=%d body=%s", selfResult.Status, selfResult.Body)
	}
	limitOne := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "set-inflight", reimaged.ComputerID,
		"--max-inflight", "1", "--expect-current", "--idempotency-key", "linux-native-submission-inflight-one")
	_ = waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	inflight := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "set-inflight", reimaged.ComputerID,
		"--max-inflight", "20", "--expect-current", "--idempotency-key", "linux-native-submission-inflight")
	_ = waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	accepted := make([]l3.RunAccepted, 0, 20)
	for index := 0; index < 20; index++ {
		result := runLiveComputerHTTP(t, reimaged, http.MethodPost, "/v1/runs", fmt.Sprintf("linux-native-root-%02d", index), liveComputerRunRequest(300*time.Second))
		if result.Status != http.StatusCreated {
			t.Fatalf("live Computer root %d status=%d body=%s", index, result.Status, result.Body)
		}
		var run l3.RunAccepted
		if err := json.Unmarshal([]byte(result.Body), &run); err != nil || run.RunID == "" {
			t.Fatalf("decode live Computer root %d: %v body=%s", index, err, result.Body)
		}
		accepted = append(accepted, run)
	}
	rootResult := runLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/runs/"+accepted[0].RunID, "", nil)
	var root contract.RunRecord
	if rootResult.Status != http.StatusOK || json.Unmarshal([]byte(rootResult.Body), &root) != nil {
		t.Fatalf("live Computer root projection status=%d body=%s", rootResult.Status, rootResult.Body)
	}
	listResult := runLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/runs?origin=computer:self&limit=100", "", nil)
	var selfRuns l3.ComputerRunPage
	if listResult.Status != http.StatusOK || json.Unmarshal([]byte(listResult.Body), &selfRuns) != nil {
		t.Fatalf("live Computer self-scoped list status=%d body=%s", listResult.Status, listResult.Body)
	}
	forbidden := liveComputerHTTPResult{}
	if !mutatingLinuxComputerRow("linux.guest_authority") {
		forbidden = runLiveComputerHTTP(t, reimaged, http.MethodPost, "/v1/runs/"+accepted[0].RunID+"/cancel", "linux-native-forbidden", map[string]any{})
	}
	limited := runLiveComputerHTTP(t, reimaged, http.MethodPost, "/v1/runs", "linux-native-root-over-limit", liveComputerRunRequest(300*time.Second))
	var limitedError contract.ErrorResponse
	_ = json.Unmarshal([]byte(limited.Body), &limitedError)
	raceCapacity := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "set-inflight", reimaged.ComputerID,
		"--max-inflight", "21", "--expect-current", "--idempotency-key", "linux-native-submission-race-capacity")
	_ = waitForLiveComputerHTTP(t, reimaged, http.MethodGet, "/v1/computer/self", "", nil, 30*time.Second)
	authorityRunsBefore := listComputerRunsFromAuthority(t, harness, reimaged.ComputerID)
	paused := startLiveComputerPausedSubmission(t, reimaged, "linux-native-revocation-race", liveComputerRunRequest(300*time.Second))
	disabled := runComputerCLI[l1.ComputerSubmissionMutationResult](t, harness, true, "services", "submission", "disable", reimaged.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-submission-disable")
	pausedResult := paused.finish(t)
	authorityRunsAfter := listComputerRunsFromAuthority(t, harness, reimaged.ComputerID)
	var pausedError contract.ErrorResponse
	pausedErrorDecoded := json.Unmarshal([]byte(pausedResult.Body), &pausedError) == nil
	guestTypedOutcome := pausedResult.TransportError == "" && pausedErrorDecoded &&
		((pausedResult.Status == http.StatusUnauthorized && pausedError.Error.Code == contract.ErrorUnauthorized) ||
			(pausedResult.Status == http.StatusBadGateway && pausedError.Error.Code == contract.ErrorPassUnavailable))
	authorityRace := classifyComputerRevocationRaceAuthority(authorityRunsBefore.Runs, authorityRunsAfter.Runs, disabled.Revoked)
	guestAssertions := map[string]bool{
		"live_default_off":                    defaultOff,
		"live_submission_enabled":             submission.SubmitEnabled && submission.Revoked != nil,
		"live_self_scope_exact":               self.ComputerID == reimaged.ComputerID && self.ComputerStorageGeneration == reimaged.StorageGeneration && len(self.Permissions) == 2,
		"live_root_run_provenance":            root.Trigger.Type == "computer" && root.Trigger.ComputerID == reimaged.ComputerID && root.Trigger.ComputerStorageGeneration == reimaged.StorageGeneration,
		"live_self_scoped_list":               len(selfRuns.Runs) == 20,
		"live_forbidden_route_rejected":       forbidden.Status == http.StatusForbidden,
		"live_one_inflight_policy_transition": limitOne.SubmitMaxInflight == 1 && limitOne.Revoked != nil,
		"live_exact_inflight_policy_set":      inflight.SubmitMaxInflight == 20 && inflight.Revoked != nil,
		"live_twenty_inflight_boundary":       limited.Status == http.StatusConflict && limitedError.Error.Code == contract.ErrorSubmitInflightLimit,
		"live_revocation_race_capacity":       raceCapacity.SubmitMaxInflight == 21 && raceCapacity.Revoked != nil,
		"live_submission_revoked":             !disabled.SubmitEnabled && disabled.Revoked != nil,
		"live_revocation_revision_advanced":   disabled.SubmitIntentRevision > raceCapacity.SubmitIntentRevision,
		"live_revocation_race_closed":         guestTypedOutcome && authorityRace.Closed,
	}
	guestEvidence := map[string]string{"policy_revision": fmt.Sprint(submission.PolicyRevision),
		"submit_intent_revision": fmt.Sprint(submission.SubmitIntentRevision),
		"race_capacity_revision": fmt.Sprint(raceCapacity.SubmitIntentRevision),
		"root_run_id":            accepted[0].RunID,
		"revocation_race_result": fmt.Sprintf("guest_status=%d guest_code=%s transport_error=%s authority_outcome=%s authority_before=%d authority_after=%d race_run_id=%s race_run_created_at=%s revocation_committed_at=%s",
			pausedResult.Status, pausedError.Error.Code, pausedResult.TransportError, authorityRace.Outcome,
			len(authorityRunsBefore.Runs), len(authorityRunsAfter.Runs), authorityRace.RunID, authorityRace.RunCreatedAt, authorityRace.RevocationCommittedAt),
		"blocked_assertion": "candidate-bound complete M3 OCI matrix root Run execution result"}
	if mutatingLinuxComputerRow("linux.guest_authority") {
		if err := receipt.pass("linux.guest_authority", guestAssertions, guestEvidence); err == nil {
			t.Fatal("guest-authority lane mutation did not fail")
		}
	} else if err := receipt.notRun("linux.guest_authority", 157,
		"the complete M3 OCI matrix does not yet publish the single candidate-bound root Run execution result required to join this live Computer authority proof",
		guestAssertions, guestEvidence); err != nil {
		t.Fatalf("%v; revocation_race_result=%s", err, guestEvidence["revocation_race_result"])
	}

	receipt.begin("linux.removal")
	archiveBefore := sha256File(t, archive)
	cacheBefore := liveContainerdImagePresent(t, digest)
	taintedComputerID := importOutput.Computer.ComputerID
	reduced := removeAndWaitComputer(t, harness, *importOutput.Computer, 4*time.Minute)
	for _, target := range []l1.Computer{reimaged, *cloneOutput.Computer} {
		_ = removeAndWaitComputer(t, harness, target, 4*time.Minute)
	}
	verifiedComputer := createReadyComputer(t, harness, reference, digest, "linux-native-verified-removal", "linux-native-verified-removal-create")
	recordComputerAuthority(receipt, verifiedComputer)
	removalCommandIntact := true
	if mutatingLinuxComputerRow("linux.removal") {
		failedRemoval := runComputerCLIExpectError(t, harness, "", "", "services", "remove", verifiedComputer.ComputerID,
			"--intent-revision", fmt.Sprint(verifiedComputer.IntentRevision+99), "--storage-id", verifiedComputer.StorageID,
			"--storage-generation", fmt.Sprint(verifiedComputer.StorageGeneration), "--idempotency-key", "linux-native-removal-mutation")
		removalCommandIntact = !strings.Contains(failedRemoval, string(contract.ErrorStaleIntentRevision))
	}
	verified := removeAndWaitComputer(t, harness, verifiedComputer, 4*time.Minute)
	if verified.RemovalOutcome != "removed_verified" || reduced.RemovalOutcome != "removed_reduced" {
		t.Fatalf("Computer removal outcomes verified=%q reduced=%q", verified.RemovalOutcome, reduced.RemovalOutcome)
	}
	harness.agent.kill(t)
	verification := inspectHelperNamespaceInventory(t, helperSocket, helperChecksum)
	receipt.ResidueInventories["post_removal_observed_inventory"] = verification.Inventory
	receipt.ResidueInventories["post_removal_helper_namespace"] = verification.Inventory
	receipt.ResidueInventories["post_removal_runtime_residue"] = verification.RuntimeResidue
	receipt.ResidueInventories["post_removal_durable_retained"] = verification.DurableRetained
	observedExact, observedGCNoNew, observedGCRemoved := compareRemovalInventoryBaseline(removalBaseline.Inventory, verification.Inventory)
	retainedExact, retainedGCNoNew, _ := compareRemovalInventoryBaseline(removalBaseline.DurableRetained, verification.DurableRetained)
	receipt.ResidueInventories["post_removal_self_gc_removed"] = observedGCRemoved
	receipt.ResidueAssertions["post_removal_observed_inventory_restored"] = observedExact && observedGCNoNew
	receipt.ResidueAssertions["post_removal_runtime_residue_restored"] = reflect.DeepEqual(verification.RuntimeResidue, removalBaseline.RuntimeResidue)
	receipt.ResidueAssertions["post_removal_durable_retained_restored"] = retainedExact && retainedGCNoNew
	receipt.ResidueAssertions["post_removal_self_gc_no_new"] = observedGCNoNew && retainedGCNoNew
	archiveAfter := sha256File(t, archive)
	cacheAfter := liveContainerdImagePresent(t, digest)
	completeLinuxComputerRow(t, receipt, "linux.removal", map[string]bool{
		"verified_absence_outcome_live":         verified.RemovalOutcome == "removed_verified",
		"reduced_custody_outcome_live":          reduced.RemovalOutcome == "removed_reduced",
		"reduced_bound_to_tainted_computer":     reduced.ComputerID == taintedComputerID,
		"independent_helper_inventory_restored": observedExact && observedGCNoNew,
		"helper_self_gc_no_new":                 observedGCNoNew && retainedGCNoNew,
		"containers_restored":                   reflect.DeepEqual(verification.Inventory.Containers, removalBaseline.Inventory.Containers),
		"tasks_restored":                        reflect.DeepEqual(verification.Inventory.Tasks, removalBaseline.Inventory.Tasks),
		"disks_loops_mounts_restored": reflect.DeepEqual(verification.Inventory.ComputerDiskImages, removalBaseline.Inventory.ComputerDiskImages) &&
			reflect.DeepEqual(verification.Inventory.ComputerDiskLoops, removalBaseline.Inventory.ComputerDiskLoops) &&
			reflect.DeepEqual(verification.Inventory.ComputerDiskMounts, removalBaseline.Inventory.ComputerDiskMounts),
		"logs_and_control_restored": reflect.DeepEqual(verification.Inventory.LogSegments, removalBaseline.Inventory.LogSegments) &&
			reflect.DeepEqual(verification.Inventory.Cgroups, removalBaseline.Inventory.Cgroups),
		"durable_retained_restored":      retainedExact && retainedGCNoNew,
		"publication_withdrawn":          verified.DisplayEndpoint == nil,
		"operator_bind_source_untouched": archiveBefore == archiveAfter,
		"shared_image_cache_untouched":   cacheBefore && cacheAfter,
		"removal_command_intact":         removalCommandIntact,
	}, map[string]string{"verified_computer_id": verified.ComputerID, "reduced_computer_id": reduced.ComputerID,
		"custody_tainted_computer_id": taintedComputerID, "verified_outcome": verified.RemovalOutcome,
		"reduced_outcome": reduced.RemovalOutcome, "inventory_source": "helper VerifyNamespaceReadOnly route",
		"self_gc_classes":                 "managed_volumes:wefty-handoff-volume-,image_spools",
		"self_gc_removed_managed_volumes": strings.Join(observedGCRemoved.ManagedVolumes, ","),
		"self_gc_removed_image_spools":    strings.Join(observedGCRemoved.ImageSpools, ",")})
	validateLinuxComputerLaneMutation(t, receipt)
}

type publishedComputerRuntimeReceipt struct {
	Digest string `json:"digest"`
}

func mergeAcceptanceInventory(left, right ocihelper.ResourceInventory) ocihelper.ResourceInventory {
	return ocihelper.ResourceInventory{
		Leases: append(left.Leases, right.Leases...), Snapshots: append(left.Snapshots, right.Snapshots...),
		Containers: append(left.Containers, right.Containers...), Tasks: append(left.Tasks, right.Tasks...),
		Shims: append(left.Shims, right.Shims...), Cgroups: append(left.Cgroups, right.Cgroups...),
		LogSegments: append(left.LogSegments, right.LogSegments...), ManagedVolumes: append(left.ManagedVolumes, right.ManagedVolumes...),
		ImageSpools:                append(left.ImageSpools, right.ImageSpools...),
		ManagedVolumeRecords:       append(left.ManagedVolumeRecords, right.ManagedVolumeRecords...),
		ComputerDiskImages:         append(left.ComputerDiskImages, right.ComputerDiskImages...),
		ComputerDiskAllocations:    append(left.ComputerDiskAllocations, right.ComputerDiskAllocations...),
		ComputerDiskQuotas:         append(left.ComputerDiskQuotas, right.ComputerDiskQuotas...),
		ComputerDiskManifests:      append(left.ComputerDiskManifests, right.ComputerDiskManifests...),
		ComputerDiskMounts:         append(left.ComputerDiskMounts, right.ComputerDiskMounts...),
		ComputerDiskLoops:          append(left.ComputerDiskLoops, right.ComputerDiskLoops...),
		ComputerAttachments:        append(left.ComputerAttachments, right.ComputerAttachments...),
		ComputerResetManifests:     append(left.ComputerResetManifests, right.ComputerResetManifests...),
		ComputerQuarantines:        append(left.ComputerQuarantines, right.ComputerQuarantines...),
		ComputerStorageDeferred:    append(left.ComputerStorageDeferred, right.ComputerStorageDeferred...),
		ComputerStorageQuarantined: append(left.ComputerStorageQuarantined, right.ComputerStorageQuarantined...),
		ComputerDiskAnomalies:      append(left.ComputerDiskAnomalies, right.ComputerDiskAnomalies...),
	}
}

// compareRemovalInventoryBaseline keeps the post-removal gate exact for every
// resource class except the two explicitly self-GC classes. Those may shrink
// while the matrix is running, but may never gain an ID relative to baseline.
func compareRemovalInventoryBaseline(before, after ocihelper.ResourceInventory) (exactNonGC, noNewSelfGC bool, removed ocihelper.ResourceInventory) {
	beforeNonGC, beforeHandoffs := inventoryWithoutSelfGC(before)
	afterNonGC, afterHandoffs := inventoryWithoutSelfGC(after)
	return reflect.DeepEqual(beforeNonGC, afterNonGC),
		stringSetSubset(afterHandoffs, beforeHandoffs) && stringSetSubset(after.ImageSpools, before.ImageSpools),
		ocihelper.ResourceInventory{
			ManagedVolumes: stringSetDifference(beforeHandoffs, afterHandoffs),
			ImageSpools:    stringSetDifference(before.ImageSpools, after.ImageSpools),
		}
}

func inventoryWithoutSelfGC(in ocihelper.ResourceInventory) (ocihelper.ResourceInventory, []string) {
	out := in
	handoffs := make([]string, 0, len(in.ManagedVolumes))
	managed := make([]string, 0, len(in.ManagedVolumes))
	for _, id := range in.ManagedVolumes {
		if strings.HasPrefix(id, "wefty-handoff-volume-") {
			handoffs = append(handoffs, id)
			continue
		}
		managed = append(managed, id)
	}
	if len(managed) == 0 {
		managed = nil
	}
	out.ManagedVolumes = managed
	out.ImageSpools = nil
	return out, handoffs
}

func stringSetSubset(candidate, baseline []string) bool {
	allowed := make(map[string]struct{}, len(baseline))
	for _, id := range baseline {
		allowed[id] = struct{}{}
	}
	for _, id := range candidate {
		if _, ok := allowed[id]; !ok {
			return false
		}
	}
	return true
}

func stringSetDifference(before, after []string) []string {
	retained := make(map[string]struct{}, len(after))
	for _, id := range after {
		retained[id] = struct{}{}
	}
	var removed []string
	for _, id := range before {
		if _, ok := retained[id]; !ok {
			removed = append(removed, id)
		}
	}
	return removed
}

type liveComputerEndpointEnvironment struct {
	ViewPort    int    `json:"view_port"`
	ControlPort int    `json:"control_port"`
	Address     string `json:"address"`
	Gateway     string `json:"gateway"`
}

type computerNetworkEgressReceipt struct {
	Version                int                    `json:"version"`
	ComputerID             string                 `json:"computer_id"`
	Address                string                 `json:"address"`
	Gateway                string                 `json:"gateway"`
	ResolverSnapshot       string                 `json:"resolver_snapshot"`
	ResolverAddress        string                 `json:"resolver_address"`
	ProxyUDPListening      bool                   `json:"proxy_udp_listening"`
	ProxyTCPListening      bool                   `json:"proxy_tcp_listening"`
	ProxyUpstreamAddress   string                 `json:"proxy_upstream_address"`
	ProxyUpstreamSource    string                 `json:"proxy_upstream_source"`
	ProxyUpstreamReachable bool                   `json:"proxy_upstream_reachable"`
	DefaultRouteInterface  string                 `json:"default_route_interface"`
	DefaultRouteGateway    string                 `json:"default_route_gateway"`
	PublicIPv4             screenCrossoverAttempt `json:"public_ipv4"`
	DNSOutcome             string                 `json:"dns_outcome"`
	ResolvedName           string                 `json:"resolved_name"`
	ResolvedAddress        string                 `json:"resolved_address"`
	HelperHTTPStatus       int                    `json:"helper_http_status"`
	HelperHTTPBody         string                 `json:"helper_http_body"`
	NodeListenerIPv4       screenCrossoverAttempt `json:"node_listener_ipv4"`
	NodeListenerIPv6       screenCrossoverAttempt `json:"node_listener_ipv6"`
}

type screenCrossoverAttempt struct {
	Address   string `json:"address"`
	Outcome   string `json:"outcome"`
	Class     string `json:"class,omitempty"`
	Errno     int    `json:"errno,omitempty"`
	ErrnoName string `json:"errno_name,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

type screenCrossoverReceipt struct {
	Version               int                    `json:"version"`
	Variant               string                 `json:"variant"`
	SourceComputerID      string                 `json:"source_computer_id"`
	TargetComputerID      string                 `json:"target_computer_id"`
	AbstractSocketName    string                 `json:"abstract_socket_name"`
	AbstractSocketVisible bool                   `json:"abstract_socket_visible"`
	AbstractSocket        screenCrossoverAttempt `json:"abstract_socket"`
	DerivedDisplay        screenCrossoverAttempt `json:"derived_display"`
	ViewRead              screenCrossoverAttempt `json:"view_read"`
	ControlInject         screenCrossoverAttempt `json:"control_inject"`
	EgressAddress         screenCrossoverAttempt `json:"egress_address"`
	NodeListenerIPv6      screenCrossoverAttempt `json:"node_listener_ipv6"`
}

func startDualStackNodeBoundaryListener(t *testing.T) (int, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "[::]:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			_, _ = connection.Write([]byte("wefty-node-boundary-live\n"))
			_ = connection.Close()
		}
	}()
	port := listener.Addr().(*net.TCPAddr).Port
	for _, target := range []struct{ network, address string }{
		{"tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(port))},
		{"tcp6", net.JoinHostPort("::1", fmt.Sprint(port))},
	} {
		connection, dialErr := net.DialTimeout(target.network, target.address, time.Second)
		if dialErr != nil {
			_ = listener.Close()
			t.Fatalf("Node boundary listener is not live on %s %s: %v", target.network, target.address, dialErr)
		}
		_ = connection.Close()
	}
	return port, func() { _ = listener.Close() }
}

func readComputerHostLinkIPv6(t *testing.T, viewPort int) string {
	t.Helper()
	link := fmt.Sprintf("wftch%d", viewPort)
	output, err := exec.Command("sudo", "/usr/sbin/ip", "-6", "-o", "addr", "show", "dev", link, "scope", "link").CombinedOutput()
	if err != nil {
		t.Fatalf("read Computer host veth IPv6 address: %v\n%s", err, output)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		fields := strings.Fields(line)
		for index, field := range fields {
			if field == "inet6" && index+1 < len(fields) {
				address, _, _ := strings.Cut(fields[index+1], "/")
				if strings.HasPrefix(address, "fe80:") {
					return address
				}
			}
		}
	}
	t.Fatalf("Computer host link %s has no link-local IPv6 address", link)
	return ""
}

const liveComputerEndpointEnvironmentPython = `
import json, socket, struct
values = {}
for item in open("/proc/1/environ", "rb").read().split(b"\0"):
    if b"=" in item:
        key, value = item.split(b"=", 1)
        values[key.decode()] = value.decode()
gateway = ""
for line in open("/proc/net/route", encoding="utf-8").read().splitlines()[1:]:
    fields = line.split()
    if len(fields) >= 3 and fields[1] == "00000000":
        gateway = socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
        break
probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
probe.connect((gateway, int(values["WEFTY_COMPUTER_VIEW_PORT"])))
address = probe.getsockname()[0]
probe.close()
print(json.dumps({
    "view_port": int(values["WEFTY_COMPUTER_VIEW_PORT"]),
    "control_port": int(values["WEFTY_COMPUTER_CONTROL_PORT"]),
    "address": address,
    "gateway": gateway,
}))
`

const liveComputerNetworkEgressPython = `
import errno, http.client, json, socket, struct, sys
computer_id, address, gateway, view_text, node_ipv6, node_port_text = sys.argv[1:7]
view_port, node_port = int(view_text), int(node_port_text)

def refused(target, error):
    number = error.errno or 0
    return {"address": target, "outcome": "refused", "errno": number,
            "errno_name": errno.errorcode.get(number, type(error).__name__), "detail": str(error)}

def attempted_connect(host, port):
    target = host + ":" + str(port)
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connection.settimeout(5)
        connection.connect((host, port))
        return {"address": target, "outcome": "connected"}
    except OSError as error:
        return refused(target, error)
    finally:
        connection.close()

def socket_present(path, host, port, tcp):
    encoded = "%08X:%04X" % (struct.unpack("<I", socket.inet_aton(host))[0], port)
    with open(path, "r", encoding="ascii") as table:
        for line in table.readlines()[1:]:
            fields = line.split()
            if len(fields) > 3 and fields[1] == encoded and (not tcp or fields[3] == "0A"):
                return True
    return False

with open("/etc/resolv.conf", "r", encoding="utf-8") as resolver_file:
    resolver_snapshot = resolver_file.read()
resolver_address = ""
for line in resolver_snapshot.splitlines():
    fields = line.split()
    if len(fields) >= 2 and fields[0] == "nameserver":
        try:
            if socket.inet_aton(fields[1]):
                resolver_address = fields[1]
                break
        except OSError:
            pass
proxy_udp = bool(resolver_address) and socket_present("/proc/net/udp", resolver_address, 53, False)
proxy_tcp = bool(resolver_address) and socket_present("/proc/net/tcp", resolver_address, 53, True)

route_interface, route_gateway = "", ""
with open("/proc/net/route", "r", encoding="ascii") as route_file:
    for line in route_file.readlines()[1:]:
        fields = line.split()
        if len(fields) >= 3 and fields[1] == "00000000":
            route_interface = fields[0]
            route_gateway = socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
            break

helper = http.client.HTTPConnection(gateway, view_port, timeout=5)
helper.request("GET", "/health")
response = helper.getresponse()
helper_status = response.status
helper_body = response.read().decode().strip()
helper.close()
public_ipv4 = attempted_connect("1.1.1.1", 443)

dns_outcome, resolved = "egress_dns_unavailable", ""
try:
    resolved = socket.getaddrinfo("example.com", 443, family=socket.AF_INET, type=socket.SOCK_STREAM)[0][4][0]
    dns_outcome = "resolved"
except socket.gaierror:
    pass
node_ipv4_address = gateway + ":" + str(node_port)
connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    connection.settimeout(5)
    connection.connect((gateway, node_port))
    node_ipv4 = {"address": node_ipv4_address, "outcome": "connected"}
except OSError as error:
    node_ipv4 = refused(node_ipv4_address, error)
finally:
    connection.close()
node_ipv6_address = "[" + node_ipv6 + "%eth0]:" + str(node_port)
connection = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
try:
    connection.settimeout(5)
    connection.connect((node_ipv6, node_port, 0, socket.if_nametoindex("eth0")))
    node_ipv6_result = {"address": node_ipv6_address, "outcome": "connected"}
except OSError as error:
    node_ipv6_result = refused(node_ipv6_address, error)
finally:
    connection.close()
print(json.dumps({
    "version": 2,
    "computer_id": computer_id,
    "address": address,
    "gateway": gateway,
    "resolver_snapshot": resolver_snapshot,
    "resolver_address": resolver_address,
    "proxy_udp_listening": proxy_udp,
    "proxy_tcp_listening": proxy_tcp,
    "default_route_interface": route_interface,
    "default_route_gateway": route_gateway,
    "public_ipv4": public_ipv4,
    "dns_outcome": dns_outcome,
    "resolved_name": "example.com",
    "resolved_address": resolved,
    "helper_http_status": helper_status,
    "helper_http_body": helper_body,
    "node_listener_ipv4": node_ipv4,
    "node_listener_ipv6": node_ipv6_result,
}, sort_keys=True))
`

func probeComputerNetworkEgress(t *testing.T, computer l1.Computer, endpoints liveComputerEndpointEnvironment, nodeIPv6 string, nodePort int) computerNetworkEgressReceipt {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("network-egress-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerNetworkEgressPython,
		computer.ComputerID, endpoints.Address, endpoints.Gateway, fmt.Sprint(endpoints.ViewPort), nodeIPv6, fmt.Sprint(nodePort)).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Computer network egress probe: %v\n%s", err, output)
	}
	var receipt computerNetworkEgressReceipt
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	if len(lines) == 0 || json.Unmarshal(lines[len(lines)-1], &receipt) != nil || receipt.Version != 2 || receipt.ComputerID != computer.ComputerID {
		t.Fatalf("decode Computer network egress receipt: %s", output)
	}
	resolverIP := net.ParseIP(receipt.ResolverAddress)
	if resolverIP != nil && resolverIP.IsLoopback() {
		upstream, upstreamErr := ocihelper.ObserveComputerDNSUpstream(t.Context(), receipt.ResolverAddress)
		if upstreamErr == nil {
			receipt.ProxyUpstreamAddress = upstream.Address
			receipt.ProxyUpstreamSource = upstream.Source
			receipt.ProxyUpstreamReachable = upstream.Reachable
		}
	}
	t.Logf("Computer egress computer=%s address=%s gateway=%s route=%s/%s resolver=%s proxy_udp=%t proxy_tcp=%t upstream=%s source=%s reachable=%t dns=%s resolved=%s public=%s helper_status=%d node_v4=%s errno=%s node_v6=%s errno=%s",
		receipt.ComputerID, receipt.Address, receipt.Gateway, receipt.DefaultRouteInterface, receipt.DefaultRouteGateway,
		receipt.ResolverAddress, receipt.ProxyUDPListening, receipt.ProxyTCPListening, receipt.ProxyUpstreamAddress,
		receipt.ProxyUpstreamSource, receipt.ProxyUpstreamReachable, receipt.DNSOutcome, receipt.ResolvedAddress, receipt.PublicIPv4.Outcome, receipt.HelperHTTPStatus,
		receipt.NodeListenerIPv4.Outcome, receipt.NodeListenerIPv4.ErrnoName, receipt.NodeListenerIPv6.Outcome, receipt.NodeListenerIPv6.ErrnoName)
	return receipt
}

const liveComputerScreenCrossoverPython = `
import base64, errno, json, os, socket, struct, subprocess, sys

variant, source_id, target_id, view_text, control_text, egress_address, egress_port_text, node_ipv6, node_port_text = sys.argv[1:10]
view_port, control_port, egress_port, node_port = int(view_text), int(control_text), int(egress_port_text), int(node_port_text)
target_host = "127.0.0.1" if source_id == target_id else egress_address

def refused(address, error):
    number = error.errno or 0
    return {"address": address, "outcome": "refused", "errno": number,
            "errno_name": errno.errorcode.get(number, type(error).__name__), "detail": str(error)}

def recv_exact(connection, count):
    result = b""
    while len(result) < count:
        chunk = connection.recv(count - len(result))
        if not chunk:
            raise EOFError("peer closed the WebSocket stream")
        result += chunk
    return result

class websocket_stream:
    def __init__(self, connection):
        self.connection = connection
        self.buffer = b""

    def send(self, payload):
        mask = os.urandom(4)
        length = len(payload)
        if length < 126:
            header = bytes([0x82, 0x80 | length])
        elif length < 65536:
            header = bytes([0x82, 0x80 | 126]) + struct.pack("!H", length)
        else:
            header = bytes([0x82, 0x80 | 127]) + struct.pack("!Q", length)
        masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        self.connection.sendall(header + mask + masked)

    def frame(self):
        first, second = recv_exact(self.connection, 2)
        opcode, length = first & 0x0f, second & 0x7f
        if length == 126:
            length = struct.unpack("!H", recv_exact(self.connection, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", recv_exact(self.connection, 8))[0]
        if second & 0x80:
            mask = recv_exact(self.connection, 4)
            payload = recv_exact(self.connection, length)
            payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
        else:
            payload = recv_exact(self.connection, length)
        if opcode == 0x8:
            raise EOFError("peer closed the WebSocket")
        if opcode == 0x9:
            self.send(payload)
            return self.frame()
        return payload

    def read(self, count):
        while len(self.buffer) < count:
            self.buffer += self.frame()
        result, self.buffer = self.buffer[:count], self.buffer[count:]
        return result

def rfb_attempt(port, inject):
    address = target_host + ":" + str(port)
    connection = None
    try:
        connection = socket.create_connection((target_host, port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        request = ("GET /websockify HTTP/1.1\r\nHost: " + address +
                   "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
                   "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key +
                   "\r\nSec-WebSocket-Protocol: binary\r\n\r\n").encode()
        connection.sendall(request)
        response = b""
        while b"\r\n\r\n" not in response:
            response += connection.recv(1)
            if len(response) > 16384:
                raise ValueError("oversized WebSocket response")
        if not response.startswith(b"HTTP/1.1 101"):
            raise RuntimeError("WebSocket upgrade refused: " + response.decode(errors="replace"))
        stream = websocket_stream(connection)
        if not stream.read(12).startswith(b"RFB 003."):
            raise RuntimeError("RFB banner missing")
        stream.send(b"RFB 003.008\n")
        count = stream.read(1)[0]
        security = stream.read(count)
        if 1 not in security:
            raise RuntimeError("RFB None security unavailable")
        stream.send(b"\x01")
        if stream.read(4) != b"\x00\x00\x00\x00":
            raise RuntimeError("RFB security failed")
        stream.send(b"\x01")
        server_init = stream.read(24)
        stream.read(struct.unpack("!I", server_init[20:24])[0])
        if inject:
            stream.send(b"\x05\x01\x00\x11\x00\x13")
            stream.send(b"\x05\x00\x00\x11\x00\x13")
            outcome = "inject_succeeded"
        else:
            outcome = "read_succeeded"
        return {"address": address, "outcome": outcome}
    except OSError as error:
        return refused(address, error)
    except Exception as error:
        return {"address": address, "outcome": "protocol_error", "detail": type(error).__name__ + ":" + str(error)}
    finally:
        if connection is not None:
            connection.close()

abstract_name = "@/tmp/.X11-unix/X" + str(view_port)
abstract_visible = False
abstract_attempt = {"address": abstract_name, "outcome": "not_applicable"}
display_attempt = {"address": ":" + str(view_port), "outcome": "not_applicable"}
if variant == "xfce":
    abstract_visible = any(len(line.split()) >= 8 and line.split()[-1] == abstract_name
                           for line in open("/proc/net/unix", encoding="utf-8"))
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.settimeout(5)
        connection.connect("\0/tmp/.X11-unix/X" + str(view_port))
        abstract_attempt = {"address": abstract_name, "outcome": "connected"}
    except OSError as error:
        abstract_attempt = refused(abstract_name, error)
    finally:
        connection.close()
    display = subprocess.run(["/usr/bin/xdpyinfo", "-display", ":" + str(view_port)],
                             text=True, capture_output=True, timeout=5)
    detail = display.stderr.strip()[-512:]
    if display.returncode == 0:
        display_attempt = {"address": ":" + str(view_port), "outcome": "read_succeeded"}
    elif "authorization required" in detail.lower() or "no protocol specified" in detail.lower():
        display_attempt = {"address": ":" + str(view_port), "outcome": "auth_refused", "class": "x_auth", "detail": detail}
    elif "unable to open display" in detail.lower():
        display_attempt = {"address": ":" + str(view_port), "outcome": "transport_refused", "class": "x_transport", "detail": detail}
    else:
        display_attempt = {"address": ":" + str(view_port), "outcome": "other_error", "class": "x_other", "detail": detail}

def tcp_attempt(host, port):
    address = host + ":" + str(port)
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        connection.settimeout(5)
        connection.connect((host, port))
        return {"address": address, "outcome": "connected"}
    except OSError as error:
        return refused(address, error)
    finally:
        connection.close()

def tcp6_attempt(host, port):
    address = "[" + host + "%eth0]:" + str(port)
    connection = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    try:
        connection.settimeout(5)
        connection.connect((host, port, 0, socket.if_nametoindex("eth0")))
        return {"address": address, "outcome": "connected"}
    except OSError as error:
        return refused(address, error)
    finally:
        connection.close()

print(json.dumps({
    "version": 1,
    "variant": variant,
    "source_computer_id": source_id,
    "target_computer_id": target_id,
    "abstract_socket_name": abstract_name,
    "abstract_socket_visible": abstract_visible,
    "abstract_socket": abstract_attempt,
    "derived_display": display_attempt,
    "view_read": rfb_attempt(view_port, False),
    "control_inject": rfb_attempt(control_port, True),
    "egress_address": tcp_attempt(egress_address, egress_port),
    "node_listener_ipv6": tcp6_attempt(node_ipv6, node_port),
}, sort_keys=True))
`

const liveComputerEgressListenerPort = 43999

const liveComputerEgressListenerPython = `
import socket, sys
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind((sys.argv[1], int(sys.argv[2])))
listener.listen(8)
while True:
    connection, _ = listener.accept()
    connection.sendall(b"wefty-crossover-live\n")
    connection.close()
`

const liveComputerTCPProbePython = `
import socket, sys, time
deadline = time.time() + 10
last = None
while time.time() < deadline:
    try:
        connection = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=1)
        payload = connection.recv(64)
        connection.close()
        if payload == b"wefty-crossover-live\n":
            print("live")
            sys.exit(0)
    except OSError as error:
        last = error
    time.sleep(0.1)
raise SystemExit("listener unavailable: " + str(last))
`

func startLiveComputerEgressListener(t *testing.T, computer l1.Computer, address string) func() {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("egress-listener-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--detach", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerEgressListenerPython,
		address, fmt.Sprint(liveComputerEgressListenerPort)).CombinedOutput()
	if err != nil {
		t.Fatalf("start Computer egress crossover listener: %v\n%s", err, output)
	}
	return func() {
		_, _ = exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
			"tasks", "kill", "--exec-id", execID, "--signal", "SIGKILL", containerID).CombinedOutput()
	}
}

func probeLiveComputerEgressListener(t *testing.T, computer l1.Computer, address string) bool {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("egress-liveness-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerTCPProbePython,
		address, fmt.Sprint(liveComputerEgressListenerPort)).CombinedOutput()
	if err != nil {
		t.Fatalf("probe Computer egress crossover listener locally: %v\n%s", err, output)
	}
	return strings.TrimSpace(string(output)) == "live"
}

func readLiveComputerEndpointEnvironment(t *testing.T, computer l1.Computer) liveComputerEndpointEnvironment {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("computer-endpoints-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerEndpointEnvironmentPython).CombinedOutput()
	if err != nil {
		t.Fatalf("read live Computer endpoint environment: %v\n%s", err, output)
	}
	var environment liveComputerEndpointEnvironment
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	if len(lines) == 0 || json.Unmarshal(lines[len(lines)-1], &environment) != nil || environment.ViewPort <= 0 || environment.ControlPort <= 0 || environment.ViewPort == environment.ControlPort || environment.Address == "" || environment.Gateway == "" || environment.Address == environment.Gateway {
		t.Fatalf("decode live Computer endpoint environment: %s", output)
	}
	return environment
}

func probeComputerScreenCrossover(t *testing.T, source, target l1.Computer, variant string, endpoints liveComputerEndpointEnvironment, nodeIPv6 string, nodePort int) screenCrossoverReceipt {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, source.CurrentJobID)
	execID := fmt.Sprintf("screen-crossover-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerScreenCrossoverPython,
		variant, source.ComputerID, target.ComputerID, fmt.Sprint(endpoints.ViewPort), fmt.Sprint(endpoints.ControlPort), endpoints.Address, fmt.Sprint(liveComputerEgressListenerPort), nodeIPv6, fmt.Sprint(nodePort)).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Computer screen crossover probe: %v\n%s", err, output)
	}
	var receipt screenCrossoverReceipt
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	if len(lines) == 0 || json.Unmarshal(lines[len(lines)-1], &receipt) != nil || receipt.Version != 1 || receipt.SourceComputerID != source.ComputerID || receipt.TargetComputerID != target.ComputerID {
		t.Fatalf("decode Computer screen crossover receipt: %s", output)
	}
	t.Logf("screen crossover source=%s target=%s abstract=%s errno=%s display=%s class=%s view=%s errno=%s control=%s errno=%s egress=%s errno=%s",
		receipt.SourceComputerID, receipt.TargetComputerID, receipt.AbstractSocket.Outcome, receipt.AbstractSocket.ErrnoName,
		receipt.DerivedDisplay.Outcome, receipt.DerivedDisplay.Class, receipt.ViewRead.Outcome, receipt.ViewRead.ErrnoName,
		receipt.ControlInject.Outcome, receipt.ControlInject.ErrnoName, receipt.EgressAddress.Outcome, receipt.EgressAddress.ErrnoName)
	return receipt
}

func TestScreenCrossoverProbeRecordsTypedTransportRefusal(t *testing.T) {
	ports := make([]int, 0, 2)
	for range 2 {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
	}
	output, err := exec.Command("python3", "-c", liveComputerScreenCrossoverPython,
		"wayland", "source", "target", fmt.Sprint(ports[0]), fmt.Sprint(ports[1]), "127.0.0.1", fmt.Sprint(ports[0]), "::1", "1").CombinedOutput()
	if err != nil {
		t.Fatalf("execute crossover probe contract: %v\n%s", err, output)
	}
	var receipt screenCrossoverReceipt
	if err := json.Unmarshal(bytes.TrimSpace(output), &receipt); err != nil {
		t.Fatalf("decode crossover probe contract: %v\n%s", err, output)
	}
	if receipt.ViewRead.Outcome != "refused" || receipt.ViewRead.ErrnoName != "ECONNREFUSED" ||
		receipt.ControlInject.Outcome != "refused" || receipt.ControlInject.ErrnoName != "ECONNREFUSED" ||
		receipt.EgressAddress.Outcome != "refused" || receipt.EgressAddress.ErrnoName != "ECONNREFUSED" ||
		receipt.AbstractSocket.Outcome != "not_applicable" || receipt.DerivedDisplay.Outcome != "not_applicable" {
		t.Fatalf("typed crossover refusal = %+v", receipt)
	}
}

func TestCompareRemovalInventoryBaselineAllowsOnlySelfGCContraction(t *testing.T) {
	baseline := ocihelper.ResourceInventory{
		Containers:  []string{"preexisting-container"},
		ImageSpools: []string{"expired-import", "retained-import"},
		ManagedVolumes: []string{
			"wefty-handoff-volume-expired",
			"wefty-handoff-volume-retained",
			"wefty-service-volume-retained",
		},
	}
	after := ocihelper.ResourceInventory{
		Containers:     []string{"preexisting-container"},
		ImageSpools:    []string{"retained-import"},
		ManagedVolumes: []string{"wefty-handoff-volume-retained", "wefty-service-volume-retained"},
	}
	exact, noNew, removed := compareRemovalInventoryBaseline(baseline, after)
	if !exact || !noNew || !reflect.DeepEqual(removed.ImageSpools, []string{"expired-import"}) ||
		!reflect.DeepEqual(removed.ManagedVolumes, []string{"wefty-handoff-volume-expired"}) {
		t.Fatalf("self-GC contraction = exact:%t no_new:%t removed:%+v", exact, noNew, removed)
	}
	after.ImageSpools = append(after.ImageSpools, "new-import")
	if exact, noNew, _ := compareRemovalInventoryBaseline(baseline, after); !exact || noNew {
		t.Fatalf("new self-GC identity = exact:%t no_new:%t", exact, noNew)
	}
	after.ImageSpools = []string{"retained-import"}
	after.Containers = []string{"replacement-container"}
	if exact, _, _ := compareRemovalInventoryBaseline(baseline, after); exact {
		t.Fatal("non-GC identity change was accepted")
	}
}

func readPublishedComputerRuntimeReceipt(t *testing.T, path string) publishedComputerRuntimeReceipt {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var receipt publishedComputerRuntimeReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Digest == "" {
		t.Fatal("published Computer runtime receipt omitted its platform digest")
	}
	return receipt
}

type storageCLIMutationReceipt struct {
	MutationApplied   bool                          `json:"mutation_applied"`
	Computer          *l1.Computer                  `json:"computer,omitempty"`
	Backups           *l1.BackupList                `json:"backups,omitempty"`
	CustodyExport     *l1.ComputerCustodyExport     `json:"custody_export,omitempty"`
	StorageProvenance *l1.ComputerStorageProvenance `json:"storage_provenance,omitempty"`
}

func requiredStorageCopyReceipt(t *testing.T, projection *l1.ComputerStorageProvenance, kind, destinationComputerID string) contract.ComputerStorageCopyReceipt {
	t.Helper()
	if projection != nil {
		for _, provenance := range projection.Provenance {
			if provenance.Kind == kind && provenance.DestinationComputerID == destinationComputerID && provenance.CopyReceipt != nil {
				return *provenance.CopyReceipt
			}
		}
	}
	t.Fatalf("%s provenance omitted copy receipt for %s: %#v", kind, destinationComputerID, projection)
	return contract.ComputerStorageCopyReceipt{}
}

type computerBackupInventoryReceipt struct {
	Backups []l1.Backup `json:"backups"`
}

func requiredComputerRealtimeEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("Linux Computer acceptance requires %s", name)
	}
	return value
}

func completeLinuxComputerRow(t *testing.T, receipt *linuxComputerMatrixReceipt, id string, assertions map[string]bool, evidence map[string]string) {
	t.Helper()
	mutated := mutatingLinuxComputerRow(id)
	err := receipt.pass(id, assertions, evidence)
	if mutated {
		if err == nil || receipt.Rows[id].Status != "FAIL" {
			t.Fatalf("lane mutation %s did not fail its owning row", id)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
}

func mutatingLinuxComputerRow(id string) bool {
	return os.Getenv("WEFTY_LINUX_COMPUTER_MUTATE_ROW") == id
}

func validateLinuxComputerLaneMutation(t *testing.T, receipt *linuxComputerMatrixReceipt) {
	t.Helper()
	mutated := os.Getenv("WEFTY_LINUX_COMPUTER_MUTATE_ROW")
	if mutated == "" {
		return
	}
	failures := 0
	for id, row := range receipt.Rows {
		if row.Status == "FAIL" {
			failures++
			if id != mutated {
				t.Fatalf("lane mutation %s also failed row %s", mutated, id)
			}
		}
	}
	if failures != 1 {
		t.Fatalf("lane mutation %s failed %d rows, want exactly one", mutated, failures)
	}
}

func runComputerCLIPerson[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	return runComputerCLIWithIdentity[T](t, harness, userID, deviceID, arguments...)
}

type computerCLIEvidence struct {
	Arguments []string `json:"arguments"`
	Stdout    string   `json:"stdout"`
	Stderr    string   `json:"stderr"`
	ExitCode  int      `json:"exit_code"`
}

func runComputerCLIPersonWithEvidence[T any](t *testing.T, evidence *realTimingEvidence, evidenceName string, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	return runComputerCLIWithIdentityObserved[T](t, harness, userID, deviceID, func(observation computerCLIEvidence) {
		evidence.recordJSON(evidenceName, observation)
	}, arguments...)
}

func runComputerCLIWithIdentity[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) T {
	t.Helper()
	return runComputerCLIWithIdentityObserved[T](t, harness, userID, deviceID, nil, arguments...)
}

func runComputerCLIWithIdentityObserved[T any](t *testing.T, harness *acceptanceHarness, userID, deviceID string, observe func(computerCLIEvidence), arguments ...string) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID, arguments...)...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if observe != nil {
		exitCode := 0
		if err != nil {
			exitCode = -1
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				exitCode = exitErr.ExitCode()
			}
		}
		observe(computerCLIEvidence{Arguments: append([]string(nil), arguments...), Stdout: stdout.String(), Stderr: stderr.String(), ExitCode: exitCode})
	}
	if err != nil {
		t.Fatalf("run Computer CLI %v: %v\nstdout:\n%s\nstderr:\n%s", arguments, err, stdout.String(), stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("run Computer CLI %v emitted unexpected stderr:\n%s", arguments, stderr.String())
	}
	var result T
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode Computer CLI %v: %v\n%s", arguments, err, stdout.String())
	}
	return result
}

func computerCLIArguments(harness *acceptanceHarness, userID, deviceID string, arguments ...string) []string {
	args := []string{"--fabric=plain", "--l1=" + harness.controlPlaneAddress, "--plain-identity=linux-computer-cli", "--json"}
	if harness.runLedgerAddress != "" {
		args = append(args, "--l3="+harness.runLedgerAddress)
	}
	if userID != "" || deviceID != "" {
		args = append(args, "--plain-user-id="+userID, "--plain-device-id="+deviceID)
	}
	return append(args, arguments...)
}

func runComputerCLIExpectError(t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) string {
	t.Helper()
	return runComputerCLIPersonExpectError(t, harness, userID, deviceID, arguments...)
}

func runComputerCLIPersonExpectError(t *testing.T, harness *acceptanceHarness, userID, deviceID string, arguments ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID, arguments...)...)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("Computer CLI %v unexpectedly succeeded:\n%s", arguments, output)
	}
	return string(output)
}

func runComputerCLIPersonExpectControlError(
	t *testing.T,
	harness *acceptanceHarness,
	userID, deviceID string,
	arguments ...string,
) contract.ComputerControlErrorResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID, arguments...)...)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("Computer CLI %v unexpectedly succeeded:\n%s", arguments, output)
	}
	var response contract.ComputerControlErrorResponse
	if decodeErr := json.Unmarshal(output, &response); decodeErr != nil || response.Error.Code == "" {
		t.Fatalf("decode Computer control error %v: %v\n%s", arguments, decodeErr, output)
	}
	return response
}

func retargetTakeoverSessionCapability(t *testing.T, path string, endpoint *string) (string, string) {
	t.Helper()
	if endpoint == nil || strings.TrimSpace(*endpoint) == "" {
		t.Fatal("Computer restart omitted its current display endpoint")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read old Computer session capability: %v", err)
	}
	var capability struct {
		Endpoint string `json:"endpoint"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(payload, &capability); err != nil || strings.TrimSpace(capability.Endpoint) == "" || strings.TrimSpace(capability.Token) == "" {
		t.Fatalf("decode old Computer session capability: %v", err)
	}
	oldEndpoint := capability.Endpoint
	capability.Endpoint = *endpoint
	updated, err := json.Marshal(capability)
	if err != nil {
		t.Fatalf("encode retargeted Computer session capability: %v", err)
	}
	updated = append(updated, '\n')
	if err := os.WriteFile(path, updated, 0o600); err != nil {
		t.Fatalf("retarget old Computer session capability: %v", err)
	}
	return oldEndpoint, capability.Endpoint
}

func TestRetargetTakeoverSessionCapabilityPreservesOldBearer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(path, []byte(`{"endpoint":"ws://127.0.0.1:10001/wefty/computer/v1","token":"old-node-lineage-bearer"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	current := "ws://127.0.0.1:10002/wefty/computer/v1"
	old, updated := retargetTakeoverSessionCapability(t, path, &current)
	var capability struct {
		Endpoint string `json:"endpoint"`
		Token    string `json:"token"`
	}
	payload, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(payload, &capability) != nil || old != "ws://127.0.0.1:10001/wefty/computer/v1" ||
		updated != current || capability.Endpoint != current || capability.Token != "old-node-lineage-bearer" {
		t.Fatalf("retargeted capability = old=%q updated=%q capability=%+v err=%v", old, updated, capability, err)
	}
}

type takeoverViewProcess struct {
	tokenFile       string
	admitted        bool
	cancel          context.CancelFunc
	done            chan error
	stdout          bytes.Buffer
	stderr          lockedBuffer
	toleratedStderr []string
	evidence        *realTimingEvidence
	evidencePrefix  string
	computerID      string
	userID          string
	deviceID        string
	retryMode       takeoverViewRetryMode
	activeAttempt   int
	stderrStart     int
	attemptStarted  time.Time
}

type takeoverViewAttemptEvidence struct {
	ComputerID       string    `json:"computer_id"`
	UserID           string    `json:"user_id"`
	DeviceID         string    `json:"device_id"`
	RetryMode        string    `json:"retry_mode"`
	Attempt          int       `json:"attempt"`
	AdmissionOutcome string    `json:"admission_outcome"`
	ExitOutcome      string    `json:"exit_outcome"`
	ExitCode         int       `json:"exit_code"`
	Error            string    `json:"error,omitempty"`
	Stderr           string    `json:"stderr"`
	StartedAt        time.Time `json:"started_at"`
	RecordedAt       time.Time `json:"recorded_at"`
}

type takeoverViewRetryMode string

const (
	takeoverViewRetryNone            takeoverViewRetryMode = "none"
	takeoverViewRetryStalePolicy     takeoverViewRetryMode = "stale_policy_revision"
	takeoverViewHarnessRetryAttempts                       = 3
	takeoverViewHarnessRetryWindow                         = 6 * time.Second
)

var takeoverViewEvidenceSequence atomic.Uint64

func startTakeoverViewCLI(t *testing.T, evidence *realTimingEvidence, harness *acceptanceHarness, computerID, userID, deviceID string, retryMode takeoverViewRetryMode) *takeoverViewProcess {
	t.Helper()
	view := &takeoverViewProcess{
		tokenFile: filepath.Join(t.TempDir(), "takeover-session.json"), done: make(chan error, 1), evidence: evidence,
		evidencePrefix: fmt.Sprintf("takeover-cli-%03d", takeoverViewEvidenceSequence.Add(1)),
		computerID:     computerID, userID: userID, deviceID: deviceID, retryMode: retryMode,
	}
	ctx, cancel := context.WithCancel(t.Context())
	view.cancel = cancel
	deadline := time.Now().Add(30 * time.Second)
	retryDeadline := time.Now().Add(takeoverViewHarnessRetryWindow)
	attempts := 0
	running := false
	for time.Now().Before(deadline) {
		attempts++
		stderrStart := view.stderr.Len()
		view.activeAttempt, view.stderrStart, view.attemptStarted = attempts, stderrStart, time.Now().UTC()
		command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, userID, deviceID,
			"services", "takeover", "view", computerID, "--session-token-file", view.tokenFile)...)
		command.Stdout, command.Stderr = &view.stdout, &view.stderr
		go func() { view.done <- command.Run() }()
		running = true
		for time.Now().Before(deadline) {
			if info, err := os.Stat(view.tokenFile); err == nil && info.Size() > 0 {
				view.admitted = true
				view.recordAttempt("success", "pending", nil)
				return view
			}
			select {
			case err := <-view.done:
				running = false
				attemptStderr := view.stderr.String()[stderrStart:]
				view.recordAttempt("failure", "failure", err)
				// L1 availability is durable discovery, while the front door is
				// live authority. A grant may be visible before the hosting agent
				// acknowledges and installs that policy revision.
				if retryMode == takeoverViewRetryStalePolicy && attempts < takeoverViewHarnessRetryAttempts && time.Now().Before(retryDeadline) &&
					strings.Contains(attemptStderr, string(contract.ErrorStalePolicyRevision)) {
					view.toleratedStderr = append(view.toleratedStderr, attemptStderr)
					time.Sleep(25 * time.Millisecond)
					break
				}
				cancel()
				t.Fatalf("takeover view exited before admission: %v\n%s", err, view.stderr.String())
			default:
				time.Sleep(25 * time.Millisecond)
				continue
			}
			break
		}
	}
	cancel()
	if running {
		select {
		case err := <-view.done:
			view.recordAttempt("failure", "failure", err)
		case <-time.After(10 * time.Second):
			view.recordAttempt("failure", "timeout", errors.New("takeover view did not stop after its admission deadline"))
			t.Fatal("takeover view did not stop after its admission deadline")
		}
	}
	t.Fatalf("takeover view did not publish its session capability: %s", view.stderr.String())
	return nil
}

func (view *takeoverViewProcess) stop(t *testing.T) {
	t.Helper()
	view.cancel()
	select {
	case err := <-view.done:
		view.recordAttempt("success", "closed", err)
	case <-time.After(10 * time.Second):
		view.recordAttempt("success", "timeout", errors.New("takeover view did not stop"))
		t.Fatal("takeover view did not stop")
	}
}

func (view *takeoverViewProcess) waitClosed(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-view.done:
		view.recordAttempt("success", "closed", err)
	case <-time.After(timeout):
		view.cancel()
		view.recordAttempt("success", "timeout", errors.New("takeover view remained open after authority revocation"))
		t.Fatal("takeover view remained open after authority revocation")
	}
}

func (view *takeoverViewProcess) recordAttempt(admissionOutcome, exitOutcome string, err error) {
	if view.evidence == nil || view.activeAttempt == 0 {
		return
	}
	exitCode := 0
	errorText := ""
	if exitOutcome == "pending" {
		exitCode = -1
	} else if err != nil {
		exitCode = -1
		errorText = err.Error()
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		}
	}
	stderr := view.stderr.String()
	if view.stderrStart < len(stderr) {
		stderr = stderr[view.stderrStart:]
	} else {
		stderr = ""
	}
	view.evidence.recordJSON(fmt.Sprintf("%s-attempt-%02d.json", view.evidencePrefix, view.activeAttempt), takeoverViewAttemptEvidence{
		ComputerID: view.computerID, UserID: view.userID, DeviceID: view.deviceID, RetryMode: string(view.retryMode),
		Attempt: view.activeAttempt, AdmissionOutcome: admissionOutcome, ExitOutcome: exitOutcome, ExitCode: exitCode,
		Error: errorText, Stderr: stderr, StartedAt: view.attemptStarted, RecordedAt: time.Now().UTC(),
	})
}

func TestTakeoverViewAttemptEvidenceIncludesFailureExitAndStderr(t *testing.T) {
	directory := t.TempDir()
	evidence := &realTimingEvidence{t: t, directory: directory}
	view := &takeoverViewProcess{
		evidence: evidence, evidencePrefix: "takeover-cli-test", computerID: "computer-1", userID: "user-1", deviceID: "device-1",
		retryMode: takeoverViewRetryNone, activeAttempt: 1, attemptStarted: time.Now().UTC(),
	}
	_, _ = view.stderr.WriteString(`{"code":"control_not_authorized"}`)
	exitErr := exec.Command("/bin/sh", "-c", "exit 3").Run()
	view.recordAttempt("failure", "failure", exitErr)
	payload, err := os.ReadFile(filepath.Join(directory, "takeover-cli-test-attempt-01.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded takeoverViewAttemptEvidence
	if err := json.Unmarshal(payload, &recorded); err != nil {
		t.Fatal(err)
	}
	if recorded.ExitCode != 3 || recorded.AdmissionOutcome != "failure" || !strings.Contains(recorded.Stderr, "control_not_authorized") {
		t.Fatalf("takeover CLI failure evidence = %+v", recorded)
	}
}

type liveDiskEvidence struct {
	Path           string
	BlocksBytes    int64
	FullyAllocated bool
}

func liveComputerDiskName(t *testing.T, computer l1.Computer) string {
	t.Helper()
	name, err := ocihelper.DeterministicComputerDiskName(ocihelper.ComputerStorageReference{
		ComputerID: computer.ComputerID, StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func inspectLiveComputerDisk(t *testing.T, computer l1.Computer) liveDiskEvidence {
	t.Helper()
	path := filepath.Join("/var/lib/wefty/oci/computer-disks", liveComputerDiskName(t, computer), "disk.ext4")
	output, err := exec.Command("sudo", "stat", "--format=%s %b", path).CombinedOutput()
	if err != nil {
		t.Fatalf("inspect live Computer disk: %v\n%s", err, output)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 {
		t.Fatalf("unexpected disk stat %q", output)
	}
	size, sizeErr := strconv.ParseInt(fields[0], 10, 64)
	blocks, blocksErr := strconv.ParseInt(fields[1], 10, 64)
	if sizeErr != nil || blocksErr != nil {
		t.Fatalf("parse disk stat %q: %v %v", output, sizeErr, blocksErr)
	}
	return liveDiskEvidence{Path: path, BlocksBytes: blocks * 512, FullyAllocated: size == computer.DesiredDiskBytes && blocks*512 >= size}
}

func plantLiveProfileMarker(t *testing.T, computer l1.Computer, marker string) string {
	t.Helper()
	path := filepath.Join("/var/lib/wefty/oci/computer-mounts", liveComputerDiskName(t, computer), marker)
	if output, err := exec.Command("sudo", "touch", path).CombinedOutput(); err != nil {
		t.Fatalf("plant live profile marker: %v\n%s", err, output)
	}
	return path
}

func assertLiveProfileMarker(t *testing.T, computer l1.Computer, priorPath string) {
	t.Helper()
	path := filepath.Join("/var/lib/wefty/oci/computer-mounts", liveComputerDiskName(t, computer), filepath.Base(priorPath))
	if output, err := exec.Command("sudo", "test", "-f", path).CombinedOutput(); err != nil {
		t.Fatalf("profile marker did not survive at %s: %v\n%s", path, err, output)
	}
}

func triggerLinuxComputerFault(t *testing.T, harness *acceptanceHarness, action string) {
	t.Helper()
	fifo := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_FAULT_FIFO")
	directory := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_FAULT_DIR")
	done := filepath.Join(directory, action+".done")
	failure := filepath.Join(directory, action+".failed")
	_ = os.Remove(done)
	_ = os.Remove(failure)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "sh", "-c", `printf '%s\n' "$1" > "$2"`, "wefty-fault", action, fifo)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("trigger fault %s: %v\n%s", action, err, output)
	}
	for ctx.Err() == nil {
		if _, err := os.Stat(done); err == nil {
			return
		}
		if payload, err := os.ReadFile(failure); err == nil {
			t.Fatalf("root assertion %s failed: %s", action, payload)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("fault %s did not complete", action)
}

type liveRFBSession struct {
	endpoint   string
	token      string
	websocket  *websocket.Conn
	connection net.Conn
}

func openLiveRFBSession(t *testing.T, endpoint, userID, deviceID string) *liveRFBSession {
	t.Helper()
	plainNetwork, err := plain.NewNetworkWithID("plain-linux-computer-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	participant := plainNetwork.NewFabric(fabric.Identity{NodeID: deviceID, UserID: userID, DeviceID: deviceID})
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
	connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{
		HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol},
	})
	transport.CloseIdleConnections()
	if err != nil {
		t.Fatalf("open live RFB edge for %s: %v", userID, err)
	}
	token := response.Header.Get(contract.ComputerControlTokenHeader)
	network := websocket.NetConn(t.Context(), connection, websocket.MessageBinary)
	if token == "" {
		_ = connection.CloseNow()
		t.Fatal("live RFB session omitted its control capability")
	}
	session := &liveRFBSession{endpoint: endpoint, token: token, websocket: connection, connection: network}
	session.negotiate(t)
	return session
}

func (session *liveRFBSession) negotiate(t *testing.T) {
	t.Helper()
	banner := make([]byte, contract.ComputerRFBVersionBannerBytes)
	if _, err := io.ReadFull(session.connection, banner); err != nil || !contract.ValidComputerRFBVersionBanner(banner) {
		_ = session.websocket.CloseNow()
		t.Fatalf("read live RFB banner: %v %q", err, banner)
	}
	if _, err := session.connection.Write([]byte("RFB 003.008\n")); err != nil {
		t.Fatal(err)
	}
	count := []byte{0}
	if _, err := io.ReadFull(session.connection, count); err != nil || count[0] == 0 {
		t.Fatalf("read live RFB security count: %v", err)
	}
	securityTypes := make([]byte, int(count[0]))
	if _, err := io.ReadFull(session.connection, securityTypes); err != nil || !bytes.Contains(securityTypes, []byte{1}) {
		t.Fatalf("live RFB None security unavailable: %v %x", err, securityTypes)
	}
	if _, err := session.connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	securityResult := make([]byte, 4)
	if _, err := io.ReadFull(session.connection, securityResult); err != nil || !bytes.Equal(securityResult, []byte{0, 0, 0, 0}) {
		t.Fatalf("live RFB security result: %v %x", err, securityResult)
	}
	if _, err := session.connection.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	serverInit := make([]byte, 24)
	if _, err := io.ReadFull(session.connection, serverInit); err != nil {
		t.Fatal(err)
	}
	name := make([]byte, int(binary.BigEndian.Uint32(serverInit[20:24])))
	if _, err := io.ReadFull(session.connection, name); err != nil {
		t.Fatal(err)
	}
}

func (session *liveRFBSession) sendPointer(t *testing.T, x, y int) {
	t.Helper()
	for _, event := range [][]byte{
		{5, 1, byte(x >> 8), byte(x), byte(y >> 8), byte(y)},
		{5, 0, byte(x >> 8), byte(x), byte(y >> 8), byte(y)},
	} {
		if _, err := session.connection.Write(event); err != nil {
			t.Fatalf("send live RFB pointer: %v", err)
		}
	}
}

func (session *liveRFBSession) capabilityFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "live-rfb-session.json")
	payload, err := json.Marshal(map[string]string{"endpoint": session.endpoint, "token": session.token})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (session *liveRFBSession) close() {
	_ = session.websocket.CloseNow()
}

type liveInputObservation struct {
	Version        int      `json:"version"`
	Generation     uint64   `json:"generation"`
	KeyEvents      uint64   `json:"key_events"`
	X              int      `json:"x"`
	Y              int      `json:"y"`
	PointerHistory [][2]int `json:"pointer_history"`
}

type liveComputerDriverObservation struct {
	Version      int  `json:"version"`
	HumanDriving bool `json:"human_driving"`
}

func readLiveComputerHumanDriving(t *testing.T, jobID string) bool {
	t.Helper()
	containerID := liveComputerContainerID(t, jobID)
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	execID := fmt.Sprintf("driver-oracle-%d", time.Now().UnixNano())
	payload, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/bin/cat", "/wefty/control/driver.json").CombinedOutput()
	if err != nil {
		t.Fatalf("read live Computer driver state: %v\n%s", err, payload)
	}
	var observation liveComputerDriverObservation
	if err := json.Unmarshal(payload, &observation); err != nil || observation.Version != 1 {
		t.Fatalf("decode live Computer driver state: %v\n%s", err, payload)
	}
	return observation.HumanDriving
}

func waitLiveComputerHumanDriving(t *testing.T, jobID string, want bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		observed := readLiveComputerHumanDriving(t, jobID)
		if observed == want || !time.Now().Before(deadline) {
			return observed
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func readLiveInputObservation(t *testing.T, jobID string) liveInputObservation {
	t.Helper()
	containerID := liveComputerContainerID(t, jobID)
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	execID := fmt.Sprintf("input-oracle-%d", time.Now().UnixNano())
	payload, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/bin/cat", "/tmp/wefty-computer/input-oracle.json").CombinedOutput()
	if err != nil {
		t.Fatalf("read live Computer input oracle: %v\n%s", err, payload)
	}
	var observation liveInputObservation
	if err := json.Unmarshal(payload, &observation); err != nil || observation.Version != 1 {
		t.Fatalf("decode live Computer input oracle: %v\n%s", err, payload)
	}
	return observation
}

func liveComputerContainerID(t *testing.T, jobID string) string {
	t.Helper()
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	list, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"containers", "list", "--quiet").CombinedOutput()
	if err != nil {
		t.Fatalf("list live Computer containers: %v\n%s", err, list)
	}
	containerID := ""
	for _, candidate := range strings.Fields(string(list)) {
		info, infoErr := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
			"containers", "info", candidate).CombinedOutput()
		if infoErr == nil && strings.Contains(string(info), jobID) {
			containerID = candidate
			break
		}
	}
	if containerID == "" {
		t.Fatalf("no live container carried job identity %s", jobID)
	}
	return containerID
}

type liveComputerHTTPResult struct {
	Status         int    `json:"status"`
	Body           string `json:"body"`
	TransportError string `json:"transport_error,omitempty"`
}

const liveComputerHTTPPython = `
import json, sys, urllib.error, urllib.request
method, path, key, body = sys.argv[1:5]
payload = body.encode() if body else None
endpoint = open("/wefty/control/l3-endpoint", encoding="utf-8").read().strip()
request = urllib.request.Request(endpoint + path, data=payload, method=method)
request.add_header("Authorization", "Bearer " + open("/wefty/control/computer-token", encoding="utf-8").read().strip())
if payload is not None:
    request.add_header("Content-Type", "application/json")
if key:
    request.add_header("Idempotency-Key", key)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        status, response_body = response.status, response.read().decode()
except urllib.error.HTTPError as error:
    status, response_body = error.code, error.read().decode()
print(json.dumps({"status": status, "body": response_body}))
`

func liveComputerRunRequest(duration time.Duration) map[string]any {
	content := fmt.Sprintf("sleep %d\n", int(duration.Seconds()))
	digest := sha256.Sum256([]byte(content))
	return map[string]any{"inline_script": map[string]any{
		"content": content, "sha256": hex.EncodeToString(digest[:]), "interpreter": []string{"/bin/sh"},
	}, "params": map[string]any{}}
}

func tryLiveComputerHTTP(t *testing.T, computer l1.Computer, method, path, idempotencyKey string, body any) (liveComputerHTTPResult, error) {
	t.Helper()
	bodyJSON := ""
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return liveComputerHTTPResult{}, err
		}
		bodyJSON = string(payload)
	}
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("computer-http-%d", time.Now().UnixNano())
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerHTTPPython,
		method, path, idempotencyKey, bodyJSON).CombinedOutput()
	if err != nil {
		return liveComputerHTTPResult{}, fmt.Errorf("execute Computer HTTP probe: %w: %s", err, output)
	}
	var result liveComputerHTTPResult
	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	if len(lines) == 0 || json.Unmarshal(lines[len(lines)-1], &result) != nil {
		return liveComputerHTTPResult{}, fmt.Errorf("decode Computer HTTP probe: %s", output)
	}
	return result, nil
}

func runLiveComputerHTTP(t *testing.T, computer l1.Computer, method, path, idempotencyKey string, body any) liveComputerHTTPResult {
	t.Helper()
	result, err := tryLiveComputerHTTP(t, computer, method, path, idempotencyKey, body)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func listComputerRunsFromAuthority(t *testing.T, harness *acceptanceHarness, computerID string) l3.ComputerRunPage {
	t.Helper()
	plainNetwork, err := plain.NewNetworkWithID("plain-linux-computer-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	participant := plainNetwork.NewFabric(fabric.Identity{NodeID: "linux-computer-run-auditor", Tags: []string{l3.DefaultCallerPrincipalTag}})
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, harness.runLedgerAddress)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 30 * time.Second, Transport: transport}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		"http://run-ledger.invalid/v1/runs?origin="+url.QueryEscape("computer:"+computerID)+"&limit=1000", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("list Computer Runs from L3 authority: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list Computer Runs from L3 authority status=%d body=%s", response.StatusCode, payload)
	}
	var page l3.ComputerRunPage
	if err := json.Unmarshal(payload, &page); err != nil {
		t.Fatalf("decode Computer Runs from L3 authority: %v body=%s", err, payload)
	}
	return page
}

type computerRevocationRaceAuthorityResult struct {
	Outcome               string
	Closed                bool
	RunID                 string
	RunCreatedAt          string
	RevocationCommittedAt string
}

func classifyComputerRevocationRaceAuthority(before, after []contract.RunRecord,
	revocation *contract.ComputerTokenRevocationReceipt,
) computerRevocationRaceAuthorityResult {
	result := computerRevocationRaceAuthorityResult{Outcome: "unexpected_run_set_change"}
	if revocation == nil || revocation.CommittedAt.IsZero() {
		return result
	}
	result.RevocationCommittedAt = revocation.CommittedAt.Format(time.RFC3339Nano)
	beforeRunIDs := make(map[string]struct{}, len(before))
	for _, run := range before {
		beforeRunIDs[run.RunID] = struct{}{}
	}
	newRuns := make([]contract.RunRecord, 0, 1)
	for _, run := range after {
		if _, existed := beforeRunIDs[run.RunID]; !existed {
			newRuns = append(newRuns, run)
		}
	}
	if len(after) == len(before) && len(newRuns) == 0 {
		result.Outcome = "no_commit"
		result.Closed = true
		return result
	}
	if len(after) != len(before)+1 || len(newRuns) != 1 {
		return result
	}
	result.RunID = newRuns[0].RunID
	result.RunCreatedAt = newRuns[0].CreatedAt.Format(time.RFC3339Nano)
	result.Outcome = "committed_after_revocation"
	if !newRuns[0].CreatedAt.After(revocation.CommittedAt) {
		result.Outcome = "committed_before_revocation"
		result.Closed = true
	}
	return result
}

func TestClassifyComputerRevocationRaceAuthorityOrder(t *testing.T) {
	revokedAt := time.Date(2026, 9, 2, 4, 0, 0, 0, time.UTC)
	baseline := []contract.RunRecord{{RunID: "run-before", CreatedAt: revokedAt.Add(-time.Minute)}}
	tests := []struct {
		name    string
		after   []contract.RunRecord
		outcome string
		closed  bool
	}{
		{name: "no commit", after: baseline, outcome: "no_commit", closed: true},
		{name: "commit won fence", after: append(append([]contract.RunRecord(nil), baseline...), contract.RunRecord{RunID: "run-race", CreatedAt: revokedAt.Add(-time.Nanosecond)}), outcome: "committed_before_revocation", closed: true},
		{name: "commit after revocation", after: append(append([]contract.RunRecord(nil), baseline...), contract.RunRecord{RunID: "run-race", CreatedAt: revokedAt.Add(time.Nanosecond)}), outcome: "committed_after_revocation", closed: false},
		{name: "multiple unexpected commits", after: append(append([]contract.RunRecord(nil), baseline...), contract.RunRecord{RunID: "run-race-1", CreatedAt: revokedAt}, contract.RunRecord{RunID: "run-race-2", CreatedAt: revokedAt}), outcome: "unexpected_run_set_change", closed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := classifyComputerRevocationRaceAuthority(baseline, test.after,
				&contract.ComputerTokenRevocationReceipt{CommittedAt: revokedAt})
			if result.Outcome != test.outcome || result.Closed != test.closed {
				t.Fatalf("authority result = %#v, want outcome=%q closed=%t", result, test.outcome, test.closed)
			}
		})
	}
}

func waitForLiveComputerHTTP(t *testing.T, computer l1.Computer, method, path, idempotencyKey string, body any, timeout time.Duration) liveComputerHTTPResult {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		result, err := tryLiveComputerHTTP(t, computer, method, path, idempotencyKey, body)
		if err == nil && result.Status >= 200 && result.Status < 300 {
			return result
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status=%d body=%s", result.Status, result.Body)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("live Computer HTTP route did not become ready: %v", lastErr)
	return liveComputerHTTPResult{}
}

// Run 33585098509 traversed the entire native guest-authority phase in 5.14s.
// The guest's 30s release wait and this 45s process bound leave measured
// disable headroom without allowing a stuck ctr stdin or response read to hang
// the 75-minute lane.
const liveComputerPausedHTTPTimeout = 45 * time.Second

const liveComputerPausedHTTPPython = `
import http.client, json, select, socket, sys, urllib.parse
path, key, body = sys.argv[1:4]
endpoint = urllib.parse.urlsplit(open("/wefty/control/l3-endpoint", encoding="utf-8").read().strip())
token = open("/wefty/control/computer-token", encoding="utf-8").read().strip()
payload = body.encode()
request_path = endpoint.path.rstrip("/") + path
request = ("POST " + request_path + " HTTP/1.1\r\nHost: " + endpoint.netloc + "\r\nAuthorization: Bearer " + token +
           "\r\nContent-Type: application/json\r\nIdempotency-Key: " + key + "\r\nExpect: 100-continue\r\nConnection: close\r\nContent-Length: " +
           str(len(payload)) + "\r\n\r\n").encode()
status, response_body, transport_error = 0, "", ""
try:
    connection = socket.create_connection((endpoint.hostname, endpoint.port), timeout=30)
    connection.sendall(request)
    interim = b""
    while b"\r\n\r\n" not in interim:
        chunk = connection.recv(1024)
        if not chunk:
            raise ConnectionError("bridge closed before admission acknowledgement")
        interim += chunk
        if len(interim) > 8192:
            raise ValueError("oversized admission acknowledgement")
    if not interim.startswith(b"HTTP/1.1 100 Continue\r\n"):
        raise RuntimeError("unexpected admission acknowledgement: " + interim.decode(errors="replace"))
    print("PAUSED", flush=True)
    ready, _, _ = select.select([sys.stdin], [], [], 30)
    if not ready:
        raise TimeoutError("release was not received within 30 seconds")
    if not sys.stdin.readline():
        raise EOFError("release input closed without a signal")
    connection.sendall(payload)
    response = http.client.HTTPResponse(connection)
    response.begin()
    status, response_body = response.status, response.read().decode()
except Exception as error:
    transport_error = type(error).__name__ + ":" + str(error)
print(json.dumps({"status": status, "body": response_body, "transport_error": transport_error}), flush=True)
`

func TestLiveComputerPausedHTTPProbeWaitsForServerAdmission(t *testing.T) {
	endpointPath := strings.Index(liveComputerPausedHTTPPython, `endpoint.path.rstrip("/") + path`)
	requestPath := strings.Index(liveComputerPausedHTTPPython, `"POST " + request_path`)
	expect := strings.Index(liveComputerPausedHTTPPython, `Expect: 100-continue`)
	acknowledged := strings.Index(liveComputerPausedHTTPPython, `HTTP/1.1 100 Continue`)
	paused := strings.Index(liveComputerPausedHTTPPython, `print("PAUSED"`)
	bounded := strings.Index(liveComputerPausedHTTPPython, `select.select([sys.stdin], [], [], 30)`)
	released := strings.Index(liveComputerPausedHTTPPython, `sys.stdin.readline()`)
	bodySent := strings.LastIndex(liveComputerPausedHTTPPython, `connection.sendall(payload)`)
	if endpointPath < 0 || requestPath < endpointPath || expect < requestPath || acknowledged < expect || paused < acknowledged || bounded < paused || released < bounded || bodySent < released {
		t.Fatalf("Computer revocation probe ordering endpoint_path=%d request_path=%d expect=%d acknowledged=%d paused=%d bounded=%d released=%d body_sent=%d", endpointPath, requestPath, expect, acknowledged, paused, bounded, released, bodySent)
	}
}

type liveComputerPausedSubmission struct {
	context context.Context
	cancel  context.CancelFunc
	command *exec.Cmd
	scanner *bufio.Scanner
	stdin   io.WriteCloser
	stderr  *bytes.Buffer
}

func startLiveComputerPausedSubmission(t *testing.T, computer l1.Computer, idempotencyKey string, body any) *liveComputerPausedSubmission {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	containerdAddress := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	containerID := liveComputerContainerID(t, computer.CurrentJobID)
	execID := fmt.Sprintf("computer-race-%d", time.Now().UnixNano())
	probeContext, cancel := context.WithTimeout(t.Context(), liveComputerPausedHTTPTimeout)
	command := exec.CommandContext(probeContext, "sudo", "/usr/local/bin/ctr", "--address", containerdAddress, "--namespace", ocihelper.ContainerdNamespace,
		"tasks", "exec", "--exec-id", execID, containerID, "/usr/bin/python3", "-c", liveComputerPausedHTTPPython,
		"/v1/runs", idempotencyKey, string(payload))
	stdout, err := command.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "PAUSED" {
		cancel()
		_ = command.Wait()
		t.Fatalf("Computer revocation race did not receive its admission acknowledgement: stdout=%q stderr=%q", scanner.Text(), stderr.String())
	}
	return &liveComputerPausedSubmission{context: probeContext, cancel: cancel, command: command, scanner: scanner, stdin: stdin, stderr: stderr}
}

func (submission *liveComputerPausedSubmission) finish(t *testing.T) liveComputerHTTPResult {
	t.Helper()
	defer submission.cancel()
	releaseErr := error(nil)
	if _, err := io.WriteString(submission.stdin, "release\n"); err != nil {
		releaseErr = fmt.Errorf("write release: %w", err)
	}
	if err := submission.stdin.Close(); err != nil {
		releaseErr = errors.Join(releaseErr, fmt.Errorf("close release: %w", err))
	}
	type scanResult struct {
		line string
		ok   bool
	}
	scanned := make(chan scanResult, 1)
	go func() {
		ok := submission.scanner.Scan()
		scanned <- scanResult{line: submission.scanner.Text(), ok: ok && submission.scanner.Err() == nil}
	}()
	var line string
	select {
	case result := <-scanned:
		line = result.line
		if !result.ok || line == "" {
			_ = submission.command.Wait()
			t.Fatalf("Computer revocation race omitted its result: release_error=%v stderr=%s", releaseErr, submission.stderr.String())
		}
	case <-submission.context.Done():
		_ = submission.command.Wait()
		t.Fatalf("Computer revocation race exceeded %s: release_error=%v stderr=%s", liveComputerPausedHTTPTimeout, releaseErr, submission.stderr.String())
	}
	var result liveComputerHTTPResult
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		submission.cancel()
		_ = submission.command.Wait()
		t.Fatalf("decode Computer revocation race: %v line=%q release_error=%v stderr=%s", err, line, releaseErr, submission.stderr.String())
	}
	if err := submission.command.Wait(); err != nil {
		result.TransportError = strings.TrimSpace(strings.Join([]string{result.TransportError, fmt.Sprintf("process=%v", err), "stderr=" + submission.stderr.String()}, " "))
	}
	if releaseErr != nil {
		result.TransportError = strings.TrimSpace(strings.Join([]string{result.TransportError, "release=" + releaseErr.Error(), "stderr=" + submission.stderr.String()}, " "))
	}
	return result
}

func observationHasPointer(observation liveInputObservation, x, y int) bool {
	return pointerHistoryHas(observation.PointerHistory, x, y)
}

func proveLiveViewInputIsolation(t *testing.T, harness *acceptanceHarness, computer l1.Computer, viewerUser, viewerDevice string) bool {
	t.Helper()
	if computer.DisplayEndpoint == nil {
		t.Fatal("Computer omitted its live display endpoint")
	}
	before := readLiveInputObservation(t, computer.CurrentJobID)
	freeViewPointer, heldViewPointer, controlPointer := freshPointerSentinels(before.PointerHistory)
	if freeViewPointer == ([2]int{}) || heldViewPointer == ([2]int{}) || controlPointer == ([2]int{}) {
		t.Fatal("Computer input history exhausted the isolation sentinels")
	}
	view := openLiveRFBSession(t, *computer.DisplayEndpoint, viewerUser, viewerDevice)
	defer view.close()
	view.sendPointer(t, freeViewPointer[0], freeViewPointer[1])
	control := openLiveRFBSession(t, *computer.DisplayEndpoint, "linux-admin", "linux-input-sentinel-device")
	defer control.close()
	controlCapability := control.capabilityFile(t)
	take := runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-input-sentinel-device",
		"services", "takeover", "take", computer.ComputerID, "--session-token-file", controlCapability)
	if take.TenureState != contract.ComputerControlTenureHeld {
		t.Fatalf("control sentinel take = %#v", take)
	}
	// The Held-tenure sentinel is the decisive isolation arm: the view-only
	// session must remain unable to drive while another session owns the wheel.
	view.sendPointer(t, heldViewPointer[0], heldViewPointer[1])
	control.sendPointer(t, controlPointer[0], controlPointer[1])
	var after liveInputObservation
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		after = readLiveInputObservation(t, computer.CurrentJobID)
		if after.Generation > before.Generation && observationHasPointer(after, controlPointer[0], controlPointer[1]) {
			break
		}
		time.Sleep(125 * time.Millisecond)
	}
	_ = runComputerCLIPerson[contract.ComputerControlReceipt](t, harness, "linux-admin", "linux-input-sentinel-device",
		"services", "takeover", "release", computer.ComputerID, "--session-token-file", controlCapability)
	return after.Generation > before.Generation && observationHasPointer(after, controlPointer[0], controlPointer[1]) &&
		!observationHasPointer(after, freeViewPointer[0], freeViewPointer[1]) &&
		!observationHasPointer(after, heldViewPointer[0], heldViewPointer[1]) && after.KeyEvents == before.KeyEvents
}

func takeoverAuditEvidence(audit l1.ComputerTakeoverAuditList) (map[l1.ComputerTakeoverAuditEventKind]bool, int64) {
	kinds := map[l1.ComputerTakeoverAuditEventKind]bool{}
	var generation int64
	for _, event := range audit.Events {
		kinds[event.Kind] = true
		if event.AuthorityGeneration > generation {
			generation = event.AuthorityGeneration
		}
	}
	return kinds, generation
}

func takeoverAuditReleasePrecedesClose(
	audit l1.ComputerTakeoverAuditList,
	sessionID string,
	reason l1.ComputerTakeoverReason,
) bool {
	releaseIndex, closeIndex := -1, -1
	for index, event := range audit.Events {
		if event.SessionID != sessionID || event.Reason != reason {
			continue
		}
		switch event.Kind {
		case l1.ComputerTakeoverControlReleased:
			if releaseIndex == -1 {
				releaseIndex = index
			}
		case l1.ComputerTakeoverSessionClose:
			if closeIndex == -1 {
				closeIndex = index
			}
		}
	}
	return sessionID != "" && releaseIndex >= 0 && closeIndex > releaseIndex
}

func waitForComputerAttemptTerminal(
	t *testing.T,
	harness *acceptanceHarness,
	jobID, attemptID string,
	timeout time.Duration,
) l1.Attempt {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get Computer Job attempts = %d body=%s", status, body)
		}
		for _, attempt := range job.Attempts {
			if attempt.AttemptID == attemptID && attempt.Result != nil {
				return attempt
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for authority-bound terminal result for attempt %q", attemptID)
	return l1.Attempt{}
}

func appendUniqueInt64(values []int64, value int64) []int64 {
	if value == 0 {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func inspectLiveComputerReimageDetachment(t *testing.T, harness *acceptanceHarness, computer l1.Computer) bool {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var payload []byte
	var status string
	if err := database.QueryRow(`SELECT preflight_receipt_json, status FROM computer_reimage_operations
		WHERE computer_id=? AND operation_revision=?`, computer.ComputerID, computer.AppliedRevision).
		Scan(&payload, &status); err != nil {
		t.Fatal(err)
	}
	var acknowledgement struct {
		Receipt l1.ComputerReimagePreflightReceipt `json:"receipt"`
	}
	if err := json.Unmarshal(payload, &acknowledgement); err != nil {
		t.Fatal(err)
	}
	receipt := acknowledgement.Receipt
	return status == "completed" && receipt.Kind == "computer_reimage_preflight_verified" &&
		receipt.ComputerID == computer.ComputerID && receipt.StorageID == computer.StorageID &&
		receipt.StorageGeneration == computer.StorageGeneration && receipt.OperationRevision == computer.AppliedRevision &&
		receipt.StagingJobID == computer.CurrentJobID && receipt.StorageEvidenceKind == "computer_reimage_detachment" &&
		receipt.DetachmentReceiptID != "" && receipt.DetachmentAttemptID != "" && receipt.DetachmentFencingToken != "" &&
		receipt.ResetPreparationReceiptID == "" && receipt.ImageUID == receipt.DiskRootUID && receipt.ImageGID == receipt.DiskRootGID
}

type liveAbortEvidence struct {
	ComputerID       string
	Aborted          bool
	StaleCASRejected bool
	NoAutoRollback   bool
}

func exerciseLiveReconfigurationAbort(t *testing.T, harness *acceptanceHarness, reference, digest string) liveAbortEvidence {
	t.Helper()
	target := createReadyComputer(t, harness, reference, digest, "linux-native-abort", "linux-native-abort-create")
	stale := runComputerCLIExpectError(t, harness, "", "", "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(256<<20), "--intent-revision", fmt.Sprint(target.IntentRevision+99),
		"--storage-id", target.StorageID, "--storage-generation", fmt.Sprint(target.StorageGeneration),
		"--idempotency-key", "linux-native-stale-cas")
	mutation := runComputerCLI[l1.Computer](t, harness, false, "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(512<<20), "--expect-current", "--idempotency-key", "linux-native-abort-grow")
	harness.agent.kill(t)
	time.Sleep(l1.DefaultNodeDeadAfter + 5*time.Second)
	beforeAbort := runComputerCLI[l1.Computer](t, harness, false, "services", "status", target.ComputerID)
	aborted := runComputerCLI[l1.Computer](t, harness, false, "services", "abort", target.ComputerID,
		"--expect-current", "--idempotency-key", "linux-native-abort")
	harness.restartAgent(t)
	_ = removeAndWaitComputer(t, harness, aborted, 5*time.Minute)
	return liveAbortEvidence{ComputerID: target.ComputerID,
		Aborted:          aborted.ReconfigurationPhase == l1.ComputerReconfigurationStable,
		StaleCASRejected: strings.Contains(stale, string(contract.ErrorStaleIntentRevision)),
		NoAutoRollback:   beforeAbort.IntentRevision == mutation.IntentRevision && beforeAbort.AppliedRevision < beforeAbort.IntentRevision}
}

type liveENOSPCEvidence struct {
	Observed bool
	Detail   string
}

type liveResizeOutcome struct {
	l1.Computer
	Capacity struct {
		LastGrow struct {
			Status                 string `json:"status"`
			FailureCode            string `json:"failure_code"`
			RequestedBytes         *int64 `json:"requested_bytes"`
			ObservedAvailableBytes *int64 `json:"observed_available_bytes"`
		} `json:"last_grow"`
	} `json:"capacity"`
}

func exerciseLiveComputerENOSPC(t *testing.T, harness *acceptanceHarness, reference, digest string) liveENOSPCEvidence {
	t.Helper()
	target := createReadyComputer(t, harness, reference, digest, "linux-native-enospc", "linux-native-enospc-create")
	availableOutput, err := exec.Command("sudo", "df", "--block-size=1", "--output=avail", "/var/lib/wefty/oci").CombinedOutput()
	if err != nil {
		t.Fatalf("observe real disk availability: %v\n%s", err, availableOutput)
	}
	fields := strings.Fields(string(availableOutput))
	available, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	requested := available + 1<<30
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, computerCLIArguments(harness, "", "", "services", "resize", target.ComputerID,
		"--disk-bytes", fmt.Sprint(requested), "--expect-current", "--idempotency-key", "linux-native-enospc-grow",
		"--wait", "5m", "--poll-interval", "250ms")...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	commandErr := command.Run()
	if commandErr == nil {
		t.Fatalf("awaited Computer ENOSPC grow unexpectedly succeeded:\n%s", stdout.String())
	}
	var outcome liveResizeOutcome
	if err := json.Unmarshal(stdout.Bytes(), &outcome); err != nil {
		t.Fatalf("decode awaited Computer ENOSPC projection: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(stderr.Bytes(), &response); err != nil {
		t.Fatalf("decode awaited Computer ENOSPC error: %v\n%s", err, stderr.String())
	}
	var failure contract.SpawnFailure
	failureDecoded := json.Unmarshal(outcome.CurrentJob.LastFailure, &failure) == nil
	lastGrow := outcome.Capacity.LastGrow
	observed := response.Error.Code == contract.ErrorCapacityExhausted &&
		outcome.ReconfigurationPhase == l1.ComputerReconfigurationStable && outcome.AppliedRevision == outcome.IntentRevision &&
		outcome.DesiredDiskBytes == target.DesiredDiskBytes && failureDecoded && failure.Code == contract.SpawnFailureInsufficientDisk &&
		lastGrow.Status == "FAIL" && lastGrow.FailureCode == string(contract.SpawnFailureInsufficientDisk) &&
		lastGrow.RequestedBytes != nil && lastGrow.ObservedAvailableBytes != nil &&
		*lastGrow.RequestedBytes == requested && failure.RequestedBytes == *lastGrow.RequestedBytes &&
		failure.ObservedAvailableBytes == *lastGrow.ObservedAvailableBytes && *lastGrow.ObservedAvailableBytes < *lastGrow.RequestedBytes
	_ = removeAndWaitComputer(t, harness, target, 5*time.Minute)
	return liveENOSPCEvidence{Observed: observed,
		Detail: fmt.Sprintf("requested=%d helper_observed_available=%d host_sample_available=%d error_code=%s",
			requested, valueOrNegative(lastGrow.ObservedAvailableBytes), available, response.Error.Code)}
}

func valueOrNegative(value *int64) int64 {
	if value == nil {
		return -1
	}
	return *value
}

func inspectHelperNamespaceInventory(t *testing.T, socketPath, checksum string) ocihelper.VerifyResponse {
	t.Helper()
	client := ocihelper.NewUnixClient(socketPath, checksum)
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		session, err := client.OpenSession(ctx, ocihelper.AcquireSessionRequest{NodeID: "acceptance-inventory", BootSessionID: "acceptance-inventory-boot"})
		cancel()
		if err == nil {
			verification, verifyErr := session.Verify(t.Context(), ocihelper.VerifyRequest{Scope: ocihelper.VerifyNamespaceReadOnly})
			_ = session.Close()
			if verifyErr != nil {
				t.Fatalf("verify helper namespace inventory: %v", verifyErr)
			}
			if !verification.Absent {
				t.Fatalf("helper namespace runtime residue=%#v durable retained=%#v observed=%#v", verification.RuntimeResidue, verification.DurableRetained, verification.Inventory)
			}
			return verification
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("acquire independent helper inventory session: %v", lastErr)
	return ocihelper.VerifyResponse{}
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func liveContainerdImagePresent(t *testing.T, digest string) bool {
	t.Helper()
	address := requiredComputerRealtimeEnvironment(t, "WEFTY_OCI_CONTAINERD_ADDRESS")
	output, err := exec.Command("sudo", "/usr/local/bin/ctr", "--address", address, "--namespace", ocihelper.ContainerdNamespace,
		"images", "list").CombinedOutput()
	if err != nil {
		t.Fatalf("list live image cache: %v\n%s", err, output)
	}
	return strings.Contains(string(output), digest)
}

func runComputerCLI[T any](t *testing.T, harness *acceptanceHarness, person bool, arguments ...string) T {
	t.Helper()
	if person {
		return runComputerCLIWithIdentity[T](t, harness, "linux-admin", "linux-admin-device-a", arguments...)
	}
	return runComputerCLIWithIdentity[T](t, harness, "", "", arguments...)
}

func bootstrapComputerAcceptanceAdmin(t *testing.T, harness *acceptanceHarness) l1.AdminPolicy {
	t.Helper()
	if harness.adminBootstrapNonce == "" {
		t.Fatal("Computer harness omitted the shipped L1 admin bootstrap challenge")
	}
	return runComputerCLI[l1.AdminPolicy](t, harness, true, "admin", "bootstrap", harness.adminBootstrapNonce)
}

func waitForComputerCLI(t *testing.T, harness *acceptanceHarness, computerID string, timeout time.Duration, ready func(l1.Computer) bool) l1.Computer {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last l1.Computer
	for time.Now().Before(deadline) {
		last = runComputerCLI[l1.Computer](t, harness, false, "services", "status", computerID)
		if ready(last) {
			return last
		}
		if harness.agent.exited() {
			t.Fatalf("Computer agent exited: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("Computer %s did not reach requested state: %#v", computerID, last)
	return l1.Computer{}
}

func computerDisplayPublished(computer l1.Computer) bool {
	// Computers forbid published_port, so ServiceJob.Ready is intentionally nil.
	// The fenced, attempt-bound display_endpoint is their readiness projection.
	return computer.CurrentJob.State == contract.JobRunning && computer.DisplayEndpoint != nil
}

func computerStoppedAndDetached(computer l1.Computer) bool {
	return computer.DesiredState == contract.ServiceDesiredStopped &&
		(computer.CurrentJob.State == contract.JobStopped || computer.CurrentJob.State == contract.JobFailed) &&
		computer.CurrentJob.CurrentAttemptID == "" && computer.ReconfigurationPhase == l1.ComputerReconfigurationStable
}

func computerStoppedAfterExplicitStop(computer l1.Computer) bool {
	return computer.DesiredState == contract.ServiceDesiredStopped &&
		computer.CurrentJob.State == contract.JobStopped && computer.CurrentJob.CurrentAttemptID == "" &&
		computer.ReconfigurationPhase == l1.ComputerReconfigurationStable
}

func failOnTypedReimagePreflight(t *testing.T, computer l1.Computer, priorSpecRevision int64) {
	t.Helper()
	if computer.ReconfigurationPhase != l1.ComputerReconfigurationStable ||
		computer.CurrentSpecRevision > priorSpecRevision || len(computer.CurrentJob.LastFailure) == 0 {
		return
	}
	var failure contract.SpawnFailure
	if json.Unmarshal(computer.CurrentJob.LastFailure, &failure) != nil {
		return
	}
	if failure.Code == contract.SpawnFailureReimagePreflight || failure.Code == contract.SpawnFailureImageUnavailable ||
		failure.Code == contract.SpawnFailureImagePlatformUnsupported {
		t.Fatalf("Computer reimage returned typed preflight failure instead of timing out: code=%s message=%s",
			failure.Code, failure.Message)
	}
}

func createReadyComputer(t *testing.T, harness *acceptanceHarness, reference, digest, name, key string) l1.Computer {
	t.Helper()
	created := runComputerCLI[l1.Computer](t, harness, false, "services", "create", "--computer", "--name", name,
		"--image", reference+"@"+digest, "--node", "acceptance-node", "--memory-bytes", fmt.Sprint(1<<30),
		"--disk-bytes", fmt.Sprint(64<<20), "--idempotency-key", key)
	return waitForComputerCLI(t, harness, created.ComputerID, 3*time.Minute, func(current l1.Computer) bool {
		return computerDisplayPublished(current)
	})
}

func removeAndWaitComputer(t *testing.T, harness *acceptanceHarness, computer l1.Computer, timeout time.Duration) l1.Computer {
	t.Helper()
	startedAt := time.Now()
	removed := runComputerCLI[l1.Computer](t, harness, false, "services", "remove", computer.ComputerID,
		"--expect-current", "--wait", timeout.String(), "--poll-interval", "500ms")
	elapsed := time.Since(startedAt)
	if removed.CurrentJob.State != contract.JobRemovedVerified || removed.CurrentJob.Removal == nil ||
		removed.CurrentJob.Removal.CleanupStatus != l1.ServiceRemovalCleanupAcknowledged ||
		removed.CurrentJob.Removal.CleanupAcknowledgedAt == nil ||
		(removed.RemovalOutcome != "removed_verified" && removed.RemovalOutcome != "removed_reduced") {
		t.Fatalf("Computer removal returned without receipt-derived terminal Slot release: %#v", removed)
	}
	t.Logf("Computer removal reached receipt-derived Slot release in %s (bound %s)", elapsed, timeout)
	return removed
}

func recordComputerAuthority(receipt *linuxComputerMatrixReceipt, computer l1.Computer) {
	receipt.ComputerIDs = appendUnique(receipt.ComputerIDs, computer.ComputerID)
	receipt.JobIDs = appendUnique(receipt.JobIDs, computer.CurrentJobID)
	receipt.AttemptIDs = appendUnique(receipt.AttemptIDs, computer.CurrentJob.CurrentAttemptID)
	receipt.StorageIDs = appendUnique(receipt.StorageIDs, computer.StorageID)
}

func appendUnique(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
