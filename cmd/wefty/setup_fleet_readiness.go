package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

type setupReadinessCheck struct {
	Check  string `json:"check"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

func setupFleetClients(ctx context.Context, paths setupPaths, profile connprofile.Profile) (*apiClients, error) {
	dialer, err := local.New(local.DefaultSocketPath(paths.home), local.Client, profile.Participants[connprofile.ParticipantClient].NodeID)
	if err != nil {
		return nil, err
	}
	if err := dialer.Check(ctx); err != nil {
		return nil, err
	}
	return newAPIClients(dialer, profile.L1Address, profile.L3Address)
}

// Poll only existing observers; never open a service's participant state.
// One shared budget bounds startup, followed by a separate two-minute smoke.
func checkFleetReadiness(ctx context.Context, host setupHost, paths setupPaths, p connprofile.Profile, services []launchagent.Agent, skipSmoke bool, role serviceplan.Role) []setupReadinessCheck {
	budget := host.readinessBudget
	if budget <= 0 {
		budget = 30 * time.Second
	}
	bounded, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	bootTime := host.bootTime
	if bootTime == nil {
		bootTime = setupSystemBootTime
	}
	bootedAt, bootErr := bootTime()
	checks := make([]setupReadinessCheck, len(services))
	for i, service := range services {
		checks[i] = setupReadinessCheck{Check: strings.TrimPrefix(service.Label, "dev.wefty.") + " ready", Status: "FAILED", Detail: "service has not reached its saved-identity ready point"}
	}
	checks = append(checks, setupReadinessCheck{Check: "client → L1", Status: "NOT-RUN", Detail: "client service not ready"}, setupReadinessCheck{Check: "client → L3", Status: "NOT-RUN", Detail: "client service not ready"}, setupReadinessCheck{Check: "node registration and admission", Status: "NOT-RUN", Detail: "L1 not yet reachable"})
	l1Index, l3Index, nodeIndex := len(services), len(services)+1, len(services)+2
	peerIndex := len(checks)
	checks = append(checks, setupReadinessCheck{Check: "L1 → L3 direction", Status: "NOT-RUN", Detail: "client service not ready"}, setupReadinessCheck{Check: "L3 → L1 direction", Status: "NOT-RUN", Detail: "client service not ready"})
	manager := fleetServiceManager{host: host}
	open := host.fleetClients
	if open == nil {
		open = setupFleetClients
	}
	var clients *apiClients
	defer func() {
		if clients != nil {
			clients.close()
		}
	}()
	for {
		currentBoot := ""
		for i, service := range services {
			probe, done := context.WithTimeout(bounded, statusProbeBudget)
			state, err := manager.state(probe, service.Label)
			done()
			checks[i].Status = "FAILED"
			if err != nil {
				checks[i].Detail = manager.readinessDetail("cannot verify LaunchAgent state")
				continue
			}
			if !state.Running {
				checks[i].Detail = manager.readinessDetail("LaunchAgent is not running")
				continue
			}
			if strings.HasSuffix(service.Label, "client-service") && role == serviceplan.ControlPlane {
				continue
			}
			var metadata serviceready.Metadata
			payload, err := os.ReadFile(filepath.Join(paths.data, "setup-ready", strings.TrimPrefix(service.Label, "dev.wefty.")+".json"))
			if err != nil || json.Unmarshal(payload, &metadata) != nil || metadata.PID <= 0 || metadata.PID != state.PID {
				checks[i].Detail = "current process has not published its ready point"
				continue
			}
			if bootErr != nil || bootedAt.IsZero() {
				checks[i].Detail = "cannot verify system boot time for the ready point"
				continue
			}
			if !metadata.ReadyAt.After(bootedAt) {
				checks[i].Detail = "ready point is missing its timestamp or predates the current system boot"
				continue
			}
			if !strings.HasSuffix(service.Label, "node-agent") && metadata.Address == "" {
				checks[i].Detail = "service has not published its listener"
				continue
			}
			if strings.HasSuffix(service.Label, "node-agent") && (metadata.NodeID != p.Identities.Node || metadata.BootSessionID == "") {
				checks[i].Detail = "node agent ready record does not match this node"
				continue
			}
			if strings.HasSuffix(service.Label, "node-agent") {
				currentBoot = metadata.BootSessionID
			}
			checks[i].Status, checks[i].Detail = "VERIFIED", manager.readinessDetail("LaunchAgent running; current process reached its saved-identity ready point")
		}

		for _, index := range []int{l1Index, l3Index, nodeIndex, peerIndex, peerIndex + 1} {
			checks[index].Status, checks[index].Detail = "NOT-RUN", "client service identity not verified in this pass"
		}
		if clients != nil {
			clients.close()
			clients = nil
		}
		probe, done := context.WithTimeout(bounded, statusProbeBudget)
		var openErr error
		clients, openErr = open(probe, paths, p)
		if openErr != nil {
			if clients != nil {
				clients.close()
				clients = nil
			}
			checks[len(services)-1].Detail = "client service is not answering as its saved client"
		}
		done()
		if clients != nil {
			if state, err := manager.state(bounded, services[len(services)-1].Label); err == nil && state.Running && (role == serviceplan.ControlPlane || checks[len(services)-1].Status == "VERIFIED") {
				checks[len(services)-1].Status, checks[len(services)-1].Detail = "VERIFIED", manager.readinessDetail("LaunchAgent running; client service answered the saved-client identity check")
			}
			probe, done := context.WithTimeout(bounded, statusProbeBudget)
			nodes, err := clients.listNodes(probe)
			done()
			if err != nil {
				checks[l1Index].Status, checks[l1Index].Detail = "FAILED", "client cannot read L1 nodes"
				checks[nodeIndex].Status, checks[nodeIndex].Detail = "NOT-RUN", "current node admission could not be read from L1"
			} else {
				checks[l1Index].Status, checks[l1Index].Detail = "VERIFIED", "saved client read L1 nodes through the client service"
				checks[nodeIndex].Status, checks[nodeIndex].Detail = "FAILED", "this node is not alive and admitted with its configured policy"
				if role == serviceplan.Join {
					checks[nodeIndex].Detail = "this node is not automatically admitted with claims enabled, its only Pinned tag and default slots. Ask the control-plane owner to check automatic admission; an identity already bound to another Node needs the owner set-claims action and explicit Pinned policy. To retire a machine, disable claims or drain it, then revoke its network identity. Joining setup never enables claims."
				}
				for _, node := range nodes.Nodes {
					admitted := setupNodeAdmitted(p, node)
					if role == serviceplan.Join {
						admitted = setupJoinNodeAdmitted(p, node)
					}
					if currentBoot != "" && node.BootSessionID == currentBoot && admitted {
						checks[nodeIndex].Status, checks[nodeIndex].Detail = "VERIFIED", "stable node is alive, claims enabled, configured admission and reserved Pinned tag match"
						if role == serviceplan.Join {
							checks[nodeIndex].Detail = "stable node is alive, automatically admitted, claims enabled, with only its Pinned tag and default slots"
						}
					}
				}
			}
			probe, done = context.WithTimeout(bounded, statusProbeBudget)
			_, err = clients.listRuns(probe, string(contract.RunQueued), 1)
			done()
			if err != nil {
				checks[l3Index].Status, checks[l3Index].Detail = "FAILED", "client cannot read L3 runs"
			} else {
				checks[l3Index].Status, checks[l3Index].Detail = "VERIFIED", "saved client read L3 runs through the client service"
			}
			for i, client := range []*apiClient{clients.l1, clients.l3} {
				probe, done := context.WithTimeout(bounded, statusProbeBudget)
				checks[peerIndex+i], _ = readFleetPeerHealth(probe, client, checks[peerIndex+i].Check)
				done()
			}
		}
		ready := true
		for i, check := range checks {
			if i >= peerIndex && check.Status == "NOT-RUN" && check.Detail == peerProbeUnavailable {
				continue
			}
			if check.Status != "VERIFIED" {
				ready = false
			}
		}
		if ready || bounded.Err() != nil {
			break
		}
		if waitForContext(bounded, 100*time.Millisecond) != nil {
			break
		}
	}
	smoke := setupReadinessCheck{Check: "smoke run", Status: "NOT-RUN", Detail: "startup checks did not pass"}
	ready := true
	for _, check := range checks {
		if check.Status == "FAILED" {
			ready = false
		}
	}
	if skipSmoke {
		smoke.Detail = "skipped by --skip-smoke-run"
	} else if ready && clients != nil {
		budget := host.smokeBudget
		if budget <= 0 {
			budget = 2 * time.Minute
		}
		probe, done := context.WithTimeout(ctx, budget)
		runID, err := runSetupSmoke(probe, clients, p.Identities.Node)
		done()
		if err != nil {
			smoke.Status, smoke.Detail = "FAILED", fmt.Sprintf("pinned smoke run %s did not succeed; installation stays published; rerun setup after repair", runID)
		} else {
			smoke.Status, smoke.Detail = "VERIFIED", "pinned run "+runID+" succeeded: client → L3, L3 → L1 dispatch, node claim and completion"
		}
	}
	return append(checks, smoke)
}

func setupNodeAdmitted(p connprofile.Profile, node l1.Node) bool {
	if node.NodeID != p.Identities.Node || node.State != contract.NodeAlive || !node.ClaimsEnabled || node.AutomaticallyAdmitted {
		return false
	}
	for _, policy := range p.NodeAdmission {
		if policy.NodeID == node.NodeID {
			return node.MaxOneshotSlots == policy.MaxOneshotSlots && node.MaxServiceSlots == policy.MaxServiceSlots && slices.Equal(l1.NormalizeTags(node.AuthoritativeTags), l1.NormalizeTags(policy.Tags))
		}
	}
	return false
}
func setupJoinNodeAdmitted(p connprofile.Profile, node l1.Node) bool {
	return node.NodeID == p.Identities.Node && node.State == contract.NodeAlive && node.ClaimsEnabled && node.AutomaticallyAdmitted && node.MaxOneshotSlots == l1.DefaultMaxOneshotSlots && node.MaxServiceSlots == l1.DefaultMaxServiceSlots && slices.Equal(node.AuthoritativeTags, []string{contract.StableNodeTagPrefix + p.Identities.Node})
}

func runSetupSmoke(ctx context.Context, clients *apiClients, nodeID string) (runID string, result error) {
	const script = "#!/bin/sh\ntrue\n"
	digest := sha256.Sum256([]byte(script))
	// A new key per check allows a rerun to test the current stack, rather than
	// replay an earlier successful run. The run itself has a short runtime cap.
	key, err := randomHex(rand.Reader, 16)
	if err != nil {
		return "", err
	}
	accepted, err := clients.submitRun(ctx, l3.CreateRunRequest{InlineScript: &l3.InlineScriptInput{Content: script, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}, Params: json.RawMessage(`{}`), Tags: []string{contract.StableNodeTagPrefix + nodeID}, Limits: &contract.RunLimits{MaxRuntimeSeconds: 30}}, "setup-smoke-"+key)
	if err != nil {
		return "", err
	}
	if accepted.RunID == "" {
		return "", errors.New("smoke acceptance omitted the run ID")
	}
	runID = accepted.RunID
	settled := false
	defer func() {
		if result != nil && !settled {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), statusProbeBudget)
			defer cancel()
			var record contract.RunRecord
			cancelErr := clients.l3.do(cleanup, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/cancel", nil, nil, &record, http.StatusOK)
			if cancelErr != nil {
				result = errors.Join(result, errors.New("smoke cancellation was not confirmed"))
			}
		}
	}()

	for {
		run, err := clients.getRun(ctx, accepted.RunID)
		if err != nil {
			return accepted.RunID, err
		}
		if run.Status == contract.RunSucceeded {
			if run.NodeID == "" {
				if err := waitForContext(ctx, 250*time.Millisecond); err != nil {
					return accepted.RunID, err
				}
				continue
			}
			settled = true
			if run.NodeID != nodeID {
				return accepted.RunID, errors.New("smoke ran on a different node")
			}
			return accepted.RunID, nil
		}
		if run.Status == contract.RunFailed {
			settled = true
			return accepted.RunID, errors.New("smoke run failed")
		}
		if err := waitForContext(ctx, 250*time.Millisecond); err != nil {
			return accepted.RunID, err
		}
	}
}
