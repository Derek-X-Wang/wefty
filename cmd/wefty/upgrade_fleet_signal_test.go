package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// These processes own temporary databases and plain loopback participants.
// No launchd service or private-network backend is used.
func TestFleetUpgradeImmediateHeartbeatSignalFromRealAgent(t *testing.T) {
	binaries := buildParticipantServices(t)
	home := newFleetTestHome(t)
	f := &serviceParticipantFixture{home: home, path: filepath.Join(home, "profile.json"), role: "l1", socket: filepath.Join(home, "client.sock"), profile: connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-upgrade-signal", L1Address: "127.0.0.1:1", L3Address: "127.0.0.1:1", Identities: connprofile.Identities{Node: "stable-node"}, Participants: map[string]connprofile.Participant{}}}
	for role, name := range map[string]string{"l1": "wefty://control-plane", "l3": "wefty://run-ledger", "node-agent": "wefty://node/stable-node", "client": "wefty://client/stable-node"} {
		state := filepath.Join(home, "fabric", role)
		if err := os.MkdirAll(state, 0700); err != nil {
			t.Fatal(err)
		}
		f.profile.Participants[role] = connprofile.Participant{Name: name, StateDirectory: state, NodeID: "opaque-" + role}
		writeServicePlainIdentity(t, state, f.profile.ServiceParticipant(role).ExpectedIdentity)
	}
	f.write(t)
	l1Ready := filepath.Join(f.home, "l1-ready.json")
	startSavedService(t, binaries, "wefty-l1", f, "--ready-file="+l1Ready)
	address := waitSavedServiceAddress(t, l1Ready)
	f.profile.L1Address = address
	f.write(t)
	l3Ready := filepath.Join(f.home, "l3-ready.json")
	startSavedService(t, binaries, "wefty-l3", f, "--ready-file="+l3Ready)
	f.profile.L3Address = waitSavedServiceAddress(t, l3Ready)
	f.write(t)
	agentReady := filepath.Join(f.home, "agent-ready.json")
	pid := startSavedService(t, binaries, "wefty-agent", f, "--heartbeat-interval=15s", "--claim-interval=1s", "--ready-file="+agentReady)
	deadline := time.Now().Add(10 * time.Second)
	ready := struct {
		PID                int  `json:"pid"`
		ImmediateHeartbeat bool `json:"immediate_heartbeat"`
	}{}
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(agentReady)
		if err == nil && json.Unmarshal(payload, &ready) == nil && ready.PID == pid {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ready.PID != pid || !ready.ImmediateHeartbeat {
		t.Fatal("running agent did not advertise safe immediate-heartbeat support")
	}
	network, err := plain.NewNetworkWithID(f.profile.PlainFabricID)
	if err != nil {
		t.Fatal(err)
	}
	client := network.NewFabric(f.profile.ServiceParticipant(connprofile.ParticipantClient).ExpectedIdentity)
	read := func() l1.Node {
		status, body := savedServiceRequest(t, client, address, http.MethodGet, "/v1/nodes/stable-node", "")
		var node l1.Node
		if status != 200 || json.Unmarshal(body, &node) != nil {
			t.Fatalf("cannot observe agent: %d %s", status, body)
		}
		return node
	}
	before := read()
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	for time.Since(started) < 2*time.Second {
		if node := read(); node.BootSessionID == before.BootSessionID && node.LastHeartbeatAt.After(before.LastHeartbeatAt) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("SIGUSR1 did not publish a heartbeat before the 15 s phase")
}
