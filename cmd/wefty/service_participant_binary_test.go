package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

var participantServicesBuild struct {
	once      sync.Once
	directory string
	output    []byte
	err       error
}

func buildParticipantServices(t *testing.T) string {
	t.Helper()
	participantServicesBuild.once.Do(func() {
		participantServicesBuild.directory = filepath.Join(packageTestDirectory, "participant-services")
		if participantServicesBuild.err = os.MkdirAll(participantServicesBuild.directory, 0o700); participantServicesBuild.err != nil {
			return
		}
		command := exec.Command("go", "build", "-o", participantServicesBuild.directory+string(filepath.Separator), ".", "../wefty-l1", "../wefty-l3", "../wefty-agent")
		command.Env = packageTestEnvironment
		participantServicesBuild.output, participantServicesBuild.err = command.CombinedOutput()
	})
	if participantServicesBuild.err != nil {
		t.Fatalf("build services: %v\n%s", participantServicesBuild.err, participantServicesBuild.output)
	}
	return participantServicesBuild.directory
}

func TestServiceParticipantMissingRecordFromRealBinaries(t *testing.T) {
	binaries := buildParticipantServices(t)
	for _, service := range []string{"wefty-l1", "wefty-l3", "wefty-agent", "wefty"} {
		t.Run(service, func(t *testing.T) {
			home := t.TempDir()
			role := connprofile.ParticipantClient
			if service == "wefty" {
				role = connprofile.ParticipantNodeAgent
			}
			p := connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-role-test", L1Address: "127.0.0.1:0", L3Address: "127.0.0.1:1", Identities: connprofile.Identities{Node: "test"}, Participants: map[string]connprofile.Participant{role: {Name: "wefty://client/test", StateDirectory: savedChoiceState(t, "other"), NodeID: "other"}}}
			if role == connprofile.ParticipantNodeAgent {
				s := p.Participants[role]
				s.Name = "wefty://node/test"
				p.Participants[role] = s
			}
			path := filepath.Join(home, "profile.json")
			if err := connprofile.Write(path, p); err != nil {
				t.Fatal(err)
			}
			args := []string{"--profile=" + path}
			switch service {
			case "wefty-l1", "wefty-l3":
				args = append(args, "--listen=127.0.0.1:0", "--db="+filepath.Join(home, "service.sqlite"))
			case "wefty-agent":
				args = append(args, "--managed-root="+filepath.Join(home, "managed"), "--log-spool-dir="+filepath.Join(home, "spool"))
			case "wefty":
				args = append(args, "--json", "client-service", "--socket="+clientServiceSocketForTest(t))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, filepath.Join(binaries, service), args...)
			cmd.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin"}
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil || err == nil || !strings.Contains(string(output), "saved participant") && !strings.Contains(string(output), "machine_identity_required") {
				t.Errorf("missing record must be typed refusal before starting: err=%v deadline=%v output=%s", err, ctx.Err(), output)
			}
		})
	}
}

// Refusals are checked at the process boundary. A coordinator listener counts
// any accidental attempt to enrol; no backend should be opened in these rows.
func TestServiceParticipantStartupRefusalsFromRealBinaries(t *testing.T) {
	binaries := buildParticipantServices(t)
	var loginAttempts atomic.Int32
	coordinator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		loginAttempts.Add(1)
		http.Error(w, "unexpected login", 500)
	}))
	defer coordinator.Close()
	for _, service := range []string{"wefty-l1", "wefty-l3", "wefty-agent", "wefty"} {
		for _, mode := range []string{"plain", "tsnet"} {
			cases := []string{"missing-state", "auth-key", "auth-key-env", "auth-key-alias-env", "control-url", "control-url-env", "ephemeral", "state-dir", "empty-state-dir", "fabric-name", "mode"}
			if mode == "plain" {
				cases = append(cases, "wrong-id", "wrong-tag", "extra-tag")
			}
			if service == "wefty-l1" || service == "wefty-l3" {
				cases = append(cases, "peer-id")
			}
			for _, name := range cases {
				t.Run(service+"/"+mode+"/"+name, func(t *testing.T) {
					f := newServiceParticipantFixture(t, service)
					f.profile.Fabric = mode
					var flags, env []string
					record := f.profile.Participants[f.role]
					switch name {
					case "missing-state":
						if err := os.Remove(filepath.Join(record.StateDirectory, "plain-identity.json")); err != nil {
							t.Fatal(err)
						}
					case "auth-key":
						flags = []string{"--auth-key=DO-NOT-PRINT-JOIN-KEY"}
					case "auth-key-env":
						env = []string{"TS_AUTHKEY=DO-NOT-PRINT-JOIN-KEY"}
					case "auth-key-alias-env":
						env = []string{"TS_AUTH_KEY=DO-NOT-PRINT-JOIN-KEY"}
					case "control-url":
						flags = []string{"--control-url=" + coordinator.URL}
					case "control-url-env":
						env = []string{"TS_CONTROL_URL=" + coordinator.URL}
					case "peer-id":
						if service == "wefty-l1" {
							flags = []string{"--run-ledger-node-id=foreign-ledger"}
						} else {
							flags = []string{"--control-plane-node-id=foreign-control"}
						}
					case "ephemeral":
						flags = []string{"--ephemeral"}
					case "state-dir":
						flags = []string{"--state-dir=" + filepath.Join(f.home, "other")}
					case "empty-state-dir":
						flags = []string{"--state-dir="}
					case "fabric-name":
						flags = []string{"--fabric-name=wefty://client/other"}
					case "mode":
						if mode == "tsnet" {
							flags = []string{"--fabric=plain"}
						} else {
							flags = []string{"--fabric=tsnet"}
						}
					case "wrong-id", "wrong-tag", "extra-tag":
						identity := f.profile.ServiceParticipant(f.role).ExpectedIdentity
						if name == "wrong-id" {
							identity.NodeID = "replacement"
						} else if name == "wrong-tag" {
							identity.Tags = []string{"tag:wrong-role"}
						} else {
							identity.Tags = append(identity.Tags, "tag:wefty-agent")
						}
						writeServicePlainIdentity(t, record.StateDirectory, identity)
					}
					f.write(t)
					args := f.arguments(service, flags...)
					ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
					defer cancel()
					command := exec.CommandContext(ctx, filepath.Join(binaries, service), args...)
					command.Env = append([]string{"HOME=" + f.home, "PATH=/usr/bin:/bin"}, env...)
					output, err := command.CombinedOutput()
					wanted := "saved participant"
					expectedExit := 1
					if service == "wefty" {
						wanted = "machine_identity_required"
						expectedExit = 3
					}
					var exited *exec.ExitError
					if ctx.Err() != nil || !errors.As(err, &exited) || exited.ExitCode() != expectedExit || !strings.Contains(string(output), wanted) || strings.Contains(string(output), "DO-NOT-PRINT") || strings.Contains(string(output), "listening on") {
						t.Fatalf("refusal exit=%v deadline=%v output=%s", err, ctx.Err(), output)
					}
					if _, err := os.Stat(filepath.Join(f.home, "service.sqlite")); !os.IsNotExist(err) {
						t.Fatalf("refused service opened database: %v", err)
					}
				})
			}
		}
	}
	if loginAttempts.Load() != 0 {
		t.Fatalf("refused services attempted login %d times", loginAttempts.Load())
	}
}

type serviceParticipantFixture struct {
	home, path, role, socket string
	profile                  connprofile.Profile
}

func newServiceParticipantFixture(t *testing.T, service string) *serviceParticipantFixture {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &serviceParticipantFixture{home: home, path: filepath.Join(home, "profile.json"), socket: clientServiceSocketForTest(t), profile: connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-saved-services", L1Address: "127.0.0.1:1", L3Address: "127.0.0.1:1", Identities: connprofile.Identities{Node: "stable-node"}, Participants: map[string]connprofile.Participant{}}}
	for role, name := range map[string]string{"l1": "wefty://control-plane", "l3": "wefty://run-ledger", "node-agent": "wefty://node/stable-node", "client": "wefty://client/stable-node"} {
		state := filepath.Join(home, "fabric", role)
		if err := os.MkdirAll(state, 0o700); err != nil {
			t.Fatal(err)
		}
		f.profile.Participants[role] = connprofile.Participant{Name: name, StateDirectory: state, NodeID: "opaque-" + role}
		writeServicePlainIdentity(t, state, f.profile.ServiceParticipant(role).ExpectedIdentity)
	}
	f.role = map[string]string{"wefty-l1": "l1", "wefty-l3": "l3", "wefty-agent": "node-agent", "wefty": "client"}[service]
	return f
}
func writeServicePlainIdentity(t *testing.T, directory string, identity fabric.Identity) {
	t.Helper()
	payload, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "plain-identity.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
func (f *serviceParticipantFixture) write(t *testing.T) {
	t.Helper()
	if err := connprofile.Write(f.path, f.profile); err != nil {
		t.Fatal(err)
	}
}
func (f *serviceParticipantFixture) arguments(service string, overrides ...string) []string {
	args := append([]string{"--profile=" + f.path}, overrides...)
	switch service {
	case "wefty-l1", "wefty-l3":
		args = append(args, "--listen=127.0.0.1:0", "--db="+filepath.Join(f.home, "service.sqlite"))
	case "wefty-agent":
		args = append(args, "--managed-root="+filepath.Join(f.home, "managed"), "--log-spool-dir="+filepath.Join(f.home, "spool"), "--handoff-root="+filepath.Join(f.home, "handoff"))
	case "wefty":
		args = append(args, "--json", "client-service", "--socket="+f.socket)
	}
	return args
}

func TestServiceParticipantsStartAndAuthorizeEnrolledPeersFromRealBinaries(t *testing.T) {
	binaries := buildParticipantServices(t)
	f := newServiceParticipantFixture(t, "wefty-l1")
	personState := filepath.Join(f.home, "fabric", "person")
	if err := os.MkdirAll(personState, 0o700); err != nil {
		t.Fatal(err)
	}
	f.profile.Participants[connprofile.ParticipantPerson] = connprofile.Participant{Name: "wefty://person/stable-node", StateDirectory: personState, NodeID: "opaque-person", UserID: "owner"}
	writeServicePlainIdentity(t, personState, f.profile.ServiceParticipant(connprofile.ParticipantPerson).ExpectedIdentity)
	f.write(t)
	l1Ready := filepath.Join(f.home, "l1-ready.json")
	l1PID := startSavedService(t, binaries, "wefty-l1", f, "--ready-file="+l1Ready)
	l1Address := waitSavedServiceAddress(t, l1Ready)
	assertSavedReadyProcess(t, l1Ready, l1PID, "", "")
	f.profile.L1Address = l1Address
	f.write(t)
	// L3 owns a separate database but reads the very same enrolled profile.
	l3Ready := filepath.Join(f.home, "l3-ready.json")
	l3PID := startSavedService(t, binaries, "wefty-l3", f, "--ready-file="+l3Ready)
	l3Address := waitSavedServiceAddress(t, l3Ready)
	assertSavedReadyProcess(t, l3Ready, l3PID, "", "")
	f.profile.L3Address = l3Address
	f.write(t)
	agentReady := filepath.Join(f.home, "agent-ready.json")
	agentPID := startSavedService(t, binaries, "wefty-agent", f, "--heartbeat-interval=1s", "--claim-interval=1s", "--handoff-root="+filepath.Join(f.home, "handoff"), "--ready-file="+agentReady)
	startSavedService(t, binaries, "wefty", f)

	network, err := plain.NewNetworkWithID(f.profile.PlainFabricID)
	if err != nil {
		t.Fatal(err)
	}
	client := network.NewFabric(f.profile.ServiceParticipant(connprofile.ParticipantClient).ExpectedIdentity)
	var node l1.Node
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, body := savedServiceRequest(t, client, l1Address, http.MethodGet, "/v1/nodes/stable-node", "")
		if status == 200 && json.Unmarshal(body, &node) == nil && node.BootSessionID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if node.BootSessionID == "" {
		t.Fatal("matching saved node-agent identity never registered")
	}
	assertSavedReadyProcess(t, agentReady, agentPID, "stable-node", node.BootSessionID)
	// The live node registration must be bound to the agent's opaque identity.
	proof, _ := json.Marshal(l1.HostBootSessionProof{HostIdentityNodeID: f.profile.Participants[connprofile.ParticipantNodeAgent].NodeID, HostStableNodeID: "stable-node", BootSessionID: node.BootSessionID})
	for _, tc := range []struct {
		name, id string
		status   int
	}{{"enrolled-ledger", "opaque-l3", 200}, {"other-client", "other-client", 403}, {"legacy-default", "run-ledger", 403}} {
		t.Run("L1/"+tc.name, func(t *testing.T) {
			peer := network.NewFabric(fabric.Identity{NodeID: tc.id, Tags: []string{"tag:wefty-client"}})
			status, body := savedServiceRequest(t, peer, l1Address, http.MethodPost, "/v1/host-boot-session-proof", string(proof))
			if status != tc.status {
				t.Fatalf("status=%d want=%d body=%s", status, tc.status, body)
			}
		})
	}
	for _, tc := range []struct {
		name, id string
		status   int
	}{{"enrolled-control", "opaque-l1", 200}, {"other-control", "other-control", 403}, {"legacy-default", "control-plane", 403}} {
		t.Run("L3/"+tc.name, func(t *testing.T) {
			peer := network.NewFabric(fabric.Identity{NodeID: tc.id, Tags: []string{"tag:wefty-control"}})
			status, body := savedServiceRequest(t, peer, l3Address, http.MethodPost, "/v1/computer-token/revoke", `{"computer_id":"test-computer","revoke_all":true,"reason":"computer_stopped"}`)
			if status != tc.status {
				t.Fatalf("status=%d want=%d body=%s", status, tc.status, body)
			}
		})
	}
	// The CLI reaches L1 through the actual client service, proving that the
	// fourth binary entry point accepted and owns its matching saved backend.
	var code int
	var output string
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		code, output = runWeftyWithEnv(t, filepath.Join(binaries, "wefty"), 3*time.Second, []string{"HOME=" + f.home, "WEFTY_CLIENT_SOCKET=" + f.socket}, "--profile="+f.path, "--json", "nodes", "list")
		if code == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != 0 || !strings.Contains(output, "stable-node") {
		t.Fatalf("client service cannot dial as its saved participant: exit=%d output=%s", code, output)
	}
	person, err := local.New(f.socket, local.Person, "opaque-person")
	if err != nil {
		t.Fatal(err)
	}
	if err := person.Check(t.Context()); err != nil {
		t.Fatalf("matching client overrides disabled optional person: %v", err)
	}
}

func startSavedService(t *testing.T, binaries, service string, f *serviceParticipantFixture, extra ...string) int {
	t.Helper()
	role := map[string]string{"wefty-l1": "l1", "wefty-l3": "l3", "wefty-agent": "node-agent", "wefty": "client"}[service]
	saved := f.profile.Participants[role]
	args := f.arguments(service, "--fabric-name="+saved.Name, "--state-dir="+saved.StateDirectory, "--ephemeral=false")
	// Extra flags precede client-service, after service defaults for daemons.
	if service == "wefty" && len(extra) > 0 {
		t.Fatal("client service helper has no extra global flags")
	}
	args = append(args, extra...)
	// The agent's database is separate; L1 and L3 must not share SQLite state.
	if service == "wefty-l3" {
		args = append(args, "--db="+filepath.Join(f.home, "ledger.sqlite"))
	}
	logFile, err := os.CreateTemp(f.home, service+"-log-")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(filepath.Join(binaries, service), args...)
	command.Env = []string{"HOME=" + f.home, "PATH=/usr/bin:/bin"}
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-done:
			if err != nil {
				payload, _ := os.ReadFile(logFile.Name())
				t.Logf("%s exit=%v log=%s", service, err, payload)
			}
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
		_ = logFile.Close()
	})
	return command.Process.Pid
}
func waitSavedServiceAddress(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(path)
		var ready struct {
			Address string `json:"address"`
		}
		if err == nil && json.Unmarshal(payload, &ready) == nil && ready.Address != "" {
			return ready.Address
		}
		time.Sleep(20 * time.Millisecond)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	var logs string
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-log-") {
			payload, _ := os.ReadFile(filepath.Join(filepath.Dir(path), entry.Name()))
			logs += string(payload)
		}
	}
	t.Fatalf("service never became ready: %s", logs)
	return ""
}
func savedServiceRequest(t *testing.T, peer fabric.Fabric, address, method, path, body string) (int, []byte) {
	t.Helper()
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return peer.Dial(ctx, network, address)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	request, err := http.NewRequestWithContext(t.Context(), method, "http://service"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, payload
}

func TestServiceParticipantStatePreflightDoesNotOpenParticipant(t *testing.T) {
	binaries := buildParticipantServices(t)
	for _, service := range []string{"wefty-l1", "wefty-l3"} {
		t.Run(service, func(t *testing.T) {
			f := newServiceParticipantFixture(t, service)
			f.write(t)
			record := f.profile.Participants[f.role]
			release, err := participantstate.Acquire(record.StateDirectory, record.Name)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			command := exec.Command(filepath.Join(binaries, service), "--profile="+f.path, "--state-preflight", "--db="+filepath.Join(f.home, "preflight.sqlite"))
			command.Env = []string{"HOME=" + f.home, "PATH=/usr/bin:/bin"}
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("preflight touched a locked participant: %v %s", err, output)
			}
		})
	}
}

func assertSavedReadyProcess(t *testing.T, path string, pid int, nodeID, boot string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var ready struct {
			PID           int    `json:"pid"`
			NodeID        string `json:"node_id"`
			BootSessionID string `json:"boot_session_id"`
		}
		b, err := os.ReadFile(path)
		if err == nil && json.Unmarshal(b, &ready) == nil && ready.PID == pid && ready.NodeID == nodeID && ready.BootSessionID == boot {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("service did not publish its own ready process: %s pid=%d", filepath.Base(path), pid)
}
