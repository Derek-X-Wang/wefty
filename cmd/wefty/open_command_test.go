package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/l1"
)

func openServiceFixture(t *testing.T, port int) (*apiClients, *l1.Job) {
	return openServiceFixtureWithHost(t, port, "wefty-studio-2")
}

func openServiceFixtureWithHost(t *testing.T, port int, host string) (*apiClients, *l1.Job) {
	return openServiceFixtureWithNode(t, port, host, http.StatusOK)
}

func openServiceFixtureWithNode(t *testing.T, port int, host string, nodeStatus int) (*apiClients, *l1.Job) {
	t.Helper()
	job := &l1.Job{JobID: "service-1", State: contract.JobRunning, Spec: contract.JobSpec{Class: contract.JobClassService, PublishedPort: &port}, ServiceJob: &l1.ServiceJob{PublishedPort: &port}}
	job.BoundNodeID = "studio-0123abcd"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/jobs/service-1":
			_ = json.NewEncoder(w).Encode(job)
		case "/v1/nodes/studio-0123abcd":
			if nodeStatus != http.StatusOK {
				writeStubAPIError(w, nodeStatus, contract.ErrorUnavailable, "node lookup unavailable", true)
				return
			}
			_ = json.NewEncoder(w).Encode(l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: job.BoundNodeID, ConnectHost: host}})
		default:
			t.Errorf("unexpected L1 request: %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	clients, err := newAPIClients(openTestFabric{dial: (&net.Dialer{}).DialContext}, strings.TrimPrefix(server.URL, "http://"), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clients.close)
	clients.servicePerson = clients.fabric
	return clients, job
}

func TestOpenRefusesMissingNetworkGrant(t *testing.T) {
	clients, _ := openServiceFixtureWithHost(t, 18480, "")
	clients.participantProfile = true
	for _, command := range [][]string{{"open", "service-1", "--no-browser", "--local-port=0"}} {
		var out bytes.Buffer
		err := execute(context.Background(), clients, true, command, &out, io.Discard)
		if commandExitCode(err) != 5 || !strings.Contains(err.Error(), "registered network name") || out.Len() != 0 {
			t.Fatalf("%v: err=%v output=%s", command, err, &out)
		}
	}
}

func TestServiceStatusFleetUnavailableAddress(t *testing.T) {
	for _, tc := range []struct {
		name, host string
		nodeStatus int
	}{
		{"node-read-failed", "wefty-studio-2", http.StatusServiceUnavailable},
		{"empty-grant", "", http.StatusOK},
		{"invalid-grant", "invalid name", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clients, _ := openServiceFixtureWithNode(t, 18480, tc.host, tc.nodeStatus)
			clients.participantProfile = true
			for _, jsonOutput := range []bool{false, true} {
				var out bytes.Buffer
				err := executeServiceStatus(context.Background(), clients, jsonOutput, []string{"service-1"}, &out)
				if err != nil {
					t.Fatalf("status failed: %v", err)
				}
				if jsonOutput {
					var got map[string]any
					if err := json.Unmarshal(out.Bytes(), &got); err != nil {
						t.Fatal(err)
					}
					if got["job_id"] != "service-1" || got["state"] != "running" {
						t.Fatalf("normal status lost: %s", &out)
					}
					if _, present := got["network_url"]; present {
						t.Fatalf("unavailable address emitted: %s", &out)
					}
				} else if !strings.Contains(out.String(), "service-1") || !strings.Contains(out.String(), "address from other devices: unavailable (") {
					t.Fatalf("missing status or unavailable reason: %s", &out)
				}
			}
			var out bytes.Buffer
			err := execute(context.Background(), clients, true, []string{"open", "service-1", "--local-port=0", "--no-browser"}, &out, io.Discard)
			wantExit := 5
			if tc.nodeStatus != http.StatusOK {
				wantExit = 13
			}
			if commandExitCode(err) != wantExit || out.Len() != 0 {
				t.Fatalf("open refusal changed: %v output=%s", err, &out)
			}
		})
	}
}

func TestOpenFleetPersonTunnelConcurrentAndShutdown(t *testing.T) {
	for _, mode := range []string{"context-free-port", "context-default-port", "SIGINT", "SIGTERM"} {
		t.Run(mode, func(t *testing.T) { testOpenFleetTunnel(t, mode) })
	}
}

func testOpenFleetTunnel(t *testing.T, mode string) {
	t.Helper()
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remotePort := reservation.Addr().(*net.TCPAddr).Port
	reservation.Close()
	ledger := realOpenServiceLedger(t, remotePort)
	var arrived atomic.Int32
	concurrent := make(chan struct{})
	holdClosed := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/request-") {
			if arrived.Add(1) == 8 {
				close(concurrent)
			}
			select {
			case <-concurrent:
			case <-r.Context().Done():
				return
			}
		}
		if r.URL.Path == "/hold" {
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(holdClosed)
			return
		}
		_, _ = fmt.Fprint(w, "echo:"+r.URL.Path)
	}))
	t.Cleanup(backend.Close)
	socket := clientServiceSocketForTest(t)
	t.Setenv("WEFTY_CLIENT_SOCKET", socket)
	var machineDials, serviceDials atomic.Int32
	var failNext atomic.Bool
	person := openTestFabric{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == fmt.Sprintf("wefty-studio-2:%d", remotePort) {
			serviceDials.Add(1)
			if failNext.Swap(false) {
				return nil, fmt.Errorf("injected peer dial failure")
			}
			address = strings.TrimPrefix(backend.URL, "http://")
		} else {
			return ledger.person.Dial(ctx, network, address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	server, err := local.NewServer(socket, map[string]local.Participant{
		local.Client: {NodeID: "client", Check: func(context.Context) error { return nil }, Fabric: openTestFabric{dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != ledger.address {
				machineDials.Add(1)
				return nil, fmt.Errorf("machine must not dial published ports")
			}
			return ledger.client.Dial(ctx, network, address)
		}}},
		local.Person: {NodeID: "person", Check: func(context.Context) error { return nil }, Fabric: person},
	})
	if err != nil {
		t.Fatal(err)
	}
	serviceCtx, stopService := context.WithCancel(context.Background())
	serviceDone, serviceReady := make(chan error, 1), make(chan struct{})
	go func() { serviceDone <- server.ServeReady(serviceCtx, func() error { close(serviceReady); return nil }) }()
	t.Cleanup(func() {
		stopService()
		if err := <-serviceDone; err != nil {
			t.Error(err)
		}
	})
	select {
	case <-serviceReady:
	case err := <-serviceDone:
		t.Fatalf("client service: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("client service not ready")
	}
	profile := filepath.Join(t.TempDir(), "profile.json")
	if err := connprofile.Write(profile, connprofile.Profile{Version: 1, Fabric: "plain", L1Address: ledger.address, Participants: map[string]connprofile.Participant{
		"client": {Name: "wefty://client/studio", NodeID: "client", StateDirectory: savedChoiceState(t, "client")},
		"person": {Name: "wefty://person/studio", NodeID: "person", UserID: "owner", StateDirectory: savedChoiceState(t, "person")},
	}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	args := []string{"--profile", profile, "--json", "open", ledger.jobID, "--no-browser"}
	if mode != "context-default-port" {
		args = append(args, "--local-port", "0")
	}
	done := make(chan struct{})
	var runErr error
	stopOpen := cancel
	var process *exec.Cmd
	if strings.HasPrefix(mode, "SIG") {
		process = exec.Command(buildWefty(t), args...)
		process.Stdout, process.Stderr = writer, io.Discard
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		signal := os.Interrupt
		if mode == "SIGTERM" {
			signal = syscall.SIGTERM
		}
		stopOpen = func() { _ = process.Process.Signal(signal) }
	}
	go func() {
		if process != nil {
			runErr = process.Wait()
		} else {
			runErr = run(ctx, args, writer, io.Discard)
		}
		_ = writer.Close()
		close(done)
	}()
	t.Cleanup(func() {
		stopOpen()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			if process != nil {
				_ = process.Process.Kill()
			}
			t.Error("open did not stop")
		}
	})
	line := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		if scanner.Scan() {
			line <- scanner.Text()
		} else {
			line <- ""
		}
	}()
	var ready struct {
		Event, Service, Node string
		LocalURL             string `json:"local_url"`
		RemotePort           int    `json:"remote_port"`
	}
	select {
	case value := <-line:
		if json.Unmarshal([]byte(value), &ready) != nil || ready.Event != "ready" || ready.Service != ledger.jobID || ready.Node != "studio-0123abcd" || ready.RemotePort != remotePort || !strings.HasPrefix(ready.LocalURL, "http://127.0.0.1:") {
			select {
			case <-done:
				t.Fatalf("open before ready: %v; ready=%q", runErr, value)
			default:
			}
			t.Fatalf("ready: %q", value)
		}
		if mode == "context-default-port" && ready.LocalURL != fmt.Sprintf("http://127.0.0.1:%d", remotePort) {
			t.Fatalf("default port changed: %s", ready.LocalURL)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("open not ready")
	}
	httpClient := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	get := func(path string) error {
		response, err := httpClient.Get(ready.LocalURL + path)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		if string(body) != "echo:"+path {
			return fmt.Errorf("unexpected body %q", body)
		}
		return nil
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := get(fmt.Sprintf("/request-%d", i)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	failNext.Store(true)
	if err := get("/fail"); err == nil {
		t.Error("injected dial failure succeeded")
	}
	if err := get("/after-failure"); err != nil {
		t.Errorf("tunnel stopped after one failed dial: %v", err)
	}
	if machineDials.Load() != 0 || serviceDials.Load() != 10 {
		t.Errorf("machine dials=%d service dials=%d", machineDials.Load(), serviceDials.Load())
	}
	held, err := httpClient.Get(ready.LocalURL + "/hold")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()
	idle, err := net.DialTimeout("tcp", strings.TrimPrefix(ready.LocalURL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	stopOpen()
	select {
	case <-done:
		if runErr != nil {
			t.Error(runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("open did not stop")
	}
	select {
	case <-holdClosed:
	case <-time.After(5 * time.Second):
		t.Error("upstream connection survived shutdown")
	}
	_ = idle.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := idle.Read(make([]byte, 1)); err == nil {
		t.Error("active connection survived shutdown")
	}
	if conn, err := net.DialTimeout("tcp", strings.TrimPrefix(ready.LocalURL, "http://"), time.Second); err == nil {
		conn.Close()
		t.Error("port remained open after shutdown")
	}
}

type openLedger struct {
	address, jobID string
	client, person fabric.Fabric
}

// Fake authenticated peer evidence feeds the real L1 Handler and its real
// authorizeJobProtocol/authorize routes. No authorization is reimplemented.
type openAuthenticatedFabric struct {
	fabric.Fabric
	peers sync.Map
}

func (f *openAuthenticatedFabric) WhoIs(_ context.Context, address string) (fabric.Identity, error) {
	if identity, ok := f.peers.Load(address); ok {
		return identity.(fabric.Identity), nil
	}
	return fabric.Identity{}, fabric.ErrIdentityNotFound
}

func (*openAuthenticatedFabric) PersonIdentityTrust() fabric.PersonIdentityTrust {
	return fabric.PersonIdentityAuthenticated
}

func realOpenServiceLedger(t *testing.T, port int) openLedger {
	t.Helper()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	nodeIdentity := fabric.Identity{NodeID: "agent", Kind: fabric.IdentityKindMachine}
	node, err := store.RegisterNode(ctx, nodeIdentity, contract.NodeRegistration{
		NodeID: "studio-0123abcd", ConnectHost: "wefty-studio-2", BootSessionID: "boot", RootInstanceID: "root",
		OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
	}, l1.NodePolicy{Tags: []string{"open-test"}, MaxServiceSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	spec := nodeCLIJobSpec("real-open", contract.JobClassService, "open-test")
	spec.PublishedPort = &port
	job, _, err := store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(ctx, nodeIdentity.NodeID, node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v %v", claim, err)
	}
	if _, err := store.StartAttempt(ctx, nodeIdentity.NodeID, job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	ready := true
	if _, err := store.SetAttemptPublication(ctx, nodeIdentity.NodeID, job.JobID, claim.Lease.AttemptID, l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready}); err != nil {
		t.Fatal(err)
	}
	peers := &openAuthenticatedFabric{}
	server, err := l1.NewServer(peers, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	address := strings.TrimPrefix(httpServer.URL, "http://")
	participant := func(identity fabric.Identity) fabric.Fabric {
		return openTestFabric{dial: func(ctx context.Context, network, target string) (net.Conn, error) {
			if target != address {
				return nil, fmt.Errorf("unexpected L1 target %s", target)
			}
			conn, err := (&net.Dialer{}).DialContext(ctx, network, target)
			if err == nil {
				peers.peers.Store(conn.LocalAddr().String(), identity)
			}
			return conn, err
		}}
	}
	return openLedger{address: address, jobID: job.JobID,
		client: participant(fabric.Identity{NodeID: "client", FabricID: "test-network", Kind: fabric.IdentityKindMachine, Tags: []string{l1.DefaultClientPrincipalTag}}),
		person: participant(fabric.Identity{NodeID: "person", FabricID: "test-network", UserID: "owner", DeviceID: "person"}),
	}
}

func TestOpenL1AuthorizationRejectsPersonReads(t *testing.T) {
	ledger := realOpenServiceLedger(t, 18480)
	for _, role := range []string{"client", "person"} {
		participant := ledger.client
		if role == "person" {
			participant = ledger.person
		}
		clients, err := newAPIClients(participant, ledger.address, "")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(clients.close)
		for _, path := range []string{"/v1/jobs", "/v1/jobs/" + ledger.jobID + "?class=service", "/v1/nodes/studio-0123abcd"} {
			var output json.RawMessage
			err := clients.l1.do(context.Background(), http.MethodGet, path, nil, nil, &output, http.StatusOK)
			if role == "client" {
				if err != nil {
					t.Errorf("client GET %s: %v", path, err)
				}
			} else {
				var refused *apiResponseError
				if !errors.As(err, &refused) || refused.StatusCode != http.StatusForbidden || refused.APIError.Code != contract.ErrorPrincipalForbidden {
					t.Errorf("person GET %s: %v", path, err)
				}
			}
		}
	}
}

// Only loopback destinations supplied by the test fixtures are dialled.
type openTestFabric struct {
	fabric.Fabric
	dial func(context.Context, string, string) (net.Conn, error)
}

func (f openTestFabric) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return f.dial(ctx, network, address)
}

func TestOpenPlainPrintsLocalhostWithoutProxy(t *testing.T) {
	busy, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	port := busy.Addr().(*net.TCPAddr).Port
	clients, _ := openServiceFixture(t, port)
	var out bytes.Buffer
	err = execute(context.Background(), clients, false, []string{"open", "service-1", "--no-browser"}, &out, io.Discard)
	if err != nil {
		t.Fatalf("plain open: %v", err)
	}
	if !strings.Contains(out.String(), fmt.Sprintf("http://localhost:%d", port)) || !strings.Contains(out.String(), "no proxy") {
		t.Fatalf("plain output: %s", &out)
	}
}

func TestOpenRequiresPersonBeforeOpeningListener(t *testing.T) {
	for _, state := range []string{"missing", "expired", "missing-from-service", "legacy-fleet"} {
		t.Run(state, func(t *testing.T) {
			probe, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := probe.Addr().String()
			port := probe.Addr().(*net.TCPAddr).Port
			probe.Close()
			participants := map[string]connprofile.Participant{"client": {Name: "wefty://client/laptop", NodeID: "client", StateDirectory: savedChoiceState(t, "client")}}
			if state == "expired" || state == "missing-from-service" {
				participants["person"] = connprofile.Participant{Name: "wefty://person/laptop", NodeID: "person", UserID: "owner", StateDirectory: savedChoiceState(t, "person"), KeyExpiry: time.Now().Add(-time.Hour)}
				if state == "missing-from-service" {
					person := participants["person"]
					person.KeyExpiry = time.Time{}
					participants["person"] = person
				}
			}
			profile := filepath.Join(t.TempDir(), "profile.json")
			mode := "plain"
			if state == "legacy-fleet" {
				mode = "tsnet"
				participants = nil
			}
			if err := connprofile.Write(profile, connprofile.Profile{Version: 1, Fabric: mode, Participants: participants}); err != nil {
				t.Fatal(err)
			}
			var calls int
			err = runWithFabricOpener(context.Background(), []string{"--profile", profile, "open", "service-1", "--local-port", fmt.Sprint(port), "--no-browser"}, io.Discard, io.Discard, func(_ context.Context, config fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
				calls++
				if state == "missing-from-service" {
					if calls == 1 {
						if config.Identity.NodeID != "client" || config.Identity.UserID != "" {
							t.Fatalf("first participant was not client: %+v", config.Identity)
						}
						return openTestFabric{}, nil, nil
					}
					if config.Identity.NodeID != "person" || config.Identity.UserID != "owner" {
						t.Fatalf("second participant was not person: %+v", config.Identity)
					}
					return nil, nil, &fabric.ParticipantIdentityError{Person: true, Reason: "is missing from the service"}
				}
				return nil, nil, fmt.Errorf("must not open Fabric")
			})
			wantCalls := 0
			if state == "missing-from-service" {
				wantCalls = 2
			}
			if commandExitCode(err) != 3 || !strings.Contains(err.Error(), "owner's person sign-in on this laptop") || calls != wantCalls {
				t.Fatalf("refusal=%v exit=%d calls=%d", err, commandExitCode(err), calls)
			}
			listener, err := net.Listen("tcp4", address)
			if err != nil {
				t.Fatalf("listener started despite person refusal: %v", err)
			}
			listener.Close()
		})
	}
}

func TestOpenMissingClientKeepsExistingIdentityRefusal(t *testing.T) {
	profile := filepath.Join(t.TempDir(), "profile.json")
	if err := connprofile.Write(profile, connprofile.Profile{Version: 1, Fabric: "plain", Participants: map[string]connprofile.Participant{
		"person": {Name: "wefty://person/laptop", NodeID: "person", UserID: "owner", StateDirectory: savedChoiceState(t, "person")},
	}}); err != nil {
		t.Fatal(err)
	}
	err := runWithFabricOpener(context.Background(), []string{"--profile", profile, "--json", "open", "service-1", "--no-browser"}, io.Discard, io.Discard, func(context.Context, fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
		t.Fatal("missing client opened a Fabric participant")
		return nil, nil, nil
	})
	var identity *fabric.ParticipantIdentityError
	if !errors.As(err, &identity) || identity.Person || commandExitCode(err) != 3 || !strings.Contains(err.Error(), "is missing from the profile") {
		t.Fatalf("missing client: %v", err)
	}
}

func TestOpenBusyDefaultPortAndInvalidPort(t *testing.T) {
	busy, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	clients, _ := openServiceFixture(t, busy.Addr().(*net.TCPAddr).Port)
	clients.participantProfile = true
	var out bytes.Buffer
	err = execute(context.Background(), clients, true, []string{"open", "service-1", "--no-browser"}, &out, io.Discard)
	if commandExitCode(err) != 5 || !strings.Contains(err.Error(), "--local-port N") || out.Len() != 0 {
		t.Fatalf("busy port: %v output=%s", err, &out)
	}
	for _, port := range []string{"-1", "65536", "not-a-port"} {
		err := execute(context.Background(), nil, true, []string{"open", "service-1", "--local-port", port, "--no-browser"}, io.Discard, io.Discard)
		if commandExitCode(err) != 2 {
			t.Errorf("port %s: %v", port, err)
		}
	}
}

func TestOpenListenFailuresIncludePortHint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		exit  int
	}{
		{"busy", syscall.EADDRINUSE, 5},
		{"permission", syscall.EACCES, 1},
		{"other", errors.New("injected listen failure"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := listenOpenShortcut(80, func(network, address string) (net.Listener, error) {
				if network != "tcp4" || address != "127.0.0.1:80" {
					t.Fatalf("listen target: %s %s", network, address)
				}
				return nil, tc.cause
			})
			if listener != nil || commandExitCode(err) != tc.exit || !strings.Contains(err.Error(), "--local-port N") {
				t.Fatalf("listen error: %v, exit=%d", err, commandExitCode(err))
			}
			if tc.exit != 5 && !errors.Is(err, tc.cause) {
				t.Fatalf("listen cause lost: %v", err)
			}
		})
	}
}

func TestOpenFleetDefaultBrowserOnce(t *testing.T) {
	clients, _ := openServiceFixture(t, 18480)
	clients.participantProfile = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out bytes.Buffer
	calls, openedURL := 0, ""
	err := executeOpenWithOpener(ctx, clients, true, []string{"service-1", "--local-port=0"}, &out, io.Discard, func(_ context.Context, url string) error {
		calls++
		openedURL = url
		cancel()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var ready struct {
		LocalURL string `json:"local_url"`
	}
	if err := json.Unmarshal(out.Bytes(), &ready); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || openedURL != ready.LocalURL || !strings.HasPrefix(openedURL, "http://127.0.0.1:") {
		t.Fatalf("opener calls=%d url=%q ready=%s", calls, openedURL, &out)
	}
	_, port, err := net.SplitHostPort(strings.TrimPrefix(openedURL, "http://"))
	if err != nil || port == "0" {
		t.Fatalf("invalid ready port: %s (%v)", openedURL, err)
	}
}

func TestOpenBrowserAndUnavailableService(t *testing.T) {
	clients, job := openServiceFixture(t, 18480)
	var calls int
	opener := func(_ context.Context, url string) error {
		calls++
		if url != "http://localhost:18480" {
			t.Errorf("browser URL=%s", url)
		}
		return nil
	}
	for _, args := range [][]string{{"service-1"}, {"service-1", "--no-browser"}} {
		var out bytes.Buffer
		if err := executeOpenWithOpener(context.Background(), clients, false, args, &out, io.Discard, opener); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("browser calls after %v: %d", args, calls)
		}
	}
	if calls != 1 {
		t.Errorf("browser calls=%d", calls)
	}
	for _, unavailable := range []string{"stopped", "unpublished"} {
		if unavailable == "stopped" {
			job.State = contract.JobStopped
		} else {
			job.State = contract.JobRunning
			job.PublishedPort = nil
		}
		err := executeOpenWithOpener(context.Background(), clients, false, []string{"service-1"}, io.Discard, io.Discard, opener)
		if commandExitCode(err) != 5 || !strings.Contains(err.Error(), "wefty services status service-1") {
			t.Errorf("%s: %v", unavailable, err)
		}
	}
	if calls != 1 {
		t.Error("refused service opened a browser")
	}
}

func TestOpenHelpAndJSONValueFlag(t *testing.T) {
	var out bytes.Buffer
	err := runWithFabricOpener(context.Background(), []string{"--profile", "/missing-profile", "open", "service-1", "--help"}, &out, io.Discard, func(context.Context, fabricconfig.Config) (fabric.Fabric, fabricconfig.CloseFunc, error) {
		t.Fatal("help opened Fabric")
		return nil, nil, nil
	})
	if err != nil || !strings.Contains(out.String(), "--local-port") || !strings.Contains(out.String(), "--no-browser") {
		t.Fatalf("help: %v %s", err, &out)
	}
	if hasJSONFlag([]string{"open", "service-1", "--local-port", "--json"}) {
		t.Error("local-port value was stolen by global JSON parser")
	}
	if strings.Contains(out.String(), "(default -1)") {
		t.Fatalf("internal port sentinel leaked in help: %s", &out)
	}
}

func TestServiceStatusFleetGrantedAddressAndPlainBytes(t *testing.T) {
	clients, job := openServiceFixture(t, 18480)
	for _, jsonOutput := range []bool{false, true} {
		var plainOut, baseline, fleetOut bytes.Buffer
		clients.participantProfile = false
		if err := executeServiceStatus(context.Background(), clients, jsonOutput, []string{"service-1"}, &plainOut); err != nil {
			t.Fatal(err)
		}
		if err := writeServiceResult(&baseline, *job, jsonOutput); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plainOut.Bytes(), baseline.Bytes()) {
			t.Fatalf("plain status bytes changed:\n%s", &plainOut)
		}
		// Captured from unmodified ea2d7a25 with this same fake L1 response.
		capture := "plain-service-status-base.txt"
		if jsonOutput {
			capture = "plain-service-status-base.json"
		}
		assertPlainBaseBytes(t, capture, plainOut.String())
		clients.participantProfile = true
		if err := executeServiceStatus(context.Background(), clients, jsonOutput, []string{"service-1"}, &fleetOut); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(fleetOut.String(), "http://wefty-studio-2:18480") {
			t.Fatalf("missing granted address: %s", &fleetOut)
		}
		if jsonOutput {
			var got map[string]any
			if err := json.Unmarshal(fleetOut.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got["network_url"] != "http://wefty-studio-2:18480" {
				t.Fatal(got)
			}
		} else if !strings.Contains(fleetOut.String(), "network app") || !strings.Contains(fleetOut.String(), "wefty open") {
			t.Fatalf("missing account-switch advice: %s", &fleetOut)
		}
	}
}
