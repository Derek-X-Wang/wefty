//go:build linux && computer_seat_lane

package computerconformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image/png"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"github.com/coder/websocket"
	containerd "github.com/containerd/containerd/v2/client"
)

// Real default image, native helper RPC, production L1 and node front door,
// and real CLI processes. This is CI timing evidence; the attended measurement
// on the owner's Mac is a separate acceptance run.
func TestSeatScreenCLISC1SC4(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("computer_seat_lane requires root")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	imageRef := requiredSeatEnv(t, "WEFTY_SEAT_IMAGE")
	binaryPath := requiredSeatEnv(t, "WEFTY_SEAT_CLI_BINARY")
	loggerPath := requiredSeatEnv(t, "WEFTY_SEAT_NODE_BINARY")
	for _, path := range []string{binaryPath, loggerPath} {
		info, err := os.Stat(path)
		if err != nil || !filepath.IsAbs(path) || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			t.Fatalf("screen lane requires an absolute executable binary: %q (%v)", path, err)
		}
	}
	imageClient, err := containerd.New(requiredSeatEnv(t, "WEFTY_SEAT_CONTAINERD"), containerd.WithDefaultNamespace(ocihelper.ContainerdNamespace))
	seatMust(t, err)
	image, err := imageClient.GetImage(ctx, imageRef)
	seatMust(t, err)
	digest := image.Target().Digest.String()
	imageClient.Close()
	engine, err := ocihelper.NewContainerdEngine(ocihelper.NativeEngineConfig{Address: requiredSeatEnv(t, "WEFTY_SEAT_CONTAINERD"), ContainerdStateRoot: "/run/wefty-seat-containerd", RuntimeRoot: t.TempDir(), LoggerExecutable: loggerPath, MemoryCapacityBytes: 2 << 30})
	seatMust(t, err)
	t.Cleanup(func() { seatMust(t, engine.Close()) })
	helper, err := ocihelper.NewServer(engine, ocihelper.ServerConfig{HelperVersion: "screen-lane", HelperChecksum: "screen-lane", AllowedUIDs: []uint32{uint32(os.Getuid())}, Logf: t.Logf})
	seatMust(t, err)
	socket := filepath.Join(t.TempDir(), "helper.sock")
	helperListener, err := net.Listen("unix", socket)
	seatMust(t, err)
	helperCtx, stopHelper := context.WithCancel(ctx)
	helperDone := make(chan error, 1)
	go func() { helperDone <- helper.Serve(helperCtx, helperListener) }()
	t.Cleanup(func() {
		stopHelper()
		helperListener.Close()
		select {
		case <-helperDone:
		case <-time.After(30 * time.Second):
			t.Error("helper shutdown did not finish")
		}
	})
	barrier, err := ocihelper.NewBootBarrier(ocihelper.NewUnixClient(socket, "screen-lane"), ocihelper.AcquireSessionRequest{NodeID: "screen-node", BootSessionID: "screen-boot", ExpectedHelperChecksum: "screen-lane"})
	seatMust(t, err)
	t.Cleanup(func() { barrier.Close() })
	seatMust(t, barrier.Ensure(ctx))
	adapter := ocirunner.NewAdapter(barrier)

	network, err := plain.NewNetworkWithID("plain-seat-screen-lane")
	seatMust(t, err)
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control"})
	nodeFabric := network.NewFabric(fabric.Identity{NodeID: "screen-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	owner := fabric.Identity{NodeID: "screen-owner", FabricID: "plain-seat-screen-lane", UserID: "owner", DeviceID: "owner-device", Tags: []string{l1.DefaultClientPrincipalTag}}
	ownerFabric := network.NewFabric(owner)
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{LeaseDuration: time.Minute, ComputerDefaultImage: imageRef})
	seatMust(t, err)
	t.Cleanup(func() { seatMust(t, store.Close()) })
	challenge, err := store.InitiateAdminBootstrap(ctx)
	seatMust(t, err)
	_, err = store.BootstrapAdmin(ctx, owner, challenge.Nonce)
	seatMust(t, err)
	server, err := l1.NewServer(controlFabric, store, screenLaneServerConfig())
	seatMust(t, err)
	listener, err := controlFabric.Listen("tcp", "127.0.0.1:0")
	seatMust(t, err)
	httpServer := &http.Server{Handler: screenLaneRegistrationHandler(t, server.Handler())}
	go httpServer.Serve(listener)
	t.Cleanup(func() { httpServer.Close() })
	config := screenLaneNodeConfig(t, nodeFabric, listener.Addr().String())
	config.SeatApprovalDialog = agent.SeatApprovalDialogFunc(func(context.Context, agent.SeatApprovalPrompt) (agent.SeatApprovalChoice, error) {
		return agent.SeatApprovalApprove, nil
	})
	config.WorkloadRuntimes = map[string]agent.WorkloadRuntime{"oci": adapter}
	config.CapabilityProbe = screenLaneProbe{adapter: adapter, image: strings.Split(imageRef, "@")[0], digest: digest}
	config.OCIBootBarrier = barrier
	config.AttemptDeadman = screenLaneDeadman{barrier: barrier}
	config.OCIIntent = func(context.Context) (agent.OCIIntentObservation, error) {
		return agent.OCIIntentObservation{Enabled: true, Revision: 1}, nil
	}
	config.HeartbeatInterval = time.Second
	config.ClaimInterval = 100 * time.Millisecond
	config.RenewalInterval = 10 * time.Second
	node, err := agent.New(config)
	seatMust(t, err)
	nodeCtx, stopNode := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- node.Run(nodeCtx) }()
	// Stop the node before L1 and the native helper are cleaned up.
	t.Cleanup(func() {
		stopNode()
		select {
		case <-done:
		case <-time.After(45 * time.Second):
			t.Error("node shutdown did not finish")
		}
		node.Close()
	})
	select {
	case <-node.Registered():
	case <-ctx.Done():
		t.Fatal("node did not register")
	}
	screenLaneWait(t, ctx, "probe-earned OCI capability", func() bool {
		snapshot := node.CapabilitySnapshot()
		return snapshot.Capabilities["kind:oci"] && snapshot.Capabilities["computer"]
	})
	id := "seat_00000000000000000000000000000001"
	_, err = store.CreateSeatCall(ctx, "screen-cli", l1.SeatCallInput{SeatID: id, NodeID: "screen-node", Harness: "screen acceptance", Folder: "/workspace", Form: "any", Network: "none"})
	seatMust(t, err)
	// The injected approval dialog answers only inside this isolated test.

	var ready l1.SeatCallResult
	screenLaneWait(t, ctx, "live Seat and screen publication", func() bool {
		ready, err = store.GetSeatCall(ctx, "screen-cli", id)
		return err == nil && ready.State == l1.SeatLive && ready.Credential != ""
	})
	state := t.TempDir()
	seatMust(t, os.Chmod(state, 0700))
	credential, _ := json.Marshal(map[string]string{"seat_id": id, "credential": ready.Credential})
	seatMust(t, os.WriteFile(filepath.Join(state, id+".json"), credential, 0600))
	call := func(args ...string) (int, []byte, time.Duration) {
		t.Helper()
		command := exec.CommandContext(ctx, binaryPath, append([]string{"--l1", listener.Addr().String(), "--plain-identity", "screen-cli", "--plain-fabric-id", "plain-seat-screen-lane", "--json", "seat"}, args...)...)
		command.Env = append(os.Environ(), "WEFTY_SEAT_STATE_ROOT="+state)
		start := time.Now()
		out, e := command.CombinedOutput()
		elapsed := time.Since(start)
		code := 0
		if e != nil {
			exit, ok := e.(*exec.ExitError)
			if !ok {
				t.Fatal(e)
			}
			code = exit.ExitCode()
		}
		return code, out, elapsed
	}
	click := func() time.Duration {
		t.Helper()
		path := filepath.Join(t.TempDir(), "click.png")
		code, out, elapsed := call("click", id, "640", "400", "--screenshot", path)
		if code != 0 {
			t.Fatalf("SC-1 CLI exit %d: %s", code, out)
		}
		f, err := os.Open(path)
		seatMust(t, err)
		im, err := png.Decode(f)
		f.Close()
		seatMust(t, err)
		if im.Bounds().Dx() != 1280 || im.Bounds().Dy() != 800 {
			t.Fatal("SC-1 wrong PNG dimensions")
		}
		return elapsed
	}
	// Preserve the actual first call separately; it is never hidden by warming.
	cold := click()
	t.Logf("SC-1 cold first click + PNG: %.3f ms", float64(cold.Microseconds())/1000)
	_ = click() // one explicit warm-up outside the measured warm sample set
	warm := []time.Duration{click(), click(), click()}
	ordered := slices.Clone(warm)
	slices.Sort(ordered)
	sc1 := ordered[1]
	t.Logf("SC-1 warm click + PNG: %v; warm median: %.3f ms", warm, float64(sc1.Microseconds())/1000)
	var target l1.SeatCopyTarget
	seatMust(t, screenLaneJSON(ctx, ownerFabric, listener.Addr().String(), http.MethodGet, "/v1/seats/"+id+"/screen-target", ready.Credential, nil, &target))
	host := strings.TrimPrefix(strings.Split(strings.TrimPrefix(target.Endpoint, "ws://"), "/")[0], "http://")
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return ownerFabric.Dial(ctx, network, host)
	}}
	defer transport.CloseIdleConnections()
	// A person's Take control uses the real node tenure and real L1 event path.
	ws, response, err := websocket.Dial(ctx, "ws://"+host+"/v1/seats/"+id+"/person/websockify", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{"binary"}})
	seatMust(t, err)
	defer ws.CloseNow()
	token := response.Header.Get(contract.ComputerControlTokenHeader)
	if token == "" {
		t.Fatal("person viewer omitted bearer")
	}
	wire := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	viewer, err := rfbclient.Open(wire)
	seatMust(t, err)
	_, err = viewer.Frame()
	seatMust(t, err)
	// The viewer stays connected and consumes server updates during Take control.
	go func() { _, _ = io.Copy(io.Discard, wire) }()

	// Warm input immediately before Take control, outside the SC-4 interval.
	code, out, _ := call("move", id, "640", "400")
	if code != 0 {
		t.Fatalf("SC-4 input warm-up exit %d: %s", code, out)
	}
	takeStart := time.Now()
	seatMust(t, screenLaneJSON(ctx, ownerFabric, host, http.MethodPost, "/v1/seats/"+id+"/person/wefty/control/take", token, nil, nil))
	code, out, _ = call("click", id, "640", "400")
	sc4 := time.Since(takeStart)
	if code != 5 || !bytes.Contains(out, []byte("person_driving")) || !bytes.Contains(out, []byte("a person is driving")) {
		t.Fatalf("SC-4 CLI exit=%d: %s", code, out)
	}
	t.Logf("SC-1 warm median click + PNG: %.3f ms; SC-4 Take control to first person_driving: %.3f ms; default image=%s; Linux/%s; process launch included", float64(sc1.Microseconds())/1000, float64(sc4.Microseconds())/1000, imageRef, runtime.GOARCH)
	// Both observations are logged even when one exceeds its limit.
	evidence, _ := json.MarshalIndent(map[string]any{"sc1_ms": float64(sc1.Microseconds()) / 1000, "sc1_cold_ms": float64(cold.Microseconds()) / 1000, "sc1_warm_ms": []float64{float64(warm[0].Microseconds()) / 1000, float64(warm[1].Microseconds()) / 1000, float64(warm[2].Microseconds()) / 1000}, "sc1_warm_median_ms": float64(sc1.Microseconds()) / 1000, "sc4_ms": float64(sc4.Microseconds()) / 1000, "image": imageRef, "platform": "linux/" + runtime.GOARCH, "process_launch_included": true, "helper_ipc_included": true}, "", "  ")
	seatMust(t, os.WriteFile(filepath.Join(filepath.Dir(requiredSeatEnv(t, "WEFTY_SEAT_EVIDENCE")), runtime.GOARCH+"-screen-cli.json"), evidence, 0600))
	if sc1 > time.Second {
		t.Errorf("SC-1 warm median %s exceeds 1s", sc1)
	}
	if sc4 > time.Second {
		t.Errorf("SC-4 %s exceeds 1s", sc4)
	}
}

func screenLaneJSON(ctx context.Context, f fabric.Fabric, host, method, path, credential string, body any, result any) error {
	var reader io.Reader
	if body != nil {
		data, e := json.Marshal(body)
		if e != nil {
			return e
		}
		reader = bytes.NewReader(data)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) { return f.Dial(ctx, network, host) }}
	defer transport.CloseIdleConnections()
	request, e := http.NewRequestWithContext(ctx, method, "http://wefty.invalid"+path, reader)
	if e != nil {
		return e
	}
	if strings.Contains(path, "/person/") {
		request.Header.Set(contract.ComputerControlTokenHeader, credential)
	} else {
		request.Header.Set(l1.SeatCredentialHeader, credential)
	}
	response, e := (&http.Client{Transport: transport}).Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		data, _ := io.ReadAll(response.Body)
		return fmt.Errorf("%s %s: HTTP %d %s", method, path, response.StatusCode, data)
	}
	if result != nil {
		return json.NewDecoder(response.Body).Decode(result)
	}
	return nil
}

// Real process readiness is observed outside the measured interval.
func screenLaneWait(t *testing.T, ctx context.Context, description string, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}

type screenLaneProbe struct {
	adapter       *ocirunner.Adapter
	image, digest string
}

func (p screenLaneProbe) Probe(ctx context.Context) (agent.CapabilityProbeResult, error) {
	if err := p.adapter.Probe(ctx, "screen-node", "screen-boot", p.image, p.digest, time.Minute); err != nil {
		return agent.CapabilityProbeResult{}, err
	}
	platform, ok := p.adapter.ProbedRuntimePlatform()
	if !ok {
		return agent.CapabilityProbeResult{}, fmt.Errorf("functional probe did not observe a runtime platform")
	}
	return agent.CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:" + ocihelper.DefaultRuntimeHandler: true, "cgroup_v2": true, "computer": true, l1.RuntimePlatformCapability(platform.OS, platform.Architecture): true}, MissingCapabilities: []string{}}, nil
}

type screenLaneDeadman struct{ barrier *ocihelper.BootBarrier }

func (d screenLaneDeadman) QueueSuccessfulRenewal(claim l1.Claim, expires time.Time, generation workloadrunner.RuntimeGeneration) error {
	session, err := d.barrier.Session()
	if err != nil {
		return err
	}
	if err = agent.ValidateAttemptDeadmanSessionGeneration(session, generation); err != nil {
		return err
	}
	removal := "attempt"
	if claim.Job.Spec.Class == contract.JobClassService {
		removal = fmt.Sprint(l1.InitialServiceRemovalGeneration)
	}
	return session.QueueAttemptRenewalUntil(ocihelper.AttemptAuthority{NodeID: "screen-node", BootSessionID: "screen-boot", JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken, Class: claim.Job.Spec.Class, RemovalGeneration: removal}, expires)
}
