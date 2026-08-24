//go:build (service_acceptance_realtiming && linux) || (service_acceptance && darwin)

package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestOCIServicePublicationThroughHelperTunnel(t *testing.T) {
	helperSocket := os.Getenv("WEFTY_OCI_HELPER_SOCKET")
	helperChecksum := os.Getenv("WEFTY_OCI_HELPER_CHECKSUM")
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	if helperSocket == "" || helperChecksum == "" || reference == "" || digest == "" {
		if runtime.GOOS == "darwin" {
			t.Skip("NOT-RUN: attended Mac/Lima OCI service publication requires the owner-hardware helper and probe environment")
		}
		t.Fatal("Linux OCI service publication realtiming provisioning is incomplete")
	}

	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	client.HeartbeatInterval = time.Second
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: "service-publication-node", BootSessionID: "service-publication-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	if err := barrier.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	if err := adapter.Probe(ctx, "service-publication-node", "service-publication-boot", reference, digest, l1.DefaultLeaseDuration); err != nil {
		t.Fatal(err)
	}

	primary := startNativeOCIService(t, ctx, adapter, reference, digest, "primary", true)
	defer primary.stop(t, adapter)
	if body := primary.request(t, http.MethodGet, "/healthz", nil); string(body) != "healthy\n" {
		t.Fatalf("health response = %q", body)
	}
	echo := []byte("echo-through-fabric-and-helper")
	if body := primary.request(t, http.MethodPost, "/cgi-bin/echo", echo); !bytes.Equal(body, echo) {
		t.Fatalf("echo response = %q, want %q", body, echo)
	}

	sibling := startNativeOCIService(t, ctx, adapter, reference, digest, "sibling", true)
	if sibling.backendPort.Load() == primary.backendPort.Load() {
		t.Fatalf("concurrent OCI services shared backend port %d", primary.backendPort.Load())
	}
	sibling.stop(t, adapter)

	primary.tunnelAvailable.Store(false)
	primary.waitReachable(t, false, 5*time.Second)
	select {
	case outcome := <-primary.done:
		t.Fatalf("helper-tunnel withdrawal killed payload: (%#v, %v)", outcome.result, outcome.err)
	default:
	}
	primary.tunnelAvailable.Store(true)
	primary.waitReachable(t, true, 5*time.Second)

	timedOut := startNativeOCIService(t, ctx, adapter, reference, digest, "startup-timeout", false)
	outcome := waitServiceOutcome(t, timedOut.done)
	if outcome.err == nil || outcome.result.SpawnError == nil || outcome.result.SpawnError.Code != contract.SpawnFailureStartupReadinessTimeout {
		t.Fatalf("OCI startup timeout = (%#v, %v)", outcome.result, outcome.err)
	}
	timedOut.reap(t, adapter)

	portlessStarted := make(chan struct{}, 1)
	portlessEndpoint := false
	portless := nativeOCIServiceRequest(reference, digest, "portless", []string{
		"/bin/sh", "-c", `test "$WEFTY_SERVICE_DIR" = "/wefty/service"`,
	})
	portless.OCIStarted = func(context.Context, workloadrunner.OCIImageObservation) error {
		portlessStarted <- struct{}{}
		return nil
	}
	portless.AttemptEndpointReady = func(workloadrunner.AttemptEndpoint) error {
		portlessEndpoint = true
		return nil
	}
	portlessResult, err := adapter.Run(ctx, portless, nil)
	if err != nil || portlessResult.Outcome.ExitCode == nil || *portlessResult.Outcome.ExitCode != 0 {
		t.Fatalf("portless OCI service = (%#v, %v)", portlessResult.Outcome, err)
	}
	select {
	case <-portlessStarted:
	default:
		t.Fatal("portless OCI service did not report authoritative Started")
	}
	if portlessEndpoint {
		t.Fatal("portless OCI service published an attempt endpoint")
	}
	if receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: portless.Authority}); err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("portless OCI cleanup = (%+v, %v)", receipt, err)
	}

	if evidenceDirectory := os.Getenv("WEFTY_REALTIME_EVIDENCE_DIR"); evidenceDirectory != "" {
		evidence := fmt.Sprintf("platform=%s/%s\nhealth=true\necho=true\nstartup_timeout=true\nwithdrawal=true\nrepublication=true\nport_collision_avoided=true\nportless_started=true\nhelper_tunnel=true\n", runtime.GOOS, runtime.GOARCH)
		if err := os.WriteFile(filepath.Join(evidenceDirectory, "oci-service-publication-"+runtime.GOOS+".txt"), []byte(evidence), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

type nativeOCIService struct {
	requestAuthority workloadrunner.AttemptAuthority
	cancel           context.CancelFunc
	done             chan serviceRunOutcome
	client           *http.Client
	address          string
	backendPort      atomic.Uint32
	tunnelAvailable  atomic.Bool
	reaped           atomic.Bool
}

func startNativeOCIService(
	t *testing.T,
	parent context.Context,
	adapter *ocirunner.Adapter,
	reference, digest, suffix string,
	tunnelInitiallyAvailable bool,
) *nativeOCIService {
	t.Helper()
	network := plain.NewNetwork()
	frontDoorFabric := network.NewFabric(fabric.Identity{NodeID: "service-node-" + suffix})
	callerFabric := network.NewFabric(fabric.Identity{NodeID: "service-caller-" + suffix})
	address := "wefty://node/oci-service-" + suffix
	listener, err := frontDoorFabric.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	request := nativeOCIServiceRequest(reference, digest, suffix, []string{"/bin/sh", "-c", nativeOCIHTTPServiceScript})
	latch := newRuntimeEndpointLatch()
	request.AttemptPortRequired = true
	endpoint := latch.endpoint()
	service := &nativeOCIService{
		requestAuthority: request.Authority, done: make(chan serviceRunOutcome, 1), address: address,
		client: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return callerFabric.Dial(ctx, "tcp", address)
		}}},
	}
	service.tunnelAvailable.Store(tunnelInitiallyAvailable)
	originalDial := endpoint.dial
	endpoint.dial = func(ctx context.Context) (net.Conn, error) {
		if !service.tunnelAvailable.Load() {
			return nil, errors.New("injected helper tunnel outage")
		}
		return originalDial(ctx)
	}
	request.AttemptEndpointReady = func(value workloadrunner.AttemptEndpoint) error {
		service.backendPort.Store(uint32(value.Port))
		return latch.publish(value)
	}
	runContext, cancel := context.WithCancel(parent)
	service.cancel = cancel
	go func() {
		result, runErr := runPortfulService(
			runContext, adapter, request, nil, listener, endpoint,
			serviceSupervisorConfig{
				startupReadinessDeadline:  750 * time.Millisecond,
				readinessProbeInterval:    50 * time.Millisecond,
				readinessConnectTimeout:   200 * time.Millisecond,
				publicationRecoveryWindow: 250 * time.Millisecond,
			},
		)
		service.done <- serviceRunOutcome{result: result, err: runErr}
	}()
	if tunnelInitiallyAvailable {
		service.waitReachable(t, true, 15*time.Second)
	}
	return service
}

func nativeOCIServiceRequest(reference, digest, suffix string, argv []string) workloadrunner.Request {
	return workloadrunner.Request{
		Authority: workloadrunner.AttemptAuthority{
			NodeID: "service-publication-node", BootSessionID: "service-publication-boot",
			JobID: "service-job-" + suffix, AttemptID: "service-attempt-" + suffix,
			FencingToken: "service-fence-" + suffix, WorkloadClass: contract.JobClassService,
			RemovalGeneration: "attempt",
		},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		Execution: contract.ExecutionSpec{
			Env: map[string]string{contract.EnvServiceDir: contract.OCIContainerServiceDirectory},
			OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: reference, Digest: &digest}, Argv: argv,
			},
		},
		InitialDeadman:   2 * time.Minute,
		OCIImageResolved: func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
		OCIStarted:       func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
	}
}

const nativeOCIHTTPServiceScript = `
test "$WEFTY_SERVICE_DIR" = "/wefty/service" || exit 91
mkdir -p /tmp/wefty-www/cgi-bin
printf 'healthy\n' >/tmp/wefty-www/healthz
printf '#!/bin/sh\nprintf "Content-Type: application/octet-stream\\r\\n\\r\\n"\ncat\n' >/tmp/wefty-www/cgi-bin/echo
chmod 0755 /tmp/wefty-www/cgi-bin/echo
exec /bin/httpd -f -p "127.0.0.1:$WEFTY_SERVICE_PORT" -h /tmp/wefty-www
`

func (service *nativeOCIService) request(t *testing.T, method, path string, body []byte) []byte {
	t.Helper()
	request, err := http.NewRequest(method, "http://service.invalid"+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		t.Fatalf("%s %s returned %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(payload)))
	}
	return payload
}

func (service *nativeOCIService) waitReachable(t *testing.T, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, "http://service.invalid/healthz", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := service.client.Do(request)
		reachable := err == nil && response.StatusCode == http.StatusOK
		if response != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			_ = response.Body.Close()
		}
		if reachable == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("OCI service reachability did not become %v", want)
}

func (service *nativeOCIService) stop(t *testing.T, adapter *ocirunner.Adapter) {
	t.Helper()
	if service.reaped.Load() {
		return
	}
	service.cancel()
	select {
	case <-service.done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out stopping OCI service publication acceptance payload")
	}
	service.reap(t, adapter)
}

func (service *nativeOCIService) reap(t *testing.T, adapter *ocirunner.Adapter) {
	t.Helper()
	if !service.reaped.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	receipt, err := adapter.ReapAndVerify(ctx, workloadrunner.ReapRequest{Authority: service.requestAuthority})
	if err != nil || !receipt.RuntimeQuiesced {
		t.Fatalf("OCI service cleanup = (%+v, %v)", receipt, err)
	}
}
