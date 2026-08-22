package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestClaimRequestsOneShotWorkWithLocalJobExclusions(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan l1.ClaimRequest, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var claim l1.ClaimRequest
		if err := json.NewDecoder(request.Body).Decode(&claim); err != nil {
			t.Errorf("decode claim request: %v", err)
		}
		received <- claim
		response.WriteHeader(http.StatusNoContent)
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve claim request: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := newClient(agentFabric, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	claim, err := client.Claim(
		context.Background(), "node-1", "boot-1", contract.JobClassOneShot, "job-finalizing-a", "job-finalizing-b",
	)
	if err != nil || claim != nil {
		t.Fatalf("claim = %#v, err = %v, want empty successful claim", claim, err)
	}
	request := <-received
	if request.Class != contract.JobClassOneShot {
		t.Fatalf("claim class = %q, want %q", request.Class, contract.JobClassOneShot)
	}
	if len(request.ExcludedJobIDs) != 2 || request.ExcludedJobIDs[0] != "job-finalizing-a" || request.ExcludedJobIDs[1] != "job-finalizing-b" {
		t.Fatalf("claim exclusions = %#v, want both locally finalizing jobs", request.ExcludedJobIDs)
	}
}

func TestClientBoundsAnOperationWhoseServerNeverResponds(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{}, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		accepted <- struct{}{}
		<-request.Context().Done()
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve hanging operation: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := newClient(agentFabric, "wefty://control-plane", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	started := time.Now()
	_, err = client.Register(context.Background(), contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", AgentVersion: "test",
	})
	if err == nil {
		t.Fatal("hanging registration returned nil error")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded registration took %s, want less than one second", elapsed)
	}
	select {
	case <-accepted:
	default:
		t.Fatal("server did not accept the bounded operation")
	}
}

func TestRenewalRequestTimeoutIsStrictlyInsideAuthority(t *testing.T) {
	for _, test := range []struct {
		remaining, operation time.Duration
	}{
		{remaining: 30 * time.Second, operation: 10 * time.Second},
		{remaining: 3 * time.Second, operation: 10 * time.Second},
		{remaining: time.Nanosecond, operation: 10 * time.Second},
	} {
		timeout := renewalRequestTimeout(test.remaining, test.operation)
		if test.remaining <= time.Nanosecond {
			if timeout != 0 {
				t.Fatalf("renewal timeout(%s, %s) = %s, want no unsafe request", test.remaining, test.operation, timeout)
			}
			continue
		}
		if timeout <= 0 || timeout >= test.remaining {
			t.Fatalf("renewal timeout(%s, %s) = %s, want positive and strictly shorter than remaining authority", test.remaining, test.operation, timeout)
		}
		if timeout > test.operation {
			t.Fatalf("renewal timeout(%s, %s) = %s, exceeds operation bound", test.remaining, test.operation, timeout)
		}
	}
}

func TestClientTransportSupportsConcurrentAttemptTraffic(t *testing.T) {
	assertClientTransportSupportsConcurrentAttemptTraffic(t)
}

func TestClientSendsAbsolutePublicationWithPut(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan l1.PublicationRequest, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			t.Errorf("publication method = %q, want PUT", request.Method)
		}
		if request.URL.EscapedPath() != "/v1/agent/jobs/job%2Fone/attempts/attempt%2Fone/publication" {
			t.Errorf("publication path = %q", request.URL.EscapedPath())
		}
		var publication l1.PublicationRequest
		if err := json.NewDecoder(request.Body).Decode(&publication); err != nil {
			t.Errorf("decode publication request: %v", err)
		}
		received <- publication
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve publication request: %v", err)
		}
	}()

	client, err := newClient(
		network.NewFabric(fabric.Identity{NodeID: "agent"}),
		"wefty://control-plane",
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ready := true
	_, err = client.SetAttemptPublication(
		context.Background(),
		"job/one",
		"attempt/one",
		l1.PublicationRequest{FencingToken: "fence-one", Ready: &ready},
	)
	if err != nil {
		t.Fatal(err)
	}
	request := <-received
	if request.FencingToken != "fence-one" || request.Ready == nil || !*request.Ready {
		t.Fatalf("publication request = %#v, want absolute ready=true", request)
	}
}

func TestClientObserveAttemptImageRoundTrip(t *testing.T) {
	received := make(chan l1.ImageObservationRequest, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut || request.URL.EscapedPath() != "/v1/agent/jobs/job-one/attempts/attempt-one/image" {
			t.Errorf("image request = %s %s", request.Method, request.URL.EscapedPath())
		}
		var observation l1.ImageObservationRequest
		if err := json.NewDecoder(request.Body).Decode(&observation); err != nil {
			t.Errorf("decode image observation: %v", err)
		}
		received <- observation
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"job_id":"job-one","state":"claimed"}`))
	}))
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	want := l1.ImageObservationRequest{
		FencingToken: "fence-one", SubmittedReference: "ghcr.io/example/tool:latest", TopLevelDigest: digest,
		TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
		Platform: l1.OCIPlatform{OS: "linux", Architecture: "arm64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	job, err := client.ObserveAttemptImage(context.Background(), "job-one", "attempt-one", want)
	if err != nil || job.JobID != "job-one" || job.State != contract.JobClaimed {
		t.Fatalf("image round trip = %#v err %v", job, err)
	}
	if got := <-received; got.FencingToken != want.FencingToken || got.TopLevelDigest != want.TopLevelDigest || got.Platform != want.Platform {
		t.Fatalf("image request = %#v, want %#v", got, want)
	}
}

func TestClientStartAttemptRoundTrip(t *testing.T) {
	received := make(chan l1.StartedRequest, 1)
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.EscapedPath() != "/v1/agent/jobs/job-one/attempts/attempt-one/started" {
			t.Errorf("Started request = %s %s", request.Method, request.URL.EscapedPath())
		}
		var started l1.StartedRequest
		if err := json.NewDecoder(request.Body).Decode(&started); err != nil {
			t.Errorf("decode Started request: %v", err)
		}
		received <- started
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"job_id":"job-one","state":"running"}`))
	}))
	want := l1.StartedRequest{FencingToken: "fence-one"}
	job, err := client.StartAttempt(context.Background(), "job-one", "attempt-one", want)
	if err != nil || job.JobID != "job-one" || job.State != contract.JobRunning {
		t.Fatalf("Started round trip = %#v err %v", job, err)
	}
	if got := <-received; got != want {
		t.Fatalf("Started request = %#v, want %#v", got, want)
	}
}

func newRoundTripClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	network := plain.NewNetwork()
	listener, err := network.NewFabric(fabric.Identity{NodeID: "control-plane"}).Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve round trip: %v", err)
		}
	})
	client, err := newClient(network.NewFabric(fabric.Identity{NodeID: "agent"}), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func assertClientTransportSupportsConcurrentAttemptTraffic(t *testing.T) {
	t.Helper()
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := NewClient(participant, "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.transport.MaxIdleConnsPerHost; got != DefaultMaxIdleConnsPerHost || got <= http.DefaultMaxIdleConnsPerHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want raised concurrent-attempt pool %d", got, DefaultMaxIdleConnsPerHost)
	}
}
