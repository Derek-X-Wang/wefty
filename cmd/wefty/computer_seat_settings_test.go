package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestComputerCreateSeatSettingsAndImageSupport covers spec P17: an owner-made
// Computer gets Seat limit 2 and idle action nothing by default, the owner may
// choose others, and an explicit --seat-limit records the image's Seat label
// before any request; a default create reads nothing (#1155).
func TestComputerCreateSeatSettingsAndImageSupport(t *testing.T) {
	t.Parallel()
	harness := newServiceCLIHarness(t)
	resolver := &fakeImageResolver{digest: computerCLITestDigest, seatPlaces: 4}
	harness.clients.images = resolver
	ctx := context.Background()
	if _, err := harness.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{
		NodeID: "computer-node", BootSessionID: "boot-seat-settings", RootInstanceID: "root-seat-settings",
		OS: "linux", Architecture: "amd64", AgentVersion: "computer-cli-test",
		Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 8}, true); err != nil {
		t.Fatal(err)
	}
	create := func(name string, extra ...string) (computerOperatorProjection, string, error) {
		args := append([]string{"services", "create", "--computer", "--name", name,
			"--image", "ghcr.io/example/desk@" + computerCLITestDigest, "--node", "computer-node"}, extra...)
		var stdout, stderr bytes.Buffer
		err := execute(ctx, harness.clients, true, args, &stdout, &stderr)
		var projection computerOperatorProjection
		if err == nil {
			if decodeErr := json.Unmarshal(stdout.Bytes(), &projection); decodeErr != nil {
				t.Fatal(decodeErr)
			}
		}
		return projection, stderr.String(), err
	}
	want := func(name string, got computerOperatorProjection, settings l1.ComputerSettings) {
		t.Helper()
		if got.ComputerSettings != settings || got.SeatRequestMade {
			t.Fatalf("%s settings = %+v made=%v, want %+v", name, got.ComputerSettings, got.SeatRequestMade, settings)
		}
		stored, err := harness.store.GetComputer(ctx, got.ComputerID)
		if err != nil || stored.ComputerSettings != settings {
			t.Fatalf("%s stored settings = %+v err=%v", name, stored.ComputerSettings, err)
		}
	}

	defaults, warning, err := create("defaults")
	if err != nil || warning != "" || len(resolver.seatReads) != 0 {
		t.Fatalf("default create err=%v stderr=%q label reads=%v", err, warning, resolver.seatReads)
	}
	want("defaults", defaults, l1.ComputerSettings{SeatLimit: 2, IdleAction: l1.ComputerIdleNothing, Network: l1.ComputerNetworkInternet})
	chosen, _, err := create("chosen", "--seat-limit", "3", "--idle-action", "stop")
	if err != nil || len(resolver.seatReads) != 1 {
		t.Fatalf("explicit create err=%v label reads=%v", err, resolver.seatReads)
	}
	want("chosen", chosen, l1.ComputerSettings{SeatLimit: 3, IdleAction: l1.ComputerIdleStop, Network: l1.ComputerNetworkInternet, ImageSeatPlaces: 4})

	// No Seats wanted: the label is never read.
	reads := len(resolver.seatReads)
	none, _, err := create("none", "--seat-limit", "0", "--idle-action", "delete")
	if err != nil || len(resolver.seatReads) != reads {
		t.Fatalf("--seat-limit 0 err=%v label reads %d -> %d", err, reads, len(resolver.seatReads))
	}
	want("none", none, l1.ComputerSettings{SeatLimit: 0, IdleAction: l1.ComputerIdleDelete, Network: l1.ComputerNetworkInternet})

	// A default create never touches the registry, so an offline registry
	// changes nothing for it.
	resolver.seatErr = errors.New("registry offline")
	offline, warning, err := create("offline")
	if err != nil || warning != "" {
		t.Fatalf("offline default create err=%v stderr=%q", err, warning)
	}
	want("offline", offline, l1.ComputerSettings{SeatLimit: 2, IdleAction: l1.ComputerIdleNothing, Network: l1.ComputerNetworkInternet})
	// An explicit Seat limit fails loudly.
	if _, _, err := create("offline-explicit", "--seat-limit", "2"); err == nil || !strings.Contains(err.Error(), "--seat-limit 0") {
		t.Fatalf("offline explicit create err=%v", err)
	}
	resolver.seatErr = nil

	resolver.seatPlaces = 0
	for _, test := range []struct {
		name  string
		args  []string
		error string
	}{
		{"unlabelled-explicit", []string{"--seat-limit", "1"}, "does not support Seats"},
		{"too-many", []string{"--seat-limit", "5"}, "between 0 and 4"},
		{"bad-idle", []string{"--idle-action", "sleep"}, "nothing, stop or delete"},
	} {
		if _, _, err := create(test.name, test.args...); err == nil || !strings.Contains(err.Error(), test.error) || commandExitCode(err) != exitUsage {
			t.Fatalf("%s err=%v exit=%d", test.name, err, commandExitCode(err))
		}
	}
	resolver.seatPlaces = 2
	if _, _, err := create("over-places", "--seat-limit", "3"); err == nil || !strings.Contains(err.Error(), "exceeds the 2 Seat places") {
		t.Fatalf("over-places err=%v", err)
	}
	resolver.seatPlaces = 1
	small, _, err := create("one-place", "--seat-limit", "1")
	if err != nil {
		t.Fatalf("explicit create on a one-place image err=%v", err)
	}
	want("one-place", small, l1.ComputerSettings{SeatLimit: 1, IdleAction: l1.ComputerIdleNothing, Network: l1.ComputerNetworkInternet, ImageSeatPlaces: 1})
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, harness.clients, true, []string{"services", "create", "--image", "ghcr.io/example/desk@" + computerCLITestDigest, "--seat-limit", "1"}, &stdout, &stderr); err == nil || !strings.Contains(err.Error(), "require --computer") {
		t.Fatalf("--seat-limit without --computer err=%v", err)
	}
}

// TestComputerReimageRecordsImageSeatPlacesOnlyForSeatComputers proves a
// reimage reads the new image's Seat label only for a Computer that opted in:
// --seat-support fails loudly, a recorded reading warns, others read nothing.
func TestComputerReimageRecordsImageSeatPlacesOnlyForSeatComputers(t *testing.T) {
	for _, test := range []struct {
		name       string
		seatLimit  int
		recorded   int
		flags      []string
		resolver   *fakeImageResolver
		wantPlaces int
		wantReads  int
		warning    bool
		wantErr    string
	}{
		{"default", 2, 0, nil, &fakeImageResolver{seatPlaces: 4}, 0, 0, false, ""},
		{"opted in", 2, 4, nil, &fakeImageResolver{seatPlaces: 4}, 4, 1, false, ""},
		{"opted in unreadable", 2, 4, nil, &fakeImageResolver{seatErr: errors.New("registry offline")}, 0, 1, true, ""},
		{"seat support", 2, 0, []string{"--seat-support"}, &fakeImageResolver{seatPlaces: 3}, 3, 1, false, ""},
		{"seat support unreadable", 2, 0, []string{"--seat-support"}, &fakeImageResolver{seatErr: errors.New("registry offline")}, 0, 1, false, "registry offline"},
		{"seat support unlabelled", 2, 0, []string{"--seat-support"}, &fakeImageResolver{}, 0, 1, false, "does not support Seats"},
		{"seat support without Seats", 0, 0, []string{"--seat-support"}, &fakeImageResolver{seatPlaces: 4}, 0, 0, false, "Seat limit above 0"},
		{"no Seats", 0, 0, nil, &fakeImageResolver{seatPlaces: 4}, 0, 0, false, ""},
		{"seat support on the current digest", 2, 0, []string{"--seat-support"}, &fakeImageResolver{seatPlaces: 4}, 0, 0, false, "new image digest"},
		{"seat support retry after it completed", 2, 3, []string{"--seat-support"}, &fakeImageResolver{seatErr: errors.New("registry offline")}, 3, 0, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			computer := l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1,
				ComputerSettings: l1.ComputerSettings{SeatLimit: test.seatLimit, IdleAction: l1.ComputerIdleNothing, Network: l1.ComputerNetworkInternet, ImageSeatPlaces: test.recorded}}
			if strings.HasPrefix(test.name, "seat support on the current digest") || test.name == "seat support retry after it completed" {
				digest := computerCLITestDigestB
				computer.CurrentJob.Spec.Execution.OCI = &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/desk", Digest: &digest}}
			}
			var body l1.ComputerReimageRequest
			clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(computer)
					return
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				w.Header().Set("Mutation-Applied", "true")
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(computer)
			})
			clients.images = test.resolver
			var stdout, stderr bytes.Buffer
			args := append([]string{"services", "reimage", "computer-1",
				"--image=ghcr.io/example/desk@" + computerCLITestDigestB, "--expect-current", "--idempotency-key=reimage-seats"}, test.flags...)
			err := execute(context.Background(), clients, true, args, &stdout, &stderr)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || len(test.resolver.seatReads) != test.wantReads {
					t.Fatalf("err=%v reads=%v, want %q", err, test.resolver.seatReads, test.wantErr)
				}
				return
			}
			if err != nil || body.ImageSeatPlaces != test.wantPlaces || len(test.resolver.seatReads) != test.wantReads ||
				strings.Contains(stderr.String(), "warning") != test.warning {
				t.Fatalf("err=%v places=%d reads=%v stderr=%q", err, body.ImageSeatPlaces, test.resolver.seatReads, stderr.String())
			}
		})
	}
}

// countingRegistryTransport counts every registry request and answers none.
type countingRegistryTransport struct{ requests atomic.Int32 }

func (c *countingRegistryTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.requests.Add(1)
	return nil, errors.New("registry unreachable")
}

// TestComputerDefaultCreateAndReimageMakeNoRegistryRequest proves option (b) of
// #1155 at the transport: a digest-pinned default create and a default reimage
// make zero registry requests, so they work offline. An explicit --seat-limit
// is the control: it reaches the registry and fails loudly.
func TestComputerDefaultCreateAndReimageMakeNoRegistryRequest(t *testing.T) {
	t.Parallel()
	transport := &countingRegistryTransport{}
	registry := newRegistryResolver(&http.Client{Transport: transport})
	harness := newServiceCLIHarness(t)
	harness.clients.images = registry
	ctx := context.Background()
	if _, err := harness.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{
		NodeID: "computer-node", BootSessionID: "boot-no-registry", RootInstanceID: "root-no-registry",
		OS: "linux", Architecture: "amd64", AgentVersion: "computer-cli-test",
		Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 8}, true); err != nil {
		t.Fatal(err)
	}
	create := func(name string, extra ...string) (string, error) {
		args := append([]string{"services", "create", "--computer", "--name", name,
			"--image", "ghcr.io/example/desk@" + computerCLITestDigest, "--node", "computer-node"}, extra...)
		var stdout, stderr bytes.Buffer
		err := execute(ctx, harness.clients, true, args, &stdout, &stderr)
		return stderr.String(), err
	}
	if stderr, err := create("default"); err != nil || stderr != "" || transport.requests.Load() != 0 {
		t.Fatalf("default create err=%v stderr=%q registry requests=%d", err, stderr, transport.requests.Load())
	}
	if _, err := create("explicit", "--seat-limit", "2"); err == nil || transport.requests.Load() == 0 {
		t.Fatalf("explicit create err=%v registry requests=%d; want a loud registry failure", err, transport.requests.Load())
	}

	before := transport.requests.Load()
	computer := l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1,
		ComputerSettings: l1.ComputerSettings{SeatLimit: 2, IdleAction: l1.ComputerIdleNothing, Network: l1.ComputerNetworkInternet}}
	clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(computer)
			return
		}
		w.Header().Set("Mutation-Applied", "true")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(computer)
	})
	clients.images = registry
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, clients, true, []string{"services", "reimage", "computer-1",
		"--image=ghcr.io/example/desk@" + computerCLITestDigestB, "--expect-current", "--idempotency-key=reimage-offline"}, &stdout, &stderr); err != nil ||
		stderr.Len() != 0 || transport.requests.Load() != before {
		t.Fatalf("default reimage err=%v stderr=%q registry requests %d -> %d", err, stderr.String(), before, transport.requests.Load())
	}
}
