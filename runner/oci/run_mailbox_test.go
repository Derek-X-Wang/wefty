package oci

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// mailboxAdapterEngine serves the mailbox over the adapter's real helper
// transport, so the test exercises the wire shape and the server's
// authorization, not a stub of either.
type mailboxAdapterEngine struct {
	*adapterTestEngine
	mu      sync.Mutex
	entries map[string][]byte
	removed []string
}

func (engine *mailboxAdapterEngine) ListRunMailbox(_ context.Context, request ocihelper.ListRunMailboxRequest) (ocihelper.ListRunMailboxResponse, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	names := make([]string, 0, len(engine.entries))
	for name := range engine.entries {
		names = append(names, name)
	}
	return ocihelper.ListRunMailboxResponse{Names: names}, nil
}

func (engine *mailboxAdapterEngine) ReadRunMailbox(_ context.Context, request ocihelper.ReadRunMailboxRequest) (ocihelper.ReadRunMailboxResponse, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	payload, ok := engine.entries[request.Name]
	if !ok {
		return ocihelper.ReadRunMailboxResponse{}, errors.New("no such run mailbox entry")
	}
	return ocihelper.ReadRunMailboxResponse{Payload: payload}, nil
}

func (engine *mailboxAdapterEngine) RemoveRunMailboxEntry(_ context.Context, request ocihelper.RemoveRunMailboxEntryRequest) (ocihelper.RemoveRunMailboxEntryResponse, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if _, ok := engine.entries[request.Name]; !ok {
		return ocihelper.RemoveRunMailboxEntryResponse{Absent: true}, nil
	}
	delete(engine.entries, request.Name)
	engine.removed = append(engine.removed, request.Name)
	return ocihelper.RemoveRunMailboxEntryResponse{Removed: true}, nil
}

const (
	mailboxAdapterOwnerKey = "run-owner-adapter"
	mailboxAdapterRunID    = "run_adapter"
)

func mailboxAdapterAuthority() workloadrunner.AttemptAuthority {
	return workloadrunner.AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "job-mailbox", AttemptID: "attempt-mailbox",
		FencingToken: "fence-mailbox", WorkloadClass: contract.JobClassOneShot, RemovalGeneration: "removal-1",
	}
}

// TestAdapterServesOneAttemptsRunMailbox proves the three calls reach the
// helper, carry the attempt's own authority, and translate the helper's
// answers into what the publisher expects -- including an already-absent
// removal, which is what a replayed retirement after a lost response looks
// like.
func TestAdapterServesOneAttemptsRunMailbox(t *testing.T) {
	engine := &mailboxAdapterEngine{
		adapterTestEngine: &adapterTestEngine{},
		entries: map[string][]byte{
			"0001-alpha": []byte("wefty-protocol: 1\nkind: envelope\nstep: alpha\n--\n"),
		},
	}
	adapter, barrier, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{})
	defer closeAdapter()
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	authority := mailboxAdapterAuthority()
	if _, err := session.Run(t.Context(), ocihelper.RunRequest{
		Authority:      HelperAuthority(authority),
		InitialDeadman: time.Minute,
		Workload: ocihelper.WorkloadInput{
			ImageDigest:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Argv:           []string{"/bin/probe"},
			ManagedVolumes: []ocihelper.ManagedVolumeDescriptor{{Kind: ocihelper.ManagedVolumeHandoff, OwnerKey: mailboxAdapterOwnerKey}},
			RunMailbox:     &ocihelper.RunMailboxSeed{RunID: mailboxAdapterRunID},
		},
	}); err != nil {
		t.Fatal(err)
	}

	reference := workloadrunner.RunMailboxReference{
		Authority: authority, OwnerKey: mailboxAdapterOwnerKey, RunID: mailboxAdapterRunID,
	}
	names, exhausted, err := adapter.ListRunMailbox(t.Context(), reference, 0)
	if err != nil || exhausted || len(names) != 1 || names[0] != "0001-alpha" {
		t.Fatalf("list = %v exhausted=%t err=%v", names, exhausted, err)
	}
	payload, truncated, err := adapter.ReadRunMailbox(t.Context(), reference, "0001-alpha", 0)
	if err != nil || truncated || string(payload) != "wefty-protocol: 1\nkind: envelope\nstep: alpha\n--\n" {
		t.Fatalf("read = %q truncated=%t err=%v", payload, truncated, err)
	}
	if err := adapter.RemoveRunMailboxEntry(t.Context(), reference, "0001-alpha"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := adapter.RemoveRunMailboxEntry(t.Context(), reference, "0001-alpha"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a replayed removal error = %v, want a not-exist so the publisher reads it as retired", err)
	}

	// Another run's volume is refused at the helper, not at the adapter: the
	// agent holds no authority of its own over what it may read.
	other := reference
	other.OwnerKey = "someone-elses-run"
	if _, _, err := adapter.ListRunMailbox(t.Context(), other, 0); err == nil {
		t.Fatal("the adapter listed a mailbox this attempt does not own")
	}
}

// TestAdapterCarriesTheRunMailboxSeedIntoTheHelperRun proves the seed reaches
// the helper on Run and that no guest path is ever assembled agent-side.
func TestAdapterCarriesTheRunMailboxSeedIntoTheHelperRun(t *testing.T) {
	request := workloadrunner.Request{
		Authority: mailboxAdapterAuthority(),
		Execution: contract.ExecutionSpec{
			OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: "example.test/image:v1", Digest: stringPointer("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
				Argv:  []string{"/bin/sh"},
			},
		},
		ManagedVolumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeHandoff, OwnerKey: mailboxAdapterOwnerKey}},
		RunMailbox:     &workloadrunner.RunMailboxSeed{RunID: mailboxAdapterRunID, Params: []byte(`{"branch":"main"}`)},
	}
	input := workloadInput(request)
	if input.RunMailbox == nil || input.RunMailbox.RunID != mailboxAdapterRunID || string(input.RunMailbox.Params) != `{"branch":"main"}` {
		t.Fatalf("workload run mailbox = %+v", input.RunMailbox)
	}
	for _, value := range input.ReservedEnvironment {
		if value.Name == contract.EnvRunDir {
			t.Fatal("the agent assembled a guest run directory; the helper mints it")
		}
	}
}

func stringPointer(value string) *string { return &value }

// TestWorkloadInputPairsTheAttemptCredentialWithItsEndpoint is the regression
// for a defect #485 left latent and this milestone's first default OCI run
// surfaced: credential delivery became opt-in, so an ordinary reporting run
// reaches the adapter with its attempt credential already withheld and the L1
// endpoint still set beside it. The helper refuses that pair on sight -- and
// should -- so every such run failed at spawn with "attempt credential and L1
// endpoint must be supplied together" and was retried forever.
func TestWorkloadInputPairsTheAttemptCredentialWithItsEndpoint(t *testing.T) {
	execution := func(mutate func(contract.ExecutionSpec) contract.ExecutionSpec) workloadrunner.Request {
		spec := contract.ExecutionSpec{
			OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{
					Reference: "example.test/image:v1",
					Digest:    stringPointer("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
				},
				Argv: []string{"/bin/sh"},
			},
			Env: map[string]string{
				contract.EnvL1Endpoint: "http://127.0.0.1:7001",
				contract.EnvL3Endpoint: "http://127.0.0.1:7002",
			},
			SensitiveEnv: map[string]string{},
		}
		return workloadrunner.Request{Authority: mailboxAdapterAuthority(), Execution: mutate(spec)}
	}

	t.Run("a reporting run carries neither", func(t *testing.T) {
		// Exactly what withholdWorkloadCredentials leaves behind: the endpoint
		// still named, the credential gone.
		input := workloadInput(execution(func(spec contract.ExecutionSpec) contract.ExecutionSpec { return spec }))
		if input.AttemptToken != "" || input.L1Endpoint != "" {
			t.Fatalf("a withheld attempt carried token=%q endpoint=%q, which the helper refuses as a half pair",
				input.AttemptToken, input.L1Endpoint)
		}
		// The run ledger is a separate surface and is not withheld with it.
		if input.L3Endpoint != "http://127.0.0.1:7002" {
			t.Fatalf("L3 endpoint = %q, want it untouched", input.L3Endpoint)
		}
	})

	t.Run("a dispatching run carries both", func(t *testing.T) {
		input := workloadInput(execution(func(spec contract.ExecutionSpec) contract.ExecutionSpec {
			spec.SensitiveEnv[contract.EnvAttemptToken] = "attempt-bearer"
			return spec
		}))
		if input.AttemptToken != "attempt-bearer" || input.L1Endpoint != "http://127.0.0.1:7001" {
			t.Fatalf("a dispatching attempt carried token=%q endpoint=%q, want both",
				input.AttemptToken, input.L1Endpoint)
		}
	})

	t.Run("a credential without its endpoint carries neither", func(t *testing.T) {
		input := workloadInput(execution(func(spec contract.ExecutionSpec) contract.ExecutionSpec {
			delete(spec.Env, contract.EnvL1Endpoint)
			spec.SensitiveEnv[contract.EnvAttemptToken] = "attempt-bearer"
			return spec
		}))
		if input.AttemptToken != "" || input.L1Endpoint != "" {
			t.Fatalf("a credential with no endpoint carried token=%q endpoint=%q", input.AttemptToken, input.L1Endpoint)
		}
	})
}
