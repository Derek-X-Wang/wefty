package ocihelper

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// runMailboxEngine is a fakeEngine that also serves the mailbox, so the server
// tests can prove the authorization rules without a filesystem. Confinement is
// proved separately, against a real directory, in run_mailbox_confinement_test.
type runMailboxFakeEngine struct {
	*fakeEngine
	mu       sync.Mutex
	requests []RunMailboxReference
	names    []string
	payload  []byte
}

func (engine *runMailboxFakeEngine) record(reference RunMailboxReference) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	engine.requests = append(engine.requests, reference)
}

func (engine *runMailboxFakeEngine) served() []RunMailboxReference {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return append([]RunMailboxReference(nil), engine.requests...)
}

func (engine *runMailboxFakeEngine) ListRunMailbox(_ context.Context, request ListRunMailboxRequest) (ListRunMailboxResponse, error) {
	engine.record(request.RunMailboxReference)
	limit := request.boundedLimit()
	names := engine.names
	if len(names) > limit {
		return ListRunMailboxResponse{Names: names[:limit], Exhausted: true}, nil
	}
	return ListRunMailboxResponse{Names: names, Exhausted: len(names) >= limit}, nil
}

func (engine *runMailboxFakeEngine) ReadRunMailbox(_ context.Context, request ReadRunMailboxRequest) (ReadRunMailboxResponse, error) {
	engine.record(request.RunMailboxReference)
	limit := request.boundedLimit()
	if len(engine.payload) > limit {
		return ReadRunMailboxResponse{Payload: engine.payload[:limit], Truncated: true}, nil
	}
	return ReadRunMailboxResponse{Payload: engine.payload}, nil
}

func (engine *runMailboxFakeEngine) RemoveRunMailboxEntry(_ context.Context, request RemoveRunMailboxEntryRequest) (RemoveRunMailboxEntryResponse, error) {
	engine.record(request.RunMailboxReference)
	return RemoveRunMailboxEntryResponse{Removed: true}, nil
}

const (
	mailboxTestOwnerKey = "run-owner-1"
	mailboxTestRunID    = "run_0001"
)

func testRunMailboxRunRequest(authority AttemptAuthority) RunRequest {
	request := testRunRequest(authority, time.Second)
	request.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeHandoff, OwnerKey: mailboxTestOwnerKey}}
	request.Workload.RunMailbox = &RunMailboxSeed{RunID: mailboxTestRunID, Params: []byte(`{"branch":"main"}`)}
	return request
}

func startRunMailboxSession(t *testing.T, engine Engine, run func(AttemptAuthority) RunRequest) (*Session, AttemptAuthority) {
	t.Helper()
	client, stop := startTestServer(t, engine, ServerConfig{})
	t.Cleanup(stop)
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	requireSweep(t, session)
	authority := testAuthority()
	if run != nil {
		if _, err := session.Run(t.Context(), run(authority)); err != nil {
			t.Fatal(err)
		}
	}
	return session, authority
}

func mailboxReference(authority AttemptAuthority) RunMailboxReference {
	return RunMailboxReference{Authority: authority, OwnerKey: mailboxTestOwnerKey, RunID: mailboxTestRunID}
}

// TestRunMailboxRequiresTheAttemptThatDeclaredIt is the whole authority rule:
// a live attempt, and the exact mailbox that attempt's Run declared. Attempt
// liveness alone is not enough, because a handoff volume is keyed by run and
// any live attempt could otherwise name any run's volume on the node.
func TestRunMailboxRequiresTheAttemptThatDeclaredIt(t *testing.T) {
	engine := &runMailboxFakeEngine{fakeEngine: newFakeEngine(), names: []string{"0001-event"}}
	session, authority := startRunMailboxSession(t, engine, testRunMailboxRunRequest)

	t.Run("the declaring attempt is served", func(t *testing.T) {
		response, err := session.ListRunMailbox(t.Context(), ListRunMailboxRequest{RunMailboxReference: mailboxReference(authority)})
		if err != nil {
			t.Fatalf("list the attempt's own mailbox: %v", err)
		}
		if len(response.Names) != 1 || response.Names[0] != "0001-event" {
			t.Fatalf("names = %v", response.Names)
		}
		if served := engine.served(); len(served) != 1 || served[0].OwnerKey != mailboxTestOwnerKey {
			t.Fatalf("engine served %v", served)
		}
	})

	for _, testCase := range []struct {
		name      string
		reference RunMailboxReference
		wantCode  ErrorCode
	}{
		{
			name: "another run's handoff volume",
			reference: RunMailboxReference{
				Authority: authority, OwnerKey: "someone-elses-run", RunID: mailboxTestRunID,
			},
			wantCode: CodeUnauthorizedAttempt,
		},
		{
			name: "another run inside this attempt's own volume",
			reference: RunMailboxReference{
				Authority: authority, OwnerKey: mailboxTestOwnerKey, RunID: "run_0002",
			},
			wantCode: CodeUnauthorizedAttempt,
		},
		{
			name: "an attempt that is not live",
			reference: func() RunMailboxReference {
				other := testAuthority()
				other.AttemptID = "attempt-2"
				return mailboxReference(other)
			}(),
			wantCode: CodeAttemptOutsideSession,
		},
		{
			// The fencing token is part of the attempt key, so a stale fence
			// does not name a live attempt at all.
			name: "an attempt whose fencing token does not match",
			reference: func() RunMailboxReference {
				stale := authority
				stale.FencingToken = "fence-stale"
				return mailboxReference(stale)
			}(),
			wantCode: CodeAttemptOutsideSession,
		},
		{
			name: "a reference with no run ID at all",
			reference: RunMailboxReference{
				Authority: authority, OwnerKey: mailboxTestOwnerKey,
			},
			wantCode: CodeInvalidRequest,
		},
		{
			name: "a run ID that is not one bounded component",
			reference: RunMailboxReference{
				Authority: authority, OwnerKey: mailboxTestOwnerKey, RunID: "../../etc",
			},
			wantCode: CodeInvalidRequest,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			before := len(engine.served())
			_, err := session.ListRunMailbox(t.Context(), ListRunMailboxRequest{RunMailboxReference: testCase.reference})
			requireRunMailboxCode(t, err, testCase.wantCode)
			if after := len(engine.served()); after != before {
				t.Fatal("a refused request still reached the engine")
			}
		})
	}
}

// TestRunMailboxRefusesAnAttemptWithoutOne proves an ordinary OCI attempt that
// never declared a mailbox cannot acquire one by asking.
func TestRunMailboxRefusesAnAttemptWithoutOne(t *testing.T) {
	engine := &runMailboxFakeEngine{fakeEngine: newFakeEngine()}
	session, authority := startRunMailboxSession(t, engine, func(authority AttemptAuthority) RunRequest {
		return testRunRequest(authority, time.Second)
	})
	_, err := session.ListRunMailbox(t.Context(), ListRunMailboxRequest{RunMailboxReference: mailboxReference(authority)})
	requireRunMailboxCode(t, err, CodeUnauthorizedAttempt)
	if served := engine.served(); len(served) != 0 {
		t.Fatalf("engine served %v for an attempt with no mailbox", served)
	}
}

// TestRunMailboxNameIsRefusedBeforeTheEngine keeps every path that could become
// a filesystem name behind one rule, checked before any engine sees it.
func TestRunMailboxNameIsRefusedBeforeTheEngine(t *testing.T) {
	engine := &runMailboxFakeEngine{fakeEngine: newFakeEngine(), payload: []byte("payload")}
	session, authority := startRunMailboxSession(t, engine, testRunMailboxRunRequest)
	for _, name := range []string{"", ".", "..", ".published", "a/b", "../escape", strings.Repeat("a", MaxRunMailboxNameBytes+1), "event\x00"} {
		t.Run("name "+name, func(t *testing.T) {
			_, err := session.ReadRunMailbox(t.Context(), ReadRunMailboxRequest{
				RunMailboxReference: mailboxReference(authority), Name: name,
			})
			requireRunMailboxCode(t, err, CodeInvalidRequest)
			_, err = session.RemoveRunMailboxEntry(t.Context(), RemoveRunMailboxEntryRequest{
				RunMailboxReference: mailboxReference(authority), Name: name,
			})
			requireRunMailboxCode(t, err, CodeInvalidRequest)
		})
	}
	if served := engine.served(); len(served) != 0 {
		t.Fatalf("engine served %d refused names", len(served))
	}
}

// TestRunMailboxCapsAreTheHelpersNotTheCallers proves a caller cannot ask for
// more than the protocol allows, in either direction.
func TestRunMailboxCapsAreTheHelpersNotTheCallers(t *testing.T) {
	names := make([]string, MaxRunMailboxListNames+64)
	for index := range names {
		names[index] = "event"
	}
	engine := &runMailboxFakeEngine{
		fakeEngine: newFakeEngine(), names: names,
		payload: make([]byte, MaxRunMailboxReadBytes+1024),
	}
	session, authority := startRunMailboxSession(t, engine, testRunMailboxRunRequest)

	listed, err := session.ListRunMailbox(t.Context(), ListRunMailboxRequest{
		RunMailboxReference: mailboxReference(authority), Limit: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Names) != MaxRunMailboxListNames || !listed.Exhausted {
		t.Fatalf("listing = %d names exhausted=%t, want the cap and exhausted", len(listed.Names), listed.Exhausted)
	}

	read, err := session.ReadRunMailbox(t.Context(), ReadRunMailboxRequest{
		RunMailboxReference: mailboxReference(authority), Name: "0001-event", Limit: 1 << 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Payload) != MaxRunMailboxReadBytes || !read.Truncated {
		t.Fatalf("read = %d bytes truncated=%t, want the cap and truncated", len(read.Payload), read.Truncated)
	}
}

// TestRunMailboxSeedIsValidatedOnTheWire keeps a malformed seed from ever
// reaching the engine that would create directories from it.
func TestRunMailboxSeedIsValidatedOnTheWire(t *testing.T) {
	for _, testCase := range []struct {
		name string
		seed *RunMailboxSeed
		want string
	}{
		{name: "no run ID", seed: &RunMailboxSeed{}, want: "bounded mailbox name"},
		{name: "traversal run ID", seed: &RunMailboxSeed{RunID: "../run"}, want: "bounded mailbox name"},
		{name: "dotted run ID", seed: &RunMailboxSeed{RunID: ".published"}, want: "bounded mailbox name"},
		{name: "oversize params", seed: &RunMailboxSeed{RunID: mailboxTestRunID, Params: make([]byte, MaxRunMailboxParamsBytes+1)}, want: "exceed"},
		{name: "params that are not JSON", seed: &RunMailboxSeed{RunID: mailboxTestRunID, Params: []byte("not json")}, want: "valid JSON"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			input := testRunMailboxRunRequest(testAuthority()).Workload
			input.RunMailbox = testCase.seed
			err := validateWorkloadWire(input)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("validation error = %v, want one mentioning %q", err, testCase.want)
			}
		})
	}
	t.Run("a mailbox without a handoff volume", func(t *testing.T) {
		input := testRunMailboxRunRequest(testAuthority()).Workload
		input.ManagedVolumes = nil
		err := validateWorkloadWire(input)
		if err == nil || !strings.Contains(err.Error(), "handoff managed volume") {
			t.Fatalf("validation error = %v", err)
		}
	})
}

// TestRunMailboxContainerDirectoryIsHelperMinted proves the guest path the
// workload receives is derived from the seed and never supplied by a caller.
func TestRunMailboxContainerDirectoryIsHelperMinted(t *testing.T) {
	seed := RunMailboxSeed{RunID: mailboxTestRunID}
	if got, want := seed.ContainerDirectory(), "/wefty/handoff/.wefty/"+mailboxTestRunID; got != want {
		t.Fatalf("container directory = %q, want %q", got, want)
	}
}

func requireRunMailboxCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("error = %v, want a typed %s refusal", err, want)
	}
	if rpcErr.Code != want {
		t.Fatalf("error code = %s (%s), want %s", rpcErr.Code, rpcErr.Message, want)
	}
}

// TestRunMailboxMintedEnvironmentSurvivesHelperValidation is the test whose
// absence cost a lane cycle. Every earlier mailbox test drove a fake engine,
// which never mints reserved environment and never builds a runtime spec, so
// nothing exercised the one step that actually runs on a real node: the helper
// validating the environment it minted for itself.
//
// WEFTY_RUN_DIR was not a reserved name, so the helper minted a value its own
// two validation sites then refused -- validateEnvironmentLayer's reserved
// layer and the runtime spec's environment merge -- and every OCI run with a
// mailbox failed inside Run before its container started.
func TestRunMailboxMintedEnvironmentSurvivesHelperValidation(t *testing.T) {
	seed := RunMailboxSeed{RunID: mailboxTestRunID}
	minted := []EnvironmentVariable{
		{Name: contract.EnvHandoffDir, Value: contract.OCIContainerHandoffDirectory},
		{Name: contract.EnvRunDir, Value: seed.ContainerDirectory()},
		{Name: contract.EnvL3Endpoint, Value: "http://127.0.0.1:9/"},
	}
	for _, variable := range minted {
		if !contract.IsOCIReservedEnvironmentName(variable.Name) {
			t.Fatalf("the helper mints %q but it is not a reserved name, so the helper refuses its own value",
				variable.Name)
		}
	}

	input := testRunMailboxRunRequest(testAuthority()).Workload
	input.ReservedEnvironment = minted
	input.helperMintedReserved = true
	if err := validateWorkloadWire(input); err != nil {
		t.Fatalf("the helper refused the environment it minted: %v", err)
	}

	// The runtime spec's own merge is the second gate, and it refuses on the
	// same rule. Both must accept what Run produces.
	merged, err := mergeRuntimeEnvironment(nil, nil, nil, minted)
	if err != nil {
		t.Fatalf("runtime spec refused the minted environment: %v", err)
	}
	if !slices.Contains(merged, contract.EnvRunDir+"="+seed.ContainerDirectory()) {
		t.Fatalf("merged environment lost the mailbox directory: %v", merged)
	}
}

// TestRunMailboxDirectoryIsNotSubmitterSupplied proves the reserved status does
// the other half of its job: a submitter or image that names WEFTY_RUN_DIR is
// stripped, never able to redirect the workload's reporting writer.
func TestRunMailboxDirectoryIsNotSubmitterSupplied(t *testing.T) {
	forged := "/tmp/attacker-chosen"
	merged, err := mergeRuntimeEnvironment(
		[]string{contract.EnvRunDir + "=" + forged},
		[]EnvironmentVariable{{Name: contract.EnvRunDir, Value: forged}},
		[]EnvironmentVariable{{Name: contract.EnvRunDir, Value: forged}},
		[]EnvironmentVariable{{Name: contract.EnvRunDir, Value: "/wefty/handoff/.wefty/" + mailboxTestRunID}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range merged {
		if strings.Contains(entry, forged) {
			t.Fatalf("a submitter-supplied run directory survived: %q", entry)
		}
	}
}
