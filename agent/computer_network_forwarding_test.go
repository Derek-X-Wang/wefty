//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type computerNetworkForwardingEngine struct {
	*preAdmissionRenewalEngine
	inputs chan ocihelper.WorkloadInput
}

func (e *computerNetworkForwardingEngine) Run(ctx context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	if !request.Workload.Computer {
		return e.preAdmissionRenewalEngine.Run(ctx, request)
	}
	e.inputs <- request.Workload
	// Observe admission without creating a task or starting desktop services.
	return ocihelper.RunResponse{}, &ocihelper.RPCError{Code: ocihelper.CodeOCISpecRejected, Message: "network forwarding observed"}
}

type computerNetworkForwardingRuntime struct{ *ocirunner.Adapter }

func (r computerNetworkForwardingRuntime) Preflight(ctx context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	// The real lifecycle compiles the request. Send it through the real adapter
	// and helper before the unrelated desktop lifecycle. Replace only the L1
	// image-observation callback and the unused private submission dial.
	request.OCIImageResolved = func(context.Context, workloadrunner.OCIImageObservation) error { return nil }
	request.HostBridgeDial = func(context.Context) (net.Conn, error) { return nil, errors.New("unexpected bridge dial") }
	request.HostBridgeFallbackActive = true
	result, err := r.Adapter.Run(ctx, request, nil)
	return workloadrunner.Admission{Request: request}, result, err
}

func TestComputerClaimNetworkReachesHelperInput(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings *l1.ComputerSettings
		want     string
		refused  bool
	}{
		{name: "none", settings: &l1.ComputerSettings{Network: l1.ComputerNetworkNone}, want: "none"},
		{name: "missing-settings", want: "internet"},
		{name: "empty-setting", settings: &l1.ComputerSettings{}, want: "internet"},
		{name: "internet", settings: &l1.ComputerSettings{Network: l1.ComputerNetworkInternet}, want: "internet"},
		{name: "unknown", settings: &l1.ComputerSettings{Network: "filtered"}, refused: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := &computerNetworkForwardingEngine{newPreAdmissionRenewalEngine(), make(chan ocihelper.WorkloadInput, 1)}
			close(engine.releaseImage)
			close(engine.releaseDelete)
			barrier, stop := startPreAdmissionHelper(t, engine, time.Now)
			defer stop()
			if err := barrier.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			adapter := ocirunner.NewAdapter(barrier)
			if err := adapter.Probe(t.Context(), "pre-admission-node", "pre-admission-boot", preAdmissionImageReference, preAdmissionImageDigest, 2*time.Second); err != nil {
				t.Fatal(err)
			}
			memory := int64(1 << 30)
			digest := preAdmissionImageDigest
			claim := l1.Claim{
				Job: l1.Job{JobID: "network-job", Spec: contract.JobSpec{
					Kind: contract.JobKindOCI, Class: contract.JobClassService,
					Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
						Image:    contract.OCIImageSpec{Reference: preAdmissionImageReference, Digest: &digest},
						Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}, Limits: &contract.OCILimits{MemoryBytes: &memory},
					}},
				}},
				Lease: l1.AttemptLease{AttemptID: "network-attempt", FencingToken: "network-fence", LeaseTTL: time.Second},
				ComputerStorage: &l1.ComputerStorageClaim{ComputerSettings: test.settings,
					ComputerID: "network-computer", StorageID: "network-storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 8 << 30},
			}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				nodeID: "pre-admission-node", bootSessionID: "pre-admission-boot", clock: systemClock{},
				runtimes: workloadRuntimeSet{contract.JobKindOCI: computerNetworkForwardingRuntime{adapter}},
			})
			_, err := lifecycle.runWorkload(t.Context(), claim)
			var rpc *ocihelper.RPCError
			wantCode := ocihelper.CodeOCISpecRejected
			if test.refused {
				wantCode = ocihelper.CodeInvalidRequest
			}
			if !errors.As(err, &rpc) || rpc.Code != wantCode {
				t.Fatalf("forwarding stopped before the expected helper verdict: %v", err)
			}
			select {
			case input := <-engine.inputs:
				if test.refused || input.ComputerNetwork != test.want {
					t.Fatalf("helper received network %q; want %q (refused=%t)", input.ComputerNetwork, test.want, test.refused)
				}
			default:
				if !test.refused {
					t.Fatal("claim never reached helper admission")
				}
			}
		})
	}
}
