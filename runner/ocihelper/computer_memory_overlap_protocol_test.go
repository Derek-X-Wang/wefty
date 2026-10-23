package ocihelper

import (
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestMutableComputerMemoryOverlapRefusalPreservesSession(t *testing.T) {
	engine := newFakeEngine()
	engine.runErr = &SeatOperationRefusalError{Reason: SeatRefusalAttemptOverlap}
	client, stop := startTestServer(t, engine, ServerConfig{})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	request := testRunRequest(testAuthority(), time.Second)
	request.Authority.Class = contract.JobClassService
	request.Workload.Computer = true
	request.Workload.Limits.MemoryBytes = 64 << 20
	request.Workload.ManagedVolumes = []ManagedVolumeDescriptor{{Kind: ManagedVolumeComputerDisk, ComputerStorage: &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 32 << 20}}}
	request.AllocateEndpoints = []string{contract.ComputerDisplayEndpointView, contract.ComputerDisplayEndpointControl}
	request.EnableHostBridgeFallback, request.ActivateHostBridgeFallback = true, true
	_, err = session.Run(t.Context(), request)
	var rpc *RPCError
	if !errors.As(err, &rpc) || rpc.Code != CodeSeatOperationRefused || string(rpc.Detail) != string(SeatRefusalAttemptOverlap) {
		t.Fatalf("overlap refusal lost: %v", err)
	}
	if session.HealthError() != nil {
		t.Fatal(session.HealthError())
	}
	if _, err := session.Verify(t.Context(), VerifyRequest{Scope: VerifyNamespace}); err != nil {
		t.Fatal(err)
	}
}
