//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type custodyNetworkEngine struct {
	*preAdmissionRenewalEngine
	requests chan ocihelper.ExportComputerCustodyRequest
}

func (e *custodyNetworkEngine) ExportComputerCustody(_ context.Context, request ocihelper.ExportComputerCustodyRequest) (ocihelper.ExportComputerCustodyResponse, error) {
	e.requests <- request
	return ocihelper.ExportComputerCustodyResponse{}, &ocihelper.RPCError{Code: ocihelper.CodeOCISpecRejected, Message: "export request observed"}
}

func TestCustodyNetworkReachesHelperRequest(t *testing.T) {
	for _, network := range []l1.ComputerNetwork{l1.ComputerNetworkNone, l1.ComputerNetworkInternet, ""} {
		t.Run("network_"+string(network), func(t *testing.T) {
			engine := &custodyNetworkEngine{newPreAdmissionRenewalEngine(), make(chan ocihelper.ExportComputerCustodyRequest, 1)}
			close(engine.releaseImage)
			close(engine.releaseDelete)
			barrier, stop := startPreAdmissionHelper(t, engine, nil)
			defer stop()
			if err := barrier.Ensure(t.Context()); err != nil {
				t.Fatal(err)
			}
			controller := newCustodyController(nil, ocirunner.NewAdapter(barrier), "pre-admission-node", "pre-admission-boot", "root", nil)
			err := controller.process(t.Context(), l1.ComputerCustodyExportDirective{
				ExportID: "export", BackupID: "backup", CopyID: "copy", ComputerID: "computer", StorageID: "storage", StorageGeneration: 1,
				AllocatedSize: 1024, ContentDigest: "digest", BoundNodeID: "pre-admission-node", RootInstanceID: "root",
				ExternalPath: "/exports/source", CustodyFence: "fence", OperationRevision: 1, Network: network,
			})
			if err == nil {
				t.Fatal("expected observation refusal")
			}
			select {
			case request := <-engine.requests:
				encoded, err := json.Marshal(request)
				if err != nil {
					t.Fatal(err)
				}
				var fields map[string]any
				if err := json.Unmarshal(encoded, &fields); err != nil {
					t.Fatal(err)
				}
				observed, _ := fields["network"].(string)
				if observed != string(network) {
					t.Fatalf("network lost before helper: %s; want %q", encoded, network)
				}
			default:
				t.Fatalf("request did not reach helper: %v", err)
			}
		})
	}
}
