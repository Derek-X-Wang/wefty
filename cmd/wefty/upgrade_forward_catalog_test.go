package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
)

func TestUpgradeForwardRecoveryCatalogFaultIsOperational(t *testing.T) {
	for _, rolling := range []bool{false, true} {
		route := "plain"
		if rolling {
			route = "rolling"
		}
		t.Run(route, func(t *testing.T) {
			f := newRollingFixture(t)
			f.operations.requestRolling = rolling
			bootReads := 0
			f.operations.bootIdentity = func() (string, error) { bootReads++; return rollingTestBoot, nil }
			catalog, err := f.install.Read()
			mustRolling(t, err)
			prior := &upgradeRollingReceipt{Version: 1, Protocol: 1, OperationID: "forward-refused-operation", NodeID: f.node.NodeID, SystemBootID: rollingTestBoot, RecoveryState: "rollback_refused", Candidate: catalog.Generations[catalog.Primary]}
			mustRolling(t, writeUpgradeRolling(f.paths, prior, "rolling_back"))
			stage := filepath.Join(f.paths.data, ".upgrade-retained", "wefty-agent")
			mustRolling(t, os.MkdirAll(filepath.Dir(stage), 0700))
			mustRolling(t, os.WriteFile(stage, []byte("retained staging"), 0700))
			mustRolling(t, os.WriteFile(filepath.Join(f.install.Root(), "catalog.json"), []byte("{}"), 0600))
			before := homeSnapshot(t, f.home)
			nodeBefore := f.node
			f.launchd.commands = nil
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			// Both runUpgrade and prepareFurtherRolling must observe this same boot.
			// A refusal from fresh-activation preflight would not exercise this path.
			if bootReads != 2 {
				t.Fatalf("did not enter same-boot forward recovery: boot reads=%d err=%v", bootReads, err)
			}
			if err == nil || commandExitCode(err) != exitFailure {
				t.Errorf("want operational exit 1, exit=%d err=%v", commandExitCode(err), err)
			}
			var document bytes.Buffer
			decideCommandReport(err).writeError(&document, true)
			var response contract.ErrorResponse
			if decodeErr := json.Unmarshal(document.Bytes(), &response); decodeErr != nil || response.Error.Code != contract.ErrorInternal {
				t.Errorf("want JSON internal: %s (%v)", document.String(), decodeErr)
			}
			var cause *agentinstall.VerificationRefusal
			if !errors.As(err, &cause) || !strings.Contains(cause.Error(), "invalid generation catalog") {
				t.Errorf("catalog validation cause lost: %v", err)
			}
			if !reflect.DeepEqual(before, homeSnapshot(t, f.home)) || !reflect.DeepEqual(nodeBefore, f.node) || len(f.launchd.commands) != 0 {
				t.Error("catalog fault changed installed files, receipts, staging, admission or services")
			}
		})
	}
}
