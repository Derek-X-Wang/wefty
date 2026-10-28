package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestStorageCopyNetworkCLIOutputs(t *testing.T) {
	for _, network := range []l1.ComputerNetwork{l1.ComputerNetworkNone, l1.ComputerNetworkInternet} {
		t.Run("clone_"+string(network), func(t *testing.T) {
			h := newStorageCLIHarness(t, &l1.ComputerSettings{SeatLimit: 2, IdleAction: l1.ComputerIdleDelete, Network: network})
			runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID, "--cap", "1", "--expect-current")
			runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID, "--expect-current", "--idempotency-key", "network-backup", "--allow-power-off")
			h.completeBackupHelper(t)
			backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
			if err != nil || len(backups.Backups) != 1 {
				t.Fatalf("Backup: %+v %v", backups, err)
			}
			args := []string{"services", "clone", h.computer.ComputerID, backups.Backups[0].BackupID, "--name", "network-clone", "--disk-bytes", fmt.Sprint(2 << 30), "--expect-current", "--idempotency-key", "network-clone"}
			for _, structured := range []bool{false, true} {
				output := runStorageCLI(t, h.ctx, h.clients, structured, args...)
				if structured {
					var reply storageMutationOutput
					if err := json.Unmarshal(output, &reply); err != nil || reply.Computer == nil || reply.Computer.Network != network {
						t.Fatalf("clone JSON network: %s %v", output, err)
					}
				} else if !strings.Contains(string(output), "NETWORK") || !strings.Contains(string(output), string(network)) {
					t.Fatalf("clone text lacks network: %s", output)
				}
			}
		})
	}
	for _, network := range []string{"none", "internet", ""} {
		t.Run("import_"+network, func(t *testing.T) {
			h := newStorageCLIHarness(t)
			manifest, path, _ := writeStorageCLICustodyManifest(t, h)
			manifest.Network = network
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			digest, err := contract.DigestComputerCustodyManifest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			args := []string{"services", "custody", "import", manifest.ExportID, "--name", "network-import", "--disk-bytes", fmt.Sprint(2 << 30), "--node", h.node.NodeID, "--path", t.TempDir(), "--manifest", path, "--manifest-digest", digest, "--idempotency-key", "network-import"}
			want := network
			if want == "" {
				want = "none"
			}
			for _, structured := range []bool{false, true} {
				output := runStorageCLI(t, h.ctx, h.clients, structured, args...)
				if structured {
					var reply storageMutationOutput
					if err := json.Unmarshal(output, &reply); err != nil || reply.CustodyImport == nil || string(reply.CustodyImport.Network) != want {
						t.Fatalf("import JSON network: %s %v", output, err)
					}
				} else if !strings.Contains(string(output), "NETWORK") || !strings.Contains(string(output), want) {
					t.Fatalf("import text lacks network: %s", output)
				}
				if network == "" && !strings.Contains(string(output), "Source network is unknown; using none.") {
					t.Fatalf("unknown network note missing: %s", output)
				}
			}
		})
	}
}
