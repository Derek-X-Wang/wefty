package l1

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestComputerCloneKeepsSourceNetwork(t *testing.T) {
	for _, network := range []ComputerNetwork{ComputerNetworkNone, ComputerNetworkInternet} {
		t.Run(string(network), func(t *testing.T) {
			h, _, source, backup, _ := publishedBackupForStorageCopy(t, 2)
			// Set up an existing Computer with this immutable creation setting.
			if _, err := h.store.db.Exec(`UPDATE computers SET network=? WHERE computer_id=?`, network, source.ComputerID); err != nil {
				t.Fatal(err)
			}
			request := ComputerCloneRequest{BackupID: backup.BackupID,
				ComputerMutationPrecondition: computerPrecondition(source, "operator"),
				Name:                         "network-clone", DiskBytes: backup.AllocatedSize, IdempotencyKey: "network-clone", Actor: "operator"}
			for _, wantReplay := range []bool{false, true} {
				clone, replayed, err := h.store.BeginComputerClone(t.Context(), request)
				if err != nil || replayed != wantReplay || clone.Network != network {
					t.Fatalf("clone network=%q replay=%t error=%v; want %q replay=%t", clone.Network, replayed, err, network, wantReplay)
				}
				observed, err := h.store.GetComputer(t.Context(), clone.ComputerID)
				if err != nil || observed.Network != network {
					t.Fatalf("observed network=%q error=%v; want %q", observed.Network, err, network)
				}
			}
		})
	}
}

func TestCustodyImportKeepsPortableNetwork(t *testing.T) {
	for _, network := range []string{"none", "internet", "", "invalid"} {
		t.Run("network_"+network, func(t *testing.T) {
			h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
			exported, directive := beginCustodyExport(t, h, node, source, backup, "network-export")
			manifest := custodyManifest(directive)
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if network != "" {
				fields["network"] = network
			} else {
				delete(fields, "network")
			}
			encoded, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			manifest = contract.ComputerCustodyManifest{}
			if err := json.Unmarshal(encoded, &manifest); err != nil {
				t.Fatal(err)
			}
			digest, err := contract.DigestComputerCustodyManifest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			request := ComputerCustodyImportRequest{Name: "network-import", DiskBytes: backup.AllocatedSize,
				NodeID: node.NodeID, ExternalPath: directive.ExternalPath, Manifest: manifest, ManifestDigest: digest,
				IdempotencyKey: "network-import", Actor: "operator"}
			for _, wantReplay := range []bool{false, true} {
				imported, replayed, err := h.store.BeginComputerCustodyImport(t.Context(), exported.ExportID, request)
				if network == "invalid" {
					if errorCode(err) != contract.ErrorInvalidRequest {
						t.Fatalf("invalid network error=%v", err)
					}
					return
				}
				if err != nil || replayed != wantReplay {
					t.Fatalf("import replay=%t error=%v", replayed, err)
				}
				want := network
				if want == "" {
					want = "none"
				}
				computer, err := h.store.GetComputer(t.Context(), imported.DestinationComputerID)
				if err != nil || string(computer.Network) != want {
					t.Fatalf("import network=%q error=%v; want %q", computer.Network, err, want)
				}
				encoded, err := json.Marshal(imported)
				if err != nil {
					t.Fatal(err)
				}
				var reply map[string]any
				if err := json.Unmarshal(encoded, &reply); err != nil {
					t.Fatal(err)
				}
				if reply["network"] != want {
					t.Fatalf("import reply lacks network %q: %s", want, encoded)
				}
				note, _ := reply["network_note"].(string)
				if network == "" && (!strings.Contains(note, "unknown") || !strings.Contains(note, "none")) {
					t.Fatalf("unknown network note missing: %s", encoded)
				}
				if network != "" && note != "" {
					t.Fatalf("known network has fallback note: %s", encoded)
				}
			}
		})
	}
}

func TestCustodyExportDirectiveRecordsSourceNetwork(t *testing.T) {
	for _, network := range []ComputerNetwork{ComputerNetworkNone, ComputerNetworkInternet} {
		t.Run(string(network), func(t *testing.T) {
			h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
			if _, err := h.store.db.Exec(`UPDATE computers SET network=? WHERE computer_id=?`, network, source.ComputerID); err != nil {
				t.Fatal(err)
			}
			_, directive := beginCustodyExport(t, h, node, source, backup, "network-export")
			encoded, err := json.Marshal(directive)
			if err != nil {
				t.Fatal(err)
			}
			var reply map[string]any
			if err := json.Unmarshal(encoded, &reply); err != nil {
				t.Fatal(err)
			}
			if reply["network"] != string(network) {
				t.Fatalf("export directive lacks source network: %s", encoded)
			}
		})
	}
}

func TestCustodyImportReplayReportsLegacyComputerNetwork(t *testing.T) {
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	exported, directive := beginCustodyExport(t, h, node, source, backup, "legacy-network")
	manifest := custodyManifest(directive)
	manifest.Network = ""
	digest, err := contract.DigestComputerCustodyManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	request := ComputerCustodyImportRequest{Name: "legacy-import", DiskBytes: backup.AllocatedSize, NodeID: node.NodeID,
		ExternalPath: directive.ExternalPath, Manifest: manifest, ManifestDigest: digest, IdempotencyKey: "legacy-network", Actor: "operator"}
	imported, _, err := h.store.BeginComputerCustodyImport(t.Context(), exported.ExportID, request)
	if err != nil {
		t.Fatal(err)
	}
	// Upgrade shape: an older build created an internet Computer from this
	// network-less manifest. Replaying must report it, without changing it.
	if _, err := h.store.db.Exec(`UPDATE computers SET network='internet' WHERE computer_id=?`, imported.DestinationComputerID); err != nil {
		t.Fatal(err)
	}
	replay, replayed, err := h.store.BeginComputerCustodyImport(t.Context(), exported.ExportID, request)
	if err != nil || !replayed || replay.Network != ComputerNetworkInternet || !strings.Contains(replay.NetworkNote, "internet") || strings.Contains(replay.NetworkNote, "using none") {
		t.Fatalf("legacy import replay reports wrong fixed network: %+v replay=%t error=%v", replay, replayed, err)
	}
}
