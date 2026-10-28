//go:build linux

package ocihelper

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// This uses the existing fake disk system and only the test user's files.
// It needs neither containerd nor root, so it belongs in the Linux unit job.
func TestCustodyExportManifestKeepsNetwork(t *testing.T) {
	for _, network := range []string{"none", "internet", ""} {
		t.Run("network_"+network, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			mountRoot, externalRoot := custodyOperatorMountRoot(t)
			request := custodyExportTestRequest(source, externalRoot)
			request.Network = network
			engine := &ContainerdEngine{config: custodyEngineConfig(root, mountRoot), diskSystem: system}
			result, err := engine.ExportComputerCustody(t.Context(), request)
			if err != nil || result.Receipt.Kind != "computer_custody_export_verified" {
				t.Fatalf("export: %+v %v", result, err)
			}
			payload, err := os.ReadFile(filepath.Join(externalRoot, "custody.json"))
			if err != nil {
				t.Fatal(err)
			}
			var manifest computerCustodyManifest
			if err := json.Unmarshal(payload, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.Network != network || manifest.Phase != "complete" || custodyManifestDigest(payload) != result.Receipt.ManifestDigest {
				t.Fatalf("wrong portable network/digest: %s", payload)
			}
			// An old manifest may resume under a new request without changing
			// its unknown network or its digest.
			if network == "" {
				request.Network = "none"
			}
			resumed, err := engine.ExportComputerCustody(t.Context(), request)
			if err != nil || resumed.Receipt.Kind != "computer_custody_export_verified" || resumed.Receipt.ManifestDigest != result.Receipt.ManifestDigest {
				t.Fatalf("resume matching or unknown network: %+v %v", resumed, err)
			}
			if network != "" {
				request.Network = "internet"
				if network == "internet" {
					request.Network = "none"
				}
				result, err = engine.ExportComputerCustody(t.Context(), request)
				if err != nil || result.Receipt.FailureCode != "destination_not_empty" {
					t.Fatalf("conflicting network was accepted: %+v %v", result, err)
				}
			}
		})
	}
}
