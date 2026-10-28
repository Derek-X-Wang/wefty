package contract

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCustodyManifestNetworkRoundTripAndLegacyBytes(t *testing.T) {
	// Frozen pre-network serialization: old manifests must keep their digest.
	legacy := []byte(`{"version":1,"export_id":"export","backup_id":"","copy_id":"","computer_id":"","storage_id":"","storage_generation":0,"allocated_size":0,"content_digest":"","encryption":"","node_id":"","root_instance_id":"","operation_revision":0,"custody_fence":"","job_spec":{"schema_version":0,"dispatch_key":"","kind":"","class":"","execution":{}},"job_spec_hash":"","disk_file":"","phase":""}`)
	for _, network := range []string{"", "none", "internet"} {
		t.Run("network_"+network, func(t *testing.T) {
			payload := legacy
			if network != "" {
				payload = append(append([]byte{}, legacy[:len(legacy)-1]...), []byte(`,"network":"`+network+`"}`)...)
			}
			var manifest ComputerCustodyManifest
			if err := json.Unmarshal(payload, &manifest); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, payload) {
				t.Fatalf("manifest did not round-trip: got %s want %s", encoded, payload)
			}
		})
	}
}
