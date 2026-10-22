package l1

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestPlainComputerPolicySnapshotBytes(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node")})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	identity := fabric.Identity{FabricID: "plain-policy-fixture", UserID: "owner", DeviceID: "owner-device"}
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.BootstrapAdmin(t.Context(), identity, challenge.Nonce); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "plain-policy", Spec: computerCapabilityJobSpec("plain-policy"), Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.store.IssueComputerPolicySnapshot(t.Context(), "fabric-computer-node", identity.FabricID, node.NodeID, node.BootSessionID, DefaultComputerPolicyFreshness)
	if err != nil || snapshot == nil || len(snapshot.Computers) != 1 {
		t.Fatalf("snapshot: %#v %v", snapshot, err)
	}
	// Replace only the randomly allocated Computer ID, then recompute the digest.
	snapshot.Computers[0].ComputerID = "computer-fixture"
	snapshot.SnapshotDigest, err = ComputeComputerPolicySnapshotDigest(*snapshot)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"device_id"`)) || bytes.Contains(raw, []byte(`"admin_device_required"`)) {
		t.Fatalf("plain snapshot gained device fields: %s", raw)
	}
	identity.DeviceID = "other-device"
	if decision := EvaluateComputerPolicyPerson(*snapshot, true, "computer-fixture", identity); decision.Permission != ComputerGrantControl || !decision.Administrator {
		t.Fatalf("plain admin device behavior changed: %#v", decision)
	}
	if path := os.Getenv("WEFTY_T1090_POLICY_CAPTURE"); path != "" {
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	expected, err := os.ReadFile("testdata/plain-computer-policy-base-12657df6.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, expected) {
		t.Fatalf("plain snapshot bytes or digest changed:\n%s", raw)
	}
}

// The frozen pre-binding bytes give an independent old-agent digest. Either
// new authority field alone must make that old digest invalid; unknown fields
// cannot be excluded from the digest while still being used for authorization.
func TestComputerPolicyDigestCoversEachAdminDeviceField(t *testing.T) {
	baseline, err := os.ReadFile("testdata/plain-computer-policy-base-12657df6.json")
	if err != nil {
		t.Fatal(err)
	}
	var original ComputerPolicySnapshot
	if err := json.Unmarshal(baseline, &original); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"admin_device_required", "device_id"} {
		t.Run(field, func(t *testing.T) {
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(baseline, &wire); err != nil {
				t.Fatal(err)
			}
			if field == "admin_device_required" {
				wire[field] = json.RawMessage(`true`)
			} else {
				var admins []map[string]json.RawMessage
				if err := json.Unmarshal(wire["admins"], &admins); err != nil {
					t.Fatal(err)
				}
				admins[0][field] = json.RawMessage(`"enrolled-device"`)
				wire["admins"], err = json.Marshal(admins)
				if err != nil {
					t.Fatal(err)
				}
			}
			raw, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			var changed ComputerPolicySnapshot
			if err := json.Unmarshal(raw, &changed); err != nil {
				t.Fatal(err)
			}
			digest, err := ComputeComputerPolicySnapshotDigest(changed)
			if err != nil {
				t.Fatal(err)
			}
			if digest == original.SnapshotDigest {
				t.Fatalf("old-agent digest still valid after changing %s", field)
			}
			if err := ValidateComputerPolicySnapshot(changed); err == nil {
				t.Fatalf("changed %s retained an unchanged digest", field)
			}
		})
	}
}
