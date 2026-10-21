package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// Fixed fictional paths keep profile projections, input digests and plist bytes
// comparable across base and current code, without writing an installation.
func TestSetupBaseByteProof(t *testing.T) {
	home := "/wefty-setup-byte-proof"
	paths := setupPaths{home: home, bin: home + "/.local/bin", config: home + "/.config/wefty", profile: home + "/.config/wefty/profile.json", data: home + "/data", logs: home + "/logs", nodeDataFolders: []string{"data/node-agent/log-spool", "data/node-agent/managed"}}
	for _, role := range []serviceplan.Role{serviceplan.Legacy, serviceplan.ControlPlane, serviceplan.Join} {
		t.Run(fmt.Sprint(role), func(t *testing.T) {
			p, err := desiredSetupProfile(setupHost{username: "fixture-owner", l1Address: "127.0.0.1:47101", l3Address: "127.0.0.1:47102", random: strings.NewReader(strings.Repeat("x", 32))}, connprofile.Profile{}, "fixture-node")
			if err != nil {
				t.Fatal(err)
			}
			if role != serviceplan.Legacy {
				p.Fabric = "tsnet"
				p.PlainFabricID = ""
				p.L1Address = "opaque-l1"
				p.L3Address = "opaque-l3"
				p.AllowPlainPersonIdentities = false
				p.Participants = map[string]connprofile.Participant{}
				plan, err := setupRolePlan(paths, role)
				if err != nil {
					t.Fatal(err)
				}
				participants, err := fleetSetupParticipants(plan, "fixture-node")
				if err != nil {
					t.Fatal(err)
				}
				for _, part := range participants {
					p.Participants[part.role] = connprofile.Participant{Name: part.name, StateDirectory: filepath.Join(paths.data, "fabric", part.role), NodeID: "opaque-" + part.role}
				}
				if role == serviceplan.Join {
					p.NodeAdmission = nil
				} else {
					p.AutomaticNodeAdmission = true
				}
			}
			var result bytes.Buffer
			payload, err := connprofile.Encode(p)
			if err != nil {
				t.Fatal(err)
			}
			result.Write(payload)
			services, err := setupRoleServices(paths, p, map[string]string{"wefty": "cli-bytes", "wefty-l1": "l1-bytes", "wefty-l3": "l3-bytes", "wefty-agent": "agent-bytes"}, "/usr/bin:/bin", role)
			if err != nil {
				t.Fatal(err)
			}
			receipt := setupManifest{Role: role, Version: 1, InstalledVersion: "fixture-version", Files: map[string]string{}}
			for _, service := range services {
				payload, err := launchagent.Render(service)
				if err != nil {
					t.Fatal(err)
				}
				result.Write(payload)
				receipt.Files[service.Label] = payloadDigest(payload)
			}
			payload, err = json.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			result.Write(payload)
			payload, err = json.Marshal(setupReport{Role: role, NodeID: "fixture-node", Profile: paths.profile, Participants: map[string]string{}})
			if err != nil {
				t.Fatal(err)
			}
			result.Write(payload)
			// Recorded from the exact 4de008e0 source through a Go overlay.
			want := map[serviceplan.Role]string{
				serviceplan.Legacy:       "sha256:d527c0e0099b85f9cc26db1b0cb546fc266f10efdcbc7e49b272634dda7b6378",
				serviceplan.ControlPlane: "sha256:ae97222def0d3fa490cb90effc9cb34868d0c36ee3cc4a719aeaa32eba974c92",
				serviceplan.Join:         "sha256:f97db22b051cddcb034c1183a6d572fa66ed25f9559576311593b81770fb2f2b",
			}[role]
			if got := payloadDigest(result.Bytes()); got != want {
				t.Fatalf("setup bytes differ from 4de008e0: got %s want %s", got, want)
			}
		})
	}
}
