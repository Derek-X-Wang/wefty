package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

func TestLegacyServicePlanRenderedBytesMatchBase(t *testing.T) {
	paths := setupPaths{home: "/test/home", bin: "/test/home/.local/bin", data: "/test/home/data", logs: "/test/home/logs", profile: "/test/home/profile.json", nodeDataFolders: []string{"data/spool", "data/managed"}}
	profile := connprofile.Profile{Version: 1, Fabric: "plain", PlainFabricID: "plain-golden", L1Address: "127.0.0.1:47101", L3Address: "127.0.0.1:47102", Identities: connprofile.Identities{Node: "golden-node"}}
	agents, err := setupServices(paths, profile, map[string]string{"wefty-l1": "l1-digest", "wefty-l3": "l3-digest", "wefty-agent": "agent-digest"}, "/test/tools:/usr/bin:/bin")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"066ac35ef7371cd0f1549b706675ab941cd21b98263cc2d78ce7c8a2aa9dddf1", "8730c67c3504ec47b0df7e51ce5e7603d8032732ecfcb3c46e108a5250de9f9f", "d5ea4f638ed853078cd01995dd07272ad6310cdeebc13b7493d7789397113ec0"}
	if len(agents) != len(want) {
		t.Fatalf("legacy services=%d", len(agents))
	}
	for i, agent := range agents {
		payload, err := launchagent.Render(agent)
		if err != nil {
			t.Fatal(err)
		}
		got := fmt.Sprintf("%x", sha256.Sum256(payload))
		if got != want[i] {
			t.Errorf("%s rendered hash=%s want=%s", agent.Label, got, want[i])
		}
	}
}

func TestFleetServicePlanMacAdapterAndRoleMetadata(t *testing.T) {
	paths := setupPaths{home: "/test/home", bin: "/test/home/bin", data: "/test/home/data", logs: "/test/home/logs", profile: "/test/home/profile.json", nodeDataFolders: []string{"data/spool", "data/managed"}}
	for _, tc := range []struct {
		role   serviceplan.Role
		labels []string
	}{{serviceplan.ControlPlane, []string{setupL1Label, setupL3Label, setupNodeAgentLabel, "dev.wefty.client-service"}}, {serviceplan.Join, []string{setupNodeAgentLabel, "dev.wefty.client-service"}}} {
		agents, err := setupRoleServices(paths, connprofile.Profile{}, nil, "/tools", tc.role)
		if err != nil {
			t.Fatal(err)
		}
		var labels []string
		for _, agent := range agents {
			labels = append(labels, agent.Label)
			if _, err := launchagent.Render(agent); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(labels, tc.labels) {
			t.Fatalf("%s labels=%v", tc.role, labels)
		}
		client := agents[len(agents)-1]
		if client.Program != "/test/home/bin/wefty" || !reflect.DeepEqual(client.Arguments, []string{"--profile=/test/home/profile.json", "client-service", "--socket=/test/home/.config/wefty/client/service.sock"}) {
			t.Fatalf("client service=%+v", client)
		}
		receipt := setupManifest{Version: 1, Role: tc.role, Files: map[string]string{}}
		payload, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		var saved setupManifest
		if err := json.Unmarshal(payload, &saved); err != nil || saved.Role != tc.role {
			t.Fatalf("role metadata roundtrip=%+v err=%v", saved, err)
		}
	}
	legacy, _ := json.Marshal(setupManifest{Version: 1, Files: map[string]string{}})
	if strings.Contains(string(legacy), "role") {
		t.Fatalf("legacy receipt gained role bytes: %s", legacy)
	}
}
