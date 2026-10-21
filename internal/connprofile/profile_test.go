package connprofile

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleProfile() Profile {
	return Profile{
		Version: Version, Fabric: "plain", PlainFabricID: "plain-wefty-test",
		L1Address: "127.0.0.1:47101", L3Address: "127.0.0.1:47102",
		Identities:                 Identities{CLI: "wefty-cli", PersonUserID: "owner", PersonDeviceID: "mac-1", Node: "mac-1"},
		AllowPlainPersonIdentities: true,
		NodeAdmission:              []NodeAdmission{{NodeID: "mac-1", Tags: []string{"wefty:node:mac-1"}, MaxOneshotSlots: 4, MaxServiceSlots: 2}},
	}
}

func TestProfileRoundTripsAndEachProgramReadsItsOwnSubset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wefty", FileName)
	profile := sampleProfile()
	if err := Write(path, profile); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("profile mode = %v err=%v", info.Mode(), err)
	}
	read, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if read.L1Address != profile.L1Address || read.Identities != profile.Identities || len(read.NodeAdmission) != 1 {
		t.Fatalf("read profile = %+v", read)
	}
	// L1 listens where everyone else dials it, and dials the run ledger where
	// it listens; the node agent knows only its own node ID.
	if l1 := read.L1(); l1.Listen != "127.0.0.1:47101" || l1.RunLedger != "127.0.0.1:47102" || !l1.AllowPlainPersonIdentities ||
		l1.NodePolicies()["mac-1"].MaxOneshotSlots != 4 {
		t.Fatalf("L1 settings = %+v", l1)
	}
	if l3 := read.L3(); l3.Listen != "127.0.0.1:47102" || l3.ControlPlane != "127.0.0.1:47101" {
		t.Fatalf("L3 settings = %+v", l3)
	}
	if node := read.NodeAgent(); node.NodeID != "mac-1" || node.ControlPlane != "127.0.0.1:47101" || node.RunLedger != "127.0.0.1:47102" {
		t.Fatalf("node agent settings = %+v", node)
	}
	if cli := read.CLI(); cli.L1 != "127.0.0.1:47101" || cli.PersonUserID != "owner" || cli.Identity != "wefty-cli" {
		t.Fatalf("CLI settings = %+v", cli)
	}
	// A setting one program does not read never changes that program's digest.
	edited := read
	edited.NodeAdmission[0].MaxOneshotSlots = 8
	before, _ := Digest(read.L3())
	after, _ := Digest(edited.L3())
	l1Before, _ := Digest(sampleProfile().L1())
	l1After, _ := Digest(edited.L1())
	if before != after || l1Before == l1After {
		t.Fatalf("digests: L3 %s/%s, L1 %s/%s", before, after, l1Before, l1After)
	}
}

func TestAutomaticNodeAdmissionIsOptionalAndOnlyChangesL1Inputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	if err := os.WriteFile(path, []byte(`{"version":1,"automatic_node_admission":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	profile, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := json.MarshalIndent(profile.L1(), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(settings), `"automatic_node_admission": true`) {
		t.Fatalf("L1 settings omit automatic admission: %s", settings)
	}
	base := Profile{Version: Version}
	before, _ := Digest(base.L1())
	after, _ := Digest(profile.L1())
	if before == after {
		t.Fatal("automatic admission did not change L1's input digest")
	}
	for _, pair := range [][2]any{{base.L3(), profile.L3()}, {base.NodeAgent(), profile.NodeAgent()}, {base.CLI(), profile.CLI()}} {
		before, _ := Digest(pair[0])
		after, _ := Digest(pair[1])
		if before != after {
			t.Fatal("automatic admission changed another program's inputs")
		}
	}
	legacy, err := json.MarshalIndent(base, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacy), "automatic_node_admission") {
		t.Fatalf("legacy profile gained a field: %s", legacy)
	}
}

func TestProfileRefusesWhatAProgramCouldNotUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	for name, payload := range map[string]string{
		"version":          `{"version":2}`,
		"unknown field":    `{"version":1,"l1_adress":"127.0.0.1:1"}`,
		"fabric":           `{"version":1,"fabric":"carrier-pigeon"}`,
		"plain fabric ID":  `{"version":1,"plain_fabric_id":"fabric-1"}`,
		"blank node":       `{"version":1,"node_admission":[{"node_id":" ","max_oneshot_slots":1,"max_service_slots":1}]}`,
		"duplicate node":   `{"version":1,"node_admission":[{"node_id":"a","max_oneshot_slots":1,"max_service_slots":1},{"node_id":"a","max_oneshot_slots":1,"max_service_slots":1}]}`,
		"negative slots":   `{"version":1,"node_admission":[{"node_id":"a","max_oneshot_slots":-1,"max_service_slots":1}]}`,
		"invalid tag":      `{"version":1,"node_admission":[{"node_id":"a","tags":["-bad"],"max_oneshot_slots":1,"max_service_slots":1}]}`,
		"spaced address":   `{"version":1,"l1_address":" 127.0.0.1:1"}`,
		"trailing content": `{"version":1}{"version":1}`,
	} {
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(path); err == nil {
			t.Errorf("%s: profile %s was accepted", name, payload)
		}
	}
}

func TestResolveNamedProfileMustExistAndDefaultIsOptional(t *testing.T) {
	directory := t.TempDir()
	missing := filepath.Join(directory, "missing.json")
	if _, _, err := Resolve(missing, ""); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a named missing profile = %v", err)
	}
	profile, path, err := Resolve("", missing)
	if err != nil || path != "" || profile.Version != 0 {
		t.Fatalf("an absent default profile = %+v %q %v", profile, path, err)
	}
	if profile, path, err := Resolve("", ""); err != nil || path != "" || profile.L1Address != "" {
		t.Fatalf("no profile = %+v %q %v", profile, path, err)
	}
	present := filepath.Join(directory, FileName)
	if err := Write(present, sampleProfile()); err != nil {
		t.Fatal(err)
	}
	if profile, path, err := Resolve("", present); err != nil || path != present || profile.L1Address != "127.0.0.1:47101" {
		t.Fatalf("default profile = %+v %q %v", profile, path, err)
	}
	if err := os.WriteFile(present, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Resolve("", present); err == nil || !strings.Contains(err.Error(), present) {
		t.Fatalf("a corrupt default profile = %v", err)
	}
}

// The precedence every program shares: flags, then environment, then the
// profile, then built-in defaults.
func TestLayerPrecedence(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	environment := map[string]string{"WEFTY_FROM_ENV": "env-value", "WEFTY_BLANK": "  "}
	getenv := func(name string) string { return environment[name] }
	flagged := flags.String("flagged", "default", "")
	fromEnvironment := flags.String("from-env", getenv("WEFTY_FROM_ENV"), "")
	blank := flags.String("blank", getenv("WEFTY_BLANK"), "")
	defaulted := flags.String("defaulted", "built-in", "")
	unset := flags.String("unset", "built-in", "")
	enabled := flags.Bool("enabled", false, "")
	kept := flags.Bool("kept", false, "")
	if err := flags.Parse([]string{"--flagged=from-flag", "--kept=false"}); err != nil {
		t.Fatal(err)
	}
	layer := Layer{Explicit: ExplicitFlags(flags), Getenv: getenv}
	layer.Fill(flagged, "flagged", "", "profile")
	layer.Fill(fromEnvironment, "from-env", "WEFTY_FROM_ENV", "profile")
	layer.Fill(blank, "blank", "WEFTY_BLANK", "profile")
	layer.Fill(defaulted, "defaulted", "", "profile")
	layer.Fill(unset, "unset", "", "")
	layer.Enable(enabled, "enabled", true)
	layer.Enable(kept, "kept", true)
	if *flagged != "from-flag" || *fromEnvironment != "env-value" || *blank != "profile" || *defaulted != "profile" || *unset != "built-in" {
		t.Fatalf("flag=%q env=%q blank=%q profile=%q default=%q", *flagged, *fromEnvironment, *blank, *defaulted, *unset)
	}
	if !*enabled || *kept {
		t.Fatalf("enabled=%v kept=%v; an explicit false flag must win over the profile", *enabled, *kept)
	}
}

func TestOneshotLoadLimitSurvivesProfileAndChangesOnlyAgentDigest(t *testing.T) {
	for _, limit := range []float64{0, 18} {
		p := sampleProfile()
		l1Before, _ := Digest(p.L1())
		l3Before, _ := Digest(p.L3())
		agentBefore, _ := Digest(p.NodeAgent())
		p.OneshotLoadLimit = &limit
		path := filepath.Join(t.TempDir(), FileName)
		if err := Write(path, p); err != nil {
			t.Fatal(err)
		}
		got, err := Read(path)
		if err != nil || got.OneshotLoadLimit == nil || *got.OneshotLoadLimit != limit || got.NodeAgent().OneshotLoadLimit == nil || *got.NodeAgent().OneshotLoadLimit != limit {
			t.Fatalf("profile=%+v err=%v", got, err)
		}
		l1After, _ := Digest(got.L1())
		l3After, _ := Digest(got.L3())
		agentAfter, _ := Digest(got.NodeAgent())
		if l1Before != l1After || l3Before != l3After || agentBefore == agentAfter {
			t.Fatal("load limit did not stay local to node agent settings")
		}
	}
}
