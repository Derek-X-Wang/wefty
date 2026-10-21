package fabricconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestSavedServiceUsesRecordedCoordinator(t *testing.T) {
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(state, "saved"), []byte("opaque"), 0600); err != nil {
		t.Fatal(err)
	}
	p := connprofile.Profile{Version: connprofile.Version, Fabric: "plain", Participants: map[string]connprofile.Participant{"client": {Name: "wefty://client/machine", StateDirectory: state, NodeID: "opaque", CoordinatorURL: "https://coordinator.example.test"}}}
	for _, url := range []string{"", "https://coordinator.example.test", "https://different.example.test"} {
		got, err := SavedService(p, "client", Config{Mode: "plain", ControlURL: url}, nil)
		if url == "https://different.example.test" {
			if err == nil || !strings.Contains(err.Error(), "recorded coordinator URL") || !strings.Contains(err.Error(), "unset") || strings.Contains(err.Error(), "retire") {
				t.Fatalf("wrong coordinator recovery advice: %v", err)
			}
		} else if err != nil || got.ControlURL != "https://coordinator.example.test" {
			t.Fatalf("recorded coordinator lost: %q %v", got.ControlURL, err)
		}
	}

	t.Setenv("TS_CONTROL_URL", "https://coordinator.example.test")
	if _, err := SavedService(p, "client", Config{Mode: "plain"}, nil); err != nil {
		t.Fatalf("matching environment refused: %v", err)
	}
	t.Setenv("TS_CONTROL_URL", "https://other.example.test")
	if _, err := SavedService(p, "client", Config{Mode: "plain"}, nil); err == nil || !strings.Contains(err.Error(), "recorded coordinator URL") || !strings.Contains(err.Error(), "unset") || strings.Contains(err.Error(), "retire") {
		t.Fatalf("wrong environment recovery advice: %v", err)
	}
	t.Setenv("TS_CONTROL_URL", "")
	payload, err := connprofile.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "profile.json")
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	restored, err := connprofile.Read(path)
	if err != nil || restored.Participants["client"].CoordinatorURL != "https://coordinator.example.test" {
		t.Fatalf("coordinator roundtrip: %+v %v", restored, err)
	}
	for _, url := range []string{"https://user:password@example.test", "https://example.test?key=secret", "http://example.test", "https://example.test/#secret"} {
		part := p.Participants["client"]
		part.CoordinatorURL = url
		p.Participants["client"] = part
		if _, err := connprofile.Encode(p); err == nil {
			t.Errorf("unsafe coordinator accepted: %s", url)
		}
	}
}
