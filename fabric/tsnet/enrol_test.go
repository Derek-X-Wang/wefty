package tsnet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
)

func TestSavedParticipantCannotUseAmbientJoinKey(t *testing.T) {
	for _, name := range []string{"TS_AUTHKEY", "TS_AUTH_KEY", "TS_CLIENT_SECRET", "TS_ID_TOKEN", "TSNET_FORCE_LOGIN", "TS_CLIENT_ID", "TS_AUDIENCE"} {
		t.Setenv(name, "test-ambient-credential")
	}
	f, err := New(Config{Name: "wefty://node/machine-1", StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"TS_AUTHKEY", "TS_AUTH_KEY", "TS_CLIENT_SECRET", "TS_ID_TOKEN", "TSNET_FORCE_LOGIN", "TS_CLIENT_ID", "TS_AUDIENCE"} {
		if os.Getenv(name) != "" {
			t.Errorf("%s remains available to tsnet or later child processes", name)
		}
	}
	if f.server.AuthKey != "" {
		t.Fatal("saved participant received a join key")
	}
}

type fakeEnrolNode struct {
	client *local.Client
	closed bool
}

func (n *fakeEnrolNode) LocalClient() (*local.Client, error) { return n.client, nil }
func (n *fakeEnrolNode) Close() error                        { n.closed = true; return nil }

func TestEnrolWaitsForRunningAndTranslatesSelfIdentity(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(map[bool]string{false: "machine", true: "person"}[person], func(t *testing.T) {
			spec := fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "one-use-key"}}
			status := `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-device","HostName":"control-plane","DNSName":"wefty-control-plane.identity-domain.","TailscaleIPs":["100.64.0.1"],"Tags":["tag:wefty-control"]}}`
			var loginURL string
			if person {
				spec.Name, spec.Credential = "wefty://person/runner-1", fabric.Credential{}
				spec.LoginURL = func(url string) { loginURL = url }
				status = `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-person","DNSName":"wefty-runner-1-person.identity-domain.","TailscaleIPs":["100.64.0.2"],"UserID":42,"KeyExpiry":"2027-01-01T00:00:00Z"}}`
			}
			starting, running := ipn.Starting, ipn.Running
			url := "https://login.invalid/one-use"
			notices := []ipn.Notify{{State: &starting, BrowseToURL: &url}, {State: &running}}
			var stream strings.Builder
			for _, notice := range notices {
				payload, _ := json.Marshal(notice)
				stream.Write(payload)
				stream.WriteByte('\n')
			}
			node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := status
				if r.URL.Path == "/localapi/v0/watch-ipn-bus" {
					body = stream.String()
				} else if r.URL.Path != "/localapi/v0/status" {
					t.Fatalf("unexpected API %s", r.URL.Path)
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			enroller := Enroller{newNode: func(config Config) (enrolNode, error) {
				if config.Credential.Value != spec.Credential.Value || config.StateDir != spec.StateDirectory || config.Ephemeral {
					t.Fatal("enrolment changed credential, folder, or lifetime")
				}
				return node, nil
			}}
			self, err := enroller.Enrol(t.Context(), spec)
			if err != nil {
				t.Fatal(err)
			}
			if !node.closed || self.Name != spec.Name || self.NodeID == "" || self.DeviceID != self.NodeID {
				t.Fatalf("self=%+v closed=%v", self, node.closed)
			}
			if person {
				if self.UserID != "42" || self.Kind == fabric.IdentityKindMachine || len(self.Tags) != 0 || self.KeyExpiry != time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) || loginURL != url {
					t.Fatalf("person=%+v login callback=%t", self, loginURL != "")
				}
			} else if self.Kind != fabric.IdentityKindMachine || self.UserID != "" || len(self.Tags) != 1 {
				t.Fatalf("machine=%+v", self)
			}
		})
	}
}

func TestEnrolRejectsGrantedNameCollisionAndScrubsKeyErrors(t *testing.T) {
	for _, collision := range []bool{true, false} {
		key := "single-use-secret"
		node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
			body := `{"State":6}`
			if !collision {
				return nil, errors.New("enrol failed for " + key)
			}
			if r.URL.Path == "/localapi/v0/status" {
				body = `{"BackendState":"Running","Self":{"ID":"opaque","HostName":"control-plane","DNSName":"control-plane-1.identity-domain.","TailscaleIPs":["100.64.0.1"]}}`
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		enroller := Enroller{newNode: func(Config) (enrolNode, error) { return node, nil }}
		_, err := enroller.Enrol(context.Background(), fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: key}})
		if err == nil || strings.Contains(err.Error(), key) || !node.closed {
			t.Fatalf("failed enrolment error=%v closed=%v", err, node.closed)
		}
		if collision && !errors.Is(err, fabric.ErrGrantedNameMismatch) {
			t.Fatalf("collision = %v", err)
		}
	}
}

func TestEnrolReadableNetworkName(t *testing.T) {
	for _, tc := range []struct {
		grant string
		ok    bool
	}{
		{"wefty-studio", true}, {"wefty-studio-1", true}, {"wefty-studio-12", true},
		{"studio", false}, {"wefty-other", false}, {"wefty-studio-0", false},
		{"wefty-studio-01", false}, {"wefty-studio-x", false}, {"wefty-studio-1-extra", false},
		{"wefty-studio-", false}, {"wefty-studio-+1", false},
		{"wefty-studio-" + strings.Repeat("1", 51), false},
	} {
		t.Run(tc.grant, func(t *testing.T) {
			node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := `{"State":6}`
				if r.URL.Path == "/localapi/v0/status" {
					body = `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-device","HostName":"wefty-studio","DNSName":"` + tc.grant + `.identity-domain.","TailscaleIPs":["100.64.0.1"],"Tags":["tag:wefty-agent"]}}`
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			enroller := Enroller{newNode: func(config Config) (enrolNode, error) {
				if config.NetworkName != "wefty-studio" {
					t.Errorf("requested name=%q", config.NetworkName)
				}
				return node, nil
			}}
			self, err := enroller.Enrol(t.Context(), fabric.EnrolSpec{Name: "wefty://node/studio-0123abcd", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "fake-key"}})
			if !node.closed {
				t.Fatal("backend remained open")
			}
			if tc.ok {
				if err != nil || self.NetworkName != tc.grant || self.Name != "wefty://node/studio-0123abcd" {
					t.Fatalf("self=%+v err=%v", self, err)
				}
			} else if !errors.Is(err, fabric.ErrGrantedNameMismatch) {
				t.Fatalf("unexpected grant accepted: %+v err=%v", self, err)
			}
		})
	}
}
