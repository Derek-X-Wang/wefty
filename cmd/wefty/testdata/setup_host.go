package main

// This source is used only by the setup binary test's Go build overlay.
// It is not part of any shipped package or production build.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet/teststate"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

type fixtureEnroller struct{}

func (fixtureEnroller) Enrol(_ context.Context, s fabric.EnrolSpec) (fabric.SelfIdentity, error) {
	role := filepath.Base(s.StateDirectory)
	if role == "person" {
		if s.Credential.Value != "" || s.LoginURL == nil {
			return fabric.SelfIdentity{}, errors.New("fixture person must use browser sign-in")
		}
		s.LoginURL(os.Getenv("WEFTY_FIXTURE_LOGIN_URL"))
	} else if !strings.HasPrefix(s.Credential.Value, "single-use-") {
		return fabric.SelfIdentity{}, errors.New("fixture key refused")
	}
	if err := teststate.Write(s.StateDirectory); err != nil {
		return fabric.SelfIdentity{}, err
	}
	name, err := fabric.RequestedNetworkName(s.Name)
	if err != nil {
		return fabric.SelfIdentity{}, err
	}
	identity := fabric.Identity{NodeID: "opaque-" + role, Tags: connprofile.ParticipantRoleTags(role), Kind: fabric.IdentityKindMachine}
	if role == "person" {
		identity.Kind = ""
		identity.UserID = "opaque-owner"
	}
	return fabric.SelfIdentity{Identity: identity, Name: s.Name, NetworkName: name}, nil
}
func (fixtureEnroller) Logout(context.Context, fabric.EnrolSpec) error { return nil }

type fixtureTransport func(*http.Request) (*http.Response, error)

func (f fixtureTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func setupCommandHost() (setupHost, error) {
	home := os.Getenv("HOME")
	if home == "" {
		return setupHost{}, errors.New("fixture needs a temporary home")
	}
	exe, err := os.Executable()
	if err != nil {
		return setupHost{}, err
	}
	loaded := map[string]bool{}
	stateFile := filepath.Join(home, "fixture-launchd.json")
	if b, err := os.ReadFile(stateFile); err == nil {
		_ = json.Unmarshal(b, &loaded)
	}
	run := func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != launchagent.Launchctl {
			return nil, errors.New("unexpected fixture runner")
		}
		label := filepath.Base(args[len(args)-1])
		label = strings.TrimSuffix(label, ".plist")
		switch args[0] {
		case "print":
			if !loaded[label] {
				return nil, launchagent.ErrServiceNotFound
			}
			return []byte("service = {\n state = running\n pid = 4242\n}\n"), nil
		case "enable":
			return nil, nil
		case "bootstrap":
			loaded[label] = true
			{
				b := []byte(`{"ready_at":"2026-10-20T12:00:00Z","pid":4242,"address":"opaque-listener","node_id":"studio-0123abcd","boot_session_id":"fixture-boot"}`)
				if err := os.WriteFile(filepath.Join(home, setupFolders.data, "setup-ready", strings.TrimPrefix(label, "dev.wefty.")+".json"), b, 0600); err != nil {
					return nil, err
				}
			}
		case "bootout":
			delete(loaded, label)
		case "kickstart":
			loaded[label] = true
		default:
			return nil, errors.New("unexpected fixture verb")
		}
		b, _ := json.Marshal(loaded)
		return nil, os.WriteFile(stateFile, b, 0600)
	}
	clients := func(_ context.Context, _ setupPaths, p connprofile.Profile) (*apiClients, error) {
		if os.Getenv("WEFTY_FIXTURE_IDENTITY_REFUSED") == "1" {
			return nil, &fabric.ParticipantIdentityError{Reason: "does not match the recorded node ID"}
		}
		if os.Getenv("WEFTY_FIXTURE_NOT_READY") == "1" {
			return nil, errors.New("fixture service unavailable")
		}
		rt := fixtureTransport(func(r *http.Request) (*http.Response, error) {
			var body any
			status := 200
			switch {
			case r.URL.Path == "/v1/jobs":
				body = l1.JobList{}
			case r.URL.Path == "/v1/peer-health":
				observation := contract.PeerHealth{Reachable: true, CheckedAt: time.Now().UTC()}
				if os.Getenv("WEFTY_FIXTURE_PEER_NOT_READY") == "1" {
					observation.Reachable = false
					observation.ErrorClass = "unavailable"
				}
				body = observation
				if os.Getenv("WEFTY_FIXTURE_PEER_UNAVAILABLE") == "1" {
					status = 404
					body = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "route not found"}}
				}
			case r.URL.Path == "/v1/nodes":
				body = l1.NodeList{Nodes: []l1.Node{{NodeRegistration: contract.NodeRegistration{NodeID: p.Identities.Node, BootSessionID: "fixture-boot"}, State: contract.NodeAlive, ClaimsEnabled: os.Getenv("WEFTY_FIXTURE_NOT_ADMITTED") != "1", AutomaticallyAdmitted: len(p.NodeAdmission) == 0, AuthoritativeTags: []string{contract.StableNodeTagPrefix + p.Identities.Node}, MaxOneshotSlots: 4, MaxServiceSlots: 2}}}
			case r.URL.Path == "/v1/runs" && r.Method == "GET":
				body = map[string]any{"runs": []any{}}
			case r.URL.Path == "/v1/runs" && r.Method == "POST":
				body, status = map[string]string{"run_id": "fixture-smoke"}, 201
			case r.URL.Path == "/v1/runs/fixture-smoke":
				body = contract.RunRecord{RunID: "fixture-smoke", Status: contract.RunSucceeded, NodeID: p.Identities.Node}
			default:
				return nil, fmt.Errorf("unexpected fixture request: %s", r.URL.Path)
			}
			b, _ := json.Marshal(body)
			return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
		})
		return &apiClients{l1: &apiClient{name: "L1", address: p.L1Address, client: &http.Client{Transport: rt}}, l3: &apiClient{name: "L3", address: p.L3Address, client: &http.Client{Transport: rt}}, wait: waitForContext}, nil
	}
	node := l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "studio-0123abcd"}, ClaimsEnabled: true}
	drainFile := filepath.Join(home, "fixture-drain.json")
	if payload, err := os.ReadFile(drainFile); err == nil {
		_ = json.Unmarshal(payload, &node)
	}
	switch os.Getenv("WEFTY_FIXTURE_BUSY") {
	case "one-shot":
		node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "fixture-busy-one-shot", AttemptID: "fixture-busy-attempt", Class: contract.JobClassOneShot}}
	case "service":
		node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "fixture-busy-service", AttemptID: "fixture-busy-attempt", Class: contract.JobClassService}}
	default:
		node.ActiveAttempts = nil
	}
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	observe := func(context.Context, connprofile.Profile) (l1.Node, error) { return node, nil }
	drain := &upgradeHost{observe: observe, readNode: observe, now: func() time.Time { return now }, wait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }, setClaims: func(_ context.Context, _ connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
		if request.IntentRevision != node.IntentRevision {
			return l1.Node{}, errors.New("fixture claim conflict")
		}
		node.IntentRevision++
		node.IntentReason = request.Reason
		node.ClaimsEnabled = request.ClaimsEnabled
		payload, err := json.Marshal(node)
		if err != nil {
			return l1.Node{}, err
		}
		return node, os.WriteFile(drainFile, payload, 0600)
	}}
	return setupHost{doctorCommand: func(_ context.Context, name string, args ...string) ([]byte, error) {
		if filepath.Base(name) == "pmset" && strings.Join(args, " ") == "-g custom" {
			return []byte("AC Power:\n sleep 0\n"), nil
		}
		if filepath.Base(name) == "fdesetup" && strings.Join(args, " ") == "status" {
			if os.Getenv("WEFTY_FIXTURE_FILEVAULT_ON") == "1" {
				return []byte("FileVault is On.\n"), nil
			}
			return []byte("FileVault is Off.\n"), nil
		}
		return nil, errors.New("unexpected fixture host probe")
	}, conversionFilesClosed: func(context.Context, []string) error { return nil }, conversionClients: func(ctx context.Context, p connprofile.Profile) (*apiClients, error) {
		return clients(ctx, setupPaths{}, p)
	}, upgrade: drain, conversionComputers: func(context.Context, connprofile.Profile) ([]l1.Computer, error) {
		if os.Getenv("WEFTY_FIXTURE_BUSY") == "computer" {
			return []l1.Computer{{ComputerID: "fixture-busy-computer", BoundNodeID: node.NodeID}}, nil
		}
		return nil, nil
	}, ownerLogin: func(context.Context, string) error { return nil }, peerLookup: func(_ context.Context, c fabricconfig.Config) (fabric.PeerLookup, fabricconfig.CloseFunc, error) {
		return fixturePeers{}, func() error { return nil }, nil
	}, bootTime: func() (time.Time, error) { return time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), nil }, goos: "darwin", euid: 501, uid: 501, home: home, username: "owner", hostname: "studio", shell: "/bin/sh", executable: exe, launchctl: run, loginShell: func(context.Context, string, ...string) ([]byte, error) {
		loginPATH := os.Getenv("WEFTY_FIXTURE_LOGIN_PATH")
		if loginPATH == "" {
			loginPATH = "/usr/bin:/bin"
		}
		return []byte(setupPATHBegin + "\n" + loginPATH + "\n" + setupPATHEnd + "\n"), nil
	}, random: strings.NewReader(strings.Repeat("a", 512)), enroller: fixtureEnroller{}, enrolmentInput: os.Stdin, fleetClients: clients, readinessBudget: 30 * time.Millisecond}, nil
}

type fixturePeers struct{}

func (fixturePeers) LookupPeer(_ context.Context, id string) (fabric.Identity, error) {
	if os.Getenv("WEFTY_FIXTURE_BAD_PEERS") == "1" {
		return fabric.Identity{NodeID: id, Tags: []string{"tag:wefty-agent"}}, nil
	}
	switch id {
	case "opaque-l1":
		return fabric.Identity{NodeID: id, Tags: []string{"tag:wefty-control"}}, nil
	case "opaque-l3":
		return fabric.Identity{NodeID: id, Tags: []string{"tag:wefty-client"}}, nil
	}
	return fabric.Identity{}, fabric.ErrIdentityNotFound
}
