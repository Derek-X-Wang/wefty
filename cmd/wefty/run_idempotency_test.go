package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// The gateway commits through the real L3 HTTP handler, then loses the response
// with a simulated upstream timeout. No acceptance or replay response is faked.
func runRetryLedger(t *testing.T) (*l3.Store, string, string, *atomic.Bool) {
	t.Helper()
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "ledger.sqlite"), l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server, err := l3.NewServer(participant, store, l3.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	timeout := &atomic.Bool{}
	listener, err := participant.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gateway := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && timeout.Swap(false) {
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, r)
			if response.Code == http.StatusCreated {
				http.Error(w, "simulated gateway timeout after Run commit", http.StatusGatewayTimeout)
				return
			}
			for name, values := range response.Header() {
				w.Header()[name] = values
			}
			w.WriteHeader(response.Code)
			_, _ = w.Write(response.Body.Bytes())
			return
		}
		server.Handler().ServeHTTP(w, r)
	})}
	go func() { _ = gateway.Serve(listener) }()
	t.Cleanup(func() { _ = gateway.Close() })
	// Submit's routing probe also uses the real L1 HTTP contract.
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	controlStore, err := l1.OpenStore(filepath.Join(t.TempDir(), "control.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlStore.Close() })
	controlServer, err := l1.NewServer(control, controlStore, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	controlListener, err := control.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controlHTTP := &http.Server{Handler: controlServer.Handler()}
	go func() { _ = controlHTTP.Serve(controlListener) }()
	t.Cleanup(func() { _ = controlHTTP.Close() })
	return store, listener.Addr().String(), controlListener.Addr().String(), timeout
}

func retryBinaryArgs(ledger, control string, jsonOutput bool, args ...string) []string {
	global := []string{"--fabric=plain", "--plain-identity=retry-operator", "--l3=" + ledger, "--l1=" + control}
	if jsonOutput {
		global = append(global, "--json")
	}
	return append(global, args...)
}

func readRetryAcceptance(t *testing.T, code int, output string, replayed bool) string {
	t.Helper()
	if code != 0 {
		t.Fatalf("command exited %d: %s", code, output)
	}
	var accepted struct {
		RunID  string `json:"run_id"`
		Replay *bool  `json:"idempotent_replay"`
	}
	if err := json.Unmarshal([]byte(firstJSONDocument(output)), &accepted); err != nil {
		t.Fatalf("acceptance: %v: %s", err, output)
	}
	if accepted.RunID == "" || accepted.Replay == nil || *accepted.Replay != replayed {
		t.Fatalf("want run ID and idempotent_replay=%t: %s", replayed, output)
	}
	return accepted.RunID
}

func assertRetryRunCount(t *testing.T, store *l3.Store, count int) {
	t.Helper()
	page, err := store.ListRuns(t.Context(), l3.RunListFilter{Limit: 500})
	if err != nil || len(page.Runs) != count {
		t.Fatalf("run count = %d, want %d, err=%v", len(page.Runs), count, err)
	}
}

func TestRunRetryAfterTimeoutFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, verb := range []string{"submit", "rerun"} {
		t.Run(verb, func(t *testing.T) {
			store, ledger, control, timeout := runRetryLedger(t)
			submit := []string{"submit", "--image", "example.test/program@sha256:" + strings.Repeat("a", 64), "--params", `{"nested":{"b":2,"a":1}}`}
			args := submit
			baseCount := 0
			if verb == "rerun" {
				code, output := runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, submit...)...)
				// This seed predates the behaviour under test, so don't require its marker.
				var seed l3.RunAccepted
				if code != 0 || json.Unmarshal([]byte(firstJSONDocument(output)), &seed) != nil || seed.RunID == "" {
					t.Fatalf("seed: %d %s", code, output)
				}
				args = []string{"rerun", seed.RunID}
				baseCount = 1
			}
			timeout.Store(true)
			code, output := runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, args...)...)
			if code == 0 || !strings.Contains(output, "504") {
				t.Fatalf("want simulated timeout: %d %s", code, output)
			}
			assertRetryRunCount(t, store, baseCount+1)
			if verb == "submit" {
				// A retry can reconstruct JSON with different object ordering.
				args = []string{"submit", "--image", "example.test/program@sha256:" + strings.Repeat("a", 64), "--params", ` { "nested": { "a": 1.0, "b": 2e0 } } `}
			}
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, args...)...)
			// Count first so original random keys fail at the duplicate-run assertion.
			assertRetryRunCount(t, store, baseCount+1)
			replayID := readRetryAcceptance(t, code, output, true)
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, false, args...)...)
			if code != 0 || !strings.Contains(output, "replayed") || !strings.Contains(output, replayID) {
				t.Fatalf("table replay: %d %s", code, output)
			}
			again := append(append([]string(nil), args...), "--again")
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, again...)...)
			fresh := readRetryAcceptance(t, code, output, false)
			if fresh == replayID {
				t.Fatal("--again replayed the existing run")
			}
			assertRetryRunCount(t, store, baseCount+2)
			changed := []string{"rerun", fresh}
			if verb == "submit" {
				changed = append(append([]string(nil), args...), "--params", `{"nested":{"a":1,"b":3}}`)
			}
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, changed...)...)
			if newID := readRetryAcceptance(t, code, output, false); newID == fresh || newID == replayID {
				t.Fatalf("changed request reused a run: %s", newID)
			}
			assertRetryRunCount(t, store, baseCount+3)
		})
	}
}

func TestRunAgainExplicitKeyFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, verb := range []string{"submit", "rerun"} {
		t.Run(verb, func(t *testing.T) {
			store, ledger, control, _ := runRetryLedger(t)
			args := []string{"submit", "--image", "example.test/program@sha256:" + strings.Repeat("a", 64)}
			baseCount := 0
			if verb == "rerun" {
				code, output := runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, args...)...)
				var seed l3.RunAccepted
				if code != 0 || json.Unmarshal([]byte(firstJSONDocument(output)), &seed) != nil {
					t.Fatalf("seed: %d %s", code, output)
				}
				args = []string{"rerun", seed.RunID}
				baseCount = 1
			}
			// Deliberate repeats are fresh on every invocation.
			again := append(append([]string(nil), args...), "--again")
			code, output := runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, again...)...)
			first := readRetryAcceptance(t, code, output, false)
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, again...)...)
			second := readRetryAcceptance(t, code, output, false)
			if first == second {
				t.Fatal("two --again invocations returned the same run")
			}
			explicit := append(append([]string(nil), again...), "--idempotency-key", "operator-"+verb)
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, explicit...)...)
			third := readRetryAcceptance(t, code, output, false)
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, explicit...)...)
			if replay := readRetryAcceptance(t, code, output, true); replay != third {
				t.Fatalf("explicit key lost to --again: %s != %s", replay, third)
			}
			assertRetryRunCount(t, store, baseCount+3)
			// Explicit keys still reach L3 unchanged when the request changes:
			// the server must refuse, rather than silently derive a different key.
			conflicting := []string{"rerun", third, "--again", "--idempotency-key", "operator-" + verb}
			if verb == "submit" {
				conflicting = append(append([]string(nil), explicit...), "--params", `{"changed":true}`)
			}
			code, output = runWefty(t, binary, 30*time.Second, retryBinaryArgs(ledger, control, true, conflicting...)...)
			if code == 0 || !strings.Contains(output, "idempotency_conflict") {
				t.Fatalf("explicit key with changed request: %d %s", code, output)
			}
			assertRetryRunCount(t, store, baseCount+3)
		})
	}
}

func TestSubmitCanonicalRequestKey(t *testing.T) {
	script := filepath.Join(t.TempDir(), "program.sh")
	alias := filepath.Join(t.TempDir(), "same-program.sh")
	for _, path := range []string{script, alias} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho hi\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	clients := &apiClients{l3: &apiClient{name: "L3", client: &http.Client{Transport: imageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		return jsonResponse(http.StatusCreated, l3.RunAccepted{RunID: "accepted"}), nil
	})}}}
	submit := func(args ...string) string {
		t.Helper()
		var out, stderr bytes.Buffer
		if err := executeSubmit(context.Background(), clients, true, args, &out, &stderr); err != nil {
			t.Fatalf("submit: %v %s", err, stderr.String())
		}
		return keys[len(keys)-1]
	}
	base := submit("--script", script, "--params", `{"nested":{"b":2,"a":1}}`, "--tag", "B", "--tag", "a", "--envelope-schema", `{"type":"object","properties":{"b":{"type":"string"},"a":{"type":"string"}}}`)
	equivalent := submit("--script", alias, "--params", ` { "nested": { "a": 1.0, "b": 2e0 } } `, "--tag", "a", "--tag", " b ", "--tag", "A", "--envelope-schema", `{"properties":{"a":{"type":"string"},"b":{"type":"string"}},"type":"object"}`)
	if base != equivalent {
		t.Fatalf("equivalent content/JSON/tag sets have different keys: %s != %s", base, equivalent)
	}
	paramsFile := filepath.Join(t.TempDir(), "params.json")
	schemaFile := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(paramsFile, []byte(`{"nested":{"a":1,"b":2}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(schemaFile, []byte(`{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if fromFiles := submit("--script", alias, "--params-file", paramsFile, "--tag", "a", "--tag", "b", "--envelope-schema-file", schemaFile); fromFiles != base {
		t.Fatalf("file contents derived a different key: %s != %s", fromFiles, base)
	}
	// The full typed request, including nested image fields, participates. These
	// assertions protect against accidentally hashing only a program or params.
	baseline := []string{"--image", "example.test/program:v1"}
	imageKey := submit(baseline...)
	for _, fields := range [][]string{
		{"--params", `{"x":1}`}, {"--tag", "linux"}, {"--max-runtime", "5"}, {"--max-cost", "2"},
		{"--envelope-schema", `{"type":"object"}`}, {"--required-envelope"}, {"--dispatch-authority"},
		{"--argv", "sh"}, {"--working-directory", "/workspace"}, {"--memory-bytes", "1024"},
		{"--cpu-millicores", "1000"}, {"--runtime-handler", "runc"}, {"--node", "node-a"},
		{"--mount", "/tmp:/work:ro", "--node", "node-a"},
	} {
		if changed := submit(append(append([]string(nil), baseline...), fields...)...); changed == imageKey {
			t.Fatalf("field %v did not change the key", fields)
		}
	}
	for _, source := range [][]string{{"--image", "example.test/program:v2"}, {"--image", "example.test/program:v1@sha256:" + strings.Repeat("a", 64)}, {"--workflow-ref", "workflow://program/v1"}, {"--workflow-ref", "workflow://program/v2"}} {
		if key := submit(source...); key == imageKey {
			t.Fatalf("source %v did not change the key", source)
		}
	}
	scriptKey := submit("--script", script)
	for _, fields := range [][]string{{"--mode", "0700"}, {"--interpreter", "/bin/sh"}} {
		if key := submit(append([]string{"--script", script}, fields...)...); key == scriptKey {
			t.Fatalf("script field %v did not change key", fields)
		}
	}
	if err := os.WriteFile(script, []byte("echo changed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if key := submit("--script", script); key == scriptKey {
		t.Fatal("script content did not change key")
	}
	// Adjacent integer resource limits above float64's exact range are still
	// distinct request fields; generic JSON canonicalization must not round them.
	large := submit("--image", "example.test/program:v1", "--memory-bytes", "9007199254740992")
	if adjacent := submit("--image", "example.test/program:v1", "--memory-bytes", "9007199254740993"); adjacent == large {
		t.Fatal("canonicalization rounded an image resource limit")
	}
	if key := submit("--image", "example.test/program:v1", "--again", "--idempotency-key", " user-key "); key != "user-key" {
		t.Fatalf("explicit key = %q", key)
	}
}

// Exercise derived and explicit keys through independent Fabric identities.
// On rerun, the node races ahead of the owner on the owner's source run.
func TestRunActorScopedKeysFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, explicit := range []bool{false, true} {
		for _, verb := range []string{"submit", "rerun"} {
			t.Run(fmt.Sprintf("%s/explicit=%t", verb, explicit), func(t *testing.T) {
				store, ledger, control, _ := runRetryLedger(t)
				invoke := func(person bool, replay bool, args ...string) string {
					t.Helper()
					global := []string{"--fabric=plain", "--plain-identity=other-node", "--plain-user-id=", "--plain-device-id=", "--l3=" + ledger, "--l1=" + control, "--json"}
					if person {
						global = append(global, "--plain-identity=person-device", "--plain-user-id=person-owner", "--plain-device-id=person-device")
					}
					code, output := runWefty(t, binary, 30*time.Second, append(global, args...)...)
					return readRetryAcceptance(t, code, output, replay)
				}
				args := []string{"submit", "--image", "example.test/program@sha256:" + strings.Repeat("a", 64)}
				count := 2
				if verb == "rerun" {
					source := invoke(true, false, args...)
					args = []string{"rerun", source}
					count = 3
				}
				if explicit {
					args = append(args, "--idempotency-key", "same-"+verb+"-key")
				}
				nodeRun := invoke(false, false, args...)
				ownerRun := invoke(true, false, args...)
				if ownerRun == nodeRun {
					t.Fatal("different actors shared a run")
				}
				if invoke(true, true, args...) != ownerRun || invoke(false, true, args...) != nodeRun {
					t.Fatal("actor replay changed identity")
				}
				assertRetryRunCount(t, store, count)
			})
		}
	}
}
