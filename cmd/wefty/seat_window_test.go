package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

func TestSeatWindowVisibleAndCloseOnly(t *testing.T) {
	var closed atomic.Bool
	expires := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	node := readyNode("mac")
	node.AuthoritativeTags = []string{contract.StableNodeTagPrefix + "mac"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/seat-approval-windows":
			windows := []l1.SeatApprovalWindow{}
			if !closed.Load() {
				windows = append(windows, l1.SeatApprovalWindow{NodeID: "mac", Network: "internet", ExpiresAt: expires})
			}
			json.NewEncoder(w).Encode(windows)
		case "/v1/seat-approval-windows/close":
			var request struct {
				NodeID  string `json:"node_id"`
				Network string `json:"network"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			if r.Method != "POST" || request.NodeID != "mac" || request.Network != "" {
				t.Errorf("close request=%+v method=%s", request, r.Method)
			}
			closed.Store(true)
			json.NewEncoder(w).Encode(map[string]bool{"closed": true})
		case "/v1/nodes/mac":
			json.NewEncoder(w).Encode(node)
		case "/v1/nodes":
			json.NewEncoder(w).Encode(l1.NodeList{Nodes: []l1.Node{node}})
		case "/v1/runs":
			json.NewEncoder(w).Encode(map[string]any{"runs": []any{}})
		case "/v1/whoami":
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	clients := &apiClients{l1: statusClient(t, "L1", "test", server), l3: statusClient(t, "L3", "test", server), nodeConfigPath: filepath.Join(t.TempDir(), "node.json")}
	if err := ocicontrol.WriteInstalledConfig(clients.nodeConfigPath, ocicontrol.InstalledConfig{Version: 1, NodeID: "mac"}); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if err := execute(t.Context(), clients, false, []string{"status"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "approval window") || !strings.Contains(out.String(), "internet") || !strings.Contains(out.String(), "2026-10-23T09:00:00Z") {
		t.Fatalf("status omitted window: %s", out.String())
	}
	out.Reset()
	if err := execute(t.Context(), clients, true, []string{"seat", "window", "close"}, &out, &errOut); err != nil {
		t.Fatal(err)
	}
	if !closed.Load() || !strings.Contains(out.String(), `"closed": true`) {
		t.Fatalf("close: closed=%v output=%s", closed.Load(), out.String())
	}
}

// AP-4 checks the compiler principal invariant and adds a source inventory
// of CLI dispatch, outbound calls and every node control-socket route. Adding a
// control verb requires updating the reviewed inventory. This is paired with
// L1 protocol tests that reject answers from CLI identities.
func TestSeatApprovalNoCLIOrControlSocketAnswer(t *testing.T) {
	TestSeatApprovalCLIPrincipalStructure(t)
	if !strings.Contains(rootUsage, "seat window close") {
		t.Fatal("the only approval mutation command must be window close")
	}
	var commands, flags int
	unseenControlVerbs := make(map[string]bool)
	for key := range seatReviewedControlVerbs {
		unseenControlVerbs[key] = true
	}
	walk := func(root string, control bool) {
		t.Helper()
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(n ast.Node) bool {
				if clause, ok := n.(*ast.CaseClause); ok && !control {
					for _, expression := range clause.List {
						if literal, ok := expression.(*ast.BasicLit); ok && literal.Kind == token.STRING {
							commands++
						}
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if !control && (selector.Sel.Name == "AnswerSeatApproval" || selector.Sel.Name == "BeginSeatApproval") {
					t.Errorf("CLI uses node approval protocol in %s", path)
				}
				if len(call.Args) == 0 {
					return true
				}
				argIndex := 0
				if !control && strings.HasSuffix(selector.Sel.Name, "Var") && len(call.Args) > 1 {
					argIndex = 1
				}
				literal, ok := call.Args[argIndex].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, _ := strconv.Unquote(literal.Value)
				if !control && (strings.HasSuffix(selector.Sel.Name, "Var") || selector.Sel.Name == "Bool" || selector.Sel.Name == "String") {
					flags++
				}
				if control && selector.Sel.Name == "HandleFunc" {
					if !seatReviewedControlVerbs[value] {
						t.Errorf("unreviewed control-socket verb %s in %s", value, path)
					}
					delete(unseenControlVerbs, value)
				}
				return true
			})
			if !control {
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if bytes.Contains(data, []byte("/v1/agent/seat-approvals")) {
					t.Errorf("CLI can send a node approval in %s", path)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	walk(".", false)
	walk("../../runner/ocicontrol", true)
	if commands < 100 || flags < 100 {
		t.Fatalf("incomplete CLI inventory: commands=%d flags=%d", commands, flags)
	}
	if len(unseenControlVerbs) != 0 {
		t.Fatalf("missing control routes: %v", unseenControlVerbs)
	}
	t.Logf("reviewed %d CLI switch arms, %d flags, and %d control-socket verbs", commands, flags, len(seatReviewedControlVerbs))
	for _, command := range []string{"seat", "seat window", "seat window close"} {
		var out, errOut bytes.Buffer
		if err := run(t.Context(), append(strings.Fields(command), "--help"), &out, &errOut); err != nil || !strings.Contains(out.String(), "seat window close") {
			t.Fatalf("help %s: err=%v output=%s", command, err, out.String())
		}
	}
}

var seatReviewedControlVerbs = map[string]bool{"GET /v1/doctor": true, "GET /v1/oci/removals": true, "GET /v1/intent": true, "POST /v1/setup": true, "POST /v1/oci/start": true, "POST /v1/oci/stop": true, "POST /v1/images/load": true}

func TestSeatWindowCommandsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, command := range []string{"seat", "seat window", "seat window close"} {
		code, stdout, stderr := runPolishBinary(t, binary, append(strings.Fields(command), "--help")...)
		if code != 0 || len(stderr) != 0 || !strings.Contains(string(stdout), "seat window close") {
			t.Fatalf("%s: exit=%d stdout=%s stderr=%s", command, code, stdout, stderr)
		}
	}
	for _, command := range []string{"seat approve", "seat window open", "seat window close --approve", "seat window close --answer=yes"} {
		code, stdout, stderr := runPolishBinary(t, binary, append([]string{"--json", "--fabric=plain", "--l1=127.0.0.1:1"}, strings.Fields(command)...)...)
		if code != exitUsage || len(stdout) != 0 || !bytes.Contains(stderr, []byte("invalid_request")) {
			t.Fatalf("%s: exit=%d stdout=%s stderr=%s", command, code, stdout, stderr)
		}
	}
}
