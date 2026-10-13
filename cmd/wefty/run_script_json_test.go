package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

// Exercise the real command parsers and output paths, rather than a projection
// helper in isolation. The API still returns the complete immutable snapshot.
func TestRunScriptJSONOutputsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, command := range []string{"wait", "wait-failed", "inspect", "cancel", "origin"} {
		for _, mode := range []string{"default", "include-before-id", "include-after-id", "explicit-false"} {
			for _, content := range []string{"#!/bin/sh\r\nprintf '脚本\\t<>&'\n\n", ""} {
				name := command + "/" + mode + "/"
				if content == "" {
					name += "empty"
				} else {
					name += "utf8-crlf"
				}
				t.Run(name, func(t *testing.T) {
					digest := sha256.Sum256([]byte(content))
					root := contract.RunRecord{
						SchemaVersion: 1, RunID: "run_script_root", Status: contract.RunSucceeded,
						DispatchKey: "dispatch-script", Trigger: contract.Trigger{Type: "computer", ComputerID: "computer-script"},
						Workflow: contract.WorkflowSource{InlineScript: &contract.InlineScript{Content: content, SHA256: hex.EncodeToString(digest[:])}},
						// Script-shaped application data must remain intact.
						Params: json.RawMessage(`{"workflow":{"inline_script":{"content":"parameter data"}},"n":9007199254740993}`),
					}
					if command == "wait-failed" || command == "cancel" {
						root.Status, root.FailureReason = contract.RunFailed, "the L1 job was canceled"
					}
					child := root
					child.RunID, child.ParentRunID = "run_script_child", root.RunID
					child.Workflow = contract.WorkflowSource{InlineScript: &contract.InlineScript{Content: content + "# child\n", SHA256: "child-digest"}}
					ref := root
					ref.RunID, ref.Workflow = "run_script_ref", contract.WorkflowSource{WorkflowRef: "workflow-script@1"}
					lineage := l3.RunLineage{Descendants: []l3.LineageEntry{{RunID: child.RunID}, {RunID: ref.RunID}}}
					address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
						var response any
						switch r.URL.Path {
						case "/v1/runs/" + root.RunID, "/v1/runs/" + root.RunID + "/cancel":
							if command == "cancel" && r.Method != http.MethodPost {
								t.Errorf("cancel method = %s", r.Method)
							}
							response = root
						case "/v1/runs/" + child.RunID:
							response = child
						case "/v1/runs/" + ref.RunID:
							response = ref
						case "/v1/runs/" + root.RunID + "/lineage":
							response = lineage
						case "/v1/runs":
							if r.URL.Query().Get("origin") != "computer:computer-script" {
								t.Errorf("origin query = %s", r.URL.RawQuery)
							}
							response = l3.ComputerRunPage{Runs: []contract.RunRecord{root, child, ref}, NextCursor: "script-next"}
						default:
							t.Errorf("unexpected request: %s %s", r.Method, r.URL)
							http.NotFound(w, r)
							return
						}
						_ = json.NewEncoder(w).Encode(response)
					})
					verb := command
					if command == "wait-failed" {
						verb = "wait"
					}
					args := []string{verb}
					if command == "origin" {
						args = []string{"runs", "list", "--origin=computer:computer-script", "--include-descendants"}
					} else {
						args = append(args, root.RunID)
					}
					include := strings.HasPrefix(mode, "include")
					switch mode {
					case "include-before-id":
						args = append(args[:1], append([]string{"--include-script"}, args[1:]...)...)
						if command == "origin" {
							args = []string{"runs", "list", "--include-script", "--origin=computer:computer-script", "--include-descendants"}
						}
					case "include-after-id":
						args = append(args, "--include-script")
					case "explicit-false":
						args = append(args, "--include-script=false")
					}
					args = append([]string{"--json", "--l3=" + address}, args...)
					code, stdout, stderr := runPolishBinary(t, binary, args...)
					wantExit := 0
					if command == "wait-failed" {
						wantExit = exitRunFailed
					}
					if code != wantExit || len(stderr) != 0 {
						t.Fatalf("exit=%d stdout=%s stderr=%s", code, stdout, stderr)
					}
					var document map[string]json.RawMessage
					if err := json.Unmarshal(stdout, &document); err != nil {
						t.Fatal(err)
					}
					switch command {
					case "wait", "wait-failed", "cancel":
						assertScriptRunJSON(t, stdout, root, include)
					case "inspect", "origin":
						if command == "inspect" {
							assertScriptRunJSON(t, document["run"], root, include)
							var gotLineage l3.RunLineage
							if err := json.Unmarshal(document["lineage"], &gotLineage); err != nil || !reflect.DeepEqual(gotLineage, lineage) {
								t.Fatalf("lineage changed: %s (%v)", document["lineage"], err)
							}
						} else if string(document["next_cursor"]) != `"script-next"` {
							t.Fatalf("lost cursor: %s", stdout)
						}
						var records []json.RawMessage
						if err := json.Unmarshal(document["runs"], &records); err != nil || len(records) != 3 {
							t.Fatalf("records: %s (%v)", stdout, err)
						}
						for i, want := range []contract.RunRecord{root, child, ref} {
							assertScriptRunJSON(t, records[i], want, include)
						}
					}
				})
			}
		}
	}
}

func assertScriptRunJSON(t *testing.T, data []byte, want contract.RunRecord, include bool) {
	t.Helper()
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	var workflow map[string]json.RawMessage
	if err := json.Unmarshal(got["workflow"], &workflow); err != nil {
		t.Fatal(err)
	}
	if want.Workflow.InlineScript != nil {
		var script map[string]json.RawMessage
		if err := json.Unmarshal(workflow["inline_script"], &script); err != nil {
			t.Fatal(err)
		}
		var digest string
		if err := json.Unmarshal(script["sha256"], &digest); err != nil || digest != want.Workflow.InlineScript.SHA256 {
			t.Fatalf("digest = %s (%v)", script["sha256"], err)
		}
		if include {
			var content string
			if err := json.Unmarshal(script["content"], &content); err != nil || content != want.Workflow.InlineScript.Content {
				t.Fatalf("content changed: %s (%v)", script["content"], err)
			}
			if len(script) != 2 {
				t.Fatalf("opt-in changed original script shape: %s", workflow["inline_script"])
			}
		} else {
			if _, present := script["content"]; present {
				t.Error("default output includes script content")
			}
			var size int
			if err := json.Unmarshal(script["size_bytes"], &size); err != nil || size != len(want.Workflow.InlineScript.Content) {
				t.Errorf("size_bytes = %s, want %d (%v)", script["size_bytes"], len(want.Workflow.InlineScript.Content), err)
			}
			if string(script["content_omitted"]) != "true" || len(script) != 3 {
				t.Errorf("omission shape = %s", workflow["inline_script"])
			}
		}
	}
	// Compare every other field without converting numbers through float64.
	var decoded contract.RunRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !include && want.Workflow.InlineScript != nil {
		copy := *want.Workflow.InlineScript
		copy.Content = ""
		want.Workflow.InlineScript = &copy
	}
	gotJSON, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("other run fields changed: got=%+v want=%+v", decoded, want)
	}
}
