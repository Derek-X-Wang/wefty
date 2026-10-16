package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Inject at Fabric.Dial, below the real HTTP transport, command dispatcher,
// and the exit/stderr decision consumed by main. No live sandbox is required.
func TestNetworkSandboxCommandReport(t *testing.T) {
	for _, scenario := range []string{"nodes", "runs", "exec-empty", "exec-marked", "exec-markerless"} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", scenario, jsonOutput), func(t *testing.T) {
				root := t.TempDir()
				t.Setenv("WEFTY_EXEC_STORAGE_ROOT", root)
				address := "127.0.0.1:47101"
				args := []string{"nodes", "list"}
				if scenario == "runs" {
					args, address = []string{"runs", "list"}, "127.0.0.1:47102"
				}
				storage := filepath.Join(root, "wefty-exec-retained")
				if strings.HasPrefix(scenario, "exec-") {
					args = []string{"exec", "--", "go", "version"}
					if scenario != "exec-empty" {
						writeExecFile(t, filepath.Join(storage, "snapshot.git", "object"), "retained")
						address = "127.0.0.1:47102"
						if scenario == "exec-marked" {
							writeExecFile(t, filepath.Join(storage, "run-id"), "run-retained\n")
						} else {
							ageExecCapture(t, storage)
						}
					}
				}
				dials := 0
				participant := exchangeTestFabric{dial: func(_ context.Context, network, target string) (net.Conn, error) {
					dials++
					if target != address {
						t.Errorf("dialed %s; want %s", target, address)
					}
					return nil, &net.OpError{Op: "dial", Net: network, Err: &os.SyscallError{Syscall: "connect", Err: syscall.EPERM}}
				}}
				clients, err := newAPIClients(participant, "127.0.0.1:47101", "127.0.0.1:47102")
				if err != nil {
					t.Fatal(err)
				}
				defer clients.close()
				clients.nodeConfigPath = filepath.Join(t.TempDir(), "node.json")
				writeExecFile(t, clients.nodeConfigPath, `{"version":1,"node_id":"node-cli","control_socket":"/tmp/unused.sock"}`)
				var stdout, stderr bytes.Buffer
				err = execute(t.Context(), clients, jsonOutput, args, &stdout, &stderr)
				writeCommandError(&stderr, err, jsonOutput)
				wantMessage := fmt.Sprintf("the caller's sandbox blocks local network connections to wefty at %s; a sandboxed AI agent must allow these connections (see the wefty skill: skills/wefty/SKILL.md)", address)
				want := "wefty: " + wantMessage + "\n"
				if jsonOutput {
					want = fmt.Sprintf("{\n  \"error\": {\n    \"code\": \"unavailable\",\n    \"message\": %q,\n    \"retryable\": true\n  }\n}\n", wantMessage)
					var response contract.ErrorResponse
					if decode := json.Unmarshal(stderr.Bytes(), &response); decode != nil || response.Error.Message != wantMessage {
						t.Errorf("JSON error=%v, stderr=%q", decode, stderr.String())
					}
				}
				if commandExitCode(err) != 13 || stderr.String() != want || (!jsonOutput && strings.Count(stderr.String(), "\n") != 1) || stdout.Len() != 0 || dials != 1 {
					t.Errorf("exit=%d dials=%d stdout=%q stderr=%q; want exit=13, one dial, empty stdout and stderr=%q", commandExitCode(err), dials, stdout.String(), stderr.String(), want)
				}
				if !errors.Is(err, syscall.EPERM) {
					t.Errorf("dial cause lost: %v", err)
				}
				if scenario == "exec-marked" || scenario == "exec-markerless" {
					if _, err := os.Stat(filepath.Join(storage, "snapshot.git", "object")); err != nil {
						t.Errorf("unverifiable capture was removed: %v", err)
					}
				}
			})
		}
	}
}

func TestNetworkRefusedKeepsAddressHint(t *testing.T) {
	for _, service := range []string{"l1", "l3"} {
		t.Run(service, func(t *testing.T) {
			participant := exchangeTestFabric{dial: func(_ context.Context, network, _ string) (net.Conn, error) {
				return nil, &net.OpError{Op: "dial", Net: network, Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}
			}}
			clients, err := newAPIClients(participant, "127.0.0.1:47101", "127.0.0.1:47102")
			if err != nil {
				t.Fatal(err)
			}
			defer clients.close()
			args := []string{"nodes", "list"}
			if service == "l3" {
				args = []string{"runs", "list"}
			}
			var stdout, stderr bytes.Buffer
			err = execute(t.Context(), clients, false, args, &stdout, &stderr)
			writeCommandError(&stderr, err, false)
			wantHint := "is it running? pass --" + service + " or set WEFTY_" + strings.ToUpper(service) + "_ADDR"
			if commandExitCode(err) != 13 || !strings.Contains(stderr.String(), wantHint) || strings.Contains(stderr.String(), "sandbox") || strings.Count(stderr.String(), "\n") != 1 || !errors.Is(err, syscall.ECONNREFUSED) {
				t.Fatalf("exit=%d stderr=%q; want refused connection hint %q", commandExitCode(err), stderr.String(), wantHint)
			}
		})
	}
}

// Status promises a full readiness document and exit 12, rather than a single
// transport error. Its independent verdict advice must still name the sandbox.
func TestNetworkSandboxStatusKeepsReadinessContract(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", jsonOutput), func(t *testing.T) {
			participant := exchangeTestFabric{dial: func(context.Context, string, string) (net.Conn, error) {
				return nil, syscall.EPERM
			}}
			clients, err := newAPIClients(participant, "127.0.0.1:47101", "127.0.0.1:47102")
			if err != nil {
				t.Fatal(err)
			}
			defer clients.close()
			var stdout, stderr bytes.Buffer
			err = execute(t.Context(), clients, jsonOutput, []string{"status"}, &stdout, &stderr)
			writeCommandError(&stderr, err, jsonOutput)
			if commandExitCode(err) != 12 || strings.Contains(stdout.String()+stderr.String(), "is it running?") || !strings.Contains(stdout.String(), "sandbox blocks local network connections") {
				t.Fatalf("exit=%d stdout=%q stderr=%q", commandExitCode(err), stdout.String(), stderr.String())
			}
			if jsonOutput {
				var status clusterStatus
				if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
					t.Fatal(err)
				}
				if status.Ready || len(status.Services) != 2 || len(status.Reasons) != 2 || status.Reasons[0].Code != "l1_unreachable" || status.Reasons[1].Code != "l3_unreachable" || stderr.Len() != 0 {
					t.Fatalf("readiness contract changed: status=%+v stderr=%q", status, stderr.String())
				}
			}
		})
	}
}
