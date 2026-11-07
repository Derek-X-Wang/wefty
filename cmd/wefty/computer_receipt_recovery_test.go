package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerCommandsResendKeepingReceipt(t *testing.T) {
	cas := []string{"--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1", "--idempotency-key=receipt-key"}
	for _, test := range []struct {
		name string
		args []string
		path string
	}{
		{"backup", append([]string{"services", "backup", "create", "computer-1"}, cas...), "/v1/computers/computer-1/backups"},
		{"restore", append([]string{"services", "restore", "computer-1", "backup-1", "--retire-old"}, cas...), "/v1/computers/computer-1/backups/backup-1/restore"},
		{"clone", append([]string{"services", "clone", "computer-1", "backup-1", "--name=clone", "--disk-bytes=1000"}, cas...), "/v1/computers/computer-1/backups/backup-1/clone"},
		{"custody attest", []string{"services", "custody", "attest", "export-1", "--idempotency-key=receipt-key"}, "/v1/custody-exports/export-1/attest-deleted"},
		{"grow", append([]string{"services", "resize", "computer-1", "--disk-bytes=1000"}, cas...), "/v1/computers/computer-1/grow"},
		{"reimage", append([]string{"services", "reimage", "computer-1", "--image=ghcr.io/example/reimaged@" + computerCLITestDigestB}, cas...), "/v1/computers/computer-1/reimage"},
		{"create", []string{"services", "create", "--computer", "--name=c", "--image=ghcr.io/example/c@" + computerCLITestDigestB, "--node=node-1", "--disk-bytes=1000", "--idempotency-key=receipt-key"}, "/v1/computers"},
		{"restart", append([]string{"services", "restart", "computer-1"}, cas...), "/v1/computers/computer-1/restart"},
		{"reset", append([]string{"services", "reset", "computer-1", "--terminate-sessions"}, cas...), "/v1/computers/computer-1/storage-reset"},
		{"abort", append([]string{"services", "abort", "computer-1"}, cas...), "/v1/computers/computer-1/reconfiguration-abort"},
	} {
		for _, secondFailure := range []bool{false, true} {
			t.Run(test.name+map[bool]string{false: "/success", true: "/second failure"}[secondFailure], func(t *testing.T) {
				var mu sync.Mutex
				var mutations []postChangeRequest
				clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
					mu.Lock()
					defer mu.Unlock()
					if r.Method == http.MethodGet {
						if strings.HasSuffix(r.URL.Path, "/storage-provenance") {
							_, _ = io.WriteString(w, `{}`)
							return
						}
						_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
						return
					}
					body, _ := io.ReadAll(r.Body)
					mutations = append(mutations, postChangeRequest{r.Method, r.URL.RequestURI(), string(body)})
					if len(mutations) == 1 {
						w.Header().Set("Mutation-Applied", "true")
						writeCommittedReadUnavailable(w, true)
						return
					}
					w.Header().Set("Mutation-Applied", "false")
					w.Header().Set("Idempotent-Replay", "true")
					if secondFailure {
						writeCommittedReadUnavailable(w, true)
						return
					}
					w.Header().Set("Backup-Id", "backup-1")
					w.Header().Set("Restore-Operation-Revision", "3")
					w.Header().Set("Clone-Computer-Id", "computer-1")
					w.Header().Set("Clone-Operation-Revision", "3")
					if test.name == "custody attest" {
						_ = json.NewEncoder(w).Encode(l1.ComputerCustodyExport{ExportID: "export-1", ComputerID: "computer-1", OperatorAttestedDeleted: true})
						return
					}
					_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 3})
				})
				withRetryWait(t, func(context.Context, time.Duration) error { return nil })
				var stdout, stderr bytes.Buffer
				err := execute(t.Context(), clients, true, test.args, &stdout, &stderr)
				mu.Lock()
				defer mu.Unlock()
				// Create carries its key as the Job spec's dispatch_key.
				if len(mutations) != 2 || mutations[0] != mutations[1] || mutations[0].path != test.path ||
					(!strings.Contains(mutations[0].body, `"idempotency_key":"receipt-key"`) && !strings.Contains(mutations[0].body, `"dispatch_key":"receipt-key"`)) {
					t.Fatalf("mutations=%+v err=%v stdout=%s stderr=%s", mutations, err, &stdout, &stderr)
				}
				if secondFailure {
					var response *apiResponseError
					want := "job change committed but its post-change read is unavailable; the change was applied, but reading it back was unavailable; " + sameKeyHint
					if commandExitCode(err) != exitUnavailable || !errors.As(err, &response) || response.APIError.Message != want || stdout.Len() != 0 {
						t.Fatalf("err=%v stdout=%s", err, &stdout)
					}
					return
				}
				var output struct {
					MutationApplied  bool `json:"mutation_applied"`
					IdempotentReplay bool `json:"idempotent_replay"`
				}
				if err != nil || json.Unmarshal(stdout.Bytes(), &output) != nil || !output.MutationApplied || output.IdempotentReplay {
					t.Fatalf("err=%v stdout=%s stderr=%s", err, &stdout, &stderr)
				}
			})
		}
	}
}

// Grow A finishes before the resend and Grow B advances intent. The selected
// replay outcome remains A, while this command's first receipt remains applied.
func TestComputerResizeResendWaitUsesSelectedGrow(t *testing.T) {
	for _, outcome := range []string{"applied", "failed"} {
		for _, firstReplay := range []bool{false, true} {
			t.Run(outcome+"/"+map[bool]string{false: "applied", true: "replay"}[firstReplay], func(t *testing.T) {
				var mu sync.Mutex
				posts, getsAfterMutation := 0, 0
				clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
					mu.Lock()
					defer mu.Unlock()
					computer := l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1}
					if r.Method == http.MethodPost {
						posts++
						if posts == 1 {
							if firstReplay {
								w.Header().Set("Mutation-Applied", "false")
								w.Header().Set("Idempotent-Replay", "true")
							} else {
								w.Header().Set("Mutation-Applied", "true")
							}
							writeCommittedReadUnavailable(w, true)
							return
						}
						w.Header().Set("Mutation-Applied", "false")
						w.Header().Set("Idempotent-Replay", "true")
						computer.IntentRevision = 4
						computer.AppliedRevision = 4
						available := int64(123)
						computer.LastGrowOperation = &l1.ComputerStorageGrowOutcome{OperationRevision: 3, Status: outcome}
						if outcome == "failed" {
							computer.LastGrowOperation.FailureCode = "insufficient_disk"
							computer.LastGrowOperation.RequestedBytes = 1000
							computer.LastGrowOperation.ObservedAvailableBytes = &available
						}
					} else if posts > 0 {
						getsAfterMutation++
						t.Error("wait followed the later grow instead of selected completed grow")
					}
					_ = json.NewEncoder(w).Encode(computer)
				})
				withRetryWait(t, func(context.Context, time.Duration) error { return nil })
				var stdout, stderr bytes.Buffer
				err := execute(t.Context(), clients, true, []string{"services", "resize", "computer-1", "--disk-bytes=1000", "--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1", "--idempotency-key=receipt-key", "--wait=1s"}, &stdout, &stderr)
				mu.Lock()
				defer mu.Unlock()
				var output computerOperatorProjection
				unexpectedError := err != nil
				if outcome == "failed" {
					unexpectedError = commandExitCode(err) != 5
				}
				if unexpectedError || json.Unmarshal(stdout.Bytes(), &output) != nil || posts != 2 || getsAfterMutation != 0 || output.MutationApplied == nil || *output.MutationApplied == firstReplay || output.IdempotentReplay == nil || *output.IdempotentReplay != firstReplay || output.Observation == nil || output.Observation.Status != "observed" {
					t.Fatalf("err=%v posts=%d stdout=%s stderr=%s", err, posts, &stdout, &stderr)
				}
				if outcome == "failed" {
					var response *apiResponseError
					if !errors.As(err, &response) || response.APIError.Code != contract.ErrorCapacityExhausted || response.APIError.Details["requested_bytes"] != int64(1000) || response.APIError.Details["observed_available_bytes"] != int64(123) {
						t.Fatalf("original grow failure lost: %v", err)
					}
				}
			})
		}
	}
}
