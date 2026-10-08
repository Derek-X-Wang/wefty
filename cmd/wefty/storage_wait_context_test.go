package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerStorageWaitBoundsEveryObservationRead(t *testing.T) {
	wait := storageWaitFlags{timeout: 50 * time.Millisecond, pollInterval: time.Millisecond}
	for _, test := range []struct {
		name string
		wait func(context.Context, *apiClients) error
	}{
		{"backup", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForBackupOperation(ctx, clients, "computer-1", "backup-1", wait)
			return err
		}},
		{"prune", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForBackupPrune(ctx, clients, "computer-1", "backup-1", wait)
			return err
		}},
		{"clone", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerClone(ctx, clients, cloneComputerOperation{computerID: "computer-1", operationRevision: 1}, wait)
			return err
		}},
		{"restore", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerRestore(ctx, clients, "computer-1", 1, wait)
			return err
		}},
		{"import", func(ctx context.Context, clients *apiClients) error {
			_, _, _, err := waitForCustodyImport(ctx, clients, "import-1", 1, wait)
			return err
		}},
		{"import-authority", func(ctx context.Context, clients *apiClients) error {
			_, _, _, err := waitForCustodyImport(ctx, clients, "import-1", 1, wait)
			return err
		}},
		{"export", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForCustodyExport(ctx, clients, "computer-1", "export-1", wait)
			return err
		}},
		{"resize", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerGrowRevision(ctx, clients, "computer-1", 1, wait)
			return err
		}},
		{"removal", func(ctx context.Context, clients *apiClients) error {
			_, _, err := waitForComputerRemoval(ctx, clients, "computer-1", wait)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.name == "import-authority" && r.URL.Path == "/v1/custody-imports/import-1" {
					_ = json.NewEncoder(w).Encode(l1.ComputerCustodyImportObservation{OperationRevision: 1, Status: "complete"})
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			// The caller deadline is only a safety net for the original bug.
			// It must not be mistaken for the much shorter --wait deadline.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			client := &apiClient{name: "L1", client: server.Client()}
			client.client.Transport = rewriteWaitTestTransport{base: server.Client().Transport, url: server.URL}
			err := test.wait(ctx, &apiClients{l1: client})
			if code := commandExitCode(err); code != exitMutationWaitTimeout || ctx.Err() != nil {
				t.Fatalf("observation exit = %d, caller = %v, want wait deadline exit 14: %v", code, ctx.Err(), err)
			}
		})
	}
}

func TestComputerWaitRetainsProjectionAtRequestDeadline(t *testing.T) {
	for _, verb := range []string{"resize", "removal"} {
		t.Run(verb, func(t *testing.T) {
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if reads.Add(1) == 1 {
					_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2})
					return
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			client := &apiClient{name: "L1", client: server.Client()}
			client.client.Transport = rewriteWaitTestTransport{base: client.client.Transport, url: server.URL}
			clients := &apiClients{l1: client}
			wait := storageWaitFlags{timeout: 50 * time.Millisecond, pollInterval: time.Millisecond}
			var observed l1.Computer
			var err error
			if verb == "resize" {
				observed, _, err = waitForComputerGrowRevision(ctx, clients, "computer-1", 2, wait)
			} else {
				observed, _, err = waitForComputerRemoval(ctx, clients, "computer-1", wait)
			}
			if commandExitCode(err) != exitMutationWaitTimeout || reads.Load() < 2 || observed.ComputerID != "computer-1" {
				t.Fatalf("last observation = %#v, reads = %d, exit = %d, want retained projection at wait timeout: %v",
					observed, reads.Load(), commandExitCode(err), err)
			}
		})
	}
}

// Keep the real HTTP transport and body reads; only replace the placeholder
// authority that apiClient normally routes through Fabric.
type rewriteWaitTestTransport struct {
	base http.RoundTripper
	url  string
}

func (transport rewriteWaitTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.URL.Scheme = "http"
	request.URL.Host = transport.url[len("http://"):]
	return transport.base.RoundTrip(request)
}

func TestStorageWaitDeadlineExitsFourteenFromBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, stall := range []string{"headers", "body", "provenance"} {
		t.Run(stall, func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/computers/computer-1":
					_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
				case "/v1/computers/computer-1/backups":
					if r.Method == http.MethodPost {
						w.Header().Set("Backup-Id", "backup-1")
						w.WriteHeader(http.StatusAccepted)
						_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: "computer-1", IntentRevision: 3})
						return
					}
					if stall == "provenance" {
						_ = json.NewEncoder(w).Encode(l1.BackupList{Operation: &l1.ComputerBackupOperationOutcome{BackupID: "backup-1", Status: "published"}})
						return
					}
					if stall == "body" {
						_, _ = w.Write([]byte("{"))
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
				case "/v1/computers/computer-1/storage-provenance":
					// A failed wait must not begin an unbounded follow-up read.
					<-r.Context().Done()
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			})
			code, output := runWefty(t, binary, 3*time.Second, "--json", "--l1="+address,
				"services", "backup", "create", "computer-1", "--idempotency-key=k",
				"--intent-revision=2", "--storage-id=storage-1", "--storage-generation=1", "--wait=50ms")
			decoder := json.NewDecoder(bytes.NewBufferString(output))
			var result storageMutationOutput
			var envelope contract.ErrorResponse
			if err := decoder.Decode(&result); err != nil {
				t.Fatalf("mutation output: %v: %s", err, output)
			}
			if err := decoder.Decode(&envelope); err != nil {
				t.Fatalf("error output: %v: %s", err, output)
			}
			if code != exitMutationWaitTimeout || envelope.Error.Code != errorWaitTimeout || envelope.Error.Retryable ||
				envelope.Error.Details["mutation_applied"] != true || !result.MutationApplied ||
				result.Observation == nil || result.Observation.Status != "failed" {
				t.Fatalf("wait exit = %d, want 14 with non-retryable wait_timeout and accepted mutation: %s", code, output)
			}
		})
	}
}

type canceledResponseBody struct{ cancel context.CancelFunc }

func (body canceledResponseBody) Read([]byte) (int, error) {
	// Cancellation happens inside Read, after Do has returned its response.
	body.cancel()
	return 0, context.Canceled
}

func (canceledResponseBody) Close() error { return nil }

type canceledBodyTransport struct {
	cancel context.CancelFunc
	status int
}

func (transport canceledBodyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: transport.status, Body: canceledResponseBody{transport.cancel}, Header: make(http.Header)}, nil
}

func TestAPICanceledResponseBodyIsLocalFailure(t *testing.T) {
	for _, service := range []string{"L1", "L3"} {
		for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
			t.Run(service+"/"+http.StatusText(status), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				client := &apiClient{name: service, client: &http.Client{Transport: canceledBodyTransport{cancel, status}}}
				err := client.do(ctx, http.MethodGet, "/test", nil, nil, nil, http.StatusOK)
				var stderr bytes.Buffer
				writeCommandError(&stderr, err, true)
				var envelope contract.ErrorResponse
				if decodeErr := json.Unmarshal(stderr.Bytes(), &envelope); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if !errors.Is(err, context.Canceled) || commandExitCode(err) != exitFailure ||
					envelope.Error.Code != contract.ErrorInternal || envelope.Error.Retryable {
					t.Fatalf("canceled body exit = %d, want local exit 1, retryable=false: %s", commandExitCode(err), stderr.String())
				}
			})
		}
	}
}
