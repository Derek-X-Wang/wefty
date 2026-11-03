package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Exercise the real CLI adapters. The node writes request IDs into the error
// body, including a copy-out refusal after payload chunks have been flushed.
func TestSeatDiagnosticsReturnedCLIRequestID(t *testing.T) {
	id := "seat_" + strings.Repeat("a", 32)
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	if _, err := storeSeatCredential(id, "secret"); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"screen-view", "screen-input", "copy-in", "copy-out", "copy-out-after-data", "copy-sync"} {
		t.Run(mode, func(t *testing.T) {
			requestID := "diagnostic-" + mode
			source := filepath.Join(t.TempDir(), "source")
			if err := os.WriteFile(source, []byte("payload"), 0600); err != nil {
				t.Fatal(err)
			}
			var archive bytes.Buffer
			if _, err := seatcopy.Pack(&archive, source); err != nil {
				t.Fatal(err)
			}
			var address string
			address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "-target") {
					_ = json.NewEncoder(w).Encode(l1.SeatCopyTarget{Endpoint: "ws://" + address})
					return
				}
				if strings.HasPrefix(mode, "screen-") {
					want := "/screen/view"
					if mode == "screen-input" {
						want = "/screen/input"
					}
					if !strings.HasSuffix(r.URL.Path, want) {
						t.Errorf("screen admission path=%s want=%s", r.URL.Path, want)
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusForbidden)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorForbidden, Message: "invalid Seat credential", RequestID: requestID}})
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/x-ndjson")
				encode := json.NewEncoder(w)
				if mode == "copy-out-after-data" {
					_ = encode.Encode(l1.SeatCopyStreamEvent{Data: archive.Bytes()})
					_ = http.NewResponseController(w).Flush()
				}
				_ = encode.Encode(l1.SeatCopyStreamEvent{CopyID: "node-copy-diagnostic", Error: &contract.APIError{Code: contract.ErrorForbidden, Message: "copy outcome unconfirmed", RequestID: requestID}})
			})
			clients, err := newAPIClients(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "diagnostic-client"}), address, "")
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(mode, "screen-") {
				args := []string{"screenshot", id, "-o", filepath.Join(t.TempDir(), "image.png")}
				if mode == "screen-input" {
					args = []string{"click", id, "1", "2"}
				}
				err = executeSeatScreen(t.Context(), clients, true, args, io.Discard, io.Discard)
			} else if mode == "copy-sync" {
				capture, captureErr := captureExec(t.Context(), execTestRepo(t))
				if captureErr != nil {
					t.Fatal(captureErr)
				}
				defer os.RemoveAll(capture.Storage)
				err = syncSeatCapture(t.Context(), clients, id, "secret", capture)
			} else {
				args := []string{id + ":file", filepath.Join(t.TempDir(), "destination")}
				if mode == "copy-in" {
					args = []string{source, id + ":file"}
				}
				err = executeCP(t.Context(), clients, true, args, io.Discard)
			}
			var api *apiResponseError
			if !errors.As(err, &api) || api.APIError.RequestID != requestID {
				t.Fatalf("returned CLI error lost request ID: error=%v API=%+v", err, api)
			}
			var rendered bytes.Buffer
			writeCommandError(&rendered, err, true)
			var envelope contract.ErrorResponse
			if json.Unmarshal(rendered.Bytes(), &envelope) != nil || envelope.Error.RequestID != requestID {
				t.Fatalf("rendered CLI error lost request ID: %s", &rendered)
			}
		})
	}
}
