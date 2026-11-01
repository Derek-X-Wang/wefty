package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCPRealBinaryCopyAndTypedFailures(t *testing.T) {
	binary := buildWefty(t)
	id := "seat_00000000000000000000000000000001"
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	if _, err := storeSeatCredential(id, "secret"); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(source, []byte("ignored secret"), 0600); err != nil {
		t.Fatal(err)
	}
	remote := filepath.Join(t.TempDir(), "remote")
	var outcomes []l1.SeatCopyOutcomeReceipt
	var outcomeAttempts []string
	copyNumber := 0
	var address string
	address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(l1.SeatCredentialHeader) != "secret" {
			t.Errorf("copy lost credential")
			w.WriteHeader(403)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/copy-outcome") {
			var value l1.SeatCopyOutcomeReceipt
			if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
				t.Error(err)
			}
			outcomeAttempts = append(outcomeAttempts, value.CopyID)
			if len(outcomeAttempts) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorUnavailable, Message: "retry", Retryable: true}})
				return
			}
			outcomes = append(outcomes, value)
			_ = json.NewEncoder(w).Encode(struct{}{})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/copy-target") {
			_ = json.NewEncoder(w).Encode(l1.SeatCopyTarget{Endpoint: "ws://" + address + "/rfb"})
			return
		}
		if r.Header.Get("X-Wefty-Copy-Id") != "" {
			t.Error("CLI supplied a caller-owned CopyID")
		}
		copyNumber++
		copyID := fmt.Sprintf("node-copy-%d", copyNumber)
		encoder := json.NewEncoder(w)
		direction := r.URL.Query().Get("direction")
		if direction == "in" {
			size, err := seatcopy.Receive(r.Body, remote)
			if err != nil {
				t.Error(err)
				return
			}
			_ = encoder.Encode(l1.SeatCopyStreamEvent{CopyID: copyID, SizeBytes: &size})
			return
		}
		switch r.URL.Query().Get("path") {
		case "fail":
			_ = encoder.Encode(l1.SeatCopyStreamEvent{Error: &contract.APIError{Code: "seat_copy_failed", Message: "disk full"}})
			return
		case "not-ready":
			_ = encoder.Encode(l1.SeatCopyStreamEvent{Error: &contract.APIError{Code: contract.ErrorConflict, Message: "Seat is not ready or changed (seat_not_ready); try again", Retryable: true}})
			return
		case "ended":
			_ = encoder.Encode(l1.SeatCopyStreamEvent{Error: &contract.APIError{Code: "seat_ended", Message: "Seat ended: released"}})
			return
		}
		var data bytes.Buffer
		size, err := seatcopy.Pack(&data, remote)
		if err != nil {
			t.Error(err)
			return
		}
		for data.Len() > 0 {
			_ = encoder.Encode(l1.SeatCopyStreamEvent{Data: data.Next(min(data.Len(), 4096))})
		}
		_ = encoder.Encode(l1.SeatCopyStreamEvent{CopyID: copyID, SizeBytes: &size})
	})
	run := func(args ...string) (int, []byte, []byte) {
		return runPolishBinary(t, binary, append([]string{"--fabric=plain", "--l1=" + address, "--json", "cp"}, args...)...)
	}
	code, out, stderr := run(source, id+":.env")
	if code != 0 {
		t.Fatalf("copy in exit=%d out=%s err=%s", code, out, stderr)
	}
	dest := filepath.Join(t.TempDir(), "result")
	code, out, stderr = run(id+":.env", dest)
	if code != 0 {
		t.Fatalf("copy out exit=%d out=%s err=%s", code, out, stderr)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "ignored secret" {
		t.Fatalf("result=%q err=%v", got, err)
	}
	if err = os.Symlink(source, filepath.Join(filepath.Dir(dest), "escape")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		src, dest, typed string
		exit             int
		retryable        bool
	}{
		{id + ":.env", filepath.Join(filepath.Dir(dest), "escape"), "seat_copy_write_denied", exitFailure, false},
		{id + ":fail", dest, "seat_copy_failed", exitFailure, false},
		{id + ":.env", filepath.Dir(dest), "seat_copy_failed", exitFailure, false},
		{filepath.Join(t.TempDir(), "missing"), id + ":in", "seat_copy_failed", exitFailure, false},
		{id + ":not-ready", dest, "conflict", exitConflict, true},
		{id + ":ended", dest, "seat_ended", exitConflict, false},
	} {
		code, out, stderr = run(tc.src, tc.dest)
		if tc.dest == filepath.Dir(dest) && !bytes.Contains(stderr, []byte("DEST names the exact result path")) {
			t.Fatalf("existing folder hint missing: %s", stderr)
		}
		var failure contract.ErrorResponse
		if err := json.Unmarshal(stderr, &failure); err != nil {
			t.Fatalf("invalid JSON error: %v: %s", err, stderr)
		}
		if code != tc.exit || string(failure.Error.Code) != tc.typed || failure.Error.Retryable != tc.retryable {
			t.Fatalf("typed failure %s exit=%d out=%s err=%s", tc.typed, code, out, stderr)
		}
	}
	got, _ = os.ReadFile(dest)
	if string(got) != "ignored secret" {
		t.Fatalf("failed copy damaged destination: %q", got)
	}
	if len(outcomeAttempts) != 4 || outcomeAttempts[0] != outcomeAttempts[1] {
		t.Fatalf("outcome retries changed ID: %+v", outcomeAttempts)
	}
	if len(outcomes) != 3 {
		t.Fatalf("missing CLI copy outcomes: %+v", outcomes)
	}
	if outcomes[0].CopyID != "node-copy-2" || outcomes[0].Outcome != "completed" || outcomes[0].SizeBytes != 14 {
		t.Fatalf("completion=%+v", outcomes[0])
	}
	for _, outcome := range outcomes[1:] {
		if outcome.Outcome != "failed" || outcome.Reason == "" || !strings.HasPrefix(outcome.CopyID, "node-copy-") {
			t.Fatalf("failure=%+v", outcome)
		}
	}

}
