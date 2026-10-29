package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/seatviewer"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
)

func TestSeatViewPollRecoveryAndBound(t *testing.T) {
	unavailable := &apiResponseError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Message: "restarting"}}
	conflict := &apiResponseError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "Seat is not ready"}}
	ended := &apiResponseError{StatusCode: 410, APIError: contract.APIError{Code: "seat_ended", Message: "This Seat has ended"}}
	for _, tc := range []struct {
		name    string
		seconds []int
		errors  []error
		exit    int
		code    contract.ErrorCode
	}{
		{"recover-and-reset", []int{0, 30, 59, 60, 61, 120, 121}, []error{unavailable, conflict, errors.New("connection reset"), nil, unavailable, conflict, ended}, 5, "seat_ended"},
		{"bounded-outage", []int{0, 59, 60}, []error{unavailable, errors.New("connection refused"), conflict}, 13, contract.ErrorUnavailable},
		{"permanent-refusal", []int{0, 1}, []error{unavailable, &apiResponseError{StatusCode: 403, APIError: contract.APIError{Code: contract.ErrorAdminRequired, Message: "admin required"}}}, 3, contract.ErrorAdminRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, _ := url.Parse("http://127.0.0.1:1/v1/seats/seat_test/person/viewer")
			relay, err := seatviewer.Start(t.Context(), plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "owner"}), target)
			if err != nil {
				t.Fatal(err)
			}
			defer relay.Close()
			ticks := make(chan time.Time, len(tc.seconds))
			start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
			for _, second := range tc.seconds {
				ticks <- start.Add(time.Duration(second) * time.Second)
			}
			count := 0
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = monitorSeatViewer(ctx, relay, ticks, func(context.Context) error { result := tc.errors[count]; count++; return result })
			report := decideCommandReport(err)
			var output bytes.Buffer
			report.writeError(&output, true)
			var result contract.ErrorResponse
			if report.exitCode != tc.exit || count != len(tc.errors) || json.Unmarshal(output.Bytes(), &result) != nil || result.Error.Code != tc.code {
				t.Fatalf("poll result: exit=%d polls=%d code=%s", report.exitCode, count, result.Error.Code)
			}
		})
	}
}

func TestSeatViewTerminalReports(t *testing.T) {
	for _, code := range []contract.ErrorCode{"seat_ended", contract.ErrorTakeoverSessionEnded} {
		err := &takeover.ActionError{APIError: contract.APIError{Code: code, Message: "Viewing ended"}}
		if code == contract.ErrorTakeoverSessionEnded {
			err.Receipt = &contract.ComputerControlReceipt{SessionEndReason: "session_cap_expired"}
		}
		report := decideCommandReport(err)
		want := 5
		if code == contract.ErrorTakeoverSessionEnded {
			want = 4
		}
		var output bytes.Buffer
		report.writeError(&output, true)
		var result contract.ComputerControlErrorResponse
		if report.exitCode != want || json.Unmarshal(output.Bytes(), &result) != nil || result.Error.Code != code {
			t.Fatal("terminal event became internal")
		}
		if err.Receipt != nil && (result.Receipt == nil || result.Receipt.SessionEndReason != "session_cap_expired") {
			t.Fatal("cap receipt was lost")
		}
	}
}
