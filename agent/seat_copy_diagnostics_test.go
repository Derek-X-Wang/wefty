package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type diagnosticCopyControl struct {
	stage   string
	failure error
}

func (c diagnosticCopyControl) AuthorizeSeatCopy(context.Context, string, l1.SeatCopyAuthorityInput) (l1.SeatCopyAuthority, error) {
	if c.stage == "authorization" {
		return l1.SeatCopyAuthority{}, c.failure
	}
	return l1.SeatCopyAuthority{Generation: 1, Place: 1}, nil
}
func (c diagnosticCopyControl) RecordSeatCopy(_ context.Context, _ string, receipt l1.SeatCopyReceipt) error {
	if c.stage == "start" || c.stage == "outcome" && receipt.Outcome != "sending" {
		return c.failure
	}
	return nil
}

func TestSeatCopyDiagnosticsPreserveStageAndBodyID(t *testing.T) {
	for _, stage := range []string{"authorization", "start", "outcome"} {
		for _, direction := range []string{"in", "out"} {
			for _, protocol := range []bool{false, true} {
				name := stage + "/" + direction + map[bool]string{false: "/transport", true: "/protocol"}[protocol]
				t.Run(name, func(t *testing.T) {
					var failure error = &url.Error{Op: "Post", URL: "http://private.test/v1/seats/" + viewerSeatID, Err: syscall.ECONNREFUSED}
					if protocol {
						failure = &ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: "seat_ended", Message: "Seat ended: released", RequestID: "copy-diagnostic-request"}}
					}
					applied := false
					runtime := copyRuntimeFunc(func(_ context.Context, _ ocihelper.CopyComputerSeatRequest, _ io.Reader, emit func([]byte) error) (int64, error) {
						applied = true
						if direction == "out" {
							if err := emit([]byte("payload")); err != nil {
								return 0, err
							}
						}
						return 7, nil
					})
					request := httptest.NewRequest(http.MethodPost, "/v1/seats/"+viewerSeatID+"/copy?direction="+direction+"&path=file", nil)
					request.Header.Set(l1.SeatCredentialHeader, "secret")
					response := httptest.NewRecorder()
					newSeatCopyHandler(runtime, diagnosticCopyControl{stage, failure}, workloadrunner.AttemptAuthority{}, "computer").ServeHTTP(response, request)
					decoder := json.NewDecoder(response.Result().Body)
					var refusal *contract.APIError
					dataEvents := 0
					for {
						var event l1.SeatCopyStreamEvent
						err := decoder.Decode(&event)
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						if len(event.Data) > 0 {
							dataEvents++
						}
						if event.Error != nil {
							refusal = event.Error
						}
					}
					if refusal == nil {
						t.Fatalf("missing refusal: %s", response.Body)
					}
					message := "Seat copy unavailable"
					if protocol {
						message = "Seat ended: released"
						if refusal.RequestID != "copy-diagnostic-request" {
							t.Fatalf("body request ID lost after flush=%t: %+v", response.Flushed, refusal)
						}
					}
					if stage != "authorization" {
						message = "copy " + stage + " unconfirmed: " + message
					}
					if refusal.Message != message {
						t.Fatalf("message=%q want=%q", refusal.Message, message)
					}
					if strings.Contains(refusal.Message, "private.test") || strings.Contains(refusal.Message, viewerSeatID) {
						t.Fatalf("transport cause exposed: %+v", refusal)
					}
					if applied != (stage == "outcome") || (direction == "out" && stage == "outcome" && (dataEvents == 0 || !response.Flushed)) {
						t.Fatalf("applied=%t data=%d flushed=%t stage=%s", applied, dataEvents, response.Flushed, stage)
					}
				})
			}
		}
	}
}
