package agent

import (
	"context"
	"encoding/json"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type syncRuntimeFixture struct{ summary string }

func (f syncRuntimeFixture) CopyComputerSeat(_ context.Context, r ocihelper.CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	if r.Direction != "sync" || r.Path != "work" {
		return 0, io.ErrUnexpectedEOF
	}
	io.Copy(io.Discard, input)
	summary := f.summary
	if summary == "" {
		summary = `{"files_written":3,"files_removed":2,"size_bytes":7}`
	}
	err := emit([]byte(summary))
	return 7, err
}
func TestSeatSyncRouteRecordsCounts(t *testing.T) {
	control := &copyControlFixture{}
	handler := newSeatCopyHandler(syncRuntimeFixture{}, control, workloadrunner.AttemptAuthority{}, "computer")
	request := httptest.NewRequest(http.MethodPost, "/v1/seats/seat_00000000000000000000000000000001/copy?direction=sync&path=work", strings.NewReader(""))
	request.Header.Set(l1.SeatCredentialHeader, "secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if len(control.receipts) != 2 {
		t.Fatalf("no sync receipts: %s", response.Body.String())
	}
	raw, _ := json.Marshal(control.receipts[1])
	var got map[string]any
	_ = json.Unmarshal(raw, &got)
	counts, _ := got["sync"].(map[string]any)
	if got["direction"] != "sync" || got["size_bytes"] != float64(7) || counts["files_written"] != float64(3) || counts["files_removed"] != float64(2) {
		t.Fatalf("sync log receipt: %s", raw)
	}
	var event l1.SeatCopyStreamEvent
	if err := json.Unmarshal(response.Body.Bytes(), &event); err != nil || event.Error != nil || event.SizeBytes == nil {
		t.Fatalf("completion=%s %v", response.Body.String(), err)
	}
}

func TestSeatSyncRouteRefusesIncompleteCounts(t *testing.T) {
	for _, summary := range []string{`{"size_bytes":7}`, `{"size_bytes":7,"files_written":1}`, `{"size_bytes":7,"files_removed":1}`, `{"size_bytes":7,"files_written":-1,"files_removed":0}`, `{"size_bytes":8,"files_written":0,"files_removed":0}`} {
		control := &copyControlFixture{}
		handler := newSeatCopyHandler(syncRuntimeFixture{summary: summary}, control, workloadrunner.AttemptAuthority{}, "computer")
		request := httptest.NewRequest(http.MethodPost, "/v1/seats/seat_00000000000000000000000000000001/copy?direction=sync&path=work", strings.NewReader(""))
		request.Header.Set(l1.SeatCredentialHeader, "secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var event l1.SeatCopyStreamEvent
		if err := json.Unmarshal(response.Body.Bytes(), &event); err != nil || event.Error == nil || len(control.receipts) != 2 || control.receipts[1].Outcome != "failed" {
			t.Fatalf("incomplete summary %s: %s receipts=%+v", summary, response.Body.String(), control.receipts)
		}
	}
}
