package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type copyControlFixture struct{ receipts []l1.SeatCopyReceipt }

func (f *copyControlFixture) AuthorizeSeatCopy(ctx context.Context, id string, r l1.SeatCopyAuthorityInput) (l1.SeatCopyAuthority, error) {
	if r.Credential != "secret" {
		return l1.SeatCopyAuthority{}, &ProtocolError{StatusCode: 403}
	}
	return l1.SeatCopyAuthority{Generation: 1, Place: 1}, nil
}
func (f *copyControlFixture) RecordSeatCopy(ctx context.Context, id string, r l1.SeatCopyReceipt) error {
	f.receipts = append(f.receipts, r)
	return nil
}

type copyRuntimeFixture struct{ directory string }

func (f copyRuntimeFixture) CopyComputerSeat(ctx context.Context, r ocihelper.CopyComputerSeatRequest, input io.Reader, emit func([]byte) error) (int64, error) {
	if r.Direction == "in" {
		return seatcopy.Receive(input, filepath.Join(f.directory, r.Path))
	}
	pr, pw := io.Pipe()
	done := make(chan struct {
		n   int64
		err error
	}, 1)
	go func() {
		n, err := seatcopy.Pack(pw, filepath.Join(f.directory, r.Path))
		pw.CloseWithError(err)
		done <- struct {
			n   int64
			err error
		}{n, err}
	}()
	defer pr.Close()
	buf := make([]byte, 4096)
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			if e := emit(buf[:n]); e != nil {
				return 0, e
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}
	result := <-done
	return result.n, result.err
}
func TestSeatCopyRouteCarriesCredentialAndRecordsOneEvent(t *testing.T) {
	directory := t.TempDir()
	control := &copyControlFixture{}
	handler := newSeatCopyHandler(copyRuntimeFixture{directory}, control, workloadrunner.AttemptAuthority{}, "computer")
	source := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(source, []byte("ignored"), 0600); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err := seatcopy.Pack(&archive, source); err != nil {
		t.Fatal(err)
	}
	for _, direction := range []string{"in", "out"} {
		var body io.Reader
		if direction == "in" {
			body = bytes.NewReader(archive.Bytes())
		}
		request := httptest.NewRequest(http.MethodPost, "/v1/seats/seat_00000000000000000000000000000001/copy?direction="+direction+"&path=.env", body)
		request.Header.Set(l1.SeatCredentialHeader, "secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != 200 {
			t.Fatalf("copy %s: %d %s", direction, response.Code, response.Body.String())
		}
		decoder := json.NewDecoder(response.Body)
		var completed bool
		for {
			var event l1.SeatCopyStreamEvent
			err := decoder.Decode(&event)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if event.Error != nil {
				t.Fatalf("copy error: %+v", event.Error)
			}
			if event.SizeBytes != nil {
				completed = *event.SizeBytes == 7
			}
		}
		if !completed {
			t.Fatal("copy completion missing")
		}
	}
	if len(control.receipts) != 4 || control.receipts[0].Outcome != "sending" || control.receipts[1].Direction != "in" || control.receipts[2].Outcome != "sending" || control.receipts[3].Direction != "out" || control.receipts[3].Path != ".env" || control.receipts[3].SizeBytes != 7 {
		t.Fatalf("receipts: %+v", control.receipts)
	}
}

type copyRuntimeFunc func(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error)

func (f copyRuntimeFunc) CopyComputerSeat(c context.Context, r ocihelper.CopyComputerSeatRequest, i io.Reader, e func([]byte) error) (int64, error) {
	return f(c, r, i, e)
}

type retryCopyControl struct {
	copyControlFixture
	t     *testing.T
	calls []l1.SeatCopyReceipt
}

func (f *retryCopyControl) RecordSeatCopy(ctx context.Context, id string, r l1.SeatCopyReceipt) error {
	f.calls = append(f.calls, r)
	if ctx.Err() != nil {
		f.t.Errorf("receipt inherited cancellation: %v", ctx.Err())
	}
	if _, ok := ctx.Deadline(); !ok {
		f.t.Error("receipt has no time bound")
	}
	if len(f.calls) == 1 {
		return errors.New("temporary L1 failure")
	}
	f.receipts = append(f.receipts, r)
	return nil
}
func TestSeatCopyReceiptRetriesAfterCancellationAndLogsFailures(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			control := &retryCopyControl{t: t}
			landed := filepath.Join(t.TempDir(), "landed")
			runtime := copyRuntimeFunc(func(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error) {
				if failed {
					cancel()
					return 3, errors.New("disk full")
				}
				if err := os.WriteFile(landed, []byte("landed!"), 0600); err != nil {
					t.Fatal(err)
				}
				cancel()
				return 7, nil
			})
			handler := newSeatCopyHandler(runtime, control, workloadrunner.AttemptAuthority{}, "computer")
			request := httptest.NewRequest(http.MethodPost, "/v1/seats/seat_00000000000000000000000000000001/copy?direction=in&path=file", nil).WithContext(ctx)
			request.Header.Set(l1.SeatCredentialHeader, "secret")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if len(control.calls) != 3 || len(control.receipts) != 2 {
				t.Fatalf("calls=%+v receipts=%+v response=%s", control.calls, control.receipts, response.Body.String())
			}
			if !failed {
				if data, err := os.ReadFile(landed); err != nil || string(data) != "landed!" {
					t.Fatalf("landed copy lost: %q %v", data, err)
				}
			}
			first, last := control.calls[0], control.calls[2]
			if first.CopyID == "" || last.CopyID != first.CopyID {
				t.Fatalf("retry changed CopyID: %+v", control.calls)
			}
			expected := "completed"
			if failed {
				expected = "failed"
			}
			if last.Outcome != expected || failed && (last.SizeBytes != 3 || last.Reason == "") {
				t.Fatalf("receipt=%+v", last)
			}
		})
	}
}

func TestSeatCopyNodeOwnsID(t *testing.T) {
	control := &copyControlFixture{}
	handler := newSeatCopyHandler(copyRuntimeFunc(func(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error) {
		return 999, nil
	}), control, workloadrunner.AttemptAuthority{}, "computer")
	for range 2 {
		req := httptest.NewRequest(http.MethodPost, "/v1/seats/seat_00000000000000000000000000000001/copy?direction=out&path=secrets.tar", nil)
		req.Header.Set(l1.SeatCredentialHeader, "secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		var event l1.SeatCopyStreamEvent
		if err := json.Unmarshal(response.Body.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		receipt := control.receipts[len(control.receipts)-1]
		if event.CopyID == "preclaimed" || len(event.CopyID) < 26 || event.CopyID != receipt.CopyID {
			t.Fatalf("node accepted caller ID or lost its ID: event=%+v receipt=%+v", event, receipt)
		}
	}
	if control.receipts[0].CopyID == control.receipts[2].CopyID {
		t.Fatal("two transfers reused one ID")
	}
}

func TestSeatCopyReceiptRetryBackoff(t *testing.T) {
	clock := &delayRecordingClock{}
	control := &retryCopyControl{t: t}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := recordSeatCopyReceiptWithClock(ctx, control, "seat", l1.SeatCopyReceipt{CopyID: "node-owned"}, clock); err != nil {
		t.Fatal(err)
	}
	if delays := clock.recorded(); len(delays) != 1 || delays[0] != 100*time.Millisecond {
		t.Fatalf("retry backoff=%v", delays)
	}
}

func TestSeatCopySendingExistsBeforePayload(t *testing.T) {
	control := &copyControlFixture{}
	runtime := copyRuntimeFunc(func(_ context.Context, _ ocihelper.CopyComputerSeatRequest, _ io.Reader, emit func([]byte) error) (int64, error) {
		if len(control.receipts) != 1 || control.receipts[0].Outcome != "sending" || control.receipts[0].SizeBytes != 0 {
			t.Errorf("bytes flowed before sending receipt: %+v", control.receipts)
		}
		return 1, emit([]byte("x"))
	})
	handler := newSeatCopyHandler(runtime, control, workloadrunner.AttemptAuthority{}, "computer")
	r := httptest.NewRequest(http.MethodPost, "/v1/seats/seat_00000000000000000000000000000001/copy?direction=out&path=file", nil)
	r.Header.Set(l1.SeatCredentialHeader, "secret")
	handler.ServeHTTP(httptest.NewRecorder(), r)
}

type refusedCopyStart struct{ copyControlFixture }

func (f *refusedCopyStart) RecordSeatCopy(_ context.Context, _ string, r l1.SeatCopyReceipt) error {
	f.receipts = append(f.receipts, r)
	if r.Outcome == "sending" {
		return &ProtocolError{StatusCode: http.StatusConflict, APIError: contract.APIError{Code: "seat_ended", Message: "Seat ended: released"}}
	}
	return nil
}

func TestSeatCopyRefusedStartRecordsFailureWithoutPayload(t *testing.T) {
	control := &refusedCopyStart{}
	runtime := copyRuntimeFunc(func(context.Context, ocihelper.CopyComputerSeatRequest, io.Reader, func([]byte) error) (int64, error) {
		t.Error("refused copy moved bytes")
		return 0, nil
	})
	handler := newSeatCopyHandler(runtime, control, workloadrunner.AttemptAuthority{}, "computer")
	r := httptest.NewRequest("POST", "/v1/seats/seat_00000000000000000000000000000001/copy?direction=in&path=file", nil)
	r.Header.Set(l1.SeatCredentialHeader, "secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if len(control.receipts) != 2 || control.receipts[1].Outcome != "failed" || control.receipts[1].SizeBytes != 0 || control.receipts[1].CopyID != control.receipts[0].CopyID {
		t.Fatalf("refused copy left a pending start: %+v", control.receipts)
	}
}
