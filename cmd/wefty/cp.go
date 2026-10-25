package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/seatcopy"
	"github.com/Derek-X-Wang/wefty/l1"
)

const cpUsage = "usage: wefty cp SRC SEAT_ID:PATH | wefty cp SEAT_ID:PATH DEST"

func copyFailure(code contract.ErrorCode, err error) error {
	var response *apiResponseError
	if errors.As(err, &response) {
		return err
	}
	return &apiResponseError{APIError: contract.APIError{Code: code, Message: err.Error()}}
}
func copyWriteFailure(err error) error {
	code := seatErrorCopyFailed
	if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EROFS) || errors.Is(err, seatcopy.ErrDestinationSymlink) {
		code = seatErrorCopyWriteDenied
	}
	return copyFailure(code, fmt.Errorf("copy-out destination was not published: %w", err))
}
func parseCopyArgs(args []string) (id, path, local, direction string, err error) {
	if len(args) != 2 {
		return "", "", "", "", usageError(cpUsage)
	}
	for i, arg := range args {
		prefix, value, found := strings.Cut(arg, ":")
		if !found || !strings.HasPrefix(prefix, "seat_") {
			continue
		}
		if !seatCLIIdentifier.MatchString(prefix) || value == "" || len(value) > 4096 || strings.ContainsRune(value, 0) {
			return "", "", "", "", usageError("copy requires a valid Seat ID and path")
		}
		if id != "" {
			return "", "", "", "", usageError("copy requires exactly one Seat side; Seat-to-Seat copies are not supported")
		}
		id, path = prefix, value
		local = args[1-i]
		direction = "in"
		if i == 0 {
			direction = "out"
		}
	}
	if id == "" || local == "" {
		err = usageError("copy requires exactly one SEAT_ID:PATH and one local path")
	}
	return
}

func executeCP(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) (resultErr error) {
	id, path, local, direction, err := parseCopyArgs(args)
	if err != nil {
		return err
	}
	secret, err := loadSeatCredential(id)
	if err != nil {
		return copyFailure(seatErrorCopyFailed, err)
	}
	if secret == "" {
		return copyFailure(seatErrorCopyFailed, errors.New("Seat credential not found; request or wait for this Seat first"))
	}
	if direction == "in" {
		if _, err = os.Stat(local); err != nil {
			return copyFailure(seatErrorCopyFailed, err)
		}
	}

	headers := http.Header{l1.SeatCredentialHeader: []string{secret}}
	var target l1.SeatCopyTarget
	if err = clients.l1.do(ctx, http.MethodGet, "/v1/seats/"+id+"/copy-target", nil, headers, &target, http.StatusOK); err != nil {
		return err
	}
	endpoint, err := url.Parse(target.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || (endpoint.Scheme != "ws" && endpoint.Scheme != "wss") {
		return copyFailure(seatErrorCopyFailed, errors.New("L1 returned an invalid Seat copy endpoint"))
	}
	var body io.Reader
	var packDone chan error
	if direction == "in" {
		reader, writer := io.Pipe()
		defer reader.Close()
		body = reader
		packDone = make(chan error, 1)
		go func() { _, err := seatcopy.Pack(writer, local); writer.CloseWithError(err); packDone <- err }()
	}
	node := newAPIClient("hosting node", "l1", clients.fabric, endpoint.Host)
	if transport, ok := node.client.Transport.(*http.Transport); ok {
		defer transport.CloseIdleConnections()
	}
	query := url.Values{"direction": {direction}, "path": {path}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://wefty.invalid/v1/seats/"+id+"/copy?"+query.Encode(), body)
	if err != nil {
		return copyFailure(seatErrorCopyFailed, err)
	}
	request.Header.Set(l1.SeatCredentialHeader, secret)
	request.Header.Set("Content-Type", "application/x-tar")
	response, err := node.client.Do(request)
	if err != nil {
		return copyFailure(seatErrorCopyFailed, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		var value contract.ErrorResponse
		if e := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&value); e == nil && value.Error.Code != "" {
			return &apiResponseError{StatusCode: response.StatusCode, APIError: value.Error}
		}
		return copyFailure(seatErrorCopyFailed, fmt.Errorf("Seat copy endpoint returned HTTP %d", response.StatusCode))
	}
	stream := newCopyStreamReader(response.Body)
	reported := false
	reason := "destination_failed"
	defer func() {
		// A node completion supplies its own CopyID only after its sent receipt
		// lands. Preflight/transport failures cannot create caller-only events.
		if resultErr == nil || reported || direction != "out" || stream.size == nil || stream.copyID == "" {
			return
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			reason = "cancelled"
		}
		reportErr := reportCopyOutcome(ctx, clients, id, secret, l1.SeatCopyOutcomeReceipt{CopyID: stream.copyID, Direction: direction, Path: path, SizeBytes: *stream.size, Outcome: "failed", Reason: reason})
		if reportErr != nil {
			var api *apiResponseError
			if errors.As(resultErr, &api) {
				copy := *api
				copy.APIError.Message += "; copy outcome unconfirmed: " + reportErr.Error()
				resultErr = &copy
			} else {
				resultErr = fmt.Errorf("%w; copy outcome unconfirmed: %v", resultErr, reportErr)
			}
		}
	}()
	var size int64
	if direction == "out" {
		staged, e := seatcopy.Stage(stream, local)
		if e != nil {
			// Finish consuming the bounded stream so a completed node transfer
			// can return its ID and size even when local staging was refused.
			_, _ = io.Copy(io.Discard, stream)
			reason = "destination_failed"
			if errors.Is(e, os.ErrPermission) || errors.Is(e, syscall.EROFS) || errors.Is(e, seatcopy.ErrDestinationSymlink) {
				reason = "destination_write_denied"
			}
			return copyWriteFailure(e)
		}
		defer staged.Close()
		if stream.size == nil || staged.Size != *stream.size {
			reason = "size_mismatch"
			return copyFailure(seatErrorCopyFailed, errors.New("copy size does not match completion evidence"))
		}
		if e = staged.Commit(); e != nil {
			reason = "destination_failed"
			return copyWriteFailure(e)
		}
		size = staged.Size
	} else {
		if _, err = io.Copy(io.Discard, stream); err != nil {
			return copyFailure(seatErrorCopyFailed, err)
		}
		if err = <-packDone; err != nil {
			return copyFailure(seatErrorCopyFailed, err)
		}
		size = *stream.size
	}
	if direction == "out" {
		reported = true
		if err = reportCopyOutcome(ctx, clients, id, secret, l1.SeatCopyOutcomeReceipt{CopyID: stream.copyID, Direction: direction, Path: path, SizeBytes: size, Outcome: "completed"}); err != nil {
			return copyFailure(seatErrorCopyFailed, fmt.Errorf("copy published but outcome unconfirmed: %w", err))
		}
	}
	return withSeatReply(stdout, jsonOutput, id, target.UnreadEvents, func(w io.Writer) error {
		if jsonOutput {
			return writeJSON(w, map[string]any{"seat_id": id, "copy_id": stream.copyID, "direction": direction, "path": path, "size_bytes": size})
		}
		_, err := fmt.Fprintf(w, "Copied %s %s (%d bytes)\n", direction, path, size)
		return err
	})
}

type copyStreamReader struct {
	scanner *bufio.Scanner
	pending []byte
	size    *int64
	ended   bool
	copyID  string
}

func newCopyStreamReader(r io.Reader) *copyStreamReader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 8192), 64<<10)
	return &copyStreamReader{scanner: scanner}
}
func (r *copyStreamReader) Read(p []byte) (int, error) {
	for len(r.pending) == 0 {
		if r.ended {
			return 0, io.EOF
		}
		if !r.scanner.Scan() {
			if err := r.scanner.Err(); err != nil {
				return 0, err
			}
			if r.size == nil {
				return 0, errors.New("copy stream ended without completion evidence")
			}
			r.ended = true
			return 0, io.EOF
		}
		if r.size != nil {
			return 0, errors.New("copy stream continued after completion")
		}
		var event l1.SeatCopyStreamEvent
		if err := json.Unmarshal(r.scanner.Bytes(), &event); err != nil {
			return 0, err
		}
		if event.CopyID != "" {
			if !l1.ValidSeatCopyID(event.CopyID) || r.copyID != "" && event.CopyID != r.copyID {
				return 0, errors.New("copy response changed or invalidated node CopyID")
			}
			r.copyID = event.CopyID
		}
		if event.Error != nil {
			return 0, &apiResponseError{APIError: *event.Error}
		}
		if event.SizeBytes != nil {
			if *event.SizeBytes < 0 || len(event.Data) != 0 || r.copyID == "" {
				return 0, errors.New("invalid copy completion")
			}
			r.size = event.SizeBytes
			continue
		}
		if len(event.Data) == 0 || len(event.Data) > 4096 {
			return 0, errors.New("invalid copy data chunk")
		}
		r.pending = event.Data
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func reportCopyOutcome(ctx context.Context, clients *apiClients, id, secret string, receipt l1.SeatCopyOutcomeReceipt) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	ctx = withoutClientRetry(ctx)
	headers := http.Header{l1.SeatCredentialHeader: []string{secret}}
	var err error
	for attempt := range 3 {
		err = clients.l1.do(ctx, http.MethodPost, "/v1/seats/"+id+"/copy-outcome", receipt, headers, nil, http.StatusOK)
		if err == nil {
			return nil
		}
		var api *apiResponseError
		if ctx.Err() != nil || errors.As(err, &api) && api.StatusCode < 500 && !api.APIError.Retryable {
			break
		}
		if attempt < 2 {
			wait := clients.wait
			if wait == nil {
				wait = waitForContext
			}
			if waitErr := wait(ctx, time.Duration(attempt+1)*100*time.Millisecond); waitErr != nil {
				return waitErr
			}
		}
	}
	return err
}
