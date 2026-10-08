package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

type reviewTransport struct{ err error }

func (r reviewTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, r.err }

func TestReviewTransportRetryAdvice(t *testing.T) {
	for _, cause := range []error{syscall.ECONNREFUSED, &net.DNSError{IsTimeout: true}, context.DeadlineExceeded} {
		t.Run(fmt.Sprint(cause), func(t *testing.T) {
			client := &apiClient{name: "L3", flag: "l3", client: &http.Client{Transport: reviewTransport{cause}}}
			err := client.do(context.Background(), "GET", "/test", nil, nil, nil, 200)
			if !errors.Is(err, cause) {
				t.Fatalf("cause lost: %v", err)
			}
			var stderr bytes.Buffer
			writeCommandError(&stderr, err, true)
			var envelope contract.ErrorResponse
			if decode := json.Unmarshal(stderr.Bytes(), &envelope); decode != nil || commandExitCode(err) != 13 || envelope.Error.Code != "unavailable" || !envelope.Error.Retryable {
				t.Fatalf("exit=%d error=%s decode=%v", commandExitCode(err), stderr.String(), decode)
			}
		})
	}
}

func TestReviewTypedOutcomesDoNotEmitErrorJSON(t *testing.T) {
	for _, outcome := range []custodyImportOutcome{custodyImportDeferred, custodyImportQuarantined, custodyImportFailed, custodyImportSuperseded} {
		var stderr bytes.Buffer
		writeCommandError(&stderr, &custodyImportOutcomeError{outcome: outcome}, true)
		if stderr.Len() != 0 {
			t.Errorf("%s: %s", outcome, stderr.String())
		}
	}
}
