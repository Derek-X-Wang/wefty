package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestClassifyAgentProtocolErrorAssignsAttemptAuthorityByCode(t *testing.T) {
	for _, code := range []contract.ErrorCode{
		contract.ErrorLeaseExpired,
		contract.ErrorStaleFence,
		contract.ErrorAttemptMismatch,
		contract.ErrorAttemptNotFound,
		contract.ErrorAttemptNotOwned,
	} {
		t.Run(string(code), func(t *testing.T) {
			classification := classifyAgentProtocolError(&ProtocolError{
				StatusCode: http.StatusForbidden,
				APIError: contract.APIError{
					Code:      code,
					Retryable: true,
				},
			})
			if classification.destination != errorDestinationAttemptAuthority {
				t.Fatalf("destination = %d, want attempt-authority", classification.destination)
			}
		})
	}
}

func TestClassifyAgentProtocolErrorSplitsNodeSessionReactionByCode(t *testing.T) {
	tests := []struct {
		code     contract.ErrorCode
		reaction nodeSessionReaction
	}{
		{code: contract.ErrorNodeNotRegistered, reaction: nodeSessionReregister},
		{code: contract.ErrorNodeDead, reaction: nodeSessionReregister},
		{code: contract.ErrorNodeDraining, reaction: nodeSessionDrain},
		{code: contract.ErrorNodeSessionReplaced, reaction: nodeSessionStopRecordAndEscalate},
		{code: contract.ErrorIdentityBound, reaction: nodeSessionStopRecordAndEscalate},
		{code: contract.ErrorPrincipalForbidden, reaction: nodeSessionStopRecordAndEscalate},
	}
	for _, test := range tests {
		t.Run(string(test.code), func(t *testing.T) {
			classification := classifyAgentProtocolError(&ProtocolError{
				StatusCode: http.StatusForbidden,
				APIError:   contract.APIError{Code: test.code},
			})
			if classification.destination != errorDestinationNodeSession {
				t.Fatalf("destination = %d, want node-session", classification.destination)
			}
			if classification.nodeSessionReaction != test.reaction {
				t.Fatalf("node-session reaction = %d, want %d", classification.nodeSessionReaction, test.reaction)
			}
		})
	}
}

func TestClassifyAgentProtocolErrorDefaultsClientFailuresToTransient(t *testing.T) {
	invalidResponse := &http.Response{
		StatusCode: http.StatusBadGateway,
		Body:       io.NopCloser(strings.NewReader("not-json")),
	}
	invalidProtocolError := decodeProtocolError(invalidResponse)

	tests := []struct {
		name string
		err  error
	}{
		{name: "transport", err: errors.New("connection refused")},
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "unparseable", err: invalidProtocolError},
		{name: "internal retryable false", err: &ProtocolError{
			StatusCode: http.StatusBadRequest,
			APIError:   contract.APIError{Code: contract.ErrorInternal, Retryable: false},
		}},
		{name: "unknown", err: &ProtocolError{
			StatusCode: http.StatusForbidden,
			APIError:   contract.APIError{Code: contract.ErrorCode("future_code"), Retryable: false},
		}},
		{name: "generic forbidden", err: &ProtocolError{
			StatusCode: http.StatusForbidden,
			APIError:   contract.APIError{Code: contract.ErrorForbidden, Retryable: false},
		}},
		{name: "local fatal is not a protocol destination", err: &ProtocolError{
			StatusCode: http.StatusBadRequest,
			APIError:   contract.APIError{Code: contract.ErrorCode("local_fatal"), Retryable: false},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			classification := classifyAgentProtocolError(test.err)
			if classification.destination != errorDestinationTransient {
				t.Fatalf("destination = %d, want transient", classification.destination)
			}
			if classification.nodeSessionReaction != nodeSessionNoReaction {
				t.Fatalf("node-session reaction = %d, want none", classification.nodeSessionReaction)
			}
		})
	}
}

func TestClassifyAgentProtocolErrorKnownAuthorityOverridesStatusAndRetryable(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		classification := classifyAgentProtocolError(&ProtocolError{
			StatusCode: http.StatusInternalServerError,
			APIError: contract.APIError{
				Code:      contract.ErrorAttemptNotOwned,
				Retryable: retryable,
			},
		})
		if classification.destination != errorDestinationAttemptAuthority {
			t.Fatalf("Retryable=%t destination = %d, want attempt-authority", retryable, classification.destination)
		}
	}
}
