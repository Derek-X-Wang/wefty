//go:build service_acceptance

package agent

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestServiceAcceptanceAgentProtocolErrorClassifierContract(t *testing.T) {
	tests := []struct {
		code        contract.ErrorCode
		destination errorDestination
		reaction    nodeSessionReaction
	}{
		{code: contract.ErrorInternal, destination: errorDestinationTransient},
		{code: contract.ErrorLeaseExpired, destination: errorDestinationAttemptAuthority},
		{code: contract.ErrorStaleFence, destination: errorDestinationAttemptAuthority},
		{code: contract.ErrorAttemptMismatch, destination: errorDestinationAttemptAuthority},
		{code: contract.ErrorAttemptNotFound, destination: errorDestinationAttemptAuthority},
		{code: contract.ErrorAttemptNotOwned, destination: errorDestinationAttemptAuthority},
		{code: contract.ErrorNodeNotRegistered, destination: errorDestinationNodeSession, reaction: nodeSessionReregister},
		{code: contract.ErrorNodeDead, destination: errorDestinationNodeSession, reaction: nodeSessionReregister},
		{code: contract.ErrorNodeDraining, destination: errorDestinationNodeSession, reaction: nodeSessionDrain},
		{code: contract.ErrorNodeSessionReplaced, destination: errorDestinationNodeSession, reaction: nodeSessionStopRecordAndEscalate},
		{code: contract.ErrorIdentityBound, destination: errorDestinationNodeSession, reaction: nodeSessionStopRecordAndEscalate},
		{code: contract.ErrorPrincipalForbidden, destination: errorDestinationNodeSession, reaction: nodeSessionStopRecordAndEscalate},
	}
	for _, test := range tests {
		t.Run(string(test.code), func(t *testing.T) {
			classification, ok := agentProtocolErrorClassifications[test.code]
			if !ok {
				t.Fatal("semantic code is absent from the classifier data")
			}
			if classification.destination != test.destination || classification.nodeSessionReaction != test.reaction {
				t.Fatalf("classification = (%d, %d), want (%d, %d)", classification.destination, classification.nodeSessionReaction, test.destination, test.reaction)
			}
		})
	}

	for _, protocolErr := range []*ProtocolError{
		{
			StatusCode: http.StatusInternalServerError,
			APIError:   contract.APIError{Code: contract.ErrorAttemptNotOwned, Retryable: true},
		},
		{
			StatusCode: http.StatusBadRequest,
			APIError:   contract.APIError{Code: contract.ErrorAttemptNotOwned, Retryable: false},
		},
	} {
		if got := classifyAgentProtocolError(protocolErr).destination; got != errorDestinationAttemptAuthority {
			t.Fatalf("known authority code was overridden by HTTP status or Retryable: destination = %d", got)
		}
	}

	for name, err := range map[string]error{
		"transport":         errors.New("connection refused"),
		"unknown":           &ProtocolError{StatusCode: http.StatusForbidden, APIError: contract.APIError{Code: "future_code"}},
		"generic forbidden": &ProtocolError{StatusCode: http.StatusForbidden, APIError: contract.APIError{Code: contract.ErrorForbidden}},
		"local fatal":       &ProtocolError{StatusCode: http.StatusBadRequest, APIError: contract.APIError{Code: "local_fatal"}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := classifyAgentProtocolError(err); got.destination != errorDestinationTransient || got.nodeSessionReaction != nodeSessionNoReaction {
				t.Fatalf("classification = (%d, %d), want transient with no node-session reaction", got.destination, got.nodeSessionReaction)
			}
		})
	}

	if _, ok := agentProtocolErrorClassifications[contract.ErrorCode("local_fatal")]; ok {
		t.Fatal("local-fatal must not be a protocol classification")
	}
}
