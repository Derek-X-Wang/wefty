package agent

import (
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
)

type agentProtocolErrorClassification struct {
	destination         errorDestination
	nodeSessionReaction nodeSessionReaction
}

// nodeSessionReaction records the code-specific reaction for the resilience
// cutover. The classifier names it as data but does not execute it.
type nodeSessionReaction uint8

const (
	nodeSessionNoReaction nodeSessionReaction = iota
	nodeSessionReregister
	nodeSessionDrain
	nodeSessionStopRecordAndEscalate
)

var agentProtocolErrorClassifications = map[contract.ErrorCode]agentProtocolErrorClassification{
	contract.ErrorInternal:        {destination: errorDestinationTransient},
	contract.ErrorLeaseExpired:    {destination: errorDestinationAttemptAuthority},
	contract.ErrorStaleFence:      {destination: errorDestinationAttemptAuthority},
	contract.ErrorAttemptMismatch: {destination: errorDestinationAttemptAuthority},
	contract.ErrorAttemptNotFound: {destination: errorDestinationAttemptAuthority},
	contract.ErrorAttemptNotOwned: {destination: errorDestinationAttemptAuthority},

	contract.ErrorNodeNotRegistered:   {destination: errorDestinationNodeSession, nodeSessionReaction: nodeSessionReregister},
	contract.ErrorNodeDead:            {destination: errorDestinationNodeSession, nodeSessionReaction: nodeSessionReregister},
	contract.ErrorNodeDraining:        {destination: errorDestinationNodeSession, nodeSessionReaction: nodeSessionDrain},
	contract.ErrorNodeSessionReplaced: {destination: errorDestinationNodeSession, nodeSessionReaction: nodeSessionStopRecordAndEscalate},
	contract.ErrorIdentityBound:       {destination: errorDestinationNodeSession, nodeSessionReaction: nodeSessionStopRecordAndEscalate},
	contract.ErrorPrincipalForbidden:  {destination: errorDestinationNodeSession, nodeSessionReaction: nodeSessionStopRecordAndEscalate},
}

// classifyAgentProtocolError classifies an error returned by a Client
// operation. A non-ProtocolError at that seam is transport, timeout, or an
// unparseable response and therefore transient. Local invariant failures must
// remain outside this classifier.
func classifyAgentProtocolError(err error) agentProtocolErrorClassification {
	var protocolErr *ProtocolError
	if errors.As(err, &protocolErr) {
		if classification, ok := agentProtocolErrorClassifications[protocolErr.APIError.Code]; ok {
			return classification
		}
	}
	return agentProtocolErrorClassification{destination: errorDestinationTransient}
}
