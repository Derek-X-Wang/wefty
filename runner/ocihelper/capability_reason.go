package ocihelper

import (
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
)

// ClassifyBarrierError maps typed helper readiness failures to the same
// capability reasons for native and supervised boot barriers. Diagnostic text
// and resource names never determine the classification.
func ClassifyBarrierError(err error) contract.CapabilityReasonCode {
	if err == nil {
		return ""
	}
	var unavailable *HelperUnitUnavailableError
	if errors.As(err, &unavailable) {
		return contract.CapabilityReasonHelperUnitUnavailable
	}
	var stalled *HelperHandshakeStalledError
	if errors.As(err, &stalled) {
		return contract.CapabilityReasonHelperHandshakeStalled
	}
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		switch rpcErr.Code {
		case CodeChecksumMismatch, CodeVersionMismatch:
			return contract.CapabilityReasonHelperVersionMismatch
		case CodePeerUnauthenticated:
			return contract.CapabilityReasonLocalPermissionDenied
		}
	}
	var reason interface {
		CapabilityReasonCode() contract.CapabilityReasonCode
	}
	if errors.As(err, &reason) {
		return reason.CapabilityReasonCode()
	}
	return contract.CapabilityReasonBootSweepFailed
}
