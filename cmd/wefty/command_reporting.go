package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
)

// commandReport is the single decision consumed by the exit and stderr paths.
// A stdout verdict suppresses JSON stderr only; text mode still prints the error.
type commandReport struct {
	exitCode      int
	errorDocument any
	stdoutVerdict bool
	err           error
	receipt       *contract.ComputerControlReceipt
}

func decideCommandReport(err error) commandReport {
	report := classifyCommandReport(err)
	// Usage always wins the exit code, independently of stderr reporting.
	if errors.As(err, new(usageError)) {
		report.exitCode = exitUsage
	}
	return report
}

func classifyCommandReport(err error) commandReport {
	var maintenanceFault *maintenanceFault
	if errors.As(err, &maintenanceFault) {
		return commandReport{exitCode: exitFailure, err: err, errorDocument: contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: err.Error(), Retryable: false}}}
	}
	var doctor *doctorResultError
	if errors.As(err, &doctor) {
		report := classifyCommandReport(doctor.cause)
		report.stdoutVerdict = true
		return report
	}
	report := commandReport{exitCode: exitFailure, err: err}
	var takeoverErr *takeoverActionError
	isTakeover := errors.As(err, &takeoverErr)
	if isTakeover {
		// Text mode carries a take-over receipt even when another error in
		// the tree takes precedence for the exit code or JSON document.
		report.receipt = takeoverErr.Receipt
	}
	var usage usageError
	isUsage := errors.As(err, &usage)

	var custody *custodyImportOutcomeError
	if errors.As(err, &custody) {
		report.stdoutVerdict = true
		switch custody.outcome {
		case custodyImportDeferred:
			report.exitCode = exitCustodyImportDeferred
		case custodyImportQuarantined:
			report.exitCode = exitCustodyImportQuarantined
		case custodyImportFailed:
			report.exitCode = exitCustodyImportFailed
		case custodyImportSuperseded:
			report.exitCode = exitCustodyImportSuperseded
		}
		if report.exitCode != exitFailure {
			return report
		}
		// Unknown custody outcomes still suppress a second JSON document, but
		// allow another typed error in the tree to determine the exit code.
	}
	var outcome *runOutcomeError
	var notReady *notReadyError
	var timeout *waitTimeoutError
	var mutationTimeout *mutationWaitTimeoutError
	var unavailable *unavailableError
	var localErr *ocicontrol.ResponseError
	var responseErr *apiResponseError
	var captureConflict *execCaptureConflictError
	var personSignIn *personSignInRequiredError
	var participantIdentity *fabric.ParticipantIdentityError
	var participantUnreachable *fabric.ParticipantUnreachableError
	var inUse *fabric.ParticipantInUseError
	var serviceUnavailable *local.UnavailableError
	var busy *local.BusyError
	var alreadyRunning *local.AlreadyRunningError
	var fenced *local.FencedCallerError
	var peerIdentity *local.PeerIdentityError
	var destinationRefused *fabric.DestinationRefusedError
	var apiError contract.APIError
	switch {
	case errors.As(err, &fenced), errors.As(err, &peerIdentity), errors.As(err, &destinationRefused):
		report.exitCode = exitUnauthorized
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorForbidden, Message: err.Error(), Retryable: false}}
		return report
	case errors.As(err, &busy):
		report.exitCode = exitUnavailable
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorClientServiceBusy, Message: err.Error(), Retryable: true}}
		return report
	case errors.As(err, &alreadyRunning):
		report.exitCode = exitConflict
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorConflict, Message: err.Error(), Retryable: false}}
		return report
	case errors.As(err, &inUse):
		report.exitCode = exitUnauthorized
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorParticipantInUse, Message: err.Error(), Retryable: false}}
		return report
	case errors.As(err, &serviceUnavailable):
		report.exitCode = exitUnavailable
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorFabricUnreachable, Message: serviceUnavailable.Error(), Retryable: false}}
		return report
	case errors.As(err, &personSignIn), errors.As(err, &participantIdentity):
		code := contract.ErrorPersonIdentityRequired
		if participantIdentity != nil && !participantIdentity.Person {
			code = contract.ErrorMachineIdentityRequired
		}
		report.exitCode = exitUnauthorized
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{
			Code: code, Message: err.Error(), Retryable: false,
		}}
		return report
	case errors.As(err, &participantUnreachable):
		report.exitCode = exitUnavailable
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorFabricUnreachable, Message: err.Error(), Retryable: true}}
		return report
	case errors.As(err, &outcome):
		report.exitCode, report.stdoutVerdict = exitRunFailed, true
		return report
	case errors.As(err, &notReady):
		report.exitCode, report.stdoutVerdict = exitNotReady, true
		return report
	case errors.As(err, &timeout):
		report.exitCode, report.stdoutVerdict = exitWaitTimeout, true
		return report
	case errors.As(err, &mutationTimeout):
		report.exitCode = exitMutationWaitTimeout
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{
			Code: errorWaitTimeout, Message: err.Error(), Retryable: false,
			Details: map[string]any{"mutation_applied": mutationTimeout.mutationApplied},
		}}
		return report
	case errors.As(err, &unavailable):
		report.exitCode = exitUnavailable
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorUnavailable, Message: err.Error(), Retryable: true,
		}}
		return report
	case isUsage:
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorInvalidRequest, Message: usage.Error(), Retryable: false,
		}}
		return report
	case errors.As(err, &captureConflict):
		apiError = contract.APIError{Code: contract.ErrorConflict, Message: captureConflict.Error(), Retryable: true}
		report.errorDocument = contract.ErrorResponse{Error: apiError}
	case errors.As(err, &localErr):
		apiError = localErr.APIError
		report.errorDocument = contract.ErrorResponse{Error: apiError}
	case errors.As(err, &responseErr):
		apiError = responseErr.APIError
		var cleanup *upgradeCleanupError
		if errors.As(err, &cleanup) {
			if cleanup.primary == nil {
				apiError.Message = cleanup.restore.Error()
			} else {
				apiError.Message += "; " + cleanup.restore.Error()
			}
		}
		report.errorDocument = contract.ErrorResponse{Error: apiError}
		if responseErr.StatusCode >= 500 && apiError.Retryable &&
			(apiError.Code == contract.ErrorInternal || apiError.Code == contract.ErrorRunLedgerUnavailable) {
			report.exitCode = exitUnavailable
			return report
		}
	case isTakeover:
		apiError = takeoverErr.APIError
		report.errorDocument = contract.ComputerControlErrorResponse{Error: apiError, Receipt: report.receipt}
	default:
		message := ""
		if err != nil {
			message = err.Error()
		}
		report.errorDocument = contract.ErrorResponse{Error: contract.APIError{
			Code: contract.ErrorInternal, Message: message, Retryable: false,
		}}
		return report
	}

	switch apiError.Code {
	case seatErrorCopyFailed, seatErrorCopyWriteDenied:
		report.exitCode = exitFailure
	case contract.ErrorRevocationWaitTimeout:
		report.exitCode = exitMutationWaitTimeout
	case contract.ErrorUnavailable, contract.ErrorFabricUnreachable, contract.ErrorClientServiceBusy:
		report.exitCode = exitUnavailable
	case contract.ErrorInvalidRequest:
		report.exitCode = exitUsage
	case contract.ErrorUnauthorized, contract.ErrorForbidden, contract.ErrorPrincipalForbidden,
		contract.ErrorParticipantInUse, contract.ErrorPersonIdentityRequired, contract.ErrorPersonDeviceMismatch, contract.ErrorMachineIdentityRequired, contract.ErrorAdminRequired, contract.ErrorControlNotAuthorized:
		report.exitCode = exitUnauthorized
	case contract.ErrorNotFound, contract.ErrorAttemptNotFound, contract.ErrorTakeoverSessionEnded:
		report.exitCode = exitNotFound
	case seatErrorRunRerun, seatErrorPersonDriving, seatErrorRoomUnconfigured, seatErrorNodeUnavailable, seatErrorWindowNotAllowed, seatErrorEnded, seatErrorRoomFull, seatErrorNoAnswer, seatErrorDenied, seatErrorDedicatedCap, seatErrorHostOff, seatErrorStartFailed, contract.ErrorCancelService, contract.ErrorCancelNotQueued, contract.ErrorConflict, contract.ErrorDeEnrollmentNotCommitted, contract.ErrorStalePolicyRevision, contract.ErrorStaleIntentRevision,
		contract.ErrorStorageReferenceConflict, contract.ErrorIdempotencyConflict, contract.ErrorDispatchKeyConflict,
		contract.ErrorInstanceKeyConflict, contract.ErrorInstanceKeyNotSupported, contract.ErrorFinalAdmin,
		contract.ErrorCapacityExhausted, contract.ErrorComputerResourceRequired, contract.ErrorComputerTraitRequired,
		contract.ErrorControllerBusy, contract.ErrorControllerAlreadyHeld, ocicontrol.ErrorHostVMRecreateBlocked:
		report.exitCode = exitConflict
	}
	return report
}

func (report commandReport) writeError(writer io.Writer, jsonOutput bool) {
	if jsonOutput {
		if !report.stdoutVerdict {
			_ = writeJSON(writer, report.errorDocument)
		}
		return
	}
	_, _ = fmt.Fprintf(writer, "wefty: %v\n", report.err)
	if report.receipt != nil {
		_ = writeComputerControlReceipt(writer, *report.receipt, false)
	}
}
