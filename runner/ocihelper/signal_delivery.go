package ocihelper

func deliverSignalAndRecord(signal Signal, cause string, deliver func() error, record func(Signal, string)) error {
	if err := deliver(); err != nil {
		return err
	}
	record(signal, cause)
	return nil
}

func terminalResultFromSignalDelivery(code uint32, waitErr error, signal Signal, cause string, oom, logIncomplete bool) WatchResponse {
	if waitErr != nil {
		return WatchResponse{RuntimeFailure: waitErr.Error(), OutOfMemory: oom, LogEvidenceIncomplete: logIncomplete}
	}
	result := WatchResponse{OutOfMemory: oom, LogEvidenceIncomplete: logIncomplete}
	if signal == SignalKILL && code == 137 || signal == SignalTERM && code == 143 {
		result.Signal = signal
		result.TerminationCause = cause
	} else {
		exitCode := int(code)
		result.ExitCode = &exitCode
	}
	return result
}
