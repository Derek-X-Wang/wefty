package ocihelper

type computerReimagePreflightStageError struct {
	stage string
	cause error
}

func (err *computerReimagePreflightStageError) Error() string {
	return "Computer reimage preflight failed at " + err.stage
}

func (err *computerReimagePreflightStageError) Unwrap() error { return err.cause }

func reimagePreflightStageError(stage string, cause error) error {
	if cause == nil {
		return nil
	}
	return &computerReimagePreflightStageError{stage: stage, cause: cause}
}
