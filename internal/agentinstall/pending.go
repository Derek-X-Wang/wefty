package agentinstall

import (
	"errors"
	"os"
)

// RecordPendingError means an expected ready/control/retirement record is not
// published yet. Installation root, catalog and generation ownership checks
// must succeed first. Storage loss is never a pending-publication observation.
// The original filesystem cause remains available through errors.Is/As.
type RecordPendingError struct{ Cause error }

func (e *RecordPendingError) Error() string { return e.Cause.Error() }
func (e *RecordPendingError) Unwrap() error { return e.Cause }

func readPendingRecord(root *os.Root, name string, target any) error {
	err := readPrivate(root, name, target)
	if errors.Is(err, os.ErrNotExist) {
		return &RecordPendingError{Cause: err}
	}
	return err
}
