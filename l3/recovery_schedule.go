package l3

import "time"

// unrecordedDispatchBackoff returns the delay after this failure, given the
// number of earlier consecutive failures.
func unrecordedDispatchBackoff(failures int) time.Duration {
	delay := unrecordedDispatchRetryBase
	for i := 0; i < failures && delay < unrecordedDispatchRetryCap; i++ {
		delay *= 2
	}
	return min(delay, unrecordedDispatchRetryCap)
}

// recoveryNow is the ledger clock reading recovery stamps on an L1 read before
// sending it.
func (s *Store) recoveryNow() time.Time { return canonicalTime(s.clock.Now()) }
