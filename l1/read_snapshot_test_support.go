package l1

import (
	"context"
	"time"
)

// SetReadSnapshotHardLimitForTest lets an in-process integration harness tolerate
// a slow test runner. Call it before sharing the Store or serving requests.
// It covers direct Store reads and every HTTP request using that Store, without
// changing page cutoffs, overrun accounting, or caller deadlines. Production
// callers must not use it; a Store without this override keeps the 200 ms limit.
func (s *Store) SetReadSnapshotHardLimitForTest(limit time.Duration) {
	if limit <= 0 {
		panic("l1: test read snapshot hard limit must be positive")
	}
	s.readSnapshotHardLimitForTest = limit
}

func (s *Store) snapshotHardLimit(ctx context.Context) time.Duration {
	if override, ok := ctx.Value(readSnapshotHardLimitContextKey{}).(time.Duration); ok {
		return override
	}
	if s.readSnapshotHardLimitForTest > 0 {
		return s.readSnapshotHardLimitForTest
	}
	return readSnapshotHardLimit
}
