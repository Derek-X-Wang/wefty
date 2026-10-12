package l1

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestReadSnapshotHarnessLimitCoversBothReadDoors(t *testing.T) {
	s, _ := snapshotStore(t)
	s.SetReadSnapshotHardLimitForTest(2 * time.Second)
	for _, test := range []struct {
		name string
		read func(context.Context, func(context.Context, readModel) error) error
	}{
		{"operator", func(ctx context.Context, use func(context.Context, readModel) error) error {
			return s.withReadSnapshot(ctx, nil, use)
		}},
		{"agent", s.withAgentReadSnapshot},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Each independent read exceeds the production hold limit. No
			// request context carries an override; the harness Store supplies it.
			for range 2 {
				if err := test.read(t.Context(), func(ctx context.Context, reads readModel) error {
					time.Sleep(readSnapshotHardLimit + 50*time.Millisecond)
					return reads.(*databaseReads).q.QueryRowContext(ctx, "SELECT 1").Scan(new(int))
				}); err != nil {
					t.Fatalf("harness read expired at the production limit: %v", err)
				}
			}
			// Existing deterministic expiry probes must still override a
			// configured harness; caller cancellation must still stop a read.
			probe := context.WithValue(t.Context(), readSnapshotHardLimitContextKey{}, time.Nanosecond)
			if err := test.read(probe, func(context.Context, readModel) error { return nil }); errorCode(err) != contract.ErrorUnavailable {
				t.Fatalf("per-request tiny limit = %v, want unavailable", err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
			defer cancel()
			<-ctx.Done()
			if err := test.read(ctx, func(context.Context, readModel) error { return nil }); err == nil {
				t.Fatal("harness limit ignored caller deadline")
			}
		})
	}
}
