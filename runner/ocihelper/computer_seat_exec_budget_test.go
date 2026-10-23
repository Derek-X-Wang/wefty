package ocihelper

import (
	"sync"
	"testing"
)

func TestSeatExecAttemptAndGlobalCapsPreserveScreenCapacity(t *testing.T) {
	server := &Server{dataStreams: make(chan struct{}, 56)}
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	take := func(attempt string, place int) {
		t.Helper()
		authority := testAuthority()
		authority.AttemptID = attempt
		release, refusal := server.admitSeatCommand(ExecComputerSeatRequest{Authority: authority, Place: place})
		if refusal != nil {
			t.Fatal(refusal)
		}
		dataRelease, ok := server.admitDataStream()
		if !ok {
			release()
			t.Fatal("exec exhausted data slots")
		}
		var dataOnce sync.Once
		releases = append(releases, func() { release(); dataOnce.Do(dataRelease) })
	}
	refuse := func(attempt string, place int, detail ErrorDetail) {
		t.Helper()
		authority := testAuthority()
		authority.AttemptID = attempt
		release, refusal := server.admitSeatCommand(ExecComputerSeatRequest{Authority: authority, Place: place})
		if refusal == nil {
			release()
			t.Fatal("over-limit exec admitted")
		}
		if refusal.Code != CodeConnectionLimit || refusal.Detail != detail {
			t.Fatalf("wrong refusal: %+v", refusal)
		}
	}
	for _, attempt := range []string{"a", "b", "c", "d"} {
		take(attempt, 1)
		take(attempt, 1)
		refuse(attempt, 1, "seat_command_limit")
		take(attempt, 2)
		take(attempt, 2)
		refuse(attempt, 3, "attempt_command_limit")
	}
	refuse("e", 1, "command_stream_limit")
	// Global 16 is below the 56 data slots. Exec cannot take the other 40.
	for i := 0; i < 40; i++ {
		release, ok := server.admitDataStream()
		if !ok {
			t.Fatal("screen slots lost to exec")
		}
		defer release()
	}
	releases[0]()
	releases[0]() // release is idempotent and does not free two slots.
	take("e", 1)
	refuse("f", 1, "command_stream_limit")
}
