package fabric

import (
	"strings"
	"testing"
	"time"
)

func TestEnrolTimeoutRecoveryAfterRollback(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state EnrolProgress
		keyed bool
		want  []string
	}{
		{"keyed approval", EnrolWaitingForDeviceApproval, true, []string{"new join key", "approve the new device within 1m30s", "pre-approved key"}},
		{"person approval", EnrolWaitingForDeviceApproval, false, []string{"sign in again", "approve the new device within 1m30s"}},
		{"sign in", EnrolWaitingForSignIn, false, []string{"rerun setup", "complete browser sign-in within 1m30s"}},
		{"keyed starting", EnrolStarting, true, []string{"check network access and the coordinator", "rerun setup with a new join key"}},
		{"person starting", EnrolStarting, false, []string{"check network access and the coordinator", "rerun setup and sign in again"}},
		{"connecting", EnrolConnecting, true, []string{"check network access and the coordinator", "rerun setup with a new join key"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := (&EnrolTimeoutError{State: tc.state, Limit: 90 * time.Second, NeedsJoinKey: tc.keyed}).Error()
			if !strings.Contains(message, "time limit reached after 1m30s") {
				t.Fatal("wrong time limit", message)
			}
			for _, want := range tc.want {
				if !strings.Contains(message, want) {
					t.Errorf("missing recovery %q: %s", want, message)
				}
			}
			if !tc.keyed && strings.Contains(message, "join key") {
				t.Fatal("person asked for a join key", message)
			}
			if strings.Contains(message, "approve it in your network's admin console") {
				t.Fatal("timeout reused live approval advice", message)
			}
		})
	}
}

func TestEnrolTimeoutWritesWholeMinutesAsWords(t *testing.T) {
	for _, tc := range []struct {
		limit time.Duration
		want  string
	}{{0, "after 15 minutes"}, {15 * time.Minute, "after 15 minutes"}, {time.Minute, "after 1 minute"}, {90 * time.Second, "after 1m30s"}} {
		message := (&EnrolTimeoutError{State: EnrolWaitingForDeviceApproval, Limit: tc.limit, NeedsJoinKey: true}).Error()
		if !strings.Contains(message, tc.want) || strings.Contains(message, "m0s") {
			t.Fatalf("limit %v: %s", tc.limit, message)
		}
	}
}
