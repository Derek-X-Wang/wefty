package l1

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/bootclock"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
)

func TestLeaseSleepCreditBootClaimAndRenewal(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	want := hostboot.DigestIdentity(c.boot)
	if f.claim.Lease.SleepCreditBoot != want {
		t.Fatalf("claim proof=%q want=%q", f.claim.Lease.SleepCreditBoot, want)
	}
	c.advance(0, time.Minute)
	lease, err := f.store.RenewLease(t.Context(), "agent-lease-node", f.job.JobID, f.claim.Lease.AttemptID, f.claim.Lease.FencingToken)
	if err != nil || lease.SleepCreditBoot != want {
		t.Fatalf("renewal proof=%q want=%q err=%v", lease.SleepCreditBoot, want, err)
	}
	// Older agents use encoding/json.Decoder without DisallowUnknownFields.
	// Decode the new reply into the exact pre-field lease shape.
	raw, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	var old struct {
		AttemptID              string             `json:"attempt_id"`
		FencingToken           string             `json:"fencing_token"`
		LeaseExpires           time.Time          `json:"lease_expires_at"`
		LeaseTTL               time.Duration      `json:"lease_ttl"`
		Directive              AttemptDirective   `json:"directive,omitempty"`
		InterruptionReason     InterruptionReason `json:"interruption_reason,omitempty"`
		InterruptionRequestKey string             `json:"interruption_request_key,omitempty"`
	}
	if err := json.NewDecoder(strings.NewReader(string(raw))).Decode(&old); err != nil || old.AttemptID != lease.AttemptID || old.LeaseTTL != lease.LeaseTTL {
		t.Fatalf("old agent decode=%+v err=%v", old, err)
	}
	if strings.Contains(string(raw), c.boot) {
		t.Fatal("raw boot identity on wire")
	}
}

func TestLeaseSleepCreditBootRequiresPinnedMeasurement(t *testing.T) {
	for _, mode := range []string{"awake_unavailable", "awake_error", "boot_error", "missing_boot", "no_pausing_clock"} {
		t.Run(mode, func(t *testing.T) {
			wall := &fakeClock{now: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
			base := &seatBootTestClock{fakeClock: wall, boot: "wire-test-boot", elapsed: time.Hour}
			c := &sleepTestClock{seatBootTestClock: base, awake: time.Hour}
			var clock Clock = c
			switch mode {
			case "awake_unavailable":
				c.awakeErr = bootclock.ErrAwakeUnavailable
			case "awake_error":
				c.awakeErr = errors.New("awake failed")
			case "boot_error":
				clock = wireBrokenBootClock{c}
			case "missing_boot":
				base.boot = ""
			case "no_pausing_clock":
				clock = base
			}
			ctx := withAttemptClock(context.Background(), clock, clock.Now())
			lease := AttemptLease{SleepCreditBoot: leaseSleepCreditBoot(ctx)}
			raw, err := json.Marshal(lease)
			if err != nil || strings.Contains(string(raw), "sleep_credit_boot") {
				t.Fatalf("%s advertised proof: %s err=%v", mode, raw, err)
			}
		})
	}
}

type wireBrokenBootClock struct{ *sleepTestClock }

func (c wireBrokenBootClock) BootTime() (string, time.Duration, error) {
	return c.boot, 0, errors.New("boot failed")
}
