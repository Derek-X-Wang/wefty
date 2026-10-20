package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Obtain the wire error through the real service restart API: a never service
// releases its slot on failure, then another service occupies that slot.
func realUpgradeCapacityRefusal(t *testing.T) error {
	t.Helper()
	h := newNodeCLIHarness(t)
	node := h.registerNode(t, "upgrade-capacity", "upgrade", l1.NodePolicy{Tags: []string{"upgrade"}, MaxServiceSlots: 1})
	spec := nodeCLIJobSpec("failed-upgrade-service", contract.JobClassService, "upgrade")
	spec.Restart = contract.RestartNever
	failed, _, err := h.store.CreateJob(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), node.identity.NodeID, node.node.NodeID, node.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim failed service: %+v %v", claim, err)
	}
	if _, err := h.store.StartAttempt(t.Context(), node.identity.NodeID, failed.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	code := 1
	if _, err := h.store.CompleteAttempt(t.Context(), node.identity.NodeID, failed.JobID, claim.Lease.AttemptID, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "failed", Result: l1.ProcessResult{ExitCode: &code}}); err != nil {
		t.Fatal(err)
	}
	h.createAndClaim(t, node, contract.JobClassService, "slot-holder", 0)
	_, err = h.clients.restartService(t.Context(), failed.JobID, "upgrade-capacity-refusal")
	var response *apiResponseError
	if !errors.As(err, &response) || response.StatusCode != 409 || response.APIError.Code != contract.ErrorCapacityExhausted || !response.APIError.Retryable {
		t.Fatalf("real L1 capacity response changed: %v", err)
	}
	return err
}

func TestUpgradeServiceOnlyFailuresDoNotBlockNextUpgrade(t *testing.T) {
	for _, failure := range []string{"capacity", "transport", "retryable-4xx", "5xx"} {
		t.Run(failure, func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "unavailable-app", AttemptID: "old-service", Class: contract.JobClassService}}
			f.operations.readJob = func(_ context.Context, _ connprofile.Profile, id string) (l1.Job, error) {
				return l1.Job{JobID: id, CurrentAttemptID: "old-service", ServiceJob: &l1.ServiceJob{BoundNodeID: f.node.NodeID, DesiredState: contract.ServiceDesiredRunning}}, nil
			}
			var capacityErr error
			if failure == "capacity" {
				capacityErr = realUpgradeCapacityRefusal(t)
			}
			f.operations.restartService = func(context.Context, connprofile.Profile, string, string) (l1.Job, error) {
				switch failure {
				case "capacity":
					return l1.Job{}, capacityErr
				case "transport":
					return l1.Job{}, errors.New("connection reset")
				case "retryable-4xx":
					return l1.Job{}, &apiResponseError{StatusCode: 429, APIError: contract.APIError{Code: contract.ErrorConflict, Retryable: true}}
				default:
					return l1.Job{}, &apiResponseError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true}}
				}
			}
			var out bytes.Buffer
			_, firstErr := runUpgrade(t.Context(), f.host, false, &out)
			if failure == "capacity" {
				if firstErr != nil {
					t.Fatalf("capacity refusal blocked ordinary cleanup: %v", firstErr)
				}
				assertNoUpgradeDrainReceipt(t, f)
			} else if firstErr == nil {
				t.Fatal("first transient recovery failure not reported")
			}
			if !f.node.ClaimsEnabled {
				t.Fatal("service failure left claims disabled")
			}
			paths, _ := resolveSetupPaths(f.home)
			if failure != "capacity" {
				if _, err := readUpgradeDrain(paths.config); err != nil {
					t.Fatal("first failure lost retry evidence", err)
				}
			}
			f.failure = "build"
			_, err := runUpgrade(t.Context(), f.host, false, &out)
			if err == nil || !strings.Contains(err.Error(), "build failed") {
				t.Fatalf("service-only failure blocked next upgrade: %v", err)
			}
			if !strings.Contains(out.String(), "wefty services restart unavailable-app --idempotency-key") {
				t.Fatalf("missing manual restart: %s", out.String())
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestUpgradeIntentCarriesDrainDeadline(t *testing.T) {
	f := newUpgradeFixture(t)
	started := f.now
	set := f.operations.setClaims
	reason := ""
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		if !r.ClaimsEnabled {
			reason = r.Reason
		}
		return set(ctx, p, r)
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	_, value, ok := strings.Cut(reason, " drain_deadline=")
	deadline, err := time.Parse(time.RFC3339Nano, value)
	if !ok || err != nil || !deadline.Equal(started.Add(2*time.Hour)) || len(reason) > 128 {
		t.Fatalf("missing bounded two-hour deadline: %q (%v)", reason, err)
	}
}

func TestStatusUpgradeDeadlineWithoutReceipt(t *testing.T) {
	now := time.Now().UTC()
	for _, c := range []struct {
		name     string
		updated  time.Time
		deadline string
		busy     bool
	}{
		{"long-live-drain", now.Add(-time.Hour), now.Add(time.Hour).Format(time.RFC3339Nano), true},
		{"deadline-margin", now.Add(-2 * time.Hour), now.Add(-29 * time.Minute).Format(time.RFC3339Nano), true},
		{"expired-deadline", now.Add(-3 * time.Hour), now.Add(-31 * time.Minute).Format(time.RFC3339Nano), false},
		{"malformed-deadline", now.Add(-time.Minute), "invalid", false},
		{"zero-deadline", now.Add(-time.Minute), "0001-01-01T00:00:00Z", false},
		{"legacy-recent", now.Add(-time.Minute), "", true},
		{"legacy-expired", now.Add(-time.Hour), "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newStatusHarness(t)
			node := readyNode("long-upgrade")
			node.ClaimsEnabled = false
			node.IntentUpdatedAt = &c.updated
			node.IntentReason = "wefty upgrade test-token"
			if c.deadline != "" {
				node.IntentReason += " drain_deadline=" + c.deadline
			}
			h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			var out bytes.Buffer
			err := executeStatus(t.Context(), h.clients(), false, nil, &out, io.Discard)
			if c.busy {
				if err != nil || !strings.Contains(out.String(), upgradeBusyMessage) {
					t.Fatalf("live deadline not busy: %v %s", err, out.String())
				}
			} else if err == nil || commandExitCode(err) != exitNotReady || !strings.Contains(out.String(), "claims disabled by an interrupted upgrade") {
				t.Fatalf("expired or malformed reason not interrupted: %v %s", err, out.String())
			}
		})
	}
}

func TestUpgradePreexistingDisableDoesNotReuseOldDeadline(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ClaimsEnabled = false
	f.node.IntentRevision = 7
	f.node.IntentReason = "wefty upgrade old-token drain_deadline=" + f.now.Add(-time.Hour).Format(time.RFC3339Nano)
	reason := f.node.IntentReason
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "finishing", AttemptID: "active", Class: contract.JobClassOneShot}}
	wait := f.operations.wait
	waited := false
	f.operations.wait = func(ctx context.Context, d time.Duration) error {
		if d == upgradeDrainPoll {
			waited = true
			f.node.ActiveAttempts = nil
		}
		return wait(ctx, d)
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if !waited || f.node.ClaimsEnabled || f.node.IntentRevision != 7 || f.node.IntentReason != reason {
		t.Fatal("preexisting disable or new drain bound changed")
	}
}
