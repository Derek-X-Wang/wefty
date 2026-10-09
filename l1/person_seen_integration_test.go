package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func personSeenRow(t *testing.T, h *integrationHarness, fabricID, userID string) (string, int64) {
	t.Helper()
	var deviceID string
	var lastSeen int64
	if err := h.store.db.QueryRow(`SELECT last_device_id, last_seen_ns FROM authenticated_people
		WHERE fabric_id=? AND user_id=?`, fabricID, userID).Scan(&deviceID, &lastSeen); err != nil {
		t.Fatal(err)
	}
	return deviceID, lastSeen
}

// The write lock is a live BEGIN IMMEDIATE on another connection; a known,
// up-to-date person must still answer over HTTP with no write.
func TestKnownPersonViewAnswersWithWriteLockHeld(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	identity := fabric.Identity{UserID: "person-alice", DeviceID: "device-a"}
	client := h.client(identity)
	status, _, body := h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("first whoami status=%d body=%s", status, body)
	}
	var observed AuthenticatedPerson
	if err := json.Unmarshal(body, &observed); err != nil {
		t.Fatal(err)
	}
	_, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID)

	lockConn, err := h.store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer lockConn.Close()
	defer func() { _, _ = lockConn.ExecContext(context.Background(), `ROLLBACK`) }()

	lockedClient := h.client(identity)
	lockedClient.Timeout = 3 * time.Second
	before := time.Now()
	status, _, body = h.do(lockedClient, http.MethodGet, "/v1/whoami", nil)
	elapsed := time.Since(before)
	if status != http.StatusOK {
		t.Fatalf("whoami with the write lock held status=%d body=%s", status, body)
	}
	if elapsed < 0 || elapsed > 500*time.Millisecond {
		t.Fatalf("known person view took %s with the write lock held, want under 500ms", elapsed)
	}
	if _, second := personSeenRow(t, h, observed.FabricID, observed.UserID); second != lastSeen {
		t.Fatalf("known person view rewrote last_seen: %d then %d", lastSeen, second)
	}
}

// Once the snapshot pool is fully busy, the observation must still be recorded
// durably before the answer: the unavailability fallback is load-bearing.
func TestPersonObservationFallsBackWhenSnapshotBusy(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	var held []*sql.Conn
	for i := 0; i < readSnapshotLimit; i++ {
		conn, err := h.store.readDB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		held = append(held, conn)
	}
	client := h.client(fabric.Identity{UserID: "person-alice", DeviceID: "device-a"})
	client.Timeout = 5 * time.Second
	status, _, body := h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("first view with the snapshot pool busy status=%d body=%s", status, body)
	}
	var observed AuthenticatedPerson
	if err := json.Unmarshal(body, &observed); err != nil {
		t.Fatal(err)
	}
	if _, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID); lastSeen == 0 {
		t.Fatal("fallback write did not record the person")
	}
	if h.store.personObservationFallbackCount() == 0 {
		t.Fatal("snapshot-unavailable view did not count a fallback write")
	}
}

// A nested snapshot is a programming error, not unavailability: the fallback
// must not turn that refusal into a write.
func TestPersonObservationNestedSnapshotFailsClosed(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	identity := fabric.Identity{FabricID: h.store.deploymentID, UserID: "person-nest", DeviceID: "device-nest"}
	var inner error
	err := h.store.withReadSnapshot(context.Background(), nil, func(ctx context.Context, reads readModel) error {
		_, inner = h.store.ObserveAuthenticatedPerson(ctx, identity)
		return nil
	})
	if err != nil {
		t.Fatalf("outer snapshot: %v", err)
	}
	if !errors.Is(inner, errNestedReadSnapshot) {
		t.Fatalf("nested observation error = %v, want %v", inner, errNestedReadSnapshot)
	}
	if h.store.personObservationFallbackCount() != 0 {
		t.Fatal("nested snapshot error counted a fallback write")
	}
	var count int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM authenticated_people
		WHERE fabric_id=? AND user_id=?`, identity.FabricID, identity.UserID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("nested observation error wrote the person: rows=%d err=%v", count, err)
	}
}

// A last_seen from the future is already stale: a negative age must refresh.
func TestPersonObservationFutureDatedRecordIsStale(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{UserID: "person-alice", DeviceID: "device-a"})
	status, _, body := h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("first whoami status=%d body=%s", status, body)
	}
	var observed AuthenticatedPerson
	if err := json.Unmarshal(body, &observed); err != nil {
		t.Fatal(err)
	}
	futureNS := observed.SeenAt.Add(time.Hour).UnixNano()
	if _, err := h.store.db.Exec(`UPDATE authenticated_people SET last_seen_ns=?`, futureNS); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("future-dated whoami status=%d", status)
	}
	if _, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID); lastSeen == futureNS {
		t.Fatalf("future-dated record was not refreshed: last_seen=%d", lastSeen)
	}
}

func TestPersonObservationRefreshesOnlyWhenDirtyOrStale(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	identity := fabric.Identity{UserID: "person-alice", DeviceID: "device-a"}
	client := h.client(identity)
	status, _, body := h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("first whoami status=%d body=%s", status, body)
	}
	var observed AuthenticatedPerson
	if err := json.Unmarshal(body, &observed); err != nil {
		t.Fatal(err)
	}

	h.clock.Advance(30 * time.Minute)
	status, _, _ = h.do(client, http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("fresh whoami status=%d", status)
	}
	if device, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID); device != "device-a" || lastSeen != observed.SeenAt.UnixNano() {
		t.Fatalf("fresh view rewrote the observation: device=%q last_seen=%d want %q/%d",
			device, lastSeen, "device-a", observed.SeenAt.UnixNano())
	}

	// A changed recorded field refreshes.
	rotated := fabric.Identity{UserID: "person-alice", DeviceID: "device-b"}
	status, _, body = h.do(h.client(rotated), http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("device-rotated whoami status=%d body=%s", status, body)
	}
	var rotatedObserved AuthenticatedPerson
	if err := json.Unmarshal(body, &rotatedObserved); err != nil {
		t.Fatal(err)
	}
	if device, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID); device != "device-b" || lastSeen != rotatedObserved.SeenAt.UnixNano() {
		t.Fatalf("device change did not refresh the observation: device=%q last_seen=%d", device, lastSeen)
	}

	// A stale record refreshes at the 1-hour interval.
	staleAt := rotatedObserved.SeenAt
	h.clock.Advance(time.Hour)
	status, _, body = h.do(h.client(rotated), http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("stale whoami status=%d", status)
	}
	if _, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID); lastSeen == staleAt.UnixNano() {
		t.Fatalf("stale observation was not refreshed: last_seen=%d", lastSeen)
	}
}

// A person's first request records them durably before it answers, so a
// grant right after the first connection succeeds; the follow-up view inside
// the refresh window performs no further write.
func TestFirstPersonViewRecordsThenGrantSucceeds(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	admin := fabric.Identity{UserID: "person-alice", DeviceID: "device-alice"}
	person := fabric.Identity{UserID: "person-bob", DeviceID: "device-bob"}
	adminClient := h.client(admin)

	challenge, err := h.store.InitiateAdminBootstrap(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(adminClient, http.MethodPost, "/v1/admin-bootstrap",
		BootstrapAdminRequest{Nonce: challenge.Nonce})
	if status != http.StatusCreated {
		t.Fatalf("bootstrap status=%d body=%s", status, body)
	}
	policy, err := h.store.GetAdminPolicy(context.Background())
	if err != nil || len(policy.Admins) != 1 {
		t.Fatalf("bootstrapped policy = %#v err=%v", policy, err)
	}
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "grant-after-first-view", Spec: computerCapabilityJobSpec("computer:grant-after-first-view"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}

	status, _, body = h.do(h.client(person), http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("first view status=%d body=%s", status, body)
	}
	var observed AuthenticatedPerson
	if err := json.Unmarshal(body, &observed); err != nil {
		t.Fatal(err)
	}
	if observed.SeenAt == (time.Time{}) {
		t.Fatalf("first view observation = %#v", observed)
	}
	h.clock.Advance(15 * time.Minute)
	status, _, _ = h.do(h.client(person), http.MethodGet, "/v1/whoami", nil)
	if status != http.StatusOK {
		t.Fatalf("second view status=%d", status)
	}
	if _, lastSeen := personSeenRow(t, h, observed.FabricID, observed.UserID); lastSeen != observed.SeenAt.UnixNano() {
		t.Fatalf("view inside the refresh window rewrote last_seen: %d then %d", observed.SeenAt.UnixNano(), lastSeen)
	}

	status, _, body = h.do(adminClient, http.MethodPut,
		"/v1/computers/"+computer.ComputerID+"/grants/"+person.UserID,
		ComputerGrantMutationRequest{PolicyRevision: policy.Revision, Permission: ComputerGrantView,
			IdempotencyKey: "grant-after-first-view"})
	if status != http.StatusOK && status != http.StatusCreated {
		t.Fatalf("grant after first view status=%d body=%s", status, body)
	}
}

// The admin-policy views are read-snapshot users: a busy read pool must reach
// the caller as the contract's retryable 503 with its read_snapshot_* reason,
// never as a scrubbed 500.
func TestAdminPolicyViewReportsBusyReadPoolAsUnavailable(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	for i := 0; i < readSnapshotLimit; i++ {
		conn, err := h.store.readDB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}
	client := h.client(fabric.Identity{UserID: "person-busy", DeviceID: "device-busy"})
	client.Timeout = 5 * time.Second
	status, _, body := h.do(client, http.MethodGet, "/v1/admin-policy", nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("admin policy with the read pool busy status=%d body=%s, want 503", status, body)
	}
	var envelope contract.ErrorResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != contract.ErrorUnavailable || !envelope.Error.Retryable ||
		envelope.Error.Details["reason"] != "read_snapshot_admission_expired" {
		t.Fatalf("admin policy busy-pool error = %+v, want retryable unavailable read_snapshot_admission_expired", envelope.Error)
	}
}
