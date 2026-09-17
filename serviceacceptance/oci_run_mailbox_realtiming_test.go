//go:build service_acceptance_realtiming && linux

package serviceacceptance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
)

// ociRunMailboxWorkflow is the workload under test: a POSIX shell script that
// writes run-mailbox events with the inline writer the scaffold ships, because
// the acceptance image is BusyBox and does not carry the wefty binary. It
// deliberately uses nothing beyond BusyBox ash builtins and applets.
//
// It reads its own parameters out of params.json, which the helper seeded, and
// reports a step, a gate and a result -- exactly what a real workflow does.
const ociRunMailboxWorkflow = `set -eu
wefty_nanos() {
	printf '%019d' "$(date -u +%s)000000000"
}
wefty_event() {
	mkdir -p "$WEFTY_RUN_DIR/tmp" "$WEFTY_RUN_DIR/events"
	WEFTY_EVENT_SEQ=$(((${WEFTY_EVENT_SEQ:-0} + 1) % 10000))
	wefty_file=$(printf '%s-%04d-%s-%.32s-%08x' "$(wefty_nanos)" "$WEFTY_EVENT_SEQ" "$1" "${2:-event}" "$$")
	{
		printf 'wefty-protocol: 1\nkind: %s\n' "$1"
		[ -z "${2:-}" ] || printf 'name: %s\n' "$2"
		[ -z "${3:-}" ] || printf 'step: %s\n' "$3"
		[ -z "${4:-}" ] || printf 'status: %s\n' "$4"
		[ -z "${5:-}" ] || printf 'outcome: %s\n' "$5"
		[ -z "${6:-}" ] || printf 'summary: %s\n' "$6"
		printf 'payload: text\n--\n'
		[ -z "${7:-}" ] || printf '%s\n' "$7"
	} >"$WEFTY_RUN_DIR/tmp/$wefty_file"
	mv "$WEFTY_RUN_DIR/tmp/$wefty_file" "$WEFTY_RUN_DIR/events/$wefty_file"
}
wefty_param() {
	[ -f "$WEFTY_RUN_DIR/params.json" ] || return 0
	tr -d '\n' <"$WEFTY_RUN_DIR/params.json" |
		sed -n 's/.*"'"$1"'"[[:space:]]*:[[:space:]]*"\([^"\\]*\)".*/\1/p'
}
[ -n "${WEFTY_RUN_DIR:-}" ] || { echo "no WEFTY_RUN_DIR" >&2; exit 64; }
branch=$(wefty_param branch)
[ "$branch" = "acceptance-branch" ] || { echo "params.json did not carry the branch: '$branch'" >&2; exit 65; }
# A credential must never reach a job that only reports.
[ -z "${WEFTY_RUN_TOKEN:-}" ] || { echo "a reporting job was handed a run token" >&2; exit 66; }
wefty_event step gates gates started '' 'running the gates'
wefty_event gate vet gates '' pass 'vet is clean' 'no findings'
wefty_event step gates gates ended '' 'gates finished'
wefty_event result result result succeeded '' "branch $branch is green" 'all gates passed'
exit 0
`

// TestOCIRunMailboxEventsReachTheRunLedgerThroughTheHelper is the live proof of
// #476 slice C: an OCI one-shot dispatched by L3, holding no credential at all,
// reports through its run mailbox, and the node agent publishes those events to
// L3 by reading the helper-owned handoff volume through the helper.
func TestOCIRunMailboxEventsReachTheRunLedgerThroughTheHelper(t *testing.T) {
	reference := os.Getenv("WEFTY_OCI_PROBE_REFERENCE")
	digest := os.Getenv("WEFTY_OCI_PROBE_DIGEST")
	if reference == "" || digest == "" {
		t.Skip("the OCI probe image is not published for this lane")
	}
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration: 10 * time.Second, runLedgerLane: true,
	})

	runID := submitOCIMailboxRun(t, harness, reference, digest)
	waitForOCIMailboxRun(t, harness, runID)

	envelopes, gates := readRunLedgerEvidence(t, harness, runID)
	assertMailboxEnvelope(t, envelopes, "gates", contract.EnvelopePartial, "running the gates")
	assertMailboxEnvelope(t, envelopes, "gates", contract.EnvelopeSucceeded, "gates finished")
	assertMailboxEnvelope(t, envelopes, "result", contract.EnvelopeSucceeded, "branch acceptance-branch is green")
	assertMailboxGate(t, gates, "vet", contract.GatePass, "no findings")

	// Every document the agent built for this run is the agent's, not the
	// workload's: L3 binds the attempt from the authenticated run token, and
	// the workload never held one.
	for _, body := range envelopes {
		var envelope contract.Envelope
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.RunID != runID {
			t.Fatalf("envelope run = %q, want %q", envelope.RunID, runID)
		}
	}
}

func submitOCIMailboxRun(t *testing.T, harness *acceptanceHarness, reference, digest string) string {
	t.Helper()
	request := l3.CreateRunRequest{
		Image: &contract.ImageProgram{
			Reference: reference, Digest: &digest,
			Argv:           []string{"/bin/sh", "-c", ociRunMailboxWorkflow},
			RuntimeHandler: "io.containerd.runc.v2",
		},
		Params: json.RawMessage(`{"branch":"acceptance-branch"}`),
		// Deliberately absent: DispatchAuthority. This run reports and
		// dispatches nothing, so it is handed no credential at all -- the whole
		// point of reading its mailbox through the helper.
	}
	var accepted l3.RunAccepted
	status, body := runLedgerJSON(t, harness, http.MethodPost, "/v1/runs", "oci-mailbox-"+fmt.Sprint(time.Now().UnixNano()), request, &accepted)
	if status != http.StatusCreated && status != http.StatusOK {
		t.Fatalf("submit OCI mailbox run status = %d body=%s", status, body)
	}
	if accepted.RunID == "" {
		t.Fatalf("run ledger accepted no run: %s", body)
	}
	return accepted.RunID
}

func waitForOCIMailboxRun(t *testing.T, harness *acceptanceHarness, runID string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		status, jobID := readRunLedgerStatus(t, harness, runID)
		switch status {
		case "succeeded":
			return
		case "failed", "cancelled":
			envelopes, _ := readRunLedgerEvidence(t, harness, runID)
			t.Fatalf("run %s (L1 job %s) ended %s with %d envelopes:\n%s",
				runID, jobID, status, len(envelopes), joinBodies(envelopes))
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach a terminal state", runID)
}

func runLedgerJSON(t *testing.T, harness *acceptanceHarness, method, path, idempotencyKey string, input, output any) (int, []byte) {
	t.Helper()
	network, err := plain.NewNetworkWithID("plain-oci-run-mailbox-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	participant := network.NewFabric(fabric.Identity{NodeID: "oci-mailbox-client", Tags: []string{l3.DefaultCallerPrincipalTag}})
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, harness.runLedgerAddress)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 30 * time.Second, Transport: transport}
	var payload *strings.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		payload = strings.NewReader(string(encoded))
	} else {
		payload = strings.NewReader("")
	}
	request, err := http.NewRequestWithContext(t.Context(), method, "http://run-ledger.invalid"+path, payload)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body := make([]byte, 0, 4096)
	buffer := make([]byte, 4096)
	for {
		count, readErr := response.Body.Read(buffer)
		body = append(body, buffer[:count]...)
		if readErr != nil {
			break
		}
	}
	if output != nil && len(body) > 0 {
		_ = json.Unmarshal(body, output)
	}
	return response.StatusCode, body
}

// readRunLedgerEvidence reads the ledger's own append-only rows. The ledger has
// no read route for them, and reading the rows is the point: the assertion is
// that the agent's publication actually landed, not that some projection says
// it did.
func readRunLedgerEvidence(t *testing.T, harness *acceptanceHarness, runID string) (envelopes [][]byte, gates [][]byte) {
	t.Helper()
	database := openRunLedgerDatabase(t, harness)
	defer database.Close()
	for _, query := range []struct {
		statement string
		into      *[][]byte
	}{
		{statement: `SELECT body_json FROM envelopes WHERE run_id=? ORDER BY accepted_ns, envelope_id`, into: &envelopes},
		{statement: `SELECT body_json FROM gate_results WHERE run_id=? ORDER BY accepted_ns, gate_id`, into: &gates},
	} {
		rows, err := database.Query(query.statement, runID)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var body []byte
			if err := rows.Scan(&body); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			*query.into = append(*query.into, body)
		}
		closeErr := rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	return envelopes, gates
}

func readRunLedgerStatus(t *testing.T, harness *acceptanceHarness, runID string) (status string, jobID string) {
	t.Helper()
	database := openRunLedgerDatabase(t, harness)
	defer database.Close()
	var l1JobID sql.NullString
	if err := database.QueryRow(`SELECT status, l1_job_id FROM runs WHERE run_id=?`, runID).Scan(&status, &l1JobID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ""
		}
		t.Fatal(err)
	}
	return status, l1JobID.String
}

func openRunLedgerDatabase(t *testing.T, harness *acceptanceHarness) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", harness.runLedgerDatabase+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func assertMailboxEnvelope(t *testing.T, bodies [][]byte, step string, status contract.EnvelopeStatus, summary string) {
	t.Helper()
	for _, body := range bodies {
		var envelope contract.Envelope
		if err := json.Unmarshal(body, &envelope); err != nil {
			continue
		}
		if envelope.StepID == step && envelope.Status == status && strings.Contains(envelope.Summary, summary) {
			return
		}
	}
	t.Fatalf("no %s envelope on step %q summarising %q among %d published envelopes:\n%s",
		status, step, summary, len(bodies), joinBodies(bodies))
}

func assertMailboxGate(t *testing.T, bodies [][]byte, name string, outcome contract.GateOutcome, evidence string) {
	t.Helper()
	for _, body := range bodies {
		var gate contract.GateResult
		if err := json.Unmarshal(body, &gate); err != nil {
			continue
		}
		if gate.Name != name || gate.Outcome != outcome {
			continue
		}
		for _, item := range gate.Evidence {
			if strings.Contains(item.Value, evidence) {
				return
			}
		}
		t.Fatalf("gate %q passed but carried no evidence containing %q: %+v", name, evidence, gate.Evidence)
	}
	t.Fatalf("no %s gate named %q among %d published gates:\n%s", outcome, name, len(bodies), joinBodies(bodies))
}

func joinBodies(bodies [][]byte) string {
	lines := make([]string, 0, len(bodies))
	for _, body := range bodies {
		lines = append(lines, string(body))
	}
	return strings.Join(lines, "\n")
}
