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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/internal/workflowhelper"
	"github.com/Derek-X-Wang/wefty/l3"
)

// ociRunMailboxWorkflow is the workload under test. It is the SHIPPED inline
// writer -- the exact bytes `wefty workflow init` embeds for an image that does
// not carry the wefty binary -- followed by the reporting a real workflow does.
// Embedding the template itself rather than a simplified copy is the point: the
// acceptance image is BusyBox, and this lane is the only place that proves the
// writer we actually ship runs under ash.
func ociRunMailboxWorkflow() string {
	return workflowhelper.InlineBashWriter() + `
set -eu
[ -n "${WEFTY_RUN_DIR:-}" ] || { echo "no WEFTY_RUN_DIR" >&2; exit 64; }
branch=$(wefty_param branch)
[ "$branch" = "acceptance-branch" ] || { echo "params.json did not carry the branch: '$branch'" >&2; exit 65; }
# A credential must never reach a job that only reports.
[ -z "${WEFTY_RUN_TOKEN:-}" ] || { echo "a reporting job was handed a run token" >&2; exit 66; }
[ -z "${WEFTY_ATTEMPT_TOKEN:-}" ] || { echo "a reporting job was handed an attempt token" >&2; exit 67; }
printf 'no findings\n' > /tmp/vet-evidence
printf 'all gates passed\n' > /tmp/result-evidence
wefty_event step gates gates started '' 'running the gates'
wefty_event gate vet gates '' pass 'vet is clean' /tmp/vet-evidence
wefty_event step gates gates ended '' 'gates finished'
wefty_event result result result succeeded '' "branch $branch is green" /tmp/result-evidence
exit 0
`
}

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

	// Exactly one row per event. Republication after an interrupted retirement
	// is meant to be a replay in L3, not a second document, so a duplicate here
	// would mean the idempotency identity is not stable across a sweep.
	if len(envelopes) != 3 {
		t.Fatalf("published %d envelopes, want exactly the three the workload wrote:\n%s",
			len(envelopes), joinBodies(envelopes))
	}
	if len(gates) != 1 {
		t.Fatalf("published %d gates, want exactly one:\n%s", len(gates), joinBodies(gates))
	}

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
			Argv:           []string{"/bin/sh", "-c", ociRunMailboxWorkflow()},
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
	var lastStatus, lastJobID string
	for time.Now().Before(deadline) {
		status, jobID := readRunLedgerStatus(t, harness, runID)
		lastStatus, lastJobID = status, jobID
		switch status {
		case "succeeded":
			return
		case "failed", "cancelled":
			envelopes, _ := readRunLedgerEvidence(t, harness, runID)
			t.Fatalf("run %s (L1 job %s) ended %s with %d envelopes:\n%s%s",
				runID, jobID, status, len(envelopes), joinBodies(envelopes),
				describeOCIMailboxRun(t, harness, runID, lastStatus, lastJobID))
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach a terminal state%s", runID,
		describeOCIMailboxRun(t, harness, runID, lastStatus, lastJobID))
}

// describeOCIMailboxRun is diagnosis only. A run stuck non-terminal is the same
// symptom whether the attempt keeps requeueing or the agent is wedged inside
// one, and the L1 job row plus the agent's own log lines are what separate them.
func describeOCIMailboxRun(t *testing.T, harness *acceptanceHarness, runID, status, jobID string) string {
	t.Helper()
	var detail strings.Builder
	fmt.Fprintf(&detail, "\n  last observed: run=%s status=%q l1_job=%q", runID, status, jobID)
	if jobID != "" {
		detail.WriteString(describeAcceptanceL1Job(t, harness, jobID))
	}
	envelopes, gates := readRunLedgerEvidence(t, harness, runID)
	fmt.Fprintf(&detail, "\n  published: %d envelopes, %d gates", len(envelopes), len(gates))
	detail.WriteString(filteredAgentOutput(harness))
	return detail.String()
}

// describeAcceptanceL1Job reads the job row straight out of L1's database. The
// attempt count is the signal: a job on its fifth attempt is requeueing, a job
// on its first with a live attempt is wedged in the agent.
func describeAcceptanceL1Job(t *testing.T, harness *acceptanceHarness, jobID string) string {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database+"?mode=ro")
	if err != nil {
		return fmt.Sprintf("\n  l1 job: unreadable: %v", err)
	}
	defer database.Close()
	var state, currentAttempt, failureReason string
	var attempts int
	row := database.QueryRow(`SELECT state, COALESCE(current_attempt_id, ''), COALESCE(failure_reason, ''),
		(SELECT COUNT(*) FROM attempts WHERE attempts.job_id = jobs.job_id) FROM jobs WHERE job_id=?`, jobID)
	if err := row.Scan(&state, &currentAttempt, &failureReason, &attempts); err != nil {
		return fmt.Sprintf("\n  l1 job %s: unreadable: %v", jobID, err)
	}
	return fmt.Sprintf("\n  l1 job %s: state=%q attempts=%d current_attempt=%q failure=%q",
		jobID, state, attempts, currentAttempt, failureReason)
}

// filteredAgentOutput returns the agent lines that bear on this failure. The
// whole buffer is far too large for a test log, and the interesting lines are
// the ones naming the mailbox, a spawn failure, finalization, the helper, or an
// attempt.
func filteredAgentOutput(harness *acceptanceHarness) string {
	if harness == nil || harness.agent == nil {
		return "\n  agent output: unavailable"
	}
	const maxLines = 200
	interesting := regexp.MustCompile(`mailbox|spawn|finaliz|helper|attempt`)
	var matched []string
	for _, line := range strings.Split(harness.agent.output.String(), "\n") {
		if interesting.MatchString(strings.ToLower(line)) {
			matched = append(matched, line)
		}
	}
	if len(matched) == 0 {
		return "\n  agent output: no line matched mailbox|spawn|finaliz|helper|attempt"
	}
	elided := 0
	if len(matched) > maxLines {
		elided = len(matched) - maxLines
		matched = matched[len(matched)-maxLines:]
	}
	var detail strings.Builder
	fmt.Fprintf(&detail, "\n  agent output (%d matching lines", len(matched))
	if elided > 0 {
		fmt.Fprintf(&detail, ", %d earlier elided", elided)
	}
	detail.WriteString("):")
	for _, line := range matched {
		detail.WriteString("\n    " + line)
	}
	return detail.String()
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
