package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type seatCredentialProbeFabric struct{ fabric.Fabric }

func (seatCredentialProbeFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}}, nil
}

func seatCredentialProbeHTTP(t *testing.T, ctx context.Context, store *Store, target, feature string, input SeatCopyAuthorityInput) (int, contract.APIError) {
	t.Helper()
	server, err := NewServer(seatCredentialProbeFabric{}, store, ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var payload any = input
	if feature == "screen-authority" {
		payload = SeatScreenAuthorityInput{SeatCopyAuthorityInput: input}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/agent/seats/"+target+"/"+feature, bytes.NewReader(body)).WithContext(ctx)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, r)
	var response contract.ErrorResponse
	if json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatalf("probe response=%d %s", w.Code, w.Body)
	}
	return w.Code, response.Error
}

// Read state directly: a public Seat read can itself check/finalize lifetime.
type seatProbeWanted struct {
	Computer string
	Place    int
}
type seatProbeState struct {
	Seat     SeatRequest
	Events   []string
	Wanted   []seatProbeWanted
	Revision int64
}

func readSeatProbeState(t *testing.T, f *seatLifecycleFixture, id string) seatProbeState {
	t.Helper()
	value, err := readSeatRequest(t.Context(), f.store.db, id)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := seatProbeState{Seat: value}
	rows, err := f.store.db.QueryContext(t.Context(), `SELECT event_json FROM seat_log_events WHERE seat_id=? ORDER BY ordinal`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var event string
		if err := rows.Scan(&event); err != nil {
			t.Fatal(err)
		}
		snapshot.Events = append(snapshot.Events, event)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wanted, err := f.store.db.QueryContext(t.Context(), `SELECT computer_id,place FROM computer_wanted_seats WHERE seat_id=? ORDER BY computer_id,place`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer wanted.Close()
	for wanted.Next() {
		var entry seatProbeWanted
		if err := wanted.Scan(&entry.Computer, &entry.Place); err != nil {
			t.Fatal(err)
		}
		snapshot.Wanted = append(snapshot.Wanted, entry)
	}
	if err := wanted.Err(); err != nil {
		t.Fatal(err)
	}
	if err := f.store.db.QueryRowContext(t.Context(), `SELECT revision FROM computer_seat_lists WHERE computer_id=?`, f.computer.ComputerID).Scan(&snapshot.Revision); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// Each bound/probe comparison gets its own Store. A screen expiry probe must
// never prepare copy's fixture by finalizing that fixture's Seat first.
func TestSeatScreenCredentialOnlyProbePreservesRefusals(t *testing.T) {
	for _, feature := range []string{"screen-authority", "copy-authority"} {
		for _, expired := range []bool{false, true} {
			for _, kind := range []string{"matching", "wrong", "foreign"} {
				name := feature + "/" + kind + map[bool]string{false: "-live", true: "-expired"}[expired]
				t.Run(name, func(t *testing.T) {
					var probeStatus int
					var probeDecision contract.APIError
					for _, bound := range []bool{false, true} {
						f, id, secret, authority := liveSeatForCopy(t)
						target, credential := id, secret
						if kind == "wrong" {
							credential = "wrong-secret"
						}
						if kind == "foreign" {
							// The credential's owner is live/expired; the requested
							// other Seat exists but does not own that credential.
							target = "seat_00000000000000000000000000000003"
							if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: target, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
								t.Fatal(err)
							}
						}
						if kind == "foreign" {
							f.request(t, 3, true)
							if reserved := f.reserve(t, target, f.computer, 2, DefaultComputerRoomConfig(2<<30)); reserved.State != SeatApproved {
								t.Fatalf("foreign Seat reservation=%+v", reserved)
							}
							if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", target, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
								t.Fatal(err)
							}
							list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", authority.ComputerID, authority.JobID, authority.AttemptID, authority.ComputerSeatAttemptRequest)
							if err != nil {
								t.Fatal(err)
							}
							report := ComputerSeatObservationReport{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, FencingToken: authority.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 2, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}, {SeatID: target, Place: 2, State: ComputerSeatReady}}}
							if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", authority.ComputerID, authority.JobID, authority.AttemptID, report); err != nil {
								t.Fatal(err)
							}
						}
						if expired {
							f.clock.Advance(31 * time.Minute)
						}
						before := readSeatProbeState(t, f, id)
						otherBefore := readSeatProbeState(t, f, target)
						if before.Seat.State != SeatLive || otherBefore.Seat.State != SeatLive {
							t.Fatalf("probe fixtures must be live before admission: owner=%+v target=%+v", before.Seat, otherBefore.Seat)
						}
						input := SeatCopyAuthorityInput{Credential: credential}
						if bound {
							input = authority
							input.Credential = credential
						}
						status, decision := seatCredentialProbeHTTP(t, t.Context(), f.store, target, feature, input)
						wantStatus, wantCode, wantMessage := 403, contract.ErrorForbidden, "invalid Seat credential"
						if kind == "matching" {
							wantMessage = contract.SeatComputerMismatchMessage
							if bound {
								wantStatus, wantCode, wantMessage = 200, "", ""
							}
							if expired && feature == "screen-authority" {
								wantStatus, wantCode, wantMessage = 409, "seat_ended", "Seat ended: expired"
							}
						}
						// Matching bound success isn't an error envelope.
						if status != wantStatus || decision.Code != wantCode || decision.Message != wantMessage {
							t.Fatalf("bound=%t status=%d decision=%+v want=%d %s %s", bound, status, decision, wantStatus, wantCode, wantMessage)
						}
						if !bound {
							after := readSeatProbeState(t, f, id)
							if kind == "matching" && expired && feature == "screen-authority" {
								if before.Seat.State != SeatLive || len(before.Wanted) != 1 || after.Seat.State != SeatEnding || after.Seat.Reason != "expired" || len(after.Wanted) != 0 || after.Revision != before.Revision+1 || len(after.Events) <= len(before.Events) {
									t.Fatalf("expiry transition: before=%+v after=%+v", before, after)
								}
								var ended bool
								for _, raw := range after.Events[len(before.Events):] {
									var event SeatLogEvent
									if err := json.Unmarshal([]byte(raw), &event); err != nil {
										t.Fatal(err)
									}
									ended = ended || event.Kind == SeatEventEnded
								}
								if !ended {
									t.Fatal("expiry did not append ended event")
								}
							} else if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(otherBefore, readSeatProbeState(t, f, target)) {
								t.Fatalf("probe mutated state: before=%+v after=%+v", before, after)
							}
							probeStatus, probeDecision = status, decision
						} else if kind != "matching" || (expired && feature == "screen-authority") {
							if status != probeStatus || decision.Code != probeDecision.Code || decision.Message != probeDecision.Message {
								t.Fatalf("bound=%d %+v probe=%d %+v", status, decision, probeStatus, probeDecision)
							}
						}
					}
				})
			}
		}
	}
}

// Reuse the existing SQLite fixture and held-write seam. The writer remains
// held until both HTTP refusals finish; the deadline is only a deadlock bound.
func TestSeatCopyCredentialOnlyProbeDoesNotWaitForWriter(t *testing.T) {
	for _, credentialKind := range []string{"wrong", "matching"} {
		t.Run(credentialKind, func(t *testing.T) {
			f, id, secret, authority := liveSeatForCopy(t)
			write, err := f.store.beginWriteTransaction(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer write.rollback()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			for _, bound := range []bool{true, false} {
				input := SeatCopyAuthorityInput{Credential: secret}
				if bound {
					input = authority
				}
				if credentialKind == "wrong" {
					input.Credential = "wrong-secret"
				}
				status, decision := seatCredentialProbeHTTP(t, ctx, f.store, id, "copy-authority", input)
				want := 403
				if bound && credentialKind == "matching" {
					want = 200
				}
				if ctx.Err() != nil || status != want {
					t.Fatalf("copy waited for writer: bound=%t status=%d error=%+v context=%v", bound, status, decision, ctx.Err())
				}
				if want == 403 {
					message := "invalid Seat credential"
					if credentialKind == "matching" {
						message = contract.SeatComputerMismatchMessage
					}
					if decision.Code != contract.ErrorForbidden || decision.Message != message {
						t.Fatalf("held-writer refusal=%+v", decision)
					}
				}
			}
		})
	}
}
