package l1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSeatScreenTenureCredentialNoteAndLife(t *testing.T) {
	f, id, secret, copyInput := liveSeatForCopy(t)
	input := SeatScreenAuthorityInput{SeatCopyAuthorityInput: copyInput, Input: true}
	if _, err := f.store.AuthorizeSeatScreen(t.Context(), "fabric-computer-node", id, input); err != nil {
		t.Fatal(err)
	}
	event := SeatLogEvent{SeatID: id, EventID: "take", Kind: SeatEventControlTaken}
	record := SeatScreenEventRequest{SeatCopyAuthorityInput: copyInput, Event: event}
	if _, err := f.store.RecordSeatScreenEvent(t.Context(), "fabric-computer-node", id, record); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AuthorizeSeatScreen(t.Context(), "fabric-computer-node", id, input); errorCode(err) != "person_driving" {
		t.Fatalf("input during tenure: %v", err)
	}
	input.Input = false
	if _, err := f.store.AuthorizeSeatScreen(t.Context(), "fabric-computer-node", id, input); err != nil {
		t.Fatalf("view during tenure: %v", err)
	}
	bad := input
	bad.Credential = "another-seat-credential"
	if _, err := f.store.AuthorizeSeatScreen(t.Context(), "fabric-computer-node", id, bad); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("credential crossover: %v", err)
	}
	f.clock.Advance(31 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	seat, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.State != SeatLive {
		t.Fatalf("person did not keep Seat alive: %+v %v", seat, err)
	}
	record.Event = SeatLogEvent{SeatID: id, EventID: "release", Kind: SeatEventControlReleased, Note: "Browser is ready."}
	if _, err := f.store.RecordSeatScreenEvent(t.Context(), "fabric-computer-node", id, record); err != nil {
		t.Fatal(err)
	}
	input.Input = true
	reply, err := f.store.AuthorizeSeatScreen(t.Context(), "fabric-computer-node", id, input)
	if err != nil || reply.ReleaseNote != "Browser is ready." {
		t.Fatalf("next action note: %+v %v", reply, err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range page.Events {
		if e.Kind == SeatEventControlReleased && e.Note == "Browser is ready." {
			found = true
		}
	}
	if !found {
		t.Fatal("release note absent from Seat log")
	}
	f.clock.Advance(30 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	seat, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.State != SeatEnding {
		t.Fatalf("idle wait after release: %+v %v", seat, err)
	}
}

func TestSeatScreenExpiredCallCommitsEnd(t *testing.T) {
	f, id, _, input := liveSeatForCopy(t)
	f.clock.Advance(31 * time.Minute)
	if _, err := f.store.AuthorizeSeatScreen(t.Context(), "fabric-computer-node", id, SeatScreenAuthorityInput{SeatCopyAuthorityInput: input}); errorCode(err) != "seat_ended" {
		t.Fatalf("expired input=%v", err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("expiry must persist: %+v %v", value, err)
	}
}

func TestSeatScreenTargetIsCredentialBound(t *testing.T) {
	f, id, secret, input := liveSeatForCopy(t)
	ready := true
	endpoint := "ws://screen-node:80/websockify"
	if _, err := f.store.SetAttemptPublication(t.Context(), "fabric-computer-node", input.JobID, input.AttemptID, PublicationRequest{FencingToken: input.FencingToken, Ready: &ready, DisplayEndpoint: &endpoint}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(copyOutcomeFabric{}, f.store, ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/seats/"+id+"/screen-target", nil)
	request.Header.Set(SeatCredentialHeader, secret)
	reply := httptest.NewRecorder()
	server.Handler().ServeHTTP(reply, request)
	var target SeatCopyTarget
	if err = json.Unmarshal(reply.Body.Bytes(), &target); err != nil || reply.Code != 200 || target.Endpoint != endpoint {
		t.Fatalf("screen target=%d %s %v", reply.Code, reply.Body.String(), err)
	}
	foreign := httptest.NewRequest(http.MethodGet, "/v1/seats/seat_00000000000000000000000000000002/screen-target", nil)
	foreign.Header.Set(SeatCredentialHeader, secret)
	reply = httptest.NewRecorder()
	server.Handler().ServeHTTP(reply, foreign)
	if reply.Code != 403 {
		t.Fatalf("foreign target=%d %s", reply.Code, reply.Body.String())
	}
}

func TestSeatScreenInputReceiptAssertsStoredEvent(t *testing.T) {
	f, id, _, input := liveSeatForCopy(t)
	receipt, err := f.store.RecordSeatScreenEvent(t.Context(), "fabric-computer-node", id, SeatScreenEventRequest{SeatCopyAuthorityInput: input, Event: SeatLogEvent{SeatID: id, EventID: "ai-input", Kind: SeatEventScreenInput, Reason: "ai_agent"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Event SeatLogEvent `json:"event"`
	}
	if err = json.Unmarshal(data, &got); err != nil || got.Event.SeatID != id || got.Event.Kind != SeatEventScreenInput || got.Event.OccurredAt.IsZero() {
		t.Fatalf("input receipt=%s %v", data, err)
	}
}

func TestSeatScreenExpiredTakeCommitsEnd(t *testing.T) {
	f, id, secret, input := liveSeatForCopy(t)
	f.clock.Advance(31 * time.Minute)
	_, err := f.store.RecordSeatScreenEvent(t.Context(), "fabric-computer-node", id, SeatScreenEventRequest{SeatCopyAuthorityInput: input, Event: SeatLogEvent{SeatID: id, EventID: "expired-take", Kind: SeatEventControlTaken}})
	if errorCode(err) != "seat_ended" {
		t.Fatalf("expired take=%v", err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding {
		t.Fatalf("expiry must persist after take refusal: %+v %v", value, err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range page.Events {
		if event.EventID == "expired-take" {
			t.Fatal("refused take recorded a successful acquisition")
		}
	}
}
