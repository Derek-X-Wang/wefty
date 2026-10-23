package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
)

func TestSeatCommandStreamBoundsAndTerminalOrder(t *testing.T) {
	var events []SeatCommandEvent
	writer := newSeatCommandStream(context.Background(), func(e SeatCommandEvent) error { events = append(events, e); return nil })
	code := 7
	for _, event := range []SeatCommandEvent{{Stream: "stdout", Data: []byte("out")}, {Stream: "stderr", Data: []byte("err")}, {Exit: &SeatCommandExit{Code: &code}}} {
		data, _ := json.Marshal(event)
		data = append(data, '\n')
		for _, b := range data {
			if _, err := writer.Write([]byte{b}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.finish(); err != nil || len(events) != 3 || *events[2].Exit.Code != 7 {
		t.Fatalf("events %+v: %v", events, err)
	}
	if _, err := writer.Write([]byte("{}\n")); err == nil {
		t.Fatal("accepted output after exit")
	}
	for _, payload := range [][]byte{bytes.Repeat([]byte("x"), seatCommandFrameBytes+1), []byte(`{"stream":"stdout","data":"` + strings.Repeat("YQ==", SeatCommandChunkBytes) + `"}` + "\n"), []byte("{}\n"), []byte("{\"exit\":{\"code\":0,\"signal\":9}}\n")} {
		w := newSeatCommandStream(context.Background(), func(SeatCommandEvent) error { return nil })
		if _, err := w.Write(payload); err == nil {
			t.Fatal("accepted invalid/oversized frame")
		}
	}
	w := newSeatCommandStream(context.Background(), func(SeatCommandEvent) error { return nil })
	if err := w.finish(); err == nil {
		t.Fatal("missing terminal evidence accepted")
	}
}
func TestSeatCommandStreamConsumerFailureCancelsAndDiscards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	boom := errors.New("consumer gone")
	calls := 0
	w := newSeatCommandStream(ctx, func(SeatCommandEvent) error { calls++; return boom })
	_, err := w.Write([]byte("{\"stream\":\"stdout\",\"data\":\"eA==\"}\n"))
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	cancel()
	if n, err := w.Write(bytes.Repeat([]byte("x"), 1<<20)); err != nil || n != 1<<20 || calls != 1 {
		t.Fatalf("cancelled stream did not discard: %d %v calls=%d", n, err, calls)
	}
}

func TestSeatCommandStreamAppliesBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		stream := newSeatCommandStream(t.Context(), func(SeatCommandEvent) error { close(entered); <-release; return nil })
		go func() { stream.Write([]byte("{\"stream\":\"stdout\",\"data\":\"eA==\"}\n")); close(done) }()
		<-entered
		synctest.Wait()
		select {
		case <-done:
			t.Error("producer ran ahead of slow consumer")
		default:
		}
		close(release)
		<-done
	})
}
