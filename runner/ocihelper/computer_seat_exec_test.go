package ocihelper

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSeatCommandRequiresCurrentReadySeatAndOwnEnvironment(t *testing.T) {
	s := newComputerSeats(newManualClock(time.Unix(1000, 0)), t.TempDir())
	if _, err := s.apply([]ComputerSeat{{1, "seat1", 7102, 7103}}); err != nil {
		t.Fatal(err)
	}
	request := ExecComputerSeatRequest{Authority: testAuthority(), Generation: 1, Place: 1, Args: []string{"env"}, MaxRuntime: time.Hour}
	checkRefused := func(reason SeatOperationRefusalReason) {
		t.Helper()
		_, err := s.command(request)
		var refusal *SeatOperationRefusalError
		if !errors.As(err, &refusal) || refusal.Reason != reason {
			t.Fatalf("refusal: %v, want %s", err, reason)
		}
	}
	checkRefused("seat_not_ready")
	s.entries[1].ready, s.entries[1].readyGeneration = true, 1
	config, err := s.command(request)
	if err != nil {
		t.Fatal(err)
	}
	if config.UID != 2001 || config.GID != 2001 || config.Directory != "/wefty/service/seats/1" {
		t.Fatalf("wrong identity: %+v", config)
	}
	for _, value := range []string{"HOME=/wefty/service/seats/1/home", "DISPLAY=:1", "XAUTHORITY=/wefty/service/seats/1/.Xauthority", "TMPDIR=/wefty/service/seats/1/tmp", "USER=seat1"} {
		if !slices.Contains(config.Environment, value) {
			t.Errorf("missing %s", value)
		}
	}
	t.Setenv("SEAT_EXEC_MUST_NOT_LEAK", "private")
	config, err = s.command(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range config.Environment {
		if strings.Contains(value, "SEAT_EXEC_MUST_NOT_LEAK") || strings.Contains(value, "seats/2") {
			t.Errorf("environment leaked: %s", value)
		}
	}
	request.Generation = 2
	checkRefused("generation_mismatch")
	request.Generation = 1
	s.entries[1].failure = "desktop_lost"
	checkRefused("seat_not_ready")
	s.entries[1].failure = ""
	s.entries[1].readyGeneration = 0
	checkRefused("seat_not_ready")
	s.entries[1].readyGeneration = 1
	s.entries[1].wanted = false
	checkRefused("seat_not_ready")
	s.entries[1].wanted = true
	s.entries[1].cleaned = true
	checkRefused("seat_not_ready")
	request.Place = 2
	checkRefused("seat_not_ready")
}

func TestSeatCommandExecIDFitsContainerdLimit(t *testing.T) {
	capability, err := randomCapability()
	if err != nil {
		t.Fatal(err)
	}
	id := seatCommandExecID(capability)
	if len(id) > containerdMaxExecIDLength || !strings.HasPrefix(id, "seat-command-") {
		t.Fatalf("exec ID %q has %d characters; containerd allows %d", id, len(id), containerdMaxExecIDLength)
	}
}
