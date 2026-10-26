package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSeatSyncProtocolStreamsFullInput(t *testing.T) {
	session, engine, authority := seatExecSession(t)
	engine.seats.entries[1].ready = true
	engine.seats.entries[1].readyGeneration = 1
	data := strings.Repeat("snapshot", 12000)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	request := CopyComputerSeatRequest{Authority: authority, Generation: 1, Place: 1, Direction: "sync", Path: "work"}
	size, err := session.CopyComputerSeat(ctx, request, bytes.NewBufferString(data), nil)
	if err != nil || size != int64(len(data)) {
		t.Fatalf("sync size=%d want=%d err=%v", size, len(data), err)
	}
	request.Path = "../escape"
	if _, err := session.CopyComputerSeat(ctx, request, nil, nil); err == nil {
		t.Fatal("sync accepted another target")
	}
}
func TestSeatCommandWorkingDirectoryProtocol(t *testing.T) {
	session, engine, authority := seatExecSession(t)
	engine.seats.entries[1].ready = true
	engine.seats.entries[1].readyGeneration = 1
	request := ExecComputerSeatRequest{Authority: authority, Generation: 1, Place: 1, Args: []string{"pwd"}, MaxRuntime: time.Minute}
	for _, directory := range []string{"work/pkg", "../outside", "/outside", "work/../outside", "work//pkg"} {
		raw, _ := json.Marshal(request)
		var fields map[string]any
		_ = json.Unmarshal(raw, &fields)
		fields["working_directory"] = directory
		raw, _ = json.Marshal(fields)
		if err := json.Unmarshal(raw, &request); err != nil {
			t.Fatal(err)
		}
		err := session.ExecComputerSeat(t.Context(), request, nil)
		if directory == "work/pkg" {
			if err != nil {
				t.Fatal(err)
			}
			config, err := engine.seats.command(request)
			if err != nil || config.Directory != "/wefty/service/seats/1" {
				t.Fatalf("folder=%s err=%v", config.Directory, err)
			}
		} else if err == nil {
			t.Fatalf("unclean folder accepted: %q", directory)
		}
	}
}
