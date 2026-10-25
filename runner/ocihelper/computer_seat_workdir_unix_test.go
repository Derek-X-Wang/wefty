//go:build linux || darwin

package ocihelper

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSeatCommandWorkingDirectoryWalkAsSeatUser(t *testing.T) {
	for _, directory := range []string{"work/nested", "work/link", "work/missing", "work/nested/link"} {
		t.Run(directory, func(t *testing.T) {
			base, outside := t.TempDir(), t.TempDir()
			if err := os.MkdirAll(filepath.Join(base, "work/nested"), 0700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"work/link", "work/nested/link"} {
				if err := os.Symlink(outside, filepath.Join(base, name)); err != nil {
					t.Fatal(err)
				}
			}
			marker := filepath.Join(base, "started")
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			cmd := seatWrapperCommand(t, ctx, `import os,sys; assert 'WEFTY_SEAT_WORKING_DIRECTORY' not in os.environ; open(sys.argv[1],'w').write(os.getcwd())`, marker)
			cmd.Dir = base
			cmd.Env = append(cmd.Env, "WEFTY_SEAT_WORKING_DIRECTORY="+directory)
			cmd.WaitDelay = time.Second
			var output bytes.Buffer
			stream := newSeatCommandStream(ctx, func(e SeatCommandEvent) error { output.Write(e.Data); return nil })
			cmd.Stdout = stream
			if err := cmd.Run(); err != nil {
				t.Fatalf("wrapper: %v", err)
			}
			err := stream.finish()
			if directory == "work/nested" {
				if err != nil {
					t.Fatal(err)
				}
				got, e := os.ReadFile(marker)
				if e != nil || string(got) != filepath.Join(base, directory) {
					t.Fatalf("cwd=%q err=%v output=%s", got, e, &output)
				}
				return
			}
			var refusal *SeatOperationRefusalError
			if !errors.As(err, &refusal) || refusal.Reason != "working_directory_unavailable" || !strings.Contains(err.Error(), directory) {
				t.Fatalf("typed refusal=%v output=%s", err, &output)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatal("refused command ran")
			}
		})
	}
}

func TestSeatCommandWorkingDirectoryRefusalPreservesRPCReason(t *testing.T) {
	stream := newSeatCommandStream(t.Context(), nil)
	message := "cannot open working_directory 'work/x'; choose an existing directory without symlinks"
	data, err := marshalSeatCommandEvent(SeatCommandEvent{Refusal: &SeatCommandRefusal{Reason: SeatRefusalWorkingDirectory, Message: message}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(data); err != nil {
		t.Fatal(err)
	}
	refusal := stream.finish()
	if refusal == nil {
		t.Fatal("missing pre-exec refusal")
	}
	helper, client := net.Pipe()
	defer helper.Close()
	defer client.Close()
	deadline := time.Now().Add(3 * time.Second)
	if err := helper.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- writeSeatOperationFailure(newFramedConn(helper), refusal) }()
	var rpc *RPCError
	err = decodeResponse(newFramedConn(client), nil)
	if !errors.As(err, &rpc) || rpc.Code != CodeSeatOperationRefused || rpc.Detail != ErrorDetail(SeatRefusalWorkingDirectory) || !strings.Contains(rpc.Message, message) {
		t.Errorf("RPC lost typed refusal or repair hint: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
