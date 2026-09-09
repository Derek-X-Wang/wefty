package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
)

func TestMacBootstrapRemovalPropagatesLocalInspection(t *testing.T) {
	inspectionError := &os.PathError{Op: "stat", Path: "/facts", Err: os.ErrPermission}
	removeError := &os.PathError{Op: "remove", Path: "/facts", Err: os.ErrPermission}
	writerError := errors.New("receipt writer unavailable")
	helperError := errors.New("helper removal incomplete")
	for _, test := range []struct {
		name        string
		removeError error
		statError   error
		writerError error
		helperError error
		wantAbsent  bool
		wantError   bool
	}{
		{name: "inspection_error", statError: inspectionError, wantError: true},
		{name: "verified_absent", statError: os.ErrNotExist, wantAbsent: true},
		{name: "already_absent", removeError: os.ErrNotExist, statError: os.ErrNotExist, wantAbsent: true},
		{name: "remove_error", removeError: removeError, wantError: true},
		{name: "still_present", wantError: true},
		{name: "writer_error", statError: os.ErrNotExist, wantAbsent: true, writerError: writerError, wantError: true},
		{name: "joined_errors", statError: inspectionError, helperError: helperError, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var commands []string
			var output bytes.Buffer
			var removalContext context.Context
			before := time.Now()
			remover := macBootstrapRemover{
				removeUnit: func(ctx context.Context) (limarunner.LaunchDaemonRemovalEvidence, error) {
					removalContext = ctx
					deadline, ok := ctx.Deadline()
					if !ok || deadline.Before(before.Add(5*time.Minute)) || deadline.After(time.Now().Add(5*time.Minute)) {
						t.Errorf("removal deadline changed: %v", deadline)
					}
					commands = append(commands, "unit")
					return limarunner.LaunchDaemonRemovalEvidence{Unloaded: true, PlistAbsent: true}, nil
				},
				removeHelper: func(ctx context.Context, config limarunner.GuestHelperRemovalConfig) (limarunner.GuestHelperRemovalEvidence, error) {
					if ctx != removalContext || config.Instance != "fixture" || config.Limactl != "fake-limactl" {
						t.Errorf("helper invocation changed: %+v", config)
					}
					commands = append(commands, "helper")
					return limarunner.GuestHelperRemovalEvidence{SocketStopped: true, ServiceStopped: true, FilesAbsent: true}, test.helperError
				},
				remove: func(path string) error {
					commands = append(commands, "remove:"+path)
					if path == "/facts" {
						return test.removeError
					}
					return nil
				},
				stat: func(path string) (os.FileInfo, error) {
					commands = append(commands, "stat:"+path)
					if path == "/facts" {
						return nil, test.statError
					}
					return nil, os.ErrNotExist
				},
				output: bootstrapReceiptWriter{output: &output, err: test.writerError},
			}
			err := remover.removeBootstrap("fixture", "fake-limactl", "/facts", "/intent", "/socket", "/config", "/setup")
			if (err != nil) != test.wantError {
				t.Errorf("removal error=%v want_error=%t", err, test.wantError)
			}
			for _, cause := range []error{test.statError, test.removeError, test.writerError, test.helperError} {
				if cause != nil && !errors.Is(cause, os.ErrNotExist) && !errors.Is(err, cause) {
					t.Errorf("lost error cause %v: got %v", cause, err)
				}
			}
			if removalContext.Err() != context.Canceled {
				t.Error("removal context not canceled after return")
			}
			wantCommands := []string{"unit", "helper", "remove:/facts"}
			if test.removeError == nil || errors.Is(test.removeError, os.ErrNotExist) {
				wantCommands = append(wantCommands, "stat:/facts")
			}
			for _, path := range []string{"/intent", "/socket", "/config", "/setup", "/setup.desired"} {
				wantCommands = append(wantCommands, "remove:"+path, "stat:"+path)
			}
			if !reflect.DeepEqual(commands, wantCommands) {
				t.Errorf("operation order=%v want=%v", commands, wantCommands)
			}
			if test.writerError == nil {
				var receipt macBootstrapRemovalEvidence
				if err := json.Unmarshal(output.Bytes(), &receipt); err != nil {
					t.Fatal(err)
				}
				if receipt.FactsAbsent != test.wantAbsent || !receipt.IntentAbsent || !receipt.ControlSocketAbsent || !receipt.NodeConfigAbsent || !receipt.SetupStateAbsent || !receipt.DesiredSetupAbsent || !receipt.Unit.Unloaded || !receipt.Unit.PlistAbsent || !receipt.GuestHelper.FilesAbsent {
					t.Errorf("receipt=%+v want_facts_absent=%t", receipt, test.wantAbsent)
				}
			}
		})
	}
}

type bootstrapReceiptWriter struct {
	output *bytes.Buffer
	err    error
}

func (writer bootstrapReceiptWriter) Write(payload []byte) (int, error) {
	if writer.err != nil {
		return 0, writer.err
	}
	return writer.output.Write(payload)
}
