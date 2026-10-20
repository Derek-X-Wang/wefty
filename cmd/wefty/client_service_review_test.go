package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/local"
)

func TestClientServiceReviewErrorSemantics(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		exit    int
		code    contract.ErrorCode
		retry   bool
		message string
	}{
		{"owner-action", &local.UnavailableError{Cause: syscall.ENOENT}, 13, "fabric_unreachable", false, "owner"},
		{"busy", &local.BusyError{}, 13, "client_service_busy", true, "busy"},
		{"socket-owner", &local.AlreadyRunningError{}, 5, "conflict", false, "already running"},
		{"destination-policy", &fabric.DestinationRefusedError{}, 3, "forbidden", false, "Fabric"},
		{"server-peer", &local.PeerIdentityError{}, 3, "forbidden", false, "owner"},
		{"fenced", &local.FencedCallerError{Cause: syscall.EPERM}, 3, "forbidden", false, "do not look for another way in"},
		{"rollback-cause", errors.Join(errors.New("granted participant tags do not match the role"), &fabric.ParticipantInUseError{Participant: "client"}), 3, "participant_in_use", false, "granted participant tags"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := decideCommandReport(tc.err)
			response, ok := report.errorDocument.(contract.ErrorResponse)
			if !ok || report.exitCode != tc.exit || response.Error.Code != tc.code || response.Error.Retryable != tc.retry || !strings.Contains(response.Error.Message, tc.message) {
				t.Fatalf("report=%+v", report)
			}
		})
	}
}
func TestParticipantFenceAdviceSurvivesHTTPAdapter(t *testing.T) {
	for _, failure := range []error{&local.FencedCallerError{Cause: syscall.EPERM}, &local.UnavailableError{Cause: syscall.EPERM}} {
		clients, err := newAPIClients(exchangeTestFabric{dial: func(context.Context, string, string) (net.Conn, error) { return nil, failure }}, "127.0.0.1:47101", "")
		if err != nil {
			t.Fatal(err)
		}
		_, err = clients.listNodes(t.Context())
		clients.close()
		var output bytes.Buffer
		writeCommandError(&output, err, false)
		if !strings.Contains(output.String(), "ask the owner") || strings.Contains(output.String(), "allow") {
			t.Fatalf("unsafe advice: %s", output.String())
		}
	}
}
func TestClientSocketIsIsolatedByTestMain(t *testing.T) {
	if packageTestSocket == "" || os.Getenv(local.SocketEnvironment) != packageTestSocket {
		t.Fatal("client socket is not isolated")
	}
	pinned := false
	for _, entry := range packageTestEnvironment {
		if entry == local.SocketEnvironment+"="+packageTestSocket {
			pinned = true
		}
	}
	if !pinned {
		t.Fatal("shared subprocess environment still carries the owner's socket")
	}
}
