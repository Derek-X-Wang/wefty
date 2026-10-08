package ocihelper

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestOpenSessionFailuresCarryCapabilityReason(t *testing.T) {
	cause := errors.New("private inventory without transport keywords")
	response := validAcquireResponse("invalid", time.Second)
	response.HelperInstanceID = ""
	preface := validAcquireResponse("helper", time.Second)
	partial := preface
	partial.SessionCapability = "partial"
	drifted := preface
	drifted.SessionCapability = "capability"
	drifted.SessionGeneration = 1
	drifted.HelperInstanceID = "replacement"
	tests := []struct {
		name   string
		client *Client
		want   contract.CapabilityReasonCode
		cause  error
	}{
		{
			name: "dial",
			client: &Client{ExpectedChecksum: "checksum-test", Dial: func(context.Context) (net.Conn, error) {
				return nil, cause
			}},
			want: contract.CapabilityReasonHelperUnreachable, cause: cause,
		},
		{
			name: "connection_deadline",
			client: &Client{ExpectedChecksum: "checksum-test", Dial: func(context.Context) (net.Conn, error) {
				agent, helper := net.Pipe()
				_ = helper.Close()
				return agent, nil
			}},
			want: contract.CapabilityReasonHelperHandshakeFailed,
		},
		{
			name: "receive_preface",
			client: &Client{ExpectedChecksum: "checksum-test", Dial: func(context.Context) (net.Conn, error) {
				agent, helper := net.Pipe()
				go func() {
					defer helper.Close()
					var request frame
					_ = newFramedConn(helper).read(&request)
				}()
				return agent, nil
			}},
			want: contract.CapabilityReasonHelperHandshakeFailed,
		},
		{
			name: "invalid_preface", client: scriptedAcquireClient(t, []scriptedAcquireAttempt{{handshake: response}}),
			want: contract.CapabilityReasonHelperHandshakeFailed,
		},
		{
			name: "partial_authority", client: scriptedAcquireClient(t, []scriptedAcquireAttempt{{handshake: partial}}),
			want: contract.CapabilityReasonHelperHandshakeFailed,
		},
		{
			name: "invalid_admission", client: scriptedAcquireClient(t, []scriptedAcquireAttempt{{handshake: preface, admission: &preface}}),
			want: contract.CapabilityReasonHelperHandshakeFailed,
		},
		{
			name: "changed_facts", client: scriptedAcquireClient(t, []scriptedAcquireAttempt{{handshake: preface, admission: &drifted}}),
			want: contract.CapabilityReasonHelperHandshakeFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session, err := test.client.OpenSession(t.Context(), AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"})
			if session != nil {
				_ = session.Close()
				t.Fatal("failed acquisition returned authority")
			}
			var reason interface {
				CapabilityReasonCode() contract.CapabilityReasonCode
			}
			if !errors.As(err, &reason) || reason.CapabilityReasonCode() != test.want {
				t.Fatalf("acquisition failure has no expected typed reason %q: %v", test.want, err)
			}
			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatal("acquisition failure lost its cause")
			}
		})
	}
}
