package fabricconfig

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
)

func TestPlainFabricIDIsExplicitAndPrefixValidated(t *testing.T) {
	participant, closeFabric, err := Open(t.Context(), Config{
		Mode: "plain", PlainFabricID: "plain-configured-dev", Identity: fabric.Identity{NodeID: "client"},
	})
	if err != nil || participant == nil || closeFabric == nil {
		t.Fatalf("explicit plain Fabric config = participant=%v close=%v err=%v", participant, closeFabric, err)
	}
	if err := closeFabric(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"fabric-production-shaped", "plain-", " plain-dev"} {
		if _, _, err := Open(t.Context(), Config{Mode: "plain", PlainFabricID: invalid}); err == nil || !strings.Contains(err.Error(), "DEVELOPMENT ONLY") {
			t.Fatalf("invalid plain Fabric ID %q error = %v", invalid, err)
		}
	}
}

// The injected participant stands in for the tsnet status source. Open must
// pass its caller's context before any application client can be constructed.
type contextParticipant struct {
	fabric.Fabric
	verify func(context.Context) error
	closed bool
}

func (p *contextParticipant) VerifyParticipant(ctx context.Context) error { return p.verify(ctx) }
func (p *contextParticipant) Close() error                                { p.closed = true; return nil }

func TestOpenParticipantHonoursCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	participant := &contextParticipant{verify: func(probe context.Context) error {
		cancel()
		select {
		case <-probe.Done():
			return probe.Err()
		case <-time.After(100 * time.Millisecond):
			return errors.New("caller cancellation did not reach participant")
		}
	}}
	_, closeFabric, err := open(ctx, Config{Mode: "tsnet", Name: "wefty://client/test", ExpectedIdentity: &fabric.Identity{NodeID: "saved"}}, func(tsnet.Config) (savedParticipant, error) { return participant, nil })
	if !errors.Is(err, context.Canceled) || closeFabric == nil {
		t.Fatalf("cleanup=%v err=%v", closeFabric, err)
	}
	if err := closeFabric(); err != nil || !participant.closed {
		t.Fatalf("closed=%v err=%v", participant.closed, err)
	}
}

func TestOpenFailedParticipantReturnsCleanupForCaller(t *testing.T) {
	participant := &contextParticipant{verify: func(context.Context) error { return context.Canceled }}
	_, closeFabric, err := open(t.Context(), Config{Mode: "tsnet", Name: "wefty://client/test", ExpectedIdentity: &fabric.Identity{NodeID: "saved"}}, func(tsnet.Config) (savedParticipant, error) { return participant, nil })
	if !errors.Is(err, context.Canceled) || closeFabric == nil || participant.closed {
		t.Fatalf("cleanup must follow reporting: closed=%v cleanup=%v err=%v", participant.closed, closeFabric, err)
	}
	if err := closeFabric(); err != nil || !participant.closed {
		t.Fatalf("closed=%v err=%v", participant.closed, err)
	}
}
