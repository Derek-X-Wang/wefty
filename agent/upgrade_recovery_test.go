package agent

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetirementClosurePersistFailureFencesExitWithoutRepeatingL1(t *testing.T) {
	var closes atomic.Int32
	closed := l1.NodeGeneration{AgentGenerationID: "old", BootSessionID: "boot", PromotionOperationID: "promotion", Role: l1.NodeGenerationClosed, ResidentAttempts: []l1.ResidentAttempt{}}
	client := newRoundTripClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/close") {
			closes.Add(1)
			_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{closed}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/generations") {
			_ = json.NewEncoder(w).Encode(l1.NodeGenerationInventory{NodeID: "node", Generations: []l1.NodeGeneration{closed}})
			return
		}
		t.Errorf("unexpected request %s", r.URL.Path)
	}))
	clock := systemClock{}
	s := newAgentSession(client, contract.NodeRegistration{NodeID: "node", AgentGenerationID: "old", BootSessionID: "boot"}, nil, time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 1, 1)
	retiring := closed
	retiring.Role = l1.NodeGenerationRetiring
	s.retiringGeneration = &retiring
	persistErr := errors.New("persist closure")
	writes := 0
	s.retirementClosed = func(g l1.NodeGeneration) error {
		writes++
		if !s.retirementMu.TryLock() {
			t.Fatal("retirement lock held during persistence")
		}
		s.retirementMu.Unlock()
		if g.Role != l1.NodeGenerationClosed {
			t.Fatal("nonpositive closure")
		}
		if writes == 1 {
			return persistErr
		}
		return nil
	}
	ended, err := s.closeRetirement(context.Background())
	if ended || !errors.Is(err, persistErr) {
		t.Fatal("exit crossed failed persistence", ended, err)
	}
	ended, err = s.closeRetirement(context.Background())
	if !ended || err != nil || closes.Load() != 1 || writes != 2 {
		t.Fatal("retry repeated close or failed to persist", ended, err, closes.Load(), writes)
	}
}

func TestUpgradeControlPayloadShapeRefusalAndDecodeFaultStayDistinct(t *testing.T) {
	var request UpgradeServiceQuiescence
	err := decodeUpgradeControlPayload([]byte(`[]`), &request, "quiescence")
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || protocol.APIError.Code != contract.ErrorConflict {
		t.Fatal("well-formed action mismatch is not a refusal", err)
	}
	err = decodeUpgradeControlPayload([]byte(`{`), &request, "quiescence")
	var syntax *json.SyntaxError
	if errors.As(err, &protocol) || !errors.As(err, &syntax) {
		t.Fatal("syntax fault lost its operational cause", err)
	}
}
