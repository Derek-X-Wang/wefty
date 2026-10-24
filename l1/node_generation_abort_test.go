package l1

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestStagedGenerationAbortFencesAndReplay(t *testing.T) {
	for _, retirees := range []int{0, 1, 2} {
		t.Run(string(rune('0'+retirees)), func(t *testing.T) {
			f := newGenerationFixture(t)
			for i := 0; i < retirees; i++ {
				f.promote(t, string(rune('2'+i)))
			}
			request := f.prepare(t, "candidate")
			before := f.inventory
			closeRequest := CloseNodeGenerationRequest{BootSessionID: request.CandidateBootSessionID, PromotionOperationID: request.OperationID, ResidentAttempts: []ResidentAttempt{}, RuntimesReaped: true, EvidenceSettled: true, Abort: &StagedGenerationAbort{OperationID: request.OperationID, PrimaryGenerationID: request.ExpectedPrimaryGenerationID, PrimaryBootSessionID: request.ExpectedPrimaryBootSessionID, AuthorityGeneration: request.ExpectedAuthorityGeneration, CatalogRevision: request.ExpectedCatalogRevision, IntentRevision: request.IntentRevision, IntentReason: f.node.IntentReason}}
			for _, field := range []string{"reason", "boot", "revision", "authority", "primary"} {
				bad := closeRequest
				copy := *bad.Abort
				bad.Abort = &copy
				switch field {
				case "reason":
					copy.IntentReason = "other"
				case "boot":
					copy.PrimaryBootSessionID = "other"
				case "revision":
					copy.IntentRevision++
				case "authority":
					copy.AuthorityGeneration++
				case "primary":
					copy.PrimaryGenerationID = "other"
				}
				if _, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "candidate", bad); abortTestErrorCode(err) != contract.ErrorConflict {
					t.Fatal("fence accepted", field, err)
				}
			}
			after, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "candidate", closeRequest)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "candidate", closeRequest)
			if err != nil || !reflect.DeepEqual(after, replay) {
				t.Fatal("replay changed", err)
			}
			changed := closeRequest
			copy := *changed.Abort
			changed.Abort = &copy
			copy.IntentReason = "changed"
			if _, err = f.h.store.CloseNodeGeneration(t.Context(), "agent", "node", "candidate", changed); abortTestErrorCode(err) != contract.ErrorConflict {
				t.Fatal("changed replay accepted", err)
			}
			if after.CatalogRevision != before.CatalogRevision {
				t.Fatal("abort promoted")
			}
			for i, g := range before.Generations {
				if g.AgentGenerationID != "candidate" && !reflect.DeepEqual(g, after.Generations[i]) {
					t.Fatal("abort changed sibling generation")
				}
			}
		})
	}
}

func abortTestErrorCode(err error) contract.ErrorCode {
	var protocol *Error
	if errors.As(err, &protocol) {
		return protocol.Code
	}
	return ""
}
