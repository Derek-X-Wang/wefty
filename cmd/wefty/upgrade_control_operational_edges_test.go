package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestUpgradeControlOperationalCauseJoinedWithL1RemainsOperational(t *testing.T) {
	f := newRollingFixture(t)
	cause := errors.Join(&agent.ProtocolError{StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict, Message: "prior lease refusal"}}, io.ErrUnexpectedEOF)
	failure := &agent.UpgradeServiceQuiescenceError{JobID: "job", AttemptID: "attempt", Phase: "lease", Cause: cause}
	r := agentinstall.ControlRequest{ID: "operation", RequestID: "joined", NodeID: f.node.NodeID, GenerationID: f.primary.ID, BootSessionID: f.node.BootSessionID, Action: "quiesce_services"}
	mustRolling(t, f.install.PublishControl(agent.NewUpgradeControlResult(r, nil, failure)))
	_, err := requestGenerationControl(t.Context(), f.install, r)
	assertUpgradeControlOperational(t, err, "job", "attempt", "lease", "prior lease refusal", "unexpected EOF")
}

func TestUpgradeControlMissingReplyIdentityIsOperational(t *testing.T) {
	f := newRollingFixture(t)
	p, err := connprofile.Read(f.paths.profile)
	mustRolling(t, err)
	op := upgradeOperations(f.host)
	op.generations = nil
	var payload json.RawMessage
	op.generationControl = func(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (json.RawMessage, error) {
		r.RequestID = string(payload)
		mustRolling(t, store.PublishControl(agent.NewUpgradeControlResult(r, payload, nil)))
		return requestGenerationControl(ctx, store, r)
	}
	host := f.host
	host.upgrade = &op
	op = upgradeOperations(host)
	for _, body := range []string{"null", "{}"} {
		payload = []byte(body)
		_, err = op.generations(t.Context(), p)
		assertUpgradeControlOperational(t, err)
	}
}
