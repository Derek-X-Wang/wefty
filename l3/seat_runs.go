package l3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/Derek-X-Wang/wefty/contract"
)

type seatRunResolver interface {
	ResolveSeatRun(context.Context, string, string) (contract.SeatRunBinding, error)
}
type seatCredentialContextKey struct{}

func (c *L1Client) ResolveSeatRun(ctx context.Context, id, credential string) (binding contract.SeatRunBinding, err error) {
	ctx = context.WithValue(ctx, seatCredentialContextKey{}, credential)
	err = c.do(ctx, http.MethodGet, "/v1/seats/"+url.PathEscape(id)+"/run-target", nil, &binding, http.StatusOK)
	if err == nil && (binding.SeatID != id || binding.NodeID == "" || binding.ComputerID == "" || binding.ComputerJobID == "" || binding.ComputerAttemptID == "") {
		err = invalidL1Success(http.MethodGet, "/v1/seats/{seat_id}/run-target", "Seat Run binding")
	}
	return
}

// The ordinary inline Run is a transport for the node's Seat runtime. Its
// fail-closed script cannot execute the requested command on an older node.
const seatRunScript = "#!/bin/sh\n# wefty:seat-run:v1\necho 'Seat Runs require a newer node agent' >&2\nexit 125\n"

func normalizeSeatRun(input *CreateRunInput) error {
	r := &input.Request
	if r.SeatRun == nil {
		if _, present, err := contract.ParseSeatRunParams(r.Params); present || err != nil {
			return protocolError(contract.ErrorForbidden, "Seat Run params require the seat_run submission source and Seat credential")
		}
		return nil
	}
	if !input.FabricClient || input.ComputerScope != nil || r.ParentRunID != "" || input.SeatRunBinding == nil {
		return protocolError(contract.ErrorForbidden, "Seat Runs require a Fabric client and Seat credential")
	}
	paramsAllowed := len(r.Params) == 0
	if !paramsAllowed {
		normalized, err := canonicalObject(r.Params, true, "params")
		paramsAllowed = err == nil && string(normalized) == "{}"
	}
	if r.InlineScript != nil || r.Image != nil || r.WorkflowRef != "" || r.Restartable || r.DispatchAuthority || r.RequiredEnvelope || len(r.EnvelopeSchema) > 0 || !paramsAllowed {
		return protocolError(contract.ErrorInvalidRequest, "Seat Runs accept only seat_run, tags and a command deadline")
	}
	value := *r.SeatRun
	if r.Limits != nil {
		if r.Limits.MaxRuntimeSeconds <= 0 {
			return protocolError(contract.ErrorInvalidRequest, "Seat Run requires a positive finite max_runtime_seconds")
		}
		if r.Limits.MaxRuntimeSeconds != 0 {
			if value.MaxRuntimeSeconds != 0 && value.MaxRuntimeSeconds != int64(r.Limits.MaxRuntimeSeconds) {
				return protocolError(contract.ErrorInvalidRequest, "Seat Run deadlines disagree")
			}
			value.MaxRuntimeSeconds = int64(r.Limits.MaxRuntimeSeconds)
		}
		if r.Limits.MaxCost != 0 {
			return protocolError(contract.ErrorInvalidRequest, "Seat Runs do not accept max_cost")
		}
	}
	if value.MaxRuntimeSeconds == 0 {
		value.MaxRuntimeSeconds = input.SeatRunBinding.DefaultMaxRuntimeSeconds
	}
	if err := value.Normalize(); err != nil {
		return protocolError(contract.ErrorInvalidRequest, "%v", err)
	}
	if input.SeatRunBinding.SeatID != value.SeatID {
		return protocolError(contract.ErrorForbidden, "Seat binding mismatch")
	}
	params := contract.SeatRunParams{Version: 1, SeatRun: value, Binding: *input.SeatRunBinding}
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	if len(raw) > MaxDispatchedRunParamsBytes {
		return protocolError(contract.ErrorInvalidRequest, "Seat Run argv exceeds the dispatched params limit")
	}
	r.Params = raw
	digest := sha256.Sum256([]byte(seatRunScript))
	r.InlineScript = &InlineScriptInput{Content: seatRunScript, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}
	r.Tags = append(r.Tags, contract.StableNodeTagPrefix+params.Binding.NodeID)
	r.Limits = &contract.RunLimits{MaxRuntimeSeconds: int(value.MaxRuntimeSeconds)}
	r.SeatRun = nil
	return nil
}
