package agentinstall

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// ControlRequestRefusal distinguishes an invalid envelope or unknown owned
// generation from filesystem/transport failures. The CLI retains conflict exit
// semantics for these local preflight refusals without reclassifying I/O.
type ControlRequestRefusal struct{ Message string }

func (e *ControlRequestRefusal) Error() string { return e.Message }

// ControlRequest is an owner-private installed-process request. It grants no L1
// authority. The runtime checks the exact generation/boot and operation again.
type ControlRequest struct {
	ID            string          `json:"id"`
	RequestID     string          `json:"request_id"`
	NodeID        string          `json:"node_id"`
	GenerationID  string          `json:"agent_generation_id"`
	BootSessionID string          `json:"boot_session_id"`
	Action        string          `json:"action"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// ControlServiceFailure retains Attempt diagnostics independently of whether
// its cause is an explicit refusal, an L1 protocol error or an operational fault.
type ControlServiceFailure struct {
	JobID     string `json:"job_id"`
	AttemptID string `json:"attempt_id"`
	Phase     string `json:"phase"`
	Pending   bool   `json:"pending"`
}
type ControlResult struct {
	ServiceFailure *ControlServiceFailure `json:"service_failure,omitempty"`
	APIError       *contract.APIError     `json:"api_error,omitempty"`
	StatusCode     int                    `json:"status_code,omitempty"`
	Request        ControlRequest         `json:"request"`
	Payload        json.RawMessage        `json:"payload,omitempty"`
	Error          string                 `json:"error,omitempty"`
}

func (s Store) control(id, name string, value any, write bool) error {
	if !validID(id) {
		return &ControlRequestRefusal{Message: "invalid control generation"}
	}
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	if _, ok := c.Generations[id]; !ok {
		return &ControlRequestRefusal{Message: "unknown control generation"}
	}
	if err := privateDirectory(filepath.Join(s.Root(), id)); err != nil {
		return err
	}
	name = filepath.Join(id, name)
	if write {
		if filepath.Base(name) == "control-result.json" {
			var previous ControlResult
			if err := readPrivate(root, name, &previous); err == nil {
				next := value.(ControlResult)
				// A generation can promote only once. Keep its successful
				// operation/request/result as durable evidence after demotion.
				// Refusals use a separate reply slot and cannot erase it.
				if previous.SuccessfulPromotion() && (!next.SuccessfulPromotion() || !SamePromotionControl(previous.Request, next.Request)) {
					name = filepath.Join(id, "control-refusal.json")
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		return writePrivate(root, name, value)
	}
	return readPendingRecord(root, name, value)
}
func (s Store) RequestControl(r ControlRequest) error {
	if r.ID == "" || r.NodeID == "" || r.BootSessionID == "" || !validControlAction(r.Action) {
		return &ControlRequestRefusal{Message: "invalid generation control request"}
	}
	return s.control(r.GenerationID, "control-request.json", r, true)
}
func (s Store) ReadControl(id string) (r ControlRequest, err error) {
	err = s.control(id, "control-request.json", &r, false)
	return
}
func (s Store) PublishControl(r ControlResult) error {
	return s.control(r.Request.GenerationID, "control-result.json", r, true)
}
func (s Store) ReadControlResult(id string) (r ControlResult, err error) {
	err = s.control(id, "control-result.json", &r, false)
	return
}
func (s Store) PublishRetirement(g l1.NodeGeneration) error {
	return s.control(g.AgentGenerationID, "retirement-ack.json", g, true)
}
func (s Store) ReadRetirement(id string) (g l1.NodeGeneration, err error) {
	err = s.readRetainedEvidence(id, "retirement-ack.json", &g)
	return
}

// This is a closed protocol, never an arbitrary node-authenticated HTTP proxy.
func validControlAction(action string) bool {
	switch action {
	case "quiesce_services", "resume_services", "generation_inventory", "stage_generation", "promote_generation", "abort_staged_generation", "de_enroll_generation":
		return true
	default:
		return false
	}
}

// SuccessfulPromotion distinguishes committed activation evidence from a
// transport refusal. The OS account remains the trust boundary for this file.
func (r ControlResult) SuccessfulPromotion() bool {
	return r.Request.Action == "promote_generation" && r.Error == "" && len(r.Payload) != 0
}

// SamePromotionControl ignores transport correlation, but binds the operation,
// node, predecessor boot and the canonical L1 promotion request. L1 treats
// resident/cohort order and empty versus omitted inventories identically.
func SamePromotionControl(a, b ControlRequest) bool {
	canonical := func(r ControlRequest) (ControlRequest, error) {
		if r.Action != "promote_generation" {
			return r, errors.New("not a promotion")
		}
		var request l1.PromoteNodeGenerationRequest
		if err := json.Unmarshal(r.Payload, &request); err != nil {
			return r, err
		}
		if request.OperationID == "" || request.OperationID != r.ID {
			return r, errors.New("promotion operation mismatch")
		}
		request.RetiringGenerationIDs = append([]string{}, request.RetiringGenerationIDs...)
		slices.Sort(request.RetiringGenerationIDs)
		request.ResidentAttempts = append([]l1.ResidentAttempt{}, request.ResidentAttempts...)
		slices.SortFunc(request.ResidentAttempts, func(a, b l1.ResidentAttempt) int { return strings.Compare(a.AttemptID, b.AttemptID) })
		var err error
		r.Payload, err = json.Marshal(request)
		r.RequestID = ""
		return r, err
	}
	a, err := canonical(a)
	if err != nil {
		return false
	}
	b, err = canonical(b)
	return err == nil && reflect.DeepEqual(a, b)
}

// ReadControlReply correlates a refusal without replacing successful promotion
// evidence. Both slots remain receipt-bound during finished-generation cleanup.
func (s Store) ReadControlReply(request ControlRequest) (ControlResult, error) {
	var refusal ControlResult
	err := s.control(request.GenerationID, "control-refusal.json", &refusal, false)
	if err == nil && reflect.DeepEqual(refusal.Request, request) {
		return refusal, nil
	}
	var pending *RecordPendingError
	if err != nil && !errors.As(err, &pending) {
		return refusal, err
	}
	return s.ReadControlResult(request.GenerationID)
}

// ReadPromotionResult keeps reply-loss resolution available after receipt-bound
// retirement cleanup. The audit copy is not evidence of a surviving process.
func (s Store) ReadPromotionResult(id string) (result ControlResult, err error) {
	err = s.readRetainedEvidence(id, "control-result.json", &result)
	return
}

func (s Store) readRetainedEvidence(id, name string, target any) error {
	if !validID(id) {
		return &ControlRequestRefusal{Message: "invalid evidence generation"}
	}
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	local, present := c.Generations[id]
	var missing error
	if present {
		dirErr := privateDirectory(filepath.Join(s.Root(), id))
		if dirErr == nil {
			err = readPrivate(root, filepath.Join(id, name), target)
			if err == nil {
				return nil
			}
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			missing = &RecordPendingError{Cause: err}
		} else if !errors.Is(dirErr, os.ErrNotExist) {
			return dirErr
		} else {
			missing = dirErr
		}
	}
	var receipt completionReceipt
	if err = readPrivate(root, "finished-"+id+".json", &receipt); err != nil {
		if errors.Is(err, os.ErrNotExist) && missing != nil {
			return missing
		}
		return err
	}
	if receipt.Version != 1 || receipt.Generation.ID != id || (present && !sameGeneration(local, receipt.Generation)) {
		return &VerificationRefusal{Message: "retained evidence receipt ownership differs"}
	}
	payload, ok := receipt.ControlFiles[name]
	if !ok {
		if missing != nil {
			return missing
		}
		return &VerificationRefusal{Message: "completed generation lacks required retained evidence"}
	}
	return json.Unmarshal(payload, target)
}

// RetainPromotionResult records startup reconciliation without replacing an
// already-published transport-correlated success from the predecessor.
func (s Store) RetainPromotionResult(result ControlResult) error {
	request := result.Request
	if !result.SuccessfulPromotion() || !validID(request.GenerationID) {
		return &VerificationRefusal{Message: "invalid retained promotion result"}
	}
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	if c.NodeID != request.NodeID {
		return &VerificationRefusal{Message: "retained promotion node differs"}
	}
	if _, ok := c.Generations[request.GenerationID]; !ok {
		return &VerificationRefusal{Message: "retained promotion predecessor missing"}
	}
	if err := privateDirectory(filepath.Join(s.Root(), request.GenerationID)); err != nil {
		return err
	}
	var previous ControlResult
	name := filepath.Join(request.GenerationID, "control-result.json")
	if err = readPrivate(root, name, &previous); err == nil && previous.SuccessfulPromotion() {
		if !SamePromotionControl(previous.Request, request) {
			return &VerificationRefusal{Message: "retained promotion payload changed"}
		}
		return nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return writePrivate(root, name, result)
}

type StagedAbortControl struct {
	CandidateGenerationID string                        `json:"candidate_generation_id"`
	Request               l1.CloseNodeGenerationRequest `json:"request"`
}
