package contract

import "time"

// AllowedAction describes an operator verb against the resource snapshot.
// Requires carries observed revisions and other input requirements; a caller
// must supply those preconditions. Absence of RefusedBecause means the verb is
// legal with those inputs, not that a later write is guaranteed to win.
// Verb and requirement keys are open for reuse by Nodes, services and Computers.
type AllowedAction struct {
	Verb           string         `json:"verb"`
	Requires       map[string]any `json:"requires"`
	RefusedBecause *APIError      `json:"refused_because,omitempty"`
}

// Condition is the last recorded notable fact about a resource. Code and Scope
// are open vocabularies. Since is when the control plane recorded that condition;
// ordinary reads and repeated observations do not advance it. A condition is
// evidence of that event, not a claim that it still holds and never advice.
type Condition struct {
	Code    string         `json:"code"`
	Scope   string         `json:"scope"`
	Since   time.Time      `json:"since"`
	Details map[string]any `json:"details"`
}
