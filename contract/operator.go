package contract

import "time"

// AllowedAction describes a resource verb evaluated for the authenticated actor.
// Requires contains only exact preconditions keyed by literal request fields;
// callers copy those values into the body (for example intent_revision: 7).
// Inputs separately describes caller-chosen fields. A field never appears in
// both. Empty Requires and Inputs are omitted, never marshaled as null.
// RefusedBecause is omitted only when the decision succeeds with valid inputs;
// it has the same error conversion and retryability as the enforcing write.
// A later write rechecks the same actor-aware predicates atomically.
// Nodes, services and Computers reuse this shape unchanged.
type AllowedAction struct {
	Verb           string         `json:"verb"`
	Requires       map[string]any `json:"requires,omitempty"`
	Inputs         []ActionInput  `json:"inputs,omitempty"`
	RefusedBecause *APIError      `json:"refused_because,omitempty"`
}

// ActionInput names a caller-chosen field by its literal request JSON name.
// Type is its JSON type: string, boolean, integer, number, object or array.
// Required indicates presence, never a required value. Endpoint schemas supply
// further constraints (for example a Node reason must be nonempty after trim).
type ActionInput struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
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
