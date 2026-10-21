package contract

import "time"

// PeerHealth is a fresh service-owned observation, not scheduling authority.
// ErrorClass contains only a protocol error code, never backend diagnostics.
type PeerHealth struct {
	Reachable  bool      `json:"reachable"`
	ErrorClass string    `json:"error_class"`
	CheckedAt  time.Time `json:"checked_at"`
}

// PeerPong proves that the answering service accepted its configured peer.
type PeerPong struct {
	OK bool `json:"ok"`
}
