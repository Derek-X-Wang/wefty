//go:build service_acceptance_realtiming

package ocihelper

// SuppressHeartbeatsForAcceptanceBlackhole is the only exported door to the
// client heartbeat blackhole, and it is deliberately unavailable to any build
// that does not carry the service_acceptance_realtiming tag -- that is, to
// every shipped binary. The acceptance lane needs a live client that stops
// sending heartbeats while its control connection stays open, because that is
// the only way to make the real helper prove it reaps a real containerd attempt
// on its own heartbeat deadline rather than on a connection close (#456).
//
// This is not a setting. There is no way to turn it back off, it takes no
// value, and it changes nothing until it is called.
func (session *Session) SuppressHeartbeatsForAcceptanceBlackhole() {
	session.suppressHeartbeats()
}
