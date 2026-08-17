//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceNodeCLIIntentAndCapacity(t *testing.T) {
	assertNodeCLIIntentAndCapacity(t)
}
