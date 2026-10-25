//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceSeatScreenCLI(t *testing.T) {
	t.Run("screen commands, PNG, person tenure, credential boundary and reply metadata", TestSeatScreenCommandsRealBinary)
	t.Run("argument contract", TestSeatScreenArgumentValidation)
	t.Run("text and optional screenshot preserve JSON flags", TestSeatScreenJSONAndTextFlags)
	t.Run("other commands keep their arguments", TestSeatScreenFlagsDoNotRewriteOtherCommands)
}
