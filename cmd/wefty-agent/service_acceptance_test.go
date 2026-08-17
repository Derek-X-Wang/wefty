//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceConsoleOutputAttribution(t *testing.T) {
	assertConsoleOutputIsAttributedAndSerialized(t)
}
