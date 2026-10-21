package fleetjoin

import (
	"strings"
	"testing"
)

func TestCodeRoundTripAndTypoRefusal(t *testing.T) {
	for _, coordinator := range []string{"", "https://coordinator.example.test"} {
		want := Descriptor{L1: "opaque-control", L3: "opaque-ledger", CoordinatorURL: coordinator}
		code, err := Encode(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Parse(code)
		if err != nil || got != want {
			t.Fatalf("roundtrip: %+v %v", got, err)
		}
		for i := 4; i < len(code); i++ {
			replacement := byte('a')
			if code[i] == replacement {
				replacement = 'b'
			}
			typo := code[:i] + string(replacement) + code[i+1:]
			if _, err := Parse(typo); err == nil {
				t.Fatalf("accepted typo at %d", i)
			}
		}
	}
	for _, value := range []Descriptor{{}, {L1: "wefty://control-plane", L3: "opaque-l3"}, {L1: "same", L3: "same"}, {L1: "opaque-l1", L3: "opaque-l3", CoordinatorURL: "https://user:secret@example.test"}, {L1: "opaque-l1", L3: "opaque-l3", CoordinatorURL: "https://example.test?key=secret"}} {
		if code, err := Encode(value); err == nil || strings.Contains(code, "secret") {
			t.Fatalf("invalid descriptor: %q %v", code, err)
		}
	}
	for _, code := range []string{"", "wj2_aaaa.12345678", strings.Repeat("a", 4097)} {
		if _, err := Parse(code); err == nil {
			t.Fatal("accepted malformed code")
		}
	}
}

// This vector was calculated independently with Python's base64 and zlib.
func TestCodeKnownVersionOneVector(t *testing.T) {
	const code = "wj1_WyJsMSIsImwzIl0.c5b49a3e"
	got, err := Parse(code)
	if err != nil || got != (Descriptor{L1: "l1", L3: "l3"}) {
		t.Fatalf("known code: %+v %v", got, err)
	}
	encoded, err := Encode(Descriptor{L1: "l1", L3: "l3"})
	if err != nil || encoded != code {
		t.Fatalf("known encoding: %s %v", encoded, err)
	}
}
