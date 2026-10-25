package main

import (
	"reflect"
	"testing"
)

func TestSeatScreenArgumentValidation(t *testing.T) {
	id := "seat_0123456789abcdef0123456789abcdef"
	for _, args := range [][]string{
		{"click", id, "0", "0"}, {"move", id, "1279", "799"}, {"drag", id, "0", "1", "2", "3"},
		{"scroll", id, "4", "5", "--dy", "-1"}, {"type", id, "hello --json"}, {"key", id, "shift+Tab"},
		{"screenshot", id, "-o", "--json"}, {"click", id, "1", "2", "--screenshot=--json"},
	} {
		if err := validateSeatArgs(args); err != nil {
			t.Errorf("valid %q: %v", args, err)
		}
	}
	for _, args := range [][]string{
		{"click", id, "1280", "0"}, {"click", id, "0", "800"}, {"move", id, "-1", "0"}, {"drag", id, "1", "2", "3"},
		{"scroll", id, "1", "2"}, {"scroll", id, "1", "2", "--dy", "no"}, {"scroll", id, "1", "2", "--dy", "10001"},
		{"key", id, "super+Unknown"}, {"key", id, "ctrl+"}, {"type", id, string([]byte{0xff})},
		{"click", id, "1", "2", "--button", "middle"}, {"screenshot", "job_0123456789abcdef0123456789abcdef"},
		{"move", id, "1", "2", "--button", "right"}, {"screenshot", id, "--screenshot"},
	} {
		if err := validateSeatArgs(args); err == nil {
			t.Errorf("accepted invalid %q", args)
		}
	}
}

func TestSeatScreenJSONAndTextFlags(t *testing.T) {
	id := "seat_0123456789abcdef0123456789abcdef"
	for _, text := range []string{"--json", "--json=false", "--screenshot", "--help"} {
		args := []string{"seat", "type", id, text, "--screenshot", "--json"}
		normalized := normalizeSeatScreenArgs(args)
		filtered, jsonOutput, err := removeBoolFlag(normalized, "--json")
		if err != nil || !jsonOutput || len(filtered) < 4 || filtered[3] != text {
			t.Errorf("text %q changed: %q json=%v err=%v", text, filtered, jsonOutput, err)
		}
		if help, _ := seatScreenWantsHelp(filtered); help {
			t.Errorf("text %q became help", text)
		}
	}
}

func TestSeatScreenFlagsDoNotRewriteOtherCommands(t *testing.T) {
	for _, args := range [][]string{
		{"exec", "--", "seat", "click", "seat_0123456789abcdef0123456789abcdef", "1", "2", "--screenshot", "result.png"},
		{"submit", "--argv", "seat", "click", "--screenshot", "result.png"},
		{"--l1", "seat", "click", "--screenshot", "result.png"},
		{"seat", "screenshot", "seat_0123456789abcdef0123456789abcdef", "-o", "--screenshot"},
	} {
		got := normalizeSeatScreenArgs(args)
		if !reflect.DeepEqual(got, args) {
			t.Errorf("unrelated arguments rewritten: got %q want %q", got, args)
		}
	}
}

func TestSeatScreenFlagsBeforeLiteralTextAndSeparator(t *testing.T) {
	id := "seat_0123456789abcdef0123456789abcdef"
	for _, args := range [][]string{
		{"type", "--screenshot", id, "--help"},
		{"type", "--screenshot", "--", id, "--json"},
		{"type", "--screenshot=capture.png", "--", id, "--screenshot"},
		{"click", "--button", "right", "--screenshot", id, "12", "34"},
		{"screenshot", "-o", "--json", id},
	} {
		parsed, err := parseSeatScreenArgs(args)
		if err != nil {
			t.Errorf("flags before literal arguments %q: %v", args, err)
			continue
		}
		if parsed.id != id {
			t.Fatalf("Seat ID changed: %+v", parsed)
		}
		if args[0] == "type" {
			want := args[len(args)-1]
			if parsed.text != want {
				t.Errorf("TEXT changed: %q want %q", parsed.text, want)
			}
		}
		normalized := normalizeSeatScreenArgs(append([]string{"seat"}, args...))
		if help, _ := seatScreenWantsHelp(normalized); help {
			t.Errorf("literal TEXT became help: %q", normalized)
		}
		filtered, jsonFlag, err := removeBoolFlag(normalized, "--json")
		if err != nil || jsonFlag || len(filtered) == 0 {
			t.Errorf("literal TEXT became JSON: %q json=%v err=%v", filtered, jsonFlag, err)
		}
	}
}
