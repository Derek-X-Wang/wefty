//go:build darwin

package process

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLsofListenerVerdictCountsOnlyListenersThatReceiveTheBackend(t *testing.T) {
	backend := serviceBackend{ip: []byte{127, 0, 0, 1}, port: 8080}
	for _, test := range []struct {
		name, output string
		want         serviceListenerVerdict
	}{
		{"nothing", "", serviceListenerUnseen},
		{"payload on loopback", "p10\ng42\nf3\nn127.0.0.1:8080\n", serviceListenerOwned},
		{"payload child on wildcard", "p11\ng42\nf5\nn*:8080\n", serviceListenerOwned},
		{"foreign on loopback", "p10\ng42\nf3\nn127.0.0.1:8080\np20\ng77\nf4\nn127.0.0.1:8080\n", serviceListenerForeign},
		{"foreign reuse-port wildcard", "p10\ng42\nf3\nn127.0.0.1:8080\np20\ng77\nf4\nn*:8080\n", serviceListenerForeign},
		{"foreign only on ::1", "p10\ng42\nf3\nn127.0.0.1:8080\np20\ng77\nf4\nn[::1]:8080\n", serviceListenerOwned},
		{"foreign only on another interface", "p20\ng77\nf4\nn192.168.1.9:8080\n", serviceListenerUnseen},
		{"foreign on v4-mapped loopback", "p20\ng77\nf4\nn[::ffff:127.0.0.1]:8080\n", serviceListenerForeign},
	} {
		t.Run(test.name, func(t *testing.T) {
			verdict, err := lsofListenerVerdict([]byte(test.output), backend, 42)
			if err != nil || verdict != test.want {
				t.Fatalf("verdict = (%d, %v), want %d", verdict, err, test.want)
			}
		})
	}
	if _, err := lsofListenerVerdict([]byte("p10\ng42\nnnot-an-address\n"), backend, 42); err == nil {
		t.Fatal("a malformed listener name was accepted")
	}
}

func TestLsofFailureIsAnInspectionErrorAndNoMatchIsUnseen(t *testing.T) {
	directory := t.TempDir()
	stand := func(name, script string) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		return path
	}
	prior := lsofPath
	t.Cleanup(func() { lsofPath = prior })
	backend := serviceBackend{ip: []byte{127, 0, 0, 1}, port: 8080}

	lsofPath = stand("broken", "echo 'lsof: cannot read kernel' >&2; exit 2")
	if _, err := inspectServiceListeners(backend, 42); err == nil {
		t.Fatal("a failing lsof was read as a verdict")
	}
	lsofPath = stand("partial", "echo p10; exit 1")
	if _, err := inspectServiceListeners(backend, 42); err == nil {
		t.Fatal("lsof exit 1 with output was read as a verdict")
	}
	lsofPath = filepath.Join(directory, "missing")
	if _, err := inspectServiceListeners(backend, 42); err == nil {
		t.Fatal("a missing lsof was read as a verdict")
	}
	lsofPath = stand("none", "exit 1")
	if verdict, err := inspectServiceListeners(backend, 42); err != nil || verdict != serviceListenerUnseen {
		t.Fatalf("lsof with no match = (%d, %v), want unseen", verdict, err)
	}
}
