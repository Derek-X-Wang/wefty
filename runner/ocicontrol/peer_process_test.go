package ocicontrol

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestOperatorAncestryWalkDoesNotForkOrTimeOut(t *testing.T) {
	calls := 0
	err := operatorPeerError(10000, 42, func(pid int) (int, error) {
		calls++
		if pid == 5000 {
			return 1, nil
		}
		return pid - 1, nil
	})
	if err != nil || calls != 5001 {
		t.Fatalf("deep owner ancestry: lookups=%d err=%v", calls, err)
	}
	for _, row := range []struct {
		name   string
		lookup func(int) (int, error)
		want   string
	}{
		{"descendant", func(int) (int, error) { return os.Getpid(), nil }, "runs under the node agent"},
		{"lookup error", func(int) (int, error) { return 0, errors.New("denied") }, "cannot verify"},
	} {
		t.Run(row.name, func(t *testing.T) {
			err := operatorPeerError(10000, os.Getpid(), row.lookup)
			if err == nil || !strings.Contains(err.Error(), row.want) || !strings.Contains(err.Error(), "owner's own shell") {
				t.Fatalf("refusal=%v", err)
			}
		})
	}
}
