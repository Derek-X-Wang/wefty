package lima

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestProofUserInstallStopIsGracefulBoundedAndConfined(t *testing.T) {
	c := UserInstallConfig{Instance: "wefty-proof", LimaHome: filepath.Join(t.TempDir(), "lima")}
	t.Setenv("LIMA_HOME", c.LimaHome)
	failure := errors.New("injected stop failure")
	for _, result := range []error{nil, failure} {
		var commands [][]string
		run := func(ctx context.Context, name string, args ...string) ([]byte, error) {
			commands = append(commands, append([]string{name}, args...))
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) <= 3*time.Minute || time.Until(deadline) > 4*time.Minute {
				t.Fatal("graceful stop lacks its independent four-minute bound")
			}
			return nil, result
		}
		if err := stopProofUserInstall(t.Context(), c, run); !errors.Is(err, result) {
			t.Fatalf("stop error=%v want=%v", err, result)
		}
		if !reflect.DeepEqual(commands, [][]string{{"limactl", "stop", "wefty-proof"}}) {
			t.Fatalf("stop commands=%v", commands)
		}
		commands = nil
		daily := c
		daily.Instance = "wefty-oci"
		if err := stopProofUserInstall(t.Context(), daily, run); err == nil || len(commands) != 0 {
			t.Fatalf("daily instance reached stop: commands=%v err=%v", commands, err)
		}
		t.Setenv("LIMA_HOME", filepath.Join(c.LimaHome, "wrong"))
		if err := stopProofUserInstall(t.Context(), c, run); err == nil || len(commands) != 0 {
			t.Fatalf("wrong home reached stop: commands=%v err=%v", commands, err)
		}
		t.Setenv("LIMA_HOME", c.LimaHome)
	}
}
