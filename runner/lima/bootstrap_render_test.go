package lima

import (
	"path/filepath"
	"testing"

	"howett.net/plist"
)

func TestLaunchDaemonRenderUsesClosedThrottledKeepAlive(t *testing.T) {
	root := t.TempDir()
	config := LaunchDaemonConfig{
		AgentPath: filepath.Join(root, "wefty-agent"), Arguments: []string{"--node-id=node"},
		OperatorUser: "operator", Home: root, LimaHome: filepath.Join(root, ".lima"), PATH: DefaultLaunchPATH,
		WorkingDirectory: root, StandardOutPath: filepath.Join(root, "out.log"), StandardErrorPath: filepath.Join(root, "err.log"),
	}
	payload, err := RenderLaunchDaemon(config)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Label     string
		RunAtLoad bool
		KeepAlive struct {
			SuccessfulExit bool
		}
		ThrottleInterval uint64
	}
	if _, err := plist.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Label != LaunchDaemonLabel || !decoded.RunAtLoad || decoded.KeepAlive.SuccessfulExit || decoded.ThrottleInterval != 10 {
		t.Fatalf("LaunchDaemon plist = %+v", decoded)
	}
}
