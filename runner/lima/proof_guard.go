package lima

import (
	"context"
	"errors"
	"os"
)

// ProbeProofRecreateGuard exercises the exact multi-line shell transport used
// at the destructive boundary, without deleting or recreating a host VM.
// It is only for the stopped-node-agent, disposable attended proof.
func ProbeProofRecreateGuard(ctx context.Context, c UserInstallConfig) error {
	if c.Instance != "wefty-proof" || os.Getenv("LIMA_HOME") != c.LimaHome {
		return errors.New("recreate quoting probe requires the isolated proof instance and Lima home")
	}
	supervisor, err := NewSupervisor(SupervisorConfig{Instance: c.Instance, Intent: FileIntentSource{Path: c.IntentFile}})
	if err != nil {
		return err
	}
	supervisor.ensureMu.Lock()
	defer supervisor.ensureMu.Unlock()
	defer supervisor.restoreRecreateSocket(ctx)
	return supervisor.guardRecreate(ctx)
}
