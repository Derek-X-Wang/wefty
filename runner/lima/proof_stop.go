package lima

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// StopProofUserInstall gracefully stops only the disposable proof host VM.
// Bootstrap has not registered OCI work yet, so no attempt quiescence is needed.
// Do not use Supervisor.Stop here: its disabled-intent recovery stop is forceful.
func StopProofUserInstall(ctx context.Context, c UserInstallConfig) error {
	return stopProofUserInstall(ctx, c, runUserInstallCommand)
}

func stopProofUserInstall(ctx context.Context, c UserInstallConfig, run commandRunner) error {
	if c.Instance != "wefty-proof" || !filepath.IsAbs(c.LimaHome) || filepath.Clean(c.LimaHome) != c.LimaHome || c.LimaHome == string(filepath.Separator) || os.Getenv("LIMA_HOME") != c.LimaHome {
		return errors.New("proof host VM stop requires wefty-proof and its configured Lima home")
	}
	bounded, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if _, err := run(bounded, "limactl", "stop", "wefty-proof"); err != nil {
		return fmt.Errorf("graceful proof host VM stop: %w", err)
	}
	return nil
}
