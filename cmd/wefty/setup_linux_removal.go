package main

import (
	"errors"
	"fmt"
	"path/filepath"
)

func refuseLinuxFleetRemoval(host setupHost) error {
	if !filepath.IsAbs(host.home) {
		return errors.New("setup needs an absolute home folder")
	}
	home, err := filepath.EvalSymlinks(host.home)
	if err != nil {
		return err
	}
	paths, issues := resolvePlatformMaintenancePaths(home, host.goos, host.xdgStateHome)
	if len(issues) > 0 {
		return issues[0].err
	}
	if err := refuseRecordedFleetRole(paths, "setup --remove"); err != nil {
		return fmt.Errorf("fleet removal is not available yet on Linux; %w", err)
	}
	if record, err := readFleetSetupRecord(paths); err != nil {
		return err
	} else if record != nil {
		return errors.New("fleet setup is staged; removal is not available yet; resume setup with its recorded role")
	}
	return errors.New("fleet removal is not available yet on Linux; plain setup and removal are Mac-only")
}
