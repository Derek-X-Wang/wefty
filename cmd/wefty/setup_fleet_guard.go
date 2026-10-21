package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// refuseRecordedFleetRole fences legacy upgrade/removal before locks, recovery,
// staging or service-manager calls. Participant shape never determines role.
func refuseRecordedFleetRole(paths setupPaths, command string) error {
	if _, err := os.Lstat(filepath.Join(paths.config, plainConversionName)); err == nil {
		return fmt.Errorf("%s refuses a plain-to-join conversion; resume setup with --role join --convert-plain", command)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	payload, err := root.ReadFile(filepath.Join(paths.configRel, setupManifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s cannot verify the installation role: %w", command, err)
	}
	var metadata map[string]json.RawMessage
	if json.Unmarshal(payload, &metadata) != nil {
		// Preserve the existing legacy receipt repair behavior.
		return nil
	}
	raw, exists := metadata["role"]
	if !exists {
		return nil
	}
	var role string
	if json.Unmarshal(raw, &role) != nil {
		return fmt.Errorf("%s refuses invalid recorded fleet role metadata; repair the installation receipt before %s", command, command)
	}
	if role != "" {
		return fmt.Errorf("%s refuses recorded fleet role %q; use the recorded role’s supported maintenance path for %s", command, role, command)
	}
	return nil
}
