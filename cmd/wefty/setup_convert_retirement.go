package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
)

// Completion is proved by the published profile, matching receipt and committed
// join record. History contents are evidence only and are never opened here.
func plainConversionCompleted(paths setupPaths, conversion *plainConversion) (bool, error) {
	if conversion == nil || conversion.Phase != "joining" {
		return false, nil
	}
	stage, err := readFleetSetupRecord(paths)
	if err != nil {
		return false, err
	}
	if stage == nil || !stage.ProfilePublished || !stage.Committed {
		return false, nil
	}
	profile, receipt, err := readFleetUpgradeInstall(paths, serviceplan.Join)
	if err != nil {
		return false, err
	}
	expected, _ := json.Marshal(conversion.History)
	actual, _ := json.Marshal(receipt.History)
	if conversion.NodeID != profile.Identities.Node || !bytes.Equal(expected, actual) {
		return false, errors.New("completed conversion disagrees with its committed join receipt")
	}
	return true, nil
}

func retireCompletedPlainConversion(paths setupPaths) (resultErr error) {
	conversion, err := readPlainConversion(paths)
	if err != nil || conversion == nil {
		return err
	}
	completed, err := plainConversionCompleted(paths, conversion)
	if err != nil {
		return err
	}
	if !completed {
		return errors.New("plain conversion is incomplete; resume setup --role join --convert-plain")
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	stage, err := readFleetSetupRecord(paths)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(conversion.History)
	if err != nil {
		return err
	}
	stage.ConversionHistoryDigest = payloadDigest(metadata)
	stagePayload, err := json.MarshalIndent(stage, "", "  ")
	if err != nil {
		return err
	}
	config, err := root.OpenRoot(paths.configRel)
	if err != nil {
		return err
	}
	err = durable.WriteFile(config, fleetSetupRecordName, append(stagePayload, '\n'), 0600)
	config.Close()
	if err != nil {
		return err
	}
	history := filepath.Join(paths.dataRel, plainHistoryFolder, conversion.NodeID)
	// Never chmod through a link, even one targeting another folder in this home.
	for name := history; name != "."; name = filepath.Dir(name) {
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("conversion history path must contain only real directories: %s", name)
		}
	}
	// Published history may have been archived or deleted by its owner. Recreate
	// only the directory for this small record; never reconstruct historical data.
	if err := durable.MkdirAll(paths.home, history, 0700); err != nil {
		return err
	}
	if err := root.Chmod(history, 0700); err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, root.Chmod(history, 0500), syncConversionFolder(root, history))
	}()
	folder, err := root.OpenRoot(history)
	if err != nil {
		return err
	}
	defer folder.Close()
	payload, err := root.ReadFile(filepath.Join(paths.configRel, plainConversionName))
	if err != nil {
		return err
	}
	const retired = "conversion-record.json"
	if info, err := folder.Lstat(retired); err == nil {
		if !info.Mode().IsRegular() || !conversionSingleLink(info) {
			return errors.New("retired conversion record must be a regular unlinked file")
		}
		previous, err := folder.ReadFile(retired)
		if err != nil {
			return err
		}
		if !bytes.Equal(payload, previous) {
			return errors.New("retired conversion record differs; preserve the original records")
		}
		if err := folder.Chmod(retired, 0400); err != nil {
			return err
		}
	} else if errors.Is(err, fs.ErrNotExist) {
		if err := durable.WriteFile(folder, retired, payload, 0400); err != nil {
			return err
		}
	} else {
		return err
	}
	// Publish and sync the read-only archive before retiring the config record.
	if err := durable.SyncDir(folder); err != nil {
		return err
	}
	if err := root.Chmod(history, 0500); err != nil {
		return err
	}
	if err := syncConversionFolder(root, history); err != nil {
		return err
	}
	if err := root.Remove(filepath.Join(paths.configRel, plainConversionName)); err != nil {
		return err
	}
	return syncConversionFolder(root, paths.configRel)
}

// Keep the cutover inventory bound to committed metadata after the active
// conversion record moves to owner-managed history, which may later be absent.
func checkFleetConversionHistory(record *fleetSetupRecord, history *plainHistory) error {
	if record.ConversionHistoryDigest == "" {
		return nil
	}
	if history == nil {
		return errors.New("converted role receipt is missing history metadata; restore the original receipt")
	}
	payload, err := json.Marshal(history)
	if err != nil {
		return err
	}
	if payloadDigest(payload) != record.ConversionHistoryDigest {
		return errors.New("converted role receipt history differs from committed history metadata")
	}
	return nil
}
