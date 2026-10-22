package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/conversioninventory"
)

// Called only after every plain service is absent and the open-file check has
// succeeded. Never use a SQLite connection (including a backup/checkpoint) on
// the originals: even a read-only connection can create WAL/SHM sidecars.
func checkQuiescentPlainConversionWork(ctx context.Context, host setupHost, request setupRequest, paths setupPaths, record *plainConversion) (resultErr error) {
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	folder, err := createPlainConversionInventory(root, paths, record)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, cleanupPlainConversionInventory(root, paths, record)) }()
	size, err := plainConversionInventorySize(root, paths)
	if err != nil {
		return err
	}
	copying, cancelCopy := context.WithTimeout(ctx, plainConversionCopyBudget(size))
	defer cancelCopy()
	for _, db := range []string{"l1.sqlite", "l3.sqlite"} {
		for _, suffix := range []string{"", "-wal", "-journal"} {
			if err := copyConversionInventoryFile(copying, root, filepath.Join(paths.dataRel, db+suffix), filepath.Join(folder, db+suffix), suffix != "", host.conversionCopy); err != nil {
				cancelCopy()
				return fmt.Errorf("quiescent conversion inventory copy failed: %w", err)
			}
		}
	}
	cancelCopy()
	// Preparation never consumes the authority-query budget.
	bounded, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
	defer cancel()

	source, err := setupSourceDirectory(host, request)
	if err != nil {
		return err
	}
	output, err := upgradeOperations(host).command(bounded, folder, os.Environ(), filepath.Join(source, "wefty-l1"), "--conversion-inventory", "--db="+filepath.Join(folder, "l1.sqlite"), "--conversion-ledger-db="+filepath.Join(folder, "l3.sqlite"), "--conversion-node-id="+record.NodeID)
	if err != nil {
		message := strings.ToLower(err.Error())
		if strings.Contains(message, "schema") || strings.Contains(message, "no such table") || strings.Contains(message, "no such column") {
			return fmt.Errorf("quiescent conversion inventory schema is incompatible; upgrade the plain install to this build first: %w", err)
		}
		return fmt.Errorf("quiescent conversion inventory unavailable: %w", err)
	}
	var inventory conversioninventory.Result
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inventory); err != nil {
		return fmt.Errorf("invalid quiescent conversion inventory: %w", err)
	}
	if decoder.Decode(new(any)) != io.EOF || inventory.Version != 1 || inventory.Blockers == nil {
		return errors.New("invalid quiescent conversion inventory version or blocker list")
	}
	for _, blocker := range inventory.Blockers {
		if strings.TrimSpace(blocker) == "" {
			return errors.New("invalid empty quiescent conversion blocker")
		}
	}
	return refusePlainConversionWork(inventory.Blockers)
}

// Check the selected companion, rather than the old installed authority, before
// changing admission. A mismatched binary must not cost a drain.
func checkPlainConversionBuild(ctx context.Context, host setupHost, request setupRequest) error {
	source, err := setupSourceDirectory(host, request)
	if err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, upgradeMutationBudget)
	defer cancel()
	output, err := runUpgradeCommand(bounded, source, os.Environ(), filepath.Join(source, "wefty-l1"), "--version")
	if err != nil || strings.TrimSpace(string(output)) != version {
		message := "conversion requires wefty and the selected wefty-l1 from the same build; use the matching binaries beside this CLI or select their directory with --from"
		if err != nil {
			message += ": " + err.Error()
		}
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: message}}
	}
	return nil
}
