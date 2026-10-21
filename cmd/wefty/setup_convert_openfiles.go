package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// lsof checks read handles too, including WAL readers that a SQLite write lock
// would not exclude. No database is opened or migrated for this inspection.
func checkConversionFilesClosed(ctx context.Context, files []string) error {
	if len(files) == 0 {
		return errors.New("plain database files are missing; open-file inspection refused")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	args := append([]string{"-nP", "-F", "p", "--"}, files...)
	cmd := exec.CommandContext(bounded, "/usr/sbin/lsof", args...)
	// lsof can fork a helper that hangs on a dead network mount; do not wait
	// for its pipes past the budget.
	cmd.WaitDelay = time.Second
	var out, diagnostics bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &diagnostics
	err := cmd.Run()
	if bounded.Err() != nil {
		return fmt.Errorf("database open-file inspection unverified: %w", bounded.Err())
	}
	// An inspection warning is not proof that every opener was visible.
	if diagnostics.Len() != 0 {
		return errors.New("database open-file inspection unverified; repair lsof access and retry")
	}
	var exit *exec.ExitError
	searched := err == nil || errors.As(err, &exit) && exit.ExitCode() == 1
	open := false
	for _, line := range strings.Fields(out.String()) {
		// lsof always emits file-descriptor fields even with only p selected.
		if strings.HasPrefix(line, "f") && len(line) > 1 {
			continue
		}
		pid, parseErr := strconv.Atoi(strings.TrimPrefix(line, "p"))
		if !strings.HasPrefix(line, "p") || parseErr != nil || pid <= 0 {
			return errors.New("database open-file inspection returned unrecognized process evidence")
		}
		open = true
	}
	// Exit 1 can mean a different search operand was closed. Positive PID
	// evidence still proves that this closed dataset is in use (lsof(8)).
	if searched && open {
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "plain L1/L3 database files are still open by a process; close the extra process and retry later"}}
	}
	if errors.As(err, &exit) && exit.ExitCode() == 1 && out.Len() == 0 {
		return nil
	}
	return fmt.Errorf("database open-file inspection unverified: %v", err)
}
