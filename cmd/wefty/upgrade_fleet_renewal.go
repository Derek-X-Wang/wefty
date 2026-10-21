package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/internal/serviceready"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A ready record opts this running boot into SIGUSR1. Old agents never
// advertise it, so they keep the existing window. Match launchd PID and L1 boot.
func requestFleetUpgradeHeartbeat(ctx context.Context, host setupHost, paths setupPaths, node l1.Node) error {
	_, err := tryFleetUpgradeHeartbeat(ctx, host, paths, node)
	return err
}

func tryFleetUpgradeHeartbeat(ctx context.Context, host setupHost, paths setupPaths, node l1.Node) (bool, error) {
	path := filepath.Join(paths.data, "setup-ready", "node-agent.json")
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return false, errors.New("invalid node-agent ready record for immediate heartbeat")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	var ready serviceready.Metadata
	if err := json.Unmarshal(payload, &ready); err != nil {
		return false, err
	}
	if !ready.ImmediateHeartbeat {
		return false, nil
	}
	manager := launchagent.Manager{Run: host.launchctl, UID: host.uid}
	state, err := manager.State(ctx, setupNodeAgentLabel)
	if err != nil {
		return false, err
	}
	if !state.Running || state.PID <= 0 || state.PID != ready.PID || ready.NodeID != node.NodeID || ready.BootSessionID != node.BootSessionID {
		return false, errors.New("cannot verify immediate-heartbeat receiver")
	}
	bootTime := host.bootTime
	if bootTime == nil {
		bootTime = setupSystemBootTime
	}
	boot, err := bootTime()
	if err != nil {
		return false, err
	}
	if !ready.ReadyAt.After(boot) {
		return false, errors.New("immediate-heartbeat ready record predates this system boot")
	}
	_, err = manager.Run(ctx, launchagent.Launchctl, "kill", "SIGUSR1", fmt.Sprintf("gui/%d/%s", host.uid, setupNodeAgentLabel))
	return err == nil, err
}

func waitFleetUpgradeReplacement(ctx context.Context, host setupHost, op upgradeHost, paths setupPaths, p connprofile.Profile, oldBoot, commit string, started time.Time, action launchagent.Action, heartbeat ...func(context.Context, l1.Node)) error {
	bounded, cancel := context.WithTimeout(ctx, upgradeReplacementBudget)
	defer cancel()
	request := fleetUpgradeHeartbeatRequester(host, paths, op, io.Discard)
	if len(heartbeat) > 0 {
		request = heartbeat[0]
	}
	for op.now().Sub(started) < upgradeReplacementBudget && bounded.Err() == nil {
		node, err := op.observe(bounded, p)
		if err == nil && node.BootSessionID != "" && (node.BootSessionID != oldBoot || action == launchagent.Unchanged) && node.AgentVersion == commit && node.State == contract.NodeAlive {
			if node.LastHeartbeatAt.After(started) || action == launchagent.Unchanged {
				return nil
			}
			request(bounded, node)
		}
		if err := op.wait(bounded, 100*time.Millisecond); err != nil {
			break
		}
	}
	return fmt.Errorf("replacement node agent did not become healthy within %s", upgradeReplacementBudget)
}

func waitFleetUpgradeRenewal(ctx context.Context, operations upgradeHost, profile connprofile.Profile, baseline l1.Node, started, restarted time.Time, heartbeat ...func(context.Context, l1.Node)) (time.Duration, error) {
	var lastErr error
	for operations.now().Sub(started) < upgradeRenewalWindow && ctx.Err() == nil {
		node, err := operations.observe(ctx, profile)
		if err == nil && node.BootSessionID == baseline.BootSessionID && node.AuthorityGeneration == baseline.AuthorityGeneration && node.LastHeartbeatAt.After(restarted) {
			renewed := true
			for _, old := range baseline.ActiveAttempts {
				for _, current := range node.ActiveAttempts {
					if current.AttemptID == old.AttemptID && (!current.LeaseExpiresAt.After(old.LeaseExpiresAt) || !current.LeaseExpiresAt.Add(-l1.DefaultLeaseDuration).After(restarted)) {
						job, readErr := operations.readJob(ctx, profile, old.JobID)
						if readErr != nil || job.Outcome != contract.JobOutcomeCanceled {
							renewed = false
						}
						if readErr != nil {
							lastErr = readErr
						}
					}
				}
			}
			elapsed := operations.now().Sub(started)
			if renewed && elapsed < upgradeRenewalWindow && ctx.Err() == nil {
				return elapsed, nil
			}
		}
		if err == nil && len(heartbeat) > 0 {
			heartbeat[0](ctx, node)
		}
		if err != nil {
			lastErr = err
		}
		if err := operations.wait(ctx, 100*time.Millisecond); err != nil {
			lastErr = err
			break
		}
	}
	return operations.now().Sub(started), fmt.Errorf("no successful node lease renewal within %s after restarting L1 and the run ledger (last observation: %v)", upgradeRenewalWindow, lastErr)
}

// Only successful L1 observations call this optional accelerator. Failure never
// changes the normal renewal proof or budget. Retry at most once per second:
// registration may precede publication of the new ready record.
func fleetUpgradeHeartbeatRequester(host setupHost, paths setupPaths, op upgradeHost, warnings io.Writer) func(context.Context, l1.Node) {
	var attempted time.Time
	var boot string
	warned := false
	return func(ctx context.Context, node l1.Node) {
		now := op.now()
		if boot == node.BootSessionID && !attempted.IsZero() && now.Sub(attempted) < time.Second {
			return
		}
		attempted, boot = now, node.BootSessionID
		bounded, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		if _, err := tryFleetUpgradeHeartbeat(bounded, host, paths, node); err != nil && !warned {
			warned = true
			fmt.Fprintf(warnings, "Immediate heartbeat unavailable; keeping the normal renewal window: %s\n", strings.Join(strings.Fields(err.Error()), " "))
		}
	}
}
