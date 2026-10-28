package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type proofDialogReceipt struct {
	Approved bool                     `json:"approved"`
	Choice   agent.SeatApprovalChoice `json:"choice,omitempty"`
	PID      int                      `json:"pid"`
	At       time.Time                `json:"at"`
}

// The boolean grants OCI startup for this process. A recorded refusal keeps the
// process-only node agent alive and cannot cause a launchd dialog restart loop.
func prepareInstalledUserOCI(ctx context.Context, path, nodeID, bootID, expectedVersion string, prepare func(context.Context, limarunner.UserInstallConfig, string, string) error, dialog agent.SeatApprovalDialog, stopProof func(context.Context, limarunner.UserInstallConfig) error) (bool, error) {
	var c limarunner.UserInstallConfig
	if err := limarunner.ReadUserInstallJSON(path, &c); err != nil {
		return false, err
	}
	if os.Getenv("LIMA_HOME") != c.LimaHome {
		return false, errors.New("per-user host VM LIMA_HOME differs from its LaunchAgent configuration")
	}
	if c.Assets.HelperVersion != expectedVersion {
		return false, errors.New("per-user Linux helper version differs from the node agent")
	}
	receiptPath := filepath.Join(filepath.Dir(c.SetupState), "proof-dialog.json")
	if c.ProofDialog {
		var receipt proofDialogReceipt
		if err := limarunner.ReadUserInstallJSON(receiptPath, &receipt); err == nil {
			if !receipt.Approved {
				return false, nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	preparedPath := filepath.Join(filepath.Dir(c.SetupState), "host-vm-prepared.json")
	wanted := preparedUserOCI{Assets: c.Assets, Instance: c.Instance, LimaHome: c.LimaHome, MountRoot: c.MountRoot, Sizing: c.Sizing}
	var prepared preparedUserOCI
	preparedErr := limarunner.ReadUserInstallJSON(preparedPath, &prepared)
	currentIntent, intentErr := (limarunner.FileIntentSource{Path: c.IntentFile}).ReadIntent(ctx)
	if intentErr != nil {
		return false, intentErr
	}
	if !currentIntent.Enabled {
		return true, nil
	}
	if preparedErr != nil && !errors.Is(preparedErr, os.ErrNotExist) {
		return false, preparedErr
	}
	skipPrepare := !c.ProofDialog && currentIntent.PositionValue() == limarunner.OCIPositionOnDemand && preparedErr == nil && prepared == wanted
	if !skipPrepare {
		log.Printf("per-user host VM bootstrap begin pid=%d at=%s", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
		if err := prepare(ctx, c, nodeID, bootID); err != nil {
			return false, err
		}
	}
	intent, err := (limarunner.FileIntentSource{Path: c.IntentFile}).ReadIntent(ctx)
	if err != nil {
		return false, err
	}
	if !intent.Enabled {
		return true, nil
	}
	if !skipPrepare && !c.ProofDialog {
		payload, err := json.Marshal(wanted)
		if err != nil {
			return false, err
		}
		root, err := os.OpenRoot(filepath.Dir(preparedPath))
		if err != nil {
			return false, err
		}
		err = durable.WriteFile(root, filepath.Base(preparedPath), append(payload, '\n'), 0600)
		closeErr := root.Close()
		if err != nil {
			return false, err
		}
		if closeErr != nil {
			return false, closeErr
		}
	}
	capacity, reserve, err := limarunner.DefaultMacComputerCapacity(c.Sizing)
	if err != nil {
		return false, err
	}

	// Setup-oci owns later configuration changes. Bootstrap only creates the
	// initial state; a node-agent restart must not replace the owner's values.
	if _, err := ocicontrol.ReadSetupState(c.SetupState); errors.Is(err, os.ErrNotExist) {
		state := ocicontrol.SetupState{VMMemory: c.Sizing.Memory, VMCPUs: c.Sizing.CPUs, VMDisk: c.Sizing.Disk, VMType: "vz", HostMountRoot: c.MountRoot, ProbeDigest: c.Assets.ProbeDigest, MemoryCapacityBytes: capacity, MemoryReserveBytes: reserve}
		if err := ocicontrol.WriteSetupState(c.SetupState, state); err != nil {
			return false, err
		}
		if err := ocicontrol.WriteSetupState(ocicontrol.DesiredSetupStatePath(c.SetupState), state); err != nil {
			return false, err
		}
	} else if err != nil {
		return false, err
	}
	if !c.ProofDialog {
		return true, nil
	}
	if _, err := os.Lstat(receiptPath); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	// Bootstrap has finished. Its remaining install budget must not shorten
	// the owner's dialog; bound this new phase independently.
	dialogContext, cancelDialog := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
	defer cancelDialog()
	choice, dialogErr := dialog.Show(dialogContext, agent.SeatApprovalPrompt{Message: "Wefty attended proof: the per-user node agent started the isolated host VM. Approve one proof Seat without internet. This is a proof fixture, not a Seat request.", Wait: 5 * time.Minute})
	if dialogErr != nil {
		choice = agent.SeatApprovalUnavailable
		if errors.Is(dialogErr, context.DeadlineExceeded) {
			choice = agent.SeatApprovalNoAnswer
		}
		log.Printf("per-user host VM proof dialog failed: %v", dialogErr)
	}
	approved := choice == agent.SeatApprovalApprove
	receipt, err := json.Marshal(proofDialogReceipt{Approved: approved, Choice: choice, PID: os.Getpid(), At: time.Now().UTC()})
	if err != nil {
		return false, err
	}
	root, err := os.OpenRoot(filepath.Dir(receiptPath))
	if err != nil {
		return false, err
	}
	defer root.Close()
	if err := durable.WriteFile(root, filepath.Base(receiptPath), append(receipt, '\n'), 0600); err != nil {
		return false, err
	}
	log.Printf("per-user host VM proof dialog choice=%s pid=%d", choice, os.Getpid())
	if !approved && c.Instance == "wefty-proof" {
		// The dialog may have outlived bootstrap's deadline. Cleanup gets its
		// own bounded Lima command, without reopening OCI or losing the refusal.
		if err := stopProof(context.WithoutCancel(ctx), c); err != nil {
			log.Printf("proof host VM graceful stop failed: %v; use the attended undo", err)
		}
	}
	return approved, nil
}

// Registered is only a startup signal. Keep a receipt only after the local
// capability snapshot proves the positive publication was acknowledged.
func writeProofStartupReceipt(ctx context.Context, dir string, node *agent.Agent, barrier *ocihelper.BootBarrier, nodeID, bootID string) {
	select {
	case <-ctx.Done():
		return
	case <-node.Registered():
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var snapshot agent.CapabilitySnapshot
	var sweep ocihelper.VerifiedSweepReceipt
	for {
		snapshot = node.CapabilitySnapshot()
		var ok bool
		sweep, ok = barrier.SweepReceipt()
		if snapshot.Capabilities["kind:oci"] && snapshot.PendingPublicationRevision == 0 && snapshot.LastProbe != nil && ok {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
	payload, err := json.Marshal(struct {
		PID           int                                  `json:"pid"`
		NodeID        string                               `json:"node_id"`
		BootSessionID string                               `json:"boot_session_id"`
		PublishedAt   time.Time                            `json:"published_at"`
		Capability    agent.CapabilitySnapshot             `json:"capability"`
		Barrier       ocihelper.BootBarrierTimelineReceipt `json:"barrier"`
	}{os.Getpid(), nodeID, bootID, time.Now().UTC(), snapshot, sweep.BarrierTimeline})
	if err != nil {
		log.Printf("proof startup receipt: %v", err)
		return
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		log.Printf("proof startup receipt: %v", err)
		return
	}
	defer root.Close()
	if err := durable.WriteFile(root, "proof-startup.json", append(payload, '\n'), 0600); err != nil {
		log.Printf("proof startup receipt: %v", err)
	}
}

// The receipt identifies prepared helper/probe assets, not host VM liveness.
// Timing settings are excluded so changing them cannot start an idle host VM.
type preparedUserOCI struct {
	Assets    limarunner.UserInstallAssets `json:"assets"`
	Instance  string                       `json:"instance"`
	LimaHome  string                       `json:"lima_home"`
	MountRoot string                       `json:"mount_root"`
	Sizing    limarunner.Sizing            `json:"sizing"`
}
