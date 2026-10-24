package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func prepareInstalledUserOCI(ctx context.Context, path, nodeID, bootID, expectedVersion string, prepare func(context.Context, limarunner.UserInstallConfig, string, string) error, dialog agent.SeatApprovalDialog) error {
	var c limarunner.UserInstallConfig
	if err := limarunner.ReadUserInstallJSON(path, &c); err != nil {
		return err
	}
	if os.Getenv("LIMA_HOME") != c.LimaHome {
		return errors.New("per-user host VM LIMA_HOME differs from its LaunchAgent configuration")
	}
	if c.Assets.HelperVersion != expectedVersion {
		return errors.New("per-user Linux helper version differs from the node agent")
	}
	log.Printf("per-user host VM bootstrap begin pid=%d at=%s", os.Getpid(), time.Now().UTC().Format(time.RFC3339Nano))
	if err := prepare(ctx, c, nodeID, bootID); err != nil {
		return err
	}
	intent, err := (limarunner.FileIntentSource{Path: c.IntentFile}).ReadIntent(ctx)
	if err != nil {
		return err
	}
	if !intent.Enabled {
		return nil
	}
	capacity, reserve, err := limarunner.DefaultMacComputerCapacity(c.Sizing)
	if err != nil {
		return err
	}

	// Setup-oci owns later configuration changes. Bootstrap only creates the
	// initial state; a node-agent restart must not replace the owner's values.
	if _, err := ocicontrol.ReadSetupState(c.SetupState); errors.Is(err, os.ErrNotExist) {
		state := ocicontrol.SetupState{VMMemory: c.Sizing.Memory, VMCPUs: c.Sizing.CPUs, VMDisk: c.Sizing.Disk, VMType: "vz", HostMountRoot: c.MountRoot, ProbeDigest: c.Assets.ProbeDigest, MemoryCapacityBytes: capacity, MemoryReserveBytes: reserve}
		if err := ocicontrol.WriteSetupState(c.SetupState, state); err != nil {
			return err
		}
		if err := ocicontrol.WriteSetupState(ocicontrol.DesiredSetupStatePath(c.SetupState), state); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if !c.ProofDialog {
		return nil
	}
	receiptPath := filepath.Join(filepath.Dir(c.SetupState), "proof-dialog.json")
	if _, err := os.Lstat(receiptPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	choice, err := dialog.Show(ctx, agent.SeatApprovalPrompt{Message: "Wefty attended proof: the per-user node agent started the isolated host VM. Approve one proof Seat without internet. This is a proof fixture, not a Seat request.", Wait: 5 * time.Minute})
	if err != nil {
		return err
	}
	if choice != agent.SeatApprovalApprove {
		return fmt.Errorf("attended proof dialog was not approved: %s", choice)
	}
	receipt, err := json.Marshal(struct {
		Approved bool      `json:"approved"`
		PID      int       `json:"pid"`
		At       time.Time `json:"at"`
	}{true, os.Getpid(), time.Now().UTC()})
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Dir(receiptPath))
	if err != nil {
		return err
	}
	defer root.Close()
	if err := durable.WriteFile(root, filepath.Base(receiptPath), append(receipt, '\n'), 0600); err != nil {
		return err
	}
	log.Printf("per-user host VM proof dialog approved pid=%d", os.Getpid())
	return nil
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
