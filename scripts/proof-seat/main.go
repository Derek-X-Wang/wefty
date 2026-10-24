// Command proof-seat drives only the disposable attended proof's existing
// store seams. It is not a Seat request API and never serves production traffic.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
)

func proofRoot(home string) (string, error) {
	root := filepath.Join(home, ".local", "share", "wefty-proof")
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	canonicalHome, err := filepath.EvalSymlinks(home)
	if err != nil {
		return "", err
	}
	if resolved != filepath.Join(canonicalHome, ".local", "share", "wefty-proof") {
		return "", errors.New("proof root is redirected")
	}
	for _, suffix := range []string{"config", "data", "data/lima", "data/l1.sqlite"} {
		path := filepath.Join(root, suffix)
		actual, e := filepath.EvalSymlinks(path)
		if e != nil || actual != path {
			return "", errors.New("proof state is missing or redirected")
		}
	}
	var c limarunner.UserInstallConfig
	if err := limarunner.ReadUserInstallJSON(filepath.Join(root, "config", "oci-user.json"), &c); err != nil {
		return "", err
	}
	if c.Instance != "wefty-proof" || c.LimaHome != filepath.Join(root, "data", "lima") {
		return "", errors.New("not an isolated proof install")
	}
	var receipt struct {
		Approved bool      `json:"approved"`
		Choice   string    `json:"choice,omitempty"`
		PID      int       `json:"pid"`
		At       time.Time `json:"at"`
	}
	if err := limarunner.ReadUserInstallJSON(filepath.Join(root, "data", "proof-dialog.json"), &receipt); err != nil {
		return "", err
	}
	if !receipt.Approved {
		return "", errors.New("owner has not approved the proof dialog")
	}
	return root, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	flags := flag.NewFlagSet("proof-seat", flag.ContinueOnError)
	image := flags.String("image", "", "digest-pinned default multi-Seat image")
	name := flags.String("name", "wefty-proof-one", "proof Computer name")
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if flags.NArg() < 1 {
		return errors.New("usage: proof-seat [--image REF@DIGEST] [--name wefty-proof-N] create|snapshot|cleanup|stop|guard [COMPUTER_ID]")
	}
	if os.Getenv("WEFTY_ATTENDED_PROOF") != "1" {
		return errors.New("requires WEFTY_ATTENDED_PROOF=1 and the owner present")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	root, err := proofRoot(home)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if flags.Arg(0) == "guard" {
		var c limarunner.UserInstallConfig
		if err := limarunner.ReadUserInstallJSON(filepath.Join(root, "config", "oci-user.json"), &c); err != nil {
			return err
		}
		return limarunner.ProbeProofRecreateGuard(ctx, c)
	}
	db := filepath.Join(root, "data", "l1.sqlite")
	if info, err := os.Stat(db); err != nil || !info.Mode().IsRegular() {
		return errors.New("proof L1 database must already exist")
	}
	profile, err := connprofile.Read(filepath.Join(root, "config", connprofile.FileName))
	if err != nil {
		return err
	}
	store, err := l1.OpenStore(db, l1.StoreOptions{})
	if err != nil {
		return err
	}
	defer store.Close()
	var result any
	switch flags.Arg(0) {
	case "create":
		if !strings.HasPrefix(*name, "wefty-proof-") {
			return errors.New("Computer name must start with wefty-proof-")
		}
		reference, digest, ok := strings.Cut(*image, "@")
		if !ok || !strings.HasPrefix(digest, "sha256:") {
			return errors.New("image must be pinned by digest")
		}
		memory := int64(1 << 30)
		keyBytes := make([]byte, 16)
		if _, err := rand.Read(keyBytes); err != nil {
			return err
		}
		key := hex.EncodeToString(keyBytes)
		spec := contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "proof-" + key, Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
			RoutingTags: []string{contract.StableNodeTagPrefix + profile.Identities.Node}, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: reference, Digest: &digest}, Limits: &contract.OCILimits{MemoryBytes: &memory}, Computer: &contract.OCIComputerSpec{DiskBytes: 1 << 30, Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}}}}}
		settings := l1.ComputerSettings{SeatLimit: 1, IdleAction: l1.ComputerIdleDelete, Network: l1.ComputerNetworkNone}
		c, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{Name: *name, Spec: spec, Actor: "attended-proof", SeatRequestSettings: &settings})
		if err != nil {
			return err
		}
		seatID := "seat_" + key
		if _, err := store.SetComputerWantedSeats(ctx, c.ComputerID, 0, []l1.WantedComputerSeat{{SeatID: seatID, Place: 1}}); err != nil {
			return fmt.Errorf("proof Computer %s created but Seat directive failed: %w", c.ComputerID, err)
		}
		result = map[string]any{"computer_id": c.ComputerID, "seat_id": seatID}
	case "snapshot", "cleanup", "stop":
		if flags.NArg() != 2 {
			return errors.New("Computer ID required")
		}
		c, err := store.GetComputer(ctx, flags.Arg(1))
		if err != nil {
			return err
		}
		if !strings.HasPrefix(c.Name, "wefty-proof-") || c.PlacementNodeID != profile.Identities.Node {
			return errors.New("refusing a Computer outside the proof")
		}
		pre := l1.ComputerMutationPrecondition{IntentRevision: c.IntentRevision, StorageID: c.StorageID, StorageGeneration: c.StorageGeneration, Actor: "attended-proof"}
		if flags.Arg(0) == "cleanup" {
			c, err = store.RemoveComputer(ctx, c.ComputerID, l1.ComputerRemoveRequest{ComputerMutationPrecondition: pre})
		}
		if flags.Arg(0) == "stop" {
			c, err = store.SetComputerDesiredState(ctx, c.ComputerID, l1.ComputerDesiredStateRequest{ComputerMutationPrecondition: pre, DesiredState: contract.ServiceDesiredStopped})
		}
		if err != nil {
			return err
		}
		if c.CurrentJobID != "" {
			job, e := store.GetJob(ctx, c.CurrentJobID)
			if e != nil {
				return e
			}
			c.CurrentJob = job
		}
		seats, err := store.GetComputerSeatObservations(ctx, c.ComputerID)
		if err != nil {
			return err
		}
		ready := false
		for _, s := range seats {
			if s.State == l1.ComputerSeatReady && s.AttemptID == c.CurrentJob.CurrentAttemptID {
				ready = true
			}
		}
		computerReady := c.CurrentJob.ServiceJob != nil && c.CurrentJob.Ready != nil && *c.CurrentJob.Ready
		node, err := store.GetNode(ctx, profile.Identities.Node)
		if err != nil {
			return err
		}
		result = map[string]any{"computer": c, "seats": seats, "node": node, "ready": ready && computerReady, "observed_at": time.Now().UTC()}
	default:
		return errors.New("unknown proof operation")
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
