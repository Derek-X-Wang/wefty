package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeGenerationServiceRecoveryUsesQuiescence(t *testing.T) {
	for _, mode := range []string{"generation", "legacy", "missing-proof", "changed-proof"} {
		t.Run(mode, func(t *testing.T) {
			f := newUpgradeFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			profile, _ := connprofile.Read(paths.profile)
			old := l1.NodeActiveAttempt{JobID: "never-service", AttemptID: "old", BootSessionID: f.node.BootSessionID, Kind: contract.JobKindProcess, Class: contract.JobClassService}
			receipt := &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, OldBoot: f.node.BootSessionID, Reason: "wefty upgrade operation", Services: []l1.NodeActiveAttempt{old}}
			if mode != "legacy" && mode != "missing-proof" {
				receipt.ServiceQuiescence = &agent.UpgradeServiceQuiescence{OperationID: "activation", NodeID: f.node.NodeID, AgentGenerationID: "old-generation", BootSessionID: f.node.BootSessionID, Services: []l1.NodeActiveAttempt{old}}
				if mode == "changed-proof" {
					receipt.ServiceQuiescence.Services[0].AttemptID = "different"
				}
			}
			var requests []l1.ServiceRestartRequest
			f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
				return l1.Job{JobID: old.JobID, CurrentAttemptID: old.AttemptID, Spec: contract.JobSpec{Restart: contract.RestartNever}, State: contract.JobFailed, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning, BoundNodeID: receipt.NodeID}}, nil
			}
			f.operations.restartService = func(context.Context, connprofile.Profile, string, string) (l1.Job, error) {
				t.Fatal("generation called unguarded legacy restart")
				return l1.Job{}, nil
			}
			f.operations.recoverService = func(_ context.Context, _ connprofile.Profile, id string, request l1.ServiceRestartRequest) (l1.Job, error) {
				requests = append(requests, request)
				if id != old.JobID || request.Upgrade == nil || request.Upgrade.AttemptID != old.AttemptID || request.Upgrade.BootSessionID != old.BootSessionID {
					t.Fatalf("wrong recovery: %s %+v", id, request)
				}
				if len(requests) == 1 {
					return l1.Job{}, errors.New("lost accepted reply")
				}
				return l1.Job{State: contract.JobQueued, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning}}, nil
			}
			if err := writeUpgradeDrain(paths.config, receipt); err != nil {
				t.Fatal(err)
			}
			err := resumeUpgradeServices(t.Context(), *f.operations, paths, profile, receipt, f.node, false, io.Discard)
			switch mode {
			case "legacy", "missing-proof":
				if err != nil || len(requests) != 0 || len(receipt.Services) != 1 {
					t.Fatalf("same-boot legacy changed: %v %+v", err, requests)
				}
			case "changed-proof":
				if err == nil || len(requests) != 0 {
					t.Fatal("mismatched proof accepted")
				}
			default:
				if err == nil || len(receipt.Services) != 1 {
					t.Fatal("lost reply discarded recovery receipt")
				}
				saved, err := readUpgradeDrain(paths.config)
				if err != nil {
					t.Fatal(err)
				}
				if err = resumeUpgradeServices(t.Context(), *f.operations, paths, profile, saved, f.node, false, io.Discard); err != nil {
					t.Fatal(err)
				}
				if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) || len(saved.Services) != 0 || len(saved.restarted) != 1 {
					t.Fatalf("replay changed key or queue: %+v %+v", requests, saved)
				}
				if err = resumeUpgradeServices(t.Context(), *f.operations, paths, profile, saved, f.node, false, io.Discard); err != nil || len(requests) != 2 {
					t.Fatal("settled queue restarted again", err)
				}
			}
		})
	}
}
