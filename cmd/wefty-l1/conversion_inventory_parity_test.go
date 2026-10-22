package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/conversioninventory"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestConversionInventoryRemovalProjectionParity(t *testing.T) {
	directory := t.TempDir()
	control, ledger := filepath.Join(directory, "l1.sqlite"), filepath.Join(directory, "l3.sqlite")
	store, err := l1.OpenStore(control, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runs, err := l3.OpenStore(ledger, l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runs.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", control)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	states := make([]contract.JobState, 0, len(contract.ServiceJobTransitions))
	for state := range contract.ServiceJobTransitions {
		states = append(states, state)
	}
	slices.Sort(states)
	var ids []string
	for _, state := range states {
		for _, desired := range []contract.ServiceDesiredState{contract.ServiceDesiredRunning, contract.ServiceDesiredStopped} {
			for _, removal := range []contract.JobState{"", contract.JobRemovalPending, contract.JobAgentCleaned, contract.JobRemovedVerified, contract.JobForgottenCleanupUnverified, contract.JobStalledCleanupUnverified} {
				id := fmt.Sprintf("%s-%s-%s", state, desired, removal)
				ids = append(ids, id)
				if _, err := tx.Exec(`INSERT INTO jobs(job_id,dispatch_key,request_hash,spec_json,state,created_ns,updated_ns) VALUES(?,?,?,'{"kind":"process","class":"service"}',?,0,0)`, id, id, "hash", state); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(`INSERT INTO service_jobs(job_id,desired_state) VALUES(?,?)`, id, desired); err != nil {
					t.Fatal(err)
				}
				if removal != "" {
					if _, err := tx.Exec(`INSERT INTO service_removals(job_id,bound_node_id,removal_generation,cleanup_fence,root_instance_id,status,requested_ns) VALUES(?,'node',1,'fixture-fence','fixture-root',?,0)`, id, removal); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	inventory, err := inventoryPlainConversion(t.Context(), control, ledger, "node")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			job, err := store.GetJob(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			var desired contract.ServiceDesiredState
			if job.ServiceJob != nil {
				desired = job.DesiredState
			}
			blocker := conversioninventory.JobBlocker(job.JobID, job.Spec.Class, job.State, desired)
			offline := slices.Contains(inventory.Blockers, "service job "+id)
			if offline != (blocker != "") {
				t.Fatalf("live/offline removal verdict differs: live=%q projected state=%s desired=%s offline=%t", blocker, job.State, desired, offline)
			}
		})
	}
	t.Logf("live GetJob + shared JobBlocker and offline inventory agree for %d service/removal/desired-state combinations", len(ids))
}

func TestConversionInventoryMissingComputerJobDoesNotScanNull(t *testing.T) {
	directory := t.TempDir()
	control, ledger := filepath.Join(directory, "l1.sqlite"), filepath.Join(directory, "l3.sqlite")
	store, err := l1.OpenStore(control, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	runs, err := l3.OpenStore(ledger, l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runs.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", control)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO computers(computer_id,name,placement_node_id,bound_node_id,grants_json,storage_id,storage_generation,desired_disk_bytes,desired_state,intent_revision,applied_revision,current_job_id,current_spec_revision,reconfiguration_phase,removal_outcome,created_ns,updated_ns) VALUES('missing-job-computer','missing-job-computer','node','node','[]','storage',1,1,'removed',1,1,'missing-job',1,'stable','',0,0)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	inventory, err := inventoryPlainConversion(t.Context(), control, ledger, "node")
	if err != nil || !slices.Contains(inventory.Blockers, "Computer missing-job-computer") {
		t.Fatalf("missing Computer job must refuse conservatively without a NULL scan failure: %+v %v", inventory, err)
	}
}
