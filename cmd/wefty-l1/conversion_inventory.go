package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/conversioninventory"
)

// Inventory only caller-owned temporary copies of a quiescent plain dataset.
// OpenStore is deliberately absent: it migrates/initializes authority state.
func inventoryPlainConversion(ctx context.Context, controlPath, ledgerPath, nodeID string) (conversioninventory.Result, error) {
	result := conversioninventory.Result{Version: 1, Blockers: []string{}}
	control, err := openConversionInventoryDB(controlPath)
	if err != nil {
		return result, err
	}
	defer control.Close()
	ledger, err := openConversionInventoryDB(ledgerPath)
	if err != nil {
		return result, err
	}
	defer ledger.Close()
	jobs, err := control.QueryContext(ctx, `WITH projected_jobs AS (
 SELECT j.job_id,j.spec_json,COALESCE(r.status,j.state) AS state,
 CASE WHEN r.job_id IS NULL THEN COALESCE(s.desired_state,'') ELSE '' END AS desired_state
 FROM jobs j LEFT JOIN service_jobs s ON s.job_id=j.job_id
 LEFT JOIN service_removals r ON r.job_id=j.job_id
 )
 SELECT j.job_id,j.spec_json,j.state,j.desired_state FROM projected_jobs j
 WHERE (j.state IN (?,?,?,?) OR j.desired_state=?)
 AND NOT EXISTS (SELECT 1 FROM computer_job_projections p WHERE p.job_id=j.job_id AND p.current=0)`, contract.JobQueued, contract.JobClaimed, contract.JobStopping, contract.JobRemovalPending, contract.ServiceDesiredRunning)
	if err != nil {
		return result, fmt.Errorf("L1 job inventory: %w", err)
	}
	defer jobs.Close()
	for jobs.Next() {
		var id string
		var payload []byte
		var state contract.JobState
		var desired contract.ServiceDesiredState
		if err := jobs.Scan(&id, &payload, &state, &desired); err != nil {
			return result, err
		}
		var spec struct {
			Class string `json:"class"`
		}
		if err := json.Unmarshal(payload, &spec); err != nil {
			return result, fmt.Errorf("L1 job inventory spec: %w", err)
		}
		if blocker := conversioninventory.JobBlocker(id, spec.Class, state, desired); blocker != "" {
			result.Blockers = append(result.Blockers, blocker)
		}
	}
	if err := jobs.Err(); err != nil {
		return result, err
	}
	if err := jobs.Close(); err != nil {
		return result, err
	}
	computers, err := control.QueryContext(ctx, `SELECT c.computer_id,c.desired_state,c.removal_outcome,COALESCE(r.status,j.state,?)
 FROM computers c LEFT JOIN jobs j ON j.job_id=c.current_job_id
 LEFT JOIN service_removals r ON r.job_id=j.job_id
 WHERE c.bound_node_id=? OR c.placement_node_id=?`, contract.JobRemovalPending, nodeID, nodeID)
	if err != nil {
		return result, fmt.Errorf("L1 Computer inventory: %w", err)
	}
	defer computers.Close()
	for computers.Next() {
		var id, outcome string
		var desired contract.ServiceDesiredState
		var state contract.JobState
		if err := computers.Scan(&id, &desired, &outcome, &state); err != nil {
			return result, err
		}
		if !conversioninventory.ComputerRetired(desired, outcome, state) {
			result.Blockers = append(result.Blockers, "Computer "+id)
		}
	}
	if err := computers.Err(); err != nil {
		return result, err
	}
	if err := computers.Close(); err != nil {
		return result, err
	}
	states := conversioninventory.UnfinishedRunStates()
	values := make([]any, len(states))
	for i, state := range states {
		values[i] = state
	}
	runs, err := ledger.QueryContext(ctx, "SELECT run_id FROM runs WHERE status IN ("+strings.TrimSuffix(strings.Repeat("?,", len(states)), ",")+")", values...)
	if err != nil {
		return result, fmt.Errorf("L3 run inventory: %w", err)
	}
	defer runs.Close()
	for runs.Next() {
		var id string
		if err := runs.Scan(&id); err != nil {
			return result, err
		}
		result.Blockers = append(result.Blockers, "run "+id)
	}
	if err := runs.Err(); err != nil {
		return result, err
	}
	slices.Sort(result.Blockers)
	result.Blockers = slices.Compact(result.Blockers)
	return result, nil
}

func openConversionInventoryDB(path string) (*sql.DB, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	query := url.Values{"mode": {"ro"}, "_pragma": {"query_only(1)", "busy_timeout(1000)"}}

	// A quiescent copy with no recovery sidecars is immutable. This avoids
	// creating empty WAL/SHM for a clean database whose header still says WAL.
	// Never use immutable with a copied WAL: SQLite would ignore its commits.
	sidecars := false
	for _, suffix := range []string{"-wal", "-journal"} {
		if _, err := os.Stat(absolute + suffix); err == nil {
			sidecars = true
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	if !sidecars {
		query.Set("immutable", "1")
	}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: absolute, RawQuery: query.Encode()}).String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}
