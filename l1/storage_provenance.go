package l1

import (
	"context"
	"database/sql"
	"time"
)

type ComputerCustodyBranch struct {
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	RemovalOutcome    string `json:"removal_outcome,omitempty"`
}

type ComputerStorageProvenance struct {
	ComputerID        string                  `json:"computer_id"`
	StorageID         string                  `json:"storage_id"`
	StorageGeneration int64                   `json:"storage_generation"`
	RemovalOutcome    string                  `json:"removal_outcome,omitempty"`
	CustodyTainted    bool                    `json:"custody_tainted"`
	CustodyForks      []ComputerCustodyBranch `json:"custody_forks"`
	Provenance        []StorageProvenance     `json:"storage_provenance"`
	CustodyExports    []ComputerCustodyExport `json:"custody_exports"`
}

const computerCustodyGraph = `WITH RECURSIVE custody(storage_id) AS (
	SELECT ?
	UNION SELECT p.source_storage_id FROM storage_provenance p
		JOIN custody c ON p.destination_storage_id=c.storage_id WHERE p.kind IN ('clone', 'import')
	UNION SELECT p.destination_storage_id FROM storage_provenance p
		JOIN custody c ON p.source_storage_id=c.storage_id WHERE p.kind IN ('clone', 'import')
)`

// ListComputerStorageProvenance returns only durable L1 ledger facts. It does
// not infer deletion: external Custody is tainted by a committed export or an
// import record, while each Computer retains its own removal outcome.
func (s *Store) ListComputerStorageProvenance(ctx context.Context, computerID string) (ComputerStorageProvenance, error) {
	computer, err := s.GetComputer(ctx, computerID)
	if err != nil {
		return ComputerStorageProvenance{}, err
	}
	projection := ComputerStorageProvenance{ComputerID: computer.ComputerID, StorageID: computer.StorageID,
		StorageGeneration: computer.StorageGeneration, RemovalOutcome: computer.RemovalOutcome,
		CustodyForks: []ComputerCustodyBranch{}, Provenance: []StorageProvenance{}, CustodyExports: []ComputerCustodyExport{}}

	rows, err := s.db.QueryContext(ctx, computerCustodyGraph+`
		SELECT c.computer_id, c.storage_id, c.storage_generation, c.removal_outcome
		FROM computers c JOIN custody ON custody.storage_id=c.storage_id ORDER BY c.created_ns, c.computer_id`, computer.StorageID)
	if err != nil {
		return ComputerStorageProvenance{}, internalError(err, "list Computer custody forks")
	}
	for rows.Next() {
		var branch ComputerCustodyBranch
		var removal sql.NullString
		if err := rows.Scan(&branch.ComputerID, &branch.StorageID, &branch.StorageGeneration, &removal); err != nil {
			rows.Close()
			return ComputerStorageProvenance{}, internalError(err, "scan Computer custody fork")
		}
		branch.RemovalOutcome = removal.String
		projection.CustodyForks = append(projection.CustodyForks, branch)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ComputerStorageProvenance{}, internalError(err, "iterate Computer custody forks")
	}
	if err := rows.Close(); err != nil {
		return ComputerStorageProvenance{}, internalError(err, "close Computer custody forks")
	}

	rows, err = s.db.QueryContext(ctx, computerCustodyGraph+`
		SELECT p.provenance_id, p.kind, p.source_storage_id, p.source_generation, p.backup_id,
			p.destination_computer_id, p.destination_storage_id, p.destination_generation, p.created_ns
		FROM storage_provenance p
		WHERE p.source_storage_id IN (SELECT storage_id FROM custody)
			OR p.destination_storage_id IN (SELECT storage_id FROM custody)
		ORDER BY p.created_ns, p.provenance_id`, computer.StorageID)
	if err != nil {
		return ComputerStorageProvenance{}, internalError(err, "list Storage provenance")
	}
	for rows.Next() {
		var provenance StorageProvenance
		var destinationComputer, destinationStorage sql.NullString
		var destinationGeneration sql.NullInt64
		var createdNS int64
		if err := rows.Scan(&provenance.ProvenanceID, &provenance.Kind, &provenance.SourceStorageID,
			&provenance.SourceGeneration, &provenance.BackupID, &destinationComputer, &destinationStorage,
			&destinationGeneration, &createdNS); err != nil {
			rows.Close()
			return ComputerStorageProvenance{}, internalError(err, "scan Storage provenance")
		}
		provenance.DestinationComputerID = destinationComputer.String
		provenance.DestinationStorageID = destinationStorage.String
		provenance.DestinationGeneration = destinationGeneration.Int64
		provenance.CreatedAt = time.Unix(0, createdNS).UTC()
		if provenance.Kind == "import" {
			projection.CustodyTainted = true
		}
		projection.Provenance = append(projection.Provenance, provenance)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ComputerStorageProvenance{}, internalError(err, "iterate Storage provenance")
	}
	if err := rows.Close(); err != nil {
		return ComputerStorageProvenance{}, internalError(err, "close Storage provenance")
	}

	rows, err = s.db.QueryContext(ctx, computerCustodyGraph+`
		SELECT `+custodyExportColumns+` FROM computer_custody_exports e
		JOIN custody ON custody.storage_id=e.source_storage_id ORDER BY e.requested_ns, e.export_id`, computer.StorageID)
	if err != nil {
		return ComputerStorageProvenance{}, internalError(err, "list Storage Custody exports")
	}
	for rows.Next() {
		exported, scanErr := scanCustodyExport(rows)
		if scanErr != nil {
			rows.Close()
			return ComputerStorageProvenance{}, internalError(scanErr, "scan Storage Custody export")
		}
		projection.CustodyTainted = true
		projection.CustodyExports = append(projection.CustodyExports, exported)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ComputerStorageProvenance{}, internalError(err, "iterate Storage Custody exports")
	}
	if err := rows.Close(); err != nil {
		return ComputerStorageProvenance{}, internalError(err, "close Storage Custody exports")
	}
	return projection, nil
}
