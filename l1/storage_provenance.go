package l1

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
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
	NextCursor        string                  `json:"next_cursor,omitempty"`
}

const maxComputerProvenanceRows = 1000

const computerCustodyGraph = `WITH RECURSIVE custody(storage_id) AS (
	SELECT ?
	UNION SELECT p.source_storage_id FROM storage_provenance p
		JOIN custody c ON p.destination_storage_id=c.storage_id WHERE p.kind IN ('clone', 'import')
	UNION SELECT p.destination_storage_id FROM storage_provenance p
		JOIN custody c ON p.source_storage_id=c.storage_id WHERE p.kind IN ('clone', 'import')
)`

// The removal writer deliberately uses the unbounded graph above. Client
// observations use an explicit bound, including one overflow sentinel.
const boundedComputerCustodyGraph = `WITH RECURSIVE custody(storage_id) AS (
 SELECT ?
 UNION SELECT p.source_storage_id FROM storage_provenance p
  JOIN custody c ON p.destination_storage_id=c.storage_id WHERE p.kind IN ('clone', 'import')
 UNION SELECT p.destination_storage_id FROM storage_provenance p
  JOIN custody c ON p.source_storage_id=c.storage_id WHERE p.kind IN ('clone', 'import')
 LIMIT 1001
)`

func custodyGraphLimitError() error {
	return &Error{Code: contract.ErrorConflict, Message: "Storage custody family exceeds the supported 1000-row bound",
		Details: map[string]any{"reason": "storage_custody_limit"}, notRetryable: true}
}

type provenanceCollectionCursor struct {
	Version      int    `json:"v"`
	ComputerID   string `json:"computer_id"`
	HighWater    int64  `json:"high_water"`
	CreatedNS    int64  `json:"created_ns"`
	ProvenanceID string `json:"provenance_id"`
}

// ListComputerStorageProvenance returns only durable L1 ledger facts. It does
// not infer deletion: external Custody is tainted by a committed export or an
// import record, while each Computer retains its own removal outcome.
func (s *Store) ListComputerStorageProvenance(ctx context.Context, computerID string) (ComputerStorageProvenance, error) {
	var result ComputerStorageProvenance
	cursor := ""
	for {
		page, err := s.ListComputerStorageProvenancePage(ctx, computerID, cursor, DefaultJobPageLimit)
		if err != nil {
			return ComputerStorageProvenance{}, err
		}
		provenance := append(result.Provenance, page.Provenance...)
		result = page
		result.Provenance = provenance
		if page.NextCursor == "" {
			return result, nil
		}
		cursor = page.NextCursor
	}
}

func (s *Store) ListComputerStorageProvenancePage(ctx context.Context, computerID, cursor string, limit int) (ComputerStorageProvenance, error) {
	var page ComputerStorageProvenance
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		page, err = reads.computerProvenancePage(ctx, computerID, cursor, limit)
		return err
	})
	return page, err
}

func (r *databaseReads) computerProvenancePage(ctx context.Context, computerID, value string, limit int) (ComputerStorageProvenance, error) {
	if limit < 1 {
		return ComputerStorageProvenance{}, protocolError(contract.ErrorInvalidRequest, "limit must be positive")
	}
	if limit > MaxJobListingPageLimit {
		limit = MaxJobListingPageLimit
	}
	cursor := provenanceCollectionCursor{Version: 1, ComputerID: computerID}
	if value != "" {
		payload, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil {
			return ComputerStorageProvenance{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
		}
		decoder := json.NewDecoder(strings.NewReader(string(payload)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&cursor); err != nil {
			return ComputerStorageProvenance{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || cursor.Version != 1 || cursor.ComputerID != computerID || cursor.HighWater < 1 || cursor.ProvenanceID == "" {
			return ComputerStorageProvenance{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid or for another Computer")
		}
	}
	if value == "" {
		if err := r.q.QueryRowContext(ctx, `SELECT COALESCE(MAX(ordinal),0) FROM provenance_listing_order`).Scan(&cursor.HighWater); err != nil {
			return ComputerStorageProvenance{}, err
		}
	}
	computer, err := r.computerViewGetComputer(ctx, computerID)
	if err != nil {
		return ComputerStorageProvenance{}, err
	}
	graph := boundedComputerCustodyGraph
	var graphRows int
	if err := r.q.QueryRowContext(ctx, graph+` SELECT COUNT(*) FROM custody`, computer.StorageID).Scan(&graphRows); err != nil {
		return ComputerStorageProvenance{}, err
	}
	if graphRows > maxComputerProvenanceRows {
		return ComputerStorageProvenance{}, custodyGraphLimitError()
	}
	projection := ComputerStorageProvenance{ComputerID: computer.ComputerID, StorageID: computer.StorageID,
		StorageGeneration: computer.StorageGeneration, RemovalOutcome: computer.RemovalOutcome,
		CustodyForks: []ComputerCustodyBranch{}, Provenance: []StorageProvenance{}, CustodyExports: []ComputerCustodyExport{}}

	if err := r.q.QueryRowContext(ctx, graph+` SELECT EXISTS(SELECT 1 FROM storage_provenance p
 WHERE p.kind='import' AND (p.source_storage_id IN (SELECT storage_id FROM custody)
 OR p.destination_storage_id IN (SELECT storage_id FROM custody)))`, computer.StorageID).Scan(&projection.CustodyTainted); err != nil {
		return ComputerStorageProvenance{}, err
	}

	rows, err := r.q.QueryContext(ctx, graph+`
		SELECT c.computer_id, c.storage_id, c.storage_generation, c.removal_outcome
		FROM computers c JOIN custody ON custody.storage_id=c.storage_id ORDER BY c.created_ns, c.computer_id LIMIT 1001`, computer.StorageID)
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
		if len(projection.CustodyForks) > maxComputerProvenanceRows {
			rows.Close()
			return ComputerStorageProvenance{}, custodyGraphLimitError()
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ComputerStorageProvenance{}, internalError(err, "iterate Computer custody forks")
	}
	if err := rows.Close(); err != nil {
		return ComputerStorageProvenance{}, internalError(err, "close Computer custody forks")
	}

	rows, err = r.q.QueryContext(ctx, graph+`
		SELECT p.provenance_id, p.kind, p.source_storage_id, p.source_generation, p.backup_id,
			p.destination_computer_id, p.destination_storage_id, p.destination_generation, p.created_ns,
			(SELECT o.verification_receipt_json FROM computer_storage_copy_operations o
			 WHERE o.destination_computer_id=p.destination_computer_id
			   AND o.destination_storage_id=p.destination_storage_id
			   AND o.destination_generation=p.destination_generation
			   AND o.operation=p.kind
			 ORDER BY o.operation_revision DESC LIMIT 1)
		FROM storage_provenance p CROSS JOIN provenance_listing_order o ON o.provenance_id=p.provenance_id
  WHERE (p.source_storage_id IN (SELECT storage_id FROM custody)
   OR p.destination_storage_id IN (SELECT storage_id FROM custody))
   AND o.ordinal<=? AND (p.created_ns,p.provenance_id)>(?,?)
  ORDER BY p.created_ns, p.provenance_id LIMIT ?`, computer.StorageID, cursor.HighWater, cursor.CreatedNS, cursor.ProvenanceID, limit+1)
	if err != nil {
		return ComputerStorageProvenance{}, internalError(err, "list Storage provenance")
	}
	for rows.Next() {
		var provenance StorageProvenance
		var destinationComputer, destinationStorage sql.NullString
		var receiptJSON []byte
		var destinationGeneration sql.NullInt64
		var createdNS int64
		if err := rows.Scan(&provenance.ProvenanceID, &provenance.Kind, &provenance.SourceStorageID,
			&provenance.SourceGeneration, &provenance.BackupID, &destinationComputer, &destinationStorage,
			&destinationGeneration, &createdNS, &receiptJSON); err != nil {
			rows.Close()
			return ComputerStorageProvenance{}, internalError(err, "scan Storage provenance")
		}
		provenance.DestinationComputerID = destinationComputer.String
		provenance.DestinationStorageID = destinationStorage.String
		provenance.DestinationGeneration = destinationGeneration.Int64
		provenance.CreatedAt = time.Unix(0, createdNS).UTC()
		if len(receiptJSON) > 0 {
			var receipt ComputerStorageCopyReceipt
			if err := json.Unmarshal(receiptJSON, &receipt); err != nil {
				rows.Close()
				return ComputerStorageProvenance{}, internalError(err, "decode Storage copy receipt")
			}
			provenance.CopyReceipt = &receipt
		}
		if len(projection.Provenance) == limit || (len(projection.Provenance) > 0 && r.pageCutoffReached()) {
			payload, _ := json.Marshal(cursor)
			projection.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
			break
		}
		projection.Provenance = append(projection.Provenance, provenance)
		cursor.CreatedNS = createdNS
		cursor.ProvenanceID = provenance.ProvenanceID
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ComputerStorageProvenance{}, internalError(err, "iterate Storage provenance")
	}
	if err := rows.Close(); err != nil {
		return ComputerStorageProvenance{}, internalError(err, "close Storage provenance")
	}

	rows, err = r.q.QueryContext(ctx, graph+`
		SELECT `+custodyExportColumns+` FROM computer_custody_exports e
		JOIN custody ON custody.storage_id=e.source_storage_id ORDER BY e.requested_ns, e.export_id LIMIT 1001`, computer.StorageID)
	if err != nil {
		return ComputerStorageProvenance{}, internalError(err, "list Storage Custody exports")
	}
	for rows.Next() {
		exported, scanErr := scanCustodyExport(rows)
		if scanErr != nil {
			rows.Close()
			return ComputerStorageProvenance{}, internalError(scanErr, "scan Storage Custody export")
		}
		// A refused export that the helper proved never reached the operator
		// destination is evidence, not taint: no byte of this Storage left
		// managed custody. Every other outcome taints permanently.
		if !contract.CustodyExportLeftDestinationUntouched(exported.Status, exported.FailureCode) {
			projection.CustodyTainted = true
		}
		projection.CustodyExports = append(projection.CustodyExports, exported)
		if len(projection.CustodyExports) > maxComputerProvenanceRows {
			rows.Close()
			return ComputerStorageProvenance{}, custodyGraphLimitError()
		}
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
