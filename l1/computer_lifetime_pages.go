package l1

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Ordinals survive deletion and reinsertion; display keys never fix membership.
type computerLifetimeCursor struct {
	Version    int    `json:"v"`
	Kind       string `json:"kind"`
	ComputerID string `json:"computer_id"`
	HighWater  int64  `json:"high_water"`
	Generation int64  `json:"generation,omitempty"`
	CreatedNS  int64  `json:"created_ns,omitempty"`
	ID         string `json:"id,omitempty"`
}

func encodeComputerLifetimeCursor(cursor computerLifetimeCursor) string {
	payload, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func (r *databaseReads) computerLifetimeCursor(ctx context.Context, kind, computerID, value string, limit int) (computerLifetimeCursor, int, error) {
	cursor := computerLifetimeCursor{Version: 1, Kind: kind, ComputerID: computerID}
	invalid := func() (computerLifetimeCursor, int, error) {
		return cursor, 0, protocolError(contract.ErrorInvalidRequest, "cursor, Computer or limit is invalid")
	}
	if limit < 1 || strings.TrimSpace(computerID) == "" {
		return invalid()
	}
	if limit > MaxComputerListingPageLimit {
		limit = MaxComputerListingPageLimit
	}
	if value != "" {
		payload, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil {
			return invalid()
		}
		decoder := json.NewDecoder(strings.NewReader(string(payload)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&cursor) != nil {
			return invalid()
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || cursor.Version != 1 || cursor.Kind != kind || cursor.ComputerID != computerID || cursor.HighWater < 1 {
			return invalid()
		}
		if (kind == "generations" && (cursor.Generation < 1 || cursor.ID != "" || cursor.CreatedNS != 0)) || (kind == "exports" && (cursor.ID == "" || cursor.Generation != 0)) {
			return invalid()
		}
	}
	var exists int
	if err := r.q.QueryRowContext(ctx, `SELECT 1 FROM computers WHERE computer_id=?`, computerID).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return cursor, 0, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	} else if err != nil {
		return cursor, 0, internalError(err, "read Computer collection authority")
	}
	if value == "" {
		query := `SELECT COALESCE(MAX(ordinal),0) FROM generation_listing_order`
		if kind == "exports" {
			query = `SELECT COALESCE(MAX(ordinal),0) FROM custody_export_listing_order`
		}
		if err := r.q.QueryRowContext(ctx, query).Scan(&cursor.HighWater); err != nil {
			return cursor, 0, err
		}
	}
	return cursor, limit, nil
}
