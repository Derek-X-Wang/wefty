package main

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/Derek-X-Wang/wefty/l1"
)

func executeSeatRelease(ctx context.Context, clients *apiClients, jsonOutput bool, id string, stdout io.Writer) error {
	secret, err := loadSeatCredential(id)
	if err != nil {
		return err
	}
	if secret == "" {
		return seatCredentialUnavailable(id)
	}
	headers := http.Header{l1.SeatCredentialHeader: []string{secret}}
	var result l1.SeatCallResult
	// Released Seats replay without release writes (l1/seat_release.go:18-41);
	// middleware admits that replay even after ending (l1/seat_log_routes.go:96-98).
	if _, err := clients.l1.resendOnBareSnapshotExpiry(ctx, http.MethodPost, "/v1/seats/"+id+"/release", nil, headers, &result, http.StatusOK); err != nil {
		return err
	}
	if result.SeatID != id || result.Reason != "released" || result.State != l1.SeatEnding && result.State != l1.SeatEnded {
		return fmt.Errorf("Seat release response did not confirm release")
	}
	return withSeatReply(stdout, jsonOutput, id, result.UnreadEvents, func(output io.Writer) error {
		if jsonOutput {
			return writeJSON(output, result)
		}
		suffix := ""
		if result.CleanupPending {
			suffix = "; cleanup pending"
		}
		_, err := fmt.Fprintf(output, "Seat %s ended: released%s\n", id, suffix)
		return err
	})
}
