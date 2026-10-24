package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const seatUsage = `usage: wefty seat request --network internet|none [--dedicated] [--window 1h|8h|24h]
       wefty seat wait SEAT_ID
       wefty seat release SEAT_ID
       wefty seat window close

Request one Seat, or wait again by its ID after an interrupted call.
The network must be named. Only the owner's native dialog can approve.
`

// seatCleanupWaitLimit bounds one CLI observation, not L1 cleanup authority.
const seatCleanupWaitLimit = time.Minute

var seatCLIIdentifier = regexp.MustCompile(`^seat_[0-9a-f]{32}$`)

type seatRequestFlags struct {
	network   string
	dedicated bool
	window    string
}

func parseSeatRequestFlags(args []string) (seatRequestFlags, error) {
	var result seatRequestFlags
	flags := flag.NewFlagSet("seat request", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&result.network, "network", "", "internet or none (required)")
	flags.BoolVar(&result.dedicated, "dedicated", false, "new Computer with a Seat limit of 1")
	flags.StringVar(&result.window, "window", "", "approval window: 1h, 8h or 24h")
	if err := flags.Parse(args); err != nil {
		return result, usageError(seatUsage + err.Error())
	}
	if flags.NArg() != 0 || (result.network != "internet" && result.network != "none") || (result.window != "" && result.window != "1h" && result.window != "8h" && result.window != "24h") {
		return result, usageError(seatUsage)
	}
	return result, nil
}
func validateSeatArgs(args []string) error {
	if len(args) == 0 {
		return usageError(seatUsage)
	}
	switch args[0] {
	case "request":
		_, err := parseSeatRequestFlags(args[1:])
		return err
	case "wait", "release":
		if len(args) == 2 && seatCLIIdentifier.MatchString(args[1]) {
			return nil
		}
	case "window":
		if len(args) == 2 && args[1] == "close" {
			return nil
		}
	}
	return usageError(seatUsage)
}
func executeSeatRequest(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	if err := validateSeatArgs(args); err != nil {
		return err
	}
	if args[0] == "release" {
		return executeSeatRelease(ctx, clients, jsonOutput, args[1], stdout)
	}
	id := ""
	if args[0] == "request" {
		flags, _ := parseSeatRequestFlags(args[1:])
		local, err := resolveLocalNode(ctx, clients)
		if err != nil {
			return err
		}
		id = l1.NewSeatRequestID()
		folder, err := os.Getwd()
		if err != nil {
			return err
		}
		harness := seatHarnessLabel()
		input := l1.SeatCallInput{SeatID: id, NodeID: local.NodeID, Harness: harness, Folder: folder, Network: flags.network, Form: "any"}
		if flags.dedicated {
			input.Form = "dedicated"
		}
		switch flags.window {
		case "1h":
			input.WindowHours = 1
		case "8h":
			input.WindowHours = 8
		case "24h":
			input.WindowHours = 24
		}
		// Print before the first mutation. A lost POST response is replayed by wait,
		// using the private input receipt, under the same ID and caller identity.
		if err := storeSeatCallInput(input); err != nil {
			return err
		}
		if jsonOutput {
			err = writeJSON(stdout, map[string]string{"seat_id": id, "state": "requested"})
		} else {
			_, err = fmt.Fprintln(stdout, id)
		}
		if err != nil {
			return err
		}
		var result l1.SeatCallResult
		if err = clients.l1.do(ctx, http.MethodPost, "/v1/seat-requests", input, nil, &result, http.StatusOK); err != nil {
			return err
		}
	} else {
		id = args[1]
	}
	return waitSeatCall(ctx, clients, jsonOutput, id, stdout)
}
func seatHarnessLabel() string {
	labels := []string{}
	if os.Getenv("CLAUDECODE") != "" {
		labels = append(labels, "Claude Code")
	}
	if os.Getenv("CODEX_SANDBOX") != "" {
		labels = append(labels, "Codex")
	}
	if len(labels) == 0 {
		return "unknown"
	}
	return strings.Join(labels, ", ")
}
func waitSeatCall(ctx context.Context, clients *apiClients, jsonOutput bool, id string, stdout io.Writer) error {
	now := clients.seatWaitNow
	if now == nil {
		now = time.Now
	}
	wait := clients.wait
	if wait == nil {
		wait = waitForContext
	}
	var cleanupDeadline time.Time
	for {
		var result l1.SeatCallResult
		err := clients.l1.do(ctx, http.MethodGet, "/v1/seat-requests/"+id, nil, nil, &result, http.StatusOK)
		var apiErr *apiResponseError
		if err != nil && errors.As(err, &apiErr) && apiErr.APIError.Code == contract.ErrorNotFound {
			input, e := loadSeatCallInput(id)
			if e != nil {
				return err
			}
			err = clients.l1.do(ctx, http.MethodPost, "/v1/seat-requests", input, nil, &result, http.StatusOK)
		}
		if err != nil {
			return err
		}
		if result.SeatID != id {
			return fmt.Errorf("Seat request response changed its ID")
		}
		switch result.State {
		case l1.SeatLive:
			if result.Credential == "" {
				return fmt.Errorf("ready Seat has no credential")
			}
			path, err := storeSeatCredential(id, result.Credential)
			if err != nil {
				return err
			}
			result.Credential = ""
			return withSeatReply(stdout, jsonOutput, id, result.UnreadEvents, func(output io.Writer) error {
				if jsonOutput {
					return writeJSON(output, struct {
						l1.SeatCallResult
						CredentialFile string `json:"credential_file"`
					}{result, path})
				}
				_, err = fmt.Fprintf(output, "Seat %s ready on %s; credential stored in %s\n", id, result.ComputerID, path)
				return err
			})
		case l1.SeatRefused, l1.SeatEnded, l1.SeatEnding:
			if result.CleanupPending {
				if cleanupDeadline.IsZero() {
					cleanupDeadline = now().Add(seatCleanupWaitLimit)
				}
				if now().Before(cleanupDeadline) {
					break
				}
			}
			outcome := seatCallRefusal(id, result.Reason)
			if result.State != l1.SeatRefused {
				outcome = &apiResponseError{APIError: contract.APIError{Code: seatErrorEnded, Message: "Seat ended: " + result.Reason, Details: map[string]any{"seat_id": id}}}
			}
			if result.CleanupPending {
				outcome.APIError.Message += "; cleanup still pending; L1 continues cleanup; check again with wefty seat wait " + id
				outcome.APIError.Details["cleanup_pending"] = true
			}
			return withSeatReply(stdout, jsonOutput, id, result.UnreadEvents, func(output io.Writer) error { return outcome })
		}
		if err := wait(ctx, 250*time.Millisecond); err != nil {
			return withSeatReply(stdout, jsonOutput, id, result.UnreadEvents, func(io.Writer) error {
				return fmt.Errorf("Seat request %s still recorded; wait again with wefty seat wait %s: %w", id, id, err)
			})
		}
	}
}

func seatCallRefusal(id, reason string) *apiResponseError {
	code := seatErrorStartFailed
	switch {
	case reason == "Computer room is not configured on this node":
		code = seatErrorRoomUnconfigured
	case reason == "the node did not pick up the request":
		code = seatErrorNodeUnavailable
	case strings.HasPrefix(reason, "Seat approval window ") && strings.Contains(reason, "allowed windows:"):
		code = seatErrorWindowNotAllowed
	case reason == "out of room, try again later":
		code = seatErrorRoomFull
	case strings.HasPrefix(reason, "dedicated Computer cap reached"):
		code = seatErrorDedicatedCap
	case reason == "no answer from the owner" || reason == "approval dialog lost" || reason == "approval dialog unavailable":
		code = seatErrorNoAnswer
	case reason == "denied by the owner":
		code = seatErrorDenied
	case reason == "the host VM is off":
		code = seatErrorHostOff
	}
	return &apiResponseError{APIError: contract.APIError{Code: code, Message: reason, Retryable: code == seatErrorRoomFull || code == seatErrorNoAnswer || code == seatErrorNodeUnavailable, Details: map[string]any{"seat_id": id}}}
}
