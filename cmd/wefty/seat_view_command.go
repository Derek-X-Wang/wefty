package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/seatviewer"
	"github.com/Derek-X-Wang/wefty/internal/takeover"
	"github.com/Derek-X-Wang/wefty/l1"
)

const seatViewUsage = `usage: wefty seat view SEAT_ID [--no-open] [--json]

Open the owner's browser viewer through a local relay. Requires administrator person authority.
Keep this command running while viewing; Ctrl-C/TERM stops the relay and its connections.
The printed link contains a one-use secret. Open it only in the owner's browser.
--no-open prints the link without opening a browser. --json prints one {"url":"..."} line.
Both modes keep the relay running until interrupted, the Seat ends, or the session ends.
An AI agent must not open this viewer itself: the viewer counts as a person.
`

type seatViewFlags struct {
	id     string
	noOpen bool
}

func parseSeatViewFlags(args []string) (result seatViewFlags, err error) {
	flags := flag.NewFlagSet("seat view", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.BoolVar(&result.noOpen, "no-open", false, "print the URL without opening the browser")
	if err = flags.Parse(moveFirstPositionalToEnd(args)); err != nil {
		return result, usageError(seatViewUsage + err.Error())
	}
	if flags.NArg() != 1 || !seatCLIIdentifier.MatchString(flags.Arg(0)) {
		return result, usageError(seatViewUsage)
	}
	result.id = flags.Arg(0)
	return result, nil
}

func executeSeatView(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer) error {
	return executeSeatViewWithOpener(ctx, clients, jsonOutput, args, stdout, openSeatViewerBrowser)
}
func executeSeatViewWithOpener(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout io.Writer, open func(context.Context, string) error) error {
	flags, err := parseSeatViewFlags(args)
	if err != nil {
		return err
	}
	var target l1.SeatViewerTarget
	if err = clients.l1.do(ctx, http.MethodGet, "/v1/seats/"+flags.id+"/viewer-target", nil, nil, &target, http.StatusOK); err != nil {
		return err
	}
	// Discovery supplies the door; the browser gets only the local relay (#1336).
	parsed, err := url.Parse(target.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/v1/seats/"+flags.id+"/person/viewer" || (parsed.Scheme != "http" && parsed.Scheme != "https") || strings.ContainsAny(target.URL, "\r\n") {
		return fmt.Errorf("Seat viewer address is unavailable")
	}
	relay, err := seatviewer.Start(ctx, clients.fabric, parsed)
	if err != nil {
		return fmt.Errorf("start Seat viewer relay: %w", err)
	}
	defer relay.Close()
	if jsonOutput {
		err = writeJSONLine(stdout, l1.SeatViewerTarget{URL: relay.URL})
	} else {
		_, err = fmt.Fprintln(stdout, relay.URL)
	}
	if err != nil {
		return err
	}
	if !flags.noOpen {
		if err = open(ctx, relay.URL); err != nil {
			return err
		}
	}
	// The owner may leave --no-open waiting before opening the browser. This
	// query-only read observes an ended Seat without starting a viewing session.
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	return monitorSeatViewer(ctx, relay, ticker.C, func(ctx context.Context) error {
		var current l1.SeatViewerTarget
		return clients.l1.do(ctx, http.MethodGet, "/v1/seats/"+flags.id+"/viewer-target", nil, nil, &current, http.StatusOK)
	})
}

// Transient L1/node restarts do not close the browser. One minute without a
// successful query is the bound; only a typed Seat end ends viewing at once.
func monitorSeatViewer(ctx context.Context, relay *seatviewer.Relay, ticks <-chan time.Time, poll func(context.Context) error) error {
	var failedSince time.Time
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("Seat viewer stopped: %w", ctx.Err())
		case <-relay.Done():
			return relay.Err()
		case now := <-ticks:
			pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := poll(pollCtx)
			cancel()
			if err == nil {
				failedSince = time.Time{}
				continue
			}
			var reply *apiResponseError
			if errors.As(err, &reply) && reply.APIError.Code == "seat_ended" {
				relay.End(&takeover.ActionError{APIError: reply.APIError})
				select {
				case <-ctx.Done():
					return fmt.Errorf("Seat viewer stopped: %w", ctx.Err())
				case <-relay.Done():
					return relay.Err()
				}
			}
			if errors.As(err, &reply) {
				switch reply.APIError.Code {
				case contract.ErrorUnauthorized, contract.ErrorForbidden, contract.ErrorPrincipalForbidden, contract.ErrorAdminRequired, contract.ErrorNotFound:
					// A lost owner role or a vanished Seat does not recover by waiting.
					return err
				}
			}
			if failedSince.IsZero() {
				failedSince = now
			}
			if now.Sub(failedSince) >= time.Minute {
				return &unavailableError{cause: fmt.Errorf("Seat viewer could not check the Seat for 60 seconds: %w", err)}
			}
		}
	}
}

// Start the owner's browser without waiting for it to exit.
func openSeatViewerBrowser(ctx context.Context, url string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	command := "xdg-open"
	if runtime.GOOS == "darwin" {
		command = "open"
	}
	process := newOpenBrowserCommand(command, url)
	if err := process.Start(); err != nil {
		return fmt.Errorf("open Seat viewer (the URL is printed above): %w", err)
	}
	go func() { _ = process.Wait() }()
	return nil
}
