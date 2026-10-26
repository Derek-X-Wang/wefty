package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"

	"github.com/Derek-X-Wang/wefty/l1"
)

const seatViewUsage = `usage: wefty seat view SEAT_ID [--no-open] [--json]

Open the owner's browser viewer. Requires administrator person authority.
--no-open prints the URL without opening a browser. --json prints {"url":"..."} only.
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
	// Only a front-door HTTP URL can be handed to the operating system.
	parsed, err := url.Parse(target.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/v1/seats/"+flags.id+"/person/viewer" || (parsed.Scheme != "http" && parsed.Scheme != "https") || strings.ContainsAny(target.URL, "\r\n") {
		return fmt.Errorf("Seat viewer address is unavailable")
	}
	if jsonOutput {
		return writeJSON(stdout, target)
	}
	if _, err = fmt.Fprintln(stdout, target.URL); err != nil {
		return err
	}
	if flags.noOpen {
		return nil
	}
	return open(ctx, target.URL)
}

// The browser belongs to the owner and can outlive this CLI call.
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
