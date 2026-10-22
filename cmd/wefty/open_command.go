package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"syscall"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func executeOpen(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) error {
	return executeOpenWithOpener(ctx, clients, jsonOutput, args, stdout, stderr, openServiceBrowser)
}

func executeOpenWithOpener(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer, opener func(context.Context, string) error) error {
	flags := flag.NewFlagSet("open", flag.ContinueOnError)
	flags.SetOutput(flagErrorOutput(stderr, jsonOutput))
	localPortFlag := flags.String("local-port", "", "local port (default: published port; 0 chooses a free port; fleet only)")
	noBrowser := flags.Bool("no-browser", false, "print the URL without opening a browser")
	flags.Usage = func() {
		_, _ = io.WriteString(flags.Output(), "Usage: wefty open SERVICE [--local-port N] [--no-browser]\nOpen a published service. Fleet shortcuts run in the foreground; stop with Ctrl-C.\n")
		flags.PrintDefaults()
	}
	if err := parseCommandFlags(flags, moveFirstPositionalToEnd(args), false); err != nil {
		return flagUsageError(err)
	}
	if flags.NArg() != 1 {
		return usageError("usage: wefty open SERVICE [--local-port N] [--no-browser]")
	}
	portSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "local-port" {
			portSet = true
		}
	})
	localPort := 0
	if portSet {
		var err error
		localPort, err = strconv.Atoi(*localPortFlag)
		if err != nil || localPort < 0 || localPort > 65535 {
			return usageError("--local-port must be between 0 and 65535")
		}
	}
	job, computer, err := resolveServiceTarget(ctx, clients, flags.Arg(0))
	if err != nil {
		return err
	}
	if computer != nil {
		job = &computer.CurrentJob
	}
	if job == nil || job.State != contract.JobRunning || job.ServiceJob == nil || job.PublishedPort == nil || *job.PublishedPort < 1 || *job.PublishedPort > 65535 {
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "service must be running with a published port; check wefty services status " + flags.Arg(0)}}
	}
	remotePort := *job.PublishedPort
	if !clients.participantProfile {
		url := fmt.Sprintf("http://localhost:%d", remotePort)
		if jsonOutput {
			if err := json.NewEncoder(stdout).Encode(openReadyEvent{Event: "ready", Service: job.JobID, Node: job.BoundNodeID, LocalURL: url, RemotePort: remotePort}); err != nil {
				return err
			}
		} else if _, err := fmt.Fprintf(stdout, "%s — service %s on this machine; no proxy started\n", url, job.JobID); err != nil {
			return err
		}
		if !*noBrowser {
			return opener(ctx, url)
		}
		return nil
	}
	node, err := clients.getNode(ctx, job.BoundNodeID)
	if err != nil {
		return err
	}
	if err := fabric.ValidateNetworkName(node.ConnectHost); err != nil {
		return &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "service node has no valid registered network name; check wefty services status " + flags.Arg(0)}}
	}
	if clients.servicePerson == nil {
		return &fabric.ParticipantIdentityError{Person: true, Reason: "is required: this shortcut needs the owner's person sign-in on this laptop"}
	}
	port := remotePort
	if portSet {
		port = localPort
	}
	listener, err := listenOpenShortcut(port, net.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	url := "http://" + listener.Addr().String()
	if jsonOutput {
		err = json.NewEncoder(stdout).Encode(openReadyEvent{Event: "ready", Service: job.JobID, Node: job.BoundNodeID, LocalURL: url, RemotePort: remotePort})
	} else {
		_, err = fmt.Fprintf(stdout, "%s — service %s on machine %s (%s); stop with Ctrl-C\n", url, job.JobID, job.BoundNodeID, node.ConnectHost)
	}
	if err != nil {
		return err
	}
	if !*noBrowser {
		if err := opener(ctx, url); err != nil {
			return err
		}
	}
	return serveOpenShortcut(ctx, listener, clients.servicePerson, net.JoinHostPort(node.ConnectHost, strconv.Itoa(remotePort)))
}

func listenOpenShortcut(port int, listen func(string, string) (net.Listener, error)) (net.Listener, error) {
	listener, err := listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		if errors.Is(err, syscall.EADDRINUSE) {
			return nil, &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: fmt.Sprintf("cannot listen on 127.0.0.1:%d; choose --local-port N", port)}}
		}
		return nil, fmt.Errorf("listen for local service shortcut; choose --local-port N: %w", err)
	}
	return listener, nil
}

type openReadyEvent struct {
	Event      string `json:"event"`
	Service    string `json:"service"`
	Node       string `json:"node"`
	LocalURL   string `json:"local_url"`
	RemotePort int    `json:"remote_port"`
}

// Each local stream owns one person-participant dial. A failed dial closes
// just that stream; cancellation closes the listener and every active pair.
func serveOpenShortcut(ctx context.Context, listener net.Listener, person fabric.Fabric, address string) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); _ = listener.Close(); workers.Wait() }()
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer conn.Close()
			stopLocal := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer stopLocal()
			remote, err := person.Dial(ctx, "tcp", address)
			if err != nil {
				return
			}
			defer remote.Close()
			stopRemote := context.AfterFunc(ctx, func() { _ = remote.Close() })
			defer stopRemote()
			copied := make(chan struct{})
			go func() { defer close(copied); _, _ = io.Copy(remote, conn); _ = remote.Close() }()
			_, _ = io.Copy(conn, remote)
			_ = remote.Close()
			_ = conn.Close()
			<-copied
		}()
	}
}

func openServiceBrowser(ctx context.Context, url string) error {
	var command string
	switch runtime.GOOS {
	case "darwin":
		command = "open"
	case "linux":
		path, err := exec.LookPath("xdg-open")
		if err != nil {
			return nil
		}
		command = path
	default:
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The browser belongs to the owner, not the foreground tunnel's context.
	// xdg-open may wait for its browser; do not block accepting local traffic.
	process := newOpenBrowserCommand(command, url)
	if err := process.Start(); err != nil {
		return err
	}
	go func() { _ = process.Wait() }()
	return nil
}
