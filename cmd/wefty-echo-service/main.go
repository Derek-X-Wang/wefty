package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-echo-service: %v", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) == 2 && os.Args[1] == "--once" {
		return runOnce(http.DefaultClient, os.Stdout, os.Stderr)
	}
	if len(os.Args) == 2 && os.Args[1] == "--portless" {
		return runPortless()
	}
	if len(os.Args) != 1 {
		return errors.New("usage: wefty-echo-service [--once|--portless]")
	}
	port := os.Getenv(contract.EnvServicePort)
	if port == "" {
		return fmt.Errorf("%s is required", contract.EnvServicePort)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("%s must be a TCP port number from 1 to 65535", contract.EnvServicePort)
	}
	serviceDirectory := os.Getenv(contract.EnvServiceDir)
	if serviceDirectory == "" {
		return fmt.Errorf("%s is required", contract.EnvServiceDir)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler(serviceDirectory, portNumber))
	mux.HandleFunc("/echo", echoHandler)
	server := &http.Server{
		Addr:              net.JoinHostPort("127.0.0.1", strconv.Itoa(portNumber)),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("wefty-echo-service: starting payload pid=%d", os.Getpid())

	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownDone := make(chan error, 1)
	go func() {
		<-shutdownSignal.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownDone <- server.Shutdown(shutdownContext)
	}()

	err = server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if shutdownSignal.Err() == nil {
		return errors.New("HTTP server closed without a shutdown signal")
	}
	if err := <-shutdownDone; err != nil {
		return fmt.Errorf("shut down HTTP server: %w", err)
	}
	return nil
}

func runOnce(client *http.Client, stdout, stderr io.Writer) error {
	handoffDirectory := os.Getenv(contract.EnvHandoffDir)
	endpoint := strings.TrimRight(os.Getenv(contract.EnvL3Endpoint), "/")
	runToken := os.Getenv(contract.EnvRunToken)
	runID := os.Getenv(contract.EnvRunID)
	for name, value := range map[string]string{
		contract.EnvHandoffDir: handoffDirectory,
		contract.EnvL3Endpoint: endpoint,
		contract.EnvRunToken:   runToken,
		contract.EnvRunID:      runID,
	} {
		if value == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if client == nil {
		client = http.DefaultClient
	}
	markerPath := filepath.Join(handoffDirectory, "wefty-echo-once.txt")
	if err := os.WriteFile(markerPath, []byte("wefty echo one-shot handoff\n"), 0o600); err != nil {
		return fmt.Errorf("write one-shot handoff: %w", err)
	}
	request, err := http.NewRequest(http.MethodGet, endpoint+"/v1/runs/"+url.PathEscape(runID), nil)
	if err != nil {
		return fmt.Errorf("create L3 bridge request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+runToken)
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("call L3 bridge: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("L3 bridge returned HTTP %d", response.StatusCode)
	}
	if _, err := fmt.Fprintln(stdout, "wefty-echo-once-stdout"); err != nil {
		return fmt.Errorf("write stdout marker: %w", err)
	}
	if _, err := fmt.Fprintln(stderr, "wefty-echo-once-stderr"); err != nil {
		return fmt.Errorf("write stderr marker: %w", err)
	}
	return nil
}

func runPortless() error {
	if os.Getenv(contract.EnvServiceDir) == "" {
		return fmt.Errorf("%s is required", contract.EnvServiceDir)
	}
	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-shutdownSignal.Done()
	return nil
}

func healthHandler(serviceDirectory string, listeningPort int) http.HandlerFunc {
	return func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(response).Encode(struct {
			PID              int    `json:"pid"`
			ServiceDirectory string `json:"service_directory"`
			ListeningPort    int    `json:"listening_port"`
		}{PID: os.Getpid(), ServiceDirectory: serviceDirectory, ListeningPort: listeningPort}); err != nil {
			log.Printf("wefty-echo-service: write health response: %v", err)
		}
	}
}

func echoHandler(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/octet-stream")
	flusher, _ := response.(http.Flusher)
	buffer := make([]byte, 32*1024)
	for {
		count, readErr := request.Body.Read(buffer)
		if count > 0 {
			if _, writeErr := response.Write(buffer[:count]); writeErr != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				log.Printf("wefty-echo-service: read echo request: %v", readErr)
			}
			return
		}
	}
}
