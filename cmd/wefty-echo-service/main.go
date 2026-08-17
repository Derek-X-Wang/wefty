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
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const servicePortEnvironment = "WEFTY_SERVICE_PORT"

func main() {
	if err := run(); err != nil {
		log.Printf("wefty-echo-service: %v", err)
		os.Exit(1)
	}
}

func run() error {
	port := os.Getenv(servicePortEnvironment)
	if port == "" {
		return fmt.Errorf("%s is required", servicePortEnvironment)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("%s must be a TCP port number from 1 to 65535", servicePortEnvironment)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("/echo", echoHandler)
	server := &http.Server{
		Addr:              net.JoinHostPort("127.0.0.1", strconv.Itoa(portNumber)),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

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

func healthHandler(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(struct {
		PID int `json:"pid"`
	}{PID: os.Getpid()}); err != nil {
		log.Printf("wefty-echo-service: write health response: %v", err)
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
