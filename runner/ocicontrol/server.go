package ocicontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const maximumControlJSONBytes = 1 << 20

type Server struct {
	path     string
	service  Service
	listener net.Listener
	server   *http.Server
}

func NewServer(path string, service Service) (*Server, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) == string(filepath.Separator) {
		return nil, errors.New("OCI control socket path must be absolute and non-root")
	}
	if service == nil {
		return nil, errors.New("OCI control service is required")
	}
	return &Server{path: filepath.Clean(path), service: service}, nil
}

func (server *Server) Serve(ctx context.Context) error {
	if server == nil {
		return errors.New("OCI control server is unavailable")
	}
	if err := prepareSocketPath(server.path); err != nil {
		return err
	}
	listener, err := net.Listen("unix", server.path)
	if err != nil {
		return fmt.Errorf("listen on OCI control socket: %w", err)
	}
	server.listener = listener
	if err := os.Chmod(server.path, 0o600); err != nil {
		_ = listener.Close()
		return fmt.Errorf("restrict OCI control socket: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/intent", server.handleIntent)
	mux.HandleFunc("POST /v1/setup", server.handleSetup)
	mux.HandleFunc("POST /v1/oci/start", server.handleStart)
	mux.HandleFunc("POST /v1/oci/stop", server.handleStop)
	mux.HandleFunc("POST /v1/images/load", server.handleLoadImage)
	server.server = &http.Server{Handler: mux, ReadHeaderTimeout: 0}
	shutdown := context.AfterFunc(ctx, func() { _ = server.server.Close() })
	defer shutdown()
	err = server.server.Serve(listener)
	_ = os.Remove(server.path)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}

func prepareSocketPath(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create OCI control directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("restrict OCI control directory: %w", err)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect OCI control socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refusing to replace a non-socket OCI control path")
	}
	if connection, dialErr := net.Dial("unix", path); dialErr == nil {
		_ = connection.Close()
		return errors.New("OCI control socket is already active")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale OCI control socket: %w", err)
	}
	return nil
}

func (server *Server) handleIntent(writer http.ResponseWriter, request *http.Request) {
	value, err := server.service.Intent(request.Context())
	writeControlResponse(writer, value, err)
}

func (server *Server) handleSetup(writer http.ResponseWriter, request *http.Request) {
	var body SetupRequest
	if !decodeControlJSON(writer, request, &body) {
		return
	}
	value, err := server.service.Setup(request.Context(), body)
	writeControlResponse(writer, value, err)
}

func (server *Server) handleStart(writer http.ResponseWriter, request *http.Request) {
	var body IntentMutationRequest
	if !decodeControlJSON(writer, request, &body) {
		return
	}
	value, err := server.service.Start(request.Context(), body)
	writeControlResponse(writer, value, err)
}

func (server *Server) handleStop(writer http.ResponseWriter, request *http.Request) {
	var body IntentMutationRequest
	if !decodeControlJSON(writer, request, &body) {
		return
	}
	value, err := server.service.Stop(request.Context(), body)
	writeControlResponse(writer, value, err)
}

func (server *Server) handleLoadImage(writer http.ResponseWriter, request *http.Request) {
	if contentType := request.Header.Get("Content-Type"); contentType != "application/vnd.oci.image.layer.v1.tar" {
		writeControlError(writer, http.StatusBadRequest, ErrorInvalidRequest, "load-image requires an OCI tar archive")
		return
	}
	value, err := server.service.LoadImage(request.Context(), request.Body)
	writeControlResponse(writer, value, err)
}

func decodeControlJSON(writer http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(io.LimitReader(request.Body, maximumControlJSONBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeControlError(writer, http.StatusBadRequest, ErrorInvalidRequest, "invalid node-local control request")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeControlError(writer, http.StatusBadRequest, ErrorInvalidRequest, "node-local control request must contain one JSON value")
		return false
	}
	return true
}

func writeControlResponse(writer http.ResponseWriter, value any, err error) {
	if err != nil {
		code := ErrorInternal
		status := http.StatusInternalServerError
		message := err.Error()
		lower := strings.ToLower(message)
		switch {
		case strings.Contains(lower, "revision conflict"):
			code, status = ErrorIntentConflict, http.StatusConflict
		case strings.Contains(lower, "unavailable") || strings.Contains(lower, "disabled"):
			code, status = ErrorRuntimeUnavailable, http.StatusServiceUnavailable
		case strings.Contains(lower, "requires") || strings.Contains(lower, "invalid"):
			code, status = ErrorInvalidRequest, http.StatusBadRequest
		}
		writeControlError(writer, status, code, message)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}

func writeControlError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(ErrorResponse{Code: code, Message: message})
}
