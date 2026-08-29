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
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const maximumControlJSONBytes = 1 << 20

type Server struct {
	path        string
	service     Service
	listener    net.Listener
	server      *http.Server
	allowedUIDs map[uint32]struct{}
}

func NewServer(path string, service Service, allowedUIDs ...uint32) (*Server, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) == string(filepath.Separator) {
		return nil, errors.New("OCI control socket path must be absolute and non-root")
	}
	if service == nil {
		return nil, errors.New("OCI control service is required")
	}
	if len(allowedUIDs) == 0 {
		allowedUIDs = []uint32{uint32(os.Geteuid())}
	}
	allowlist := make(map[uint32]struct{}, len(allowedUIDs))
	for _, uid := range allowedUIDs {
		allowlist[uid] = struct{}{}
	}
	return &Server{path: filepath.Clean(path), service: service, allowedUIDs: allowlist}, nil
}

func (server *Server) Serve(ctx context.Context) error {
	if server == nil {
		return errors.New("OCI control server is unavailable")
	}
	lock, err := acquireSocketLock(server.path)
	if err != nil {
		return err
	}
	defer releaseSocketLock(lock)
	if err := prepareSocketPath(server.path); err != nil {
		return err
	}
	listener, err := net.Listen("unix", server.path)
	if err != nil {
		return fmt.Errorf("listen on OCI control socket: %w", err)
	}
	authenticated := &peerAuthenticatedListener{Listener: listener, allowedUIDs: server.allowedUIDs}
	server.listener = authenticated
	if err := os.Chmod(server.path, 0o600); err != nil {
		_ = listener.Close()
		return fmt.Errorf("restrict OCI control socket: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/doctor", server.handleDoctor)
	mux.HandleFunc("GET /v1/intent", server.handleIntent)
	mux.HandleFunc("POST /v1/setup", server.handleSetup)
	mux.HandleFunc("POST /v1/oci/start", server.handleStart)
	mux.HandleFunc("POST /v1/oci/stop", server.handleStop)
	mux.HandleFunc("POST /v1/images/load", server.handleLoadImage)
	server.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Minute}
	shutdown := context.AfterFunc(ctx, func() { _ = server.server.Close() })
	defer shutdown()
	err = server.server.Serve(authenticated)
	_ = os.Remove(server.path)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}

func (server *Server) handleDoctor(writer http.ResponseWriter, request *http.Request) {
	value, err := server.service.Doctor(request.Context())
	writeControlResponse(writer, value, err)
}

func prepareSocketPath(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create OCI control directory: %w", err)
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
	if connection, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond); dialErr == nil {
		_ = connection.Close()
		return errors.New("OCI control socket is already active")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale OCI control socket: %w", err)
	}
	return nil
}

type peerAuthenticatedListener struct {
	net.Listener
	allowedUIDs map[uint32]struct{}
}

func (listener *peerAuthenticatedListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		uid, err := unixPeerUID(connection)
		if _, allowed := listener.allowedUIDs[uid]; err == nil && allowed {
			return connection, nil
		}
		_ = connection.Close()
	}
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
		failure := &ControlError{Code: ErrorInternal, Status: http.StatusInternalServerError, Message: "node-local OCI control failed", Cause: err}
		if !errors.As(err, &failure) {
			failure.Cause = err
		}
		writeControlErrorDetails(writer, failure.Status, failure.Code, failure.Message, helperMechanicsDetails(err))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(value)
}

func writeControlError(writer http.ResponseWriter, status int, code contract.ErrorCode, message string) {
	writeControlErrorDetails(writer, status, code, message, nil)
}

func writeControlErrorDetails(writer http.ResponseWriter, status int, code contract.ErrorCode, message string, details map[string]any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: message, Details: details}})
}

// helperMechanicsDetails preserves only the helper protocol's closed,
// sanitized facts. Engine errors and paths remain helper-local.
func helperMechanicsDetails(err error) map[string]any {
	var failure *ocihelper.RPCError
	if !errors.As(err, &failure) {
		var reasoned interface{ ControlFailureReason() string }
		if errors.As(err, &reasoned) {
			switch reason := reasoned.ControlFailureReason(); reason {
			case string(ocihelper.CodeInvalidRequest), string(ocihelper.CodeEngineFailure),
				string(ocihelper.CodeDiagnosticFailure), string(ocihelper.CodeImageUnavailable):
				return map[string]any{"reason": reason}
			}
		}
		return map[string]any{"reason": string(contract.ErrorInternal)}
	}
	details := map[string]any{"reason": string(failure.Code)}
	if failure.ImageFailure != nil {
		details["image_failure"] = failure.ImageFailure
	}
	if failure.MemoryFailure != nil {
		details["memory_failure"] = failure.MemoryFailure
	}
	if failure.DiskFailure != nil {
		details["disk_failure"] = failure.DiskFailure
	}
	return details
}
