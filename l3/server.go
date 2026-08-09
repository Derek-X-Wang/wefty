package l3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type ServerConfig struct {
	CallerPrincipalTag string
	Reconciler         *Reconciler
}

type Server struct {
	fabric             fabric.Fabric
	store              *Store
	callerPrincipalTag string
	reconciler         *Reconciler
	handler            http.Handler
}

func NewServer(f fabric.Fabric, store *Store, config ServerConfig) (*Server, error) {
	if f == nil {
		return nil, fmt.Errorf("l3: fabric is required")
	}
	if store == nil {
		return nil, fmt.Errorf("l3: store is required")
	}
	tag := strings.ToLower(strings.TrimSpace(config.CallerPrincipalTag))
	if tag == "" {
		tag = DefaultCallerPrincipalTag
	}
	server := &Server{fabric: f, store: store, callerPrincipalTag: tag, reconciler: config.Reconciler}
	server.handler = server.routes()
	return server, nil
}

func (s *Server) Handler() http.Handler { return s.handler }

func (s *Server) ListenAndServe(ctx context.Context, network, address string) error {
	listener, err := s.fabric.Listen(network, address)
	if err != nil {
		return err
	}
	return s.Serve(ctx, listener)
}

func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	serveCtx, stopReconciler := context.WithCancel(ctx)
	defer stopReconciler()
	if s.reconciler != nil {
		go func() { _ = s.reconciler.Run(serveCtx) }()
	}
	httpServer := &http.Server{Handler: s.handler}
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = httpServer.Close()
		case <-stopped:
		}
	}()
	err := httpServer.Serve(listener)
	close(stopped)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}

type identityContextKey struct{}

func (s *Server) routes() http.Handler {
	runs := http.NewServeMux()
	runs.HandleFunc("POST /v1/runs", s.createRun)
	runs.HandleFunc("GET /v1/runs/{run_id}", s.getRun)

	root := http.NewServeMux()
	root.Handle("/v1/runs", s.authorize(runs))
	root.Handle("/v1/runs/", s.authorize(runs))
	return root
}

func (s *Server) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := s.fabric.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil {
			writeError(w, protocolError(contract.ErrorUnauthorized, "fabric identity could not be authenticated"))
			return
		}
		tags, err := normalizeTags(identity.Tags)
		if err != nil || !slices.Contains(tags, s.callerPrincipalTag) {
			writeError(w, protocolError(contract.ErrorForbidden, "fabric identity is not authorized for the L3 caller protocol"))
			return
		}
		ctx := context.WithValue(r.Context(), identityContextKey{}, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func identityFromRequest(r *http.Request) fabric.Identity {
	identity, _ := r.Context().Value(identityContextKey{}).(fabric.Identity)
	return identity
}

func actorFromIdentity(identity fabric.Identity) string {
	if strings.TrimSpace(identity.User) != "" {
		return identity.User
	}
	return identity.NodeID
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var request CreateRunRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	record, replayed, err := s.store.CreateRun(r.Context(), CreateRunInput{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Actor:          actorFromIdentity(identity),
		Request:        request,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, RunAccepted{
		RunID: record.RunID, StatusURL: "/v1/runs/" + record.RunID,
		LogsURL: "/v1/runs/" + record.RunID + "/logs",
	})
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	record, err := s.store.GetRun(r.Context(), r.PathValue("run_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 16<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return protocolError(contract.ErrorInvalidRequest, "invalid JSON request: %v", err)
	}
	err := decoder.Decode(&struct{}{})
	if err == nil {
		return protocolError(contract.ErrorInvalidRequest, "request body must contain one JSON value")
	}
	if !errors.Is(err, io.EOF) {
		return protocolError(contract.ErrorInvalidRequest, "invalid trailing JSON: %v", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	code, retryable := errorDetails(err)
	status := http.StatusConflict
	switch code {
	case contract.ErrorInvalidRequest:
		status = http.StatusBadRequest
	case contract.ErrorUnauthorized:
		status = http.StatusUnauthorized
	case contract.ErrorForbidden:
		status = http.StatusForbidden
	case contract.ErrorNotFound:
		status = http.StatusNotFound
	case contract.ErrorNotImplemented:
		status = http.StatusNotImplemented
	case contract.ErrorInternal:
		status = http.StatusInternalServerError
	}
	message := err.Error()
	if code == contract.ErrorInternal {
		message = "internal server error"
	}
	writeJSON(w, status, contract.ErrorResponse{Error: contract.APIError{Code: code, Message: message, Retryable: retryable}})
}
