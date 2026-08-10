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
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

type ServerConfig struct {
	CallerPrincipalTag string
	Reconciler         *Reconciler
	Logs               JobLogClient
}

type Server struct {
	fabric             fabric.Fabric
	store              *Store
	callerPrincipalTag string
	reconciler         *Reconciler
	logs               JobLogClient
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
	server := &Server{fabric: f, store: store, callerPrincipalTag: tag, reconciler: config.Reconciler, logs: config.Logs}
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
type runTokenContextKey struct{}

func (s *Server) routes() http.Handler {
	runs := http.NewServeMux()
	runs.HandleFunc("POST /v1/runs", s.createRun)
	runs.HandleFunc("GET /v1/runs/{run_id}", s.getRun)
	runs.HandleFunc("GET /v1/runs/{run_id}/lineage", s.getRunLineage)
	runs.HandleFunc("GET /v1/runs/{run_id}/logs", s.getRunLogs)
	runs.HandleFunc("POST /v1/runs/{run_id}/envelopes", s.appendEnvelope)
	runs.HandleFunc("POST /v1/runs/{run_id}/gates", s.appendGateResult)
	runs.HandleFunc("POST /v1/runs/{run_id}/rerun", s.rerun)
	runs.HandleFunc("POST /v1/runs/{run_id}/cancel", s.notImplemented)

	workflows := http.NewServeMux()
	workflows.HandleFunc("POST /v1/workflows/{workflow_id}/versions", s.createWorkflowVersion)
	workflows.HandleFunc("GET /v1/workflows/{workflow_id}/versions/{version}", s.getWorkflowVersion)

	root := http.NewServeMux()
	root.Handle("/v1/runs", s.authenticateFabric(s.authorize(runs)))
	root.Handle("/v1/runs/", s.authenticateFabric(s.authorize(runs)))
	root.Handle("/v1/workflows/", s.authenticateFabric(s.authorize(s.requireCaller(workflows))))
	return root
}

func (s *Server) authenticateFabric(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := s.fabric.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil || (strings.TrimSpace(identity.NodeID) == "" && strings.TrimSpace(identity.User) == "") {
			writeError(w, protocolError(contract.ErrorUnauthorized, "fabric identity could not be authenticated"))
			return
		}
		ctx := context.WithValue(r.Context(), identityContextKey{}, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := strings.TrimSpace(r.Header.Get("Authorization"))
		if authorization != "" {
			token, ok := strings.CutPrefix(authorization, "Bearer ")
			if !ok || strings.TrimSpace(token) == "" || strings.ContainsAny(token, " \t\r\n") {
				writeError(w, protocolError(contract.ErrorUnauthorized, "run token bearer authorization is malformed"))
				return
			}
			scope, err := s.store.AuthenticateRunToken(r.Context(), token)
			if err != nil {
				writeError(w, err)
				return
			}
			ctx := context.WithValue(r.Context(), runTokenContextKey{}, scope)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		identity := identityFromRequest(r)
		tags, err := normalizeTags(identity.Tags)
		if err != nil || !slices.Contains(tags, s.callerPrincipalTag) {
			writeError(w, protocolError(contract.ErrorForbidden, "fabric identity is not authorized for the L3 caller protocol"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) requireCaller(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := runTokenFromRequest(r); ok {
			writeError(w, protocolError(contract.ErrorForbidden, "run tokens are not authorized for workflow administration"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func identityFromRequest(r *http.Request) fabric.Identity {
	identity, _ := r.Context().Value(identityContextKey{}).(fabric.Identity)
	return identity
}

func runTokenFromRequest(r *http.Request) (RunTokenScope, bool) {
	scope, ok := r.Context().Value(runTokenContextKey{}).(RunTokenScope)
	return scope, ok
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
	actor := actorFromIdentity(identity)
	if scope, ok := runTokenFromRequest(r); ok {
		if request.ParentRunID != scope.RunID {
			writeError(w, protocolError(contract.ErrorForbidden, "run token may dispatch only a direct child of its own run"))
			return
		}
		actor = "run:" + scope.RunID
	}
	record, replayed, err := s.store.CreateRun(r.Context(), CreateRunInput{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Actor:          actor,
		Request:        request,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeRunAccepted(w, record, replayed)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	if scope, ok := runTokenFromRequest(r); ok {
		allowed, err := s.store.CanReadRun(r.Context(), scope.RunID, runID)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allowed {
			writeError(w, protocolError(contract.ErrorForbidden, "run token cannot read an ancestor or sibling run"))
			return
		}
	}
	record, err := s.store.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (s *Server) getRunLineage(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	var scope RunTokenScope
	var scoped bool
	if scope, scoped = runTokenFromRequest(r); scoped {
		allowed, err := s.store.CanReadRun(r.Context(), scope.RunID, runID)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allowed {
			writeError(w, protocolError(contract.ErrorForbidden, "run token cannot read an ancestor or sibling lineage"))
			return
		}
	}
	lineage, err := s.store.GetLineage(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	if scoped {
		lineage.Ancestors, err = s.filterVisibleLineage(r.Context(), scope.RunID, lineage.Ancestors)
		if err == nil {
			lineage.Descendants, err = s.filterVisibleLineage(r.Context(), scope.RunID, lineage.Descendants)
		}
		if err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, lineage)
}

func (s *Server) getRunLogs(w http.ResponseWriter, r *http.Request) {
	if s.logs == nil {
		writeError(w, internalError(errors.New("L1 log client is not configured"), "poll run logs"))
		return
	}
	runID := r.PathValue("run_id")
	if scope, ok := runTokenFromRequest(r); ok {
		allowed, err := s.store.CanReadRun(r.Context(), scope.RunID, runID)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allowed {
			writeError(w, protocolError(contract.ErrorForbidden, "run token cannot read an ancestor or sibling run"))
			return
		}
	}
	limit, err := parseRunLogLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	jobID, dispatched, err := s.store.runJobID(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	cursor := r.URL.Query().Get("cursor")
	if !dispatched {
		writeJSON(w, http.StatusOK, l1.LogPage{Events: []contract.LogEvent{}, NextCursor: cursor})
		return
	}
	page, err := s.logs.GetJobLogs(r.Context(), jobID, cursor, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func parseRunLogLimit(value string) (int, error) {
	if value == "" {
		return l1.DefaultLogPageLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > l1.MaxLogPageLimit {
		return 0, protocolError(contract.ErrorInvalidRequest, "limit must be an integer between 1 and %d", l1.MaxLogPageLimit)
	}
	return limit, nil
}

func (s *Server) filterVisibleLineage(ctx context.Context, ownerRunID string, entries []LineageEntry) ([]LineageEntry, error) {
	visible := make([]LineageEntry, 0, len(entries))
	for _, entry := range entries {
		allowed, err := s.store.CanReadRun(ctx, ownerRunID, entry.RunID)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, entry)
		}
	}
	return visible, nil
}

func (s *Server) appendEnvelope(w http.ResponseWriter, r *http.Request) {
	scope, ok := runTokenFromRequest(r)
	if !ok {
		writeError(w, protocolError(contract.ErrorForbidden, "a run token is required for in-run writes"))
		return
	}
	if r.PathValue("run_id") != scope.RunID {
		writeError(w, protocolError(contract.ErrorForbidden, "run token may write only its own run"))
		return
	}
	raw, err := decodeRawJSON(r)
	if err != nil {
		writeError(w, err)
		return
	}
	value, replayed, err := s.store.AppendEnvelope(r.Context(), scope, raw)
	if err != nil {
		writeError(w, err)
		return
	}
	writeProtocolAppend(w, value, replayed)
}

func (s *Server) appendGateResult(w http.ResponseWriter, r *http.Request) {
	scope, ok := runTokenFromRequest(r)
	if !ok {
		writeError(w, protocolError(contract.ErrorForbidden, "a run token is required for in-run writes"))
		return
	}
	if r.PathValue("run_id") != scope.RunID {
		writeError(w, protocolError(contract.ErrorForbidden, "run token may write only its own run"))
		return
	}
	raw, err := decodeRawJSON(r)
	if err != nil {
		writeError(w, err)
		return
	}
	value, replayed, err := s.store.AppendGateResult(r.Context(), scope, raw)
	if err != nil {
		writeError(w, err)
		return
	}
	writeProtocolAppend(w, value, replayed)
}

func (s *Server) rerun(w http.ResponseWriter, r *http.Request) {
	if _, ok := runTokenFromRequest(r); ok {
		writeError(w, protocolError(contract.ErrorForbidden, "run tokens are not authorized to rerun terminal snapshots"))
		return
	}
	identity := identityFromRequest(r)
	record, replayed, err := s.store.CreateRerun(r.Context(), CreateRerunInput{
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Actor:          actorFromIdentity(identity),
		SourceRunID:    r.PathValue("run_id"),
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeRunAccepted(w, record, replayed)
}

func (s *Server) notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, protocolError(contract.ErrorNotImplemented, "operation is reserved for a future version"))
}

func (s *Server) createWorkflowVersion(w http.ResponseWriter, r *http.Request) {
	var input WorkflowVersionInput
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, err)
		return
	}
	record, err := s.store.CreateWorkflowVersion(r.Context(), r.PathValue("workflow_id"), input)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, record)
}

func (s *Server) getWorkflowVersion(w http.ResponseWriter, r *http.Request) {
	versionPart := r.PathValue("version")
	if len(versionPart) < 2 || versionPart[0] != 'v' {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "workflow version path must use v<version>"))
		return
	}
	version, err := strconv.Atoi(versionPart[1:])
	if err != nil || version < 1 || strconv.Itoa(version) != versionPart[1:] {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "workflow version path must use a positive integer"))
		return
	}
	record, err := s.store.GetWorkflowVersion(r.Context(), r.PathValue("workflow_id"), version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func writeRunAccepted(w http.ResponseWriter, record contract.RunRecord, replayed bool) {
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

func decodeRawJSON(r *http.Request) (json.RawMessage, error) {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 16<<20))
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, protocolError(contract.ErrorInvalidRequest, "invalid JSON request: %v", err)
	}
	err := decoder.Decode(&struct{}{})
	if err == nil {
		return nil, protocolError(contract.ErrorInvalidRequest, "request body must contain one JSON value")
	}
	if !errors.Is(err, io.EOF) {
		return nil, protocolError(contract.ErrorInvalidRequest, "invalid trailing JSON: %v", err)
	}
	return raw, nil
}

func writeProtocolAppend(w http.ResponseWriter, value any, replayed bool) {
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, value)
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
