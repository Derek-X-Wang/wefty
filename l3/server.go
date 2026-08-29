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
	ControlPlaneNodeID string
	Reconciler         *Reconciler
	Jobs               JobClient
	Logs               JobLogClient
	ComputerGrants     ComputerGrantVerifier
}

type Server struct {
	fabric              fabric.Fabric
	store               *Store
	callerPrincipalTag  string
	controlPlaneNodeID  string
	reconciler          *Reconciler
	jobs                JobClient
	logs                JobLogClient
	computerGrants      ComputerGrantVerifier
	filterRunVisibility func(context.Context, string, string) (bool, error)
	handler             http.Handler
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
	controlPlaneNodeID := strings.TrimSpace(config.ControlPlaneNodeID)
	if controlPlaneNodeID == "" {
		controlPlaneNodeID = "control-plane"
	}
	jobs := config.Jobs
	if jobs == nil && config.Reconciler != nil {
		jobs = config.Reconciler.jobs
	}
	computerGrants := config.ComputerGrants
	if computerGrants == nil {
		computerGrants, _ = jobs.(ComputerGrantVerifier)
	}
	server := &Server{fabric: f, store: store, callerPrincipalTag: tag, controlPlaneNodeID: controlPlaneNodeID,
		reconciler: config.Reconciler, jobs: jobs, logs: config.Logs, computerGrants: computerGrants}
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
type computerTokenContextKey struct{}

func (s *Server) routes() http.Handler {
	runs := http.NewServeMux()
	runs.HandleFunc("GET /v1/runs/{run_id}/execution", s.getRunExecution)
	runs.HandleFunc("POST /v1/runs/{run_id}/envelopes", s.appendEnvelope)
	runs.HandleFunc("POST /v1/runs/{run_id}/gates", s.appendGateResult)
	runs.HandleFunc("POST /v1/runs/{run_id}/rerun", s.rerun)
	runs.HandleFunc("POST /v1/runs/{run_id}/cancel", s.notImplemented)

	workflows := http.NewServeMux()
	workflows.HandleFunc("POST /v1/workflows/{workflow_id}/versions", s.createWorkflowVersion)
	workflows.HandleFunc("GET /v1/workflows/{workflow_id}/versions/{version}", s.getWorkflowVersion)

	root := http.NewServeMux()
	root.Handle("/v1/computer-token/mint", s.authenticateFabric(http.HandlerFunc(s.mintComputerToken)))
	root.Handle("/v1/computer-token/revoke", s.authenticateFabric(http.HandlerFunc(s.revokeComputerTokens)))
	root.Handle("/v1/computer-token/revoke-attempt", s.authenticateFabric(http.HandlerFunc(s.revokeComputerAttemptTokens)))
	root.Handle("/v1/computer-token/revoke-host", s.authenticateFabric(http.HandlerFunc(s.revokeHostComputerTokens)))
	root.Handle("GET /v1/computers/{computer_id}/inflight", s.authenticateFabric(http.HandlerFunc(s.getComputerInflight)))
	s.registerComputerTokenRoutes(runs, root)
	root.Handle("/v1/runs", s.authenticateFabric(s.authorize(runs)))
	root.Handle("/v1/runs/", s.authenticateFabric(s.authorize(runs)))
	root.Handle("/v1/workflows/", s.authenticateFabric(s.authorize(s.requireCaller(workflows))))
	return root
}

func (s *Server) getRunExecution(w http.ResponseWriter, r *http.Request) {
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
	} else if scope, ok := computerTokenFromRequest(r); ok {
		_ = scope
		writeError(w, protocolError(contract.ErrorForbidden, "Computer tokens are not authorized to inspect Run execution"))
		return
	}
	projection, err := s.store.GetRunExecution(r.Context(), runID)
	if err != nil {
		writeError(w, err)
		return
	}
	if projection.L1JobID != "" {
		if s.jobs == nil {
			writeError(w, internalError(errors.New("L1 job client is not configured"), "inspect run execution"))
			return
		}
		job, err := s.jobs.GetJob(r.Context(), projection.L1JobID)
		if err != nil {
			writeError(w, err)
			return
		}
		job.Spec.Execution.SensitiveEnv = nil
		projection.Job = &job
	}
	writeJSON(w, http.StatusOK, projection)
}

func (s *Server) authenticateFabric(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := s.fabric.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil || (strings.TrimSpace(identity.NodeID) == "" && strings.TrimSpace(identity.UserID) == "") {
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
			if err == nil {
				ctx := context.WithValue(r.Context(), runTokenContextKey{}, scope)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			if code, _ := errorDetails(err); code != contract.ErrorUnauthorized {
				writeError(w, err)
				return
			}
			computerScope, computerErr := s.store.AuthenticateComputerToken(r.Context(), token)
			if computerErr != nil {
				if code, _ := errorDetails(computerErr); code != contract.ErrorUnauthorized {
					writeError(w, computerErr)
					return
				}
				writeError(w, protocolError(contract.ErrorUnauthorized, "bearer token is invalid"))
				return
			}
			identity := identityFromRequest(r)
			if strings.TrimSpace(identity.NodeID) == "" || identity.NodeID != computerScope.HostNodeID {
				writeError(w, protocolError(contract.ErrorUnauthorized, "Computer token is bound to another Fabric node"))
				return
			}
			if err := s.verifyComputerScope(r.Context(), computerScope, identity.NodeID); err != nil {
				writeError(w, err)
				return
			}
			ctx := context.WithValue(r.Context(), computerTokenContextKey{}, computerScope)
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
		if _, ok := computerTokenFromRequest(r); ok {
			writeError(w, protocolError(contract.ErrorForbidden, "Computer tokens are not authorized for workflow administration"))
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

func computerTokenFromRequest(r *http.Request) (ComputerTokenScope, bool) {
	scope, ok := r.Context().Value(computerTokenContextKey{}).(ComputerTokenScope)
	return scope, ok
}

func actorFromIdentity(identity fabric.Identity) string {
	if strings.TrimSpace(identity.UserID) != "" {
		return identity.UserID
	}
	return identity.NodeID
}

func (s *Server) mintComputerToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, protocolError(contract.ErrorNotFound, "route was not found"))
		return
	}
	identity := identityFromRequest(r)
	if strings.TrimSpace(identity.NodeID) == "" {
		writeError(w, protocolError(contract.ErrorForbidden, "Computer token minting requires an authenticated Node agent"))
		return
	}
	if s.computerGrants == nil {
		writeError(w, internalError(errors.New("L1 Computer grant verifier is not configured"), "mint Computer token"))
		return
	}
	var request ComputerTokenMintRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	proof, err := s.computerGrants.ProveComputerTokenScope(r.Context(), request.ComputerID, request.ComputerAttemptID, identity.NodeID, "")
	if err != nil {
		writeError(w, err)
		return
	}
	grant, err := s.store.MintComputerToken(r.Context(), proof)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, grant)
}

func (s *Server) revokeComputerTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, protocolError(contract.ErrorNotFound, "route was not found"))
		return
	}
	if identityFromRequest(r).NodeID != s.controlPlaneNodeID {
		writeError(w, protocolError(contract.ErrorForbidden, "only the L1 control plane may revoke Computer grants administratively"))
		return
	}
	var request ComputerTokenRevocationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	receipt, err := s.store.RevokeComputerTokens(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) getComputerInflight(w http.ResponseWriter, r *http.Request) {
	if identityFromRequest(r).NodeID != s.controlPlaneNodeID {
		writeError(w, protocolError(contract.ErrorForbidden, "only the L1 control plane may read Computer inflight state"))
		return
	}
	computerID := r.PathValue("computer_id")
	count, err := s.store.CountComputerInflight(r.Context(), computerID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ComputerInflightState{ComputerID: computerID, NonterminalRootLineages: count})
}

func (s *Server) revokeComputerAttemptTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, protocolError(contract.ErrorNotFound, "route was not found"))
		return
	}
	identity := identityFromRequest(r)
	if strings.TrimSpace(identity.NodeID) == "" {
		writeError(w, protocolError(contract.ErrorForbidden, "Computer attempt revocation requires an authenticated Node agent"))
		return
	}
	var request ComputerAttemptTokenRevocationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.RevokeComputerAttemptTokens(r.Context(), request.ComputerID, request.ComputerAttemptID, identity.NodeID, request.Reason); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) revokeHostComputerTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, protocolError(contract.ErrorNotFound, "route was not found"))
		return
	}
	identity := identityFromRequest(r)
	if strings.TrimSpace(identity.NodeID) == "" {
		writeError(w, protocolError(contract.ErrorForbidden, "host revocation requires an authenticated Node agent"))
		return
	}
	var request HostComputerTokenRevocationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if err := s.store.RevokeHostComputerTokens(r.Context(), identity.NodeID, request.Reason); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getComputerSelf(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, protocolError(contract.ErrorNotFound, "route was not found"))
		return
	}
	scope, ok := computerTokenFromRequest(r)
	if !ok {
		writeError(w, protocolError(contract.ErrorForbidden, "a Computer token is required"))
		return
	}
	writeJSON(w, http.StatusOK, s.store.ComputerSelf(scope))
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	for name, values := range query {
		switch name {
		case "origin", "include_descendants", "limit", "cursor":
			if len(values) != 1 {
				writeError(w, protocolError(contract.ErrorInvalidRequest, "%s must be supplied at most once", name))
				return
			}
		default:
			writeError(w, protocolError(contract.ErrorInvalidRequest, "unknown Run list parameter %q", name))
			return
		}
	}
	origins, present := query["origin"]
	if !present || len(origins) != 1 {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "exactly one origin is required"))
		return
	}
	includeDescendants := false
	if values, present := query["include_descendants"]; present {
		if len(values) != 1 || (values[0] != "true" && values[0] != "false") {
			writeError(w, protocolError(contract.ErrorInvalidRequest, "include_descendants must be true or false"))
			return
		}
		includeDescendants = values[0] == "true"
	}
	limit := DefaultComputerRunPageLimit
	if value := query.Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > MaxComputerRunPageLimit {
			writeError(w, protocolError(contract.ErrorInvalidRequest, "limit must be an integer between 1 and %d", MaxComputerRunPageLimit))
			return
		}
		limit = parsed
	}
	var (
		page ComputerRunPage
		err  error
	)
	if scope, ok := computerTokenFromRequest(r); ok {
		if origins[0] != "computer:self" {
			writeError(w, protocolError(contract.ErrorInvalidRequest, "origin=computer:self is required"))
			return
		}
		page, err = s.store.ListComputerRuns(r.Context(), scope, query.Get("cursor"), limit, includeDescendants)
	} else {
		if _, ok := runTokenFromRequest(r); ok {
			writeError(w, protocolError(contract.ErrorForbidden, "run tokens cannot enumerate Computer-originated Runs"))
			return
		}
		computerID, found := strings.CutPrefix(origins[0], "computer:")
		if !found || strings.TrimSpace(computerID) == "" || computerID == "self" || computerID != strings.TrimSpace(computerID) {
			writeError(w, protocolError(contract.ErrorInvalidRequest, "origin must be computer:COMPUTER_ID"))
			return
		}
		page, err = s.store.ListRunsByComputerOrigin(r.Context(), computerID, query.Get("cursor"), limit, includeDescendants)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) verifyComputerScope(ctx context.Context, scope ComputerTokenScope, hostIdentityNodeID string) error {
	if s.computerGrants == nil {
		return internalError(errors.New("L1 Computer grant verifier is not configured"), "verify Computer token scope")
	}
	proof, err := s.computerGrants.ProveComputerTokenScope(ctx, scope.ComputerID, scope.ComputerAttemptID, hostIdentityNodeID, "")
	if err != nil {
		code, _ := errorDetails(err)
		switch code {
		case contract.ErrorUnauthorized, contract.ErrorForbidden, contract.ErrorNotFound,
			contract.ErrorConflict, contract.ErrorStalePolicyRevision:
			return protocolError(contract.ErrorUnauthorized, "Computer token scope is no longer authoritative")
		default:
			return err
		}
	}
	if proof.ComputerID != scope.ComputerID || proof.ComputerAttemptID != scope.ComputerAttemptID ||
		proof.ComputerStorageGeneration != scope.ComputerStorageGeneration ||
		proof.SubmitIntentRevision != scope.SubmitIntentRevision || proof.HostNodeID != scope.HostNodeID ||
		proof.SubmitMaxInflight != scope.SubmitMaxInflight {
		return protocolError(contract.ErrorUnauthorized, "Computer token scope is no longer authoritative")
	}
	return nil
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
	} else if scope, ok := computerTokenFromRequest(r); ok {
		if request.ParentRunID != "" {
			writeError(w, protocolError(contract.ErrorForbidden, "Computer tokens may submit only root Runs"))
			return
		}
		actor = "computer:" + scope.ComputerID
		record, replayed, err := s.store.CreateRun(r.Context(), CreateRunInput{
			IdempotencyKey: r.Header.Get("Idempotency-Key"), Actor: actor, Request: request, ComputerScope: &scope,
			VerifyComputerScope: func(ctx context.Context, current ComputerTokenScope) error {
				return s.verifyComputerScope(ctx, current, identity.NodeID)
			},
		})
		if err != nil {
			writeError(w, err)
			return
		}
		writeRunAccepted(w, record, replayed)
		return
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
	} else if scope, ok := computerTokenFromRequest(r); ok {
		allowed, err := s.store.CanComputerReadRun(r.Context(), scope, runID)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allowed {
			writeError(w, protocolError(contract.ErrorForbidden, "Computer token cannot read a foreign or earlier-generation Run"))
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
	} else if computerScope, computerScoped := computerTokenFromRequest(r); computerScoped {
		allowed, err := s.store.CanComputerReadRun(r.Context(), computerScope, runID)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allowed {
			writeError(w, protocolError(contract.ErrorForbidden, "Computer token cannot read a foreign or earlier-generation Run Lineage"))
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
	} else if computerScope, ok := computerTokenFromRequest(r); ok {
		lineage.Ancestors, err = s.filterComputerVisibleLineage(r.Context(), computerScope, lineage.Ancestors)
		if err == nil {
			lineage.Descendants, err = s.filterComputerVisibleLineage(r.Context(), computerScope, lineage.Descendants)
		}
	}
	if err != nil {
		writeError(w, err)
		return
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
	} else if scope, ok := computerTokenFromRequest(r); ok {
		allowed, err := s.store.CanComputerReadRun(r.Context(), scope, runID)
		if err != nil {
			writeError(w, err)
			return
		}
		if !allowed {
			writeError(w, protocolError(contract.ErrorForbidden, "Computer token cannot read foreign or earlier-generation logs"))
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
		authorize := s.store.CanReadRun
		if s.filterRunVisibility != nil {
			authorize = s.filterRunVisibility
		}
		allowed, err := authorize(ctx, ownerRunID, entry.RunID)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, entry)
		}
	}
	return visible, nil
}

func (s *Server) filterComputerVisibleLineage(ctx context.Context, scope ComputerTokenScope, entries []LineageEntry) ([]LineageEntry, error) {
	visible := make([]LineageEntry, 0, len(entries))
	for _, entry := range entries {
		allowed, err := s.store.CanComputerReadRun(ctx, scope, entry.RunID)
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
	if _, ok := computerTokenFromRequest(r); ok {
		writeError(w, protocolError(contract.ErrorForbidden, "Computer tokens are not authorized to rerun terminal snapshots"))
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

func (s *Server) notImplemented(w http.ResponseWriter, r *http.Request) {
	if _, ok := computerTokenFromRequest(r); ok {
		writeError(w, protocolError(contract.ErrorForbidden, "Computer tokens are not authorized for this operation"))
		return
	}
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
	apiError := apiErrorFrom(err)
	code := apiError.Code
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
		if apiError.Retryable {
			status = http.StatusServiceUnavailable
		}
	}
	if code == contract.ErrorInternal {
		apiError.Message = "internal server error"
		apiError.Details = nil
	}
	writeJSON(w, status, contract.ErrorResponse{Error: apiError})
}
