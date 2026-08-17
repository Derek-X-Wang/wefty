package l1

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
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type ServerConfig struct {
	ClientPrincipalTag string
	AgentPrincipalTag  string
	NodePolicies       map[string]NodePolicy
	ReconcileInterval  time.Duration
}

// NodePolicy is authoritative control-plane configuration for one stable node.
// Tags and the two class-scoped slot limits are never supplied by the agent.
type NodePolicy struct {
	Tags            []string
	MaxOneshotSlots int
	MaxServiceSlots int
}

// DefaultNodePolicy returns policy with the owner-selected class capacities.
func DefaultNodePolicy(tags ...string) NodePolicy {
	return NodePolicy{
		Tags: tags, MaxOneshotSlots: DefaultMaxOneshotSlots, MaxServiceSlots: DefaultMaxServiceSlots,
	}
}

// Server serves separate client and agent protocols over one Fabric listener.
// Fabric identity tags select the protocol principal; configured node policy
// controls job eligibility and class-scoped admission.
type Server struct {
	fabric             fabric.Fabric
	store              *Store
	clientPrincipalTag string
	agentPrincipalTag  string
	nodePolicies       map[string]NodePolicy
	reconcileInterval  time.Duration
	handler            http.Handler
}

func NewServer(f fabric.Fabric, store *Store, config ServerConfig) (*Server, error) {
	if f == nil {
		return nil, fmt.Errorf("l1: fabric is required")
	}
	if store == nil {
		return nil, fmt.Errorf("l1: store is required")
	}
	clientTag := normalizePrincipalTag(config.ClientPrincipalTag, DefaultClientPrincipalTag)
	agentTag := normalizePrincipalTag(config.AgentPrincipalTag, DefaultAgentPrincipalTag)
	if clientTag == agentTag {
		return nil, fmt.Errorf("l1: client and agent principal tags must differ")
	}
	nodePolicies := make(map[string]NodePolicy, len(config.NodePolicies))
	for nodeID, policy := range config.NodePolicies {
		if strings.TrimSpace(nodeID) == "" {
			return nil, fmt.Errorf("l1: node policy has an empty stable node ID")
		}
		if policy.MaxOneshotSlots < 0 || policy.MaxServiceSlots < 0 {
			return nil, fmt.Errorf("l1: node policy %q slot limits must be non-negative", nodeID)
		}
		policy.Tags = NormalizeTags(policy.Tags)
		nodePolicies[nodeID] = policy
	}
	reconcileInterval := config.ReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = DefaultReconcileInterval
	}
	s := &Server{
		fabric: f, store: store,
		clientPrincipalTag: clientTag, agentPrincipalTag: agentTag,
		nodePolicies:      nodePolicies,
		reconcileInterval: reconcileInterval,
	}
	s.handler = s.routes()
	return s, nil
}

func normalizePrincipalTag(tag, fallback string) string {
	tag = strings.ToLower(strings.TrimSpace(tag))
	if tag == "" {
		return fallback
	}
	return tag
}

func (s *Server) Handler() http.Handler { return s.handler }

// ListenAndServe obtains the listener exclusively through the Fabric seam.
func (s *Server) ListenAndServe(ctx context.Context, network, address string) error {
	listener, err := s.fabric.Listen(network, address)
	if err != nil {
		return err
	}
	return s.Serve(ctx, listener)
}

// Serve runs the L1 HTTP protocols on an already-created Fabric listener.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if _, err := s.store.Reconcile(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("l1: initial recovery: %w", err)
	}
	httpServer := &http.Server{Handler: s.handler}
	reconcileFailures := make(chan error, 1)
	reconcileContext, stopReconcile := context.WithCancel(ctx)
	defer stopReconcile()
	go func() {
		ticker := time.NewTicker(s.reconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-reconcileContext.Done():
				return
			case <-ticker.C:
				if _, err := s.store.Reconcile(reconcileContext); err != nil {
					reconcileFailures <- err
					_ = httpServer.Close()
					return
				}
			}
		}
	}()
	stopped := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = httpServer.Close()
		case <-stopped:
		}
	}()
	err := httpServer.Serve(listener)
	stopReconcile()
	close(stopped)
	select {
	case reconcileErr := <-reconcileFailures:
		return fmt.Errorf("l1: reconcile failure state: %w", reconcileErr)
	default:
	}
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}

type principal int

const (
	clientPrincipal principal = iota
	agentPrincipal
)

type identityContextKey struct{}

func (s *Server) routes() http.Handler {
	client := http.NewServeMux()
	client.HandleFunc("POST /v1/jobs", s.createJob)
	client.HandleFunc("GET /v1/jobs", s.listJobs)
	client.HandleFunc("GET /v1/jobs/{$}", s.listJobs)
	client.HandleFunc("GET /v1/jobs/{job_id}", s.getJob)
	client.HandleFunc("GET /v1/jobs/{job_id}/logs", s.getJobLogs)
	client.HandleFunc("PUT /v1/jobs/{job_id}/desired-state", s.setServiceDesiredState)
	client.HandleFunc("POST /v1/jobs/{job_id}/restart", s.restartService)
	client.HandleFunc("POST /v1/jobs/{job_id}/remove", s.removeService)
	client.HandleFunc("POST /v1/jobs/{job_id}/forget", s.forceForgetService)
	client.HandleFunc("POST /v1/jobs/{job_id}/prompt", s.notImplemented)
	client.HandleFunc("POST /v1/jobs/{job_id}/cancel", s.notImplemented)
	client.HandleFunc("GET /v1/nodes", s.listNodes)
	client.HandleFunc("POST /v1/nodes/{node_id}/drain", s.operatorDrainNode)
	client.HandleFunc("POST /v1/nodes/{node_id}/claims", s.setNodeClaims)

	agent := http.NewServeMux()
	agent.HandleFunc("POST /v1/agent/nodes/register", s.registerNode)
	agent.HandleFunc("POST /v1/agent/nodes/{node_id}/heartbeat", s.heartbeatNode)
	agent.HandleFunc("POST /v1/agent/nodes/{node_id}/drain", s.drainNode)
	agent.HandleFunc("POST /v1/agent/jobs/claim", s.claimJob)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/lease", s.renewLease)
	agent.HandleFunc("PUT /v1/agent/jobs/{job_id}/attempts/{attempt_id}/publication", s.setAttemptPublication)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/logs", s.appendLogs)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/complete", s.completeAttempt)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/removal-acknowledgement", s.acknowledgeServiceRemoval)

	root := http.NewServeMux()
	root.Handle("/v1/agent/", s.authorize(agentPrincipal, agent))
	root.Handle("/v1/jobs", s.authorize(clientPrincipal, client))
	root.Handle("/v1/jobs/", s.authorize(clientPrincipal, client))
	root.Handle("/v1/nodes", s.authorize(clientPrincipal, client))
	root.Handle("/v1/nodes/", s.authorize(clientPrincipal, client))
	return root
}

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	if _, err := s.store.Reconcile(r.Context()); err != nil {
		writeError(w, err)
		return
	}
	nodes, err := s.store.ListNodes(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, NodeList{Nodes: nodes})
}

func (s *Server) operatorDrainNode(w http.ResponseWriter, r *http.Request) {
	var request NodeIntentRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if request.ClaimsEnabled {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "drain requires claims_enabled=false"))
		return
	}
	s.writeNodeIntent(w, r, request)
}

func (s *Server) setNodeClaims(w http.ResponseWriter, r *http.Request) {
	var request NodeIntentRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	s.writeNodeIntent(w, r, request)
}

func (s *Server) writeNodeIntent(w http.ResponseWriter, r *http.Request, request NodeIntentRequest) {
	identity := identityFromRequest(r)
	node, err := s.store.SetNodeClaimsByOperator(r.Context(), r.PathValue("node_id"), identity.NodeID, request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) authorize(principal principal, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := s.fabric.WhoIs(r.Context(), r.RemoteAddr)
		if err != nil {
			writeError(w, protocolError(contract.ErrorUnauthorized, "fabric identity could not be authenticated"))
			return
		}
		tag := s.clientPrincipalTag
		if principal == agentPrincipal {
			tag = s.agentPrincipalTag
		}
		if !slices.Contains(NormalizeTags(identity.Tags), tag) {
			writeError(w, protocolError(contract.ErrorPrincipalForbidden, "fabric identity is not authorized for this protocol"))
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

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var spec contract.JobSpec
	if err := decodeJSON(r, &spec); err != nil {
		writeError(w, err)
		return
	}
	job, replayed, err := s.store.CreateJob(r.Context(), spec)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	if job.ServiceJob != nil || job.Removal != nil {
		job, err = s.store.projectServiceJob(r.Context(), job)
		if err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, status, redactJob(job))
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	if err := requireServiceClass(r); err != nil {
		writeError(w, err)
		return
	}
	limit, err := parseJobLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := s.store.ListServiceJobs(r.Context(), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	for index := range page.Jobs {
		page.Jobs[index] = redactJob(page.Jobs[index])
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if err := validateJobRouteClass(r, job); err != nil {
		writeError(w, err)
		return
	}
	if job.ServiceJob != nil || job.Removal != nil {
		job, err = s.store.projectServiceJob(r.Context(), job)
		if err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, redactJob(job))
}

func (s *Server) getJobLogs(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if err := validateJobRouteClass(r, job); err != nil {
		writeError(w, err)
		return
	}
	limit, err := parseLogLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := s.store.GetJobLogs(r.Context(), r.PathValue("job_id"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) setServiceDesiredState(w http.ResponseWriter, r *http.Request) {
	if err := requireServiceClass(r); err != nil {
		writeError(w, err)
		return
	}
	var request ServiceDesiredStateRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	job, err := s.store.SetServiceDesiredState(r.Context(), r.PathValue("job_id"), request.DesiredState)
	if err != nil {
		writeError(w, err)
		return
	}
	job, err = s.store.projectServiceJob(r.Context(), job)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, redactJob(job))
}

func (s *Server) restartService(w http.ResponseWriter, r *http.Request) {
	if err := requireServiceClass(r); err != nil {
		writeError(w, err)
		return
	}
	var request ServiceRestartRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	job, replayed, err := s.store.RestartService(r.Context(), r.PathValue("job_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	job, err = s.store.projectServiceJob(r.Context(), job)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactJob(job))
}

func (s *Server) removeService(w http.ResponseWriter, r *http.Request) {
	if err := requireServiceClass(r); err != nil {
		writeError(w, err)
		return
	}
	job, err := s.store.RemoveService(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	job, err = s.store.projectServiceJob(r.Context(), job)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, redactJob(job))
}

func (s *Server) forceForgetService(w http.ResponseWriter, r *http.Request) {
	if err := requireServiceClass(r); err != nil {
		writeError(w, err)
		return
	}
	var request ForceForgetRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if !request.Force {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "forget requires force=true"))
		return
	}
	job, err := s.store.ForceForgetService(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	job, err = s.store.projectServiceJob(r.Context(), job)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactJob(job))
}

func (s *Server) registerNode(w http.ResponseWriter, r *http.Request) {
	var registration contract.NodeRegistration
	if err := decodeJSON(r, &registration); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	policy, configured := s.nodePolicies[registration.NodeID]
	if !configured {
		policy = NodePolicy{MaxOneshotSlots: DefaultMaxOneshotSlots, MaxServiceSlots: DefaultMaxServiceSlots}
	}
	node, err := s.store.RegisterNode(r.Context(), identity, registration, policy, configured)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) heartbeatNode(w http.ResponseWriter, r *http.Request) {
	var request HeartbeatRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	node, err := s.store.HeartbeatNode(r.Context(), identity.NodeID, r.PathValue("node_id"), request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) drainNode(w http.ResponseWriter, r *http.Request) {
	var request DrainRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	node, err := s.store.DrainNode(r.Context(), identity.NodeID, r.PathValue("node_id"), request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func (s *Server) claimJob(w http.ResponseWriter, r *http.Request) {
	var request ClaimRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if request.Class != contract.JobClassOneShot && request.Class != contract.JobClassService {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "claim class must be %q or %q", contract.JobClassOneShot, contract.JobClassService))
		return
	}
	identity := identityFromRequest(r)
	claim, err := s.store.ClaimJob(
		r.Context(), identity.NodeID, request.NodeID, request.BootSessionID, request.Class, request.ExcludedJobIDs...,
	)
	if err != nil {
		writeError(w, err)
		return
	}
	if claim == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, claim)
}

func (s *Server) renewLease(w http.ResponseWriter, r *http.Request) {
	var request RenewalRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	lease, err := s.store.RenewLease(r.Context(), identity.NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), request.FencingToken)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lease)
}

func (s *Server) setAttemptPublication(w http.ResponseWriter, r *http.Request) {
	var request PublicationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	job, err := s.store.SetAttemptPublication(
		r.Context(), identity.NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), request,
	)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactJob(job))
}

func (s *Server) appendLogs(w http.ResponseWriter, r *http.Request) {
	var request AppendLogsRequest
	if err := decodeJSONWithLimit(r, &request, MaxLogUploadBodyBytes); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	response, err := s.store.AppendLogs(r.Context(), identity.NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) completeAttempt(w http.ResponseWriter, r *http.Request) {
	var request CompletionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if request.ProtocolOutputHash != "" && !validSHA256(request.ProtocolOutputHash) {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "protocol_output_digest must be a lowercase SHA-256 digest"))
		return
	}
	identity := identityFromRequest(r)
	job, err := s.store.CompleteAttempt(r.Context(), identity.NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactJob(job))
}

func (s *Server) acknowledgeServiceRemoval(w http.ResponseWriter, r *http.Request) {
	var request RemovalAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	acknowledged, err := s.store.AcknowledgeServiceRemoval(r.Context(), identity.NodeID, r.PathValue("job_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	finalized, changed, err := s.store.FinalizeServiceRemoval(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if changed || finalized.JobID != "" {
		acknowledged = finalized
	}
	writeJSON(w, http.StatusOK, redactJob(acknowledged))
}

func redactJob(job Job) Job {
	job.Spec.Execution.SensitiveEnv = nil
	return job
}

func (s *Server) notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, protocolError(contract.ErrorNotImplemented, "operation is reserved for a future version"))
}

func decodeJSON(r *http.Request, target any) error {
	return decodeJSONWithLimit(r, target, 1<<20)
}

func decodeJSONWithLimit(r *http.Request, target any, limit int64) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, limit))
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

func parseLogLimit(value string) (int, error) {
	if value == "" {
		return DefaultLogPageLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > MaxLogPageLimit {
		return 0, protocolError(contract.ErrorInvalidRequest, "limit must be an integer between 1 and %d", MaxLogPageLimit)
	}
	return limit, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	code := errorCode(err)
	status := http.StatusConflict
	switch code {
	case contract.ErrorInvalidRequest:
		status = http.StatusBadRequest
	case contract.ErrorUnauthorized:
		status = http.StatusUnauthorized
	case contract.ErrorForbidden, contract.ErrorPrincipalForbidden, contract.ErrorIdentityBound, contract.ErrorAttemptNotOwned:
		status = http.StatusForbidden
	case contract.ErrorNotFound, contract.ErrorAttemptNotFound:
		status = http.StatusNotFound
	case contract.ErrorUnsupportedKind, contract.ErrorUnsupportedRuntimeHandler:
		status = http.StatusUnprocessableEntity
	case contract.ErrorNotImplemented:
		status = http.StatusNotImplemented
	case contract.ErrorInternal:
		status = http.StatusInternalServerError
	}
	message := err.Error()
	var details map[string]any
	var protocolErr *Error
	if errors.As(err, &protocolErr) {
		details = protocolErr.Details
	}
	if code == contract.ErrorInternal {
		message = "internal server error"
	}
	writeJSON(w, status, contract.ErrorResponse{Error: contract.APIError{
		Code: code, Message: message, Retryable: code == contract.ErrorInternal || code == contract.ErrorCapacityExhausted,
		Details: details,
	}})
}
