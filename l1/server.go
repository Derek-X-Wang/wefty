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
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type ServerConfig struct {
	ClientPrincipalTag    string
	AgentPrincipalTag     string
	AuthoritativeNodeTags map[string][]string
}

// Server serves separate client and agent protocols over one Fabric listener.
// Fabric identity tags select the protocol principal; configured node tags
// select job eligibility.
type Server struct {
	fabric                fabric.Fabric
	store                 *Store
	clientPrincipalTag    string
	agentPrincipalTag     string
	authoritativeNodeTags map[string][]string
	handler               http.Handler
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
	authoritativeTags := make(map[string][]string, len(config.AuthoritativeNodeTags))
	for nodeID, tags := range config.AuthoritativeNodeTags {
		authoritativeTags[nodeID] = NormalizeTags(tags)
	}
	s := &Server{
		fabric: f, store: store,
		clientPrincipalTag: clientTag, agentPrincipalTag: agentTag,
		authoritativeNodeTags: authoritativeTags,
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

type principal int

const (
	clientPrincipal principal = iota
	agentPrincipal
)

type identityContextKey struct{}

func (s *Server) routes() http.Handler {
	client := http.NewServeMux()
	client.HandleFunc("POST /v1/jobs", s.createJob)
	client.HandleFunc("GET /v1/jobs/{job_id}", s.getJob)
	client.HandleFunc("POST /v1/jobs/{job_id}/prompt", s.notImplemented)
	client.HandleFunc("POST /v1/jobs/{job_id}/cancel", s.notImplemented)

	agent := http.NewServeMux()
	agent.HandleFunc("POST /v1/agent/nodes/register", s.registerNode)
	agent.HandleFunc("POST /v1/agent/nodes/{node_id}/heartbeat", s.heartbeatNode)
	agent.HandleFunc("POST /v1/agent/jobs/claim", s.claimJob)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/lease", s.renewLease)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/complete", s.completeAttempt)

	root := http.NewServeMux()
	root.Handle("/v1/agent/", s.authorize(agentPrincipal, agent))
	root.Handle("/v1/jobs", s.authorize(clientPrincipal, client))
	root.Handle("/v1/jobs/", s.authorize(clientPrincipal, client))
	return root
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
			writeError(w, protocolError(contract.ErrorForbidden, "fabric identity is not authorized for this protocol"))
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
	writeJSON(w, status, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.GetJob(r.Context(), r.PathValue("job_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) registerNode(w http.ResponseWriter, r *http.Request) {
	var registration contract.NodeRegistration
	if err := decodeJSON(r, &registration); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	node, err := s.store.RegisterNode(r.Context(), identity, registration, s.authoritativeNodeTags[identity.NodeID])
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

func (s *Server) claimJob(w http.ResponseWriter, r *http.Request) {
	var request ClaimRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	claim, err := s.store.ClaimJob(r.Context(), identity.NodeID, request.NodeID, request.BootSessionID)
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
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, protocolError(contract.ErrorNotImplemented, "operation is reserved for a future version"))
}

func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
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
	code := errorCode(err)
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
	case contract.ErrorUnsupportedKind, contract.ErrorUnsupportedRuntimeHandler:
		status = http.StatusUnprocessableEntity
	case contract.ErrorNotImplemented:
		status = http.StatusNotImplemented
	case contract.ErrorInternal:
		status = http.StatusInternalServerError
	}
	message := err.Error()
	if code == contract.ErrorInternal {
		message = "internal server error"
	}
	writeJSON(w, status, contract.ErrorResponse{Error: contract.APIError{Code: code, Message: message, Retryable: false}})
}
