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

// MaxRequestBodyBytes is the JSON body limit shared by ordinary L1 routes and
// clients that need to reject requests which cannot cross that boundary.
const MaxRequestBodyBytes = 1 << 20

type ServerConfig struct {
	ClientPrincipalTag                string
	AgentPrincipalTag                 string
	NodePolicies                      map[string]NodePolicy
	ReconcileInterval                 time.Duration
	AllowSelfAssertedPersonIdentities bool
	ComputerPolicyFreshness           time.Duration
	ComputerPolicyWatchWait           time.Duration
	ComputerTokenRevoker              ComputerTokenRevoker
	RunLedgerNodeID                   string
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

func (s *Server) effectiveNodePolicy(nodeID string) (NodePolicy, bool) {
	policy, configured := s.nodePolicies[nodeID]
	if !configured {
		policy = DefaultNodePolicy()
	}
	return policy, configured
}

// Server serves separate client and agent protocols over one Fabric listener.
// Fabric identity tags select the protocol principal; configured node policy
// controls job eligibility and class-scoped admission.
type Server struct {
	fabric                  fabric.Fabric
	store                   *Store
	clientPrincipalTag      string
	agentPrincipalTag       string
	nodePolicies            map[string]NodePolicy
	reconcileInterval       time.Duration
	allowPersonIdentities   bool
	computerPolicyFreshness time.Duration
	computerPolicyWatchWait time.Duration
	computerTokenRevoker    ComputerTokenRevoker
	runLedgerNodeID         string
	handler                 http.Handler
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
	policyFreshness := config.ComputerPolicyFreshness
	if policyFreshness <= 0 {
		policyFreshness = DefaultComputerPolicyFreshness
	}
	policyWatchWait := config.ComputerPolicyWatchWait
	if policyWatchWait <= 0 {
		policyWatchWait = DefaultComputerPolicyWatchWait
	}
	runLedgerNodeID := strings.TrimSpace(config.RunLedgerNodeID)
	if runLedgerNodeID == "" {
		runLedgerNodeID = "run-ledger"
	}
	if policyFreshness <= policyWatchWait {
		return nil, fmt.Errorf("l1: Computer policy freshness must exceed watch wait")
	}
	if policyWatchWait >= ComputerPolicyClientTimeout {
		return nil, fmt.Errorf("l1: Computer policy watch wait must be shorter than the shared client timeout")
	}
	s := &Server{
		fabric: f, store: store,
		clientPrincipalTag: clientTag, agentPrincipalTag: agentTag,
		nodePolicies:            nodePolicies,
		reconcileInterval:       reconcileInterval,
		allowPersonIdentities:   personIdentitiesAllowed(f, config.AllowSelfAssertedPersonIdentities),
		computerPolicyFreshness: policyFreshness,
		computerPolicyWatchWait: policyWatchWait,
		computerTokenRevoker:    config.ComputerTokenRevoker,
		runLedgerNodeID:         runLedgerNodeID,
	}
	s.handler = s.routes()
	return s, nil
}

func personIdentitiesAllowed(f fabric.Fabric, allowSelfAsserted bool) bool {
	provider, ok := f.(fabric.PersonIdentityTrustProvider)
	return ok && (provider.PersonIdentityTrust() == fabric.PersonIdentityAuthenticated || allowSelfAsserted)
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
	personPrincipal
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
	client.HandleFunc("POST /v1/computers", s.createComputer)
	client.HandleFunc("GET /v1/computers/{computer_id}", s.getComputer)
	client.HandleFunc("GET /v1/computers/{computer_id}/intents", s.listComputerIntents)
	client.HandleFunc("PUT /v1/computers/{computer_id}/desired-state", s.setComputerDesiredState)
	client.HandleFunc("PUT /v1/computers/{computer_id}/backup-cap", s.setComputerBackupCap)
	client.HandleFunc("POST /v1/computers/{computer_id}/restart", s.restartComputer)
	client.HandleFunc("POST /v1/computers/{computer_id}/reimage", s.reimageComputer)
	client.HandleFunc("POST /v1/computers/{computer_id}/grow", s.growComputer)
	client.HandleFunc("POST /v1/computers/{computer_id}/reconfiguration-abort", s.abortComputerReconfiguration)
	client.HandleFunc("POST /v1/computers/{computer_id}/storage-reset", s.resetComputerStorage)
	client.HandleFunc("GET /v1/computers/{computer_id}/storage-generations", s.listComputerStorageGenerations)
	client.HandleFunc("POST /v1/computers/{computer_id}/backups", s.createComputerBackup)
	client.HandleFunc("GET /v1/computers/{computer_id}/backups", s.listComputerBackups)
	client.HandleFunc("POST /v1/computers/{computer_id}/backups/{backup_id}/prune", s.pruneComputerBackup)
	client.HandleFunc("POST /v1/computers/{computer_id}/backups/{backup_id}/restore", s.restoreComputerBackup)
	client.HandleFunc("POST /v1/computers/{computer_id}/backups/{backup_id}/clone", s.cloneComputerBackup)
	client.HandleFunc("POST /v1/computers/{computer_id}/projections", s.installComputerProjection)
	client.HandleFunc("POST /v1/computers/{computer_id}/remove", s.removeComputer)
	client.HandleFunc("POST /v1/computers/{computer_id}/token-scope-proof", s.proveComputerTokenScope)
	client.HandleFunc("GET /v1/nodes", s.listNodes)
	client.HandleFunc("POST /v1/nodes/{node_id}/drain", s.operatorDrainNode)
	client.HandleFunc("POST /v1/nodes/{node_id}/claims", s.setNodeClaims)

	agent := http.NewServeMux()
	agent.HandleFunc("POST /v1/agent/nodes/register", s.registerNode)
	agent.HandleFunc("POST /v1/agent/nodes/{node_id}/heartbeat", s.heartbeatNode)
	agent.HandleFunc("GET /v1/agent/nodes/{node_id}/computer-policy", s.watchComputerPolicy)
	agent.HandleFunc("POST /v1/agent/nodes/{node_id}/computer-policy-acknowledgement", s.acknowledgeComputerPolicy)
	agent.HandleFunc("POST /v1/agent/nodes/{node_id}/drain", s.drainNode)
	agent.HandleFunc("POST /v1/agent/jobs/claim", s.claimJob)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/service-binding-proof", s.proveServiceBinding)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/image-reconciliation-failure", s.latchServiceImageReconciliationFailure)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/lease", s.renewLease)
	agent.HandleFunc("PUT /v1/agent/jobs/{job_id}/attempts/{attempt_id}/image", s.observeAttemptImage)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/started", s.startAttempt)
	agent.HandleFunc("PUT /v1/agent/jobs/{job_id}/attempts/{attempt_id}/publication", s.setAttemptPublication)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/jobs/{job_id}/attempts/{attempt_id}/takeover-audit", s.appendComputerTakeoverAudit)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/logs", s.appendLogs)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/attempts/{attempt_id}/complete", s.completeAttempt)
	agent.HandleFunc("POST /v1/agent/jobs/{job_id}/removal-acknowledgement", s.acknowledgeServiceRemoval)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/storage-reset-acknowledgement", s.acknowledgeComputerStorageReset)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/storage-grow-acknowledgement", s.acknowledgeComputerStorageGrow)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/reimage-preflight-acknowledgement", s.acknowledgeComputerReimagePreflight)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/storage-retirement-acknowledgement", s.acknowledgeComputerStorageRetirement)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/backup-acknowledgement", s.acknowledgeComputerBackup)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/backup-prune-acknowledgement", s.acknowledgeComputerBackupPrune)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/storage-copy-acknowledgement", s.acknowledgeComputerStorageCopy)
	agent.HandleFunc("POST /v1/agent/computers/{computer_id}/restore-retirement-acknowledgement", s.acknowledgeComputerRestoreRetirement)

	person := http.NewServeMux()
	person.HandleFunc("GET /v1/whoami", s.whoAmI)
	person.HandleFunc("POST /v1/admin-bootstrap", s.bootstrapAdmin)
	person.HandleFunc("GET /v1/admin-policy", s.getAdminPolicy)
	person.HandleFunc("GET /v1/admin-policy/audit", s.listAdminPolicyAudit)
	person.HandleFunc("PUT /v1/admin-policy/admins/{user_id}", s.addAdmin)
	person.HandleFunc("DELETE /v1/admin-policy/admins/{user_id}", s.removeAdmin)
	person.HandleFunc("GET /v1/computers/{computer_id}/grants", s.listComputerGrants)
	person.HandleFunc("PUT /v1/computers/{computer_id}/grants/{user_id}", s.mutateComputerGrant)
	person.HandleFunc("DELETE /v1/computers/{computer_id}/grants/{user_id}", s.deleteComputerGrant)
	person.HandleFunc("GET /v1/computers/{computer_id}/grants/audit", s.listComputerPolicyAudit)
	person.HandleFunc("GET /v1/computers/{computer_id}/revocations/{policy_revision}", s.getComputerPolicyRevocation)
	person.HandleFunc("GET /v1/computers/{computer_id}/submission", s.getComputerSubmission)
	person.HandleFunc("PUT /v1/computers/{computer_id}/submission", s.mutateComputerSubmission)

	root := http.NewServeMux()
	root.Handle("/v1/agent/", s.authorize(agentPrincipal, agent))
	root.Handle("/v1/jobs", s.authorize(clientPrincipal, client))
	root.Handle("/v1/jobs/", s.authorize(clientPrincipal, client))
	root.Handle("/v1/computers", s.authorize(clientPrincipal, client))
	root.Handle("/v1/computers/{computer_id}/grants", s.authorize(personPrincipal, person))
	root.Handle("/v1/computers/{computer_id}/grants/", s.authorize(personPrincipal, person))
	root.Handle("/v1/computers/{computer_id}/revocations/", s.authorize(personPrincipal, person))
	root.Handle("/v1/computers/{computer_id}/submission", s.authorize(personPrincipal, person))
	root.Handle("/v1/computers/", s.authorize(clientPrincipal, client))
	root.Handle("/v1/nodes", s.authorize(clientPrincipal, client))
	root.Handle("/v1/nodes/", s.authorize(clientPrincipal, client))
	root.Handle("/v1/admin-bootstrap", s.authorize(personPrincipal, person))
	root.Handle("/v1/admin-policy", s.authorize(personPrincipal, person))
	root.Handle("/v1/admin-policy/", s.authorize(personPrincipal, person))
	root.Handle("/v1/whoami", s.authorize(personPrincipal, person))
	return root
}

func (s *Server) proveServiceBinding(w http.ResponseWriter, r *http.Request) {
	var request ServiceBindingProofRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	bound, err := s.store.ProveServiceBinding(r.Context(), identityFromRequest(r).NodeID, r.PathValue("job_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ServiceBindingProofResponse{Bound: bound})
}

func (s *Server) proveComputerTokenScope(w http.ResponseWriter, r *http.Request) {
	if identityFromRequest(r).NodeID != s.runLedgerNodeID {
		writeError(w, protocolError(contract.ErrorForbidden, "only the L3 run ledger may request Computer token scope proof"))
		return
	}
	var request struct {
		ComputerAttemptID  string `json:"computer_attempt_id"`
		HostIdentityNodeID string `json:"host_identity_node_id,omitempty"`
		HostNodeID         string `json:"host_node_id,omitempty"`
	}
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	proof, err := s.store.ProveComputerTokenScope(r.Context(), r.PathValue("computer_id"),
		request.ComputerAttemptID, request.HostIdentityNodeID, request.HostNodeID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, proof)
}

func (s *Server) mutateComputerSubmission(w http.ResponseWriter, r *http.Request) {
	var request ComputerSubmissionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	computer, replayed, mutationApplied, err := s.store.PrepareComputerSubmissionMutation(r.Context(), identity, r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replay", "true")
		result, resultErr := s.computerSubmissionResult(r.Context(), identity, computer, false, nil)
		if resultErr != nil {
			writeError(w, resultErr)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	var receipt *contract.ComputerTokenRevocationReceipt
	if mutationApplied && s.computerTokenRevoker == nil {
		writeError(w, internalError(errors.New("L3 Computer token revoker is not configured"), "revoke Computer token grants"))
		return
	}
	if mutationApplied {
		observed, revokeErr := s.computerTokenRevoker.RevokeComputerTokens(r.Context(), ComputerTokenRevocation{
			ComputerID: computer.ComputerID, NewSubmitIntentRevision: computer.SubmitIntentRevision + 1,
			Reason: "submission_intent_advanced",
		})
		if revokeErr != nil {
			writeError(w, internalError(revokeErr, "revoke Computer token grants before submission mutation"))
			return
		}
		if observed.ComputerID != computer.ComputerID || observed.SubmitIntentRevision != computer.SubmitIntentRevision+1 || observed.CommittedAt.IsZero() {
			writeError(w, internalError(errors.New("L3 Computer token revocation receipt did not match the mutation"), "verify Computer token revocation receipt"))
			return
		}
		receipt = &observed
	}
	computer, replayed, mutationApplied, err = s.store.MutateComputerSubmission(r.Context(), identity, computer.ComputerID, request)
	if err != nil {
		writeError(w, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replay", "true")
	}
	if replayed || !mutationApplied {
		receipt = nil
	}
	result, err := s.computerSubmissionResult(r.Context(), identity, computer, mutationApplied, receipt)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) getComputerSubmission(w http.ResponseWriter, r *http.Request) {
	state, err := s.store.GetComputerSubmissionState(r.Context(), identityFromRequest(r), r.PathValue("computer_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	if s.computerTokenRevoker == nil {
		writeError(w, internalError(errors.New("L3 Computer inflight reader is not configured"), "read Computer inflight state"))
		return
	}
	state.InflightCount, err = s.computerTokenRevoker.CountComputerInflight(r.Context(), state.ComputerID)
	if err != nil {
		writeError(w, internalError(err, "read Computer inflight state"))
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) computerSubmissionResult(ctx context.Context, identity fabric.Identity, computer Computer, mutationApplied bool,
	receipt *contract.ComputerTokenRevocationReceipt) (ComputerSubmissionMutationResult, error) {
	state, err := s.store.GetComputerSubmissionState(ctx, identity, computer.ComputerID)
	if err != nil {
		return ComputerSubmissionMutationResult{}, err
	}
	if s.computerTokenRevoker == nil {
		return ComputerSubmissionMutationResult{}, internalError(errors.New("L3 Computer inflight reader is not configured"), "read Computer inflight state")
	}
	state.InflightCount, err = s.computerTokenRevoker.CountComputerInflight(ctx, computer.ComputerID)
	if err != nil {
		return ComputerSubmissionMutationResult{}, internalError(err, "read Computer inflight state")
	}
	return ComputerSubmissionMutationResult{ComputerSubmissionState: state, MutationApplied: mutationApplied, Revoked: receipt}, nil
}

func (s *Server) latchServiceImageReconciliationFailure(w http.ResponseWriter, r *http.Request) {
	var request ServiceImageReconciliationFailureRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	job, err := s.store.LatchServiceImageReconciliationFailure(r.Context(), identityFromRequest(r).NodeID, r.PathValue("job_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactJob(job))
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
		if principal == personPrincipal {
			if !s.allowPersonIdentities {
				writeError(w, protocolError(contract.ErrorPrincipalForbidden,
					"this Fabric does not authenticate person identities"))
				return
			}
			tags := NormalizeTags(identity.Tags)
			if identity.Kind == fabric.IdentityKindMachine ||
				slices.Contains(tags, s.clientPrincipalTag) || slices.Contains(tags, s.agentPrincipalTag) {
				writeError(w, protocolError(contract.ErrorPrincipalForbidden,
					"machine principals cannot use person protocols"))
				return
			}
			if err := validatePersonIdentity(identity); err != nil {
				writeError(w, err)
				return
			}
			if _, err := s.store.ObserveAuthenticatedPerson(r.Context(), identity); err != nil {
				writeError(w, err)
				return
			}
		} else {
			tag := s.clientPrincipalTag
			if principal == agentPrincipal {
				tag = s.agentPrincipalTag
			}
			if !slices.Contains(NormalizeTags(identity.Tags), tag) {
				writeError(w, protocolError(contract.ErrorPrincipalForbidden, "fabric identity is not authorized for this protocol"))
				return
			}
		}
		ctx := context.WithValue(r.Context(), identityContextKey{}, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) whoAmI(w http.ResponseWriter, r *http.Request) {
	identity := identityFromRequest(r)
	writeJSON(w, http.StatusOK, AuthenticatedPerson{FabricID: identity.FabricID,
		UserID: identity.UserID, DeviceID: identity.DeviceID, SeenAt: canonicalTime(s.store.clock.Now())})
}

func identityFromRequest(r *http.Request) fabric.Identity {
	identity, _ := r.Context().Value(identityContextKey{}).(fabric.Identity)
	return identity
}

func (s *Server) bootstrapAdmin(w http.ResponseWriter, r *http.Request) {
	var request BootstrapAdminRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	policy, err := s.store.BootstrapAdmin(r.Context(), identityFromRequest(r), request.Nonce)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, policy)
}

func (s *Server) getAdminPolicy(w http.ResponseWriter, r *http.Request) {
	policy, err := s.store.GetVisibleAdminPolicy(r.Context(), identityFromRequest(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) listAdminPolicyAudit(w http.ResponseWriter, r *http.Request) {
	limit, err := parseJobLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := s.store.ListAdminPolicyAudit(r.Context(), identityFromRequest(r),
		r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) addAdmin(w http.ResponseWriter, r *http.Request) {
	var request AdminPolicyMutationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	policy, err := s.store.AddAdmin(r.Context(), identityFromRequest(r),
		r.PathValue("user_id"), request.PolicyRevision)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) removeAdmin(w http.ResponseWriter, r *http.Request) {
	var request AdminPolicyMutationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	policy, err := s.store.RemoveAdmin(r.Context(), identityFromRequest(r),
		r.PathValue("user_id"), request.PolicyRevision)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *Server) listComputerGrants(w http.ResponseWriter, r *http.Request) {
	grants, err := s.store.ListComputerGrants(r.Context(), identityFromRequest(r), r.PathValue("computer_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, grants)
}

func (s *Server) mutateComputerGrant(w http.ResponseWriter, r *http.Request) {
	var request ComputerGrantMutationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	result, err := s.store.MutateComputerGrant(r.Context(), identityFromRequest(r),
		r.PathValue("computer_id"), r.PathValue("user_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if result.Replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) deleteComputerGrant(w http.ResponseWriter, r *http.Request) {
	var request ComputerGrantDeleteRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	result, err := s.store.DeleteForeignComputerGrant(r.Context(), identityFromRequest(r),
		r.PathValue("computer_id"), r.PathValue("user_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if result.Replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) listComputerPolicyAudit(w http.ResponseWriter, r *http.Request) {
	limit, err := parseJobLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := s.store.ListComputerPolicyAudit(r.Context(), identityFromRequest(r),
		r.PathValue("computer_id"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) getComputerPolicyRevocation(w http.ResponseWriter, r *http.Request) {
	revision, err := strconv.ParseInt(r.PathValue("policy_revision"), 10, 64)
	if err != nil || revision < 1 {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "policy_revision must be positive"))
		return
	}
	revocation, err := s.store.GetComputerPolicyRevocation(r.Context(), identityFromRequest(r),
		revision, r.PathValue("computer_id"), r.URL.Query().Get("fabric_id"), r.URL.Query().Get("user_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, revocation)
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
	job, err = s.store.projectJob(r.Context(), job)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, status, redactJob(job))
}

func (s *Server) createComputer(w http.ResponseWriter, r *http.Request) {
	var request CreateComputerRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.CreateComputer(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) getComputer(w http.ResponseWriter, r *http.Request) {
	computer, err := s.store.GetComputer(r.Context(), r.PathValue("computer_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) listComputerIntents(w http.ResponseWriter, r *http.Request) {
	limit, err := parseJobLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := s.store.ListComputerIntents(r.Context(), r.PathValue("computer_id"),
		r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) setComputerDesiredState(w http.ResponseWriter, r *http.Request) {
	var request ComputerDesiredStateRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, err := s.store.SetComputerDesiredState(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if request.DesiredState == contract.ServiceDesiredStopped {
		if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "computer_stopped"); err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusAccepted, redactComputer(computer))
}

func (s *Server) setComputerBackupCap(w http.ResponseWriter, r *http.Request) {
	var request ComputerBackupCapRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, err := s.store.SetComputerBackupCap(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) restartComputer(w http.ResponseWriter, r *http.Request) {
	var request ComputerRestartRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.RestartComputer(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if !replayed {
		if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "computer_restarted"); err != nil {
			writeError(w, err)
			return
		}
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) resetComputerStorage(w http.ResponseWriter, r *http.Request) {
	var request ComputerStorageResetRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.BeginComputerStorageReset(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if !replayed {
		if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "storage_generation_advanced"); err != nil {
			writeError(w, err)
			return
		}
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) growComputer(w http.ResponseWriter, r *http.Request) {
	var request ComputerGrowRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.BeginComputerGrow(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) abortComputerReconfiguration(w http.ResponseWriter, r *http.Request) {
	var request ComputerReconfigurationAbortRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.AbortComputerReconfiguration(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) listComputerStorageGenerations(w http.ResponseWriter, r *http.Request) {
	generations, err := s.store.ListComputerStorageGenerations(r.Context(), r.PathValue("computer_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, generations)
}

func (s *Server) createComputerBackup(w http.ResponseWriter, r *http.Request) {
	var request ComputerBackupCreateRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.BeginComputerBackup(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) listComputerBackups(w http.ResponseWriter, r *http.Request) {
	backups, err := s.store.ListComputerBackups(r.Context(), r.PathValue("computer_id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, backups)
}

func (s *Server) pruneComputerBackup(w http.ResponseWriter, r *http.Request) {
	var request ComputerBackupPruneRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.BackupID = r.PathValue("backup_id")
	request.Actor = identityFromRequest(r).NodeID
	backup, replayed, err := s.store.BeginComputerBackupPrune(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, backup)
}

func (s *Server) restoreComputerBackup(w http.ResponseWriter, r *http.Request) {
	var request ComputerRestoreRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.BackupID = r.PathValue("backup_id")
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.BeginComputerRestore(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) cloneComputerBackup(w http.ResponseWriter, r *http.Request) {
	var request ComputerCloneRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.BackupID = r.PathValue("backup_id")
	request.SourceComputerID = r.PathValue("computer_id")
	request.Actor = identityFromRequest(r).NodeID
	computer, replayed, err := s.store.BeginComputerClone(r.Context(), request)
	if err != nil {
		writeError(w, err)
		return
	}
	status := http.StatusAccepted
	if replayed {
		status = http.StatusOK
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, status, redactComputer(computer))
}

func (s *Server) installComputerProjection(w http.ResponseWriter, r *http.Request) {
	var request ComputerProjectionRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, err := s.store.InstallComputerProjection(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "computer_reimaged"); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, redactComputer(computer))
}

func (s *Server) reimageComputer(w http.ResponseWriter, r *http.Request) {
	var request ComputerReimageRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, err := s.store.ReimageComputer(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "computer_reimaged"); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, redactComputer(computer))
}

func (s *Server) removeComputer(w http.ResponseWriter, r *http.Request) {
	var request ComputerRemoveRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	request.Actor = identityFromRequest(r).NodeID
	computer, err := s.store.RemoveComputer(r.Context(), r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "computer_removed"); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, redactComputer(computer))
}

func (s *Server) revokeComputerAuthority(ctx context.Context, computerID, reason string) error {
	if s.computerTokenRevoker == nil {
		return nil
	}
	if _, err := s.computerTokenRevoker.RevokeComputerTokens(ctx, ComputerTokenRevocation{
		ComputerID: computerID, NewSubmitIntentRevision: 1, RevokeAll: true, Reason: reason,
	}); err != nil {
		return internalError(err, "revoke Computer token grants after authority loss")
	}
	return nil
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
	job, err = s.store.projectJob(r.Context(), job)
	if err != nil {
		writeError(w, err)
		return
	}
	job.Attempts, err = s.store.ListJobAttempts(r.Context(), job.JobID)
	if err != nil {
		writeError(w, err)
		return
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
	if registration.CapabilityRevision < 1 {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "capability revision must be positive"))
		return
	}
	identity := identityFromRequest(r)
	policy, configured := s.effectiveNodePolicy(registration.NodeID)
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
	nodeID := r.PathValue("node_id")
	policy, _ := s.effectiveNodePolicy(nodeID)
	node, err := s.store.HeartbeatNodeWithCapabilityObservation(
		r.Context(), identity.NodeID, nodeID, request.BootSessionID, heartbeatCapabilityObservation(request), policy,
	)
	if err != nil {
		writeError(w, err)
		return
	}
	directives, err := s.store.ListNodeRemovalDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	storageResets, err := s.store.ListNodeComputerStorageResetDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	storageGrows, err := s.store.ListNodeComputerStorageGrowDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	reimages, err := s.store.ListNodeComputerReimagePreflightDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	backups, err := s.store.ListNodeComputerBackupDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	backupPrunes, err := s.store.ListNodeComputerBackupPruneDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	restoreRevocations, err := s.store.ListNodeComputerRestoreRevocations(r.Context(), nodeID)
	if err != nil {
		writeError(w, err)
		return
	}
	for _, computerID := range restoreRevocations {
		if s.computerTokenRevoker == nil {
			writeError(w, internalError(errors.New("L3 Computer token revoker is not configured"),
				"revoke pre-restore Computer authority"))
			return
		}
		if err := s.revokeComputerAuthority(r.Context(), computerID, "computer_restoring"); err != nil {
			writeError(w, err)
			return
		}
		if err := s.store.RecordComputerRestoreAuthorityRevoked(r.Context(), computerID); err != nil {
			writeError(w, err)
			return
		}
	}
	storageCopies, err := s.store.ListNodeComputerStorageCopyDirectives(r.Context(), identity.NodeID, nodeID, request.BootSessionID)
	if err != nil {
		writeError(w, err)
		return
	}
	computerPolicy, err := s.store.IssueComputerPolicySnapshot(r.Context(), identity.NodeID, identity.FabricID, nodeID,
		request.BootSessionID, s.computerPolicyFreshness)
	if err != nil {
		computerPolicy = nil
	}
	writeJSON(w, http.StatusOK, HeartbeatResponse{Node: node, RemovalDirectives: directives,
		StorageResetDirectives: storageResets, StorageGrowDirectives: storageGrows, ReimageDirectives: reimages, BackupDirectives: backups,
		BackupPruneDirectives: backupPrunes, StorageCopyDirectives: storageCopies, ComputerPolicy: computerPolicy})
}

func (s *Server) watchComputerPolicy(w http.ResponseWriter, r *http.Request) {
	afterRevision, err := strconv.ParseInt(r.URL.Query().Get("after_revision"), 10, 64)
	if err != nil || afterRevision < 0 {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "after_revision must be non-negative"))
		return
	}
	identity := identityFromRequest(r)
	nodeID := r.PathValue("node_id")
	bootSessionID := r.URL.Query().Get("boot_session_id")
	change := s.store.computerPolicyChangeChannel()
	snapshot, err := s.store.IssueComputerPolicySnapshot(r.Context(), identity.NodeID, identity.FabricID, nodeID,
		bootSessionID, s.computerPolicyFreshness)
	if err != nil {
		writeError(w, err)
		return
	}
	if snapshot != nil && snapshot.PolicyRevision > afterRevision {
		writeJSON(w, http.StatusOK, snapshot)
		return
	}
	timer := newClockTimer(s.store.clock, s.computerPolicyWatchWait)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return
	case <-change:
	case <-timer.C():
	}
	snapshot, err = s.store.IssueComputerPolicySnapshot(r.Context(), identity.NodeID, identity.FabricID, nodeID,
		bootSessionID, s.computerPolicyFreshness)
	if err != nil {
		writeError(w, err)
		return
	}
	if snapshot == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) acknowledgeComputerPolicy(w http.ResponseWriter, r *http.Request) {
	var request ComputerPolicyInstallAcknowledgement
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	if request.NodeID != r.PathValue("node_id") {
		writeError(w, protocolError(contract.ErrorInvalidRequest, "node_id does not match route"))
		return
	}
	if err := s.store.AcknowledgeComputerPolicyInstallation(r.Context(), identityFromRequest(r).NodeID, request); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) acknowledgeComputerStorageReset(w http.ResponseWriter, r *http.Request) {
	var request ComputerStorageResetAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	computer, err := s.store.AcknowledgeComputerStorageReset(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) acknowledgeComputerStorageGrow(w http.ResponseWriter, r *http.Request) {
	var request ComputerStorageGrowAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	computer, err := s.store.AcknowledgeComputerStorageGrow(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if computer.CurrentJob.State == contract.JobFailed {
		if err := s.revokeComputerAuthority(r.Context(), computer.ComputerID, "computer_grow_capacity_failed"); err != nil {
			writeError(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) acknowledgeComputerReimagePreflight(w http.ResponseWriter, r *http.Request) {
	var request ComputerReimagePreflightAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	computer, err := s.store.AcknowledgeComputerReimagePreflight(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) acknowledgeComputerStorageRetirement(w http.ResponseWriter, r *http.Request) {
	var request RemovalAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	computer, err := s.store.AcknowledgeComputerStorageRetirement(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) acknowledgeComputerBackup(w http.ResponseWriter, r *http.Request) {
	var request ComputerBackupAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	backup, computer, err := s.store.AcknowledgeComputerBackup(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	response := ComputerBackupAcknowledgementResponse{Computer: redactComputer(computer)}
	if backup.BackupID != "" {
		response.Backup = &backup
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) acknowledgeComputerBackupPrune(w http.ResponseWriter, r *http.Request) {
	var request ComputerBackupPruneAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	backup, err := s.store.AcknowledgeComputerBackupPrune(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if backup.BackupID == "" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, backup)
}

func (s *Server) acknowledgeComputerStorageCopy(w http.ResponseWriter, r *http.Request) {
	var request ComputerStorageCopyAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	computer, err := s.store.AcknowledgeComputerStorageCopy(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
}

func (s *Server) acknowledgeComputerRestoreRetirement(w http.ResponseWriter, r *http.Request) {
	var request RemovalAcknowledgementRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	computer, err := s.store.AcknowledgeComputerRestoreRetirement(r.Context(), identityFromRequest(r).NodeID,
		r.PathValue("computer_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactComputer(computer))
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

func (s *Server) observeAttemptImage(w http.ResponseWriter, r *http.Request) {
	var request ImageObservationRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	job, err := s.store.ObserveAttemptImage(r.Context(), identity.NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactJob(job))
}

func (s *Server) startAttempt(w http.ResponseWriter, r *http.Request) {
	var request StartedRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	job, err := s.store.StartAttempt(r.Context(), identity.NodeID, r.PathValue("job_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, redactJob(job))
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

func (s *Server) appendComputerTakeoverAudit(w http.ResponseWriter, r *http.Request) {
	var request ComputerTakeoverAuditRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	identity := identityFromRequest(r)
	receipt, err := s.store.AppendComputerTakeoverAudit(
		r.Context(), identity.NodeID, r.PathValue("computer_id"), r.PathValue("job_id"),
		r.PathValue("attempt_id"), request,
	)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
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
	if computerID, lookupErr := s.store.ComputerIDForJob(r.Context(), job.JobID); lookupErr != nil {
		writeError(w, lookupErr)
		return
	} else if computerID != "" {
		if revokeErr := s.revokeComputerAuthority(r.Context(), computerID, "attempt_terminal"); revokeErr != nil {
			writeError(w, revokeErr)
			return
		}
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

func redactComputer(computer Computer) Computer {
	computer.CurrentJob = redactJob(computer.CurrentJob)
	computer.Grants = []ComputerGrant{}
	return computer
}

func (s *Server) notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeError(w, protocolError(contract.ErrorNotImplemented, "operation is reserved for a future version"))
}

func decodeJSON(r *http.Request, target any) error {
	return decodeJSONWithLimit(r, target, MaxRequestBodyBytes)
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
	case contract.ErrorUnauthorized, contract.ErrorPersonIdentityRequired:
		status = http.StatusUnauthorized
	case contract.ErrorForbidden, contract.ErrorPrincipalForbidden, contract.ErrorIdentityBound, contract.ErrorAttemptNotOwned,
		contract.ErrorAdminRequired, contract.ErrorAdminBootstrapInvalid:
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
