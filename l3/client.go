package l3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// JobClient is the submission and job-read dependency of the ledger reconciler.
// Only classified L1 submission answers can express a work refusal. Every
// other submission error, including errors from alternate clients without
// classified HTTP evidence, is transient and never fails a Run.
type JobClient interface {
	SubmitJob(context.Context, contract.JobSpec) (l1.Job, error)
	GetJob(context.Context, string) (l1.Job, error)
}

// JobCancelClient is the public L1 one-shot cancellation seam. It is optional
// so clients that only submit or read jobs retain their existing contract.
type JobCancelClient interface {
	CancelJob(context.Context, string) (l1.Job, error)
}

// JobDispatchLookupClient is the lookup-only recovery seam for a dispatch L1
// accepted before L3 recorded its job ID. It stays separate from JobClient so
// existing clients and test fakes do not acquire another required method.
type JobDispatchLookupClient interface {
	LookupJobByDispatchKey(context.Context, string) (l1.Job, error)
}

// JobImageEvidenceClient is the L1 result-ingestion seam for tag-only image
// runs. It remains separate from JobClient so tests and alternate L1 clients
// can implement the evidence projection independently.
type JobImageEvidenceClient interface {
	GetJobImageEvidence(context.Context, string) ([]AttemptImageEvidence, error)
}

// JobLogClient is the public L1 log-polling dependency of the L3 API.
type JobLogClient interface {
	GetJobLogs(context.Context, string, string, int) (l1.LogPage, error)
}

// JobResultClient is the L1 result-read dependency of the L3 API. It is its
// own interface rather than another method on JobLogClient so an existing
// client or fake does not have to grow a method to keep compiling.
type JobResultClient interface {
	GetJobResult(context.Context, string) (l1.JobResult, error)
}

type ComputerGrantVerifier interface {
	ProveComputerTokenScope(context.Context, string, string, string, string) (ComputerTokenScopeProof, error)
}

// HostBootSessionVerifier is the L1 authority seam for startup revocation:
// it proves that a boot session is the current boot of one stable node bound
// to the given Fabric identity. It stays separate from ComputerGrantVerifier
// so alternate L1 clients and test fakes can implement either capability
// independently.
type HostBootSessionVerifier interface {
	ProveHostBootSession(ctx context.Context, hostIdentityNodeID, hostStableNodeID, bootSessionID string) error
}

// L1Client calls the L1 client protocol exclusively through Fabric.Dial.
type L1Client struct {
	client           *http.Client
	operationTimeout time.Duration
}

const l1OperationTimeout = 10 * time.Second

type l1RequestContextKey struct{}

func NewL1Client(f fabric.Fabric, address string) (*L1Client, error) {
	return newL1Client(f, address, l1OperationTimeout, l1OperationTimeout, l1OperationTimeout)
}

func newL1Client(f fabric.Fabric, address string, operationTimeout, dialTimeout, headerTimeout time.Duration) (*L1Client, error) {
	if operationTimeout <= 0 || dialTimeout <= 0 || headerTimeout <= 0 {
		return nil, fmt.Errorf("l3: positive L1 client timeouts are required")
	}
	if f == nil {
		return nil, fmt.Errorf("l3: fabric is required for L1 client")
	}
	if strings.TrimSpace(address) == "" {
		address = DefaultL1Address
	}
	transport := &http.Transport{ResponseHeaderTimeout: headerTimeout, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		// net/http detaches dial cancellation to permit connection reuse. Restore
		// this operation's bound so a canceled request cannot leave Fabric dialing.
		if requestCtx, ok := ctx.Value(l1RequestContextKey{}).(context.Context); ok {
			ctx = requestCtx
		}
		dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
		defer cancel()
		return f.Dial(dialCtx, network, address)
	}}
	return &L1Client{client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, operationTimeout: operationTimeout}, nil
}

func (c *L1Client) CloseIdleConnections() { c.client.CloseIdleConnections() }

func (c *L1Client) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	var job l1.Job
	if err := c.do(ctx, http.MethodPost, "/v1/jobs", spec, &job, http.StatusCreated, http.StatusOK); err != nil {
		return l1.Job{}, err
	}
	if job.JobID == "" || job.Spec.DispatchKey != spec.DispatchKey {
		return l1.Job{}, invalidL1Success(http.MethodPost, "/v1/jobs", "submission acknowledgement")
	}
	return job, nil
}

func (c *L1Client) CancelJob(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	if err := c.do(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/cancel", nil, &job, http.StatusOK); err != nil {
		return l1.Job{}, err
	}
	if job.JobID != jobID {
		return l1.Job{}, invalidL1Success(http.MethodPost, "/v1/jobs/"+url.PathEscape(jobID)+"/cancel", "cancel acknowledgement")
	}
	return job, nil
}

func (c *L1Client) GetJob(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/jobs/" + url.PathEscape(jobID)
	if err := c.do(ctx, http.MethodGet, path, nil, &job, http.StatusOK); err != nil {
		if authoritativeL1NotFound(err, http.MethodGet, path) {
			return l1.Job{}, &JobNotFoundError{JobID: jobID, Cause: err}
		}
		return l1.Job{}, err
	}
	if job.JobID != jobID {
		return l1.Job{}, invalidL1Success(http.MethodGet, path, "job projection")
	}
	return job, nil
}

func (c *L1Client) LookupJobByDispatchKey(ctx context.Context, dispatchKey string) (l1.Job, error) {
	var job l1.Job
	path := "/v1/dispatch-keys/" + url.PathEscape(dispatchKey) + "/job"
	if err := c.do(ctx, http.MethodGet, path, nil, &job, http.StatusOK); err != nil {
		if authoritativeL1NotFound(err, http.MethodGet, path) {
			return l1.Job{}, &DispatchNotFoundError{DispatchKey: dispatchKey, Cause: err}
		}
		return l1.Job{}, err
	}
	if _, known := contract.JobTransitions[job.State]; job.JobID == "" || job.Spec.DispatchKey != dispatchKey || !known || job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() {
		return l1.Job{}, &l1ResponseError{status: http.StatusOK, method: http.MethodGet, path: path, protocol: &Error{Code: contract.ErrorInternal, Message: "invalid L1 dispatch lookup response", Retryable: true}}
	}
	return job, nil
}

func (c *L1Client) GetJobImageEvidence(ctx context.Context, jobID string) ([]AttemptImageEvidence, error) {
	job, err := c.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	evidence := make([]AttemptImageEvidence, 0, len(job.Attempts))
	for _, attempt := range job.Attempts {
		if attempt.Image == nil {
			continue
		}
		var platformDigest *string
		if attempt.Image.PlatformManifestDigest != "" {
			value := attempt.Image.PlatformManifestDigest
			platformDigest = &value
		}
		evidence = append(evidence, AttemptImageEvidence{
			AttemptID: attempt.AttemptID, SubmittedReference: attempt.Image.SubmittedReference,
			TopLevelDigest: attempt.Image.TopLevelDigest, PlatformDigest: platformDigest,
			ObservedAt: attempt.Image.ResolvedAt,
		})
	}
	return evidence, nil
}

func (c *L1Client) GetJobLogs(ctx context.Context, jobID, cursor string, limit int) (l1.LogPage, error) {
	path := "/v1/jobs/" + jobID + "/logs?limit=" + strconv.Itoa(limit)
	if cursor != "" {
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	var page l1.LogPage
	if err := c.do(ctx, http.MethodGet, path, nil, &page, http.StatusOK); err != nil {
		return l1.LogPage{}, err
	}
	return page, nil
}

func (c *L1Client) GetJobResult(ctx context.Context, jobID string) (l1.JobResult, error) {
	var result l1.JobResult
	path := "/v1/jobs/" + url.PathEscape(jobID) + "/result"
	if err := c.do(ctx, http.MethodGet, path, nil, &result, http.StatusOK); err != nil {
		return l1.JobResult{}, err
	}
	return result, nil
}

func (c *L1Client) ProveComputerTokenScope(ctx context.Context, computerID, attemptID, hostIdentityNodeID, hostNodeID string) (ComputerTokenScopeProof, error) {
	var proof l1.ComputerTokenScopeProof
	path := "/v1/computers/" + url.PathEscape(computerID) + "/token-scope-proof"
	request := map[string]string{"computer_attempt_id": attemptID, "host_identity_node_id": hostIdentityNodeID, "host_node_id": hostNodeID}
	if err := c.do(ctx, http.MethodPost, path, request, &proof, http.StatusOK); err != nil {
		return ComputerTokenScopeProof{}, err
	}
	return ComputerTokenScopeProof{ComputerID: proof.ComputerID, ComputerAttemptID: proof.ComputerAttemptID,
		ComputerStorageGeneration: proof.ComputerStorageGeneration, SubmitIntentRevision: proof.SubmitIntentRevision,
		HostNodeID: proof.HostNodeID, HostStableNodeID: proof.HostStableNodeID, HostBootSessionID: proof.HostBootSessionID,
		SubmitMaxInflight: proof.SubmitMaxInflight}, nil
}

func (c *L1Client) ProveHostBootSession(ctx context.Context, hostIdentityNodeID, hostStableNodeID, bootSessionID string) error {
	request := map[string]string{"host_identity_node_id": hostIdentityNodeID, "host_stable_node_id": hostStableNodeID,
		"boot_session_id": bootSessionID}
	var proof struct {
		HostIdentityNodeID string `json:"host_identity_node_id"`
		HostStableNodeID   string `json:"host_stable_node_id"`
		BootSessionID      string `json:"boot_session_id"`
	}
	if err := c.do(ctx, http.MethodPost, "/v1/host-boot-session-proof", request, &proof, http.StatusOK); err != nil {
		return err
	}
	if proof.HostIdentityNodeID != hostIdentityNodeID || proof.HostStableNodeID != hostStableNodeID || proof.BootSessionID != bootSessionID {
		return internalError(fmt.Errorf("L1 echoed host boot session %q/%q/%q, want %q/%q/%q",
			proof.HostIdentityNodeID, proof.HostStableNodeID, proof.BootSessionID, hostIdentityNodeID, hostStableNodeID, bootSessionID),
			"validate L1 host boot session proof")
	}
	return nil
}

func (c *L1Client) do(ctx context.Context, method, path string, body any, target any, success ...int) error {
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return internalError(err, "encode L1 request")
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(context.WithValue(ctx, l1RequestContextKey{}, ctx), method, "http://control-plane.invalid"+path, reader)
	if err != nil {
		return internalError(err, "create L1 request")
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return &l1ResponseError{transportFailure: true, requestMethod: method, requestPath: path, method: method, path: path, protocol: &Error{Code: contract.ErrorInternal, Message: "call L1 control plane", Retryable: true, Cause: err}}
	}
	defer response.Body.Close()
	// A redirected response belongs to its final endpoint, not the original
	// GetJob request. Preserve that origin before classifying absence.
	redirected := response.StatusCode >= 300 && response.StatusCode < 400
	if response.Request != nil && (response.Request.Method != method || response.Request.URL.RequestURI() != path || response.Request.URL.Host != request.URL.Host) {
		redirected = true
	}
	if response.Request != nil {
		method, path = response.Request.Method, response.Request.URL.RequestURI()
	}
	evidence := l1ErrorEvidence(nil, response.Header.Get("X-Request-Id"))
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if err != nil {
		return &l1ResponseError{transportFailure: true, requestMethod: request.Method, requestPath: request.URL.RequestURI(), status: response.StatusCode, method: method, path: path, protocol: &Error{Code: contract.ErrorInternal, Message: "read L1 response", Retryable: true, Cause: err}}
	}
	// Read one extra byte so a truncated JSON prefix cannot establish absence.
	if len(responseBody) > 2<<20 {
		return &l1ResponseError{requestMethod: request.Method, requestPath: request.URL.RequestURI(), status: response.StatusCode, method: method, path: path, redirected: redirected, evidence: evidence, protocol: &Error{
			Code: contract.ErrorInternal, Message: "L1 response exceeds size limit",
			Retryable: response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests,
		}}
	}
	if redirected {
		evidence = l1ErrorEvidence(responseBody, response.Header.Get("X-Request-Id"))
		return &l1ResponseError{requestMethod: request.Method, requestPath: request.URL.RequestURI(), status: response.StatusCode, method: method, path: path, redirected: true, evidence: evidence, protocol: &Error{Code: contract.ErrorInternal, Message: "L1 redirected the request", Retryable: true}}
	}
	for _, status := range success {
		if response.StatusCode == status {
			if err := json.Unmarshal(responseBody, target); err != nil {
				return &l1ResponseError{requestMethod: request.Method, requestPath: request.URL.RequestURI(), status: response.StatusCode, method: method, path: path, redirected: redirected, evidence: evidence, protocol: &Error{Code: contract.ErrorInternal, Message: "decode L1 response", Retryable: true, Cause: err}}
			}
			if job, ok := target.(*l1.Job); ok {
				_, knownOneShot := contract.JobTransitions[job.State]
				_, knownService := contract.ServiceJobTransitions[job.State]
				if job.JobID == "" || (!knownOneShot && !knownService) {
					return invalidL1Success(method, path, "job response")
				}
			}
			return nil
		}
	}
	evidence = l1ErrorEvidence(responseBody, response.Header.Get("X-Request-Id"))
	var responseError contract.ErrorResponse
	if err := json.Unmarshal(responseBody, &responseError); err != nil || responseError.Error.Code == "" {
		return &l1ResponseError{requestMethod: request.Method, requestPath: request.URL.RequestURI(), status: response.StatusCode, method: method, path: path, redirected: redirected, evidence: evidence, protocol: &Error{Code: contract.ErrorInternal, Message: fmt.Sprintf("L1 returned HTTP %d", response.StatusCode), Retryable: response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests}}
	}
	// Normalise every decoded error at the client seam, before classification
	// or any persistence, logging, hold creation or HTTP relay can use it.
	responseError.Error = boundedL1Error(responseError.Error)
	return &l1ResponseError{requestMethod: request.Method, requestPath: request.URL.RequestURI(), status: response.StatusCode, method: method, path: path, redirected: redirected, validEnvelope: validL1ErrorEnvelope(responseBody), evidence: evidence, protocol: &Error{
		Code: responseError.Error.Code, Message: responseError.Error.Message,
		Retryable: responseError.Error.Retryable, Details: responseError.Error.Details,
		RequestID: responseError.Error.RequestID,
	}}
}

var _ JobClient = (*L1Client)(nil)
var _ JobDispatchLookupClient = (*L1Client)(nil)
var _ JobImageEvidenceClient = (*L1Client)(nil)
var _ JobLogClient = (*L1Client)(nil)
var _ JobResultClient = (*L1Client)(nil)
var _ ComputerGrantVerifier = (*L1Client)(nil)
var _ HostBootSessionVerifier = (*L1Client)(nil)

// Keep the response origin internal while preserving errors.As(*Error).
type l1ResponseError struct {
	transportFailure           bool
	requestMethod, requestPath string
	status                     int
	method, path               string
	validEnvelope              bool
	redirected                 bool
	protocol                   *Error
	evidence                   map[string]any
}

func (e *l1ResponseError) Error() string { return e.protocol.Error() }
func (e *l1ResponseError) Unwrap() error { return e.protocol }

func authoritativeL1NotFound(err error, method, path string) bool {
	a := classifyL1Answer(err)
	return a.Kind == l1AuthoritativeAbsence && a.Method == method && a.Target == path
}

// Absence is destructive evidence: require all mandatory envelope fields rather
// than accepting a partial JSON object whose missing fields decode to zero.
func validL1ErrorEnvelope(body []byte) bool {
	var envelope struct {
		Error *struct {
			Code      *string `json:"code"`
			Message   *string `json:"message"`
			Retryable *bool   `json:"retryable"`
		} `json:"error"`
	}
	return json.Unmarshal(body, &envelope) == nil && envelope.Error != nil && envelope.Error.Code != nil && *envelope.Error.Code != "" && envelope.Error.Message != nil && envelope.Error.Retryable != nil
}

func invalidL1Success(method, path, what string) error {
	return &l1ResponseError{status: http.StatusOK, method: method, path: path, protocol: &Error{Code: contract.ErrorInternal, Message: "invalid L1 " + what, Retryable: true}}
}

// Retain only typed diagnostic fields, including an explicitly false retryable.
// An incomplete envelope cannot establish refusal, but can still supply evidence.
// Bound strings before they reach persisted errors, reconcile logs or headers.
func l1ErrorEvidence(body []byte, requestID string) map[string]any {
	evidence := make(map[string]any)
	if requestID != "" {
		evidence["request_id"] = boundedL1Evidence(requestID)
	}
	var envelope struct {
		Error map[string]json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return evidence
	}
	for _, field := range []string{"code", "retryable", "request_id"} {
		raw, ok := envelope.Error[field]
		if !ok || string(raw) == "null" {
			continue
		}
		if field == "retryable" {
			var value bool
			if json.Unmarshal(raw, &value) == nil {
				evidence[field] = value
			}
		} else {
			var value string
			if json.Unmarshal(raw, &value) == nil {
				evidence[field] = boundedL1Evidence(value)
			}
		}
	}
	return evidence
}

// All L1 error strings use the same bound, including nested detail strings and
// the reason that becomes a Dispatch hold. Non-string detail values retain
// their decoded types. Raw response bodies never leave the client seam.
func boundedL1Error(value contract.APIError) contract.APIError {
	value.Code = contract.ErrorCode(boundedL1Evidence(string(value.Code)))
	value.Message = boundedL1Evidence(value.Message)
	value.RequestID = boundedL1Evidence(value.RequestID)
	if value.Details != nil {
		value.Details = boundedL1Details(value.Details).(map[string]any)
	}
	return value
}

const l1DetailsBytes = 4 << 10

func boundedL1Details(value any) any {
	details, _, _ := boundedL1DetailValue(value, l1DetailsBytes)
	return details
}

// Count the encoded JSON, including delimiters and escaping, against one
// shared budget. Containers keep a deterministic prefix; nested containers
// use only the space left by their parent. Decoded non-string types survive.
func boundedL1DetailValue(value any, budget int) (any, int, bool) {
	switch value := value.(type) {
	case string:
		value = boundedL1Evidence(value)
		encoded, _ := json.Marshal(value)
		return value, len(encoded), len(encoded) <= budget
	case map[string]any:
		if budget < 2 {
			return nil, 0, false
		}
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		// reason is the only detail read by classifyL1Answer. Reserve it
		// before optional evidence, including malformed non-string reasons.
		if _, ok := value["reason"]; ok {
			index := sort.SearchStrings(keys, "reason")
			copy(keys[1:index+1], keys[:index])
			keys[0] = "reason"
		}
		details := make(map[string]any)
		size := 2 // braces
		for _, key := range keys {
			boundedKey := boundedL1Evidence(key)
			if _, exists := details[boundedKey]; exists {
				continue // first sorted original key wins a truncation collision
			}
			encodedKey, _ := json.Marshal(boundedKey)
			overhead := len(encodedKey) + 1 // colon
			if len(details) > 0 {
				overhead++ // comma
			}
			item, itemSize, fits := boundedL1DetailValue(value[key], budget-size-overhead)
			if !fits {
				break
			}
			details[boundedKey] = item
			size += overhead + itemSize
		}
		return details, size, true
	case []any:
		if budget < 2 {
			return nil, 0, false
		}
		items := make([]any, 0)
		size := 2 // brackets
		for _, item := range value {
			overhead := 0
			if len(items) > 0 {
				overhead = 1 // comma
			}
			bounded, itemSize, fits := boundedL1DetailValue(item, budget-size-overhead)
			if !fits {
				break
			}
			items = append(items, bounded)
			size += overhead + itemSize
		}
		return items, size, true
	default:
		encoded, err := json.Marshal(value)
		return value, len(encoded), err == nil && len(encoded) <= budget
	}
}

// The ASCII marker occupies the last three of 128 runes. Iterate only far
// enough to find the boundary, preserving UTF-8 without a large rune slice.
func boundedL1Evidence(value string) string {
	const limit = 128
	var runes, cutoff int
	for i := range value {
		if runes == limit-3 {
			cutoff = i
		}
		if runes == limit {
			return value[:cutoff] + "..."
		}
		runes++
	}
	return value
}
