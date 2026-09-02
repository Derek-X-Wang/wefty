package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

const workflowBridgeCloseTimeout = 2 * time.Second

var errComputerSubmissionRevoked = errors.New("Computer submission authority revoked")

type workflowBridgeSurface uint8

const (
	workflowBridgeSurfaceRun workflowBridgeSurface = iota
	workflowBridgeSurfaceComputer
)

type workflowBridge struct {
	l3Endpoint         string
	listener           net.Listener
	server             *http.Server
	l3                 *http.Transport
	hostBridgeFallback bool
	dial               func(context.Context) (net.Conn, error)
	surface            workflowBridgeSurface
	mu                 sync.Mutex
	reachable          bool
	reachability       context.Context
	cancelReachability context.CancelCauseFunc
	closed             bool
}

func (a *Agent) startWorkflowBridge(ctx context.Context, kind string, execution contract.ExecutionSpec) (*workflowBridge, error) {
	computer := contract.IsComputerExecution(execution)
	computerEnabled := computer && execution.SensitiveEnv[contract.EnvComputerToken] != ""
	if a.fabric == nil || (computer && !computerEnabled) || (!computer && execution.Env[contract.EnvL3Endpoint] == "") {
		return nil, nil
	}
	surface := workflowBridgeSurfaceRun
	if computer {
		surface = workflowBridgeSurfaceComputer
	}
	if kind == contract.JobKindOCI && a.ociBridgeBinder != nil {
		binding, err := a.ociBridgeBinder.Bind(ctx)
		if err != nil {
			return nil, err
		}
		return newWorkflowBridgeWithSurface(ctx, a.fabric, a.runLedgerAddr, binding, surface,
			computerEnabled)
	}
	return newWorkflowBridgeWithSurface(ctx, a.fabric, a.runLedgerAddr, workloadrunner.WorkflowBridgeBinding{}, surface,
		computerEnabled)
}

func newWorkflowBridge(ctx context.Context, participant fabric.Fabric, l3Address string) (*workflowBridge, error) {
	return newWorkflowBridgeWithSurface(ctx, participant, l3Address, workloadrunner.WorkflowBridgeBinding{}, workflowBridgeSurfaceRun, true)
}

func newComputerAttemptBridge(ctx context.Context, participant fabric.Fabric, l3Address string, reachable bool) (*workflowBridge, error) {
	return newWorkflowBridgeWithSurface(ctx, participant, l3Address, workloadrunner.WorkflowBridgeBinding{}, workflowBridgeSurfaceComputer, reachable)
}

func newWorkflowBridgeWithSurface(ctx context.Context, participant fabric.Fabric, l3Address string, binding workloadrunner.WorkflowBridgeBinding, surface workflowBridgeSurface, reachable bool) (*workflowBridge, error) {
	if binding.Listener != nil || binding.AdvertiseHost != "" {
		return newWorkflowBridgeWithBindingAndSurface(ctx, participant, l3Address, binding, surface, reachable)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return newWorkflowBridgeWithBindingAndSurface(ctx, participant, l3Address, workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	}, surface, reachable)
}

func newWorkflowBridgeWithBinding(ctx context.Context, participant fabric.Fabric, l3Address string, binding workloadrunner.WorkflowBridgeBinding) (*workflowBridge, error) {
	return newWorkflowBridgeWithBindingAndSurface(ctx, participant, l3Address, binding, workflowBridgeSurfaceRun, true)
}

func newWorkflowBridgeWithBindingAndSurface(ctx context.Context, participant fabric.Fabric, l3Address string, binding workloadrunner.WorkflowBridgeBinding, surface workflowBridgeSurface, reachable bool) (*workflowBridge, error) {
	if binding.Listener == nil || binding.AdvertiseHost == "" {
		return nil, errors.New("workflow bridge binding is incomplete")
	}
	tcpAddress, ok := binding.Listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.Port <= 0 {
		_ = binding.Listener.Close()
		return nil, errors.New("workflow bridge binding is not TCP")
	}
	if tcpAddress.IP == nil || tcpAddress.IP.IsUnspecified() {
		_ = binding.Listener.Close()
		return nil, errors.New("workflow bridge binding must not listen on an unspecified address")
	}
	if advertisedIP := net.ParseIP(binding.AdvertiseHost); advertisedIP != nil && advertisedIP.IsUnspecified() {
		_ = binding.Listener.Close()
		return nil, errors.New("workflow bridge must not advertise an unspecified address")
	}
	dialer := &net.Dialer{}
	dialAddress := binding.Listener.Addr().String()
	bridge := &workflowBridge{
		listener:           binding.Listener,
		l3:                 workflowBridgeTransport(participant, l3Address),
		hostBridgeFallback: binding.HostBridgeFallback,
		surface:            surface,
		dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", dialAddress)
		},
	}
	proxyError := contract.ErrorInternal
	if surface == workflowBridgeSurfaceComputer {
		proxyError = contract.ErrorPassUnavailable
	}
	l3Proxy := workflowReverseProxy(bridge.l3, "/l3", proxyError)
	var handler http.Handler
	if surface == workflowBridgeSurfaceComputer {
		l3Proxy.ErrorHandler = func(w http.ResponseWriter, request *http.Request, err error) {
			if errors.Is(context.Cause(request.Context()), errComputerSubmissionRevoked) {
				writeWorkflowBridgeError(w, http.StatusUnauthorized, contract.ErrorUnauthorized, errComputerSubmissionRevoked.Error())
				return
			}
			writeWorkflowBridgeError(w, http.StatusBadGateway, contract.ErrorPassUnavailable, err.Error())
		}
		computer := bridge.computerHandler(l3Proxy)
		handler = http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if !strings.HasPrefix(request.URL.Path, "/l3/") {
				http.NotFound(w, request)
				return
			}
			computer.ServeHTTP(w, request)
		})
		bridge.setReachable(reachable)
	} else {
		mux := http.NewServeMux()
		mux.Handle("/l3/", l3Proxy)
		handler = mux
	}
	bridge.server = &http.Server{Handler: handler, BaseContext: func(net.Listener) context.Context { return ctx }}
	baseURL := "http://" + net.JoinHostPort(binding.AdvertiseHost, strconv.Itoa(tcpAddress.Port))
	bridge.l3Endpoint = baseURL + "/l3"
	go func() {
		_ = bridge.server.Serve(binding.Listener)
	}()
	return bridge, nil
}

func (b *workflowBridge) computerHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !computerBridgeRouteAllowed(request.Method, strings.TrimPrefix(request.URL.Path, "/l3")) {
			writeWorkflowBridgeError(w, http.StatusForbidden, contract.ErrorForbidden, "route is outside the Computer attempt bridge allowlist")
			return
		}
		requestContext, cancel, ok := b.reachableRequestContext(request.Context())
		if !ok {
			writeWorkflowBridgeError(w, http.StatusServiceUnavailable, contract.ErrorPassUnavailable, "Computer attempt bridge is not reachable")
			return
		}
		defer cancel()
		next.ServeHTTP(w, request.WithContext(requestContext))
	})
}

func computerBridgeRouteAllowed(method, path string) bool {
	for _, route := range computerBridgeRoutes {
		if method == route.Method && computerBridgePathMatches(route.Path, path) {
			return true
		}
	}
	return false
}

var computerBridgeRoutes = []l3.ComputerTokenRoute{
	{Method: http.MethodGet, Path: "/v1/computer/self"},
	{Method: http.MethodGet, Path: "/v1/runs"},
	{Method: http.MethodPost, Path: "/v1/runs"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/lineage"},
	{Method: http.MethodGet, Path: "/v1/runs/{run_id}/logs"},
}

func computerBridgePathMatches(pattern, requestPath string) bool {
	patternParts := strings.Split(strings.TrimPrefix(pattern, "/"), "/")
	requestParts := strings.Split(strings.TrimPrefix(requestPath, "/"), "/")
	if len(patternParts) != len(requestParts) || !strings.HasPrefix(requestPath, "/") {
		return false
	}
	for index := range patternParts {
		if patternParts[index] == "{run_id}" {
			if requestParts[index] == "" || requestParts[index] == "." || requestParts[index] == ".." ||
				strings.TrimSpace(requestParts[index]) != requestParts[index] {
				return false
			}
			continue
		}
		if requestParts[index] != patternParts[index] {
			return false
		}
	}
	return true
}

func (b *workflowBridge) setReachable(reachable bool) {
	if b == nil || b.surface != workflowBridgeSurfaceComputer {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancelReachability != nil {
		b.cancelReachability(errComputerSubmissionRevoked)
		b.cancelReachability = nil
		b.reachability = nil
	}
	b.reachable = reachable && !b.closed
	if b.reachable {
		b.reachability, b.cancelReachability = context.WithCancelCause(context.Background())
	}
}

func (b *workflowBridge) reachableRequestContext(parent context.Context) (context.Context, context.CancelFunc, bool) {
	b.mu.Lock()
	reachability, reachable := b.reachability, b.reachable && !b.closed
	b.mu.Unlock()
	if !reachable || reachability == nil {
		return nil, nil, false
	}
	requestContext, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(reachability, func() { cancel(context.Cause(reachability)) })
	return requestContext, func() {
		stop()
		cancel(context.Canceled)
	}, true
}

func workflowBridgeTransport(participant fabric.Fabric, address string) *http.Transport {
	return &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
}

func workflowReverseProxy(transport http.RoundTripper, prefix string, errorCode contract.ErrorCode) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL.Scheme = "http"
			request.URL.Host = "wefty.invalid"
			request.URL.Path = strings.TrimPrefix(request.URL.Path, prefix)
			request.Host = "wefty.invalid"
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeWorkflowBridgeError(w, http.StatusBadGateway, errorCode, err.Error())
		},
	}
}

func writeWorkflowBridgeError(w http.ResponseWriter, status int, code contract.ErrorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: message}})
}

func (b *workflowBridge) close() error {
	if b.surface == workflowBridgeSurfaceComputer {
		b.mu.Lock()
		b.closed = true
		if b.cancelReachability != nil {
			b.cancelReachability(errComputerSubmissionRevoked)
			b.cancelReachability = nil
			b.reachability = nil
		}
		b.reachable = false
		b.mu.Unlock()
	}
	closeContext, cancel := context.WithTimeout(context.Background(), workflowBridgeCloseTimeout)
	defer cancel()
	err := b.server.Shutdown(closeContext)
	b.l3.CloseIdleConnections()
	if errors.Is(err, context.DeadlineExceeded) {
		_ = b.listener.Close()
	}
	return err
}
