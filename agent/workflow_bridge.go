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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

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
	cancelServe        context.CancelFunc
	mu                 sync.Mutex
	reachable          bool
	reachability       context.Context
	cancelReachability context.CancelFunc
	closed             bool
}

func (a *Agent) startWorkflowBridge(ctx context.Context, kind string, execution contract.ExecutionSpec) (*workflowBridge, error) {
	computer := contract.IsComputerExecution(execution)
	if a.fabric == nil || (!computer && execution.Env[contract.EnvL3Endpoint] == "") {
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
			execution.SensitiveEnv[contract.EnvComputerToken] != "")
	}
	return newWorkflowBridgeWithSurface(ctx, a.fabric, a.runLedgerAddr, workloadrunner.WorkflowBridgeBinding{}, surface,
		execution.SensitiveEnv[contract.EnvComputerToken] != "")
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
	serveContext, cancelServe := context.WithCancel(ctx)
	bridge := &workflowBridge{
		listener:           binding.Listener,
		l3:                 workflowBridgeTransport(participant, l3Address),
		hostBridgeFallback: binding.HostBridgeFallback,
		surface:            surface,
		cancelServe:        cancelServe,
		dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", dialAddress)
		},
	}
	l3Proxy := workflowReverseProxy(bridge.l3, "/l3")
	mux := http.NewServeMux()
	if surface == workflowBridgeSurfaceComputer {
		mux.Handle("/l3/", bridge.computerHandler(l3Proxy))
		bridge.setReachable(reachable)
	} else {
		mux.Handle("/l3/", l3Proxy)
	}
	bridge.server = &http.Server{Handler: mux, BaseContext: func(net.Listener) context.Context { return serveContext }}
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
			writeWorkflowBridgeError(w, http.StatusServiceUnavailable, contract.ErrorInternal, "Computer attempt bridge is not reachable")
			return
		}
		defer cancel()
		next.ServeHTTP(w, request.WithContext(requestContext))
	})
}

func computerBridgeRouteAllowed(method, path string) bool {
	if method == http.MethodGet && (path == "/v1/computer/self" || path == "/v1/runs") {
		return true
	}
	if method == http.MethodPost && path == "/v1/runs" {
		return true
	}
	if method != http.MethodGet || !strings.HasPrefix(path, "/v1/runs/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/v1/runs/"), "/")
	if len(parts) == 1 {
		return parts[0] != ""
	}
	if len(parts) != 2 || parts[0] == "" {
		return false
	}
	switch parts[1] {
	case "lineage", "logs", "envelopes":
		return true
	default:
		return false
	}
}

func (b *workflowBridge) setReachable(reachable bool) {
	if b == nil || b.surface != workflowBridgeSurfaceComputer {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancelReachability != nil {
		b.cancelReachability()
		b.cancelReachability = nil
		b.reachability = nil
	}
	b.reachable = reachable && !b.closed
	if b.reachable {
		b.reachability, b.cancelReachability = context.WithCancel(context.Background())
	}
}

func (b *workflowBridge) reachableRequestContext(parent context.Context) (context.Context, context.CancelFunc, bool) {
	b.mu.Lock()
	reachability, reachable := b.reachability, b.reachable && !b.closed
	b.mu.Unlock()
	if !reachable || reachability == nil {
		return nil, nil, false
	}
	requestContext, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(reachability, cancel)
	return requestContext, func() {
		stop()
		cancel()
	}, true
}

func workflowBridgeTransport(participant fabric.Fabric, address string) *http.Transport {
	return &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
}

func workflowReverseProxy(transport http.RoundTripper, prefix string) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(request *http.Request) {
			request.URL.Scheme = "http"
			request.URL.Host = "wefty.invalid"
			request.URL.Path = strings.TrimPrefix(request.URL.Path, prefix)
			request.Host = "wefty.invalid"
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			writeWorkflowBridgeError(w, http.StatusBadGateway, contract.ErrorInternal, err.Error())
		},
	}
}

func writeWorkflowBridgeError(w http.ResponseWriter, status int, code contract.ErrorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: message}})
}

func (b *workflowBridge) close() error {
	b.mu.Lock()
	b.closed = true
	b.reachable = false
	if b.cancelReachability != nil {
		b.cancelReachability()
		b.cancelReachability = nil
		b.reachability = nil
	}
	b.mu.Unlock()
	b.cancelServe()
	err := b.server.Close()
	b.l3.CloseIdleConnections()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
