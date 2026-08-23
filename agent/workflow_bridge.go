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
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

const workflowBridgeCloseTimeout = 2 * time.Second

type workflowBridge struct {
	l3Endpoint         string
	listener           net.Listener
	server             *http.Server
	l3                 *http.Transport
	hostBridgeFallback bool
	dial               func(context.Context) (net.Conn, error)
}

func (a *Agent) startWorkflowBridge(ctx context.Context, kind string, execution contract.ExecutionSpec) (*workflowBridge, error) {
	if a.fabric == nil || execution.Env[contract.EnvL3Endpoint] == "" {
		return nil, nil
	}
	if kind == contract.JobKindOCI && a.ociBridgeBinder != nil {
		binding, err := a.ociBridgeBinder.Bind(ctx)
		if err != nil {
			return nil, err
		}
		return newWorkflowBridgeWithBinding(ctx, a.fabric, a.runLedgerAddr, binding)
	}
	return newWorkflowBridge(ctx, a.fabric, a.runLedgerAddr)
}

func newWorkflowBridge(ctx context.Context, participant fabric.Fabric, l3Address string) (*workflowBridge, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	return newWorkflowBridgeWithBinding(ctx, participant, l3Address, workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	})
}

func newWorkflowBridgeWithBinding(ctx context.Context, participant fabric.Fabric, l3Address string, binding workloadrunner.WorkflowBridgeBinding) (*workflowBridge, error) {
	if binding.Listener == nil || binding.AdvertiseHost == "" {
		return nil, errors.New("workflow bridge binding is incomplete")
	}
	tcpAddress, ok := binding.Listener.Addr().(*net.TCPAddr)
	if !ok || tcpAddress.Port <= 0 {
		_ = binding.Listener.Close()
		return nil, errors.New("workflow bridge binding is not TCP")
	}
	dialer := &net.Dialer{}
	dialAddress := binding.Listener.Addr().String()
	bridge := &workflowBridge{
		listener:           binding.Listener,
		l3:                 workflowBridgeTransport(participant, l3Address),
		hostBridgeFallback: binding.HostBridgeFallback,
		dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", dialAddress)
		},
	}
	l3Proxy := workflowReverseProxy(bridge.l3, "/l3")
	mux := http.NewServeMux()
	mux.Handle("/l3/", l3Proxy)
	bridge.server = &http.Server{Handler: mux, BaseContext: func(net.Listener) context.Context { return ctx }}
	baseURL := "http://" + net.JoinHostPort(binding.AdvertiseHost, strconv.Itoa(tcpAddress.Port))
	bridge.l3Endpoint = baseURL + "/l3"
	go func() {
		_ = bridge.server.Serve(binding.Listener)
	}()
	return bridge, nil
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
	closeContext, cancel := context.WithTimeout(context.Background(), workflowBridgeCloseTimeout)
	defer cancel()
	err := b.server.Shutdown(closeContext)
	b.l3.CloseIdleConnections()
	if errors.Is(err, context.DeadlineExceeded) {
		_ = b.listener.Close()
	}
	return err
}
