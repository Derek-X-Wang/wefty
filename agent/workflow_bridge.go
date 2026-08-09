package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const workflowBridgeCloseTimeout = 2 * time.Second

type workflowBridge struct {
	l1Endpoint string
	l3Endpoint string
	listener   net.Listener
	server     *http.Server
	l1         *http.Transport
	l3         *http.Transport
}

func (a *Agent) startWorkflowBridge(ctx context.Context, execution contract.ExecutionSpec) (*workflowBridge, error) {
	if a.fabric == nil || execution.Env[contract.EnvL1Endpoint] == "" || execution.Env[contract.EnvL3Endpoint] == "" {
		return nil, nil
	}
	return newWorkflowBridge(ctx, a.fabric, a.controlPlaneAddr, a.runLedgerAddr)
}

func newWorkflowBridge(ctx context.Context, participant fabric.Fabric, l1Address, l3Address string) (*workflowBridge, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	bridge := &workflowBridge{
		listener: listener,
		l1:       workflowBridgeTransport(participant, l1Address),
		l3:       workflowBridgeTransport(participant, l3Address),
	}
	l1Proxy := workflowReverseProxy(bridge.l1, "/l1")
	l3Proxy := workflowReverseProxy(bridge.l3, "/l3")
	mux := http.NewServeMux()
	mux.Handle("/l3/", l3Proxy)
	mux.HandleFunc("/l1/", func(w http.ResponseWriter, r *http.Request) {
		if !allowedWorkflowL1Read(r) {
			writeWorkflowBridgeError(w, http.StatusForbidden, contract.ErrorForbidden, "workflow bridge exposes only L1 job status and log reads")
			return
		}
		l1Proxy.ServeHTTP(w, r)
	})
	bridge.server = &http.Server{Handler: mux, BaseContext: func(net.Listener) context.Context { return ctx }}
	baseURL := "http://" + listener.Addr().String()
	bridge.l1Endpoint = baseURL + "/l1"
	bridge.l3Endpoint = baseURL + "/l3"
	go func() {
		_ = bridge.server.Serve(listener)
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

func allowedWorkflowL1Read(request *http.Request) bool {
	if request.Method != http.MethodGet {
		return false
	}
	path := strings.TrimPrefix(request.URL.Path, "/l1")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 3 && parts[0] == "v1" && parts[1] == "jobs" && parts[2] != "" {
		return true
	}
	return len(parts) == 4 && parts[0] == "v1" && parts[1] == "jobs" && parts[2] != "" && parts[3] == "logs"
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
	b.l1.CloseIdleConnections()
	b.l3.CloseIdleConnections()
	if errors.Is(err, context.DeadlineExceeded) {
		_ = b.listener.Close()
	}
	return err
}
