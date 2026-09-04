package takeover

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/coder/websocket"
)

type ActionError struct {
	APIError contract.APIError
	Receipt  *contract.ComputerControlReceipt
}

func (failure *ActionError) Error() string {
	if failure == nil {
		return "Computer take-over action failed"
	}
	return fmt.Sprintf("%s: %s", failure.APIError.Code, failure.APIError.Message)
}

type Session struct {
	Endpoint    string
	ConnectHost string
	Token       string
	conn        *websocket.Conn
}

func Open(ctx context.Context, participant fabric.Fabric, endpoint string) (*Session, error) {
	return OpenAtPolicyRevision(ctx, participant, endpoint, 0)
}

func OpenAtPolicyRevision(ctx context.Context, participant fabric.Fabric, endpoint string, policyRevision int64) (*Session, error) {
	parsed, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	transport := transportFor(participant, parsed.Host)
	header := http.Header{}
	if policyRevision > 0 {
		header.Set(contract.ComputerPolicyRevisionHeader, fmt.Sprint(policyRevision))
	}
	connection, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient:   &http.Client{Transport: transport},
		HTTPHeader:   header,
		Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol},
	})
	if err != nil {
		transport.CloseIdleConnections()
		if response != nil {
			defer response.Body.Close()
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
			var failure contract.ComputerControlErrorResponse
			if readErr == nil && json.Unmarshal(body, &failure) == nil && failure.Error.Code != "" {
				failure.Error.Message = fmt.Sprintf("%s (HTTP %d)", failure.Error.Message, response.StatusCode)
				return nil, &ActionError{APIError: failure.Error, Receipt: failure.Receipt}
			}
		}
		return nil, fmt.Errorf("open Computer view session: %w", err)
	}
	token := response.Header.Get(contract.ComputerControlTokenHeader)
	if token == "" || token != strings.TrimSpace(token) {
		_ = connection.CloseNow()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("Computer view session omitted its session capability")
	}
	_, banner, err := connection.Read(ctx)
	if err != nil {
		_ = connection.CloseNow()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("read Computer view admission banner: %w", err)
	}
	if len(banner) != contract.ComputerRFBVersionBannerBytes || !contract.ValidComputerRFBVersionBanner(banner) {
		_ = connection.CloseNow()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("Computer view admission banner is invalid")
	}
	transport.CloseIdleConnections()
	return &Session{Endpoint: endpoint, ConnectHost: parsed.Hostname(), Token: token, conn: connection}, nil
}

func (session *Session) Wait(ctx context.Context) error {
	if session == nil || session.conn == nil {
		return fmt.Errorf("Computer view session is not open")
	}
	for {
		if _, _, err := session.conn.Read(ctx); err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return fmt.Errorf("Computer view session ended: %w", err)
		}
	}
}

func (session *Session) Close() error {
	if session == nil || session.conn == nil {
		return nil
	}
	return session.conn.CloseNow()
}

func Perform(
	ctx context.Context,
	participant fabric.Fabric,
	endpoint, token, action string,
) (contract.ComputerControlReceipt, error) {
	parsed, err := parseEndpoint(endpoint)
	if err != nil {
		return contract.ComputerControlReceipt{}, err
	}
	path := contract.ComputerControlTakePath
	if action == "release" {
		path = contract.ComputerControlReleasePath
	} else if action != "take" {
		return contract.ComputerControlReceipt{}, fmt.Errorf("unknown Computer take-over action %q", action)
	}
	transport := transportFor(participant, parsed.Host)
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://computer.invalid"+path, nil)
	if err != nil {
		return contract.ComputerControlReceipt{}, fmt.Errorf("create Computer %s request: %w", action, err)
	}
	request.Header.Set(contract.ComputerControlTokenHeader, token)
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return contract.ComputerControlReceipt{}, fmt.Errorf("perform Computer %s: %w", action, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return contract.ComputerControlReceipt{}, fmt.Errorf("read Computer %s response: %w", action, err)
	}
	if response.StatusCode == http.StatusOK {
		var receipt contract.ComputerControlReceipt
		if err := json.Unmarshal(body, &receipt); err != nil {
			return contract.ComputerControlReceipt{}, fmt.Errorf("decode Computer %s receipt: %w", action, err)
		}
		return receipt, nil
	}
	var failure contract.ComputerControlErrorResponse
	if err := json.Unmarshal(body, &failure); err != nil || failure.Error.Code == "" {
		failure.Error = contract.APIError{Code: contract.ErrorInternal,
			Message: fmt.Sprintf("Computer %s returned HTTP %d", action, response.StatusCode)}
	}
	failure.Error.Retryable = false
	switch response.StatusCode {
	case http.StatusUnauthorized:
		failure.Error.Code = contract.ErrorUnauthorized
	case http.StatusForbidden:
		failure.Error.Code = contract.ErrorControlNotAuthorized
	case http.StatusConflict:
		if failure.Error.Code != contract.ErrorControllerAlreadyHeld {
			failure.Error.Code = contract.ErrorControllerBusy
		}
		failure.Error.Retryable = failure.Error.Code == contract.ErrorControllerBusy
	case http.StatusGone:
		failure.Error.Code = contract.ErrorTakeoverSessionEnded
	default:
		failure.Error = contract.APIError{Code: contract.ErrorInternal,
			Message: fmt.Sprintf("Computer %s returned HTTP %d", action, response.StatusCode), Retryable: false}
	}
	return receiptOrZero(failure.Receipt), &ActionError{APIError: failure.Error, Receipt: failure.Receipt}
}

func parseEndpoint(endpoint string) (*url.URL, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "ws" || parsed.Host == "" || parsed.Path != contract.ComputerDisplayWebSocketPath ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("Computer display endpoint is not the fixed private front door")
	}
	return parsed, nil
}

func transportFor(participant fabric.Fabric, address string) *http.Transport {
	return &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}
}

func receiptOrZero(receipt *contract.ComputerControlReceipt) contract.ComputerControlReceipt {
	if receipt == nil {
		return contract.ComputerControlReceipt{}
	}
	return *receipt
}
