package takeover

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type ActionError struct {
	APIError contract.APIError
}

func (failure *ActionError) Error() string {
	if failure == nil {
		return "Computer take-over action failed"
	}
	return fmt.Sprintf("%s: %s", failure.APIError.Code, failure.APIError.Message)
}

func Perform(ctx context.Context, participant fabric.Fabric, endpoint, token, action string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "ws" || parsed.Host == "" || parsed.Path != contract.ComputerDisplayWebSocketPath ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("Computer display endpoint is not the fixed private front door")
	}
	path := contract.ComputerControlTakePath
	if action == "release" {
		path = contract.ComputerControlReleasePath
	} else if action != "take" {
		return fmt.Errorf("unknown Computer take-over action %q", action)
	}
	transport := &http.Transport{DialContext: func(dialContext context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(dialContext, network, parsed.Host)
	}}
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://computer.invalid"+path, nil)
	if err != nil {
		return fmt.Errorf("create Computer %s request: %w", action, err)
	}
	request.Header.Set(contract.ComputerControlTokenHeader, token)
	response, err := (&http.Client{Transport: transport}).Do(request)
	if err != nil {
		return fmt.Errorf("perform Computer %s: %w", action, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return fmt.Errorf("read Computer %s response: %w", action, err)
	}
	if response.StatusCode == http.StatusNoContent {
		return nil
	}
	message := strings.TrimSpace(string(body))
	code := contract.ErrorTenureUnavailable
	switch response.StatusCode {
	case http.StatusUnauthorized:
		code = contract.ErrorUnauthorized
	case http.StatusForbidden:
		code = contract.ErrorControlNotAuthorized
	case http.StatusConflict:
		if message == string(contract.ErrorControllerAlreadyHeld) {
			code = contract.ErrorControllerAlreadyHeld
		} else {
			code = contract.ErrorControllerBusy
		}
	case http.StatusGone:
		code = contract.ErrorTakeoverSessionEnded
	}
	if message == "" {
		message = fmt.Sprintf("Computer %s returned HTTP %d", action, response.StatusCode)
	}
	return &ActionError{APIError: contract.APIError{Code: code, Message: message,
		Retryable: code == contract.ErrorControllerBusy || code == contract.ErrorTenureUnavailable}}
}
