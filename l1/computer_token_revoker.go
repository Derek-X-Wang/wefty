package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const DefaultRunLedgerAddress = "wefty://run-ledger"

type ComputerTokenRevocationClient struct {
	httpClient *http.Client
	transport  *http.Transport
}

func NewComputerTokenRevocationClient(f fabric.Fabric, address string) (*ComputerTokenRevocationClient, error) {
	if f == nil || strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("l1: Fabric and run-ledger address are required")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return f.Dial(ctx, network, address)
	}}
	return &ComputerTokenRevocationClient{httpClient: &http.Client{Transport: transport, Timeout: ComputerPolicyClientTimeout}, transport: transport}, nil
}

func (c *ComputerTokenRevocationClient) CloseIdleConnections() { c.transport.CloseIdleConnections() }

func (c *ComputerTokenRevocationClient) RevokeComputerTokens(ctx context.Context, revocation ComputerTokenRevocation) error {
	payload, err := json.Marshal(struct {
		ComputerID           string `json:"computer_id"`
		SubmitIntentRevision int64  `json:"submit_intent_revision"`
		RevokeAll            bool   `json:"revoke_all,omitempty"`
		Reason               string `json:"reason"`
	}{revocation.ComputerID, revocation.NewSubmitIntentRevision, revocation.RevokeAll, revocation.Reason})
	if err != nil {
		return fmt.Errorf("l1: encode Computer token revocation: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://run-ledger.invalid/v1/computer-token/revoke", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("l1: build Computer token revocation: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("l1: revoke Computer tokens: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	var responseError contract.ErrorResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, MaxRequestBodyBytes)).Decode(&responseError); err != nil {
		return fmt.Errorf("l1: Computer token revocation returned HTTP %d", response.StatusCode)
	}
	return fmt.Errorf("l1: Computer token revocation rejected: %s: %s", responseError.Error.Code, responseError.Error.Message)
}

var _ ComputerTokenRevoker = (*ComputerTokenRevocationClient)(nil)
