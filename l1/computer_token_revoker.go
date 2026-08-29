package l1

import (
	"bytes"
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

func (c *ComputerTokenRevocationClient) RevokeComputerTokens(ctx context.Context, revocation ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
	payload, err := json.Marshal(struct {
		ComputerID           string `json:"computer_id"`
		SubmitIntentRevision int64  `json:"submit_intent_revision"`
		RevokeAll            bool   `json:"revoke_all,omitempty"`
		Reason               string `json:"reason"`
	}{revocation.ComputerID, revocation.NewSubmitIntentRevision, revocation.RevokeAll, revocation.Reason})
	if err != nil {
		return contract.ComputerTokenRevocationReceipt{}, fmt.Errorf("l1: encode Computer token revocation: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://run-ledger.invalid/v1/computer-token/revoke", bytes.NewReader(payload))
	if err != nil {
		return contract.ComputerTokenRevocationReceipt{}, fmt.Errorf("l1: build Computer token revocation: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return contract.ComputerTokenRevocationReceipt{}, fmt.Errorf("l1: revoke Computer tokens: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		var receipt contract.ComputerTokenRevocationReceipt
		if err := json.NewDecoder(io.LimitReader(response.Body, MaxRequestBodyBytes)).Decode(&receipt); err != nil {
			return contract.ComputerTokenRevocationReceipt{}, fmt.Errorf("l1: decode Computer token revocation receipt: %w", err)
		}
		return receipt, nil
	}
	var responseError contract.ErrorResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, MaxRequestBodyBytes)).Decode(&responseError); err != nil {
		return contract.ComputerTokenRevocationReceipt{}, fmt.Errorf("l1: Computer token revocation returned HTTP %d", response.StatusCode)
	}
	return contract.ComputerTokenRevocationReceipt{}, fmt.Errorf("l1: Computer token revocation rejected: %s: %s", responseError.Error.Code, responseError.Error.Message)
}

func (c *ComputerTokenRevocationClient) CountComputerInflight(ctx context.Context, computerID string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://run-ledger.invalid/v1/computers/"+url.PathEscape(computerID)+"/inflight", nil)
	if err != nil {
		return 0, fmt.Errorf("l1: build Computer inflight request: %w", err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("l1: read Computer inflight state: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		var state struct {
			ComputerID              string `json:"computer_id"`
			NonterminalRootLineages int    `json:"nonterminal_root_lineages"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, MaxRequestBodyBytes)).Decode(&state); err != nil {
			return 0, fmt.Errorf("l1: decode Computer inflight state: %w", err)
		}
		if state.ComputerID != computerID || state.NonterminalRootLineages < 0 {
			return 0, fmt.Errorf("l1: Computer inflight state did not match request")
		}
		return state.NonterminalRootLineages, nil
	}
	var responseError contract.ErrorResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, MaxRequestBodyBytes)).Decode(&responseError); err != nil {
		return 0, fmt.Errorf("l1: Computer inflight request returned HTTP %d", response.StatusCode)
	}
	return 0, fmt.Errorf("l1: Computer inflight request rejected: %s: %s", responseError.Error.Code, responseError.Error.Message)
}

var _ ComputerTokenRevoker = (*ComputerTokenRevocationClient)(nil)
