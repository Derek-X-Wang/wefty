package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l3"
)

type ComputerTokenMinter interface {
	MintComputerToken(context.Context, l3.ComputerTokenMintRequest) (l3.ComputerTokenGrant, error)
}

type ComputerTokenRevoker interface {
	RevokeComputerAttemptTokens(context.Context, l3.ComputerAttemptTokenRevocationRequest) error
	RevokeHostComputerTokens(context.Context, l3.HostComputerTokenRevocationRequest) error
}

type computerTokenClient struct {
	httpClient *http.Client
	transport  *http.Transport
}

func newComputerTokenClient(f fabric.Fabric, address string, timeout time.Duration) (*computerTokenClient, error) {
	if f == nil || strings.TrimSpace(address) == "" {
		return nil, errors.New("agent: Fabric and run-ledger address are required for Computer tokens")
	}
	transport := &http.Transport{MaxIdleConnsPerHost: DefaultMaxIdleConnsPerHost,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return f.Dial(ctx, network, address)
		}}
	return &computerTokenClient{httpClient: &http.Client{Transport: transport, Timeout: timeout}, transport: transport}, nil
}

func (c *computerTokenClient) Close() { c.transport.CloseIdleConnections() }

func (c *computerTokenClient) MintComputerToken(ctx context.Context, requestBody l3.ComputerTokenMintRequest) (l3.ComputerTokenGrant, error) {
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return l3.ComputerTokenGrant{}, fmt.Errorf("agent: encode Computer token mint: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://run-ledger.invalid/v1/computer-token/mint", bytes.NewReader(payload))
	if err != nil {
		return l3.ComputerTokenGrant{}, fmt.Errorf("agent: build Computer token mint: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return l3.ComputerTokenGrant{}, fmt.Errorf("agent: mint Computer token: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var responseError contract.ErrorResponse
		if err := json.NewDecoder(io.LimitReader(response.Body, computerTokenResponseLimit)).Decode(&responseError); err != nil {
			return l3.ComputerTokenGrant{}, fmt.Errorf("agent: Computer token mint returned HTTP %d", response.StatusCode)
		}
		return l3.ComputerTokenGrant{}, fmt.Errorf("agent: Computer token mint rejected: %s: %s", responseError.Error.Code, responseError.Error.Message)
	}
	var grant l3.ComputerTokenGrant
	if err := json.NewDecoder(io.LimitReader(response.Body, computerTokenResponseLimit)).Decode(&grant); err != nil {
		return l3.ComputerTokenGrant{}, fmt.Errorf("agent: decode Computer token grant: %w", err)
	}
	return grant, nil
}

func (c *computerTokenClient) RevokeComputerAttemptTokens(ctx context.Context, request l3.ComputerAttemptTokenRevocationRequest) error {
	return c.revoke(ctx, "/v1/computer-token/revoke-attempt", request)
}

func (c *computerTokenClient) RevokeHostComputerTokens(ctx context.Context, request l3.HostComputerTokenRevocationRequest) error {
	return c.revoke(ctx, "/v1/computer-token/revoke-host", request)
}

func (c *computerTokenClient) revoke(ctx context.Context, path string, requestBody any) error {
	payload, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("agent: encode Computer token revocation: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://run-ledger.invalid"+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("agent: build Computer token revocation: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("agent: revoke Computer tokens: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	var responseError contract.ErrorResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, computerTokenResponseLimit)).Decode(&responseError); err != nil {
		return fmt.Errorf("agent: Computer token revocation returned HTTP %d", response.StatusCode)
	}
	return fmt.Errorf("agent: Computer token revocation rejected: %s: %s", responseError.Error.Code, responseError.Error.Message)
}

var _ ComputerTokenRevoker = (*computerTokenClient)(nil)

const computerTokenResponseLimit = 1 << 20
