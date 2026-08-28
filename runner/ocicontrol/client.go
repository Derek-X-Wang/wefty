package ocicontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

type Client struct {
	socket string
	http   *http.Client
}

type ResponseError struct {
	Status int
	contract.APIError
}

func (err *ResponseError) Error() string { return err.Message }

func NewClient(socket string) (*Client, error) {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) == string(filepath.Separator) {
		return nil, errors.New("OCI control socket path must be absolute and non-root")
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	return &Client{socket: socket, http: &http.Client{Transport: transport}}, nil
}

func (client *Client) Close() { client.http.CloseIdleConnections() }

func (client *Client) Intent(ctx context.Context) (lima.OCIIntent, error) {
	var response lima.OCIIntent
	err := client.call(ctx, http.MethodGet, "/v1/intent", nil, "", &response)
	return response, err
}

func (client *Client) Setup(ctx context.Context, request SetupRequest) (SetupResponse, error) {
	var response SetupResponse
	err := client.callJSON(ctx, "/v1/setup", request, &response)
	return response, err
}

func (client *Client) Start(ctx context.Context, expected uint64) (IntentResponse, error) {
	var response IntentResponse
	err := client.callJSON(ctx, "/v1/oci/start", IntentMutationRequest{ExpectedRevision: expected}, &response)
	return response, err
}

func (client *Client) Stop(ctx context.Context, expected uint64) (IntentResponse, error) {
	var response IntentResponse
	err := client.callJSON(ctx, "/v1/oci/stop", IntentMutationRequest{ExpectedRevision: expected}, &response)
	return response, err
}

func (client *Client) LoadImage(ctx context.Context, path string) (LoadImageResponse, error) {
	if !filepath.IsAbs(path) {
		return LoadImageResponse{}, errors.New("OCI image archive path must be absolute")
	}
	file, err := os.Open(path)
	if err != nil {
		return LoadImageResponse{}, fmt.Errorf("open OCI image archive: %w", err)
	}
	defer file.Close()
	var response LoadImageResponse
	err = client.call(ctx, http.MethodPost, "/v1/images/load", file, "application/vnd.oci.image.layer.v1.tar", &response)
	return response, err
}

func (client *Client) callJSON(ctx context.Context, path string, request, response any) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	return client.call(ctx, http.MethodPost, path, bytes.NewReader(payload), "application/json", response)
}

func (client *Client) call(ctx context.Context, method, path string, body io.Reader, contentType string, response any) error {
	request, err := http.NewRequestWithContext(ctx, method, "http://wefty.local"+path, body)
	if err != nil {
		return err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	result, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("call node-local OCI control socket: %w", err)
	}
	defer result.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(result.Body, maximumControlJSONBytes))
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var failure contract.ErrorResponse
		if err := decoder.Decode(&failure); err != nil {
			return fmt.Errorf("node-local OCI control returned HTTP %d", result.StatusCode)
		}
		return &ResponseError{Status: result.StatusCode, APIError: failure.Error}
	}
	if err := decoder.Decode(response); err != nil {
		return fmt.Errorf("decode node-local OCI control response: %w", err)
	}
	return nil
}
