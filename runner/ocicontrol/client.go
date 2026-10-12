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
	"net/url"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
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

func (client *Client) Doctor(ctx context.Context) (DoctorResponse, error) {
	var response DoctorResponse
	err := client.call(ctx, http.MethodGet, "/v1/doctor", nil, "", &response)
	return response, err
}

// Removals reads the node-local agent's durable runtime removal records. It
// mutates nothing; a removal leaves this surface when its cleanup is
// acknowledged.
func (client *Client) Removals(ctx context.Context) (RemovalsResponse, error) {
	var response RemovalsResponse
	err := client.call(ctx, http.MethodGet, "/v1/oci/removals", nil, "", &response)
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

func (client *Client) LoadImage(ctx context.Context, path string, request LoadImageRequest) (LoadImageResponse, error) {
	if !filepath.IsAbs(path) {
		return LoadImageResponse{}, errors.New("OCI image archive path must be absolute")
	}
	file, err := os.Open(path)
	if err != nil {
		return LoadImageResponse{}, fmt.Errorf("open OCI image archive: %w", err)
	}
	defer file.Close()
	target := "/v1/images/load"
	if request.Reference != "" {
		target += "?" + url.Values{"reference": []string{request.Reference}}.Encode()
	}
	var response LoadImageResponse
	err = client.call(ctx, http.MethodPost, target, file, "application/vnd.oci.image.layer.v1.tar", &response)
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
		return &httpexchange.Failure{Phase: httpexchange.Request, Network: "unix", Cause: fmt.Errorf("call node-local OCI control socket: %w", err)}
	}
	defer result.Body.Close()
	// Finish the bounded read before decoding: a valid JSON prefix does not
	// prove that the response body arrived completely.
	payload, err := io.ReadAll(io.LimitReader(result.Body, maximumControlJSONBytes+1))
	if err != nil {
		return &httpexchange.Failure{Phase: httpexchange.Body, Cause: fmt.Errorf("read node-local OCI control response: %w", err)}
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		var failure contract.ErrorResponse
		if err := json.Unmarshal(payload, &failure); err == nil && len(payload) <= maximumControlJSONBytes && failure.Error.Code != "" {
			return &ResponseError{Status: result.StatusCode, APIError: failure.Error}
		}
		if result.StatusCode >= 500 && result.StatusCode < 600 {
			return &httpexchange.Failure{Phase: httpexchange.BareStatus, Cause: errors.New(httpexchange.ResponseMessage("node-local OCI control", result.StatusCode, payload))}
		}
		return &httpexchange.Failure{Phase: httpexchange.Decode, Cause: fmt.Errorf("node-local OCI control returned HTTP %d", result.StatusCode)}
	}
	if len(payload) > maximumControlJSONBytes {
		return &httpexchange.Failure{Phase: httpexchange.Decode, Cause: errors.New("node-local OCI control response exceeds JSON limit")}
	}
	if err := json.Unmarshal(payload, response); err != nil {
		return &httpexchange.Failure{Phase: httpexchange.Decode, Cause: fmt.Errorf("decode node-local OCI control response: %w", err)}
	}
	return nil
}
