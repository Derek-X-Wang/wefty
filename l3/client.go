package l3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// JobClient is the only L1 dependency of the ledger reconciler.
type JobClient interface {
	SubmitJob(context.Context, contract.JobSpec) (l1.Job, error)
	GetJob(context.Context, string) (l1.Job, error)
}

// JobLogClient is the public L1 log-polling dependency of the L3 API.
type JobLogClient interface {
	GetJobLogs(context.Context, string, string, int) (l1.LogPage, error)
}

// L1Client calls the L1 client protocol exclusively through Fabric.Dial.
type L1Client struct {
	client *http.Client
}

func NewL1Client(f fabric.Fabric, address string) (*L1Client, error) {
	if f == nil {
		return nil, fmt.Errorf("l3: fabric is required for L1 client")
	}
	if strings.TrimSpace(address) == "" {
		address = DefaultL1Address
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return f.Dial(ctx, network, address)
	}}
	return &L1Client{client: &http.Client{Transport: transport}}, nil
}

func (c *L1Client) CloseIdleConnections() { c.client.CloseIdleConnections() }

func (c *L1Client) SubmitJob(ctx context.Context, spec contract.JobSpec) (l1.Job, error) {
	var job l1.Job
	if err := c.do(ctx, http.MethodPost, "/v1/jobs", spec, &job, http.StatusCreated, http.StatusOK); err != nil {
		return l1.Job{}, err
	}
	return job, nil
}

func (c *L1Client) GetJob(ctx context.Context, jobID string) (l1.Job, error) {
	var job l1.Job
	if err := c.do(ctx, http.MethodGet, "/v1/jobs/"+jobID, nil, &job, http.StatusOK); err != nil {
		return l1.Job{}, err
	}
	return job, nil
}

func (c *L1Client) GetJobLogs(ctx context.Context, jobID, cursor string, limit int) (l1.LogPage, error) {
	path := "/v1/jobs/" + jobID + "/logs?limit=" + strconv.Itoa(limit)
	if cursor != "" {
		path += "&cursor=" + url.QueryEscape(cursor)
	}
	var page l1.LogPage
	if err := c.do(ctx, http.MethodGet, path, nil, &page, http.StatusOK); err != nil {
		return l1.LogPage{}, err
	}
	return page, nil
}

func (c *L1Client) do(ctx context.Context, method, path string, body any, target any, success ...int) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return internalError(err, "encode L1 request")
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://control-plane.invalid"+path, reader)
	if err != nil {
		return internalError(err, "create L1 request")
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.client.Do(request)
	if err != nil {
		return &Error{Code: contract.ErrorInternal, Message: "call L1 control plane", Retryable: true, Cause: err}
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return &Error{Code: contract.ErrorInternal, Message: "read L1 response", Retryable: true, Cause: err}
	}
	for _, status := range success {
		if response.StatusCode == status {
			if err := json.Unmarshal(responseBody, target); err != nil {
				return internalError(err, "decode L1 response")
			}
			return nil
		}
	}
	var responseError contract.ErrorResponse
	if err := json.Unmarshal(responseBody, &responseError); err != nil || responseError.Error.Code == "" {
		return &Error{Code: contract.ErrorInternal, Message: fmt.Sprintf("L1 returned HTTP %d", response.StatusCode), Retryable: response.StatusCode >= 500}
	}
	return &Error{
		Code: responseError.Error.Code, Message: responseError.Error.Message,
		Retryable: responseError.Error.Retryable,
	}
}

var _ JobClient = (*L1Client)(nil)
var _ JobLogClient = (*L1Client)(nil)
