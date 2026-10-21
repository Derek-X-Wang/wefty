package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
)

const (
	peerProbeUnavailable    = "peer probe unavailable on this version"
	doctorPeerBudgetExpired = "doctor time budget expired"
)

// Fleet callers use only the already verified running client service. This
// request never opens another participant or falls back to a direct backend.
func readFleetPeerHealth(ctx context.Context, client *apiClient, name string) (setupReadinessCheck, error) {
	check := setupReadinessCheck{Check: name, Status: "FAILED", Detail: "peer observation could not be read through the client service"}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://wefty.invalid/v1/peer-health", nil)
	if err != nil {
		return check, err
	}
	if client == nil || client.client == nil {
		return check, &unavailableError{cause: errors.New("peer observer service unavailable")}
	}
	// No redirects: an older server's route absence is evidence only from that
	// exact service, and diagnostic reads must never follow another target.
	httpClient := *client.client
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := httpClient.Do(request)
	if err != nil {
		return check, classifyFailedExchange(ctx, httpexchange.Request, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	// A known HTTP identity refusal stays a refusal even if its body is reset
	// or too large. Retain a valid bounded authority envelope when available.
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		code := contract.ErrorUnauthorized
		if response.StatusCode == http.StatusForbidden {
			code = contract.ErrorForbidden
		}
		refusal := &apiResponseError{Service: client.name, StatusCode: response.StatusCode, APIError: contract.APIError{Code: code, Message: "peer observer service refused the request"}}
		var envelope contract.ErrorResponse
		if err == nil && len(body) <= 4096 && json.Unmarshal(body, &envelope) == nil && envelope.Error.Code != "" {
			answer := &apiResponseError{Service: client.name, StatusCode: response.StatusCode, APIError: envelope.Error}
			if classifyCommandReport(answer).exitCode == exitUnauthorized {
				refusal = answer
			}
		}
		return check, refusal
	}
	if err != nil {
		return check, classifyFailedExchange(ctx, httpexchange.Body, err)
	}
	if response.StatusCode == http.StatusNotFound {
		check.Status, check.Detail = "NOT-RUN", peerProbeUnavailable
		return check, nil
	}
	if len(body) > 4096 {
		if response.StatusCode == http.StatusOK {
			check.Detail = "peer observer returned an invalid result"
			return check, nil
		}
		return check, errors.New("peer observation exceeds size limit")
	}
	if response.StatusCode != http.StatusOK {
		var envelope contract.ErrorResponse
		if json.Unmarshal(body, &envelope) == nil && envelope.Error.Code != "" {
			return check, &apiResponseError{Service: client.name, StatusCode: response.StatusCode, APIError: envelope.Error}
		}
		code := contract.ErrorUnavailable
		return check, &apiResponseError{Service: client.name, StatusCode: response.StatusCode, APIError: contract.APIError{Code: code, Message: "peer observer service did not answer", Retryable: code == contract.ErrorUnavailable}}
	}
	var result struct {
		Reachable  *bool   `json:"reachable"`
		ErrorClass *string `json:"error_class"`
	}
	var observation contract.PeerHealth
	if json.Unmarshal(body, &result) != nil || result.Reachable == nil || result.ErrorClass == nil || json.Unmarshal(body, &observation) != nil || observation.CheckedAt.IsZero() {
		check.Detail = "peer observer returned an invalid result"
		return check, nil
	}
	if observation.Reachable && observation.ErrorClass == "" {
		check.Status, check.Detail = "VERIFIED", "service reached and authenticated its configured peer"
		return check, nil
	}
	if !observation.Reachable {
		switch observation.ErrorClass {
		case "not_found":
			check.Status, check.Detail = "NOT-RUN", peerProbeUnavailable
			return check, nil
		case "unavailable", "forbidden", "internal":
			check.Detail = "service could not confirm its configured peer (" + observation.ErrorClass + ")"
			return check, nil
		}
	}
	check.Detail = "peer observer returned an invalid result"
	return check, nil
}

// Doctor distinguishes a failed observation from an observer it could not
// read. Readiness retains its existing FAILED result for an unreadable row.
func readDoctorPeerHealth(budget context.Context, client *apiClient, name string) (setupReadinessCheck, error) {
	if errors.Is(budget.Err(), context.DeadlineExceeded) {
		return setupReadinessCheck{Check: name, Status: "NOT-RUN", Detail: doctorPeerBudgetExpired}, budget.Err()
	}
	probe, cancel := context.WithTimeout(budget, statusProbeBudget)
	defer cancel()
	check, err := readFleetPeerHealth(probe, client, name)
	if err == nil {
		return check, nil
	}
	check.Status, check.Detail = "NOT-RUN", "peer observer could not be read; direction unverified"
	var response *apiResponseError
	if classifyCommandReport(err).exitCode == exitUnauthorized || errors.As(err, &response) && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		check.Status, check.Detail = "REFUSED", "the request was refused; this does not prove a network-policy denial"
	} else if errors.Is(budget.Err(), context.DeadlineExceeded) {
		check.Detail = doctorPeerBudgetExpired
	}
	return check, err
}
