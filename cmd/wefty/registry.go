package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
)

const registryManifestAccept = "application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"

type imageDigestResolver interface {
	ResolveDigest(context.Context, string) (string, error)
}

type registryResolver struct {
	client *http.Client
}

func newRegistryResolver(client *http.Client) *registryResolver {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &registryResolver{client: client}
}

func (r *registryResolver) ResolveDigest(ctx context.Context, reference string) (string, error) {
	registry, repository, tag, err := registryLocation(reference)
	if err != nil {
		return "", err
	}
	manifestURL := "https://" + registry + "/v2/" + repository + "/manifests/" + url.PathEscape(tag)
	response, err := r.headManifest(ctx, manifestURL, "")
	if err != nil {
		return "", err
	}
	if response.StatusCode == http.StatusUnauthorized {
		challenge := response.Header.Get("WWW-Authenticate")
		response.Body.Close()
		token, err := r.publicBearerToken(ctx, challenge)
		if err != nil {
			return "", err
		}
		response, err = r.headManifest(ctx, manifestURL, "Bearer "+token)
		if err != nil {
			return "", err
		}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", registryStatusError(ctx, fmt.Sprintf("registry HEAD for %q", reference), response)
	}
	digest := strings.TrimSpace(response.Header.Get("Docker-Content-Digest"))
	program := contract.ImageProgram{Reference: reference, Digest: &digest}
	if err := contract.ValidateImageProgram(program, contract.JobClassService); err != nil {
		return "", fmt.Errorf("registry HEAD for %q returned an invalid digest: %w", reference, err)
	}
	return digest, nil
}

func (r *registryResolver) headManifest(ctx context.Context, manifestURL, authorization string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, manifestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create registry HEAD request: %w", err)
	}
	request.Header.Set("Accept", registryManifestAccept)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := r.client.Do(request)
	if err != nil {
		return nil, classifyFailedExchange(ctx, httpexchange.Request, fmt.Errorf("registry HEAD: %w", err))
	}
	return response, nil
}

func (r *registryResolver) publicBearerToken(ctx context.Context, challenge string) (string, error) {
	params, err := parseBearerChallenge(challenge)
	if err != nil {
		return "", err
	}
	tokenURL, err := url.Parse(params["realm"])
	if err != nil || tokenURL.Scheme != "https" || tokenURL.Host == "" {
		return "", fmt.Errorf("registry bearer challenge has an invalid HTTPS realm")
	}
	query := tokenURL.Query()
	for _, name := range []string{"service", "scope"} {
		if params[name] != "" {
			query.Set(name, params[name])
		}
	}
	tokenURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("create registry token request: %w", err)
	}
	response, err := r.client.Do(request)
	if err != nil {
		return "", classifyFailedExchange(ctx, httpexchange.Request, fmt.Errorf("request public registry token: %w", err))
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", registryStatusError(ctx, "public registry token request", response)
	}
	var body struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", classifyFailedExchange(ctx, httpexchange.Body, fmt.Errorf("read public registry token: %w", err))
	}
	if err := json.Unmarshal(responseBody, &body); err != nil {
		return "", classifyFailedExchange(ctx, httpexchange.Decode, fmt.Errorf("decode public registry token: %w", err))
	}
	if body.Token == "" {
		body.Token = body.AccessToken
	}
	if body.Token == "" {
		return "", fmt.Errorf("public registry token response omitted a token")
	}
	return body.Token, nil
}

func parseBearerChallenge(challenge string) (map[string]string, error) {
	prefix, rest, ok := strings.Cut(strings.TrimSpace(challenge), " ")
	if !ok || !strings.EqualFold(prefix, "Bearer") {
		return nil, fmt.Errorf("registry requires unsupported authentication")
	}
	params := make(map[string]string)
	for _, item := range strings.Split(rest, ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(item), "=")
		if !ok {
			return nil, fmt.Errorf("registry bearer challenge is malformed")
		}
		value = strings.Trim(strings.TrimSpace(value), `"`)
		params[strings.ToLower(name)] = value
	}
	if params["realm"] == "" {
		return nil, fmt.Errorf("registry bearer challenge omitted realm")
	}
	return params, nil
}

func registryLocation(reference string) (registry, repository, tag string, err error) {
	parts := strings.Split(reference, "/")
	if len(parts) == 0 {
		return "", "", "", usageError("invalid image reference")
	}
	first := parts[0]
	if len(parts) > 1 && (strings.Contains(first, ".") || strings.Contains(first, ":") || first == "localhost") {
		registry = first
		repository = strings.Join(parts[1:], "/")
	} else {
		registry = "registry-1.docker.io"
		repository = reference
		if len(parts) == 1 {
			repository = "library/" + reference
		}
	}
	if registry == "docker.io" || registry == "index.docker.io" {
		registry = "registry-1.docker.io"
	}
	if repository == "" {
		return "", "", "", usageError("image reference must include a repository")
	}
	tag = "latest"
	lastSlash := strings.LastIndexByte(repository, '/')
	lastColon := strings.LastIndexByte(repository, ':')
	if lastColon > lastSlash {
		tag = repository[lastColon+1:]
		repository = repository[:lastColon]
	}
	return registry, repository, tag, nil
}

// Registry refusals retain their status semantics. Its protocol has no wefty
// error envelope; only a 5xx is an availability failure.
func registryStatusError(ctx context.Context, service string, response *http.Response) error {
	if response.StatusCode >= 500 && response.StatusCode < 600 {
		body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
		if err != nil {
			return classifyFailedExchange(ctx, httpexchange.Body, fmt.Errorf("read %s response: %w", service, err))
		}
		return classifyFailedExchange(ctx, httpexchange.Headers, fmt.Errorf("%s", httpexchange.ResponseMessage(service, response.StatusCode, body)))
	}
	return fmt.Errorf("%s returned HTTP %d", service, response.StatusCode)
}
