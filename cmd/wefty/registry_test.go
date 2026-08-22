package main

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRegistryResolverUsesAuthenticatedManifestHEAD(t *testing.T) {
	digest := "sha256:" + strings.Repeat("d", 64)
	var methods, paths, authorizations []string
	resolver := newRegistryResolver(&http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodHead && request.Header.Get("Accept") != registryManifestAccept {
			t.Fatalf("manifest Accept = %q, want %q", request.Header.Get("Accept"), registryManifestAccept)
		}
		methods = append(methods, request.Method)
		paths = append(paths, request.URL.Path)
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		switch {
		case request.URL.Host == "ghcr.io" && request.Header.Get("Authorization") == "":
			response := jsonResponse(http.StatusUnauthorized, map[string]any{})
			response.Header.Set("WWW-Authenticate", `Bearer realm="https://auth.example.test/token",service="ghcr.io",scope="repository:example/service:pull"`)
			return response, nil
		case request.URL.Host == "auth.example.test":
			return jsonResponse(http.StatusOK, map[string]string{"token": "public-token"}), nil
		case request.URL.Host == "ghcr.io" && request.Header.Get("Authorization") == "Bearer public-token":
			response := jsonResponse(http.StatusOK, map[string]any{})
			response.Header.Set("Docker-Content-Digest", digest)
			return response, nil
		default:
			t.Fatalf("unexpected registry request: %s %s auth=%q", request.Method, request.URL, request.Header.Get("Authorization"))
			return nil, nil
		}
	})})

	got, err := resolver.ResolveDigest(context.Background(), "ghcr.io/example/service:stable")
	if err != nil {
		t.Fatal(err)
	}
	if got != digest {
		t.Fatalf("resolved digest = %q, want %q", got, digest)
	}
	if strings.Join(methods, ",") != "HEAD,GET,HEAD" ||
		strings.Join(paths, ",") != "/v2/example/service/manifests/stable,/token,/v2/example/service/manifests/stable" ||
		strings.Join(authorizations, ",") != ",,Bearer public-token" {
		t.Fatalf("registry exchange methods=%v paths=%v auth=%v", methods, paths, authorizations)
	}
}

func TestRegistryLocationParsesDockerReferences(t *testing.T) {
	for _, test := range []struct {
		reference  string
		registry   string
		repository string
		tag        string
	}{
		{"nginx", "registry-1.docker.io", "library/nginx", "latest"},
		{"nginx:1.25", "registry-1.docker.io", "library/nginx", "1.25"},
		{"user/repo:v1", "registry-1.docker.io", "user/repo", "v1"},
		{"ghcr.io/a/b:c", "ghcr.io", "a/b", "c"},
		{"localhost:5000/x", "localhost:5000", "x", "latest"},
		{"docker.io/library/alpine", "registry-1.docker.io", "library/alpine", "latest"},
		{"index.docker.io/library/alpine", "registry-1.docker.io", "library/alpine", "latest"},
	} {
		t.Run(test.reference, func(t *testing.T) {
			registry, repository, tag, err := registryLocation(test.reference)
			if err != nil {
				t.Fatal(err)
			}
			if registry != test.registry || repository != test.repository || tag != test.tag {
				t.Fatalf("registry location = %q/%q:%q, want %q/%q:%q", registry, repository, tag, test.registry, test.repository, test.tag)
			}
		})
	}
}

func TestServiceRegistryResolutionFailuresNeverSubmitJob(t *testing.T) {
	for name, registryClient := range map[string]*http.Client{
		"non-2xx HEAD": {Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			assertManifestAccept(t, request)
			return jsonResponse(http.StatusBadGateway, map[string]any{}), nil
		})},
		"missing digest": {Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			assertManifestAccept(t, request)
			return jsonResponse(http.StatusOK, map[string]any{}), nil
		})},
		"non-Bearer challenge": {Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			assertManifestAccept(t, request)
			response := jsonResponse(http.StatusUnauthorized, map[string]any{})
			response.Header.Set("WWW-Authenticate", `Basic realm="registry"`)
			return response, nil
		})},
		"malformed realm": {Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			assertManifestAccept(t, request)
			response := jsonResponse(http.StatusUnauthorized, map[string]any{})
			response.Header.Set("WWW-Authenticate", `Bearer realm="http://auth.example.test/token"`)
			return response, nil
		})},
		"timeout": {
			Timeout: 5 * time.Millisecond,
			Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				assertManifestAccept(t, request)
				<-request.Context().Done()
				return nil, request.Context().Err()
			}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			posts := 0
			clients := &apiClients{
				images: newRegistryResolver(registryClient),
				l1: &apiClient{name: "L1", client: &http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
						posts++
					}
					return jsonResponse(http.StatusCreated, map[string]any{}), nil
				})}},
			}
			var stdout, stderr bytes.Buffer
			err := executeServiceCreate(context.Background(), clients, true, []string{"--image", "ghcr.io/example/service:moving"}, &stdout, &stderr)
			if err == nil {
				t.Fatal("registry failure unexpectedly succeeded")
			}
			if posts != 0 {
				t.Fatalf("L1 job POST count = %d, want 0", posts)
			}
		})
	}
}

func assertManifestAccept(t *testing.T, request *http.Request) {
	t.Helper()
	if request.Method != http.MethodHead {
		t.Fatalf("registry method = %s, want HEAD", request.Method)
	}
	if request.Header.Get("Accept") != registryManifestAccept {
		t.Fatalf("manifest Accept = %q, want %q", request.Header.Get("Accept"), registryManifestAccept)
	}
}
