package main

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestRegistryResolverUsesAuthenticatedManifestHEAD(t *testing.T) {
	digest := "sha256:" + strings.Repeat("d", 64)
	var methods, paths, authorizations []string
	resolver := newRegistryResolver(&http.Client{Transport: imageRoundTripFunc(func(request *http.Request) (*http.Response, error) {
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
