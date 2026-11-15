package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/Derek-X-Wang/wefty/internal/httpexchange"
	"github.com/Derek-X-Wang/wefty/l1"
)

// imageSeatLabel is the OCI image config label that declares Seat support
// (docs/contracts/computer-image.md, "Seat support detection"). Its value is
// the number of Seat places the image provides, 1 to 4.
const imageSeatLabel = "dev.wefty.computer.seat-places"

const registryBodyLimit = 4 << 20

// imageSeatLabelReader reads an image's Seat label before L1 records the
// Computer, so a by-name request can be refused before any dialog.
type imageSeatLabelReader interface {
	ImageSeatPlaces(ctx context.Context, reference, digest string) (int, error)
}

// ImageSeatPlaces returns the label's value when every Linux platform of the
// pinned image declares the same valid value, and 0 when any of them lacks it.
// It returns an error only when the registry could not be read.
func (r *registryResolver) ImageSeatPlaces(ctx context.Context, reference, digest string) (int, error) {
	registry, repository, _, err := registryLocation(reference)
	if err != nil {
		return 0, err
	}
	base := "https://" + registry + "/v2/" + repository
	session := registrySession{resolver: r}
	var top struct {
		MediaType string `json:"mediaType"`
		Config    struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Manifests []struct {
			Digest   string `json:"digest"`
			Platform struct {
				OS string `json:"os"`
			} `json:"platform"`
		} `json:"manifests"`
	}
	if err := session.getVerifiedJSON(ctx, base+"/manifests/"+digest, registryManifestAccept, digest, &top); err != nil {
		return 0, err
	}
	configs := []string{}
	if len(top.Manifests) == 0 {
		configs = append(configs, top.Config.Digest)
	}
	for _, manifest := range top.Manifests {
		if manifest.Platform.OS != "linux" {
			continue // attestation entries use os "unknown"
		}
		var platform struct {
			Config struct {
				Digest string `json:"digest"`
			} `json:"config"`
		}
		if err := session.getVerifiedJSON(ctx, base+"/manifests/"+manifest.Digest, registryManifestAccept, manifest.Digest, &platform); err != nil {
			return 0, err
		}
		configs = append(configs, platform.Config.Digest)
	}
	if len(configs) == 0 {
		return 0, nil
	}
	places := -1
	for _, config := range configs {
		var image struct {
			Config struct {
				Labels map[string]string `json:"Labels"`
			} `json:"config"`
		}
		if err := session.getVerifiedJSON(ctx, base+"/blobs/"+config, "application/vnd.oci.image.config.v1+json, application/vnd.docker.container.image.v1+json", config, &image); err != nil {
			return 0, err
		}
		value, err := strconv.Atoi(strings.TrimSpace(image.Config.Labels[imageSeatLabel]))
		if err != nil || value < 1 || value > l1.MaxComputerSeatPlaces || places >= 0 && value != places {
			return 0, nil
		}
		places = value
	}
	return places, nil
}

// registrySession reuses one anonymous pull token across the reads of one
// image, as the digest HEAD path does for one request.
type registrySession struct {
	resolver *registryResolver
	token    string
}

func (s *registrySession) getVerifiedJSON(ctx context.Context, target, accept, digest string, value any) error {
	algorithm, want, ok := strings.Cut(digest, ":")
	if !ok || algorithm != "sha256" || len(want) != 64 {
		return fmt.Errorf("registry content digest %q is not sha256", digest)
	}
	response, err := s.get(ctx, target, accept)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return registryStatusError(ctx, "registry GET "+target, response)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, registryBodyLimit+1))
	if err != nil {
		return classifyFailedExchange(ctx, httpexchange.Body, fmt.Errorf("read registry content: %w", err))
	}
	if len(body) > registryBodyLimit {
		return fmt.Errorf("registry content %s exceeds %d bytes", digest, registryBodyLimit)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != want {
		return fmt.Errorf("registry content does not match digest %s", digest)
	}
	if err := json.Unmarshal(body, value); err != nil {
		return fmt.Errorf("decode registry content %s: %w", digest, err)
	}
	return nil
}

func (s *registrySession) get(ctx context.Context, target, accept string) (*http.Response, error) {
	send := func() (*http.Response, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, fmt.Errorf("create registry GET request: %w", err)
		}
		request.Header.Set("Accept", accept)
		if s.token != "" {
			request.Header.Set("Authorization", "Bearer "+s.token)
		}
		response, err := s.resolver.client.Do(request)
		if err != nil {
			return nil, classifyFailedExchange(ctx, httpexchange.Request, fmt.Errorf("registry GET: %w", err))
		}
		return response, nil
	}
	response, err := send()
	if err != nil || response.StatusCode != http.StatusUnauthorized || s.token != "" {
		return response, err
	}
	challenge := response.Header.Get("WWW-Authenticate")
	response.Body.Close()
	if s.token, err = s.resolver.publicBearerToken(ctx, challenge); err != nil {
		return nil, err
	}
	return send()
}
