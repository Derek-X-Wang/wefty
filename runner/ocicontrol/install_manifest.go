package ocicontrol

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const OCIInstallManifestVersion = 1

var sha256ValuePattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// OCIInstallManifest is the relocatable release-time handoff between
// packaging and the configure-only Linux setup command.
type OCIInstallManifest struct {
	Version          int    `json:"version"`
	HelperChecksum   string `json:"helper_checksum"`
	ProbeReference   string `json:"probe_reference"`
	ProbeDigest      string `json:"probe_digest"`
	ProbeArchivePath string `json:"probe_archive_path"`
}

func DefaultOCIInstallManifestPath(executable string) (string, error) {
	if !filepath.IsAbs(executable) {
		return "", errors.New("wefty executable path must be absolute")
	}
	prefix := filepath.Dir(filepath.Dir(filepath.Clean(executable)))
	return filepath.Join(prefix, "share", "wefty", "oci", "manifest.json"), nil
}

func ReadOCIInstallManifest(path string) (OCIInstallManifest, error) {
	if !filepath.IsAbs(path) {
		return OCIInstallManifest{}, errors.New("OCI install manifest path must be absolute")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return OCIInstallManifest{}, err
	}
	var manifest OCIInstallManifest
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return OCIInstallManifest{}, fmt.Errorf("decode OCI install manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return OCIInstallManifest{}, errors.New("OCI install manifest contains trailing data")
	}
	if manifest.Version != OCIInstallManifestVersion || !sha256ValuePattern.MatchString(manifest.HelperChecksum) ||
		!sha256ValuePattern.MatchString(manifest.ProbeDigest) || strings.TrimSpace(manifest.ProbeReference) == "" ||
		strings.ContainsAny(manifest.ProbeReference+manifest.ProbeArchivePath, "\r\n") || strings.TrimSpace(manifest.ProbeArchivePath) == "" {
		return OCIInstallManifest{}, errors.New("OCI install manifest is invalid")
	}
	if !filepath.IsAbs(manifest.ProbeArchivePath) {
		clean := filepath.Clean(manifest.ProbeArchivePath)
		if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return OCIInstallManifest{}, errors.New("OCI install manifest probe archive escapes the release directory")
		}
		manifest.ProbeArchivePath = filepath.Join(filepath.Dir(path), clean)
	}
	return manifest, nil
}
