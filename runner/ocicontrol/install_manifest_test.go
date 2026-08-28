package ocicontrol

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOCIInstallManifestDefaultsAndRelativeArchive(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "share", "wefty", "oci", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := `{"version":1,"helper_checksum":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","probe_reference":"wefty.local/probe","probe_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","probe_archive_path":"probe.oci.tar"}`
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
	manifest, err := ReadOCIInstallManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ProbeArchivePath != filepath.Join(filepath.Dir(path), "probe.oci.tar") {
		t.Fatalf("archive path = %q", manifest.ProbeArchivePath)
	}
	defaultPath, err := DefaultOCIInstallManifestPath(filepath.Join(root, "bin", "wefty"))
	if err != nil {
		t.Fatal(err)
	}
	if defaultPath != path {
		t.Fatalf("default manifest path = %q, want %q", defaultPath, path)
	}
}

func TestOCIInstallManifestRejectsUnknownOrMalformedValues(t *testing.T) {
	for name, payload := range map[string]string{
		"unknown": `{\"version\":1,\"unknown\":true}`,
		"digest":  `{"version":1,"helper_checksum":"sha256:no","probe_reference":"probe","probe_digest":"sha256:no","probe_archive_path":"probe.tar"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manifest.json")
			if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadOCIInstallManifest(path); err == nil {
				t.Fatal("malformed manifest accepted")
			}
		})
	}
}
