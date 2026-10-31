package l3

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestExecCaptureRunGitObjectIDs(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		valid    bool
	}{
		{"sha1", strings.Repeat("a", 40), true},
		{"sha256", strings.Repeat("b", 64), true},
		{"short", strings.Repeat("a", 39), false},
		{"intermediate", strings.Repeat("a", 41), false},
		{"long", strings.Repeat("a", 65), false},
		{"uppercase_sha1", strings.Repeat("A", 40), false},
		{"uppercase_sha256", strings.Repeat("B", 64), false},
		{"nonhex_sha1", strings.Repeat("g", 40), false},
		{"nonhex_sha256", strings.Repeat("g", 64), false},
	} {
		for _, field := range []string{"base_commit", "snapshot_tree"} {
			t.Run(tc.name+"/"+field, func(t *testing.T) {
				request := execCaptureRunRequest(strings.Repeat("c", 64), 1)
				var params map[string]any
				if err := json.Unmarshal(request.Params, &params); err != nil {
					t.Fatal(err)
				}
				params[field] = tc.id
				request.Params, _ = json.Marshal(params)
				err := validateExecCaptureRun(CreateRunInput{FabricClient: true, Request: request})
				if (err == nil) != tc.valid {
					t.Fatalf("valid=%v, validation error=%v", tc.valid, err)
				}
			})
		}
	}
}

func TestExecCaptureTotalCapNamesHeldOrReservedBytes(t *testing.T) {
	for _, declared := range []int64{3, -1} {
		name := "declared"
		if declared < 0 {
			name = "streamed"
		}
		t.Run(name, func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), StoreOptions{ExecCaptureMaxBytes: 10, ExecCaptureTotalBytes: 10})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			digest := sha256.Sum256([]byte("held"))
			if _, err = store.uploadExecCapture(t.Context(), hex.EncodeToString(digest[:]), "owner", 4, strings.NewReader("held")); err != nil {
				t.Fatal(err)
			}
			// An upload has reserved four bytes but has not produced any yet.
			reader, writer := io.Pipe()
			done := make(chan error, 1)
			go func() {
				_, err := store.uploadExecCapture(t.Context(), strings.Repeat("a", 64), "owner", 4, reader)
				done <- err
			}()
			defer func() { reader.Close(); writer.Close(); <-done }()
			// Its first read proves admission has already reserved the bytes.
			if _, err := writer.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			_, err = store.uploadExecCapture(t.Context(), strings.Repeat("b", 64), "owner", declared, strings.NewReader("new"))
			var refusal *Error
			if !errors.As(err, &refusal) || refusal.Code != contract.ErrorConflict || !refusal.Retryable || !strings.Contains(refusal.Message, "(8 bytes held or reserved)") {
				t.Fatalf("total-cap refusal=%v", err)
			}
		})
	}
}
