package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image/png"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

// The fake front door speaks real RFB bytes, including split WebSocket frames.
// Assertions use CLI exit/output, the PNG, and the input bytes the door receives.
func TestSeatScreenCommandsRealBinary(t *testing.T) {
	binaryPath := buildWefty(t)
	id := "seat_" + strings.Repeat("a", 32)
	other := "seat_" + strings.Repeat("b", 32)
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	credentials := map[string]string{id: "secret-a", other: "secret-b"}
	for seat, secret := range credentials {
		if _, err := storeSeatCredential(seat, secret); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var events []byte
	person := false
	var refusedCredential atomic.Int32
	address := ""
	address = startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 || parts[1] != "v1" || parts[2] != "seats" || credentials[parts[3]] == "" {
			t.Error("unexpected Seat path")
			w.WriteHeader(404)
			return
		}
		expected := credentials[parts[3]]
		if r.Header.Get(l1.SeatCredentialHeader) != expected {
			refusedCredential.Add(1)
			w.WriteHeader(403)
			json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorForbidden, Message: "invalid Seat credential"}})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/screen-target") {
			json.NewEncoder(w).Encode(map[string]any{"endpoint": "ws://" + address, "unread_events": 2})
			return
		}

		mu.Lock()
		driving := person
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/input") && driving {
			w.WriteHeader(409)
			json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: "person_driving", Message: "a person is driving"}})
			return
		}
		w.Header().Set("Wefty-Seat-Release-Note", base64.RawURLEncoding.EncodeToString([]byte("Please continue.")))
		w.Header().Set("Wefty-Seat-Unread-Events", "3")
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"binary"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		conn := websocket.NetConn(ctx, c, websocket.MessageBinary)
		if err := screenFakeRFB(conn, func(b []byte) { mu.Lock(); events = append(events, b...); mu.Unlock() }); err != nil && ctx.Err() == nil { /* client closes each short session */
		}
	})
	for _, args := range [][]string{
		{"screenshot", id},
		{"click", id, "12", "34", "--button", "double", "--screenshot"},
		{"move", id, "12", "34"}, {"drag", id, "1", "2", "12", "34"},
		{"scroll", id, "12", "34", "--dy", "-2", "--dx", "1"},
		{"type", id, "hé世界"}, {"key", id, "ctrl+l"},
	} {
		t.Run(args[0], func(t *testing.T) {
			cli := append([]string{"--l1", address, "--json", "seat"}, args...)
			code, out, diagnostic := screenRunCLI(t, binaryPath, cli...)
			if code != 0 {
				t.Fatalf("exit=%d %s", code, out)
			}
			wantNotes := 1
			if strings.Contains(out, "Please continue.") || strings.Count(diagnostic, "Please continue.") != wantNotes || !strings.Contains(out, "3 new Seat events") || !strings.Contains(out, "elapsed_ms") {
				t.Fatalf("missing reply metadata: %s", out)
			}
			// The result is one stdout JSON document; the release note is on stderr.
			begin := strings.Index(out, "{")
			var result struct {
				Path   string `json:"path"`
				Unread int    `json:"unread_events"`
			}
			if begin < 0 || json.Unmarshal([]byte(out[begin:]), &result) != nil || result.Unread != 3 {
				t.Fatalf("invalid reply: %s", out)
			}
			if args[0] == "screenshot" || args[0] == "click" {
				f, err := os.Open(result.Path)
				if err != nil {
					t.Fatal(err)
				}
				im, err := png.Decode(f)
				f.Close()
				if err != nil || im.Bounds().Dx() != 1280 || im.Bounds().Dy() != 800 {
					t.Fatalf("PNG: %v", err)
				}
				wd, _ := os.Getwd()
				if strings.HasPrefix(result.Path, wd+string(os.PathSeparator)) {
					t.Fatal("default screenshot in checkout")
				}
				r, g, b, _ := im.At(0, 0).RGBA()
				if r != 65535 || g != 0 || b != 0 {
					t.Errorf("decoded pixel = %d,%d,%d", r, g, b)
				}
				os.Remove(result.Path)
			}
		})
	}
	mu.Lock()
	person = true
	mu.Unlock()
	refusedPath := filepath.Join(t.TempDir(), "refused.png")
	code, out := runWefty(t, binaryPath, 30*time.Second, "--l1", address, "--json", "seat", "click", id, "1", "2", "--screenshot", refusedPath)
	if code != 5 || !strings.Contains(out, "person_driving") || !strings.Contains(out, "a person is driving") || !strings.Contains(out, "elapsed_ms") {
		t.Fatalf("refusal exit=%d %s", code, out)
	}
	if _, err := os.Stat(refusedPath); !os.IsNotExist(err) {
		t.Fatalf("refused input left a PNG: %v", err)
	}
	path := filepath.Join(t.TempDir(), "person.png")
	code, out = runWefty(t, binaryPath, 30*time.Second, "--l1", address, "seat", "screenshot", id, "-o", path)
	if code != 0 {
		t.Fatalf("screenshot while person drives: %d %s", code, out)
	}
	// Seat B works with its own distinct credential. Every accepted request above
	// checks that the path and header name the same Seat.
	otherPath := filepath.Join(t.TempDir(), "other.png")
	code, out = runWefty(t, binaryPath, 30*time.Second, "--l1", address, "seat", "screenshot", other, "-o", otherPath)
	if code != 0 {
		t.Fatalf("Seat B credential failed: %d %s", code, out)
	}
	// Deliberately put A's secret under B's ID; the door must refuse the crossover.
	if _, err := storeSeatCredential(other, credentials[id]); err != nil {
		t.Fatal(err)
	}
	code, out = runWefty(t, binaryPath, 30*time.Second, "--l1", address, "seat", "click", other, "1", "2")
	if code != 3 || !strings.Contains(out, "invalid Seat credential") {
		t.Fatalf("cross-Seat refusal: %d %s", code, out)
	}
	if refusedCredential.Load() != 1 {
		t.Fatalf("cross-Seat requests refused=%d", refusedCredential.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for name, want := range map[string][]byte{
		"double click":     {5, 1, 0, 12, 0, 34, 5, 0, 0, 12, 0, 34, 5, 1, 0, 12, 0, 34, 5, 0, 0, 12, 0, 34},
		"drag starts held": {5, 1, 0, 1, 0, 2},
		"drag releases":    {5, 0, 0, 12, 0, 34},
		"scroll up":        {5, 8, 0, 12, 0, 34, 5, 0, 0, 12, 0, 34},
		"scroll right":     {5, 64, 0, 12, 0, 34, 5, 0, 0, 12, 0, 34},
		"Unicode key":      {4, 1, 0, 0, 1, 0, 0x4e, 0x16, 4, 0, 0, 0, 1, 0, 0x4e, 0x16},
		"ctrl+l":           {4, 1, 0, 0, 0, 0, 0xff, 0xe3, 4, 1, 0, 0, 0, 0, 0, 0x6c, 4, 0, 0, 0, 0, 0, 0, 0x6c, 4, 0, 0, 0, 0, 0, 0xff, 0xe3},
	} {
		if !bytes.Contains(events, want) {
			t.Errorf("missing %s input bytes", name)
		}
	}
}

func screenFakeRFB(conn io.ReadWriter, event func([]byte)) error {
	return screenFakeRFBRequests(conn, event, func(int, int) {})
}
func screenFakeRFBRequests(conn io.ReadWriter, event func([]byte), request func(int, int)) error {
	conn.Write([]byte("RFB 003.008\n"))
	b := make([]byte, 12)
	if _, err := io.ReadFull(conn, b); err != nil {
		return err
	}
	conn.Write([]byte{1, 1})
	if _, err := io.ReadFull(conn, b[:1]); err != nil {
		return err
	}
	conn.Write([]byte{0, 0, 0, 0})
	if _, err := io.ReadFull(conn, b[:1]); err != nil {
		return err
	}
	init := make([]byte, 24)
	binary.BigEndian.PutUint16(init, 1280)
	binary.BigEndian.PutUint16(init[2:], 800)
	copy(init[4:], []byte{32, 24, 0, 1, 0, 255, 0, 255, 0, 255, 16, 8, 0})
	conn.Write(init)
	for {
		if _, err := io.ReadFull(conn, b[:1]); err != nil {
			return err
		}
		kind := b[0]
		n := 0
		switch kind {
		case 0:
			n = 19
		case 2:
			n = 3
		case 3:
			n = 9
		case 4:
			n = 7
		case 5:
			n = 5
		default:
			return io.ErrUnexpectedEOF
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}
		if kind == 2 {
			enc := make([]byte, int(binary.BigEndian.Uint16(payload[1:]))*4)
			if _, err := io.ReadFull(conn, enc); err != nil {
				return err
			}
		}
		if kind == 4 || kind == 5 {
			event(append([]byte{kind}, payload...))
		}
		if kind == 3 {
			width, height := int(binary.BigEndian.Uint16(payload[5:])), int(binary.BigEndian.Uint16(payload[7:]))
			request(width, height)
			header := make([]byte, 16)
			header[3] = 1
			binary.BigEndian.PutUint16(header[8:], uint16(width))
			binary.BigEndian.PutUint16(header[10:], uint16(height))
			if _, err := conn.Write(header); err != nil {
				return err
			}
			pixels := make([]byte, width*height*4)
			for i := 0; i < len(pixels); i += 4 {
				pixels[i+2] = 255
			}
			if _, err := conn.Write(pixels); err != nil {
				return err
			}
		}
	}
}

func screenRunCLI(t *testing.T, binaryPath string, args ...string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binaryPath, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if ctx.Err() != nil {
		t.Fatalf("screen command timed out: %s", stderr.String())
	}
	code := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatal(err)
		}
		code = exit.ExitCode()
	}
	return code, stdout.String(), stderr.String()
}
