package computerconformance

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"image/color"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

func TestHeldViewProbeObservesViewAndNeverRetriesFirstFrame(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprint("stale=", stale), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var green atomic.Bool
			var observerGreen, observerRed atomic.Bool
			var viewOpens, heldRequests, controlRequests, launches atomic.Int32
			options := HeldViewProbeOptions{
				Prepare: func(context.Context, int) (string, error) {
					if launches.Add(1) == 1 {
						return "shell_exit=1", errors.New("transient launch failure")
					}
					green.Store(false)
					return "shell_exit=0 launch_pid=test", nil
				},
				Diagnostics: func(context.Context) string { return "chromium_exit=1 log_tail=launch diagnostic" },
				Open: func(ctx context.Context, mode string) (*rfbclient.Client, func(), error) {
					held := false
					if mode == "input" {
						observerRed.Store(false)
					}
					if mode == "view" {
						held = viewOpens.Add(1)%2 == 0
					}
					if held && (green.Load() || !observerRed.Load()) {
						t.Error("held connection opened before observer red")
					}
					if held {
						observerGreen.Store(false)
					}
					return heldViewFakeClient(t, ctx, func(key uint32) {
						if key == 'r' {
							green.Store(false)
						} else {
							green.Store(true)
						}
					}, func() color.RGBA {
						if mode == "input" {
							controlRequests.Add(1)
							return color.RGBA{G: 255, A: 255}
						}
						if held {
							heldRequests.Add(1)
							if !observerGreen.Load() {
								t.Error("held first request preceded view observer green")
							}
							if stale {
								return color.RGBA{R: 255, A: 255}
							}
						} else if green.Load() {
							observerGreen.Store(true)
						} else {
							observerRed.Store(true)
						}
						if green.Load() {
							return color.RGBA{G: 255, A: 255}
						}
						return color.RGBA{R: 255, A: 255}
					})
				},
			}
			observations, err := ProbeHeldViewFrames(ctx, options)
			if controlRequests.Load() != 0 {
				t.Error("freshness oracle read control framebuffer")
			}
			if stale {
				var unavailable *HeldViewSetupUnavailableError
				if err == nil || errors.As(err, &unavailable) || !strings.Contains(err.Error(), "no retry") || !strings.Contains(err.Error(), "ff0000/ff") || !strings.Contains(err.Error(), "log_tail=launch diagnostic") {
					t.Fatalf("stale assertion: %v", err)
				}
				if heldRequests.Load() != 1 || launches.Load() != 2 || viewOpens.Load() != 2 {
					t.Fatalf("stale frame retried: requests=%d launches=%d opens=%d", heldRequests.Load(), launches.Load(), viewOpens.Load())
				}
			} else {
				if err != nil || len(observations) != 3 || heldRequests.Load() != 3 {
					t.Fatalf("probe result: observations=%+v requests=%d error=%v", observations, heldRequests.Load(), err)
				}
				if len(observations[0].SetupAttempts) != 2 || !strings.Contains(observations[0].SetupAttempts[0], "shell_exit=1") {
					t.Fatal("setup retry diagnostics lost")
				}
			}
		})
	}
}

func TestHeldViewProbeSetupUnavailableHasBoundedAttempts(t *testing.T) {
	attempts := 0
	_, err := ProbeHeldViewFrames(t.Context(), HeldViewProbeOptions{
		Prepare: func(context.Context, int) (string, error) {
			attempts++
			return "shell_exit=127", errors.New("Chromium unavailable")
		},
		Diagnostics: func(context.Context) string { return "chromium_exit=127 log_tail=missing binary" },
		Open: func(context.Context, string) (*rfbclient.Client, func(), error) {
			t.Fatal("open after failed preparation")
			return nil, nil, nil
		},
	})
	var unavailable *HeldViewSetupUnavailableError
	if !errors.As(err, &unavailable) || attempts != 3 || !strings.Contains(err.Error(), "log_tail=missing binary") {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
}

func TestHeldViewOptionalEvidenceDoesNotWaitOrLaunch(t *testing.T) {
	for _, scenario := range []string{"no-opt-in", "no-oracle", "oracle-unavailable", "oracle-not-ready", "preparation-unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			calls, launches := 0, 0
			runner := runtimeRunner{config: RuntimeConfig{ReferenceSurfaceProbe: scenario != "no-opt-in", InputOraclePath: "/oracle"}, recorder: NewRecorder("test", "docker", "linux/arm64", time.Now())}
			if scenario == "no-oracle" {
				runner.config.InputOraclePath = ""
			}
			runner.runCommandHook = func(_ context.Context, args ...string) (commandResult, error) {
				calls++
				if len(args) > 2 && args[2] == "cat" {
					if scenario == "oracle-unavailable" {
						return commandResult{}, errors.New("missing oracle")
					}
					if scenario == "oracle-not-ready" {
						return commandResult{stdout: `{"version":1,"ready":false}`}, nil
					}
					return commandResult{stdout: `{"version":1,"ready":true}`}, nil
				}
				if strings.Contains(strings.Join(args, " "), "command -v chromium") {
					launches++
					return commandResult{exitCode: 127, stderr: "no Chromium"}, nil
				}
				return commandResult{stdout: "chromium_exit=127 log_tail=missing binary"}, nil
			}
			runner.checkHeldViewFrames(t.Context())
			if runner.failed {
				t.Fatal("optional evidence became FAIL")
			}
			for _, check := range runner.recorder.Finish(time.Now()).Checks {
				if check.ID == "screen.held-view-current-frame" && check.Status != StatusNotRun {
					t.Fatalf("check=%+v", check)
				}
			}
			if scenario == "preparation-unavailable" {
				if launches != 3 {
					t.Fatalf("setup launches=%d", launches)
				}
			} else if calls > 1 || launches != 0 {
				t.Fatalf("unexpected wait or preparation: calls=%d launches=%d", calls, launches)
			}
		})
	}
}

// Small real RFB streams keep these checks independent of loopback permissions.
func heldViewFakeClient(t *testing.T, ctx context.Context, key func(uint32), frame func() color.RGBA) (*rfbclient.Client, func(), error) {
	t.Helper()
	client, server := net.Pipe()
	stop := context.AfterFunc(ctx, func() { _ = client.Close(); _ = server.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		write := func(p []byte) bool { _, err := server.Write(p); return err == nil }
		read := func(n int) ([]byte, error) { p := make([]byte, n); _, err := io.ReadFull(server, p); return p, err }
		if !write([]byte("RFB 003.008\n")) {
			return
		}
		if _, err := read(12); err != nil {
			return
		}
		if !write([]byte{1, 1}) {
			return
		}
		if _, err := read(1); err != nil {
			return
		}
		if !write([]byte{0, 0, 0, 0}) {
			return
		}
		if _, err := read(1); err != nil {
			return
		}
		init := make([]byte, 24)
		binary.BigEndian.PutUint16(init, 3)
		binary.BigEndian.PutUint16(init[2:], 3)
		if !write(init) {
			return
		}
		for {
			kind, err := read(1)
			if err != nil {
				return
			}
			switch kind[0] {
			case 0:
				if _, err := read(19); err != nil {
					return
				}
			case 2:
				if _, err := read(7); err != nil {
					return
				}
			case 4:
				p, err := read(7)
				if err != nil {
					return
				}
				if p[0] == 1 {
					key(binary.BigEndian.Uint32(p[3:]))
				}
			case 5:
				if _, err := read(5); err != nil {
					return
				}
			case 3:
				if _, err := read(9); err != nil {
					return
				}
				pixel := frame()
				reply := make([]byte, 16+3*3*4)
				reply[3] = 1
				binary.BigEndian.PutUint16(reply[8:], 3)
				binary.BigEndian.PutUint16(reply[10:], 3)
				for i := 16; i < len(reply); i += 4 {
					reply[i] = pixel.B
					reply[i+1] = pixel.G
					reply[i+2] = pixel.R
				}
				if !write(reply) {
					return
				}
			default:
				return
			}
		}
	}()
	close := func() { stop(); _ = client.Close(); _ = server.Close(); <-done }
	rfb, err := rfbclient.Open(client)
	if err != nil {
		close()
		return nil, nil, err
	}
	return rfb, close, nil
}
