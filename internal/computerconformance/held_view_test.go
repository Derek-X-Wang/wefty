package computerconformance

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image/color"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

func TestHeldViewExperimentTrialsAndSummary(t *testing.T) {
	for _, scenario := range []string{"all-fresh", "first-held-stale", "both-stale", "fresh-stale"} {
		t.Run(scenario, func(t *testing.T) {
			expected := []string{"A", "B", "B", "A", "A", "B", "B", "A", "A", "B", "B", "A"}
			var tick, redObserved, greenObserved atomic.Int64
			tick.Store(time.Unix(100, 0).UnixNano())
			var green atomic.Bool
			var activeViews, viewOpens, viewRequests, controls, launches atomic.Int32
			var lines []string
			options := HeldViewProbeOptions{
				Now:   func() time.Time { return time.Unix(0, tick.Load()) },
				Sleep: func(_ context.Context, d time.Duration) error { tick.Add(int64(d)); return nil },
				Prepare: func(context.Context, int) (string, error) {
					if launches.Add(1) == 1 {
						return "shell_exit=1", errors.New("transient launch failure")
					}
					green.Store(false)
					return "shell_exit=0 launch_pid=test", nil
				},
				Diagnostics: func(context.Context) string { return "chromium_exit=1 log_tail=launch diagnostic" },
				Report:      func(line string) { lines = append(lines, line) },
				Open: func(ctx context.Context, mode string) (*rfbclient.Client, func(), error) {
					trial := int(controls.Load())
					if mode == "input" {
						trial = int(controls.Add(1))
					}
					if trial < 1 || trial > 12 {
						t.Fatalf("unexpected control count %d", trial)
					}
					kind := expected[trial-1]
					if mode == "view" {
						viewOpens.Add(1)
						if activeViews.Add(1) != 1 {
							t.Error("extra view connection during a trial")
						}
						if tick.Load()-redObserved.Load() < int64(300*time.Millisecond) {
							t.Error("view opened before red separation interval")
						}
						if (kind == "A" && green.Load()) || (kind == "B" && !green.Load()) {
							t.Errorf("wrong view open point: trial=%d kind=%s green=%t", trial, kind, green.Load())
						}
					}
					var requests atomic.Int32
					client, closeClient, err := heldViewFakeClient(t, ctx, func(key uint32) {
						if key == 'r' {
							if activeViews.Load() != 0 {
								t.Error("previous view still open at red reset")
							}
							green.Store(false)
						} else {
							if key != 'a' {
								t.Errorf("green keysym=%#x want delay-0 a", key)
							}
							want := int32(0)
							if kind == "A" {
								want = 1
							}
							if activeViews.Load() != want {
								t.Errorf("trial %d kind=%s: view count at green tap=%d", trial, kind, activeViews.Load())
							}
							green.Store(true)
						}
					}, func() color.RGBA {
						if mode == "input" {
							if green.Load() {
								greenObserved.Store(tick.Load())
								return color.RGBA{G: 255, A: 255}
							}
							redObserved.Store(tick.Load())
							return color.RGBA{R: 255, A: 255}
						}
						viewRequests.Add(1)
						if requests.Add(1) != 1 {
							t.Error("measurement retried on view connection")
						}
						if tick.Load()-greenObserved.Load() < int64(250*time.Millisecond) {
							t.Error("view request preceded extra control-green settle")
						}
						stale := scenario == "both-stale" || scenario == "first-held-stale" && trial == 1 || scenario == "fresh-stale" && kind == "B"
						if stale {
							return color.RGBA{R: 255, A: 255}
						}
						return color.RGBA{G: 255, A: 255}
					})
					return client, func() {
						closeClient()
						if mode == "view" {
							activeViews.Add(-1)
						}
					}, err
				},
			}
			result, err := ProbeHeldViewFrames(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Trials) != 12 || controls.Load() != 12 || viewOpens.Load() != 12 || viewRequests.Load() != 12 || activeViews.Load() != 0 {
				t.Fatalf("measurements changed/retried: trials=%d control=%d view=%d requests=%d open=%d", len(result.Trials), controls.Load(), viewOpens.Load(), viewRequests.Load(), activeViews.Load())
			}
			for i, trial := range result.Trials {
				if trial.Kind != expected[i] || trial.Index != i+1 {
					t.Errorf("trial %d: %+v", i+1, trial)
				}
				if trial.FirstRequestMS < trial.ControlGreenMS+250 {
					t.Errorf("trial did not settle after control green: %+v", trial)
				}
				wantColour := "00ff00/ff"
				if trial.Result == "stale" {
					wantColour = "ff0000/ff"
				}
				if strings.Count(trial.Pixels, wantColour) != 4 {
					t.Errorf("missing sample pixels: %+v", trial)
				}
			}
			wantSummary := "held-vs-fresh summary: A stale=0/6 fresh=6/6; B stale=0/6 fresh=6/6"
			switch scenario {
			case "first-held-stale":
				wantSummary = "held-vs-fresh summary: A stale=1/6 fresh=5/6; B stale=0/6 fresh=6/6"
			case "both-stale":
				wantSummary = "held-vs-fresh summary: A stale=6/6 fresh=0/6; B stale=6/6 fresh=0/6"
			case "fresh-stale":
				wantSummary = "held-vs-fresh summary: A stale=0/6 fresh=6/6; B stale=6/6 fresh=0/6"
			}
			if result.Summary != wantSummary || lines[len(lines)-1] != wantSummary {
				t.Fatalf("summary=%q last log=%q", result.Summary, lines[len(lines)-1])
			}
			logged := 0
			for _, line := range lines {
				if !strings.HasPrefix(line, "held-vs-fresh trial: ") {
					continue
				}
				var trial HeldViewTrial
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "held-vs-fresh trial: ")), &trial); err != nil {
					t.Fatal(err)
				}
				if trial.Index != logged+1 {
					t.Errorf("trial log order: %+v", trial)
				}
				logged++
			}
			if logged != 12 {
				t.Errorf("trial JSON lines=%d", logged)
			}
			if result.HeldOnlyStale() != (scenario == "first-held-stale") {
				t.Error("Seat experimental failure rule changed")
			}
			if result.HasStaleHeld() != (scenario == "first-held-stale" || scenario == "both-stale") {
				t.Error("runtime experimental failure rule changed")
			}
			if launches.Load() != 2 || !strings.Contains(result.SetupAttempts[0], "shell_exit=1") || !strings.Contains(result.SetupAttempts[0], "log_tail=launch diagnostic") {
				t.Error("setup retry diagnostics lost")
			}
		})
	}
}

func TestHeldViewProbeSetupUnavailableHasBoundedAttempts(t *testing.T) {
	attempts := 0
	var lines []string
	result, err := ProbeHeldViewFrames(t.Context(), HeldViewProbeOptions{
		Report: func(line string) { lines = append(lines, line) },
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
	if !errors.As(err, &unavailable) || attempts != 3 || !strings.Contains(strings.Join(result.SetupAttempts, "; "), "log_tail=missing binary") {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
	if result.Summary != "held-vs-fresh summary: A stale=0/6 fresh=0/6; B stale=0/6 fresh=0/6" || lines[len(lines)-1] != result.Summary {
		t.Errorf("missing failure summary: %q", result.Summary)
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
