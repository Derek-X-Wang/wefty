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
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
)

// inputCapturePage is the fake page and backend. Time is virtual: it moves
// only through the probe's Sleep, so the page redraw happens while the probe
// sleeps, never during the real 250 ms settle wait.
type inputCapturePage struct {
	t        *testing.T
	scenario string
	mu       sync.Mutex
	tick     time.Time
	greenAt  time.Time // zero: red with nothing pending
	keyAt    time.Time
	trial    int
	open     int // live connections to the backend
	views    int
	captures []inputCaptureObservation
	// cancelNext fails the next Sleep, after the first T capture in the
	// "t-cleanup-cancel" scenario.
	cancelNext bool
}

type inputCaptureObservation struct {
	kind     string
	sinceKey time.Duration
	open     int
	held     bool
}

func (p *inputCapturePage) now() time.Time { p.mu.Lock(); defer p.mu.Unlock(); return p.tick }
func (p *inputCapturePage) sleep(_ context.Context, d time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancelNext {
		return context.Canceled // The probe was cancelled during T's cleanup.
	}
	p.tick = p.tick.Add(d)
	return nil
}
func (p *inputCapturePage) green() bool { return !p.greenAt.IsZero() && !p.tick.Before(p.greenAt) }

func (p *inputCapturePage) key(key uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch key {
	case 'r': // Each trial's setup resets the page once.
		p.trial++
		p.greenAt = time.Time{}
	case 'a':
		p.greenAt = p.tick
	case 'd':
		p.keyAt = p.tick
		if p.scenario != "inconclusive" {
			p.greenAt = p.tick.Add(inputCaptureRedraw)
		}
	default:
		p.t.Errorf("unexpected key %#x", key)
	}
}

func (p *inputCapturePage) kind() string { return inputCaptureOrder[p.trial-1] }

// syncChanged makes a T trial's Sync report a change, so its settle never
// times out. Trial 5 is the first T.
func (p *inputCapturePage) syncChanged() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kind() == "T" && (p.scenario == "t-no-timeout" || p.scenario == "t-one-miss" && p.trial == 5)
}

// reply chooses the colour for a full or Sync request. held is true when an
// incremental request is outstanding: that full request is the capture.
func (p *inputCapturePage) reply(mode string, held bool) color.RGBA {
	p.mu.Lock()
	defer p.mu.Unlock()
	green := p.green()
	if held {
		// A capture arrives only while an incremental request is held, which
		// orders it after the probe's settle wait without measuring time.
		p.captures = append(p.captures, inputCaptureObservation{kind: p.kind(), sinceKey: p.tick.Sub(p.keyAt), open: p.open, held: held})
		p.cancelNext = p.cancelNext || p.scenario == "t-cleanup-cancel" && p.kind() == "T"
		if (p.scenario == "c-stale" || p.scenario == "c-leftover-stale") && p.kind() == "C" {
			green = false
		}
	}
	if mode == "view" && p.scenario == "b-stale" {
		green = false
	}
	if green {
		return color.RGBA{G: 255, A: 255}
	}
	return color.RGBA{R: 255, A: 255}
}

func (p *inputCapturePage) options(lines *[]string) InputCaptureProbeOptions {
	return InputCaptureProbeOptions{
		Now:         p.now,
		Sleep:       p.sleep,
		Prepare:     func(context.Context, int) (string, error) { return "shell_exit=0", nil },
		Diagnostics: func(context.Context) string { return "chromium_exit=pending" },
		Report:      func(line string) { *lines = append(*lines, line) },
		Open: func(ctx context.Context, mode string) (*rfbclient.Client, func(), error) {
			p.mu.Lock()
			if mode == "view" {
				p.views++
			}
			p.open++
			p.mu.Unlock()
			client, closeClient, err := inputCaptureFakeClient(p.t, ctx, p, mode)
			var once sync.Once
			return client, func() {
				once.Do(func() {
					closeClient()
					p.mu.Lock()
					p.open--
					p.mu.Unlock()
				})
			}, err
		},
	}
}

func TestInputCaptureProbeTrialsAndSummary(t *testing.T) {
	for _, tc := range []struct {
		scenario, summary    string
		stale, conclusive    int
		status               Status
		failsWithError       bool
		trialsBeforeStopping int
	}{
		{"current", "input-capture trials: C current=6/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=3/3 inconclusive=0/3 red=3 green=0 max_ms=0", 0, 6, StatusPass, false, 15},
		{"c-stale", "input-capture trials: C current=0/6 stale=6/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=3/3 inconclusive=0/3 red=3 green=0 max_ms=0", 6, 6, StatusFail, false, 15},
		// A red reply stays queued after the red capture: the first ground-truth
		// read consumes it, the next one sees green. The capture is still stale.
		{"c-leftover-stale", "input-capture trials: C current=0/6 stale=6/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=3/3 inconclusive=0/3 red=3 green=0 max_ms=0", 6, 6, StatusFail, false, 15},
		{"b-stale", "input-capture trials: C current=6/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=6/6; T returned=3/3 inconclusive=0/3 red=3 green=0 max_ms=0", 0, 6, StatusPass, false, 15},
		{"inconclusive", "input-capture trials: C current=0/6 stale=0/6 inconclusive=6/6; B (fresh view) stale=0/6; T returned=3/3 inconclusive=0/3 red=3 green=0 max_ms=0", 0, 0, StatusNotRun, false, 15},
		{"t-error", "input-capture trials: C current=2/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=0/3 inconclusive=0/3 red=0 green=0 max_ms=0", 0, 2, StatusFail, true, 5},
		{"t-no-timeout", "input-capture trials: C current=6/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=0/3 inconclusive=3/3 red=0 green=0 max_ms=0", 0, 6, StatusNotRun, false, 15},
		// Cancelled while T waits for the page after its capture: the trial keeps
		// "returned", and the probe reports the error.
		{"t-cleanup-cancel", "input-capture trials: C current=2/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=1/3 inconclusive=0/3 red=1 green=0 max_ms=0", 0, 2, StatusFail, true, 5},
		{"t-one-miss", "input-capture trials: C current=6/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=2/3 inconclusive=1/3 red=2 green=0 max_ms=0", 0, 6, StatusPass, false, 15},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			page := &inputCapturePage{t: t, scenario: tc.scenario, tick: time.Unix(100, 0)}
			var lines []string
			result, err := ProbeInputCapture(t.Context(), page.options(&lines))
			if (err != nil) != tc.failsWithError {
				t.Fatalf("err=%v", err)
			}
			if len(result.Trials) != tc.trialsBeforeStopping {
				t.Fatalf("trials=%d: %+v", len(result.Trials), result.Trials)
			}
			for i, trial := range result.Trials {
				if trial.Kind != inputCaptureOrder[i] || trial.Index != i+1 {
					t.Errorf("trial %d: %+v", i+1, trial)
				}
				switch {
				case trial.Result == "error":
				case trial.Kind == "B":
					if trial.FirstRequestMS < trial.ControlGreenMS+250 {
						t.Errorf("view opened before control green + 250 ms: %+v", trial)
					}
				case trial.Kind == "C":
					wantReads := 1
					switch tc.scenario {
					case "c-leftover-stale":
						wantReads = 2
					case "inconclusive": // Never green: bounded at 1 s of 50 ms reads.
						wantReads = 21
					}
					if !trial.SettleTimedOut || trial.TimeoutMS != 0 || trial.GroundTruth == "" || trial.GroundTruthReads != wantReads {
						t.Errorf("C trial: %+v", trial)
					}
				default: // T: no ground truth; its result comes from the capture alone.
					missed := tc.scenario == "t-no-timeout" || tc.scenario == "t-one-miss" && trial.Index == 5
					if trial.SettleTimedOut == missed || trial.GroundTruth != "" || trial.GroundTruthReads != 0 || trial.Capture == "" {
						t.Errorf("T trial: %+v", trial)
					}
				}
			}
			if result.Summary != tc.summary || lines[len(lines)-1] != tc.summary {
				t.Fatalf("summary=%q last log=%q", result.Summary, lines[len(lines)-1])
			}
			logged := 0
			for _, line := range lines {
				if payload, ok := strings.CutPrefix(line, "input-capture trial: "); ok {
					var trial InputCaptureTrial
					if err := json.Unmarshal([]byte(payload), &trial); err != nil || trial.Index != logged+1 {
						t.Errorf("trial line %q: %v", line, err)
					}
					logged++
				}
			}
			if logged != tc.trialsBeforeStopping {
				t.Errorf("trial JSON lines=%d", logged)
			}
			if result.StaleCaptures() != tc.stale || result.ConclusiveCaptures() != tc.conclusive {
				t.Errorf("stale=%d conclusive=%d", result.StaleCaptures(), result.ConclusiveCaptures())
			}
			if status, detail := inputCaptureStatus(result, err); status != tc.status || !strings.Contains(detail, "input-capture trials:") {
				t.Errorf("status=%s detail=%s", status, detail)
			}
			// Each C or T capture is the full request that drains the held
			// incremental request, on the only open connection. C holds it past
			// the redraw; T sends it as soon as the real 250 ms settle expires.
			page.mu.Lock()
			defer page.mu.Unlock()
			// A T trial without a timeout has nothing to drain.
			wantCaptures, wantViews := 6+result.ReturnedTimeoutCaptures(), 6
			switch tc.scenario {
			case "t-error":
				wantCaptures, wantViews = 2, 2 // C,B,B,C, then T's capture fails.
			case "t-cleanup-cancel":
				wantCaptures, wantViews = 3, 2 // C,B,B,C,T, then T's cleanup is cancelled.
				if last := result.Trials[4]; last.Result != "returned" || !strings.Contains(last.Error, "cleanup after the capture") {
					t.Errorf("cancelled cleanup changed T's result: %+v", last)
				}
			}
			if len(page.captures) != wantCaptures {
				t.Fatalf("captures=%+v", page.captures)
			}
			for _, capture := range page.captures {
				if capture.open != 1 || !capture.held {
					t.Errorf("capture %+v", capture)
				}
				if capture.kind == "C" && capture.sinceKey < inputCaptureRedraw+inputCaptureSettle || capture.kind == "T" && capture.sinceKey != 0 {
					t.Errorf("capture point %+v", capture)
				}
			}
			if page.views != wantViews {
				t.Errorf("view connections=%d", page.views)
			}
			if page.open != 0 {
				t.Errorf("connections left open=%d", page.open)
			}
		})
	}
}

func TestInputCaptureProbeSetupUnavailableHasBoundedAttempts(t *testing.T) {
	attempts := 0
	var lines []string
	result, err := ProbeInputCapture(t.Context(), InputCaptureProbeOptions{
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
	var unavailable *InputCaptureSetupUnavailableError
	if !errors.As(err, &unavailable) || attempts != 3 || !strings.Contains(strings.Join(result.SetupAttempts, "; "), "log_tail=missing binary") {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
	if status, _ := inputCaptureStatus(result, err); status != StatusNotRun {
		t.Errorf("setup unavailable status=%s", status)
	}
	if result.Summary != "input-capture trials: C current=0/6 stale=0/6 inconclusive=0/6; B (fresh view) stale=0/6; T returned=0/3 inconclusive=0/3 red=0 green=0 max_ms=0" || lines[len(lines)-1] != result.Summary {
		t.Errorf("missing failure summary: %q", result.Summary)
	}
}

func TestInputCaptureStatusStaleWinsOverLaterSetupLoss(t *testing.T) {
	experiment := InputCaptureExperiment{Trials: []InputCaptureTrial{{Kind: "C", Index: 1, Result: "stale"}}}
	if status, _ := inputCaptureStatus(experiment, &InputCaptureSetupUnavailableError{Err: errors.New("Chromium exited")}); status != StatusFail {
		t.Fatalf("status=%s", status)
	}
}

// Four conclusive C trials and two returned T trials are the minimum to pass.
func TestInputCaptureStatusThresholds(t *testing.T) {
	trials := func(current, returned int) InputCaptureExperiment {
		var r InputCaptureExperiment
		for i := 0; i < 6; i++ {
			result := "inconclusive"
			if i < current {
				result = "current"
			}
			r.Trials = append(r.Trials, InputCaptureTrial{Kind: "C", Result: result})
		}
		for i := 0; i < 3; i++ {
			result := "inconclusive"
			if i < returned {
				result = "returned"
			}
			r.Trials = append(r.Trials, InputCaptureTrial{Kind: "T", Result: result})
		}
		return r
	}
	for _, tc := range []struct {
		current, returned int
		want              Status
	}{{4, 2, StatusPass}, {3, 3, StatusNotRun}, {6, 1, StatusNotRun}, {6, 3, StatusPass}} {
		if status, detail := inputCaptureStatus(trials(tc.current, tc.returned), nil); status != tc.want {
			t.Errorf("C current=%d T returned=%d: status=%s detail=%s", tc.current, tc.returned, status, detail)
		}
	}
}

func TestInputCaptureOptionalEvidenceDoesNotWaitOrLaunch(t *testing.T) {
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
			runner.checkInputCapture(t.Context())
			if runner.failed {
				t.Fatal("optional evidence became FAIL")
			}
			for _, check := range runner.recorder.Finish(time.Now()).Checks {
				if check.ID == "screen.input-capture-current-frame" && check.Status != StatusNotRun {
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

// inputCaptureFakeClient is a small real RFB peer on net.Pipe. It holds
// incremental requests, as x11vnc does without damage, and answers a later
// full request with one merged update, as LibVNCServer does.
func inputCaptureFakeClient(t *testing.T, ctx context.Context, page *inputCapturePage, mode string) (*rfbclient.Client, func(), error) {
	t.Helper()
	client, server := net.Pipe()
	stop := context.AfterFunc(ctx, func() { _ = client.Close(); _ = server.Close() })
	done, written := make(chan struct{}), make(chan struct{})
	// Replies go through a queue, so a reply can stay queued while the read
	// loop serves the next request, as on a real server.
	out := make(chan []byte, 64)
	go func() {
		defer close(written)
		failed := false
		for p := range out {
			if !failed {
				_, err := server.Write(p)
				failed = err != nil
			}
		}
	}()
	go func() {
		defer close(done)
		defer server.Close()
		defer close(out)
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
		held := false
		for {
			kind, err := read(1)
			if err != nil {
				return
			}
			switch kind[0] {
			case 0:
				_, err = read(19)
			case 2:
				var header []byte
				if header, err = read(3); err == nil {
					_, err = read(4 * int(binary.BigEndian.Uint16(header[1:])))
				}
			case 4:
				var p []byte
				if p, err = read(7); err == nil && p[0] == 1 {
					if mode == "view" {
						t.Error("key on view connection")
					}
					page.key(binary.BigEndian.Uint32(p[3:]))
				}
			case 5:
				_, err = read(5)
			case 3:
				var p []byte
				if p, err = read(9); err != nil {
					return
				}
				if p[0] == 1 {
					if held {
						t.Error("second outstanding incremental request")
					}
					held = true
					continue
				}
				x, y := int(binary.BigEndian.Uint16(p[1:])), int(binary.BigEndian.Uint16(p[3:]))
				w, h := int(binary.BigEndian.Uint16(p[5:])), int(binary.BigEndian.Uint16(p[7:]))
				if held {
					page.mu.Lock()
					fail := page.scenario == "t-error" && page.kind() == "T"
					page.mu.Unlock()
					if fail {
						return // The capture's transport fails.
					}
					x, y, w, h = 0, 0, 3, 3 // One merged update for both requests.
				}
				pixel := page.reply(mode, held)
				if w == 1 && h == 1 && page.syncChanged() {
					pixel = color.RGBA{G: 255, A: 255} // Sync reports a change.
				}
				reply := make([]byte, 16+w*h*4)
				reply[3] = 1
				for i, n := range []int{x, y, w, h} {
					binary.BigEndian.PutUint16(reply[4+i*2:], uint16(n))
				}
				for i := 16; i < len(reply); i += 4 {
					reply[i], reply[i+1], reply[i+2] = pixel.B, pixel.G, pixel.R
				}
				out <- reply
				page.mu.Lock()
				leftover := held && page.scenario == "c-leftover-stale" && page.kind() == "C"
				page.mu.Unlock()
				if leftover {
					out <- reply // A separate red answer to the full request stays queued.
				}
				held = false
			default:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	close := func() { stop(); _ = client.Close(); _ = server.Close(); <-done; <-written }
	rfb, err := rfbclient.Open(client)
	if err != nil {
		close()
		return nil, nil, err
	}
	return rfb, close, nil
}
