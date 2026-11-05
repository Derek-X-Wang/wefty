package computerconformance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1" // #nosec G505 -- RFC 6455 mandates SHA-1 for the public handshake nonce.
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
	"github.com/coder/websocket"
)

const websocketKey = "d2VmdHktY29uZm9ybWFuY2U="

type websocketConnection struct {
	connection net.Conn
	reader     *bufio.Reader
	banner     []byte
}

type rfbUpgradeRejectedError struct {
	status string
}

func (e *rfbUpgradeRejectedError) Error() string {
	return "upgrade failed: " + e.status
}

type websocketUpgradeIncompleteError struct{ err error }

func (e *websocketUpgradeIncompleteError) Error() string { return e.err.Error() }
func (e *websocketUpgradeIncompleteError) Unwrap() error { return e.err }

type rfbFirstFrameReadError struct{ err error }

func (e *rfbFirstFrameReadError) Error() string { return "read RFB greeting: " + e.err.Error() }
func (e *rfbFirstFrameReadError) Unwrap() error { return e.err }

type rfbProtocolError struct{ err error }

func (e *rfbProtocolError) Error() string { return e.err.Error() }
func (e *rfbProtocolError) Unwrap() error { return e.err }

func dialWebSocket(ctx context.Context, port int, path string, protocol *string) (*websocketConnection, string, textproto.MIMEHeader, error) {
	dialer := net.Dialer{Timeout: 5 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, "", nil, err
	}
	deadline, ok := ctx.Deadline()
	if ok {
		_ = connection.SetDeadline(deadline)
	} else {
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	}
	request := strings.Builder{}
	fmt.Fprintf(&request, "GET %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n", path, port)
	request.WriteString("Upgrade: websocket\r\nConnection: Upgrade\r\n")
	fmt.Fprintf(&request, "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n", websocketKey)
	if protocol != nil {
		fmt.Fprintf(&request, "Sec-WebSocket-Protocol: %s\r\n", *protocol)
	}
	request.WriteString("\r\n")
	if _, err := io.WriteString(connection, request.String()); err != nil {
		_ = connection.Close()
		return nil, "", nil, &websocketUpgradeIncompleteError{err: err}
	}
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil {
		_ = connection.Close()
		return nil, "", nil, &websocketUpgradeIncompleteError{err: err}
	}
	headers, err := textproto.NewReader(reader).ReadMIMEHeader()
	if err != nil {
		_ = connection.Close()
		return nil, "", nil, &websocketUpgradeIncompleteError{err: err}
	}
	return &websocketConnection{connection: connection, reader: reader}, strings.TrimSpace(status), headers, nil
}

func (connection *websocketConnection) close() { _ = connection.connection.Close() }

func (connection *websocketConnection) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(connection.reader, header); err != nil {
		return 0, nil, err
	}
	opcode := header[0] & 0x0f
	size := uint64(header[1] & 0x7f)
	switch size {
	case 126:
		value := make([]byte, 2)
		if _, err := io.ReadFull(connection.reader, value); err != nil {
			return 0, nil, err
		}
		size = uint64(binary.BigEndian.Uint16(value))
	case 127:
		value := make([]byte, 8)
		if _, err := io.ReadFull(connection.reader, value); err != nil {
			return 0, nil, err
		}
		size = binary.BigEndian.Uint64(value)
	}
	if size > 1<<20 {
		return 0, nil, fmt.Errorf("WebSocket frame is too large: %d", size)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(connection.reader, payload); err != nil {
		return 0, nil, err
	}
	return opcode, payload, nil
}

func (connection *websocketConnection) writeFrame(opcode byte, payload []byte) error {
	if len(payload) >= 126 {
		return fmt.Errorf("probe payload is too large: %d", len(payload))
	}
	mask := [4]byte{1, 2, 3, 4}
	frame := []byte{0x80 | opcode, 0x80 | byte(len(payload))}
	frame = append(frame, mask[:]...)
	for index, value := range payload {
		frame = append(frame, value^mask[index%len(mask)])
	}
	_, err := connection.connection.Write(frame)
	return err
}

func expectedWebSocketAccept() string {
	digest := sha1.Sum([]byte(websocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")) // #nosec G401 -- RFC 6455 protocol checksum, not cryptography.
	return base64.StdEncoding.EncodeToString(digest[:])
}

func OpenRFB(ctx context.Context, port int, path string) (*websocketConnection, error) {
	protocol := contract.ComputerDisplayWebSocketSubprotocol
	connection, status, headers, err := dialWebSocket(ctx, port, path, &protocol)
	if err != nil {
		return nil, err
	}
	failProtocol := func(format string, args ...any) (*websocketConnection, error) {
		connection.close()
		return nil, &rfbProtocolError{err: fmt.Errorf(format, args...)}
	}
	if !strings.HasPrefix(status, "HTTP/1.1 101 ") {
		connection.close()
		return nil, &rfbUpgradeRejectedError{status: status}
	}
	if headers.Get("Sec-WebSocket-Accept") != expectedWebSocketAccept() {
		return failProtocol("invalid Sec-WebSocket-Accept")
	}
	if headers.Get("Sec-WebSocket-Protocol") != contract.ComputerDisplayWebSocketSubprotocol {
		return failProtocol("binary subprotocol was not negotiated")
	}
	opcode, banner, err := connection.readFrame()
	if err != nil {
		connection.close()
		return nil, &rfbFirstFrameReadError{err: err}
	}
	if opcode != 2 || !contract.ValidComputerRFBVersionBanner(banner) {
		return failProtocol("invalid binary RFB greeting: opcode=%d payload=%q", opcode, banner)
	}
	connection.banner = banner
	return connection, nil
}

func AssertUpgradeRejected(ctx context.Context, port int, path string, protocol *string) error {
	connection, status, _, err := dialWebSocket(ctx, port, path, protocol)
	if err != nil {
		return err
	}
	defer connection.close()
	if strings.HasPrefix(status, "HTTP/1.1 101 ") {
		return fmt.Errorf("request unexpectedly upgraded")
	}
	return nil
}

func AssertTextRejected(ctx context.Context, port int) error {
	connection, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath)
	if err != nil {
		return err
	}
	defer connection.close()
	if err := connection.writeFrame(1, []byte("forbidden")); err != nil {
		return err
	}
	opcode, payload, err := connection.readFrame()
	if err != nil {
		// An immediate protocol-error close is conformant; the contract does not
		// prescribe a particular RFC 6455 close code.
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return fmt.Errorf("text frame was accepted and the connection remained open: %w", err)
		}
		return nil
	}
	if opcode != 8 {
		return fmt.Errorf("text frame produced opcode %d payload %x instead of closing", opcode, payload)
	}
	return nil
}

type rfbStream struct {
	connection *websocketConnection
	buffer     []byte
}

func (stream *rfbStream) read(size int) ([]byte, error) {
	for len(stream.buffer) < size {
		opcode, payload, err := stream.connection.readFrame()
		if err != nil {
			return nil, err
		}
		if opcode != 2 {
			return nil, fmt.Errorf("unexpected WebSocket opcode %d", opcode)
		}
		stream.buffer = append(stream.buffer, payload...)
	}
	value := bytes.Clone(stream.buffer[:size])
	stream.buffer = stream.buffer[size:]
	return value, nil
}

func (stream *rfbStream) write(value []byte) error { return stream.connection.writeFrame(2, value) }

// StartInput sends the same real RFB key and pointer bytes to either role. The
// caller compares an image-owned oracle before and after; the probe never
// assumes what the deterministic surface renders.
type InputSession struct {
	connection *websocketConnection
}

func (session *InputSession) Close() { session.connection.close() }

// SendKey appends a key press and release to an established RFB session. The
// control oracle keeps the pointer and key on one session while the image's
// keyboard observer retains focus.
func (session *InputSession) SendKey() error {
	for _, event := range rfbKeyEvents() {
		if err := session.connection.writeFrame(2, event); err != nil {
			return err
		}
	}
	return nil
}

// SendInput repeats a real key and pointer sequence on an established client.
// A new wayvnc client may create its virtual Wayland input objects after the
// first RFB messages arrive, so the isolation oracle exercises the active
// client as well as its cold-start path.
func (session *InputSession) SendInput(x, y int) error {
	for _, event := range rfbInputEvents(true, x, y) {
		if err := session.connection.writeFrame(2, event); err != nil {
			return err
		}
	}
	return nil
}

func StartInput(ctx context.Context, port, x, y int) (*InputSession, error) {
	return startRFBEvents(ctx, port, rfbInputEvents(true, x, y))
}

// StartKey opens a control session without moving or clicking the pointer, so
// the image's focused keyboard observer can prove its own liveness.
func StartKey(ctx context.Context, port int) (*InputSession, error) {
	return startRFBEvents(ctx, port, rfbKeyEvents())
}

// StartPointer is the consumption sentinel used after a view probe. Observing
// its unique coordinates proves the earlier key and pointer bytes have passed
// through the backend's input queue without relying on a fixed sleep. Motion
// without a button press preserves the keyboard observer's Sway focus.
func StartPointer(ctx context.Context, port, x, y int) (*InputSession, error) {
	return startRFBEvents(ctx, port, rfbInputEvents(false, x, y))
}

func startRFBEvents(ctx context.Context, port int, events [][]byte) (*InputSession, error) {
	connection, err := OpenRFB(ctx, port, contract.ComputerDisplayWebSocketPath)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*InputSession, error) { connection.close(); return nil, err }
	stream := &rfbStream{connection: connection, buffer: bytes.Clone(connection.banner)}
	if _, err := stream.read(contract.ComputerRFBVersionBannerBytes); err != nil {
		return fail(err)
	}
	if err := stream.write([]byte("RFB 003.008\n")); err != nil {
		return fail(err)
	}
	count, err := stream.read(1)
	if err != nil || count[0] == 0 {
		return fail(fmt.Errorf("RFB security types unavailable: %w", err))
	}
	types, err := stream.read(int(count[0]))
	if err != nil {
		return fail(err)
	}
	if !bytes.Contains(types, []byte{1}) {
		return fail(fmt.Errorf("RFB None security type unavailable"))
	}
	if err := stream.write([]byte{1}); err != nil {
		return fail(err)
	}
	security, err := stream.read(4)
	if err != nil || !bytes.Equal(security, []byte{0, 0, 0, 0}) {
		return fail(fmt.Errorf("RFB security negotiation failed: %x: %w", security, err))
	}
	if err := stream.write([]byte{1}); err != nil {
		return fail(err)
	}
	serverInit, err := stream.read(24)
	if err != nil {
		return fail(err)
	}
	nameSize := int(binary.BigEndian.Uint32(serverInit[20:24]))
	if _, err := stream.read(nameSize); err != nil {
		return fail(err)
	}
	for _, event := range events {
		if err := stream.write(event); err != nil {
			return fail(err)
		}
	}
	return &InputSession{connection: connection}, nil
}

func rfbInputEvents(withKey bool, x, y int) [][]byte {
	// Send the key while the image's keyboard oracle retains focus, then move
	// the pointer without clicking away that focus. The exact byte sequence
	// remains identical for view and control.
	events := make([][]byte, 0, 3)
	if withKey {
		events = append(events, rfbKeyEvents()...)
	}
	events = append(events, []byte{5, 0, byte(x >> 8), byte(x), byte(y >> 8), byte(y)})
	return events
}

func rfbKeyEvents() [][]byte {
	return [][]byte{
		{4, 1, 0, 0, 0, 0, 0, 'w'},
		{4, 0, 0, 0, 0, 0, 0, 'w'},
	}
}

// The input-capture probe repeats the Seat screen CLI's timeout path with the
// same rfbclient calls (#1484). These values mirror the CLI and the page.
const (
	inputCaptureSettle    = 250 * time.Millisecond // the CLI's settle limit
	inputCaptureRedraw    = 400 * time.Millisecond // the page's delay for key d
	inputCaptureCallBound = 15 * time.Second       // the CLI's call deadline
	inputCaptureSeparate  = 300 * time.Millisecond // red baseline before each trial
	inputCaptureTruthPace = 50 * time.Millisecond  // between ground-truth reads
	inputCaptureTruthSpan = time.Second            // bound on ground-truth reads
	// InputCaptureMinConclusive is the number of conclusive C trials, of six,
	// that a passing proof needs.
	InputCaptureMinConclusive = 4
	// InputCaptureMinReturned is the number of T trials, of three, that must
	// time out and return a capture.
	InputCaptureMinReturned = 2
)

// C is the CLI's timeout capture, held past the redraw; B is a fresh view
// connection (a positive control that shows the probe can see staleness); T is
// the CLI's capture at the moment of the timeout.
var inputCaptureOrder = []string{"C", "B", "B", "C", "T", "C", "B", "B", "C", "T", "C", "B", "B", "C", "T"}

// InputCaptureProbeURL is a disposable colour surface for the actual image's
// Chromium. Launch it in a new window after other image oracles have finished.
// Key r turns the page red at once and cancels a pending change; a turns it
// green at once; d turns it green after 400 ms.
func InputCaptureProbeURL() string {
	page := `<title>Wefty Wayland Computer</title><body style="margin:0;background:#ff0000" tabindex="0"><script>let pending;onkeydown=e=>{const s=document.body.style;if(e.key==='r'){clearTimeout(pending);s.background='#ff0000'}if(e.key==='a')s.background='#00ff00';if(e.key==='d')pending=setTimeout(()=>{s.background='#00ff00'},400)};document.body.focus()</script>`
	return "data:text/html;base64," + base64.StdEncoding.EncodeToString([]byte(page))
}

// InputCaptureProbeLaunchScript launches only the reference image's disposable
// Chromium surface. Its caller opts in and retains the command's exit status.
// The child status file distinguishes shell admission from Chromium exit.
const InputCaptureProbeLaunchScript = `command -v chromium >/dev/null || exit 127
for candidate in "$XDG_RUNTIME_DIR"/wayland-*; do
 if [ -S "$candidate" ]; then export WAYLAND_DISPLAY="${candidate##*/}"; break; fi
done
ozone=
if [ -n "${WAYLAND_DISPLAY:-}" ]; then ozone=--ozone-platform=wayland
elif [ -z "${DISPLAY:-}" ]; then export DISPLAY=:$WEFTY_COMPUTER_VIEW_PORT; fi
root="${TMPDIR:-/tmp}/wefty-input-capture"
rm -f "$root.status"
( chromium $ozone --no-sandbox --disable-gpu --disable-dev-shm-usage --no-first-run --no-default-browser-check --start-fullscreen --user-data-dir="$HOME/.config/chromium" --app="$1" >"$root.log" 2>&1
  result=$?; printf 'chromium_exit=%s\n' "$result" >"$root.status"
) </dev/null >/dev/null 2>&1 &
printf 'launch_pid=%s\n' "$!"`

const InputCaptureProbeDiagnosticsScript = `root="${TMPDIR:-/tmp}/wefty-input-capture"
if [ -f "$root.status" ]; then cat "$root.status"; else echo chromium_exit=pending; fi
if [ -f "$root.log" ]; then tail -c 4096 "$root.log"; else echo chromium_log=unavailable; fi`

type InputCaptureProbeOptions struct {
	// Open returns an RFB client for mode "input" (control) or "view". Its
	// close function must be safe to call once; ctx cancellation closes it.
	Open        func(context.Context, string) (*rfbclient.Client, func(), error)
	Prepare     func(context.Context, int) (string, error)
	Diagnostics func(context.Context) string
	Report      func(string)
	Now         func() time.Time
	Sleep       func(context.Context, time.Duration) error
}

// InputCaptureSetupUnavailableError is missing optional evidence, distinct
// from a stale capture or a measurement error.
type InputCaptureSetupUnavailableError struct{ Err error }

func (e *InputCaptureSetupUnavailableError) Error() string { return e.Err.Error() }
func (e *InputCaptureSetupUnavailableError) Unwrap() error { return e.Err }

// InputCaptureTrial is one independent colour change and its measured frame.
// Times are milliseconds from the end of the key write. Colours are red,
// green or other.
type InputCaptureTrial struct {
	Kind  string `json:"kind"`
	Index int    `json:"index"`
	// C: current, stale or inconclusive. B: fresh or stale. T: returned or
	// inconclusive. Any kind: error.
	Result            string `json:"result"`
	Capture           string `json:"capture,omitempty"`
	Pixels            string `json:"pixels,omitempty"`
	GroundTruth       string `json:"ground_truth,omitempty"`
	GroundTruthPixels string `json:"ground_truth_pixels,omitempty"`
	GroundTruthReads  int    `json:"ground_truth_reads,omitempty"`
	SettleTimedOut    bool   `json:"settle_timed_out,omitempty"`
	TimeoutMS         int64  `json:"timeout_ms,omitempty"`
	CaptureMS         int64  `json:"capture_ms,omitempty"`
	TimeoutToFrameMS  int64  `json:"timeout_to_frame_ms,omitempty"`
	ControlGreenMS    int64  `json:"control_green_ms,omitempty"`
	FirstRequestMS    int64  `json:"first_request_ms,omitempty"`
	Error             string `json:"error,omitempty"`
}

type InputCaptureExperiment struct {
	Trials        []InputCaptureTrial `json:"trials"`
	Summary       string              `json:"summary"`
	SetupAttempts []string            `json:"setup_attempts,omitempty"`
	Error         string              `json:"error,omitempty"`
}

func (r InputCaptureExperiment) count(kind, result string) int {
	count := 0
	for _, trial := range r.Trials {
		if trial.Kind == kind && trial.Result == result {
			count++
		}
	}
	return count
}

// StaleCaptures counts C trials whose capture showed red after the page had
// turned green. Any stale capture fails the proof.
func (r InputCaptureExperiment) StaleCaptures() int { return r.count("C", "stale") }

// ConclusiveCaptures counts C trials that could tell current from stale.
func (r InputCaptureExperiment) ConclusiveCaptures() int {
	return r.count("C", "current") + r.StaleCaptures()
}

// ReturnedTimeoutCaptures counts T trials whose settle timed out and whose
// capture returned a full frame.
func (r InputCaptureExperiment) ReturnedTimeoutCaptures() int { return r.count("T", "returned") }

func (r InputCaptureExperiment) summary() string {
	red, green, maxMS := 0, 0, int64(0)
	for _, trial := range r.Trials {
		if trial.Kind == "T" && trial.Result == "returned" {
			red += boolCount(trial.Capture == "red")
			green += boolCount(trial.Capture == "green")
			maxMS = max(maxMS, trial.TimeoutToFrameMS)
		}
	}
	return fmt.Sprintf("input-capture trials: C current=%d/6 stale=%d/6 inconclusive=%d/6; B (fresh view) stale=%d/6; T returned=%d/3 inconclusive=%d/3 red=%d green=%d max_ms=%d",
		r.count("C", "current"), r.StaleCaptures(), r.count("C", "inconclusive"), r.count("B", "stale"), r.ReturnedTimeoutCaptures(), r.count("T", "inconclusive"), red, green, maxMS)
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

// ProbeInputCapture proves that the CLI's capture after a settle timeout, on
// the input connection, never shows the screen from before a change that is
// already visible. Every trial is logged as one JSON line, and the summary is
// logged before return. Setup retries are bounded; a measurement is never
// retried, and a measurement error ends the probe.
func ProbeInputCapture(ctx context.Context, options InputCaptureProbeOptions) (experiment InputCaptureExperiment, resultErr error) {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Sleep == nil {
		options.Sleep = sleepContext
	}
	report := func(message string) {
		if options.Report != nil {
			options.Report(message)
		}
	}
	diagnostics := func() string {
		if options.Diagnostics == nil {
			return ""
		}
		diagnosticCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		return options.Diagnostics(diagnosticCtx)
	}
	defer func() {
		if resultErr != nil {
			experiment.Error = resultErr.Error()
		}
		experiment.Summary = experiment.summary()
		report(experiment.Summary) // Always print accumulated data before callers fail.
	}()
	attemptNumber := 0
	for index, kind := range inputCaptureOrder {
		var control *rfbclient.Client
		var closeControl func()
		var trialCtx context.Context
		var cancelTrial context.CancelFunc
		var setupErr error
		for attempt := 1; attempt <= 3; attempt++ {
			attemptNumber++
			trialCtx, cancelTrial = context.WithCancel(ctx)
			setupCtx, cancelSetup := context.WithTimeout(trialCtx, 8*time.Second)
			// Bound Dial and setup I/O, then remove the setup deadline on selection.
			// The selected control must remain alive for the whole measurement.
			stopSetup := context.AfterFunc(setupCtx, cancelTrial)
			var detail string
			control, closeControl, detail, setupErr = prepareInputCaptureControl(setupCtx, trialCtx, options, attemptNumber, index == 0 || attempt > 1)
			stopped := stopSetup()
			if setupErr == nil && !stopped {
				setupErr = setupCtx.Err()
			}
			cancelSetup()
			message := fmt.Sprintf("trial=%d kind=%s setup attempt=%d: %s", index+1, kind, attempt, detail)
			if setupErr != nil {
				if closeControl != nil {
					closeControl()
				}
				cancelTrial()
				message += fmt.Sprintf("; error=%v; %s", setupErr, diagnostics())
			}
			experiment.SetupAttempts = append(experiment.SetupAttempts, message)
			report(message)
			if setupErr == nil || ctx.Err() != nil {
				break
			}
		}
		if setupErr != nil {
			return experiment, &InputCaptureSetupUnavailableError{Err: fmt.Errorf("input-capture setup unavailable: %w", setupErr)}
		}
		trial, err := measureInputCaptureTrial(trialCtx, options, control, closeControl, kind, index+1)
		cancelTrial()
		if err != nil {
			if trial.Result == "" { // Keep a result already recorded, such as T's before cleanup.
				trial.Result = "error"
			}
			trial.Error = fmt.Sprintf("%v; %s", err, diagnostics())
		}
		experiment.Trials = append(experiment.Trials, trial)
		payload, _ := json.Marshal(trial)
		report("input-capture trial: " + string(payload))
		if err != nil {
			return experiment, fmt.Errorf("trial %d kind=%s measurement failed (no retry): %s", index+1, kind, trial.Error)
		}
	}
	return experiment, nil
}

func prepareInputCaptureControl(setupCtx, connectionCtx context.Context, options InputCaptureProbeOptions, attempt int, launch bool) (control *rfbclient.Client, closeControl func(), detail string, resultErr error) {
	var notes []string
	defer func() { detail = strings.Join(notes, "; ") }()
	if launch && options.Prepare != nil {
		note, err := options.Prepare(setupCtx, attempt)
		notes = append(notes, note)
		if err != nil {
			return nil, nil, "", err
		}
	}
	control, closeControl, resultErr = options.Open(connectionCtx, "input")
	if resultErr != nil {
		return nil, nil, "", resultErr
	}
	if launch {
		pixels, err := waitProbeColour(setupCtx, options, control, color.RGBA{R: 255, A: 255})
		notes = append(notes, "control ready pixels="+pixels)
		if err != nil {
			return control, closeControl, "", err
		}
	}
	if err := control.Pointer(control.Width/2, control.Height/2, 1); err != nil {
		return control, closeControl, "", err
	}
	if err := control.Pointer(control.Width/2, control.Height/2, 0); err != nil {
		return control, closeControl, "", err
	}
	if err := tapProbeKey(control, 'r'); err != nil {
		return control, closeControl, "", err
	}
	pixels, err := waitProbeColour(setupCtx, options, control, color.RGBA{R: 255, A: 255})
	notes = append(notes, "control red pixels="+pixels)
	return control, closeControl, "", err
}

func tapProbeKey(control *rfbclient.Client, key uint32) error {
	if err := control.Key(key, true); err != nil {
		return err
	}
	return control.Key(key, false)
}

func waitProbeColour(ctx context.Context, options InputCaptureProbeOptions, control *rfbclient.Client, want color.RGBA) (string, error) {
	pixels := "unavailable"
	for {
		frame, err := control.Frame()
		if err != nil {
			return pixels, fmt.Errorf("control observer: %w; last pixels=%s", err, pixels)
		}
		pixels = probeFrameSamples(frame)
		if probeFrameColour(frame, want) {
			return pixels, nil
		}
		if err := options.Sleep(ctx, 20*time.Millisecond); err != nil {
			return pixels, fmt.Errorf("control observer: %w; last pixels=%s", err, pixels)
		}
	}
}

// measureInputCaptureTrial owns closeControl and calls it exactly once.
func measureInputCaptureTrial(ctx context.Context, options InputCaptureProbeOptions, control *rfbclient.Client, closeControl func(), kind string, index int) (trial InputCaptureTrial, resultErr error) {
	trial.Kind, trial.Index = kind, index
	sleepUntil := func(at time.Time) error {
		if wait := at.Sub(options.Now()); wait > 0 {
			return options.Sleep(ctx, wait)
		}
		return nil
	}
	if kind == "B" {
		defer closeControl()
		if err := options.Sleep(ctx, inputCaptureSeparate); err != nil {
			return trial, err
		}
		tapped := options.Now()
		if err := tapProbeKey(control, 'a'); err != nil {
			return trial, err
		}
		if _, err := waitProbeColour(ctx, options, control, color.RGBA{G: 255, A: 255}); err != nil {
			return trial, err
		}
		trial.ControlGreenMS = options.Now().Sub(tapped).Milliseconds()
		if err := options.Sleep(ctx, inputCaptureSettle); err != nil {
			return trial, err
		}
		view, closeView, err := options.Open(ctx, "view")
		if err != nil {
			return trial, err
		}
		defer closeView()
		trial.FirstRequestMS = options.Now().Sub(tapped).Milliseconds()
		frame, err := view.Frame() // The only view request in this trial; never retry it.
		if err != nil {
			return trial, err
		}
		trial.Capture, trial.Pixels = probeColourName(frame), probeFrameSamples(frame)
		trial.Result = "stale"
		if trial.Capture == "green" {
			trial.Result = "fresh"
		}
		return trial, nil
	}
	// C and T: one new input connection, the only one to the backend, used in
	// the CLI's order with the CLI's calls and bound.
	closeControl()
	if err := options.Sleep(ctx, inputCaptureSeparate); err != nil {
		return trial, err
	}
	callCtx, cancel := context.WithTimeout(ctx, inputCaptureCallBound)
	defer cancel()
	input, closeInput, err := options.Open(callCtx, "input")
	if err != nil {
		return trial, err
	}
	defer closeInput()
	baseline, err := input.Frame()
	if err != nil {
		return trial, fmt.Errorf("baseline: %w", err)
	}
	if err := tapProbeKey(input, 'd'); err != nil {
		return trial, err
	}
	written := options.Now()
	changed, err := input.SyncAfterInput(baseline.RGBAAt(0, 0))
	if err != nil {
		return trial, fmt.Errorf("sync: %w", err)
	}
	if remaining := inputCaptureSettle - options.Now().Sub(written); !changed && remaining > 0 {
		if changed, err = input.WaitForChange(remaining); err != nil {
			return trial, fmt.Errorf("settle: %w", err)
		}
	}
	timedOut := options.Now()
	trial.SettleTimedOut, trial.TimeoutMS = !changed, timedOut.Sub(written).Milliseconds()
	if kind == "C" {
		// Keep the incremental request outstanding past the redraw.
		if err := sleepUntil(written.Add(inputCaptureRedraw + inputCaptureSettle)); err != nil {
			return trial, err
		}
	}
	trial.CaptureMS = options.Now().Sub(written).Milliseconds()
	frame, err := input.FrameAfterWait(nil) // The measured capture; never retried.
	if err != nil {
		return trial, fmt.Errorf("capture: %w", err)
	}
	trial.TimeoutToFrameMS = options.Now().Sub(timedOut).Milliseconds()
	trial.Capture, trial.Pixels = probeColourName(frame), probeFrameSamples(frame)
	if kind == "T" {
		// T's result comes from its capture alone. Red and green are both
		// correct at the timeout. Let the page's pending change finish before
		// the deferred close and the next trial.
		trial.Result = "returned"
		if !trial.SettleTimedOut {
			trial.Result = "inconclusive" // The timeout path did not run.
		}
		if err := sleepUntil(written.Add(inputCaptureRedraw + inputCaptureSettle)); err != nil {
			return trial, fmt.Errorf("cleanup after the capture: %w", err)
		}
		return trial, nil
	}
	// Ground truth: an observation on the same connection after the redraw is
	// due (the capture waited until 650 ms; the page redraws at 400 ms). A reply
	// left over from the capture's requests may answer the first read, so read
	// until green or the bound; never reread the capture. The 1 s bound is soft:
	// it is checked between reads, and one read can block until the call's
	// context ends. A red capture followed by green ground truth is stale. That
	// is the conservative direction: if the page redrew later than 650 ms, a
	// correct red capture is still reported stale, never the reverse.
	if err := sleepUntil(written.Add(inputCaptureRedraw + inputCaptureSettle)); err != nil {
		return trial, err
	}
	deadline := options.Now().Add(inputCaptureTruthSpan)
	for {
		truth, err := input.Frame()
		if err != nil {
			return trial, fmt.Errorf("ground truth: %w", err)
		}
		trial.GroundTruthReads++
		trial.GroundTruth, trial.GroundTruthPixels = probeColourName(truth), probeFrameSamples(truth)
		if trial.GroundTruth == "green" || !options.Now().Before(deadline) {
			break
		}
		if err := options.Sleep(ctx, inputCaptureTruthPace); err != nil {
			return trial, err
		}
	}
	switch {
	case !trial.SettleTimedOut || trial.GroundTruth != "green":
		trial.Result = "inconclusive"
	case trial.Capture == "green":
		trial.Result = "current"
	case trial.Capture == "red":
		trial.Result = "stale"
	default:
		trial.Result = "inconclusive"
	}
	return trial, nil
}

func probeColourName(frame *image.RGBA) string {
	switch {
	case probeFrameColour(frame, color.RGBA{G: 255, A: 255}):
		return "green"
	case probeFrameColour(frame, color.RGBA{R: 255, A: 255}):
		return "red"
	}
	return "other"
}

func probeFrameSamples(frame *image.RGBA) string {
	var samples []string
	for _, x := range []int{frame.Bounds().Dx() / 3, frame.Bounds().Dx() * 2 / 3} {
		for _, y := range []int{frame.Bounds().Dy() / 3, frame.Bounds().Dy() * 2 / 3} {
			pixel := frame.RGBAAt(x, y)
			samples = append(samples, fmt.Sprintf("(%d,%d)=%02x%02x%02x/%02x", x, y, pixel.R, pixel.G, pixel.B, pixel.A))
		}
	}
	return strings.Join(samples, ",")
}

func probeFrameColour(frame *image.RGBA, want color.RGBA) bool {
	// Require a broad patch, away from browser chrome and the pointer.
	for _, x := range []int{frame.Bounds().Dx() / 3, frame.Bounds().Dx() * 2 / 3} {
		for _, y := range []int{frame.Bounds().Dy() / 3, frame.Bounds().Dy() * 2 / 3} {
			if frame.RGBAAt(x, y) != want {
				return false
			}
		}
	}
	return true
}

func openFrameProbe(ctx context.Context, port int) (*rfbclient.Client, func(), error) {
	ws, _, err := websocket.Dial(ctx, fmt.Sprintf("ws://127.0.0.1:%d%s", port, contract.ComputerDisplayWebSocketPath), &websocket.DialOptions{Subprotocols: []string{contract.ComputerDisplayWebSocketSubprotocol}})
	if err != nil {
		return nil, nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = ws.CloseNow() })
	close := func() { stop(); _ = ws.CloseNow() }
	ws.SetReadLimit(128 << 20)
	client, err := rfbclient.Open(websocket.NetConn(ctx, ws, websocket.MessageBinary))
	if err != nil {
		close()
		return nil, nil, err
	}
	return client, close, nil
}
