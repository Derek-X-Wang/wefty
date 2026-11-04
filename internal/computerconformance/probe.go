package computerconformance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1" // #nosec G505 -- RFC 6455 mandates SHA-1 for the public handshake nonce.
	"encoding/base64"
	"encoding/binary"
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

// HeldViewProbeURL is a disposable colour surface for the actual image's
// Chromium. Launch it in a new window after other image oracles have finished.
func HeldViewProbeURL() string {
	page := `<title>Wefty Wayland Computer</title><body style="margin:0;background:#ff0000" tabindex="0"><script>onkeydown=e=>{if(e.key==='r')document.body.style.background='#ff0000';if('abc'.includes(e.key))setTimeout(()=>document.body.style.background='#00ff00',({a:0,b:225,c:300})[e.key])};document.body.focus()</script>`
	return "data:text/html;base64," + base64.StdEncoding.EncodeToString([]byte(page))
}

// HeldViewProbeLaunchScript launches only the reference image's disposable
// Chromium surface. Its caller opts in and retains the command's exit status.
// The child status file distinguishes shell admission from Chromium exit.
const HeldViewProbeLaunchScript = `command -v chromium >/dev/null || exit 127
for candidate in "$XDG_RUNTIME_DIR"/wayland-*; do
 if [ -S "$candidate" ]; then export WAYLAND_DISPLAY="${candidate##*/}"; break; fi
done
ozone=
if [ -n "${WAYLAND_DISPLAY:-}" ]; then ozone=--ozone-platform=wayland
elif [ -z "${DISPLAY:-}" ]; then export DISPLAY=:$WEFTY_COMPUTER_VIEW_PORT; fi
root="${TMPDIR:-/tmp}/wefty-held-view"
rm -f "$root.status"
( chromium $ozone --no-sandbox --disable-gpu --disable-dev-shm-usage --no-first-run --no-default-browser-check --start-fullscreen --user-data-dir="$HOME/.config/chromium" --app="$1" >"$root.log" 2>&1
  result=$?; printf 'chromium_exit=%s\n' "$result" >"$root.status"
) </dev/null >/dev/null 2>&1 &
printf 'launch_pid=%s\n' "$!"`

const HeldViewProbeDiagnosticsScript = `root="${TMPDIR:-/tmp}/wefty-held-view"
if [ -f "$root.status" ]; then cat "$root.status"; else echo chromium_exit=pending; fi
if [ -f "$root.log" ]; then tail -c 4096 "$root.log"; else echo chromium_log=unavailable; fi`

type HeldViewProbeOptions struct {
	Open        func(context.Context, string) (*rfbclient.Client, func(), error)
	Prepare     func(context.Context, int) (string, error)
	Diagnostics func(context.Context) string
	Report      func(string)
}

// HeldViewSetupUnavailableError is optional evidence, distinct from a failed
// first-frame assertion. Reference-image publication still requires PASS.
type HeldViewSetupUnavailableError struct{ Err error }

func (e *HeldViewSetupUnavailableError) Error() string { return e.Err.Error() }
func (e *HeldViewSetupUnavailableError) Unwrap() error { return e.Err }

// HeldViewObservation records requested delays separately from observed pixel
// times. A delayed application redraw is not a screenshot freshness proof.
type HeldViewObservation struct {
	RedrawDelayMS    int      `json:"redraw_delay_ms"`
	ChangeObservedMS int64    `json:"change_observed_ms"`
	FirstRequestMS   int64    `json:"first_request_ms"`
	SetupAttempts    []string `json:"setup_attempts"`
	started          time.Time
}

// ProbeHeldViewFrames uses a separate view observer to establish both colours
// on the backend being tested. Setup may retry at most three times per case.
// The selected held connection's first-frame assertion is NEVER retried.
func ProbeHeldViewFrames(ctx context.Context, options HeldViewProbeOptions) ([]HeldViewObservation, error) {
	var observations []HeldViewObservation
	attemptNumber := 0
	diagnostics := func() string {
		if options.Diagnostics == nil {
			return ""
		}
		diagnosticCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		return options.Diagnostics(diagnosticCtx)
	}
	report := func(message string) {
		if options.Report != nil {
			options.Report(message)
		}
	}
	for i, delay := range []int{0, 225, 300} {
		var attempts []string
		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			attemptNumber++
			setupCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			held, closeHeld, observation, detail, err := prepareHeldView(setupCtx, options, uint32('a'+i), delay, attemptNumber, i == 0 || attempt > 1)
			if err != nil {
				cancel()
				lastErr = err
				message := fmt.Sprintf("redraw=%dms setup attempt=%d: %s; error=%v; %s", delay, attempt, detail, err, diagnostics())
				attempts = append(attempts, message)
				report(message)
				if ctx.Err() != nil {
					break
				}
				continue
			}
			message := fmt.Sprintf("redraw=%dms setup attempt=%d ready: %s", delay, attempt, detail)
			attempts = append(attempts, message)
			report(message)
			observation.SetupAttempts = attempts
			observation.FirstRequestMS = time.Since(observation.started).Milliseconds()
			// No request has been sent on held. This read and assertion are outside
			// the retryable setup; stale pixels or a read failure fail immediately.
			frame, frameErr := held.Frame()
			closeHeld()
			cancel()
			observations = append(observations, observation)
			if frameErr != nil {
				return observations, fmt.Errorf("held first frame failed (no retry): %w; %s; %s", frameErr, strings.Join(attempts, "; "), diagnostics())
			}
			if !probeFrameColour(frame, color.RGBA{G: 255, A: 255}) {
				return observations, fmt.Errorf("held first frame stale (no retry), redraw=%dms pixels=%s; %s; %s", delay, probeFrameSamples(frame), strings.Join(attempts, "; "), diagnostics())
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			return observations, &HeldViewSetupUnavailableError{Err: fmt.Errorf("held-view setup unavailable: %w; %s", lastErr, strings.Join(attempts, "; "))}
		}
	}
	return observations, nil
}

func prepareHeldView(ctx context.Context, options HeldViewProbeOptions, key uint32, delay, attempt int, launch bool) (held *rfbclient.Client, closeHeld func(), observation HeldViewObservation, detail string, resultErr error) {
	var notes []string
	defer func() { detail = strings.Join(notes, "; ") }()
	if launch && options.Prepare != nil {
		note, err := options.Prepare(ctx, attempt)
		notes = append(notes, note)
		if err != nil {
			return nil, nil, observation, "", err
		}
	}
	control, closeControl, err := options.Open(ctx, "input")
	if err != nil {
		return nil, nil, observation, "", err
	}
	defer closeControl()
	observer, closeObserver, err := options.Open(ctx, "view")
	if err != nil {
		return nil, nil, observation, "", err
	}
	defer closeObserver()
	tap := func(sym uint32) error {
		if err := control.Key(sym, true); err != nil {
			return err
		}
		return control.Key(sym, false)
	}
	waitColour := func(want color.RGBA) error {
		pixels := "unavailable"
		for {
			frame, err := observer.Frame()
			if err != nil {
				return fmt.Errorf("view observer: %w; last pixels=%s", err, pixels)
			}
			pixels = probeFrameSamples(frame)
			if probeFrameColour(frame, want) {
				notes = append(notes, "view observer pixels="+pixels)
				return nil
			}
			if err := sleepContext(ctx, 20*time.Millisecond); err != nil {
				return fmt.Errorf("view observer: %w; last pixels=%s", err, pixels)
			}
		}
	}
	red := color.RGBA{R: 255, A: 255}
	if launch {
		if err := waitColour(red); err != nil {
			return nil, nil, observation, "", err
		}
	}
	if err := control.Pointer(control.Width/2, control.Height/2, 1); err != nil {
		return nil, nil, observation, "", err
	}
	if err := control.Pointer(control.Width/2, control.Height/2, 0); err != nil {
		return nil, nil, observation, "", err
	}
	if err := tap('r'); err != nil {
		return nil, nil, observation, "", err
	}
	// Red is confirmed on view BEFORE the held connection is opened.
	if err := waitColour(red); err != nil {
		return nil, nil, observation, "", err
	}
	held, closeHeld, err = options.Open(ctx, "view")
	if err != nil {
		return nil, nil, observation, "", err
	}
	defer func() {
		if resultErr != nil {
			closeHeld()
			held, closeHeld = nil, nil
		}
	}()
	started := time.Now()
	if err := tap(key); err != nil {
		return nil, closeHeld, observation, "", err
	}
	if err := sleepContext(ctx, 250*time.Millisecond); err != nil {
		return nil, closeHeld, observation, "", err
	}
	// Observe GREEN on the SAME VIEW BACKEND through a separate connection.
	// The held connection remains request-free throughout this observer wait.
	if err := waitColour(color.RGBA{G: 255, A: 255}); err != nil {
		return nil, closeHeld, observation, "", err
	}
	observation = HeldViewObservation{RedrawDelayMS: delay, ChangeObservedMS: time.Since(started).Milliseconds(), started: started}
	return held, closeHeld, observation, "", nil
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
