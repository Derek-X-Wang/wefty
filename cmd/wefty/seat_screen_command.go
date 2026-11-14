package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/rfbclient"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/coder/websocket"
)

// The owner bounded the post-input wait so a click that does not redraw
// still meets SC-1 (wefty's part of a click and its screenshot within 1 s).
const seatScreenSettleLimit = 250 * time.Millisecond

const seatScreenUsage = `usage: wefty seat screenshot SEAT_ID [-o FILE]
       wefty seat click SEAT_ID X Y [--button left|right|double] [--screenshot [FILE]]
       wefty seat move SEAT_ID X Y [--screenshot [FILE]]
       wefty seat drag SEAT_ID X1 Y1 X2 Y2 [--screenshot [FILE]]
       wefty seat scroll SEAT_ID X Y --dy N [--dx N] [--screenshot [FILE]]
       wefty seat type SEAT_ID TEXT [--screenshot [FILE]]
       wefty seat key SEAT_ID COMBO [--screenshot [FILE]]

Coordinates are pixels on a 1280x800 screen, starting at the top left.
Positive dy scrolls down; positive dx scrolls right; one unit is one wheel tick.
Keys: Return (Enter), Tab, Escape (Esc), BackSpace, Delete, Insert, Home,
End, PageUp, PageDown, Left, Right, Up, Down, space, plus, F1-F12, or one
Unicode character. Join ctrl/control, shift, alt, super, meta with +:
ctrl+l, Return, shift+Tab. Text is UTF-8, without a clipboard or shell.
TEXT is literal even when it starts with -. Put flags before SEAT_ID and TEXT, or use --.
Screenshots alone use the view path, including while a person drives.
Input then returns person_driving (exit 5): a person is driving.
Default PNG files go to the system temp folder. --json includes elapsed_ms.
`

type seatScreenStartKey struct{}

// seatScreenWriteHookKey is a test seam: a func([]byte) in the context runs
// before each write on a Seat screen stream.
type seatScreenWriteHookKey struct{}

// seatScreenStreamError marks a failure of the Seat screen stream itself, as
// opposed to a local file, encoding, output or RFB protocol error.
type seatScreenStreamError struct{ err error }

func (e *seatScreenStreamError) Error() string { return e.err.Error() }
func (e *seatScreenStreamError) Unwrap() error { return e.err }

type seatScreenStreamWriter struct {
	io.Writer
	hook func([]byte)
}

func (w seatScreenStreamWriter) Write(p []byte) (int, error) {
	if w.hook != nil {
		w.hook(p)
	}
	n, err := w.Writer.Write(p)
	if err != nil {
		err = &seatScreenStreamError{err}
	}
	return n, err
}

// seatScreenReceiveLimit bounds one WebSocket message and the bytes the
// receiver holds ahead of the RFB client. A full 1280x800 frame is 4 MiB.
const seatScreenReceiveLimit = 128 << 20

// seatScreenReceiver reads every WebSocket frame as it arrives, so a take's
// typed close (#1680) is seen even while the CLI only writes input. RFB reads
// drain its buffer; the first read error ends it. It holds at most limit
// bytes; a node that sends more ends the stream with an error.
type seatScreenReceiver struct {
	mu   sync.Mutex
	more *sync.Cond
	buf  bytes.Buffer
	err  error
	done chan struct{}
}

// receiveSeatScreen must run after websocket.NetConn, which removes the
// connection's read limit.
func receiveSeatScreen(ctx context.Context, c *websocket.Conn, limit int) *seatScreenReceiver {
	r := &seatScreenReceiver{done: make(chan struct{})}
	r.more = sync.NewCond(&r.mu)
	c.SetReadLimit(int64(limit))
	go func() {
		defer close(r.done)
		for {
			typ, data, err := c.Read(ctx)
			if err == nil && typ != websocket.MessageBinary {
				err = fmt.Errorf("Seat screen sent a non-binary WebSocket message")
				_ = c.CloseNow()
			}
			r.mu.Lock()
			if err == nil && r.buf.Len()+len(data) > limit {
				err = fmt.Errorf("Seat screen sent more than %d bytes ahead of the client", limit)
				_ = c.CloseNow()
			}
			if err != nil {
				r.err = err
			} else {
				r.buf.Write(data)
			}
			r.more.Broadcast()
			r.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	return r
}

func (r *seatScreenReceiver) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.buf.Len() == 0 && r.err == nil {
		r.more.Wait()
	}
	if r.buf.Len() > 0 {
		return r.buf.Read(p)
	}
	switch websocket.CloseStatus(r.err) {
	case websocket.StatusNormalClosure, websocket.StatusGoingAway:
		return 0, io.EOF
	}
	return 0, r.err
}

// seatScreenTakeFailure reports a take's typed close (#1680) as the
// person_driving refusal. A stream write can fail first, as a broken pipe,
// once the node drops the stream; the receiver then holds the close. Other
// failures keep their own error. Call it after closing the connection, which
// ends the receiver.
func seatScreenTakeFailure(err error, r *seatScreenReceiver) error {
	taken := websocket.CloseStatus(err) == contract.SeatInputPersonDrivingClose
	var stream *seatScreenStreamError
	if !taken && r != nil && errors.As(err, &stream) {
		select {
		case <-r.done:
			taken = websocket.CloseStatus(r.err) == contract.SeatInputPersonDrivingClose
		case <-time.After(time.Second):
		}
	}
	if !taken {
		return err
	}
	return &apiResponseError{StatusCode: http.StatusConflict, APIError: contract.APIError{Code: seatErrorPersonDriving, Message: "a person is driving"}}
}

type seatScreenArgs struct {
	command, id, output, button, text string
	screenshot                        bool
	x, y, x2, y2, dy, dx              int
	keys                              []uint32
}

func isSeatScreenCommand(s string) bool {
	switch s {
	case "screenshot", "click", "move", "drag", "scroll", "type", "key":
		return true
	}
	return false
}

// Normalize the optional screenshot value before the global JSON/help scanners.
// Attached values permit filenames beginning with '-'; a bare flag uses temp.
func normalizeSeatScreenArgs(args []string) []string {
	result := append([]string(nil), args...)
	values := map[string]bool{}
	for _, name := range strings.Fields(jsonValueFlags) {
		values[name] = true
	}
	for i := 0; i < len(result); i++ {
		arg := result[i]
		if arg == "--" {
			return result
		}
		if strings.HasPrefix(arg, "-") {
			if flagArgumentTakesValue(arg, nil, values) {
				i++
			}
			continue
		}
		// Only the actual root command can select screen flag semantics.
		if arg != "seat" || i+1 >= len(result) || !isSeatScreenCommand(result[i+1]) {
			return result
		}
		positionals, flags := seatScreenParts(result[i+1], result[i+2:])
		result = append(result[:i+2], append(positionals, flags...)...)
		return result
	}
	return result
}

// Includes the Seat ID. The shell has already grouped TEXT into one argument.
func seatScreenPositionals(command string) int {
	switch command {
	case "click", "move", "scroll":
		return 3
	case "drag":
		return 5
	case "type", "key":
		return 2
	}
	return 1
}

// Required arguments become literal once the Seat ID begins. Options can
// precede them, or follow all required arguments. -- starts literal arguments.
func seatScreenParts(command string, args []string) (positionals, flags []string) {
	required := seatScreenPositionals(command)
	values := map[string]bool{}
	for _, name := range strings.Fields(jsonValueFlags) {
		values[name] = true
	}
	literal := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if literal || len(positionals) > 0 && len(positionals) < required {
			positionals = append(positionals, arg)
			continue
		}
		if arg == "--" {
			literal = true
			continue
		}
		if !strings.HasPrefix(arg, "-") {
			positionals = append(positionals, arg)
			continue
		}
		if arg == "--screenshot" {
			value := ""
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") && (len(positionals) > 0 || !seatCLIIdentifier.MatchString(args[i+1])) {
				i++
				value = args[i]
			}
			flags = append(flags, "--screenshot="+value)
		} else {
			flags = append(flags, arg)
			if flagArgumentTakesValue(arg, nil, values) && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
		}
	}
	return
}

func seatScreenWantsHelp(args []string) (bool, bool) {
	if len(args) < 2 || args[0] != "seat" || !isSeatScreenCommand(args[1]) {
		return false, false
	}
	_, flags := seatScreenParts(args[1], args[2:])
	for i := 0; i < len(flags); i++ {
		if isHelpFlag(flags[i]) {
			return true, true
		}
		if flags[i] == "-o" || flags[i] == "--o" || flags[i] == "--button" || flags[i] == "-button" || flags[i] == "--dy" || flags[i] == "-dy" || flags[i] == "--dx" || flags[i] == "-dx" {
			i++
		}
	}
	return false, true
}

func parseSeatScreenArgs(args []string) (seatScreenArgs, error) {
	var a seatScreenArgs
	fail := func() (seatScreenArgs, error) { return a, usageError(seatScreenUsage) }
	if len(args) < 2 || !isSeatScreenCommand(args[0]) {
		return fail()
	}
	a.command = args[0]
	positionals, flagArgs := seatScreenParts(a.command, args[1:])
	if len(positionals) != seatScreenPositionals(a.command) || !seatCLIIdentifier.MatchString(positionals[0]) {
		return fail()
	}
	a.id = positionals[0]
	pos := positionals[1:]
	flags := flag.NewFlagSet("seat "+a.command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	if a.command == "screenshot" {
		flags.StringVar(&a.output, "o", "", "PNG destination")
	} else {
		flags.Func("screenshot", "optional PNG destination", func(value string) error { a.screenshot = true; a.output = value; return nil })
	}
	if a.command == "click" {
		flags.StringVar(&a.button, "button", "left", "left, right or double")
	}
	if a.command == "scroll" {
		flags.IntVar(&a.dy, "dy", 0, "vertical wheel ticks (required)")
		flags.IntVar(&a.dx, "dx", 0, "horizontal wheel ticks")
	}
	if err := flags.Parse(flagArgs); err != nil || flags.NArg() != 0 {
		return fail()
	}
	if a.command == "click" && a.button != "left" && a.button != "right" && a.button != "double" {
		return fail()
	}
	if a.command == "scroll" {
		dySet := false
		flags.Visit(func(f *flag.Flag) {
			if f.Name == "dy" {
				dySet = true
			}
		})
		if !dySet || a.dy < -10000 || a.dy > 10000 || a.dx < -10000 || a.dx > 10000 {
			return fail()
		}
	}
	if a.command == "type" || a.command == "key" {
		a.text = pos[0]
		var err error
		if len(a.text) == 0 || len(a.text) > 1<<20 || !utf8.ValidString(a.text) {
			return fail()
		}
		if a.command == "type" {
			a.keys, err = rfbclient.TextKeys(a.text)
		} else {
			a.keys, err = rfbclient.ComboKeys(a.text)
		}
		if err != nil {
			return a, usageError(seatScreenUsage)
		}
	} else {
		values := []*int{&a.x, &a.y, &a.x2, &a.y2}
		for i, value := range pos {
			number, err := strconv.Atoi(value)
			bound := 1280
			if i%2 == 1 {
				bound = 800
			}
			if err != nil || number < 0 || number >= bound {
				return fail()
			}
			*values[i] = number
		}
	}
	return a, nil
}

func executeSeatScreen(ctx context.Context, clients *apiClients, jsonOutput bool, args []string, stdout, stderr io.Writer) (resultErr error) {
	start, ok := ctx.Value(seatScreenStartKey{}).(time.Time)
	if !ok {
		start = time.Now()
	}
	elapsed := func() float64 { return float64(time.Since(start).Microseconds()) / 1000 }
	timing := newSeatScreenTiming(start)
	a, err := parseSeatScreenArgs(args)
	if err != nil {
		return err
	}
	// Reserve the destination before input: a local refusal must never repeat an action.
	var capture *os.File
	path := ""
	captured := false
	if a.command == "screenshot" || a.screenshot {
		if a.output == "" {
			capture, err = os.CreateTemp("", "wefty-"+a.id+"-*.png")
		} else {
			path, err = filepath.Abs(a.output)
			if err == nil {
				capture, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
			}
		}
		if err != nil {
			return err
		}
		path = capture.Name()
		defer func() {
			if !captured {
				_ = capture.Close()
				_ = os.Remove(path)
			}
		}()
	}
	secret, err := loadSeatCredential(a.id)
	if err != nil {
		return err
	}
	if secret == "" {
		return seatCredentialUnavailable(a.id)
	}
	var target struct {
		Endpoint string `json:"endpoint"`
		Unread   int    `json:"unread_events"`
	}
	if err = clients.l1.do(ctx, http.MethodGet, "/v1/seats/"+a.id+"/screen-target", nil, http.Header{l1.SeatCredentialHeader: []string{secret}}, &target, http.StatusOK); err != nil {
		return err
	}
	// Screen input is never retried: an interrupted call may already have acted.
	ctx = withoutClientRetry(ctx)
	timing.mark("l1_discovery")
	unread := target.Unread
	actionDone, replied := false, false
	var receiver *seatScreenReceiver
	phase := "input synchronization"
	defer func() {
		if resultErr != nil {
			resultErr = seatScreenTakeFailure(resultErr, receiver)
			if actionDone {
				prefix := a.command + " done; " + phase + " failed: "
				var api *apiResponseError
				if errors.As(resultErr, &api) {
					copy := *api
					copy.APIError.Message = prefix + api.APIError.Message
					resultErr = &copy
				} else {
					resultErr = &apiResponseError{APIError: contract.APIError{Code: contract.ErrorInternal, Message: prefix + resultErr.Error()}}
				}
			}
			var api *apiResponseError
			if errors.As(resultErr, &api) {
				copy := *api
				copy.APIError.Details = map[string]any{}
				for k, v := range api.APIError.Details {
					copy.APIError.Details[k] = v
				}
				copy.APIError.Details["elapsed_ms"] = elapsed()
				if timing != nil {
					copy.APIError.Details["debug"] = timing.debug()
				}
				if actionDone {
					copy.APIError.Details["action_done"] = true
					copy.APIError.Retryable = false
				}
				resultErr = &copy
			}
			if !replied {
				resultErr = withSeatReply(stdout, jsonOutput, a.id, unread, func(io.Writer) error { return resultErr })
			}
		}
	}()
	endpoint, err := url.Parse(target.Endpoint)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Scheme != "ws" && endpoint.Scheme != "wss" {
		return fmt.Errorf("L1 returned an invalid Seat screen endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	open := func(mode string) (*rfbclient.Client, func(), error) {
		u := *endpoint
		u.Path = "/v1/seats/" + a.id + "/screen/" + mode
		u.RawPath = ""
		// Use the same Fabric-backed HTTP transport as copies and take-over. The
		// legacy takeover.Open also consumes its admission banner and requires a
		// person bearer, so it cannot open a credential-bound Seat stream.
		node := newAPIClient("hosting node", "l1", clients.fabric, endpoint.Host)
		if transport, ok := node.client.Transport.(*http.Transport); ok {
			defer transport.CloseIdleConnections()
		}
		node.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		c, response, e := websocket.Dial(ctx, u.String(), &websocket.DialOptions{HTTPClient: node.client, HTTPHeader: http.Header{l1.SeatCredentialHeader: []string{secret}}, Subprotocols: []string{"binary"}})
		timing.mark("door_dial")
		if e != nil {
			if response != nil {
				defer response.Body.Close()
				var failure contract.ErrorResponse
				if decode := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&failure); decode == nil && failure.Error.Code != "" {
					return nil, nil, &apiResponseError{StatusCode: response.StatusCode, APIError: failure.Error}
				}
			}
			return nil, nil, e
		}
		close := func() { _ = c.CloseNow() }
		if mode == "view" || capture != nil {
			timing.mark("screenshot_connection_open")
		}
		if c.Subprotocol() != "binary" {
			close()
			return nil, nil, fmt.Errorf("Seat screen did not negotiate binary WebSocket")
		}
		if count, e := strconv.Atoi(response.Header.Get("Wefty-Seat-Unread-Events")); e == nil && count >= 0 {
			unread = count
		}
		if encoded := response.Header.Get("Wefty-Seat-Release-Note"); encoded != "" {
			decoded, e := base64.RawURLEncoding.DecodeString(encoded)
			if e != nil || !utf8.Valid(decoded) || len(decoded) > 8192 {
				close()
				return nil, nil, fmt.Errorf("invalid Seat release note")
			}
			if _, e = fmt.Fprintln(stderr, string(decoded)); e != nil {
				close()
				return nil, nil, e
			}
		}
		hook, _ := ctx.Value(seatScreenWriteHookKey{}).(func([]byte))
		writer := seatScreenStreamWriter{websocket.NetConn(ctx, c, websocket.MessageBinary), hook}
		receiver = receiveSeatScreen(ctx, c, seatScreenReceiveLimit)
		wire := &seatScreenBannerWire{ReadWriter: struct {
			io.Reader
			io.Writer
		}{receiver, writer}, remaining: 12, received: func() { timing.mark("rfb_banner") }}
		client, e := rfbclient.Open(wire)
		if e != nil {
			close()
			return nil, nil, e
		}
		return client, close, nil
	}
	mode := "view"
	if a.command != "screenshot" {
		mode = "input"
	}
	client, close, err := open(mode)
	if err != nil {
		return err
	}
	defer close()
	frameAfterInput := false
	var inputWritten time.Time
	var baselinePixel color.RGBA
	if mode == "input" {
		if capture != nil {
			baseline, e := client.Frame()
			if e != nil {
				return e
			}
			baselinePixel = baseline.RGBAAt(0, 0)
			timing.mark("frame_baseline")
		}
		if err := performSeatScreenInput(client, a); err != nil {
			return err
		}
		inputWritten = time.Now()
		// All input bytes are written; subsequent failures must not repeat it.
		actionDone = true
		timing.mark("input_written")
		if capture != nil {
			frameAfterInput, err = client.SyncAfterInput(baselinePixel)
		} else {
			err = client.Sync()
		}
		if err != nil {
			return err
		}
		timing.mark("sync_done")
	}
	if capture != nil {
		phase = "screenshot"
		if actionDone {
			var e error
			if remaining := time.Until(inputWritten.Add(seatScreenSettleLimit)); !frameAfterInput && remaining > 0 {
				frameAfterInput, e = client.WaitForChange(remaining)
				if e != nil {
					return e
				}
			}
			if frameAfterInput {
				timing.mark("frame_update_seen")
			} else {
				timing.mark("frame_update_timeout")
			}
		}
		// After a timeout one incremental request may still be outstanding.
		// Capture on this input connection anyway: a new view connection's first
		// frame can predate the input on x11vnc (#1484). Every reply read here
		// answers a request sent after the input's Sync. A frame taken after the
		// input but before the application redraws is correct, not stale.
		// capture_drained marks only a successful drain of an outstanding request.
		frame, e := client.FrameAfterWait(func() { timing.mark("capture_drained") })
		if e != nil {
			return e
		}
		timing.mark("frame_read")
		e = png.Encode(capture, frame)
		e = errors.Join(e, capture.Close())
		if e != nil {
			return e
		}
		captured = true
		timing.mark("png_written")
	}
	close()
	phase = "reply"
	replied = true
	return withSeatReply(stdout, jsonOutput, a.id, unread, func(w io.Writer) error {
		if jsonOutput {
			value := map[string]any{"seat_id": a.id, "action": a.command, "path": path, "elapsed_ms": elapsed(), "unread_events": unread}
			if actionDone && capture != nil {
				value["frame_after_input"] = frameAfterInput
			}
			if timing != nil {
				value["debug"] = timing.debug()
			}
			return writeJSON(w, value)
		}
		if path != "" {
			_, e := fmt.Fprintln(w, path)
			return e
		}
		_, e := fmt.Fprintf(w, "Seat %s: %s complete\n", a.id, a.command)
		return e
	})
}

func performSeatScreenInput(c *rfbclient.Client, a seatScreenArgs) error {
	pulse := func(mask byte) error {
		if err := c.Pointer(a.x, a.y, mask); err != nil {
			return err
		}
		return c.Pointer(a.x, a.y, 0)
	}
	switch a.command {
	case "move":
		return c.Pointer(a.x, a.y, 0)
	case "click":
		mask := byte(1)
		if a.button == "right" {
			mask = 4
		}
		if err := pulse(mask); err != nil {
			return err
		}
		if a.button == "double" {
			return pulse(mask)
		}
	case "drag":
		if err := c.Pointer(a.x, a.y, 1); err != nil {
			return err
		}
		for i := 1; i <= 16; i++ {
			if err := c.Pointer(a.x+(a.x2-a.x)*i/16, a.y+(a.y2-a.y)*i/16, 1); err != nil {
				return err
			}
		}
		return c.Pointer(a.x2, a.y2, 0)
	case "scroll":
		for _, axis := range []struct {
			n                  int
			negative, positive byte
		}{{a.dy, 8, 16}, {a.dx, 32, 64}} {
			n, mask := axis.n, axis.positive
			if n < 0 {
				n = -n
				mask = axis.negative
			}
			for i := 0; i < n; i++ {
				if err := pulse(mask); err != nil {
					return err
				}
			}
		}
	case "type":
		for _, k := range a.keys {
			if err := c.Key(k, true); err != nil {
				return err
			}
			if err := c.Key(k, false); err != nil {
				return err
			}
		}
	case "key":
		for _, k := range a.keys {
			if err := c.Key(k, true); err != nil {
				return err
			}
		}
		for i := len(a.keys) - 1; i >= 0; i-- {
			if err := c.Key(a.keys[i], false); err != nil {
				return err
			}
		}
	}
	return nil
}
