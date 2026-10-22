//go:build darwin

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"time"
)

// NativeSeatApprovalDialog runs inside the per-user node agent's login
// session. It never uses sudo, launchctl, System Events or GUI automation.
type NativeSeatApprovalDialog struct{}

func (NativeSeatApprovalDialog) Show(ctx context.Context, prompt SeatApprovalPrompt) (SeatApprovalChoice, error) {
	if prompt.Wait <= 0 {
		return SeatApprovalNoAnswer, nil
	}
	payload, err := json.Marshal(struct {
		Message string `json:"message"`
		Seconds int    `json:"seconds"`
	}{prompt.Message, int(math.Ceil(prompt.Wait.Seconds()))})
	if err != nil {
		return SeatApprovalUnavailable, err
	}
	// Both the native dialog and its containing process have an independent
	// timeout. Arguments carry user text as JSON data, never AppleScript code.
	bounded, cancel := context.WithTimeout(ctx, prompt.Wait+time.Second)
	defer cancel()
	output, err := exec.CommandContext(bounded, "/usr/bin/osascript", "-l", "JavaScript", "-e", seatApprovalScript, "--", string(payload)).Output()
	if bounded.Err() != nil {
		return SeatApprovalNoAnswer, nil
	}
	if err != nil {
		return SeatApprovalUnavailable, fmt.Errorf("native Seat approval dialog failed: %w", err)
	}
	switch strings.TrimSpace(string(output)) {
	case "approve":
		return SeatApprovalApprove, nil
	case "deny":
		return SeatApprovalDeny, nil
	case "no-answer":
		return SeatApprovalNoAnswer, nil
	case "session-unavailable":
		return SeatApprovalUnavailable, nil
	case "unavailable":
		return SeatApprovalUnavailable, fmt.Errorf("native Seat approval dialog unavailable")
	default:
		return SeatApprovalUnavailable, fmt.Errorf("native Seat approval dialog returned an unknown answer")
	}
}

// This fragment is also run by the non-dialog session probe in macOS CI.
const seatApprovalSessionScript = `
ObjC.import('Foundation');
ObjC.import('CoreGraphics');
function ownerSessionAvailable(session, username) {
    return !!(session && session.kCGSSessionOnConsoleKey === true &&
        session.kCGSessionLoginDoneKey === true &&
        session.kCGSSessionUserNameKey === username &&
        !session.CGSSessionScreenIsLocked);
}
function ownerSession() {
    try {
        var ref = $.CGSessionCopyCurrentDictionary();
        var session = ref ? ObjC.deepUnwrap(ObjC.castRefToObject(ref)) : null;
        return ownerSessionAvailable(session, ObjC.unwrap($.NSUserName()));
    } catch (error) { return false; }
}
`

// The presentation function accepts an app so the probe can use an ordinary
// JavaScript fake, with no reference to a native application or actual dialog.
const seatApprovalPresentationScript = `
function presentSeatApproval(app, prompt) {
    try {
        app.activate();
        var reply = app.displayDialog(prompt.message, {
            withTitle: 'Wefty Computer request',
            buttons: ['Deny', 'Approve'], defaultButton: 'Deny',
            cancelButton: 'Deny', givingUpAfter: prompt.seconds
        });
        if (reply.gaveUp) return 'no-answer';
        return reply.buttonReturned === 'Approve' ? 'approve' : 'deny';
    } catch (error) {
        return Number(error.errorNumber) === -128 ? 'deny' : 'unavailable';
    }
}
`

const seatApprovalScript = seatApprovalSessionScript + seatApprovalPresentationScript + `
function run(argv) {
    var prompt = JSON.parse(argv[0]);
    if (!ownerSession()) return 'session-unavailable';
    var app = Application.currentApplication();
    app.includeStandardAdditions = true;
    var answer = presentSeatApproval(app, prompt);
    if (answer === 'approve' && !ownerSession()) return 'session-unavailable';
    return answer;
}
`
