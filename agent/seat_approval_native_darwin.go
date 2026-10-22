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
	case "unavailable":
		return SeatApprovalUnavailable, nil
	default:
		return SeatApprovalUnavailable, fmt.Errorf("native Seat approval dialog returned an unknown answer")
	}
}

const seatApprovalScript = `
ObjC.import('Foundation');
ObjC.import('CoreGraphics');
function ownerSession() {
    try {
    var session = ObjC.deepUnwrap($.CGSessionCopyCurrentDictionary());
    return session && session.kCGSSessionOnConsoleKey === true &&
        session.kCGSessionLoginDoneKey === true &&
        session.kCGSSessionUserNameKey === ObjC.unwrap($.NSUserName()) &&
        session.CGSSessionScreenIsLocked !== true;
    } catch (error) { return false; }
}
function run(argv) {
    var prompt = JSON.parse(argv[0]);
    if (!ownerSession()) return 'unavailable';
    var app = Application.currentApplication();
    app.includeStandardAdditions = true;
    try {
        var reply = app.displayDialog(prompt.message, {
            withTitle: 'Wefty Computer request',
            buttons: ['Deny', 'Approve'], defaultButton: 'Deny',
            cancelButton: 'Deny', givingUpAfter: prompt.seconds
        });
        if (reply.gaveUp) return 'no-answer';
        if (!ownerSession()) return 'unavailable';
        return reply.buttonReturned === 'Approve' ? 'approve' : 'deny';
    } catch (error) {
        return 'deny';
    }
}
`
