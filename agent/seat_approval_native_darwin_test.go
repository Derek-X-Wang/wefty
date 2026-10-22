//go:build darwin

package agent

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

// The probe contains only the shared session functions and a JSON-returning
// entry point. It contains no Application or displayDialog invocation.
func TestSeatApprovalNativeSessionProbe(t *testing.T) {
	if !strings.Contains(seatApprovalSessionScript, "ObjC.castRefToObject(") {
		t.Fatal("CF session Ref must be bridged to an Objective-C object before unwrapping")
	}
	script := seatApprovalSessionScript + `
function run() {
 var raw=$.CGSessionCopyCurrentDictionary();
 var session=raw ? ObjC.deepUnwrap(ObjC.castRefToObject(raw)) : null;
 var expected=!!(session && session.kCGSSessionOnConsoleKey && session.kCGSessionLoginDoneKey &&
  session.kCGSSessionUserNameKey===ObjC.unwrap($.NSUserName()) && !session.CGSSessionScreenIsLocked);
 var sample={kCGSSessionOnConsoleKey:true,kCGSessionLoginDoneKey:true,kCGSSessionUserNameKey:'probe',CGSSessionScreenIsLocked:1};
 var lockedNumeric=ownerSessionAvailable(sample,'probe');
 sample.CGSSessionScreenIsLocked=0;
 var unlockedNumeric=ownerSessionAvailable(sample,'probe');
 return JSON.stringify({actual:!!ownerSession(),expected:expected,lockedNumeric:lockedNumeric,unlockedNumeric:unlockedNumeric});
}`
	if strings.Contains(script, "displayDialog") || strings.Contains(script, "Application") {
		t.Fatal("session probe must never contain a dialog call")
	}
	output, err := exec.CommandContext(t.Context(), "/usr/bin/osascript", "-l", "JavaScript", "-e", script).CombinedOutput()
	if err != nil {
		t.Fatalf("session probe: %v %s", err, output)
	}
	var result struct{ Actual, Expected, LockedNumeric, UnlockedNumeric bool }
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("probe JSON: %v %s", err, output)
	}
	if result.LockedNumeric || !result.UnlockedNumeric {
		t.Fatalf("numeric lock state not handled by truthiness: %+v", result)
	}
	if result.Actual != result.Expected {
		t.Fatalf("session check=%t, reference CF dictionary check=%t", result.Actual, result.Expected)
	}
	t.Logf("session available=%t; production and reference checks agree", result.Actual)
}

// Only the fake object's displayDialog is called. No native app is obtained.
func TestSeatApprovalNativePresentationProbe(t *testing.T) {
	script := seatApprovalPresentationScript + `
function run() {
 var results=[];
 [-128,-1708].forEach(function(code) {
  var calls=[];
  var app={activate:function(){calls.push('activate');},displayDialog:function(){calls.push('dialog');throw {errorNumber:code};}};
  results.push({answer:presentSeatApproval(app,{message:'test',seconds:1}),calls:calls});
 });
 return JSON.stringify(results);
}`
	if strings.Contains(script, "Application") {
		t.Fatal("fake presentation probe must never obtain a native application")
	}
	output, err := exec.CommandContext(t.Context(), "/usr/bin/osascript", "-l", "JavaScript", "-e", script).CombinedOutput()
	if err != nil {
		t.Fatalf("fake presentation probe: %v %s", err, output)
	}
	var result []struct {
		Answer string
		Calls  []string
	}
	if err := json.Unmarshal(output, &result); err != nil || len(result) != 2 {
		t.Fatalf("presentation probe: %v %s", err, output)
	}
	for i, want := range []string{"deny", "unavailable"} {
		if result[i].Answer != want || strings.Join(result[i].Calls, ",") != "activate,dialog" {
			t.Fatalf("probe[%d]=%+v", i, result[i])
		}
	}
}
