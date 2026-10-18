package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestRunLogsTail(t *testing.T) {
	events := append(runLogPagingEvents(), runLogPagingEvents()[:2]...)
	for _, test := range []struct {
		name   string
		tail   string
		cursor string
		want   []contract.LogEvent
	}{
		{name: "last three across pages", tail: "3", want: events[2:]},
		{name: "larger than log", tail: "50", want: events},
		{name: "one event", tail: "1", want: events[4:]},
		{name: "cursor limits retained suffix", tail: "50", cursor: "3", want: events[3:]},
		{name: "empty suffix", tail: "3", cursor: "5"},
	} {
		for _, jsonOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", test.name, jsonOutput), func(t *testing.T) {
				clients := runLogPagingClients(t, &cutoffStubLogServer{events: events})
				var stdout, stderr, expected bytes.Buffer
				args := []string{"logs", "paging-run", "--tail", test.tail, "--cursor", test.cursor}
				if err := execute(t.Context(), clients, jsonOutput, args, &stdout, &stderr); err != nil {
					t.Fatalf("tail: %v; stderr=%s", err, &stderr)
				}
				for _, event := range test.want {
					if jsonOutput {
						if err := writeJSONLine(&expected, event); err != nil {
							t.Fatal(err)
						}
					} else {
						expected.Write(event.Bytes)
					}
				}
				if stdout.String() != expected.String() || stderr.Len() != 0 {
					t.Fatalf("stdout=%q stderr=%q, want stdout=%q", &stdout, &stderr, &expected)
				}
			})
		}
	}
}

func TestRunLogsTailUsage(t *testing.T) {
	for _, test := range []struct {
		args    []string
		message string
	}{
		{[]string{"--tail", "0"}, "--tail must be positive"},
		{[]string{"--tail", "-1"}, "--tail must be positive"},
		{[]string{"--tail", "2", "--all"}, "--tail cannot be combined with --all"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := execute(t.Context(), nil, false, append([]string{"logs", "paging-run"}, test.args...), &stdout, &stderr)
			var usage usageError
			if !errors.As(err, &usage) || decideCommandReport(err).exitCode != 2 || err.Error() != test.message {
				t.Fatalf("want usage exit 2 (%q), got %v", test.message, err)
			}
		})
	}
}

func TestRunLogsTailFollow(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("json=%t", jsonOutput), func(t *testing.T) {
			events := runLogPagingEvents()
			var stdout, stderr bytes.Buffer
			logReads, statusReads := 0, 0
			clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
				if r.URL.Path == "/v1/runs/paging-run" {
					statusReads++
					if stdout.String() != "second event\nlast event" && !jsonOutput && statusReads == 1 {
						t.Errorf("tail must be printed before following: %q", &stdout)
					}
					status := contract.RunRunning
					if statusReads > 1 {
						status = contract.RunSucceeded
					}
					_ = json.NewEncoder(w).Encode(contract.RunRecord{Status: status})
					return
				}
				logReads++
				cursors := []string{"", "1", "2", "3", "3", "4"}
				if logReads > len(cursors) || r.URL.Path != "/v1/runs/paging-run/logs" || r.URL.Query().Get("cursor") != cursors[logReads-1] || r.URL.Query().Get("limit") != "1" {
					t.Errorf("unexpected log read %d: %s", logReads, r.URL)
					http.NotFound(w, r)
					return
				}
				if logReads <= 4 && stdout.Len() != 0 {
					t.Errorf("tail printed before walk ended: %q", &stdout)
				}
				page := l1.LogPage{NextCursor: r.URL.Query().Get("cursor")}
				if logReads <= 3 {
					page.Events = events[logReads-1 : logReads]
					page.NextCursor = fmt.Sprint(logReads)
				} else if logReads == 5 {
					page.Events = []contract.LogEvent{{AttemptID: "paging-attempt", Stream: contract.LogStdout, Sequence: 4, Bytes: []byte("later event\n")}}
					page.NextCursor = "4"
				}
				_ = json.NewEncoder(w).Encode(page)
			})
			err := execute(t.Context(), clients, jsonOutput, []string{"logs", "paging-run", "--tail", "2", "--follow", "--limit", "1", "--poll-interval", "1ms"}, &stdout, &stderr)
			if err != nil {
				t.Fatal(err)
			}
			if jsonOutput {
				var seen []string
				scanJSONEvents(t, stdout.Bytes(), func(event contract.LogEvent) { seen = append(seen, string(event.Bytes)) })
				if !reflect.DeepEqual(seen, []string{"second event\n", "last event", "later event\n"}) {
					t.Fatalf("events=%q", seen)
				}
			} else if stdout.String() != "second event\nlast eventlater event\n" {
				t.Fatalf("stdout=%q", &stdout)
			}
			if logReads != 6 || statusReads != 3 || stderr.Len() != 0 {
				t.Fatalf("logs=%d status=%d stderr=%q", logReads, statusReads, &stderr)
			}
		})
	}
}

func TestRunLogsTailStopsAtWalkPageCap(t *testing.T) {
	savedCap := logWalkPageCap
	logWalkPageCap = 2
	defer func() { logWalkPageCap = savedCap }()
	for _, jsonOutput := range []bool{false, true} {
		for _, follow := range []bool{false, true} {
			clients := runLogPagingClients(t, &cutoffStubLogServer{events: runLogPagingEvents()})
			var stdout, stderr bytes.Buffer
			args := []string{"logs", "paging-run", "--tail", "2"}
			resume := "resume with --tail 2 --cursor 2"
			if follow {
				args = append(args, "--follow")
				resume = "resume with --tail 2 --follow --cursor 2"
			}
			err := execute(t.Context(), clients, jsonOutput, args, &stdout, &stderr)
			if err == nil || decideCommandReport(err).exitCode != 1 || !strings.Contains(err.Error(), "stopped after 2 pages") || !strings.Contains(err.Error(), resume) {
				t.Fatalf("cap must fail with resume cursor: %v", err)
			}
			if stdout.Len() != 0 || stderr.Len() != 0 {
				t.Fatalf("incomplete tail printed: stdout=%q stderr=%q", &stdout, &stderr)
			}
		}
	}
}

func TestRunLogsTailRetryBound(t *testing.T) {
	for _, failures := range []int{2, logWalkRetryLimit + 1} {
		for _, follow := range []bool{false, true} {
			stub := &cutoffStubLogServer{events: runLogPagingEvents(), retryableFailures: map[int]int{1: failures}}
			clients := runLogPagingClients(t, stub)
			var stdout, stderr bytes.Buffer
			args := []string{"logs", "paging-run", "--tail", "2", "--poll-interval", "1ms"}
			if follow {
				args = append(args, "--follow")
			}
			err := execute(t.Context(), clients, false, args, &stdout, &stderr)
			if failures == 2 {
				if err != nil || stdout.String() != "second event\nlast event" {
					t.Fatalf("retry must not duplicate events: %v stdout=%q", err, &stdout)
				}
			} else if !isRetryableL1Answer(err) || stdout.Len() != 0 {
				t.Fatalf("retry bound must return last error without an incomplete tail: %v stdout=%q", err, &stdout)
			}
			if stub.servedRetryableFailures() != failures {
				t.Fatalf("served %d failures, want %d", stub.servedRetryableFailures(), failures)
			}
		}
	}
}

func TestRunLogsTailStreamsAndTruncation(t *testing.T) {
	for _, jsonOutput := range []bool{false, true} {
		for _, fullyTrimmed := range []bool{false, true} {
			t.Run(fmt.Sprintf("json=%t/fullyTrimmed=%t", jsonOutput, fullyTrimmed), func(t *testing.T) {
				marker := &l1.LogTruncation{BoundKind: l1.LogRetentionBytes, EvictedEventCount: 9, EvictedByteCount: 4096}
				events := runLogPagingEvents()
				events[1].Stream = contract.LogStderr
				if fullyTrimmed {
					events = nil
				}
				reads := 0
				clients, _ := readRetryStub(t, func(_ int32, r *http.Request, w http.ResponseWriter) {
					if r.URL.Path != "/v1/runs/paging-run/logs" || r.URL.Query().Get("limit") != "2" {
						t.Errorf("unexpected log request: %s", r.URL)
						http.NotFound(w, r)
						return
					}
					page := l1.LogPage{NextCursor: r.URL.Query().Get("cursor"), Truncation: marker}
					start := reads * 2
					reads++
					if start < len(events) {
						page.Events = events[start:min(start+2, len(events))]
						page.NextCursor = fmt.Sprint(min(start+2, len(events)))
					}
					_ = json.NewEncoder(w).Encode(page)
				})
				var stdout, stderr bytes.Buffer
				if err := execute(t.Context(), clients, jsonOutput, []string{"logs", "paging-run", "--tail", "2", "--limit", "2"}, &stdout, &stderr); err != nil {
					t.Fatal(err)
				}
				var wantNotice bytes.Buffer
				if err := writeLogTruncationNotice(&wantNotice, marker); err != nil {
					t.Fatal(err)
				}
				wantStdout, wantStderr := "", wantNotice.String()
				if !fullyTrimmed {
					if jsonOutput {
						var expected bytes.Buffer
						for _, event := range events[1:] {
							if err := writeJSONLine(&expected, event); err != nil {
								t.Fatal(err)
							}
						}
						wantStdout = expected.String()
					} else {
						wantStdout = "last event"
						wantStderr += "second event\n"
					}
				}
				if stdout.String() != wantStdout || stderr.String() != wantStderr {
					t.Fatalf("stdout=%q stderr=%q; want stdout=%q stderr=%q", &stdout, &stderr, wantStdout, wantStderr)
				}
			})
		}
	}
}
