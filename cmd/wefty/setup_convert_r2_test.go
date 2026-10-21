package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func assertConversionPlainMaintenance(t *testing.T, f *conversionFixture) {
	t.Helper()
	paths, _ := resolveSetupPaths(f.home)
	if _, err := os.Stat(filepath.Join(paths.config, plainConversionName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("plain conversion record still blocks maintenance: %v", err)
	}
	if _, err := os.Stat(filepath.Join(paths.config, upgradeDrainFile)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("drain receipt remains: %v", err)
	}
	if _, err := runSetup(t.Context(), f.host, setupRequest{}); err != nil {
		t.Fatalf("plain setup refused: %v", err)
	}
	f.host.upgrade.command = func(context.Context, string, []string, string, ...string) ([]byte, error) {
		return nil, errors.New("fixture preparation stopped")
	}
	if _, err := runUpgrade(t.Context(), f.host, false, io.Discard); err != nil && strings.Contains(err.Error(), "conversion") {
		t.Fatalf("plain upgrade still fenced: %v", err)
	}
	if err := refuseRecordedFleetRole(paths, "upgrade"); err != nil {
		t.Fatal(err)
	}
	if _, err := runSetupRemove(t.Context(), f.host, false); err != nil {
		t.Fatalf("plain remove refused: %v", err)
	}
}

func TestSetupConvertRound2TimeoutRestoresPlainMaintenance(t *testing.T) {
	f := newConversionFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "busy", AttemptID: "busy", Class: contract.JobClassOneShot}}
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err == nil || commandExitCode(err) != exitConflict {
		t.Fatalf("missing timeout: %v", err)
	}
	assertConversionPlainMaintenance(t, f)
}

func TestSetupConvertRound2RestoreFailureCanAbandon(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	read := f.host.upgrade.readNode
	unreadable := false
	f.host.upgrade.readNode = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		if unreadable {
			return l1.Node{}, errors.New("fixture L1 unreadable")
		}
		return read(ctx, p)
	}
	r.afterFleetStep = func(step string) error {
		if step == "convert:inventory" {
			unreadable = true
			return errors.New("fixture interrupted after inventory")
		}
		return nil
	}
	_, err := runSetup(t.Context(), f.host, r)
	if err == nil || !strings.Contains(err.Error(), "--abandon-conversion") || strings.Contains(err.Error(), "run wefty upgrade") {
		t.Fatalf("wrong restoration recovery advice: %v", err)
	}
	if f.node.ClaimsEnabled {
		t.Fatal("fixture did not leave restoration pending")
	}
	f.host.upgrade.readNode = read
	if _, err := runSetup(t.Context(), f.host, setupRequest{role: "join", convertPlain: true, abandonConversion: true}); err != nil {
		t.Fatalf("abandon failed to finish restore: %v", err)
	}
	assertConversionPlainMaintenance(t, f)
}

func TestSetupConvertRound2CrashAfterPlistRemovalCanAbandon(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:inventory" {
			panic("fixture crash after plist removal")
		}
		return nil
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("missing crash")
			}
		}()
		_, _ = runSetup(t.Context(), f.host, r)
	}()
	if _, err := runSetup(t.Context(), f.host, setupRequest{role: "join", convertPlain: true, abandonConversion: true}); err != nil {
		t.Fatalf("abandon after crash failed: %v", err)
	}
	assertConversionPlainMaintenance(t, f)
}

func TestSetupConvertRound2PreBoundaryChoicesCanChange(t *testing.T) {
	f := newConversionFixture(t)
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:claims" {
			panic("fixture crash")
		}
		return nil
	}
	func() { defer func() { _ = recover() }(); _, _ = runSetup(t.Context(), f.host, r) }()
	r.afterFleetStep = nil
	r.joinCode = joinCodeForTest(t, "wrong-l1", "opaque-l3", "")
	r.withPerson = true
	if _, err := runSetup(t.Context(), f.host, r); err == nil || !strings.Contains(err.Error(), "peer verification failed") {
		t.Fatalf("pre-boundary choices were locked: %v", err)
	}
	if len(f.enroller.roles) != 3 {
		t.Fatal("new person choice was ignored")
	}
}

func TestSetupConvertRound2RemovedComputerDoesNotBlock(t *testing.T) {
	for _, test := range []struct {
		name     string
		desired  contract.ServiceDesiredState
		outcome  string
		jobState contract.JobState
		blocked  bool
	}{
		{name: "removed", desired: contract.ServiceDesiredRemoved, outcome: "removed_verified"},
		{name: "removed reduced", desired: contract.ServiceDesiredRemoved, outcome: "removed_reduced"},
		{name: "stopped", desired: contract.ServiceDesiredStopped, blocked: true},
		{name: "removal pending", desired: contract.ServiceDesiredRemoved, outcome: "removal_pending", jobState: contract.JobRemovalPending, blocked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newConversionFixture(t)
			f.computers = []l1.Computer{{ComputerID: "old-computer", BoundNodeID: f.node.NodeID, DesiredState: test.desired, RemovalOutcome: test.outcome, CurrentJob: l1.Job{State: test.jobState}}}
			_, err := runSetup(t.Context(), f.host, f.request(t))
			if !test.blocked {
				if err != nil {
					t.Fatalf("removed Computer blocked conversion: %v", err)
				}
			} else if err == nil || commandExitCode(err) != exitConflict {
				t.Fatalf("nonterminal Computer ignored: %v", err)
			}
		})
	}
}

func conversionInventoryClient(jobs []l1.Job, runs []l3.RunSummary) *apiClients {
	transport := storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		var payload any
		// Like the real servers, answer only what the filters ask for; an
		// unfiltered listing would read the whole history inside the budget.
		query := r.URL.Query()
		switch r.URL.Path {
		case "/v1/jobs":
			class, state := query.Get("class"), query.Get("state")
			if class == "" || class == contract.JobClassOneShot && state == "" {
				return nil, errors.New("unfiltered job inventory request")
			}
			var matched []l1.Job
			for _, job := range jobs {
				if string(job.Spec.Class) == class && (state == "" || string(job.State) == state) {
					matched = append(matched, job)
				}
			}
			payload = l1.JobList{Jobs: matched}
		case "/v1/runs":
			status := query.Get("status")
			if status == "" {
				return nil, errors.New("unfiltered run inventory request")
			}
			var matched []l3.RunSummary
			for _, run := range runs {
				if string(run.Status) == status {
					matched = append(matched, run)
				}
			}
			payload = l3.RunListPage{Runs: matched}
		default:
			return nil, errors.New("unexpected inventory request")
		}
		b, _ := json.Marshal(payload)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	return &apiClients{l1: &apiClient{address: "fixture", client: &http.Client{Transport: transport}}, l3: &apiClient{address: "fixture", client: &http.Client{Transport: transport}}}
}

func TestSetupConvertRound2UnfinishedWorkRefused(t *testing.T) {
	cases := []struct {
		name string
		job  *l1.Job
		run  *l3.RunSummary
	}{
		{name: "queued one-shot", job: &l1.Job{JobID: "queued-job", State: contract.JobQueued, Spec: contract.JobSpec{Class: contract.JobClassOneShot}}},
		{name: "service backoff", job: &l1.Job{JobID: "backoff-job", State: contract.JobQueued, Spec: contract.JobSpec{Class: contract.JobClassService}, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning}}},
	}
	for _, state := range []contract.RunState{contract.RunPending, contract.RunDispatching, contract.RunQueued, contract.RunRunning, contract.RunAwaitingInput} {
		cases = append(cases, struct {
			name string
			job  *l1.Job
			run  *l3.RunSummary
		}{name: string(state), run: &l3.RunSummary{RunID: "unfinished-run", Status: state}})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newConversionFixture(t)
			var jobs []l1.Job
			var runs []l3.RunSummary
			var id string
			if test.job != nil {
				jobs = append(jobs, *test.job)
				id = test.job.JobID
			}
			if test.run != nil {
				runs = append(runs, *test.run)
				id = test.run.RunID
			}
			f.host.conversionClients = func(context.Context, connprofile.Profile) (*apiClients, error) {
				return conversionInventoryClient(jobs, runs), nil
			}
			_, err := runSetup(t.Context(), f.host, f.request(t))
			if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), id) || !strings.Contains(err.Error(), "wefty cancel") {
				t.Fatalf("unfinished work not refused/listed: %v", err)
			}
			if len(f.launchd.mutating()) != 0 {
				t.Fatal("stopped services with unfinished work")
			}
			if !f.node.ClaimsEnabled {
				t.Fatal("claims not restored")
			}
		})
	}
}

func TestSetupConvertRound2OpenDatabaseRefuses(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native macOS lsof probe; other conversion tests inject file-use inspection")
	}
	f := newConversionFixture(t)
	f.host.conversionFilesClosed = nil
	paths, _ := resolveSetupPaths(f.home)
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c", `exec 3<"$1"; echo ready; read line`, "file-holder", filepath.Join(paths.data, "l1.sqlite"))
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = cmd.Wait() })
	if _, err := bufio.NewReader(output).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	_, err = runSetup(t.Context(), f.host, f.request(t))
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "still open") {
		t.Fatalf("open database accepted: %v", err)
	}
	if len(f.enroller.roles) != 0 {
		t.Fatal("enrolled with an open plain database")
	}
	f.assertPlain(t, before)
	_ = input.Close()
	_ = cmd.Wait()
	if err := checkConversionFilesClosed(t.Context(), []string{filepath.Join(paths.data, "l1.sqlite")}); err != nil {
		t.Fatalf("closed database was not confirmed closed: %v", err)
	}
}

func conversionOutbox(t *testing.T, f *conversionFixture) *sql.DB {
	t.Helper()
	paths, _ := resolveSetupPaths(f.home)
	sum := sha256.Sum256([]byte(f.node.NodeID))
	name := f.node.NodeID + "-" + hex.EncodeToString(sum[:8]) + ".sqlite"
	db, err := sql.Open("sqlite", filepath.Join(paths.data, "node-agent", "log-spool", name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE spool_attempts (job_id TEXT,attempt_id TEXT,fencing_token TEXT,class TEXT,kind TEXT,created_ns INTEGER,incomplete_json BLOB,result_json BLOB,completion_disposition TEXT);
 CREATE TABLE spool_events (attempt_id TEXT);
 CREATE TABLE spool_removals (job_id TEXT);
 INSERT INTO spool_attempts VALUES ('done-job','late-attempt','not-a-credential','one-shot','process',0,NULL,NULL,NULL);
 INSERT INTO spool_events VALUES ('late-attempt');`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestSetupConvertRound2PendingOutboxRefuses(t *testing.T) {
	f := newConversionFixture(t)
	db := conversionOutbox(t, f)
	_, err := runSetup(t.Context(), f.host, f.request(t))
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "outbox") {
		t.Fatalf("late evidence was ignored: %v", err)
	}
	if f.waits == 0 || len(f.launchd.mutating()) != 0 || !f.node.ClaimsEnabled {
		t.Fatal("did not wait/refuse before stopping and restore claims")
	}
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM spool_events").Scan(&n); err != nil || n != 1 {
		t.Fatal("pending evidence was changed")
	}
}

func TestSetupConvertRound2PublishedHistoryIsOwnerData(t *testing.T) {
	for _, choice := range []string{"archive", "delete", "change"} {
		t.Run(choice, func(t *testing.T) {
			f := newConversionFixture(t)
			report, err := runSetup(t.Context(), f.host, f.request(t))
			if err != nil {
				t.Fatal(err)
			}
			history := report.History.Directory
			switch choice {
			case "archive":
				_ = os.Chmod(history, 0700)
				err = os.Rename(history, filepath.Join(f.home, "archived-history"))
			case "delete":
				_ = filepath.WalkDir(history, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if d.IsDir() {
						return os.Chmod(path, 0700)
					}
					return nil
				})
				err = os.RemoveAll(history)
			case "change":
				file := filepath.Join(history, "l1.sqlite")
				_ = os.Chmod(file, 0600)
				err = os.WriteFile(file, []byte("owner edited history"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runSetup(t.Context(), f.host, setupRequest{}); err != nil {
				t.Fatalf("published setup still owns history: %v", err)
			}
		})
	}
}

func TestSetupConvertRound2KnownDurableHardlinkRecovered(t *testing.T) {
	f := newConversionFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	const staging = "l1.sqlite.authority-instance.tmp-0123456789abcdef"
	if err := os.Link(filepath.Join(paths.data, "l1.sqlite.authority-instance"), filepath.Join(paths.data, staging)); err != nil {
		t.Fatal(err)
	}
	report, err := runSetup(t.Context(), f.host, f.request(t))
	if err != nil {
		t.Fatalf("known durable leftover refused: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(report.History.Directory, "l1.sqlite.authority-instance"))
	if err != nil || string(got) != f.originals["l1.sqlite.authority-instance"] {
		t.Fatal("authority identity changed")
	}
	if _, err := os.Lstat(filepath.Join(paths.data, staging)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("staging link remains active")
	}
	for _, entry := range report.History.Files {
		if entry.Name == staging {
			t.Fatal("staging hardlink recorded as separate history")
		}
	}
}

func TestSetupConvertRound2UpgradeStagingRefusesBeforeDrain(t *testing.T) {
	f := newConversionFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	stage := filepath.Join(paths.data, ".upgrade-interrupted")
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "preflight-0.sqlite"), []byte("temporary preflight"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := runSetup(t.Context(), f.host, f.request(t))
	if err == nil || !strings.Contains(err.Error(), "wefty upgrade") {
		t.Fatalf("interrupted upgrade not refused: %v", err)
	}
	if f.node.IntentRevision != 0 || len(f.launchd.mutating()) != 0 {
		t.Fatal("drained or stopped before upgrade recovery")
	}
	if _, err := os.Stat(filepath.Join(paths.config, plainConversionName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("refusal still fences plain upgrade")
	}
}

func TestSetupConvertRound2PurgeKeepsHistoryWithoutManifest(t *testing.T) {
	f := newConversionFixture(t)
	report, err := runSetup(t.Context(), f.host, f.request(t))
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(f.home)
	if err := os.Remove(filepath.Join(paths.config, setupManifestName)); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(f.home)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var removal setupRemoveReport
	if err := purgeSetupEntries(root, paths, &removal); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(removal.Warnings, " "), plainHistoryFolder) {
		t.Fatal("history retention still depends on manifest")
	}
	if _, err := os.Stat(filepath.Join(report.History.Directory, "l1.sqlite")); err != nil {
		t.Fatal("history removed")
	}
}

func TestSetupConvertRound2WorkArrivingAtStopIsRefused(t *testing.T) {
	f := newConversionFixture(t)
	var jobs []l1.Job
	f.host.conversionClients = func(context.Context, connprofile.Profile) (*apiClients, error) {
		return conversionInventoryClient(jobs, nil), nil
	}
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:stopped:"+setupNodeAgentLabel {
			jobs = []l1.Job{{JobID: "late-queued", State: contract.JobQueued, Spec: contract.JobSpec{Class: contract.JobClassOneShot}}}
		}
		return nil
	}
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	if _, err := runSetup(t.Context(), f.host, r); err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "late-queued") {
		t.Fatalf("final inventory missed work: %v", err)
	}
	f.assertPlain(t, before)
}

func TestSetupConvertRound2OutboxDrainsAndRechecksAtStop(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "drains", true: "late evidence"}[late], func(t *testing.T) {
			f := newConversionFixture(t)
			db := conversionOutbox(t, f)
			wait := f.host.upgrade.wait
			f.host.upgrade.wait = func(ctx context.Context, d time.Duration) error {
				if _, err := db.Exec("DELETE FROM spool_events"); err != nil {
					return err
				}
				return wait(ctx, d)
			}
			r := f.request(t)
			r.afterFleetStep = func(step string) error {
				if late && step == "convert:stopped:"+setupNodeAgentLabel {
					_, err := db.Exec("INSERT INTO spool_events VALUES ('late-attempt')")
					return err
				}
				return nil
			}
			_, err := runSetup(t.Context(), f.host, r)
			if late {
				if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "outbox") {
					t.Fatalf("late outbox accepted: %v", err)
				}
			} else if err != nil {
				t.Fatalf("empty outbox blocked: %v", err)
			}
			if f.waits == 0 {
				t.Fatal("did not wait for evidence")
			}
		})
	}
}

func TestSetupConvertRound2RemoveFlagUsage(t *testing.T) {
	for _, args := range [][]string{{"--remove", "--convert-plain"}, {"--remove", "--role", "join", "--convert-plain"}} {
		var out, diagnostics bytes.Buffer
		err := executeSetup(t.Context(), globalOptions{}, args, &out, &diagnostics)
		if err == nil || commandExitCode(err) != 2 || !strings.Contains(err.Error(), "--remove cannot use") || !strings.Contains(err.Error(), "--convert-plain") {
			t.Fatalf("incorrect conversion removal usage: %v", err)
		}
	}
}

func TestSetupConvertRound2RestoreKeepsRecordUntilServicesRun(t *testing.T) {
	f := newConversionFixture(t)
	runner := f.host.launchctl
	f.host.launchctl = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		payload, err := runner(ctx, name, args...)
		if err == nil && args[0] == "bootstrap" && strings.HasSuffix(args[2], setupL1Label+".plist") {
			f.launchd.stopped[setupL1Label] = true
		}
		return payload, err
	}
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:inventory" {
			return errors.New("fixture restore")
		}
		return nil
	}
	if _, err := runSetup(t.Context(), f.host, r); err == nil || !strings.Contains(err.Error(), "--abandon-conversion") {
		t.Fatalf("unverified service restoration called complete: %v", err)
	}
	paths, _ := resolveSetupPaths(f.home)
	if _, err := readPlainConversion(paths); err != nil {
		t.Fatal(err)
	}
	f.host.launchctl = runner
	if _, err := runSetup(t.Context(), f.host, setupRequest{role: "join", convertPlain: true, abandonConversion: true}); err != nil {
		t.Fatal(err)
	}
	assertConversionPlainMaintenance(t, f)
}

func TestSetupConvertRound2WorkInventoryPagesAndFailures(t *testing.T) {
	for _, mode := range []string{"second page work", "repeated job cursor", "repeated run cursor", "unavailable", "terminal"} {
		t.Run(mode, func(t *testing.T) {
			f := newConversionFixture(t)
			f.host.conversionClients = func(context.Context, connprofile.Profile) (*apiClients, error) {
				if mode == "unavailable" {
					return nil, errors.New("fixture inventory unavailable")
				}
				client := conversionInventoryClient(nil, nil)
				transport := storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					var payload any
					switch r.URL.Path {
					case "/v1/jobs":
						page := l1.JobList{}
						if mode == "repeated job cursor" {
							page.NextCursor = "same"
						} else if mode == "second page work" {
							if r.URL.Query().Get("cursor") == "" {
								page.NextCursor = "next"
							} else {
								page.Jobs = []l1.Job{{JobID: "second-page", State: contract.JobQueued, Spec: contract.JobSpec{Class: contract.JobClassOneShot}}}
							}
						}
						if mode == "terminal" {
							page.Jobs = []l1.Job{{JobID: "finished", State: contract.JobSucceeded, Spec: contract.JobSpec{Class: contract.JobClassOneShot}}, {JobID: "stopped-service", Spec: contract.JobSpec{Class: contract.JobClassService}, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredStopped}}}
						}
						payload = page
					case "/v1/runs":
						page := l3.RunListPage{}
						if mode == "repeated run cursor" {
							page.NextCursor = "same"
						}
						if mode == "terminal" {
							page.Runs = []l3.RunSummary{{RunID: "finished-run", Status: contract.RunSucceeded}}
						}
						payload = page
					default:
						return nil, errors.New("unexpected request")
					}
					b, _ := json.Marshal(payload)
					return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
				})
				client.l1.client.Transport, client.l3.client.Transport = transport, transport
				return client, nil
			}
			_, err := runSetup(t.Context(), f.host, f.request(t))
			if mode == "terminal" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("incomplete work inventory accepted")
			}
			if mode == "second page work" && !strings.Contains(err.Error(), "second-page") {
				t.Fatalf("second page not listed: %v", err)
			}
			if len(f.launchd.mutating()) != 0 || !f.node.ClaimsEnabled {
				t.Fatal("inventory failure changed plain stack")
			}
		})
	}
}

func TestSetupConvertRound2OutboxWaitRechecksAdmissionBeforeStop(t *testing.T) {
	f := newConversionFixture(t)
	db := conversionOutbox(t, f)
	wait := f.host.upgrade.wait
	f.host.upgrade.wait = func(ctx context.Context, d time.Duration) error {
		if _, err := db.Exec("DELETE FROM spool_events"); err != nil {
			return err
		}
		f.node.ClaimsEnabled = true
		f.node.IntentRevision++
		f.node.IntentReason = "owner reopened admission"
		f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "late-claim", AttemptID: "late", Class: contract.JobClassOneShot}}
		return wait(ctx, d)
	}
	if _, err := runSetup(t.Context(), f.host, f.request(t)); err == nil || commandExitCode(err) != exitConflict {
		t.Fatalf("outbox wait bypassed admission fence: %v", err)
	}
	if len(f.launchd.mutating()) != 0 {
		t.Fatal("stopped new work after outbox wait")
	}
}
