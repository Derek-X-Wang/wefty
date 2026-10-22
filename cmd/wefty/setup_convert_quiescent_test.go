package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSetupConvertLateSubmissionRestoresBeforeHistory(t *testing.T) {
	f := newConversionFixture(t)
	before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
	late, checked := false, false
	f.host.upgrade.command = func(_ context.Context, _ string, _ []string, name string, args ...string) ([]byte, error) {
		checked = true
		if !late {
			t.Fatal("offline inventory preceded late submission")
		}
		for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
			if f.launchd.loaded[label] {
				t.Fatalf("inventory ran before %s stopped", label)
			}
		}
		if filepath.Base(name) != "wefty-l1" || !strings.Contains(strings.Join(args, " "), "--conversion-inventory") {
			t.Fatalf("unexpected inventory command %s %v", name, args)
		}
		return []byte(`{"version":1,"blockers":["queued job late-submission"]}`), nil
	}
	r := f.request(t)
	r.afterFleetStep = func(step string) error {
		if step == "convert:stopped:"+setupL3Label {
			late = true
		}
		if step == "convert:inventory" || strings.HasPrefix(step, "convert:moved:") {
			t.Fatal("history reached with unfinished work")
		}
		return nil
	}
	_, err := runSetup(t.Context(), f.host, r)
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "late-submission") {
		t.Fatalf("late work not refused/listed: %v", err)
	}
	if !checked {
		t.Fatal("quiescent inventory did not run")
	}
	f.assertPlain(t, before)
	paths, _ := resolveSetupPaths(f.home)
	if _, err := os.Stat(filepath.Join(paths.config, plainConversionName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restored conversion still recorded: %v", err)
	}
}

// Exit without Close to leave a real committed WAL after its only process is
// gone. This models the final submission window without any live installation.
func TestSetupConvertQuiescentWALWriter(t *testing.T) {
	directory := os.Getenv("WEFTY_TEST_CONVERSION_WAL")
	if directory == "" {
		return
	}
	for _, entry := range []struct{ name, statement string }{
		{"l1.sqlite", `INSERT INTO jobs(job_id,dispatch_key,request_hash,spec_json,state,created_ns,updated_ns) VALUES('late-job','late-job','hash','{"class":"one-shot"}','queued',0,0)`},
		{"l3.sqlite", `INSERT INTO runs(run_id,dispatch_key,actor,idempotency_key,request_hash,status,params_json,tags_json,created_ns,updated_ns) VALUES('late-run','late-run','owner','late-run','hash','awaiting-input','{}','[]',0,0)`},
	} {
		db, err := sql.Open("sqlite", filepath.Join(directory, entry.name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(entry.statement); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(0)
}

type conversionDatabaseStamp struct {
	Hash  [32]byte
	Mtime time.Time
	Mode  os.FileMode
}

func conversionDatabaseStamps(t *testing.T, data string) map[string]conversionDatabaseStamp {
	t.Helper()
	entries, err := os.ReadDir(data)
	if err != nil {
		t.Fatal(err)
	}
	result := map[string]conversionDatabaseStamp{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "l1.sqlite") && !strings.HasPrefix(entry.Name(), "l3.sqlite") {
			continue
		}
		p := filepath.Join(data, entry.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		result[entry.Name()] = conversionDatabaseStamp{sha256.Sum256(b), info.ModTime(), info.Mode()}
	}
	return result
}

func TestSetupConvertQuiescentRealBinaryAndWAL(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "wefty-l1")
	build := exec.Command("go", "build", "-o", binary, "../wefty-l1")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build L1: %v %s", err, out)
	}
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "late WAL"}[late], func(t *testing.T) {
			f := newConversionFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			for name := range f.originals {
				if strings.HasPrefix(name, "l1.sqlite") || strings.HasPrefix(name, "l3.sqlite") {
					if err := os.Remove(filepath.Join(paths.data, name)); err != nil {
						t.Fatal(err)
					}
					delete(f.originals, name)
				}
			}
			s, err := l1.OpenStore(filepath.Join(paths.data, "l1.sqlite"), l1.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			l, err := l3.OpenStore(filepath.Join(paths.data, "l3.sqlite"), l3.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}

			// L3 startup retires stale upload temporaries before conversion.
			delete(f.originals, "exec-captures/.upload-partial")
			refresh := func() {
				for name := range conversionDatabaseStamps(t, paths.data) {
					b, err := os.ReadFile(filepath.Join(paths.data, name))
					if err != nil {
						t.Fatal(err)
					}
					f.originals[name] = string(b)
				}
			}
			refresh()
			if err := os.Remove(filepath.Join(f.source, "wefty-l1")); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(binary, filepath.Join(f.source, "wefty-l1")); err != nil {
				t.Fatal(err)
			}
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			r := f.request(t)
			r.afterFleetStep = func(step string) error {
				if late && step == "convert:stopped:"+setupL3Label {
					writer := exec.Command(os.Args[0], "-test.run=^TestSetupConvertQuiescentWALWriter$")
					writer.Env = append(os.Environ(), "WEFTY_TEST_CONVERSION_WAL="+paths.data)
					if out, err := writer.CombinedOutput(); err != nil {
						t.Fatalf("late WAL writer: %v %s", err, out)
					}
					for _, db := range []string{"l1.sqlite", "l3.sqlite"} {
						info, err := os.Stat(filepath.Join(paths.data, db+"-wal"))
						if err != nil || info.Size() == 0 {
							t.Fatalf("missing committed WAL for %s: %v", db, err)
						}
					}
					refresh()
				}
				if late && (step == "convert:inventory" || strings.HasPrefix(step, "convert:moved:")) {
					t.Fatal("late WAL reached history")
				}
				return nil
			}
			checked, closed := false, false
			f.host.conversionFilesClosed = func(context.Context, []string) error { closed = true; return nil }
			var copyFolder string
			f.host.upgrade.command = func(ctx context.Context, directory string, env []string, name string, args ...string) ([]byte, error) {
				checked = true
				copyFolder = directory
				if !closed {
					t.Fatal("offline inventory preceded open-file check")
				}
				for _, label := range []string{setupL1Label, setupL3Label, setupNodeAgentLabel} {
					if f.launchd.loaded[label] {
						t.Fatalf("offline inventory preceded %s stop", label)
					}
				}
				originals := conversionDatabaseStamps(t, paths.data)
				folderInfo, err := os.Stat(directory)
				if err != nil || folderInfo.Mode().Perm() != 0700 {
					t.Fatalf("non-private inventory directory: %v", err)
				}
				for _, db := range []string{"l1.sqlite", "l3.sqlite"} {
					for _, suffix := range []string{"", "-wal"} {
						source := filepath.Join(paths.data, db+suffix)
						want, err := os.ReadFile(source)
						if errors.Is(err, os.ErrNotExist) {
							continue
						}
						if err != nil {
							t.Fatal(err)
						}
						copied := filepath.Join(directory, db+suffix)
						got, err := os.ReadFile(copied)
						if err != nil || !bytes.Equal(got, want) {
							t.Fatalf("copy omitted/changed %s: %v", db+suffix, err)
						}
						info, err := os.Stat(copied)
						if err != nil || info.Mode().Perm() != 0600 {
							t.Fatalf("non-private copy %s: %v", db+suffix, err)
						}
					}
					if _, err := os.Stat(filepath.Join(directory, db+"-shm")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("original SHM copied: %v", err)
					}
				}
				queryStarted := time.Now()
				output, err := runUpgradeCommand(ctx, directory, env, name, args...)
				t.Logf("real offline inventory query (late WAL=%t): %s", late, time.Since(queryStarted))
				if after := conversionDatabaseStamps(t, paths.data); !reflect.DeepEqual(originals, after) {
					t.Fatalf("inventory changed original bytes, mtimes, modes or sidecars: before=%v after=%v", originals, after)
				}
				return output, err
			}
			report, err := runSetup(t.Context(), f.host, r)
			if !checked {
				t.Fatal("real inventory did not run")
			}
			if _, e := os.Stat(copyFolder); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("temporary inventory retained: %v", e)
			}
			if late {
				if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "queued job late-job") || !strings.Contains(err.Error(), "run late-run") {
					t.Fatalf("WAL submission not refused/listed: %v", err)
				}
				f.assertPlain(t, before)
			} else {
				if err != nil || report.History == nil || !report.History.ReadOnly {
					t.Fatalf("empty inventory prevented conversion: %v", err)
				}
				for name, want := range f.originals {
					got, e := os.ReadFile(filepath.Join(report.History.Directory, name))
					if e != nil || string(got) != want {
						t.Fatalf("history changed %s: %v", name, e)
					}
				}
			}
		})
	}
}

func TestSetupConvertQuiescentFailureAndCrashRecovery(t *testing.T) {
	for _, mode := range []string{"unavailable", "bad JSON", "null list", "wrong version", "unknown field", "crash resume", "crash abandon", "restore failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newConversionFixture(t)
			before := installedUpgradeFiles(t, &upgradeFixture{setupFixture: f.setupFixture})
			r := f.request(t)
			originalCommand := f.host.upgrade.command
			read := f.host.upgrade.readNode

			unreadable := false
			f.host.upgrade.readNode = func(ctx context.Context, profile connprofile.Profile) (l1.Node, error) {
				if unreadable {
					return l1.Node{}, errors.New("fixture L1 unreadable")
				}
				return read(ctx, profile)
			}
			f.host.upgrade.command = func(context.Context, string, []string, string, ...string) ([]byte, error) {
				switch mode {
				case "bad JSON":
					return []byte(`bad`), nil
				case "null list":
					return []byte(`{"version":1,"blockers":null}`), nil
				case "wrong version":
					return []byte(`{"version":2,"blockers":[]}`), nil
				case "unknown field":
					return []byte(`{"version":1,"blockers":[],"unknown":true}`), nil
				case "crash resume", "crash abandon":
					panic("fixture crash in quiescent inventory")
				case "restore failure":
					unreadable = true
				}
				return nil, errors.New("fixture offline inventory unavailable")
			}
			if strings.HasPrefix(mode, "crash") {
				func() {
					defer func() {
						if recover() == nil {
							t.Fatal("missing inventory crash")
						}
					}()
					_, _ = runSetup(t.Context(), f.host, r)
				}()
				paths, _ := resolveSetupPaths(f.home)
				record, err := readPlainConversion(paths)
				if err != nil || record == nil || record.Phase != "stop" {
					t.Fatalf("inventory lost stop phase: %+v %v", record, err)
				}
			} else {
				_, err := runSetup(t.Context(), f.host, r)
				if err == nil || commandExitCode(err) != exitConflict {
					t.Fatalf("inventory failure did not refuse with exit 5: %v", err)
				}
				if mode == "restore failure" && !strings.Contains(err.Error(), "--abandon-conversion") {
					t.Fatalf("missing restore advice: %v", err)
				}
			}
			f.host.upgrade.command = originalCommand
			f.host.upgrade.readNode = read
			if mode == "crash abandon" || mode == "restore failure" {
				if _, err := runSetup(t.Context(), f.host, setupRequest{role: "join", convertPlain: true, abandonConversion: true}); err != nil {
					t.Fatal(err)
				}
			}
			if !strings.HasPrefix(mode, "crash") || mode == "crash abandon" {
				f.assertPlain(t, before)
			}
			if _, err := runSetup(t.Context(), f.host, r); err != nil {
				t.Fatalf("retry did not resume/restore: %v", err)
			}
		})
	}
}
