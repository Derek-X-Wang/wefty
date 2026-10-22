package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestConversionInventoryOfflineFlag(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "wefty-l1")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	dir := t.TempDir()
	control, ledger := filepath.Join(dir, "l1.sqlite"), filepath.Join(dir, "l3.sqlite")
	s, err := l1.OpenStore(control, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := l3.OpenStore(ledger, l3.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"--conversion-inventory", "--db=" + control, "--conversion-ledger-db=" + ledger, "--conversion-node-id=node"}
	run := func(extra ...string) ([]byte, error) {
		cmd := exec.Command(binary, append(append([]string{}, args...), extra...)...)
		// An offline flag must not load profiles, enrol participants or use keys.
		cmd.Env = append(os.Environ(), "WEFTY_PROFILE="+filepath.Join(dir, "missing-profile"))
		return cmd.CombinedOutput()
	}
	assertUntouched := func(action func()) {
		t.Helper()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		type stamp struct {
			sum   [32]byte
			mtime int64
		}
		before := map[string]stamp{}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			p := filepath.Join(dir, entry.Name())
			b, e := os.ReadFile(p)
			if e != nil {
				t.Fatal(e)
			}
			info, e := os.Stat(p)
			if e != nil {
				t.Fatal(e)
			}
			before[entry.Name()] = stamp{sha256.Sum256(b), info.ModTime().UnixNano()}
		}
		action()
		after, err := os.ReadDir(dir)
		if err != nil || len(after) != len(entries) {
			t.Fatalf("inventory created sidecars: %v %v", after, err)
		}
		for _, entry := range after {
			if entry.IsDir() {
				continue
			}
			p := filepath.Join(dir, entry.Name())
			b, e := os.ReadFile(p)
			if e != nil {
				t.Fatal(e)
			}
			info, e := os.Stat(p)
			if e != nil {
				t.Fatal(e)
			}
			if before[entry.Name()] != (stamp{sha256.Sum256(b), info.ModTime().UnixNano()}) {
				t.Fatalf("inventory changed %s", entry.Name())
			}
		}
	}
	assertUntouched(func() {
		out, err := run()
		if err != nil || string(out) != "{\"version\":1,\"blockers\":[]}\n" {
			t.Fatalf("empty inventory: %v %s", err, out)
		}
	})
	for _, extra := range [][]string{{"--version"}, {"--state-preflight"}, {"--initiate-admin-bootstrap"}, {"--reset-admin-policy"}, {"--rebind-admin=owner", "--admin-device-id=device"}} {
		assertUntouched(func() {
			out, err := run(extra...)
			if err == nil || !bytes.Contains(out, []byte("mutually exclusive")) {
				t.Fatalf("offline flags accepted %v: %v %s", extra, err, out)
			}
		})
	}

	assertUntouched(func() {
		db, err := openConversionInventoryDB(control)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Exec("DELETE FROM jobs"); err == nil {
			t.Fatal("offline connection allowed a write")
		}
	})
	for _, bad := range [][]string{
		{"--conversion-ledger-db=" + ledger}, {"--conversion-node-id=node"},
		{"--profile-preflight", "--conversion-inventory", "--db=" + control},
	} {
		assertUntouched(func() {
			if out, err := exec.Command(binary, bad...).CombinedOutput(); err == nil {
				t.Fatalf("accepted unsupported offline arguments %v: %s", bad, out)
			}
		})
	}
	for _, missing := range []int{1, 2, 3} {
		bad := append([]string{}, args[:missing]...)
		bad = append(bad, args[missing+1:]...)
		out, err := exec.Command(binary, bad...).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "requires") {
			t.Fatalf("missing required flag accepted: %v %s", err, out)
		}
	}
	// Populate actual current schemas, not a schema invented by the inventory.
	db, err := sql.Open("sqlite", control)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ id, class, state, desired string }{
		{"queued", "one-shot", "queued", ""}, {"claimed", "one-shot", "claimed", ""}, {"finished", "one-shot", "succeeded", ""},
		{"retired-projection", "service", "queued", "running"}, {"backoff", "service", "queued", "running"}, {"stopping", "service", "stopping", "stopped"}, {"removing", "service", "removal_pending", "running"}, {"stopped", "service", "stopped", "stopped"}, {"removed", "service", "removed_verified", "stopped"},
	} {
		if _, err := db.Exec(`INSERT INTO jobs(job_id,dispatch_key,request_hash,spec_json,state,created_ns,updated_ns) VALUES(?,?,?, ?,?,0,0)`, row.id, row.id, "hash", `{"class":"`+row.class+`"}`, row.state); err != nil {
			t.Fatal(err)
		}
		if row.desired != "" {
			if _, err := db.Exec(`INSERT INTO service_jobs(job_id,desired_state) VALUES(?,?)`, row.id, row.desired); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(`INSERT INTO service_removals(job_id,bound_node_id,removal_generation,cleanup_fence,root_instance_id,status,requested_ns) VALUES('removing','node',1,'fixture-fence','fixture-root','removal_pending',0)`); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ id, desired, outcome, job, bound, placed string }{
		{"pending-computer", "removed", "removal_pending", "removing", "node", "node"},
		{"stopped-computer", "stopped", "", "stopped", "node", "node"},
		{"retired-computer", "removed", "removed_verified", "removed", "node", "node"},
		{"foreign-computer", "stopped", "", "finished", "other", "other"},
	} {
		if _, err := db.Exec(`INSERT INTO computers(computer_id,name,placement_node_id,bound_node_id,grants_json,storage_id,storage_generation,desired_disk_bytes,desired_state,intent_revision,applied_revision,current_job_id,current_spec_revision,reconfiguration_phase,removal_outcome,created_ns,updated_ns) VALUES(?,?,?,?,'[]',?,1,1,?,1,1,?,1,'stable',?,0,0)`, row.id, row.id, row.placed, row.bound, row.id, row.desired, row.job, row.outcome); err != nil {
			t.Fatal(err)
		}
	}
	// The live jobs API excludes old Computer projections, even while their
	// retained job facts still look unfinished. The offline check must agree.
	if _, err := db.Exec(`INSERT INTO computer_job_projections(computer_id,job_id,spec_revision,current,created_ns) VALUES('foreign-computer','retired-projection',1,0,0)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", ledger)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"pending", "dispatching", "queued", "running", "awaiting-input", "succeeded", "failed"} {
		if _, err := db.Exec(`INSERT INTO runs(run_id,dispatch_key,actor,idempotency_key,request_hash,status,params_json,tags_json,created_ns,updated_ns) VALUES(?,?,'owner',?,'hash',?,'{}','[]',0,0)`, status, status, status, status); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertUntouched(func() {
		out, err := run()
		want := `{"version":1,"blockers":["Computer pending-computer","Computer stopped-computer","claimed job claimed","queued job queued","run awaiting-input","run dispatching","run pending","run queued","run running","service job backoff","service job removing","service job stopping"]}` + "\n"
		if err != nil || string(out) != want {
			t.Fatalf("inventory JSON: %v\ngot %s\nwant %s", err, out, want)
		}
	})
}
