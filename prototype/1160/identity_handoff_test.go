// Throwaway #1160 fixture. No production state or control-plane credentials.
package identityhandoff

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/participantstate"
	"tailscale.com/derp/derpserver"
	"tailscale.com/envknob"
	"tailscale.com/net/netns"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
	"tailscale.com/tstest/integration/testcontrol"
	"tailscale.com/types/key"
	"tailscale.com/types/logger"
)

const leaseTTL = 30 * time.Second
const renewEvery = 10 * time.Second

func isolate(t *testing.T) {
	t.Helper()
	for _, k := range []string{"TS_AUTHKEY", "TS_AUTH_KEY", "TS_CLIENT_SECRET", "TS_ID_TOKEN", "TSNET_FORCE_LOGIN", "TS_CLIENT_ID", "TS_AUDIENCE", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(k, "")
	}
	for k, v := range map[string]string{"TS_NO_LOGS_NO_SUPPORT": "true", "TS_DISABLE_PORTMAPPER": "true", "TS_DEBUG_ALWAYS_USE_DERP": strconv.FormatBool(os.Getenv("IDENTITY_1160_REPRO_DUMMY") == "1"), "TS_DEBUG_NEVER_DIRECT_UDP": "true", "TS_DEBUG_OMIT_LOCAL_ADDRS": "true", "IN_TS_TEST": "true"} {
		envknob.SetenvForTest(t, k, v)
	}
	netns.SetEnabled(false)
	t.Cleanup(func() { netns.SetEnabled(true) })
}

// This uses the same pinned tsnet backend and production participant lock as
// fabric/tsnet.New/Close, with raw Up for identity/ready instrumentation only.
type owner struct {
	s      *tsnet.Server
	unlock func() error
	id, ip string
}

func openOwner(dir, control, name string) (*owner, error) {
	unlock, err := participantstate.Acquire(dir, "wefty://node/experiment-1160")
	if err != nil {
		return nil, err
	}
	s := &tsnet.Server{Dir: dir, Hostname: name, ControlURL: control, Logf: logger.Discard, UserLogf: logger.Discard}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	st, err := s.Up(ctx)
	if err != nil {
		if closeErr := bounded("failed start cleanup", time.Second, s.Close); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		unlock()
		return nil, err
	}
	ln, err := s.Listen("tcp", ":8080")
	if err != nil {
		if closeErr := bounded("failed start cleanup", time.Second, s.Close); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		unlock()
		return nil, err
	}
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, string(st.Self.ID)) }))
	return &owner{s: s, unlock: unlock, id: string(st.Self.ID), ip: st.TailscaleIPs[0].String()}, nil
}
func bounded(name string, timeout time.Duration, fn func() error) error {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("%s exceeded %s", name, timeout)
	}
}
func (o *owner) close() error {
	// Never unlock state while a timed-out backend may still own the identity.
	return bounded("participant close", 3*time.Second, func() error { return errors.Join(o.s.Close(), o.unlock()) })
}
func tailRequest(s *tsnet.Server, url string) (string, error) {
	tr := &http.Transport{DialContext: s.Dial, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	c := &http.Client{Transport: tr, Timeout: 750 * time.Millisecond}
	resp, err := c.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err == nil && resp.StatusCode != 200 {
		err = fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	return string(b), err
}

type sample struct {
	At       time.Time
	OK       bool
	Error    string
	Forced   bool
	ViaProxy bool
}
type generation struct {
	mu                              sync.Mutex
	owner                           *owner
	dir, control, name, peer, proxy string
	samples                         []sample
	stop                            chan struct{}
}

func (g *generation) renew(forced bool) {
	g.mu.Lock()
	o, proxy := g.owner, g.proxy
	g.mu.Unlock()
	suffix := ""
	if forced {
		suffix = "?probe=1"
	}
	var err error
	if proxy != "" {
		resp, e := (&http.Client{Timeout: time.Second}).Get(proxy + "/forward" + suffix)
		err = e
		if e == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				err = fmt.Errorf("proxy HTTP %d", resp.StatusCode)
			}
		}
	} else if o != nil {
		_, err = tailRequest(o.s, g.peer+suffix)
	} else {
		err = errors.New("no participant owner")
	}
	s := sample{At: time.Now(), OK: err == nil, Forced: forced, ViaProxy: proxy != ""}
	if err != nil {
		s.Error = err.Error()
	}
	g.mu.Lock()
	g.samples = append(g.samples, s)
	g.mu.Unlock()
}

// A child remains alive after release, including its real-time renewal loop.
// The candidate is killed with SIGKILL, not a graceful tsnet close.
func TestGeneration(t *testing.T) {
	if os.Getenv("IDENTITY_1160_CHILD") == "" {
		t.Skip("subprocess helper")
	}
	isolate(t)
	g := &generation{dir: os.Getenv("IDENTITY_1160_DIR"), control: os.Getenv("IDENTITY_1160_CONTROL"), name: os.Getenv("IDENTITY_1160_NAME"), peer: os.Getenv("IDENTITY_1160_PEER"), stop: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/stall", func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
	mux.HandleFunc("/debug/goroutines", func(w http.ResponseWriter, r *http.Request) { pprof.Lookup("goroutine").WriteTo(w, 2) })
	mux.HandleFunc("/open", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.owner != nil {
			http.Error(w, "already open", 409)
			return
		}
		o, err := openOwner(g.dir, g.control, g.name)
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		g.owner = o
		g.proxy = ""
		json.NewEncoder(w).Encode(map[string]string{"id": o.id, "ip": o.ip})
	})
	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.proxy = r.URL.Query().Get("proxy")
		if g.owner == nil {
			http.Error(w, "no owner", 409)
			return
		}
		if err := g.owner.close(); err != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
			http.Error(w, err.Error(), 500)
			return
		}
		g.owner = nil
	})
	mux.HandleFunc("/forward", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		o := g.owner
		g.mu.Unlock()
		if o == nil {
			http.Error(w, "participant unavailable", 503)
			return
		}
		suffix := ""
		if r.URL.Query().Get("probe") == "1" {
			suffix = "?probe=1"
		}
		b, err := tailRequest(o.s, g.peer+suffix)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		io.WriteString(w, b)
	})
	mux.HandleFunc("/begin", func(w http.ResponseWriter, r *http.Request) {
		g.renew(false)
		go func() {
			ticker := time.NewTicker(renewEvery)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					g.renew(false)
				case <-g.stop:
					return
				}
			}
		}()
	})
	mux.HandleFunc("/force", func(w http.ResponseWriter, r *http.Request) { g.renew(true) })
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		defer g.mu.Unlock()
		json.NewEncoder(w).Encode(g.samples)
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("IDENTITY_1160_READY"), []byte("http://"+ln.Addr().String()), 0600); err != nil {
		t.Fatal(err)
	}
	if err := http.Serve(ln, mux); err != nil {
		t.Fatal(err)
	}
}

type child struct {
	cmd     *exec.Cmd
	url     string
	log     *os.File
	logPath string
	done    chan struct{}
}

func (c *child) kill() error {
	select {
	case <-c.done:
		return nil
	default:
	}
	_ = c.cmd.Process.Kill()
	select {
	case <-c.done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("child did not exit after SIGKILL within 2s")
	}
}
func spawn(t *testing.T, dir, control, name, peer string) *child {
	t.Helper()
	root := t.TempDir()
	c := &child{done: make(chan struct{}), logPath: filepath.Join(root, "child.log")}
	var err error
	c.log, err = os.Create(c.logPath)
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(root, "ready")
	c.cmd = exec.Command(os.Args[0], "-test.run=^TestGeneration$", "-test.timeout=80s")
	c.cmd.Env = append(os.Environ(), "IDENTITY_1160_CHILD=1", "IDENTITY_1160_DIR="+dir, "IDENTITY_1160_CONTROL="+control, "IDENTITY_1160_NAME="+name, "IDENTITY_1160_PEER="+peer, "IDENTITY_1160_READY="+ready)
	c.cmd.Stdout = c.log
	c.cmd.Stderr = c.log
	if err := c.cmd.Start(); err != nil {
		c.log.Close()
		t.Fatal(err)
	}
	go func() { _ = c.cmd.Wait(); close(c.done) }()
	t.Cleanup(func() {
		if err := c.kill(); err != nil {
			t.Error(err)
		}
		c.log.Close()
		if t.Failed() {
			b, _ := os.ReadFile(c.logPath)
			if len(b) > 24000 {
				b = b[:24000]
			}
			t.Logf("child %s: %s", name, b)
		}
	})
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-c.done:
			b, _ := os.ReadFile(c.logPath)
			t.Fatalf("child exited before ready: %s", b)
		case <-deadline.C:
			t.Fatal("child admin never ready")
		default:
			if b, e := os.ReadFile(ready); e == nil {
				c.url = string(b)
				return c
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
func call(t *testing.T, c *child, path string) []byte {
	t.Helper()
	timeout := 5 * time.Second
	if strings.HasPrefix(path, "/open") {
		timeout = 15 * time.Second
	}
	resp, err := (&http.Client{Timeout: timeout}).Get(c.url + path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("%s: HTTP %d: %s", path, resp.StatusCode, b)
	}
	return b
}
func open(t *testing.T, c *child) (id, ip string) {
	var v map[string]string
	if err := json.Unmarshal(call(t, c, "/open"), &v); err != nil {
		t.Fatal(err)
	}
	return v["id"], v["ip"]
}
func reachable(t *testing.T, peer *tsnet.Server, ip, id string) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		b, err := tailRequest(peer, "http://"+net.JoinHostPort(ip, "8080"))
		if err == nil && b == id {
			return
		}
		last = fmt.Errorf("body=%q error=%v", b, err)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("peer cannot reach saved node: %v", last)
}

type lease struct {
	mu        sync.Mutex
	last      time.Time
	maxGap    time.Duration
	expired   bool
	id        string
	successes int
}

func (l *lease) serve(peer *tsnet.Server, w http.ResponseWriter, r *http.Request) {
	lc, err := peer.LocalClient()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	who, err := lc.WhoIs(r.Context(), r.RemoteAddr)
	if err != nil {
		http.Error(w, err.Error(), 403)
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if string(who.Node.StableID) != l.id {
		http.Error(w, "identity changed", 403)
		return
	}
	if r.URL.Query().Get("probe") == "1" {
		io.WriteString(w, "probe")
		return
	}
	if !l.last.IsZero() {
		gap := now.Sub(l.last)
		l.maxGap = max(l.maxGap, gap)
		if gap >= leaseTTL {
			l.expired = true
		}
	}
	if l.expired {
		http.Error(w, "lease expired; cannot resurrect", 409)
		return
	}
	l.last = now
	l.successes++
	io.WriteString(w, "renewed")
}

type result struct {
	Trial                                                                                             int
	CloseMS, HandoffMS, RenewalHandoffMS, CrashToReachableMS, RollbackAfterDetectionMS, MaxRenewGapMS float64
	Scheduled, Missed, ForcedMissed, ProxyScheduledOK, RenewalsAccepted                               int
	LeaseSurvived                                                                                     bool
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func TestIdentityHandoff(t *testing.T) {
	isolate(t)
	// Every physical destination is literal loopback. No public DERP fallback;
	// direct UDP discovery and port mapping are disabled; telemetry is disabled.
	d := derpserver.New(key.NewNode(), logger.Discard)
	ds := httptest.NewUnstartedServer(derpserver.Handler(d))
	ds.StartTLS()
	t.Cleanup(func() {
		if err := bounded("DERP cleanup", 2*time.Second, func() error { d.Close(); ds.CloseClientConnections(); ds.Close(); return nil }); err != nil {
			t.Error(err)
		}
	})
	dm := &tailcfg.DERPMap{Regions: map[tailcfg.DERPRegionID]*tailcfg.DERPRegion{1: {RegionID: 1, RegionCode: "local", Nodes: []*tailcfg.DERPNode{{Name: "local", RegionID: 1, HostName: "127.0.0.1", IPv4: "127.0.0.1", IPv6: "none", DERPPort: ds.Listener.Addr().(*net.TCPAddr).Port, STUNPort: -1, InsecureForTests: true}}}}}
	control := &testcontrol.Server{DERPMap: dm, Logf: logger.Discard}
	cs := httptest.NewServer(control)
	control.HTTPTestServer = cs
	t.Cleanup(func() {
		if err := bounded("control cleanup", 2*time.Second, func() error { cs.CloseClientConnections(); cs.Close(); return nil }); err != nil {
			t.Error(err)
		}
	})
	peer := &tsnet.Server{Dir: t.TempDir(), ControlURL: cs.URL, Hostname: "lease-peer", Logf: logger.Discard, UserLogf: logger.Discard}
	t.Cleanup(func() {
		if err := bounded("peer cleanup", 3*time.Second, peer.Close); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	st, err := peer.Up(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var currentMu sync.Mutex
	var current *lease
	ln, err := peer.Listen("tcp", ":8081")
	if err != nil {
		t.Fatal(err)
	}
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentMu.Lock()
		l := current
		currentMu.Unlock()
		if l == nil {
			http.Error(w, "no lease", 503)
			return
		}
		l.serve(peer, w, r)
	}))
	var results []result
	trials := 5
	if os.Getenv("IDENTITY_1160_PROBE") == "1" {
		trials = 1
	}
	for trial := 1; trial <= trials; trial++ {
		uptimeCtx, uptimeCancel := context.WithTimeout(context.Background(), 2*time.Second)
		b, e := exec.CommandContext(uptimeCtx, "uptime").Output()
		uptimeCancel()
		if e == nil {
			t.Logf("uptime before trial %d: %s", trial, b)
			if loadOver20(string(b)) {
				t.Fatal("load >20; rerun when lower")
			}
		}
		dir := t.TempDir()
		name := "handoff-" + strconv.Itoa(trial)
		peerURL := "http://" + net.JoinHostPort(st.TailscaleIPs[0].String(), "8081")
		old := spawn(t, dir, cs.URL, name, peerURL)
		candidate := spawn(t, dir, cs.URL, name, peerURL)
		id, ip := open(t, old)
		reachable(t, peer, ip, id)
		// Verify the real lock prevents simultaneous state owners.
		unlock, e := participantstate.Acquire(dir, "duplicate")
		var inUse *fabric.ParticipantInUseError
		if !errors.As(e, &inUse) {
			if unlock != nil {
				unlock()
			}
			t.Fatalf("overlap lock not enforced: %v", e)
		}
		// Clean sequential handoff, before the adverse renewal/rollback cycle.
		seqStart := time.Now()
		call(t, old, "/release?proxy="+candidate.url)
		seqReleased := time.Now()
		seqID, seqIP := open(t, candidate)
		if seqID != id || seqIP != ip {
			t.Fatal("identity changed in sequential handoff")
		}
		reachable(t, peer, ip, id)
		seqReachable := time.Now()
		if os.Getenv("IDENTITY_1160_PROBE") == "1" {
			if err := old.kill(); err != nil {
				t.Fatal(err)
			}
			if err := candidate.kill(); err != nil {
				t.Fatal(err)
			}
			t.Logf("PROBE close_ms=%.3f handoff_ms=%.3f", ms(seqReleased.Sub(seqStart)), ms(seqReachable.Sub(seqReleased)))
			return
		}
		call(t, candidate, "/release")
		resetID, resetIP := open(t, old)
		if resetID != id || resetIP != ip {
			t.Fatal("identity changed resetting sequential fixture")
		}
		reachable(t, peer, ip, id)
		l := &lease{id: id}
		currentMu.Lock()
		current = l
		currentMu.Unlock()
		call(t, old, "/begin")
		// Deliberately cross the first scheduled renewal during a 2s handoff gap.
		time.Sleep(9 * time.Second)
		call(t, old, "/release?proxy="+candidate.url)
		released := time.Now()
		call(t, old, "/force") // forced diagnostic, kept separate from 10s cadence.
		time.Sleep(2 * time.Second)
		nextID, nextIP := open(t, candidate)
		if nextID != id || nextIP != ip {
			t.Fatal("saved identity changed at handoff")
		}
		reachable(t, peer, ip, id)
		handed := time.Now()
		call(t, old, "/force") // prove old -> loopback proxy -> tailnet peer now works.
		// Keep candidate long enough for a real scheduled 10s renewal via proxy.
		time.Sleep(11 * time.Second)
		failed := time.Now()
		if err := candidate.kill(); err != nil {
			t.Fatal(err)
		}
		call(t, old, "/force")
		// Explicit 2s failure-detection budget; measured separately in report.
		time.Sleep(2 * time.Second)
		detected := time.Now()
		backID, backIP := open(t, old)
		if backID != id || backIP != ip {
			t.Fatal("saved identity changed at rollback")
		}
		reachable(t, peer, ip, id)
		rolled := time.Now()
		call(t, old, "/force")
		time.Sleep(7 * time.Second) // next real scheduled renewal must succeed after rollback.
		var samples []sample
		if err := json.Unmarshal(call(t, old, "/stats"), &samples); err != nil {
			t.Fatal(err)
		}
		r := result{Trial: trial, CloseMS: ms(seqReleased.Sub(seqStart)), HandoffMS: ms(seqReachable.Sub(seqReleased)), RenewalHandoffMS: ms(handed.Sub(released)), CrashToReachableMS: ms(rolled.Sub(failed)), RollbackAfterDetectionMS: ms(rolled.Sub(detected))}
		for _, s := range samples {
			if s.Forced {
				if !s.OK {
					r.ForcedMissed++
				}
			} else {
				r.Scheduled++
				if s.OK && s.ViaProxy {
					r.ProxyScheduledOK++
				}
				if !s.OK {
					r.Missed++
				}
			}
		}
		l.mu.Lock()
		r.MaxRenewGapMS = ms(l.maxGap)
		r.LeaseSurvived = !l.expired && time.Since(l.last) < leaseTTL
		r.RenewalsAccepted = l.successes
		l.mu.Unlock()
		if !r.LeaseSurvived {
			t.Error("lease expired")
		}
		if r.ProxyScheduledOK < 1 {
			t.Error("no scheduled renewal succeeded through candidate proxy")
		}
		if r.RenewalsAccepted < 3 {
			t.Error("too few accepted renewals")
		}
		results = append(results, r)
		resultJSON, _ := json.Marshal(r)
		t.Logf("RESULT %s", resultJSON)
		if err := old.kill(); err != nil {
			t.Fatal(err)
		}
	}
	b, _ := json.MarshalIndent(results, "", "  ")
	if path := os.Getenv("IDENTITY_1160_RESULTS"); path != "" {
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range []struct {
		name string
		get  func(result) float64
	}{{"close_ms", func(r result) float64 { return r.CloseMS }}, {"sequential_release_to_reachable_ms", func(r result) float64 { return r.HandoffMS }}, {"renewal_handoff_ms_includes_2000ms_gap", func(r result) float64 { return r.RenewalHandoffMS }}, {"crash_to_reachable_ms_includes_detection", func(r result) float64 { return r.CrashToReachableMS }}, {"rollback_after_detection_ms", func(r result) float64 { return r.RollbackAfterDetectionMS }}, {"max_accepted_renewal_gap_ms", func(r result) float64 { return r.MaxRenewGapMS }}, {"missed_scheduled", func(r result) float64 { return float64(r.Missed) }}} {
		var a []float64
		for _, r := range results {
			a = append(a, m.get(r))
		}
		slices.Sort(a)
		t.Logf("SUMMARY %s min=%.3f median=%.3f max=%.3f", m.name, a[0], a[len(a)/2], a[len(a)-1])
	}
}
func loadOver20(s string) bool {
	// macOS uptime: "load averages: 1.00 2.00 3.00".
	var a, b, c float64
	for i := 0; i < len(s); i++ {
		if _, err := fmt.Sscanf(s[i:], "load averages: %f %f %f", &a, &b, &c); err == nil {
			return a > 20 || b > 20 || c > 20
		}
	}
	return false
}

// This regression traverses the real call -> Fatal -> child cleanup path,
// without opening tsnet (so it also runs in a route-socket-restricted sandbox).
func TestFailureHelper(t *testing.T) {
	if os.Getenv("IDENTITY_1160_FAILURE_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	c := spawn(t, t.TempDir(), "", "cleanup-fixture", "")
	if err := os.WriteFile(os.Getenv("IDENTITY_1160_PID"), []byte(strconv.Itoa(c.cmd.Process.Pid)), 0600); err != nil {
		t.Fatal(err)
	}
	call(t, c, "/debug/stall")
}
func TestFailureCleanup(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "pid")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFailureHelper$", "-test.timeout=9s")
	cmd.Env = append(os.Environ(), "IDENTITY_1160_FAILURE_HELPER=1", "IDENTITY_1160_PID="+pidPath)
	start := time.Now()
	b, err := cmd.CombinedOutput()
	if ctx.Err() != nil || err == nil || !strings.Contains(string(b), "Client.Timeout") {
		t.Fatalf("expected bounded HTTP failure, got %v: %s", err, b)
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("child %d survived failure cleanup: %v", pid, err)
	}
	t.Logf("HTTP step failed and generation was killed/reaped in %s", time.Since(start))
}
