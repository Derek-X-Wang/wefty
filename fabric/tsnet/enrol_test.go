package tsnet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"tailscale.com/client/local"
	"tailscale.com/ipn"
)

func TestSavedParticipantCannotUseAmbientJoinKey(t *testing.T) {
	for _, name := range []string{"TS_AUTHKEY", "TS_AUTH_KEY", "TS_CLIENT_SECRET", "TS_ID_TOKEN", "TSNET_FORCE_LOGIN", "TS_CLIENT_ID", "TS_AUDIENCE"} {
		t.Setenv(name, "test-ambient-credential")
	}
	f, err := New(Config{Name: "wefty://node/machine-1", StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"TS_AUTHKEY", "TS_AUTH_KEY", "TS_CLIENT_SECRET", "TS_ID_TOKEN", "TSNET_FORCE_LOGIN", "TS_CLIENT_ID", "TS_AUDIENCE"} {
		if os.Getenv(name) != "" {
			t.Errorf("%s remains available to tsnet or later child processes", name)
		}
	}
	if f.server.AuthKey != "" {
		t.Fatal("saved participant received a join key")
	}
}

type fakeEnrolNode struct {
	client   *local.Client
	closed   bool
	closeErr error
}

func (n *fakeEnrolNode) LocalClient() (*local.Client, error) { return n.client, nil }
func (n *fakeEnrolNode) Close() error                        { n.closed = true; return n.closeErr }

func TestEnrolWaitsForRunningAndTranslatesSelfIdentity(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(map[bool]string{false: "machine", true: "person"}[person], func(t *testing.T) {
			spec := fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "one-use-key"}}
			status := `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-device","HostName":"control-plane","DNSName":"wefty-control-plane.identity-domain.","TailscaleIPs":["100.64.0.1"],"Tags":["tag:wefty-control"]}}`
			var loginURL string
			if person {
				spec.Name, spec.Credential = "wefty://person/runner-1", fabric.Credential{}
				spec.LoginURL = func(url string) { loginURL = url }
				status = `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-person","DNSName":"wefty-runner-1-person.identity-domain.","TailscaleIPs":["100.64.0.2"],"UserID":42,"KeyExpiry":"2027-01-01T00:00:00Z"}}`
			}
			starting, running := ipn.Starting, ipn.Running
			url := "https://login.invalid/one-use"
			notices := []ipn.Notify{{State: &starting, BrowseToURL: &url}, {State: &running}}
			var stream strings.Builder
			for _, notice := range notices {
				payload, _ := json.Marshal(notice)
				stream.Write(payload)
				stream.WriteByte('\n')
			}
			node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := status
				if r.URL.Path == "/localapi/v0/watch-ipn-bus" {
					body = stream.String()
				} else if r.URL.Path != "/localapi/v0/status" {
					t.Fatalf("unexpected API %s", r.URL.Path)
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			enroller := Enroller{newNode: func(config Config) (enrolNode, error) {
				if config.Credential.Value != spec.Credential.Value || config.StateDir != spec.StateDirectory || config.Ephemeral {
					t.Fatal("enrolment changed credential, folder, or lifetime")
				}
				return node, nil
			}}
			self, err := enroller.Enrol(t.Context(), spec)
			if err != nil {
				t.Fatal(err)
			}
			if !node.closed || self.Name != spec.Name || self.NodeID == "" || self.DeviceID != self.NodeID {
				t.Fatalf("self=%+v closed=%v", self, node.closed)
			}
			if person {
				if self.UserID != "42" || self.Kind == fabric.IdentityKindMachine || len(self.Tags) != 0 || self.KeyExpiry != time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) || loginURL != url {
					t.Fatalf("person=%+v login callback=%t", self, loginURL != "")
				}
			} else if self.Kind != fabric.IdentityKindMachine || self.UserID != "" || len(self.Tags) != 1 {
				t.Fatalf("machine=%+v", self)
			}
		})
	}
}

func TestEnrolRejectsGrantedNameCollisionAndScrubsKeyErrors(t *testing.T) {
	for _, collision := range []bool{true, false} {
		key := "single-use-secret"
		node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
			body := `{"State":6}`
			if !collision {
				return nil, errors.New("enrol failed for " + key)
			}
			if r.URL.Path == "/localapi/v0/status" {
				body = `{"BackendState":"Running","Self":{"ID":"opaque","HostName":"control-plane","DNSName":"control-plane-1.identity-domain.","TailscaleIPs":["100.64.0.1"]}}`
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		enroller := Enroller{newNode: func(Config) (enrolNode, error) { return node, nil }}
		_, err := enroller.Enrol(context.Background(), fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: key}})
		if err == nil || strings.Contains(err.Error(), key) || !node.closed {
			t.Fatalf("failed enrolment error=%v closed=%v", err, node.closed)
		}
		if collision && !errors.Is(err, fabric.ErrGrantedNameMismatch) {
			t.Fatalf("collision = %v", err)
		}
	}
}

func TestEnrolReadableNetworkName(t *testing.T) {
	for _, tc := range []struct {
		grant string
		ok    bool
	}{
		{"wefty-studio", true}, {"wefty-studio-1", true}, {"wefty-studio-12", true},
		{"studio", false}, {"wefty-other", false}, {"wefty-studio-0", false},
		{"wefty-studio-01", false}, {"wefty-studio-x", false}, {"wefty-studio-1-extra", false},
		{"wefty-studio-", false}, {"wefty-studio-+1", false},
		{"wefty-studio-" + strings.Repeat("1", 51), false},
	} {
		t.Run(tc.grant, func(t *testing.T) {
			node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
				body := `{"State":6}`
				if r.URL.Path == "/localapi/v0/status" {
					body = `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-device","HostName":"wefty-studio","DNSName":"` + tc.grant + `.identity-domain.","TailscaleIPs":["100.64.0.1"],"Tags":["tag:wefty-agent"]}}`
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			enroller := Enroller{newNode: func(config Config) (enrolNode, error) {
				if config.NetworkName != "wefty-studio" {
					t.Errorf("requested name=%q", config.NetworkName)
				}
				return node, nil
			}}
			self, err := enroller.Enrol(t.Context(), fabric.EnrolSpec{Name: "wefty://node/studio-0123abcd", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "fake-key"}})
			if !node.closed {
				t.Fatal("backend remained open")
			}
			if tc.ok {
				if err != nil || self.NetworkName != tc.grant || self.Name != "wefty://node/studio-0123abcd" {
					t.Fatalf("self=%+v err=%v", self, err)
				}
			} else if !errors.Is(err, fabric.ErrGrantedNameMismatch) {
				t.Fatalf("unexpected grant accepted: %+v err=%v", self, err)
			}
		})
	}
}

// The fake local API emits only notifications; no embedded node or network starts.
func TestEnrolProgressMapsBackendStates(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(map[bool]string{false: "keyed", true: "person"}[person], func(t *testing.T) {
			for _, tc := range []struct {
				state      ipn.State
				browseOnly bool
				want       fabric.EnrolProgress
			}{
				{state: ipn.NoState, want: fabric.EnrolStarting},
				{state: ipn.InUseOtherUser, want: fabric.EnrolStarting},
				{state: ipn.NeedsLogin, want: fabric.EnrolStarting},
				{state: ipn.NeedsMachineAuth, want: fabric.EnrolWaitingForDeviceApproval},
				{state: ipn.Stopped, want: fabric.EnrolStarting},
				{state: ipn.Starting, want: fabric.EnrolConnecting},
				{state: ipn.Running, want: fabric.EnrolConnecting},
				{state: ipn.State(99), want: fabric.EnrolStarting},
				{browseOnly: true, want: fabric.EnrolStarting},
			} {
				name := strconv.Itoa(int(tc.state))
				if tc.browseOnly {
					name = "browse_url"
				}
				t.Run(name, func(t *testing.T) {
					var got []fabric.EnrolProgress
					spec := fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "secret-progress-key"}, Progress: func(p fabric.EnrolProgress) { got = append(got, p) }}
					status := `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-device","DNSName":"wefty-control-plane.identity-domain.","TailscaleIPs":["100.64.0.1"],"Tags":["tag:wefty-control"]}}`
					expected := tc.want
					if person {
						spec.Name = "wefty://person/studio"
						spec.Credential = fabric.Credential{}
						spec.LoginURL = func(string) {}
						status = `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-person","DNSName":"wefty-studio-person.identity-domain.","TailscaleIPs":["100.64.0.1"],"UserID":42}}`
						if tc.state == ipn.NeedsLogin || tc.browseOnly {
							expected = fabric.EnrolWaitingForSignIn
						}
					}
					node := &fakeEnrolNode{client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
						body := status
						if r.URL.Path == "/localapi/v0/watch-ipn-bus" {
							var stream strings.Builder
							notice := ipn.Notify{State: &tc.state}
							if tc.browseOnly {
								url := "https://login.invalid/private"
								notice = ipn.Notify{BrowseToURL: &url}
							}
							running := ipn.Running
							for _, n := range []ipn.Notify{notice, notice, {State: &running}} {
								data, _ := json.Marshal(n)
								stream.Write(data)
								stream.WriteByte('\n')
							}
							body = stream.String()
						}
						return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
					})}}
					e := Enroller{newNode: func(Config) (enrolNode, error) { return node, nil }}
					_, err := e.Enrol(t.Context(), spec)
					want := []fabric.EnrolProgress{fabric.EnrolStarting}
					if expected != fabric.EnrolStarting {
						want = append(want, expected)
					}
					if want[len(want)-1] != fabric.EnrolConnecting {
						want = append(want, fabric.EnrolConnecting)
					}
					if err != nil || !slices.Equal(got, want) {
						t.Fatalf("progress=%v want=%v err=%v", got, want, err)
					}
				})
			}
		})
	}
}

type waitingEnrolWatcher struct {
	ctx     context.Context
	notices chan ipn.Notify
	closed  bool
}

func (w *waitingEnrolWatcher) Next() (ipn.Notify, error) {
	select {
	case n := <-w.notices:
		return n, nil
	case <-w.ctx.Done():
		return ipn.Notify{}, w.ctx.Err()
	}
}
func (w *waitingEnrolWatcher) Close() error { w.closed = true; return nil }

func TestEnrolProgressRepeatsAndTimesOut(t *testing.T) {
	for _, person := range []bool{false, true} {
		t.Run(map[bool]string{false: "machine", true: "person"}[person], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				watcher := &waitingEnrolWatcher{notices: make(chan ipn.Notify)}
				node := &fakeEnrolNode{}
				e := Enroller{
					newNode: func(Config) (enrolNode, error) { return node, nil },
					watch: func(ctx context.Context, _ *local.Client) (enrolWatcher, error) {
						watcher.ctx = ctx
						return watcher, nil
					},
				}
				var progress enrolProgressLog
				spec := fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "secret-timing-key"}, Progress: progress.record}
				state, want := ipn.NeedsMachineAuth, fabric.EnrolWaitingForDeviceApproval
				if person {
					spec.Name = "wefty://person/studio"
					spec.Credential = fabric.Credential{}
					spec.LoginURL = func(string) {}
					state = ipn.NeedsLogin
					want = fabric.EnrolWaitingForSignIn
				}
				started := time.Now()
				done := make(chan error, 1)
				go func() { _, err := e.Enrol(t.Context(), spec); done <- err }()
				synctest.Wait()
				watcher.notices <- ipn.Notify{State: &state}
				synctest.Wait()
				for range 3 {
					watcher.notices <- ipn.Notify{State: &state}
				}
				synctest.Wait()
				got, _ := progress.snapshot()
				if !slices.Equal(got, []fabric.EnrolProgress{fabric.EnrolStarting, want}) {
					t.Fatalf("duplicate state output: %v", got)
				}
				time.Sleep(29 * time.Second)
				synctest.Wait()
				got, _ = progress.snapshot()
				if len(got) != 2 {
					t.Fatal("repeated before 30s", got)
				}
				time.Sleep(time.Second)
				synctest.Wait()
				got, _ = progress.snapshot()
				if len(got) != 3 || got[2] != want {
					t.Fatal("no 30s repeat", got)
				}
				time.Sleep(14*time.Minute + 29*time.Second)
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("timed out early: %v", err)
				default:
				}
				time.Sleep(time.Second)
				synctest.Wait()
				err := <-done
				got, times := progress.snapshot()
				var timeout *fabric.EnrolTimeoutError
				if !errors.As(err, &timeout) || timeout.State != want || timeout.Limit != 15*time.Minute || timeout.NeedsJoinKey == person || time.Since(started) != 15*time.Minute || !node.closed || !watcher.closed {
					t.Fatalf("timeout=%v state=%v closed=%t/%t", err, got, node.closed, watcher.closed)
				}
				for i := 2; i < len(times); i++ {
					if times[i].Sub(times[i-1]) < 30*time.Second {
						t.Fatal("repeat too fast", times)
					}
				}
				for _, secret := range []string{"secret-timing-key", "https://", "NeedsLogin", "NeedsMachineAuth"} {
					if strings.Contains(err.Error(), secret) {
						t.Fatal("unsafe timeout message")
					}
				}
			})
		})
	}
}

func TestEnrolTimeLimitAlsoBoundsOpeningTheWatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancelParent := context.WithCancel(t.Context())
		defer cancelParent()
		node := &fakeEnrolNode{}
		var got []fabric.EnrolProgress
		e := Enroller{timeLimit: time.Minute, newNode: func(Config) (enrolNode, error) { return node, nil }, watch: func(ctx context.Context, _ *local.Client) (enrolWatcher, error) {
			<-ctx.Done()
			cancelParent()
			// The real local API client fails a cancelled request with the cause.
			return nil, &url.Error{Op: "Get", URL: "http://local-api/watch", Err: context.Cause(ctx)}
		}}
		started := time.Now()
		_, err := e.Enrol(ctx, fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "secret-startup-key"}, Progress: func(p fabric.EnrolProgress) { got = append(got, p) }})
		var timeout *fabric.EnrolTimeoutError
		if !errors.As(err, &timeout) || timeout.State != fabric.EnrolStarting || timeout.Limit != time.Minute || !timeout.NeedsJoinKey || time.Since(started) != time.Minute || !node.closed || !slices.Equal(got, []fabric.EnrolProgress{fabric.EnrolStarting}) {
			t.Fatalf("startup timeout=%v progress=%v closed=%t", err, got, node.closed)
		}
	})
}

func TestEnrolProgressStateChangeRestartsIntervalAndCancellationStaysCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		watcher := &waitingEnrolWatcher{notices: make(chan ipn.Notify)}
		node := &fakeEnrolNode{}
		e := Enroller{newNode: func(Config) (enrolNode, error) { return node, nil }, watch: func(ctx context.Context, _ *local.Client) (enrolWatcher, error) {
			watcher.ctx = ctx
			return watcher, nil
		}}
		var progress enrolProgressLog
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := e.Enrol(ctx, fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "secret-cancel-key"}, Progress: progress.record})
			done <- err
		}()
		synctest.Wait()
		approval, connecting := ipn.NeedsMachineAuth, ipn.Starting
		watcher.notices <- ipn.Notify{State: &approval}
		synctest.Wait()
		time.Sleep(20 * time.Second)
		watcher.notices <- ipn.Notify{State: &connecting}
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		got, _ := progress.snapshot()
		if !slices.Equal(got, []fabric.EnrolProgress{fabric.EnrolStarting, fabric.EnrolWaitingForDeviceApproval, fabric.EnrolConnecting}) {
			t.Fatal("state change did not reset repeat interval", got)
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		got, _ = progress.snapshot()
		if !slices.Equal(got, []fabric.EnrolProgress{fabric.EnrolStarting, fabric.EnrolWaitingForDeviceApproval, fabric.EnrolConnecting, fabric.EnrolConnecting}) {
			t.Fatal("new state did not repeat", got)
		}
		cancel()
		synctest.Wait()
		err := <-done
		var timeout *fabric.EnrolTimeoutError
		if !errors.Is(err, context.Canceled) || errors.As(err, &timeout) || !node.closed || !watcher.closed {
			t.Fatalf("cancel=%v closed=%t/%t", err, node.closed, watcher.closed)
		}
	})
}

// Delay closing a fully read status response until the enrolment deadline.
// This exercises a result already supplied by the backend as expiry arrives.
type deadlineEnrolBody struct{ io.Reader }

func (deadlineEnrolBody) Close() error { time.Sleep(time.Minute); synctest.Wait(); return nil }

func TestEnrolDeadlinePreservesCompletedResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		valid    bool
		closeErr error
	}{{name: "name refusal"}, {name: "success", valid: true}, {name: "name refusal with cancelled close", closeErr: context.Canceled}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				node := &fakeEnrolNode{closeErr: tc.closeErr, client: &local.Client{OmitAuth: true, Transport: identityRoundTripper(func(r *http.Request) (*http.Response, error) {
					body := `{"State":6}`
					var reader io.ReadCloser = io.NopCloser(strings.NewReader(body))
					if r.URL.Path == "/localapi/v0/status" {
						name := "wefty-control-plane"
						if !tc.valid {
							name = "wefty-other"
						}
						body = `{"BackendState":"Running","CurrentTailnet":{"MagicDNSSuffix":"identity-domain"},"Self":{"ID":"opaque-device","DNSName":"` + name + `.identity-domain.","TailscaleIPs":["100.64.0.1"],"Tags":["tag:wefty-control"]}}`
						reader = deadlineEnrolBody{strings.NewReader(body)}
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: reader}, nil
				})}}
				e := Enroller{timeLimit: time.Minute, newNode: func(Config) (enrolNode, error) { return node, nil }}
				self, err := e.Enrol(t.Context(), fabric.EnrolSpec{Name: "wefty://control-plane", StateDirectory: t.TempDir(), Credential: fabric.Credential{Value: "secret-result-key"}})
				var timeout *fabric.EnrolTimeoutError
				if errors.As(err, &timeout) {
					t.Fatalf("completed result contains timeout: %v", err)
				}
				if tc.valid {
					if err != nil || self.NodeID != "opaque-device" {
						t.Fatalf("completed enrolment became %v", err)
					}
				} else if !errors.Is(err, fabric.ErrGrantedNameMismatch) {
					t.Fatalf("name refusal became %v", err)
				}
			})
		})
	}
}

// Snapshots are copies: synctest time control does not itself make shared
// slice reads safe while the enrolment goroutine reports progress.
type enrolProgressLog struct {
	mu     sync.Mutex
	states []fabric.EnrolProgress
	times  []time.Time
}

func (l *enrolProgressLog) record(p fabric.EnrolProgress) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.states = append(l.states, p)
	l.times = append(l.times, time.Now())
}
func (l *enrolProgressLog) snapshot() ([]fabric.EnrolProgress, []time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.states), slices.Clone(l.times)
}
