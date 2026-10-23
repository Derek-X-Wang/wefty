//go:build linux && computer_seat_lane

package ocihelper

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
)

// These are helper-level AP rows. Task creation is fixture plumbing using the
// real profile and network selection; CLI copies and full Run admission belong
// to the end-to-end lane. Every command and byte stream uses ExecComputerSeat.
func TestHelperComputerNetworkNoneAP1AP2AP3(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("computer_seat_lane requires root")
	}
	image, address := os.Getenv("WEFTY_SEAT_IMAGE"), os.Getenv("WEFTY_SEAT_CONTAINERD")
	if image == "" || address == "" {
		t.Fatal("Seat lane provisioning missing")
	}
	withSeatEndpointComputerNetwork(t, image, address, ComputerNetworkNone, func(ctx context.Context, engine *ContainerdEngine, authority AttemptAuthority, task containerd.Task, _ string) {
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		applied, err := engine.ApplyComputerSeats(ctx, ApplyComputerSeatsRequest{authority, []ComputerSeat{{1, "seat1", 7102, 7103}}})
		must(err)
		var observed ObserveComputerSeatsResponse
		deadline := time.NewTimer(65 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			observed, err = engine.ObserveComputerSeats(ctx, ObserveComputerSeatsRequest{authority, applied.Generation})
			if err == nil && len(observed.Seats) == 1 && observed.Seats[0].State == "ready" && len(observed.Seats[0].Endpoints) == 2 {
				break
			}
			if err == nil && len(observed.Seats) == 1 && observed.Seats[0].State == "failed" {
				t.Fatalf("none Seat failed readiness: %+v", observed)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-deadline.C:
				t.Fatalf("none Seat never ready: %+v %v", observed, err)
			case <-ticker.C:
			}
		}
		// Dial both production Unix-socket endpoints through the helper from
		// outside the Computer namespace, including rfb-websocket-v1 greetings.
		for _, endpoint := range observed.Seats[0].Endpoints {
			must(probeComputerSeatGreeting(ctx, func(probeCtx context.Context) (net.Conn, error) {
				client, helper := net.Pipe()
				go func() {
					defer helper.Close()
					_ = engine.DialSeatEndpoint(probeCtx, DialSeatEndpointRequest{authority, applied.Generation, 1, endpoint}, helper)
				}()
				_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
				var marker [1]byte
				if _, err := io.ReadFull(client, marker[:]); err != nil || marker[0] != attemptPortBackendReady {
					client.Close()
					return nil, fmt.Errorf("none endpoint not ready: %v %v", marker, err)
				}
				_ = client.SetDeadline(time.Time{})
				return client, nil
			}))
		}
		run := func(script string, args ...string) []byte {
			t.Helper()
			var stdout, stderr bytes.Buffer
			var exit *SeatCommandExit
			argv := append([]string{"python3", "-I", "-S", "-c", script}, args...)
			must(engine.ExecComputerSeat(ctx, ExecComputerSeatRequest{authority, applied.Generation, 1, argv, time.Minute}, func(e SeatCommandEvent) error {
				if e.Exit != nil {
					exit = e.Exit
				} else if e.Stream == "stdout" {
					stdout.Write(e.Data)
				} else {
					stderr.Write(e.Data)
				}
				return nil
			}))
			if exit == nil || exit.Code == nil || *exit.Code != 0 || stderr.Len() != 0 {
				t.Fatalf("none command failed: exit=%+v stderr=%s stdout=%s", exit, stderr.String(), stdout.String())
			}
			return stdout.Bytes()
		}
		run(`import os
assert os.getuid()==2001 and os.getgid()==2001
assert os.getcwd()=='/wefty/service/seats/1'
assert os.environ['HOME']=='/wefty/service/seats/1/home'
`)
		// Command-mediated copies use the helper's existing command transport.
		// Binary bytes travel in and out without a guest TCP listener or route.
		payload := append([]byte("none-copy\x00\xff\n"), bytes.Repeat([]byte{0, 1, 127, 255}, 512)...)
		run(`import base64,sys
open('copied-in.bin','wb').write(base64.b64decode(sys.argv[1]))
`, base64.StdEncoding.EncodeToString(payload))
		got := run(`import pathlib,sys
p=pathlib.Path('copied-in.bin')
assert p.stat().st_uid==2001
data=p.read_bytes()
pathlib.Path('copied-out.bin').write_bytes(data[::-1])
sys.stdout.buffer.write(pathlib.Path('copied-out.bin').read_bytes())
`)
		want := bytes.Clone(payload)
		for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
			want[i], want[j] = want[j], want[i]
		}
		if !bytes.Equal(got, want) {
			t.Fatal("none copy lost bytes")
		}
		proof := run(`import socket,errno,urllib.request,json
assert [name for _,name in socket.if_nameindex()]==['lo']
assert len(open('/proc/net/route').read().splitlines())==1
assert open('/etc/resolv.conf').read()=='nameserver 127.0.0.1\noptions timeout:1 attempts:1\n'
assert open('/etc/hosts').read()=='127.0.0.1 localhost\n::1 localhost\n'
for family,address in ((socket.AF_INET,('1.1.1.1',443)),(socket.AF_INET6,('2606:4700:4700::1111',443))):
 with socket.socket(family,socket.SOCK_STREAM) as s:
  s.settimeout(1)
  try:s.connect(address)
  except OSError as e:assert e.errno in (errno.ENETUNREACH,errno.EHOSTUNREACH,errno.EAFNOSUPPORT),e
  else:raise AssertionError('outbound TCP worked')
try:urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://1.1.1.1/',timeout=1)
except OSError:pass
else:raise AssertionError('outbound web worked')
try:socket.getaddrinfo('example.com',443)
except socket.gaierror:pass
else:raise AssertionError('outward DNS resolved')
with socket.socket() as listener:
 listener.bind(('127.0.0.1',0));listener.listen(1)
 with socket.create_connection(listener.getsockname(),timeout=1) as client:
  server,_=listener.accept()
  with server:
   server.sendall(b'loopback works')
   assert client.recv(64)==b'loopback works'
print(json.dumps({'loopback':True,'outbound_tcp_refused':True,'web_refused':True,'dns_refused':True,'interfaces':['lo']}))
`)
		// Repeat real endpoint probes after the network refusals. Missing internet
		// must neither revoke readiness nor kill the Computer's task.
		health, err := engine.ObserveComputerSeats(ctx, ObserveComputerSeatsRequest{authority, applied.Generation})
		must(err)
		if len(health.Seats) != 1 || health.Seats[0].State != "ready" || health.Seats[0].Reason != "" {
			t.Fatalf("missing internet reported as a fault: %+v", health)
		}
		status, err := task.Status(ctx)
		must(err)
		if status.Status != containerd.Running {
			t.Fatalf("none Computer stopped: %+v", status)
		}
		var networkProof map[string]any
		must(json.Unmarshal(proof, &networkProof))
		if output := os.Getenv("WEFTY_HELPER_NETWORK_EVIDENCE"); output != "" {
			payload, err := json.MarshalIndent(map[string]any{"version": 1, "acceptance": []string{"AP-1", "AP-2", "AP-3"}, "scope": "helper; command-mediated copies", "platform": "linux/" + runtime.GOARCH, "image": image, "network": "none", "screen_endpoints": 2, "seat_uid": 2001, "copy_bytes": len(got), "seat_ready_after_network_refusal": true, "task_running": true, "network_proof": networkProof}, "", "  ")
			must(err)
			must(os.WriteFile(output, payload, 0600))
		}
		t.Logf("AP-1/AP-2/AP-3 helper: screen endpoints answer; Seat uid 2001; %d binary bytes copied; %s; readiness and liveness retained", len(got), proof)
	})
}
