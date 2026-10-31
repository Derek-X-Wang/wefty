# Local Headscale tsnet smoke experiment (#1241 Part A)

This is an experimental recipe, not a supported-control-server claim. It uses
Headscale **v0.29.5** and wefty's `tailscale.com` **v1.104.1** from `go.mod`.
Part B and the attended A6 setup are outside this experiment.

## A1. Start a throwaway server

Run from the wefty checkout, in Bash, with Python 3, curl, and Go available.
Keep the shell open through cleanup. Do not enable shell tracing, print the
environment, or copy key values into logs. All credentials and server state
stay in the temporary directory with restrictive permissions.

The control URL is **plain HTTP on IPv4 loopback**,
`http://127.0.0.1:18081`. This avoids certificates and any public listener for
this same-machine experiment. The smoke tests pass this URL directly to tsnet;
they do not exercise the HTTPS-only saved connection profile validation.
Choose another unused loopback port in both URL and listener if this port is busy.

The following downloads the official Darwin arm64 release into scratch space.
On another platform, put that platform's v0.29.5 binary at `$HS_ROOT/headscale`
instead. An already installed v0.29.5 binary can also be used without changing
the installation.

```bash
set +x
umask 077
HS_ROOT="$(mktemp -d /tmp/wefty-headscale.XXXXXX)"
chmod 700 "$HS_ROOT"
curl --fail --silent --show-error --location \
  https://github.com/juanfont/headscale/releases/download/v0.29.5/headscale_0.29.5_darwin_arm64 \
  -o "$HS_ROOT/headscale"
curl --fail --silent --show-error --location \
  https://github.com/juanfont/headscale/releases/download/v0.29.5/checksums.txt \
  -o "$HS_ROOT/checksums.txt"
awk '$2 == "headscale_0.29.5_darwin_arm64" {print $1 "  headscale"}' \
  "$HS_ROOT/checksums.txt" > "$HS_ROOT/checksum.expected"
test -s "$HS_ROOT/checksum.expected" || exit 1
(cd "$HS_ROOT" && shasum -a 256 -c checksum.expected) || exit 1
chmod 700 "$HS_ROOT/headscale"
"$HS_ROOT/headscale" version
```

Peers need a DERP relay. Run a local `derper` from the same `tailscale.com`
version wefty pins (v1.104.1), bound to loopback, with a self-signed
certificate it creates itself, and point a one-node DERP map at it. A map with
a region but **no relay nodes** is accepted by Headscale, but then netcheck
never records a working IPv4 send, and every node loops on
`magicsock: last netcheck reported send error. Rebinding.` and never completes
a handshake (#1376). An entirely empty DERP map is rejected with
`initial DERPMap is empty, Headscale requires at least one entry`.
Do not use `derper -dev`: in v1.104.1 it listens on all interfaces.
`InsecureForTests` skips certificate checks; it does not turn TLS off.
There are no external DERP map URLs, update checks, or logtail upload configured.
The metrics listener is disabled. Remote gRPC is disabled without TLS because
`grpc_allow_insecure` is false; its configured address is also loopback.
Administration uses a mode-600 Unix socket.

```bash
# Build derper outside the wefty module: wefty's go.sum does not list its
# dependencies. GOBIN keeps the binary inside the scratch directory.
(cd "$HS_ROOT" && GOBIN="$HS_ROOT" go install tailscale.com/cmd/derper@v1.104.1)
mkdir -p "$HS_ROOT/certs"
"$HS_ROOT/derper" -a=127.0.0.1:18443 -http-port=-1 \
  -stun=true -stun-port=13478 \
  -certmode=manual -hostname=127.0.0.1 -certdir="$HS_ROOT/certs" \
  -c="$HS_ROOT/derper.json" -verify-clients=false \
  > "$HS_ROOT/derper.log" 2>&1 &
DERP_PID=$!
# Ready when this succeeds. Listeners must be 127.0.0.1 only.
curl --fail --silent --show-error --insecure \
  https://127.0.0.1:18443/derp/latency-check >/dev/null
lsof -nP -a -p "$DERP_PID" -i

cat > "$HS_ROOT/derp.yaml" <<'YAML'
regions:
  999:
    regionid: 999
    regioncode: local
    regionname: Local TLS relay
    nodes:
      - name: local-1
        regionid: 999
        hostname: 127.0.0.1
        ipv4: 127.0.0.1
        ipv6: none
        derpport: 18443
        stunport: 13478
        insecurefortests: true
        canport80: false
YAML

cat > "$HS_ROOT/policy.hujson" <<'JSON'
{
  "tagOwners": {"tag:wefty-agent": ["smoke@"]},
  "acls": [{"action": "accept", "src": ["*"], "dst": ["*:*"]}]
}
JSON

cat > "$HS_ROOT/config.yaml" <<YAML
server_url: http://127.0.0.1:18081
listen_addr: 127.0.0.1:18081
metrics_listen_addr: ""
grpc_listen_addr: 127.0.0.1:18082
grpc_allow_insecure: false
noise:
  private_key_path: $HS_ROOT/noise_private.key
prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
  allocation: sequential
derp:
  server:
    enabled: false
  urls: []
  paths: [$HS_ROOT/derp.yaml]
  auto_update_enabled: false
disable_check_updates: true
database:
  type: sqlite
  sqlite:
    path: $HS_ROOT/db.sqlite
    write_ahead_log: true
policy:
  mode: file
  path: $HS_ROOT/policy.hujson
dns:
  magic_dns: true
  base_domain: wefty.test
  override_local_dns: false
  nameservers:
    global: []
    split: {}
  search_domains: []
  extra_records: []
unix_socket: $HS_ROOT/headscale.sock
unix_socket_permission: "0600"
log:
  level: warn
  format: text
logtail:
  enabled: false
YAML

"$HS_ROOT/headscale" --config "$HS_ROOT/config.yaml" configtest || exit 1
"$HS_ROOT/headscale" --config "$HS_ROOT/config.yaml" serve \
  > "$HS_ROOT/server.log" 2>&1 &
HS_PID=$!
# Wait until this returns {"status":"pass"}; stop if the server exits.
curl --fail --silent --show-error http://127.0.0.1:18081/health
```

Create the synthetic user and the two reusable, two-hour pre-auth keys. The
machine key needs reuse because each command starts several separate nodes.
Its `tag:wefty-agent` matches the agent tests' default principal tag. The person
key has **no tags**. The policy deliberately allows all traffic in this
throwaway network. The `smoke@` spelling is Headscale's policy user selector;
plain `smoke` was rejected as an invalid owner format.

```bash
export HS_ROOT
python3 <<'PY'
import json
import os
import subprocess
from pathlib import Path

root = Path(os.environ["HS_ROOT"])
base = [str(root / "headscale"), "--config", str(root / "config.yaml")]

def call(*args):
    result = subprocess.run(base + list(args) + ["--output", "json"],
                            capture_output=True, text=True)
    if result.returncode:
        raise SystemExit("Headscale CLI failed; output withheld to protect credentials")
    try:
        return json.loads(result.stdout)
    except ValueError:
        raise SystemExit("Headscale CLI did not return JSON; output withheld")

user_id = str(call("users", "create", "smoke")["id"])
for name, tags in [("machine", ["--tags", "tag:wefty-agent"]), ("person", [])]:
    key = call("preauthkeys", "create", "--user", user_id,
               "--reusable", "--expiration", "2h", *tags).get("key")
    if not isinstance(key, str) or not key:
        raise SystemExit("Missing pre-auth key; output withheld")
    target = root / (name + ".key")
    target.write_text(key + "\n")
    target.chmod(0o600)
PY

export TS_CONTROL_URL=http://127.0.0.1:18081
export TS_AUTHKEY="$(cat "$HS_ROOT/machine.key")"
export TS_AUTHKEY_CI_TESTER="$(cat "$HS_ROOT/person.key")"
# Send peer traffic through the local relay; skip router port mapping.
export TS_DEBUG_ALWAYS_USE_DERP=true
export TS_DISABLE_PORTMAPPER=true
```

The environment values are `TS_CONTROL_URL=http://127.0.0.1:18081`,
`TS_AUTHKEY=<machine-pre-auth-key>`, and
`TS_AUTHKEY_CI_TESTER=<person-pre-auth-key>`. Never print the actual keys.

## A2. Run the existing tests

On a shared machine, check `sysctl -n vm.loadavg` before each command and wait
until it is low. Do not save raw verbose test output without first replacing
the key values in it. These commands need a host shell: a sandbox that blocks
`sysctl`, routing sockets or loopback networking cannot run them.

```bash
go test -count=1 -timeout 45m -tags=tsnet_smoke ./fabric/tsnet -run '^TestTSNet(Smoke|PersonWhoAmI)$'
# Repeat the load preflight before this second command.
go test -count=1 -timeout 45m -tags=tsnet_smoke ./agent -run '^TestTSNet(Smoke|ServiceReachability)$'
```

The agent tests need #1383 (managed root under a symbolic-link temp folder) and
#1393 (port 80 is reserved; registration retry) on the checkout.

## A3. Check identity and the missing-domain case

The existing Fabric `TestTSNetSmoke` checks a nonempty machine `NodeID`,
`DeviceID`, and `FabricID`. `TestTSNetPersonWhoAmI` checks a nonempty person
`UserID`, `DeviceID`, and `FabricID` through `/v1/whoami`. Both are already
behind `tsnet_smoke`. A passing run with the configuration above would cover
the populated-domain claims. In this experiment the tests failed before those
checks (see the results), so the claims are not yet shown.

To inspect the unset-domain case without changing the running server:

```bash
sed '/^  base_domain:/d' "$HS_ROOT/config.yaml" > "$HS_ROOT/config-no-domain.yaml"
"$HS_ROOT/headscale" --config "$HS_ROOT/config-no-domain.yaml" configtest
```

Observed by hand: v0.29.5 exits 1 with
`dns.base_domain must be set when using MagicDNS (dns.magic_dns)`.
It rejects this configuration before any client can connect. This does **not**
prove a runtime `ErrIssuingFabricUnavailable` result. Source inspection of
`fabric/tsnet/tsnet.go`, `issuingFabricID`, shows that it returns that error
when the client's current tailnet or MagicDNS suffix is absent. Disabling
MagicDNS as well and measuring that client-side result remains untried.
No new test or product-code change was added.

## A4–A5. Results and limits

### Second run, with a local relay (2026-10-31)

Same Headscale and `tailscale.com` versions, checkout `3472dddc` plus #1393,
the local `derper` and DERP map above, `TS_DEBUG_ALWAYS_USE_DERP=true`:

| Test | Result |
| --- | --- |
| `fabric/tsnet.TestTSNetSmoke` | PASS (9.8 s) |
| `fabric/tsnet.TestTSNetPersonWhoAmI` | PASS (11.3 s) |
| `agent.TestTSNetSmoke` | PASS (9.9 s) |
| `agent.TestTSNetServiceReachability` | PASS (11.9 s) |

No `Rebinding` lines. `derper` listened only on `127.0.0.1:18443` (TCP) and
`127.0.0.1:13478` (UDP). Without #1393 the two agent tests failed for test-only
reasons: a 10.2 s registration timeout, and `published port 80 is reserved for
wefty's Seat front door`. A3's identity checks run after a connection, so they
now ran and passed inside `TestTSNetPersonWhoAmI` and `TestTSNetSmoke`.

Still open: direct UDP paths between nodes on one Mac (the relay was forced).
On macOS, netns does not bind loopback TCP to an interface, but magicsock's
wildcard UDP sockets can be bound to the default-route interface. Testing
that needs a test-only `netns.SetEnabled(false)` build without
`TS_DEBUG_ALWAYS_USE_DERP`.

### First run, without a relay (2026-10-30)

Date: **2026-10-30**. Platform: macOS arm64. Headscale: **v0.29.5**, official
release binary, commit `0f7d1e536bed9f01dd7161f7637af19bb0908335`.
`tailscale.com`: **v1.104.1**. Checkout starting point: `33acb168`.

The release checksum matched. Headscale started with the local direct-only
DERP map and MagicDNS/base domain set. `GET /health` returned
`{"status":"pass"}`. User and both key creation commands succeeded; key
files had mode 600. The default route on the test machine used its Wi-Fi
interface; the control server was on loopback.

The A2 commands ran on 2026-10-30 from a host shell (a sandboxed agent could
not run them). Both commands failed:

| Test | Result | First error |
| --- | --- | --- |
| `fabric/tsnet.TestTSNetSmoke` | FAIL (120 s) | `tsnet_smoke_test.go:88: context deadline exceeded` (client `Dial` to the server node) |
| `fabric/tsnet.TestTSNetPersonWhoAmI` | FAIL (32 s) | `person_identity_smoke_test.go:87: Get "http://fabric.invalid/v1/whoami": context deadline exceeded` |
| `agent.TestTSNetSmoke` | FAIL (11 s) | `tsnet_smoke_test.go:92: agent: call L1: Post "http://control-plane.invalid/v1/agent/nodes/register": context deadline exceeded` |
| `agent.TestTSNetServiceReachability` | FAIL (0.1 s) | `tsnet_smoke_test.go:181: agent: initialize managed service root: open directory "var" without following links: not a directory` |

What the logs show:

- Every node logged in and reached the `Running` state
  (`NoState -> NeedsLogin -> Starting -> Running`), so Headscale accepted both
  the tagged machine key and the untagged person key, and sent network maps.
- WireGuard handshakes between the nodes never completed
  (`Handshake did not complete after 5 seconds, retrying`).
- Each node repeated `magicsock: last netcheck reported send error. Rebinding.`
  more than a thousand times, and kept opening and closing its connection to
  the empty DERP region (`rebind-no-localaddr`).
- `agent.TestTSNetServiceReachability` failed for a reason unrelated to
  Headscale: on macOS, `t.TempDir()` is under `/var`, which is a symbolic link,
  and the managed service root refuses to follow links. The other three tests
  failed on the network path.

**A3:** With `base_domain: wefty.test`, the nodes came up and the Fabric tests
got as far as dialing, so creating the Fabric did not fail with
`ErrIssuingFabricUnavailable`. The `NodeID`, `DeviceID`, `FabricID` and
`UserID` checks run only after a connection, so they are **not shown**. With
the base domain unset and MagicDNS still enabled, the server's configuration
rejection above was **observed by hand**. The client-side missing-suffix result
remains untried.

**A4:** Tried one layout: HTTP control on `127.0.0.1`, same-machine userspace
nodes, no DERP relay, default route on Wi-Fi. The nodes reached the control
server but could not reach each other, and netcheck reported send errors in a
loop. Two explanations fit and this run does not separate them: the embedded
nodes bind their sockets to the default-route interface (the research's
concern), or peers on one machine need a reachable DERP relay or STUN when
direct paths fail. Telling them apart needs either a local DERP relay or a
build that calls `netns.SetEnabled(false)`. No code was changed; that decision
remains the owner's.

Follow-up work (owner decides):

- Done 2026-10-31: rerun with a local DERP relay (all four pass, see above).
  Still open: direct UDP with `netns.SetEnabled(false)` in a test-only build.
- Done: `agent.TestTSNetServiceReachability` on macOS (#1383).
- OIDC person login; not tried. The person key only tests untagged key
  enrollment.
- Saved connection profile with an HTTPS control URL,
  `wefty setup --control-url` and attended `--with-person` (A6); not tried.
- Headscale `main` / the unreleased 0.30 line; not tried.
- Other interface layouts, multi-homed hosts, exit nodes, remote clients,
  HTTPS, or a public DERP relay; not tried.

## Cleanup

Stop only the server this shell started, then remove its entire scratch
directory and clear the environment. Do not stop unrelated Headscale processes.

```bash
kill "$HS_PID" "$DERP_PID"
wait "$HS_PID" "$DERP_PID"
unset TS_AUTHKEY TS_AUTHKEY_CI_TESTER TS_CONTROL_URL
unset TS_DEBUG_ALWAYS_USE_DERP TS_DISABLE_PORTMAPPER
rm -rf -- "$HS_ROOT"
pgrep -fl 'headscale|derper'
ls "$HS_ROOT"
```

Expected in an isolated run: pgrep has no output, and ls reports the directory
absent. If process listing is sandbox-blocked, report the denial instead of
claiming an empty process list.

Observed cleanup in this experiment: the server process was stopped, the
scratch directory (binary, state, keys) was removed, and afterwards
`pgrep -fl headscale` found no process and the directory was absent.

## Pinned references

- [Official v0.29.5 release and checksums](https://github.com/juanfont/headscale/releases/tag/v0.29.5)
- [v0.29.5 configuration example](https://github.com/juanfont/headscale/blob/v0.29.5/config-example.yaml)
- [v0.29.5 DNS configuration validation](https://github.com/juanfont/headscale/blob/v0.29.5/hscontrol/types/config.go)
- [Prior source research](../research/2026-10-20-multi-account-network-access.md)
