#!/usr/bin/env bash
# Runs the four tsnet_smoke tests against a Headscale control server and a
# DERP relay that this script starts on loopback (#1488), then a negative
# control. It needs no Tailscale account and no secret: the pre-auth keys are
# created here and die with the throwaway server.
#
# The setup follows docs/runbooks/local-headscale-tsnet-smoke.md. Peers need
# the local relay (#1376), so peer traffic is forced through it.
#
# It runs the same way in GitHub Actions and in a plain Linux golang container
# with network access and the repository mounted. Run it from the repository
# root: bash scripts/tsnet-headscale-lane.sh
set -euo pipefail
# Tracing would print the pre-auth keys before they are masked.
set +x

# Headscale supports only a window of Tailscale client versions. A
# tailscale.com bump in go.mod may need a Headscale bump here.
readonly HEADSCALE_VERSION=0.29.5
# SHA-256 of the official v0.29.5 release binaries (its checksums.txt, which
# matches the GitHub release asset digests).
readonly HEADSCALE_SHA256_LINUX_AMD64=e979d27fe17d924d33bb6ab7504f6338d0635989a0cfdcfbc0ceb637dbcbe980
readonly HEADSCALE_SHA256_LINUX_ARM64=3afddd207b89ac22cfad4235cc163c6e538cc59ada139e7c0bf47640c5d9e945
readonly CONTROL_PORT=18081 GRPC_PORT=18082 DERP_PORT=18443 STUN_PORT=13478
readonly AGENT_TAG=tag:wefty-agent

say() { printf 'tsnet-headscale-lane: %s\n' "$*"; }
die() { say "$*" >&2; exit 1; }

[ "$(uname -s)" = Linux ] || die "Linux only (the Headscale pin is a Linux release binary)"
case "$(uname -m)" in
  x86_64 | amd64) headscale_arch=amd64 headscale_sha256=$HEADSCALE_SHA256_LINUX_AMD64 ;;
  aarch64 | arm64) headscale_arch=arm64 headscale_sha256=$HEADSCALE_SHA256_LINUX_ARM64 ;;
  *) die "unsupported machine $(uname -m)" ;;
esac
[ -f go.mod ] && [ -f scripts/tsnet-headscale-lane.sh ] || die "run from the repository root"
for tool in go curl sha256sum; do
  command -v "$tool" >/dev/null || die "$tool is required"
done
if ! command -v jq >/dev/null; then
  # The golang Debian images lack jq; GitHub's Ubuntu runners have it.
  if [ "$(id -u)" != 0 ] || ! command -v apt-get >/dev/null; then
    die "jq is required"
  fi
  say "installing jq"
  DEBIAN_FRONTEND=noninteractive apt-get update -qq >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends jq >/dev/null
fi

umask 077
root=$(mktemp -d "${TMPDIR:-/tmp}/wefty-headscale.XXXXXX")
headscale_pid='' derper_pid=''

stop_process() {
  local pid=$1
  [ -n "$pid" ] || return 0
  kill "$pid" 2>/dev/null || return 0
  for _ in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local status=$?
  trap - EXIT
  if [ "$status" -ne 0 ]; then
    for log in derper.log headscale.log; do
      if [ -f "$root/$log" ]; then
        echo "--- $log (last 200 lines) ---"
        tail -n 200 "$root/$log" || true
      fi
    done
  fi
  stop_process "$headscale_pid"
  stop_process "$derper_pid"
  rm -rf -- "$root"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# wait_ready NAME PID URL [CURL-ARGS...]: bounded wait for a loopback endpoint.
wait_ready() {
  local name=$1 pid=$2 url=$3
  shift 3
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || die "$name exited before it was ready"
    if curl --fail --silent --max-time 2 "$@" "$url" >/dev/null 2>&1; then
      say "$name ready"
      return 0
    fi
    sleep 1
  done
  die "$name not ready after 60 attempts"
}

# HEADSCALE_CACHE_DIR, when set, keeps the verified binary between runs; CI
# restores it with actions/cache, so a short GitHub release-download outage
# does not fail the lane (#1567). A cached binary gets the same pinned SHA-256
# check as a download, and only a binary that passed it is stored.
headscale_asset=headscale_${HEADSCALE_VERSION}_linux_$headscale_arch
headscale_cached=${HEADSCALE_CACHE_DIR:+$HEADSCALE_CACHE_DIR/$headscale_asset}
if [ -n "$headscale_cached" ] && [ -f "$headscale_cached" ]; then
  say "using the cached Headscale v$HEADSCALE_VERSION ($headscale_arch); no download"
  cp "$headscale_cached" "$root/headscale"
else
  say "downloading Headscale v$HEADSCALE_VERSION ($headscale_arch)"
  # The window rides out a short burst of 5xx answers from the release host;
  # --max-time ends a stalled attempt so that it is retried.
  curl --fail --silent --show-error --location \
    --connect-timeout 20 --max-time 120 --retry 7 --retry-all-errors --retry-max-time 180 \
    -o "$root/headscale" \
    "https://github.com/juanfont/headscale/releases/download/v$HEADSCALE_VERSION/$headscale_asset"
fi
echo "$headscale_sha256  $root/headscale" | sha256sum --check --quiet - || die "Headscale checksum mismatch"
if [ -n "$headscale_cached" ] && [ ! -f "$headscale_cached" ]; then
  mkdir -p "$HEADSCALE_CACHE_DIR"
  cp "$root/headscale" "$headscale_cached"
fi
chmod 700 "$root/headscale"
"$root/headscale" version

# derper follows the tailscale.com pin in go.mod; the Go checksum database
# verifies the module. Build outside the wefty module: its go.sum does not
# list derper's dependencies.
tailscale_version=$(go list -m -f '{{.Version}}' tailscale.com)
say "building derper from tailscale.com $tailscale_version"
(cd "$root" && GOBIN="$root/bin" go install "tailscale.com/cmd/derper@$tailscale_version")

# Loopback only, self-signed TLS that derper creates itself. Not -dev: it
# listens on all interfaces.
mkdir -p "$root/certs"
"$root/bin/derper" -a="127.0.0.1:$DERP_PORT" -http-port=-1 \
  -stun=true -stun-port="$STUN_PORT" \
  -certmode=manual -hostname=127.0.0.1 -certdir="$root/certs" \
  -c="$root/derper.json" -verify-clients=false \
  >"$root/derper.log" 2>&1 &
derper_pid=$!
wait_ready derper "$derper_pid" "https://127.0.0.1:$DERP_PORT/derp/latency-check" --insecure

# insecurefortests trusts the relay's self-signed certificate.
cat >"$root/derp.yaml" <<YAML
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
        derpport: $DERP_PORT
        stunport: $STUN_PORT
        insecurefortests: true
        canport80: false
YAML

cat >"$root/policy.hujson" <<JSON
{
  "tagOwners": {"$AGENT_TAG": ["smoke@"]},
  "acls": [{"action": "accept", "src": ["*"], "dst": ["*:*"]}]
}
JSON

cat >"$root/config.yaml" <<YAML
server_url: http://127.0.0.1:$CONTROL_PORT
listen_addr: 127.0.0.1:$CONTROL_PORT
metrics_listen_addr: ""
grpc_listen_addr: 127.0.0.1:$GRPC_PORT
grpc_allow_insecure: false
noise:
  private_key_path: $root/noise_private.key
prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
  allocation: sequential
derp:
  server:
    enabled: false
  urls: []
  paths: [$root/derp.yaml]
  auto_update_enabled: false
disable_check_updates: true
database:
  type: sqlite
  sqlite:
    path: $root/db.sqlite
    write_ahead_log: true
policy:
  mode: file
  path: $root/policy.hujson
dns:
  magic_dns: true
  base_domain: wefty.test
  override_local_dns: false
  nameservers:
    global: []
    split: {}
  search_domains: []
  extra_records: []
unix_socket: $root/headscale.sock
unix_socket_permission: "0600"
log:
  level: warn
  format: text
logtail:
  enabled: false
YAML

headscale() { NO_COLOR=1 "$root/headscale" --config "$root/config.yaml" "$@"; }

headscale configtest
# The binary itself, not the function, so the PID is the server's.
"$root/headscale" --config "$root/config.yaml" serve >"$root/headscale.log" 2>&1 &
headscale_pid=$!
wait_ready headscale "$headscale_pid" "http://127.0.0.1:$CONTROL_PORT/health"

# Keys are secrets even here: never print them or the CLI output that holds
# them.
user_id=$(headscale users create smoke --output json 2>/dev/null | jq -er '.id') ||
  die "could not create the Headscale user"
create_key() {
  local output
  output=$(headscale preauthkeys create --user "$user_id" --reusable --expiration 2h "$@" --output json 2>/dev/null) ||
    die "could not create a pre-auth key; output withheld"
  jq -er '.key | select(type == "string" and length > 0)' <<<"$output" 2>/dev/null ||
    die "no pre-auth key in the Headscale output; output withheld"
}
tagged_key=$(create_key --tags "$AGENT_TAG")
person_key=$(create_key)
if [ -n "${GITHUB_ACTIONS:-}" ]; then
  echo "::add-mask::$tagged_key"
  echo "::add-mask::$person_key"
fi
say "created user smoke, a $AGENT_TAG pre-auth key and an untagged (person) pre-auth key"

export TS_CONTROL_URL="http://127.0.0.1:$CONTROL_PORT"
export TS_AGENT_PRINCIPAL_TAG=$AGENT_TAG
# Send peer traffic through the local relay; skip router port mapping.
export TS_DEBUG_ALWAYS_USE_DERP=true
export TS_DISABLE_PORTMAPPER=true
unset WEFTY_FABRIC_MACHINE_RECEIPT WEFTY_FABRIC_PERSON_RECEIPT

module=$(go list -m)
# final_action REPORT PACKAGE TEST: the test's last pass, fail or skip event.
final_action() {
  jq -r --arg p "$2" --arg t "$3" \
    'select(.Package == $p and .Test == $t and (.Action == "pass" or .Action == "fail" or .Action == "skip")) | .Action' \
    "$1" | tail -n 1
}

say "running the four tsnet_smoke tests"
report="$root/positive.json"
set +e
TS_AUTHKEY="$tagged_key" TS_AUTHKEY_CI_TESTER="$person_key" \
  go test -json -count=1 -v -p 1 -timeout=8m -tags=tsnet_smoke \
  -run '^TestTSNet(Smoke|PersonWhoAmI|ServiceReachability)$' ./fabric/tsnet ./agent |
  tee "$report" | jq --unbuffered -rj 'select(.Action == "output" or .Action == "build-output") | .Output'
status=("${PIPESTATUS[@]}")
set -e

# A skipped or missing test must never count as success.
failed=0
for row in fabric/tsnet:TestTSNetSmoke fabric/tsnet:TestTSNetPersonWhoAmI \
  agent:TestTSNetSmoke agent:TestTSNetServiceReachability; do
  package=${row%%:*} test=${row#*:}
  result=$(final_action "$report" "$module/$package" "$test")
  printf '%-7s %s.%s\n' "${result:-missing}" "$package" "$test"
  [ "$result" = pass ] || failed=1
done
[ "$failed" -eq 0 ] || die "not every tsnet_smoke test passed"
[ "${status[0]}" -eq 0 ] && [ "${status[1]}" -eq 0 ] && [ "${status[2]}" -eq 0 ] ||
  die "go test or its report pipe failed (statuses ${status[*]})"

echo "--- Headscale nodes: tagged machines show $AGENT_TAG; the person node has no tag ---"
headscale nodes list

# Negative control: the person peer logs in with the tagged key, so L1 must
# classify it as a machine and refuse /v1/whoami. Only that refusal counts;
# any other failure, a skip or a pass fails the lane.
say "negative control: TestTSNetPersonWhoAmI with the tagged key as the person"
negative="$root/negative.json"
set +e
TS_AUTHKEY="$tagged_key" TS_AUTHKEY_CI_TESTER="$tagged_key" \
  go test -json -count=1 -v -timeout=5m -tags=tsnet_smoke \
  -run '^TestTSNetPersonWhoAmI$' ./fabric/tsnet >"$negative"
negative_status=$?
set -e
negative_result=$(final_action "$negative" "$module/fabric/tsnet" TestTSNetPersonWhoAmI)
# person_identity_smoke_test.go: t.Fatalf("whoami status=%d body=%s", ...);
# l1/server.go authorize: "machine principals cannot use person protocols".
# Join the output events before matching: test2json may split one line
# across events, and only this one test runs here. tsnet debug logs contain
# NUL bytes, so grep must treat the stream as text (-a); otherwise GNU grep
# prints only "binary file matches".
identity_line=$(jq -rj --arg p "$module/fabric/tsnet" \
  'select(.Action == "output" and .Package == $p) | .Output' "$negative" |
  grep -aF 'whoami status=403 body=' |
  grep -aF '"code":"principal_forbidden"' |
  grep -aF 'machine principals cannot use person protocols' || true)
if [ "$negative_status" -eq 0 ] || [ "$negative_result" != fail ] || [ -z "$identity_line" ]; then
  jq -rj 'select(.Action == "output" or .Action == "build-output") | .Output' "$negative"
  echo "--- whoami output events ---"
  jq -c 'select(.Action == "output" and (.Output | contains("whoami")))' "$negative"
  die "negative control did not fail on the identity check (go test status $negative_status, result ${negative_result:-missing})"
fi
printf '%s\n' "$identity_line"
echo "negative control: TestTSNetPersonWhoAmI with tagged key failed on the identity check as expected"
