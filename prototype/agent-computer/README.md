# One-Mac agent-computer prototype

This directory contains the throwaway prototype for ticket #118. It runs the
same XFCE, Chromium, x11vnc, and noVNC workload in either a desktop container
inside a shared Lima VM or a dedicated Lima VM. The browser profile is a stable
guest path while each container is a disposable attempt.

The Python gate is deliberately small prototype code, not a production Fabric
implementation. It binds only the authenticated role endpoints to the tailnet
IP and keeps Lima's raw noVNC forwards on host loopback. A controller is routed
to x11vnc's input-capable RFB endpoint; a viewer is routed to a separate
server-side `-viewonly` endpoint.

## Reproduce the shared-VM topology

Prerequisites are Lima 2.2 or newer, Python 3, and an ARM64 Mac. From the repo
root:

```sh
limactl create --name=wefty-proto-118 prototype/agent-computer/lima-shared.yaml
limactl start wefty-proto-118

limactl shell wefty-proto-118 -- \
  sudo nerdctl --namespace wefty-118 build \
  -t wefty-agent-computer:proto \
  "$PWD/prototype/agent-computer/container"

limactl shell wefty-proto-118 -- \
  sudo mkdir -p /var/lib/wefty-agent-computer/profiles/computer-0
limactl shell wefty-proto-118 -- \
  sudo nerdctl --namespace wefty-118 run -d \
  --name computer-0-attempt-1 \
  --label wefty.computer=computer-0 --label wefty.attempt=1 \
  -e COMPUTER_INDEX=0 \
  -p 127.0.0.1:16080:6080 -p 127.0.0.1:16081:6081 \
  -v /var/lib/wefty-agent-computer/profiles/computer-0:/profile \
  wefty-agent-computer:proto
```

Resolve the Mac's tailnet IP with
`/Applications/Tailscale.app/Contents/MacOS/Tailscale ip -4`, falling back to
`127.0.0.1` when Tailscale is unavailable. Run the gate on the Mac:

```sh
python3 prototype/agent-computer/fabric_gate.py \
  --bind 100.68.208.71 --count 1 \
  --control-token control-118 --view-token viewer-118
```

For this throwaway run, the controller URL is
`http://100.68.208.71:19080/login?token=control-118` and the view-only URL is
`http://100.68.208.71:19180/login?token=viewer-118`. Replace the recorded IP
with the reproducing Mac's IP. These static tokens are disposable prototype
capabilities, not secrets suitable for production.

To create computers 1 through 3, increment `COMPUTER_INDEX`, container name,
profile path, and both guest mappings by two (`16082:6082`, `16083:6083`, and
so on), then start the gate with `--count 4`.

## Reproduce the micro-VM topology

```sh
limactl create --name=wefty-proto-118-micro \
  prototype/agent-computer/lima-micro.yaml
limactl start wefty-proto-118-micro
```

Build and run the identical payload as above, using the micro VM name and host
ports 17080/17081. This isolates the topology comparison from desktop-stack
differences. The dedicated VM reserves 3 GiB and is intentionally stopped in
the delivered inspection state.

## Probes and removal

`rfb_probe.py` is a dependency-free RFB 3.8 raw client. Run it inside the guest
against a container's RFB address to inject text, save a PPM framebuffer, or
measure a first-framebuffer-difference latency:

```sh
python3 prototype/agent-computer/rfb_probe.py HOST PORT --text probe
python3 prototype/agent-computer/rfb_probe.py HOST PORT --latency
python3 prototype/agent-computer/rfb_probe.py HOST PORT --ppm screen.ppm
```

Primary memory and CPU measurements come from inside the VM, never Lima
hostagent RSS:

```sh
limactl shell wefty-proto-118 -- free -m
limactl shell wefty-proto-118 -- vmstat 1 6
limactl shell wefty-proto-118 -- \
  sudo nerdctl --namespace wefty-118 stats --no-stream
limactl shell wefty-proto-118 -- \
  sudo du -sk /var/lib/wefty-agent-computer/profiles /var/lib/containerd
vm_stat
pmset -g therm
```

Removal is an explicit two-part operation: remove the attempt, then remove the
stable profile. Verify that no container, task, active snapshot, profile path,
or nerdctl state directory matches the removed computer ID. The shared image
cache is intentionally outside the per-computer deletion boundary.

```sh
limactl shell wefty-proto-118 -- \
  sudo nerdctl --namespace wefty-118 rm -f computer-0-attempt-1
limactl shell wefty-proto-118 -- \
  sudo find /var/lib/wefty-agent-computer/profiles/computer-0 -depth -delete
```

See [VERDICT.md](VERDICT.md) for the observed results, exact limitations, and
the owner-facing recommendation.
