#!/usr/bin/env bash
set -euo pipefail

command -v docker >/dev/null 2>&1 || { printf 'docker is required for the privileged OCI install acceptance\n' >&2; exit 1; }

repo_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
artifacts="$(mktemp -d "${TMPDIR:-/tmp}/wefty-oci-privileged.XXXXXX")"
trap 'rm -rf -- "$artifacts"' EXIT
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$artifacts/wefty" ./cmd/wefty
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$artifacts/wefty-agent" ./cmd/wefty-agent

docker run --rm --privileged --platform linux/amd64 --volume "$repo_root:/src:ro" --volume "$artifacts:/artifacts:ro" ubuntu:24.04 bash -s <<'CONTAINER'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -qq -y ca-certificates curl coreutils tar sudo systemd util-linux >/dev/null
install -d -m 0755 /usr/local/bin /usr/local/libexec /usr/local/share/wefty/oci /var/lib/wefty /srv/wefty /test-bin
install -m 0755 /artifacts/wefty /usr/local/bin/wefty
install -m 0755 /artifacts/wefty-agent /usr/local/libexec/wefty-agent
printf 'probe archive fixture\n' >/tmp/wefty-probe.oci.tar
bash /src/scripts/build-oci-install-manifest.sh \
  --helper /usr/local/libexec/wefty-agent \
  --probe-reference wefty.local/probe \
  --probe-digest sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --probe-archive /tmp/wefty-probe.oci.tar \
  --output /usr/local/share/wefty/oci/manifest.json >/dev/null

test ! -e /usr/local/lib/wefty/oci-runtime
bash /src/scripts/install-oci-deps.sh >/tmp/install-first.log
test -x /usr/local/lib/wefty/oci-runtime/containerd-2.3.4/bin/containerd
test -x /usr/local/lib/wefty/oci-runtime/containerd-2.3.4/bin/containerd-shim-runc-v2
test -x /usr/local/lib/wefty/oci-runtime/containerd-2.3.4/bin/ctr
test -x /usr/local/lib/wefty/oci-runtime/runc-1.5.1/runc
grep -Fxq 'Type=notify' /etc/systemd/system/containerd.service
grep -Fxq 'ExecStart=/usr/local/lib/wefty/oci-runtime/containerd-2.3.4/bin/containerd' /etc/systemd/system/containerd.service
grep -Fxq 'Restart=always' /etc/systemd/system/containerd.service
find /usr/local/lib/wefty/oci-runtime /etc/systemd/system/containerd.service -type f -exec sha256sum {} + | sort -k2 >/tmp/install-before.sha256
bash /src/scripts/install-oci-deps.sh >/tmp/install-second.log
find /usr/local/lib/wefty/oci-runtime /etc/systemd/system/containerd.service -type f -exec sha256sum {} + | sort -k2 >/tmp/install-after.sha256
cmp /tmp/install-before.sha256 /tmp/install-after.sha256

useradd --create-home operator
printf 'operator ALL=(root) NOPASSWD: ALL\n' >/etc/sudoers.d/operator
chmod 0440 /etc/sudoers.d/operator
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$*" >>/tmp/systemctl-called\n+exit 99\n' >/test-bin/systemctl
chmod 0755 /test-bin/systemctl
su - operator -c 'sudo env PATH=/test-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin wefty node setup-oci' >/tmp/setup-first.log
test -f /etc/systemd/system/wefty-agent.service
test -f /etc/systemd/system/wefty-oci-helper.socket
test -f /etc/systemd/system/wefty-oci-helper.service
test -f /home/operator/.config/wefty/node.json
test ! -e /tmp/systemctl-called
grep -Fq 'systemctl daemon-reload' /tmp/setup-first.log
find /etc/systemd/system/wefty-agent.service /etc/systemd/system/wefty-oci-helper.socket /etc/systemd/system/wefty-oci-helper.service /home/operator/.config/wefty/node.json /var/lib/wefty/oci-intent.json /var/lib/wefty/oci-setup.json /var/lib/wefty/oci-setup.json.desired -type f -exec sha256sum {} + | sort -k2 >/tmp/setup-before.sha256
su - operator -c 'sudo env PATH=/test-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin wefty node setup-oci' >/tmp/setup-second.log
find /etc/systemd/system/wefty-agent.service /etc/systemd/system/wefty-oci-helper.socket /etc/systemd/system/wefty-oci-helper.service /home/operator/.config/wefty/node.json /var/lib/wefty/oci-intent.json /var/lib/wefty/oci-setup.json /var/lib/wefty/oci-setup.json.desired -type f -exec sha256sum {} + | sort -k2 >/tmp/setup-after.sha256
cmp /tmp/setup-before.sha256 /tmp/setup-after.sha256
test ! -e /tmp/systemctl-called
printf 'privileged OCI install acceptance: PASS\n'
CONTAINER
