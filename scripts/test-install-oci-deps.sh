#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
installer="$script_dir/install-oci-deps.sh"
manifest_builder="$script_dir/build-oci-install-manifest.sh"
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

run_case() {
  local name="$1" expected_status="$2"; shift 2
  local output status=0
  output="$(env "$@" bash "$installer" --dry-run 2>&1)" || status=$?
  [[ $status == "$expected_status" ]] || fail "$name status=$status output=$output"
  printf '%s\n' "$output"
}

supported_mac="$(run_case supported-mac 0 WEFTY_OCI_INSTALL_TEST_PLATFORM=darwin WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=15.0 WEFTY_OCI_INSTALL_TEST_ARCH=arm64 WEFTY_OCI_INSTALL_TEST_VZ=yes WEFTY_OCI_INSTALL_TEST_LIMA_VERSION=)"
grep -Fq 'DRY-RUN: brew install lima' <<<"$supported_mac" || fail "supported mac did not plan Lima"
grep -Fq '1. wefty node setup-oci' <<<"$supported_mac" || fail "supported mac omitted setup before doctor"

rerun_linux="$(run_case rerun-linux 0 WEFTY_OCI_INSTALL_TEST_PLATFORM=linux WEFTY_OCI_INSTALL_TEST_DISTRO_ID=debian WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=13 WEFTY_OCI_INSTALL_TEST_ARCH=amd64 WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION=2.3.4 WEFTY_OCI_INSTALL_TEST_RUNC_VERSION=1.5.1 WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT=/usr/lib/systemd/system/containerd.service)"
grep -Fq 'containerd 2.3.4 already satisfies' <<<"$rerun_linux" || fail "rerun did not preserve containerd"
grep -Fq 'runc 1.5.1 already satisfies' <<<"$rerun_linux" || fail "rerun did not preserve runc"
grep -Fq 'preserve packaged containerd unit at /usr/lib/systemd/system/containerd.service' <<<"$rerun_linux" || fail "distro unit was not preserved"
grep -Fq '1. sudo wefty node setup-oci' <<<"$rerun_linux" || fail "Linux next-command order is wrong"

unknown="$(run_case unknown-release 64 WEFTY_OCI_INSTALL_TEST_PLATFORM=linux WEFTY_OCI_INSTALL_TEST_DISTRO_ID=arch WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=rolling WEFTY_OCI_INSTALL_TEST_ARCH=amd64 WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION= WEFTY_OCI_INSTALL_TEST_RUNC_VERSION= WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT=none)"
grep -Fq 'unsupported Linux release' <<<"$unknown" || fail "unknown release was not refused"

below_minimum="$(run_case below-minimum 64 WEFTY_OCI_INSTALL_TEST_PLATFORM=linux WEFTY_OCI_INSTALL_TEST_DISTRO_ID=ubuntu WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=24.04 WEFTY_OCI_INSTALL_TEST_ARCH=amd64 WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION=1.7.27 WEFTY_OCI_INSTALL_TEST_RUNC_VERSION=1.2.6 WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT=none)"
grep -Fq 'below the supported minimum 2.0.0' <<<"$below_minimum" || fail "below-minimum runtime was not refused"

supported_linux="$(run_case supported-linux 0 WEFTY_OCI_INSTALL_TEST_PLATFORM=linux WEFTY_OCI_INSTALL_TEST_DISTRO_ID=fedora WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=44 WEFTY_OCI_INSTALL_TEST_ARCH=arm64 WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION= WEFTY_OCI_INSTALL_TEST_RUNC_VERSION= WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT=none)"
for expected in \
  'containerd-2.3.4-linux-arm64.tar.gz' \
  '/usr/local/lib/wefty/oci-runtime/containerd-2.3.4' \
  '/usr/local/lib/wefty/oci-runtime/runc-1.5.1/runc' \
  'root:root 0644' \
  'ExecStart=/usr/local/lib/wefty/oci-runtime/containerd-2.3.4/bin/containerd; Type=notify; Restart=always' \
  'No services were started'; do
  grep -Fq "$expected" <<<"$supported_linux" || fail "supported Linux plan omitted $expected"
done

root="$(mktemp -d "${TMPDIR:-/tmp}/wefty-manifest-test.XXXXXX")"
trap 'rm -rf -- "$root"' EXIT
printf 'helper' >"$root/helper"
printf 'probe' >"$root/probe.oci.tar"
bash "$manifest_builder" --helper "$root/helper" --probe-reference wefty.local/probe --probe-digest sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --probe-archive "$root/probe.oci.tar" --output "$root/share/wefty/oci/manifest.json" >/dev/null
grep -Fq '"helper_checksum": "sha256:' "$root/share/wefty/oci/manifest.json" || fail "manifest helper checksum missing"
grep -Fq '"probe_archive_path": "probe.oci.tar"' "$root/share/wefty/oci/manifest.json" || fail "manifest archive is not relocatable"

if grep -En 'curl.*\|.*(sh|bash)|apt(-get)?-repository|dnf config-manager|(brew|apt|dnf).*(cni|CNI)|systemctl[[:space:]]+(start|enable|restart)' "$installer"; then
  fail "installer widened its prerequisite, repository, or configure-only boundary"
fi
printf 'install-oci-deps matrix: PASS\n'
