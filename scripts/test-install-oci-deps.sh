#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
installer="$script_dir/install-oci-deps.sh"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

run_case() {
  local name="$1" expected_status="$2"
  shift 2
  local output status=0
  output="$(env "$@" bash "$installer" --dry-run 2>&1)" || status=$?
  [[ $status == "$expected_status" ]] || fail "$name status=$status output=$output"
  printf '%s\n' "$output"
}

supported_mac="$(run_case supported-mac 0 \
  WEFTY_OCI_INSTALL_TEST_PLATFORM=darwin \
  WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=15.0 \
  WEFTY_OCI_INSTALL_TEST_ARCH=arm64 \
  WEFTY_OCI_INSTALL_TEST_VZ=yes \
  WEFTY_OCI_INSTALL_TEST_LIMA_VERSION=)"
grep -Fq 'DRY-RUN: brew install lima' <<<"$supported_mac" || fail "supported mac did not plan Lima"
grep -Fq 'wefty node setup-oci' <<<"$supported_mac" || fail "supported mac omitted next setup command"

rerun_linux="$(run_case rerun-linux 0 \
  WEFTY_OCI_INSTALL_TEST_PLATFORM=linux \
  WEFTY_OCI_INSTALL_TEST_DISTRO_ID=debian \
  WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=13 \
  WEFTY_OCI_INSTALL_TEST_ARCH=amd64 \
  WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes \
  WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION=2.3.4 \
  WEFTY_OCI_INSTALL_TEST_RUNC_VERSION=1.5.1)"
grep -Fq 'containerd 2.3.4 already satisfies' <<<"$rerun_linux" || fail "rerun did not preserve containerd"
grep -Fq 'runc 1.5.1 already satisfies' <<<"$rerun_linux" || fail "rerun did not preserve runc"
if grep -Fq 'install containerd 2.3.4' <<<"$rerun_linux"; then
  fail "rerun planned a containerd reinstall"
fi

unknown="$(run_case unknown-release 64 \
  WEFTY_OCI_INSTALL_TEST_PLATFORM=linux \
  WEFTY_OCI_INSTALL_TEST_DISTRO_ID=arch \
  WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=rolling \
  WEFTY_OCI_INSTALL_TEST_ARCH=amd64 \
  WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes \
  WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION= \
  WEFTY_OCI_INSTALL_TEST_RUNC_VERSION=)"
grep -Fq 'unsupported Linux release' <<<"$unknown" || fail "unknown release was not refused"

below_minimum="$(run_case below-minimum 64 \
  WEFTY_OCI_INSTALL_TEST_PLATFORM=linux \
  WEFTY_OCI_INSTALL_TEST_DISTRO_ID=ubuntu \
  WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=24.04 \
  WEFTY_OCI_INSTALL_TEST_ARCH=amd64 \
  WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes \
  WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION=1.7.27 \
  WEFTY_OCI_INSTALL_TEST_RUNC_VERSION=1.2.6)"
grep -Fq 'below the supported minimum 2.0.0' <<<"$below_minimum" || fail "below-minimum runtime was not refused"

supported_linux="$(run_case supported-linux 0 \
  WEFTY_OCI_INSTALL_TEST_PLATFORM=linux \
  WEFTY_OCI_INSTALL_TEST_DISTRO_ID=fedora \
  WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION=44 \
  WEFTY_OCI_INSTALL_TEST_ARCH=arm64 \
  WEFTY_OCI_INSTALL_TEST_OVERLAYFS=yes \
  WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION= \
  WEFTY_OCI_INSTALL_TEST_RUNC_VERSION=)"
grep -Fq 'install containerd 2.3.4' <<<"$supported_linux" || fail "supported Linux did not use the tested containerd pin"
grep -Fq 'install runc 1.5.1' <<<"$supported_linux" || fail "supported Linux did not use the tested runc pin"
grep -Fq 'No services were started' <<<"$supported_linux" || fail "configure-only promise missing"

if grep -En 'curl.*\|.*(sh|bash)|apt(-get)?-repository|dnf config-manager|(brew|apt|dnf).*(cni|CNI)' "$installer"; then
  fail "installer widened its prerequisite or repository boundary"
fi

printf 'install-oci-deps matrix: PASS\n'
