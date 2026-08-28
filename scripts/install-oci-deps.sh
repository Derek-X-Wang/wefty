#!/usr/bin/env bash
set -euo pipefail

script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
# shellcheck disable=SC1091
. "$script_dir/oci-tested-versions.env"

PATH="/usr/local/sbin:/usr/local/bin:$PATH"
export PATH

dry_run=0

usage() {
  cat <<'EOF'
Usage: scripts/install-oci-deps.sh [--dry-run]

Install only the OCI runtime prerequisites supported by wefty. The script
never starts services, installs CNI, adds package repositories, or changes
wefty configuration. --dry-run prints the exact planned mutations.
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 64
}

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

while (($# > 0)); do
  case "$1" in
    --dry-run) dry_run=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

if ((dry_run == 0)); then
  for name in PLATFORM DISTRO_ID DISTRO_VERSION ARCH LIMA_VERSION CONTAINERD_VERSION RUNC_VERSION OVERLAYFS VZ; do
    variable="WEFTY_OCI_INSTALL_TEST_${name}"
    if [[ -n ${!variable+x} ]]; then
      die "$variable is accepted only with --dry-run"
    fi
  done
fi

tested_version() {
  local wanted="$1" pair
  # The value is a deliberately small, fixed key=value list.
  # shellcheck disable=SC2086
  for pair in $WEFTY_OCI_TESTED_VERSIONS; do
    if [[ ${pair%%=*} == "$wanted" ]]; then
      printf '%s\n' "${pair#*=}"
      return 0
    fi
  done
  die "tested version is missing for $wanted"
}

LIMA_TESTED="$(tested_version lima)"
CONTAINERD_TESTED="$(tested_version containerd)"
RUNC_TESTED="$(tested_version runc)"
LIMA_MINIMUM=2.2.0
CONTAINERD_MINIMUM=2.0.0
RUNC_MINIMUM=1.0.0

parse_version() {
  local value="$1"
  value="${value#v}"
  if [[ $value =~ ^([0-9]+)\.([0-9]+)(\.([0-9]+))? ]]; then
    printf '%s %s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[4]:-0}"
    return 0
  fi
  return 1
}

version_at_least() {
  local actual="$1" minimum="$2"
  local actual_parts minimum_parts
  actual_parts="$(parse_version "$actual")" || return 1
  minimum_parts="$(parse_version "$minimum")" || return 1
  local a_major a_minor a_patch m_major m_minor m_patch
  read -r a_major a_minor a_patch <<<"$actual_parts"
  read -r m_major m_minor m_patch <<<"$minimum_parts"
  ((a_major > m_major)) && return 0
  ((a_major < m_major)) && return 1
  ((a_minor > m_minor)) && return 0
  ((a_minor < m_minor)) && return 1
  ((a_patch >= m_patch))
}

version_major() {
  local parts
  parts="$(parse_version "$1")" || return 1
  printf '%s\n' "${parts%% *}"
}

platform_value() {
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then
    printf '%s\n' "$WEFTY_OCI_INSTALL_TEST_PLATFORM"
  else
    uname -s | tr '[:upper:]' '[:lower:]'
  fi
}

arch_value() {
  local value
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_ARCH+x} ]]; then
    value="$WEFTY_OCI_INSTALL_TEST_ARCH"
  else
    value="$(uname -m)"
  fi
  case "$value" in
    x86_64|amd64) printf 'amd64\n' ;;
    arm64|aarch64) printf 'arm64\n' ;;
    *) die "unsupported architecture: $value" ;;
  esac
}

probe_version() {
  local test_name="$1" command_name="$2" pattern="$3" output
  local variable="WEFTY_OCI_INSTALL_TEST_${test_name}"
  if [[ -n ${!variable+x} ]]; then
    printf '%s\n' "${!variable}"
    return 0
  fi
  if ! command -v "$command_name" >/dev/null 2>&1; then
    return 0
  fi
  output="$($command_name --version 2>&1 || true)"
  sed -nE "$pattern" <<<"$output" | head -n 1
}

require_minimum() {
  local name="$1" actual="$2" minimum="$3"
  [[ -z $actual ]] && return 0
  if ! version_at_least "$actual" "$minimum"; then
    die "$name $actual is below the supported minimum $minimum; no changes made"
  fi
}

print_next_commands() {
  local setup_command="$1"
  printf '%s\n' \
    'OCI prerequisites are ready. No services were started and no wefty configuration was changed.' \
    'Next commands:' \
    "  $setup_command" \
    '  wefty node doctor'
}

install_macos() {
  local lima_version macos_version vz_available
  if ! command -v brew >/dev/null 2>&1; then
    if ((dry_run)) && [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then
      :
    else
      die "Homebrew is required; install it separately, then rerun this script"
    fi
  fi
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION+x} ]]; then
    macos_version="$WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION"
  else
    macos_version="$(sw_vers -productVersion)"
  fi
  version_at_least "$macos_version" 13.5.0 || die "macOS $macos_version does not provide the supported Lima vz baseline (13.5 or newer)"
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_VZ+x} ]]; then
    vz_available="$WEFTY_OCI_INSTALL_TEST_VZ"
  else
    vz_available="$(sysctl -n kern.hv_support 2>/dev/null || printf '0')"
  fi
  [[ $vz_available == 1 || $vz_available == yes ]] || die "Apple virtualization support is unavailable; Lima vz cannot run"
  lima_version="$(probe_version LIMA_VERSION limactl 's/.*[^0-9]([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
  require_minimum Lima "$lima_version" "$LIMA_MINIMUM"
  if [[ -z $lima_version ]]; then
    if ((dry_run)); then
      printf 'DRY-RUN: brew install lima\n'
      lima_version="$LIMA_TESTED"
    else
      HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 brew install lima
      lima_version="$(probe_version LIMA_VERSION limactl 's/.*[^0-9]([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
      require_minimum Lima "$lima_version" "$LIMA_MINIMUM"
      [[ -n $lima_version ]] || die "Lima installation completed but limactl is unavailable"
    fi
  else
    printf 'Lima %s already satisfies the supported minimum.\n' "$lima_version"
  fi
  [[ $lima_version == "$LIMA_TESTED" ]] || warn "Lima $lima_version is supported but outside the tested version $LIMA_TESTED"
  print_next_commands 'wefty node setup-oci'
}

read_linux_release() {
  local key="$1" os_release_key
  local variable="WEFTY_OCI_INSTALL_TEST_${key}"
  if [[ -n ${!variable+x} ]]; then
    printf '%s\n' "${!variable}"
    return 0
  fi
  [[ -r /etc/os-release ]] || die "/etc/os-release is required on Linux"
  case "$key" in
    DISTRO_ID) os_release_key=ID ;;
    DISTRO_VERSION) os_release_key=VERSION_ID ;;
    *) die "unknown os-release field: $key" ;;
  esac
  sed -nE "s/^${os_release_key}=['\"]?([^'\"]+)['\"]?$/\\1/p" /etc/os-release | head -n 1
}

assert_supported_linux_release() {
  local id="$1" version="$2"
  case "$id:$version" in
    ubuntu:24.04|ubuntu:26.04|debian:12|debian:13|fedora:43|fedora:44) ;;
    *) die "unsupported Linux release: $id $version; supported: Ubuntu 24.04/26.04, Debian 12/13, Fedora 43/44" ;;
  esac
}

root_install() {
  if ((EUID == 0)); then
    install "$@"
  else
    command -v sudo >/dev/null 2>&1 || die "root installation requires sudo"
    sudo install "$@"
  fi
}

install_containerd_bundle() (
  local arch="$1" temporary archive checksum base_url binary target
  temporary="$(mktemp -d "${TMPDIR:-/tmp}/wefty-oci-deps.XXXXXX")"
  trap 'rm -rf -- "$temporary"' EXIT
  archive="containerd-${CONTAINERD_TESTED}-linux-${arch}.tar.gz"
  checksum="${archive}.sha256sum"
  base_url="https://github.com/containerd/containerd/releases/download/v${CONTAINERD_TESTED}"
  curl --fail --location --proto '=https' --tlsv1.2 --output "$temporary/$archive" "$base_url/$archive"
  curl --fail --location --proto '=https' --tlsv1.2 --output "$temporary/$checksum" "$base_url/$checksum"
  (cd "$temporary" && sha256sum --check "$checksum")
  tar --extract --gzip --file "$temporary/$archive" --directory "$temporary"
  for binary in containerd containerd-shim-runc-v2 ctr; do
    target="/usr/local/bin/$binary"
    [[ ! -e $target ]] || die "refusing to overwrite existing $target"
  done
  for binary in containerd containerd-shim-runc-v2 ctr; do
    root_install --owner=root --group=root --mode=0755 "$temporary/bin/$binary" "/usr/local/bin/$binary"
  done
)

install_runc_binary() (
  local arch="$1" temporary binary checksum_url base_url
  temporary="$(mktemp -d "${TMPDIR:-/tmp}/wefty-oci-deps.XXXXXX")"
  trap 'rm -rf -- "$temporary"' EXIT
  binary="runc.$arch"
  base_url="https://github.com/opencontainers/runc/releases/download/v${RUNC_TESTED}"
  checksum_url="$base_url/runc.sha256sum"
  curl --fail --location --proto '=https' --tlsv1.2 --output "$temporary/$binary" "$base_url/$binary"
  curl --fail --location --proto '=https' --tlsv1.2 --output "$temporary/runc.sha256sum" "$checksum_url"
  (cd "$temporary" && grep "  $binary\$" runc.sha256sum | sha256sum --check)
  [[ ! -e /usr/local/sbin/runc ]] || die "refusing to overwrite existing /usr/local/sbin/runc"
  root_install --owner=root --group=root --mode=0755 "$temporary/$binary" /usr/local/sbin/runc
)

install_containerd_unit_if_absent() (
  local path temporary
  for path in /etc/systemd/system/containerd.service /usr/lib/systemd/system/containerd.service /lib/systemd/system/containerd.service; do
    [[ -e $path ]] && exit 0
  done
  if ((dry_run)); then
    printf 'DRY-RUN: install inactive containerd.service at /etc/systemd/system/containerd.service\n'
    exit 0
  fi
  temporary="$(mktemp "${TMPDIR:-/tmp}/wefty-containerd-unit.XXXXXX")"
  trap 'rm -f -- "$temporary"' EXIT
  cat >"$temporary" <<'EOF'
[Unit]
Description=containerd container runtime
Documentation=https://containerd.io
After=network.target local-fs.target

[Service]
ExecStart=/usr/local/bin/containerd
Delegate=yes
KillMode=process
OOMScoreAdjust=-999
LimitNOFILE=1048576
LimitNPROC=infinity
LimitCORE=infinity
TasksMax=infinity

[Install]
WantedBy=multi-user.target
EOF
  root_install --owner=root --group=root --mode=0644 "$temporary" /etc/systemd/system/containerd.service
)

install_linux() {
  local id version arch containerd_version runc_version overlayfs
  id="$(read_linux_release DISTRO_ID)"
  version="$(read_linux_release DISTRO_VERSION)"
  assert_supported_linux_release "$id" "$version"
  arch="$(arch_value)"
  for command_name in curl tar sha256sum install grep; do
    command -v "$command_name" >/dev/null 2>&1 || die "$command_name is required before installing OCI prerequisites"
  done
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_OVERLAYFS+x} ]]; then
    overlayfs="$WEFTY_OCI_INSTALL_TEST_OVERLAYFS"
  elif grep -qw overlay /proc/filesystems; then
    overlayfs=yes
  else
    overlayfs=no
  fi
  [[ $overlayfs == yes || $overlayfs == 1 ]] || die "overlayfs is unavailable; load or enable it before rerunning"

  containerd_version="$(probe_version CONTAINERD_VERSION containerd 's/.*[[:space:]]v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
  runc_version="$(probe_version RUNC_VERSION runc 's/^runc version v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
  require_minimum containerd "$containerd_version" "$CONTAINERD_MINIMUM"
  require_minimum runc "$runc_version" "$RUNC_MINIMUM"
  if [[ -n $runc_version && $(version_major "$runc_version") != 1 ]]; then
    die "runc $runc_version is outside the supported 1.x major; no changes made"
  fi

  if [[ -z $containerd_version ]]; then
    if ((dry_run)); then
      printf 'DRY-RUN: install containerd %s from its checksummed upstream release archive\n' "$CONTAINERD_TESTED"
      containerd_version="$CONTAINERD_TESTED"
    else
      install_containerd_bundle "$arch"
      containerd_version="$(probe_version CONTAINERD_VERSION containerd 's/.*[[:space:]]v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
      [[ $containerd_version == "$CONTAINERD_TESTED" ]] || die "installed containerd version is $containerd_version, expected $CONTAINERD_TESTED"
    fi
  else
    printf 'containerd %s already satisfies the supported minimum.\n' "$containerd_version"
  fi

  if [[ -z $runc_version ]]; then
    if ((dry_run)); then
      printf 'DRY-RUN: install runc %s from its checksummed upstream release binary\n' "$RUNC_TESTED"
      runc_version="$RUNC_TESTED"
    else
      install_runc_binary "$arch"
      runc_version="$(probe_version RUNC_VERSION runc 's/^runc version v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
      [[ $runc_version == "$RUNC_TESTED" ]] || die "installed runc version is $runc_version, expected $RUNC_TESTED"
    fi
  else
    printf 'runc %s already satisfies the supported major.\n' "$runc_version"
  fi

  [[ $containerd_version == "$CONTAINERD_TESTED" ]] || warn "containerd $containerd_version is supported but outside the tested version $CONTAINERD_TESTED"
  [[ $runc_version == "$RUNC_TESTED" ]] || warn "runc $runc_version is supported but outside the tested version $RUNC_TESTED"
  install_containerd_unit_if_absent
  print_next_commands 'sudo wefty node setup-oci'
}

case "$(platform_value)" in
  darwin) install_macos ;;
  linux) install_linux ;;
  *) die "unsupported platform; only macOS and named Ubuntu, Debian, or Fedora releases are supported" ;;
esac
