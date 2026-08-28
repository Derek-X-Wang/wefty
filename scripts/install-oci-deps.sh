#!/usr/bin/env bash
set -euo pipefail

script_path="${BASH_SOURCE[0]}"
[[ $script_path == */* ]] || script_path="./$script_path"
script_dir="$(CDPATH='' cd -- "${script_path%/*}" && pwd)"
# shellcheck disable=SC1091
. "$script_dir/oci-tested-versions.env"
dry_run=0
repair=0
declare -a written_paths=()
declare -a temporary_paths=()

cleanup() {
  if [[ -n ${RM_BIN:-} && -x ${RM_BIN:-} && ${#temporary_paths[@]} -gt 0 ]]; then
    "$RM_BIN" -rf -- "${temporary_paths[@]}"
  fi
}

usage() {
  printf '%s\n' \
    'Usage: scripts/install-oci-deps.sh [--dry-run] [--repair]' '' \
    'Install only the OCI runtime prerequisites supported by wefty. The script' \
    'never starts services, installs CNI, adds package repositories, or changes' \
    'wefty configuration. --dry-run prints exact sources, destinations, owners,' \
    'and modes. --repair explicitly permits replacement of conflicting paths in' \
    'the disclosed wefty-managed install set.'
}

die() { local status="$1"; shift; printf 'ERROR: %s\n' "$*" >&2; exit "$status"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }

while (($# > 0)); do
  case "$1" in
    --dry-run) dry_run=1 ;;
    --repair) repair=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die 64 "unknown argument: $1" ;;
  esac
  shift
done

if ((dry_run == 0)); then
  for name in PLATFORM DISTRO_ID DISTRO_VERSION ARCH LIMA_VERSION CONTAINERD_VERSION RUNC_VERSION OVERLAYFS VZ PACKAGED_UNIT; do
    variable="WEFTY_OCI_INSTALL_TEST_${name}"
    [[ -z ${!variable+x} ]] || die 64 "$variable is accepted only with --dry-run"
  done
fi

version_from_set() {
  local set="$1" wanted="$2" pair
  # shellcheck disable=SC2086
  for pair in $set; do
    if [[ ${pair%%=*} == "$wanted" ]]; then printf '%s\n' "${pair#*=}"; return; fi
  done
  die 64 "version is missing for $wanted"
}

LIMA_TESTED="$(version_from_set "$WEFTY_OCI_TESTED_VERSIONS" lima)"
CONTAINERD_TESTED="$(version_from_set "$WEFTY_OCI_TESTED_VERSIONS" containerd)"
RUNC_TESTED="$(version_from_set "$WEFTY_OCI_TESTED_VERSIONS" runc)"
LIMA_MINIMUM="$(version_from_set "$WEFTY_OCI_MINIMUM_VERSIONS" lima)"
CONTAINERD_MINIMUM="$(version_from_set "$WEFTY_OCI_MINIMUM_VERSIONS" containerd)"
RUNC_MINIMUM="$(version_from_set "$WEFTY_OCI_MINIMUM_VERSIONS" runc)"

parse_version() {
  local value="${1#v}"
  [[ $value =~ ^([0-9]+)\.([0-9]+)(\.([0-9]+))? ]] || return 1
  printf '%s %s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[4]:-0}"
}

version_at_least() {
  local actual_parts minimum_parts a_major a_minor a_patch m_major m_minor m_patch
  actual_parts="$(parse_version "$1")" || return 1
  minimum_parts="$(parse_version "$2")" || return 1
  read -r a_major a_minor a_patch <<<"$actual_parts"
  read -r m_major m_minor m_patch <<<"$minimum_parts"
  ((a_major > m_major)) && return 0
  ((a_major < m_major)) && return 1
  ((a_minor > m_minor)) && return 0
  ((a_minor < m_minor)) && return 1
  ((a_patch >= m_patch))
}

version_major() { parse_version "$1" | { read -r major _; printf '%s\n' "$major"; }; }

platform_value() {
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then printf '%s\n' "$WEFTY_OCI_INSTALL_TEST_PLATFORM"
  else case "$OSTYPE" in darwin*) printf 'darwin\n' ;; linux*) printf 'linux\n' ;; *) printf '%s\n' "$OSTYPE" ;; esac; fi
}

arch_value() {
  local value="${WEFTY_OCI_INSTALL_TEST_ARCH:-${HOSTTYPE:-}}"
  case "$value" in x86_64|amd64) printf 'amd64\n' ;; arm64|aarch64) printf 'arm64\n' ;; *) die 64 "unsupported architecture: $value" ;; esac
}

probe_version() {
  local test_name="$1" executable="$2" output variable="WEFTY_OCI_INSTALL_TEST_${1}"
  if [[ -n ${!variable+x} ]]; then printf '%s\n' "${!variable}"; return; fi
  [[ -n $executable && -x $executable ]] || return
  output="$($executable --version 2>&1 || true)"
  case "$test_name" in
    LIMA_VERSION) [[ $output =~ ([0-9]+\.[0-9]+\.[0-9]+) ]] && printf '%s\n' "${BASH_REMATCH[1]}" ;;
    CONTAINERD_VERSION) [[ $output =~ [[:space:]]v?([0-9]+\.[0-9]+\.[0-9]+) ]] && printf '%s\n' "${BASH_REMATCH[1]}" ;;
    RUNC_VERSION) [[ $output =~ runc[[:space:]]version[[:space:]]v?([0-9]+\.[0-9]+\.[0-9]+) ]] && printf '%s\n' "${BASH_REMATCH[1]}" ;;
    *) die 64 "unknown version probe: $test_name" ;;
  esac
  return 0
}

require_minimum() {
  local name="$1" actual="$2" minimum="$3"
  [[ -z $actual ]] && return
  version_at_least "$actual" "$minimum" || die 64 "$name $actual is below the supported minimum $minimum; no changes made"
}

print_next_commands() {
  local setup="$1"
  printf '%s\n' \
    'OCI prerequisites are ready. No services were started and no wefty configuration was changed.' \
    'Next, after installing the matching wefty release (including share/wefty/oci/manifest.json):' \
    "  1. $setup" \
    '  2. Review and run the service convergence commands printed by setup-oci.' \
    '  3. wefty node doctor'
}

install_macos() {
	((EUID != 0)) || die 64 "refusing to run the macOS/Homebrew path as root; rerun without sudo"
	local lima_version macos_version vz_available
	local simulated=0
	((dry_run)) && [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]] && simulated=1
	if ((simulated == 0)) && ! command -v brew >/dev/null 2>&1; then
		((dry_run)) && [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]] || die 64 "Homebrew is required; install it separately, then rerun this script"
	fi
  macos_version="${WEFTY_OCI_INSTALL_TEST_DISTRO_VERSION:-$(sw_vers -productVersion)}"
  version_at_least "$macos_version" 13.5.0 || die 64 "macOS $macos_version does not provide the supported Lima vz baseline (13.5 or newer)"
  vz_available="${WEFTY_OCI_INSTALL_TEST_VZ:-$(sysctl -n kern.hv_support 2>/dev/null || printf '0')}"
  [[ $vz_available == 1 || $vz_available == yes ]] || die 64 "Apple virtualization support is unavailable; Lima vz cannot run"
	if ((simulated)); then lima_version="${WEFTY_OCI_INSTALL_TEST_LIMA_VERSION:-}"
	else lima_version="$(probe_version LIMA_VERSION "$(command -v limactl 2>/dev/null || true)" 's/.*[^0-9]([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"; fi
  require_minimum Lima "$lima_version" "$LIMA_MINIMUM"
  if [[ -z $lima_version ]]; then
    if ((dry_run)); then printf 'DRY-RUN: brew install lima (Homebrew-managed paths and ownership)\n'; lima_version="$LIMA_TESTED"
    else
      HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_INSTALL_UPGRADE=1 brew install lima
      lima_version="$(probe_version LIMA_VERSION "$(command -v limactl 2>/dev/null || true)" 's/.*[^0-9]([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
      require_minimum Lima "$lima_version" "$LIMA_MINIMUM"
      [[ -n $lima_version ]] || die 70 "Lima installation completed but limactl is unavailable"
    fi
  else printf 'Lima %s already satisfies the supported minimum.\n' "$lima_version"; fi
  [[ $lima_version == "$LIMA_TESTED" ]] || warn "Lima $lima_version is supported but outside the tested version $LIMA_TESTED"
  print_next_commands 'wefty node setup-oci'
}

read_linux_release() {
  local key="$1" os_key variable="WEFTY_OCI_INSTALL_TEST_${1}"
  if [[ -n ${!variable+x} ]]; then printf '%s\n' "${!variable}"; return; fi
  [[ -r /etc/os-release ]] || die 64 "/etc/os-release is required on Linux"
  case "$key" in DISTRO_ID) os_key=ID ;; DISTRO_VERSION) os_key=VERSION_ID ;; *) die 64 "unknown os-release field: $key" ;; esac
  local release_key release_value
  while IFS='=' read -r release_key release_value; do
    if [[ $release_key == "$os_key" ]]; then
      release_value="${release_value%\"}"; release_value="${release_value#\"}"
      release_value="${release_value%\'}"; release_value="${release_value#\'}"
      printf '%s\n' "$release_value"
      return
    fi
  done </etc/os-release
  die 64 "/etc/os-release omitted $os_key"
}

assert_supported_linux_release() {
  case "$1:$2" in ubuntu:24.04|ubuntu:26.04|debian:12|debian:13|fedora:43|fedora:44) ;; *) die 64 "unsupported Linux release: $1 $2; supported: Ubuntu 24.04/26.04, Debian 12/13, Fedora 43/44" ;; esac
}

resolve_root_tool() {
  local variable="$1" name="$2" path
  if ((dry_run)) && [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then printf -v "$variable" '/usr/bin/%s' "$name"; return; fi
  path="$(type -P -- "$name" || true)"
  [[ $path == /* && -x $path ]] || die 64 "$name is required before installing OCI prerequisites"
  [[ -O $path ]] || die 64 "refusing privileged execution of non-root-owned tool: $path"
  printf -v "$variable" '%s' "$path"
}

discover_packaged_unit() {
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT+x} ]]; then
    [[ $WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT != none ]] && printf '%s\n' "$WEFTY_OCI_INSTALL_TEST_PACKAGED_UNIT"
    return 0
  fi
  local path
  declare -A seen=()
  local -a paths=(/etc/systemd/system /etc/systemd/system.control /run/systemd/system /run/systemd/transient /run/systemd/generator.early /etc/systemd/system.attached /run/systemd/generator /usr/local/lib/systemd/system /usr/lib/systemd/system /lib/systemd/system /run/systemd/generator.late)
  if [[ -n ${SYSTEMD_ANALYZE_BIN:-} && -x ${SYSTEMD_ANALYZE_BIN:-} ]]; then
    while IFS= read -r path; do [[ $path == /* ]] && paths+=("$path"); done < <("$SYSTEMD_ANALYZE_BIN" unit-paths 2>/dev/null || true)
  fi
  for path in "${paths[@]}"; do
    [[ -n ${seen[$path]:-} ]] && continue
    seen[$path]=1
    [[ -e $path/containerd.service ]] && { printf '%s\n' "$path/containerd.service"; return; }
  done
}

plan_linux_install() {
  local arch="$1" base archive runc_base
  base="https://github.com/containerd/containerd/releases/download/v${CONTAINERD_TESTED}"
  archive="containerd-${CONTAINERD_TESTED}-linux-${arch}.tar.gz"
  runc_base="https://github.com/opencontainers/runc/releases/download/v${RUNC_TESTED}"
  printf '%s\n' \
    "DRY-RUN: download $base/$archive and $base/$archive.sha256sum" \
    "DRY-RUN: verify the upstream checksum, stage the complete bundle, then atomically move it to /usr/local/lib/wefty/oci-runtime/containerd-${CONTAINERD_TESTED} (root:root directories 0755; binaries 0755)" \
    "DRY-RUN: publish root-owned symlinks /usr/local/bin/{containerd,containerd-shim-runc-v2,ctr} to that bundle" \
    "DRY-RUN: download $runc_base/runc.$arch and $runc_base/runc.sha256sum" \
    "DRY-RUN: verify the upstream checksum, atomically move runc to /usr/local/lib/wefty/oci-runtime/runc-${RUNC_TESTED}/runc (root:root 0755), and publish /usr/local/sbin/runc"
}

publish_link() {
  local source="$1" target="$2"
  if [[ -L $target && $($READLINK_BIN "$target") == "$source" ]]; then return; fi
  if [[ -e $target || -L $target ]]; then
    ((repair)) || die 65 "$target conflicts with the pinned install; inspect it, then rerun with --repair to replace only disclosed install paths"
    "$RM_BIN" -f -- "$target"
  fi
  "$LN_BIN" -s -- "$source" "$target"
  written_paths+=("$target")
}

preflight_link() {
	local source="$1" target="$2"
	if [[ -e $target || -L $target ]]; then
		[[ -L $target && $($READLINK_BIN "$target") == "$source" ]] || ((repair)) || die 65 "$target conflicts with the pinned install; inspect it, then rerun with --repair to replace only disclosed install paths"
	fi
}

install_containerd_bundle() {
  local arch="$1" parent final download stage archive checksum base binary
  parent="/usr/local/lib/wefty/oci-runtime"
  final="$parent/containerd-${CONTAINERD_TESTED}"
  if [[ -x $final/bin/containerd && -x $final/bin/containerd-shim-runc-v2 && -x $final/bin/ctr && -f $final/.wefty-managed ]] &&
    [[ $(<"$final/.wefty-managed") == "containerd=$CONTAINERD_TESTED" ]] &&
    [[ $(probe_version CONTAINERD_VERSION "$final/bin/containerd" '') == "$CONTAINERD_TESTED" ]]; then
    CONTAINERD_PATH="$final/bin/containerd"
  else
    if [[ -e $final ]]; then ((repair)) || die 65 "$final is incomplete or version-mismatched; rerun with --repair to replace this disclosed bundle"; "$RM_BIN" -rf -- "$final"; fi
    "$MKDIR_BIN" -p -- "$parent"
    download="$($MKTEMP_BIN -d "${TMPDIR:-/tmp}/wefty-oci-download.XXXXXX")"
    stage="$($MKTEMP_BIN -d "$parent/.containerd-${CONTAINERD_TESTED}.XXXXXX")"
    temporary_paths+=("$download" "$stage")
    archive="containerd-${CONTAINERD_TESTED}-linux-${arch}.tar.gz"
    checksum="$archive.sha256sum"
    base="https://github.com/containerd/containerd/releases/download/v${CONTAINERD_TESTED}"
    "$CURL_BIN" --fail --location --proto '=https' --tlsv1.2 --output "$download/$archive" "$base/$archive"
    "$CURL_BIN" --fail --location --proto '=https' --tlsv1.2 --output "$download/$checksum" "$base/$checksum"
    (cd "$download" && "$SHA256SUM_BIN" --check "$checksum")
    "$TAR_BIN" --extract --gzip --file "$download/$archive" --directory "$download"
    "$MKDIR_BIN" -p -- "$stage/bin"
    for binary in containerd containerd-shim-runc-v2 ctr; do
      [[ -x $download/bin/$binary ]] || die 70 "upstream containerd bundle omitted $binary"
      "$INSTALL_BIN" --owner=root --group=root --mode=0755 "$download/bin/$binary" "$stage/bin/$binary"
    done
    printf 'containerd=%s\n' "$CONTAINERD_TESTED" >"$stage/.wefty-managed"
    "$CHMOD_BIN" 0644 "$stage/.wefty-managed"
    "$CHOWN_BIN" root:root "$stage/.wefty-managed"
    "$MV_BIN" -- "$stage" "$final"
    written_paths+=("$final")
    CONTAINERD_PATH="$final/bin/containerd"
  fi
  "$MKDIR_BIN" -p -- /usr/local/bin
  for binary in containerd containerd-shim-runc-v2 ctr; do publish_link "$final/bin/$binary" "/usr/local/bin/$binary"; done
}

install_runc_binary() {
  local arch="$1" parent final download stage base binary
  parent="/usr/local/lib/wefty/oci-runtime"
  final="$parent/runc-${RUNC_TESTED}"
  if [[ -x $final/runc && -f $final/.wefty-managed && $(<"$final/.wefty-managed") == "runc=$RUNC_TESTED" && $(probe_version RUNC_VERSION "$final/runc" '') == "$RUNC_TESTED" ]]; then RUNC_PATH="$final/runc"
  else
    if [[ -e $final ]]; then ((repair)) || die 65 "$final is incomplete or version-mismatched; rerun with --repair to replace this disclosed bundle"; "$RM_BIN" -rf -- "$final"; fi
    "$MKDIR_BIN" -p -- "$parent"
    download="$($MKTEMP_BIN -d "${TMPDIR:-/tmp}/wefty-runc-download.XXXXXX")"
    stage="$($MKTEMP_BIN -d "$parent/.runc-${RUNC_TESTED}.XXXXXX")"
    temporary_paths+=("$download" "$stage")
    binary="runc.$arch"
    base="https://github.com/opencontainers/runc/releases/download/v${RUNC_TESTED}"
    "$CURL_BIN" --fail --location --proto '=https' --tlsv1.2 --output "$download/$binary" "$base/$binary"
    "$CURL_BIN" --fail --location --proto '=https' --tlsv1.2 --output "$download/runc.sha256sum" "$base/runc.sha256sum"
    (cd "$download" && "$GREP_BIN" "  $binary\$" runc.sha256sum | "$SHA256SUM_BIN" --check)
    "$INSTALL_BIN" --directory --owner=root --group=root --mode=0755 "$stage"
    "$INSTALL_BIN" --owner=root --group=root --mode=0755 "$download/$binary" "$stage/runc"
    printf 'runc=%s\n' "$RUNC_TESTED" >"$stage/.wefty-managed"
    "$CHMOD_BIN" 0644 "$stage/.wefty-managed"
    "$CHOWN_BIN" root:root "$stage/.wefty-managed"
    "$MV_BIN" -- "$stage" "$final"
    written_paths+=("$final")
    RUNC_PATH="$final/runc"
  fi
  "$MKDIR_BIN" -p -- /usr/local/sbin
  publish_link "$final/runc" /usr/local/sbin/runc
}

install_containerd_unit_if_absent() {
  local existing unit=/etc/systemd/system/containerd.service temporary
  existing="$(discover_packaged_unit)"
  if [[ -n $existing ]]; then printf 'Preserving packaged containerd unit: %s\n' "$existing"; return; fi
  if ((dry_run)); then
    printf '%s\n' "DRY-RUN: write $unit (root:root 0644)" "DRY-RUN: unit ExecStart=$CONTAINERD_PATH; Type=notify; Restart=always; the unit remains disabled and stopped"
    return
  fi
  "$INSTALL_BIN" --directory --owner=root --group=root --mode=0755 /etc/systemd/system
  temporary="$($MKTEMP_BIN /etc/systemd/system/.containerd.service.XXXXXX)"
  temporary_paths+=("$temporary")
  {
    printf '%s\n' '[Unit]' 'Description=containerd container runtime' 'Documentation=https://containerd.io' 'After=network.target local-fs.target' '' '[Service]' 'Type=notify'
    printf 'ExecStart=%s\n' "$CONTAINERD_PATH"
    printf '%s\n' 'Restart=always' 'RestartSec=5s' 'Delegate=yes' 'KillMode=process' 'OOMScoreAdjust=-999' 'LimitNOFILE=1048576' 'LimitNPROC=infinity' 'LimitCORE=infinity' 'TasksMax=infinity' '' '[Install]' 'WantedBy=multi-user.target'
  } >"$temporary"
  "$CHMOD_BIN" 0644 "$temporary"
  "$CHOWN_BIN" root:root "$temporary"
  "$MV_BIN" -- "$temporary" "$unit"
  written_paths+=("$unit")
}

install_linux() {
  local id version arch overlayfs containerd_version runc_version packaged containerd_dir containerd_marker runc_marker
  id="$(read_linux_release DISTRO_ID)"; version="$(read_linux_release DISTRO_VERSION)"; assert_supported_linux_release "$id" "$version"; arch="$(arch_value)"
  if ! ((dry_run)) || [[ -z ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then ((EUID == 0)) || die 64 "Linux installation must run as root; rerun with sudo"; fi
  resolve_root_tool CURL_BIN curl; resolve_root_tool TAR_BIN tar; resolve_root_tool SHA256SUM_BIN sha256sum; resolve_root_tool INSTALL_BIN install
  resolve_root_tool GREP_BIN grep; resolve_root_tool MKTEMP_BIN mktemp; resolve_root_tool RM_BIN rm; resolve_root_tool MV_BIN mv
  resolve_root_tool MKDIR_BIN mkdir; resolve_root_tool LN_BIN ln; resolve_root_tool READLINK_BIN readlink; resolve_root_tool CHMOD_BIN chmod; resolve_root_tool CHOWN_BIN chown
  trap cleanup EXIT
	if ((dry_run)) && [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then SYSTEMD_ANALYZE_BIN=/usr/bin/systemd-analyze
	else SYSTEMD_ANALYZE_BIN="$(type -P -- systemd-analyze || true)"; fi
	if ! ((dry_run)) || [[ -z ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then
		[[ -z $SYSTEMD_ANALYZE_BIN || -O $SYSTEMD_ANALYZE_BIN ]] || die 64 "refusing privileged execution of non-root-owned tool: $SYSTEMD_ANALYZE_BIN"
	fi
  if [[ -n ${WEFTY_OCI_INSTALL_TEST_OVERLAYFS+x} ]]; then overlayfs="$WEFTY_OCI_INSTALL_TEST_OVERLAYFS"; elif "$GREP_BIN" -qw overlay /proc/filesystems; then overlayfs=yes; else overlayfs=no; fi
  [[ $overlayfs == yes || $overlayfs == 1 ]] || die 64 "overlayfs is unavailable; load or enable it before rerunning"

  if ((dry_run)) && [[ -n ${WEFTY_OCI_INSTALL_TEST_PLATFORM+x} ]]; then
    containerd_version="${WEFTY_OCI_INSTALL_TEST_CONTAINERD_VERSION:-}"; runc_version="${WEFTY_OCI_INSTALL_TEST_RUNC_VERSION:-}"
  else
    CONTAINERD_PATH="$(type -P -- containerd || true)"; RUNC_PATH="$(type -P -- runc || true)"
    containerd_version="$(probe_version CONTAINERD_VERSION "$CONTAINERD_PATH" '')"
    runc_version="$(probe_version RUNC_VERSION "$RUNC_PATH" '')"
  fi
  require_minimum containerd "$containerd_version" "$CONTAINERD_MINIMUM"; require_minimum runc "$runc_version" "$RUNC_MINIMUM"
  [[ -z $runc_version || $(version_major "$runc_version") == 1 ]] || die 64 "runc $runc_version is outside the supported 1.x major; no changes made"
  packaged="$(discover_packaged_unit)"
  if ((dry_run)); then
    if [[ -z $containerd_version || -z $runc_version ]]; then plan_linux_install "$arch"; CONTAINERD_PATH="/usr/local/lib/wefty/oci-runtime/containerd-${CONTAINERD_TESTED}/bin/containerd"; fi
    [[ -n $containerd_version ]] && printf 'containerd %s already satisfies the supported minimum (dry-run fixture).\n' "$containerd_version"
    [[ -n $runc_version ]] && printf 'runc %s already satisfies the supported major (dry-run fixture).\n' "$runc_version"
    if [[ -n $packaged ]]; then printf 'DRY-RUN: preserve packaged containerd unit at %s\n' "$packaged"; else install_containerd_unit_if_absent; fi
	else
		containerd_dir="${CONTAINERD_PATH%/*}"
		containerd_marker="/usr/local/lib/wefty/oci-runtime/containerd-${CONTAINERD_TESTED}/.wefty-managed"
		runc_marker="/usr/local/lib/wefty/oci-runtime/runc-${RUNC_TESTED}/.wefty-managed"
		if [[ $CONTAINERD_PATH == /usr/local/bin/containerd && -L $CONTAINERD_PATH ]] &&
		  [[ $($READLINK_BIN "$CONTAINERD_PATH") == "/usr/local/lib/wefty/oci-runtime/containerd-${CONTAINERD_TESTED}/bin/containerd" ]] &&
		  [[ ! -f $containerd_marker || $(<"$containerd_marker") != "containerd=$CONTAINERD_TESTED" ]]; then
			containerd_version=""
		fi
		if [[ $RUNC_PATH == /usr/local/sbin/runc && -L $RUNC_PATH ]] &&
		  [[ $($READLINK_BIN "$RUNC_PATH") == "/usr/local/lib/wefty/oci-runtime/runc-${RUNC_TESTED}/runc" ]] &&
		  [[ ! -f $runc_marker || $(<"$runc_marker") != "runc=$RUNC_TESTED" ]]; then
			runc_version=""
		fi
		if [[ -z $containerd_version || ! -x $containerd_dir/containerd-shim-runc-v2 || ! -x $containerd_dir/ctr ]]; then
			for binary in containerd containerd-shim-runc-v2 ctr; do preflight_link "/usr/local/lib/wefty/oci-runtime/containerd-${CONTAINERD_TESTED}/bin/$binary" "/usr/local/bin/$binary"; done
			install_containerd_bundle "$arch"; containerd_version="$CONTAINERD_TESTED"
		fi
		if [[ -z $runc_version ]]; then preflight_link "/usr/local/lib/wefty/oci-runtime/runc-${RUNC_TESTED}/runc" /usr/local/sbin/runc; install_runc_binary "$arch"; runc_version="$RUNC_TESTED"; fi
    install_containerd_unit_if_absent
  fi
  [[ $containerd_version == "$CONTAINERD_TESTED" ]] || warn "containerd $containerd_version is supported but outside the tested version $CONTAINERD_TESTED"
  [[ $runc_version == "$RUNC_TESTED" ]] || warn "runc $runc_version is supported but outside the tested version $RUNC_TESTED"
  if ((${#written_paths[@]})); then printf 'Written paths (root:root; remove only after stopping dependents):\n'; printf '  %s\n' "${written_paths[@]}"; fi
  printf '%s\n' 'Uninstall a wefty-managed runtime by stopping dependents, removing the disclosed symlinks/unit, then removing /usr/local/lib/wefty/oci-runtime. A packaged containerd unit is never removed or shadowed.'
  print_next_commands 'sudo wefty node setup-oci'
}

case "$(platform_value)" in darwin) install_macos ;; linux) install_linux ;; *) die 64 "unsupported platform; only macOS and named Ubuntu, Debian, or Fedora releases are supported" ;; esac
