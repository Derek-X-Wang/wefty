#!/bin/sh
set -eu

usage() {
  echo 'usage: wefty-verify-licenses --generate DEBIAN_TSV COMPONENT_TSV | --check DEBIAN_TSV COMPONENT_TSV' >&2
  exit 64
}

test "$#" -eq 3 || usage
mode=$1
debian_inventory=$2
component_inventory=$3

check_inventories() {
  test -s "$debian_inventory" || { echo 'installed Debian package license inventory is missing or empty' >&2; exit 1; }
  test -s "$component_inventory" || { echo 'non-dpkg component license inventory is missing or empty' >&2; exit 1; }
  while IFS="	" read -r package section copyright; do
    test -n "$package" && test -n "$section" && test -s "$copyright" || {
      echo "invalid Debian package license row: $package" >&2
      exit 1
    }
  done < "$debian_inventory"
  expected_components='neatvnc-patched
mise
wefty-image-files'
  observed_components=
  while IFS="	" read -r component license notice; do
    test -n "$component" && test -n "$license" && test -s "$notice" || {
      echo "invalid non-dpkg component license row: $component" >&2
      exit 1
    }
    observed_components="${observed_components}${observed_components:+
}${component}"
  done < "$component_inventory"
  test "$observed_components" = "$expected_components" || {
    echo 'non-dpkg component inventory is not the closed expected set' >&2
    exit 1
  }
}

case "$mode" in
  --generate)
    debian_temporary=$debian_inventory.new
    component_temporary=$component_inventory.new
    dpkg-query -W -f='${binary:Package}\t${Section}\n' | LC_ALL=C sort | while IFS="	" read -r package section; do
      base=${package%%:*}
      copyright="/usr/share/doc/$base/copyright"
      test -s "$copyright" || { echo "missing package copyright: $package" >&2; exit 1; }
      printf '%s\t%s\t%s\n' "$package" "$section" "$copyright"
    done > "$debian_temporary"
    printf '%s\t%s\t%s\n' \
      neatvnc-patched ISC /usr/share/wefty/licenses/neatvnc-ISC.txt \
      mise MIT /usr/share/wefty/licenses/mise-MIT.txt \
      wefty-image-files Apache-2.0 /usr/share/wefty/licenses/wefty-Apache-2.0.txt \
      > "$component_temporary"
    chmod 0644 "$debian_temporary" "$component_temporary"
    mv "$debian_temporary" "$debian_inventory"
    mv "$component_temporary" "$component_inventory"
    check_inventories
    ;;
  --check)
    check_inventories
    ;;
  *) usage ;;
esac
