#!/bin/sh
set -eu
output=${1:?output path required}
temporary=$output.new
: > "$temporary"
dpkg-query -W -f='${binary:Package}\t${Section}\n' | LC_ALL=C sort | while IFS="	" read -r package section; do
  case "$section" in non-free*|contrib*) echo "forbidden package section: $package $section" >&2; exit 1 ;; esac
  base=${package%%:*}
  test -e "/usr/share/doc/$base/copyright" || { echo "missing package copyright: $package" >&2; exit 1; }
  printf '%s\t%s\t%s\n' "$package" "$section" "/usr/share/doc/$base/copyright"
done > "$temporary"
if [ ! -s "$temporary" ]; then
  echo 'installed Debian package license inventory is empty' >&2
  exit 1
fi
chmod 0644 "$temporary"
mv "$temporary" "$output"
