#!/bin/sh
set -eu

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates chromium dbus-x11 mesa-utils novnc procps sqlite3 \
  tigervnc-tools vainfo websockify x11-utils x11-xserver-utils \
  x11vnc xdotool xfce4 xfce4-terminal xvfb
rm -rf /var/lib/apt/lists/*
id computer >/dev/null 2>&1 || useradd --create-home --uid 1000 --shell /bin/bash computer
install -d -m 0755 /opt/wefty
install -m 0755 /prototype/entrypoint.sh /opt/wefty/entrypoint.sh
install -m 0644 /prototype/home.html /opt/wefty/home.html
