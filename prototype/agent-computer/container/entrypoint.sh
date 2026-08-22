#!/bin/sh
set -eu

index=${COMPUTER_INDEX:-0}
display_num=$((10 + index))
control_port=$((5900 + index * 2))
view_port=$((5901 + index * 2))
control_web_port=$((6080 + index * 2))
view_web_port=$((6081 + index * 2))
display=":${display_num}"

install -d -o computer -g computer -m 0700 /profile
install -d -o computer -g computer -m 0700 /run/wefty-computer
rm -f "/tmp/.X${display_num}-lock" "/tmp/.X11-unix/X${display_num}"

Xvfb "$display" -screen 0 1280x720x24 -nolisten tcp > /run/wefty-computer/xvfb.log 2>&1 &
xvfb_pid=$!

cleanup() {
  kill "$xvfb_pid" 2>/dev/null || true
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT INT TERM

tries=0
while ! DISPLAY="$display" xdpyinfo >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -gt 100 ]; then
    echo "Xvfb did not become ready" >&2
    exit 1
  fi
  sleep 0.1
done

su -s /bin/sh computer -c "DISPLAY=$display dbus-launch --exit-with-session startxfce4" \
  > /run/wefty-computer/xfce.log 2>&1 &
sleep 2

su -s /bin/sh computer -c "DISPLAY=$display chromium --no-sandbox --no-first-run --disable-dev-shm-usage --disable-gpu --user-data-dir=/profile --app=file:///opt/wefty/home.html" \
  > /run/wefty-computer/chromium.log 2>&1 &

DISPLAY="$display" xev -root -event keyboard > /run/wefty-computer/input-events.log 2>&1 &

x11vnc -display "$display" -forever -shared -nopw -no6 -rfbport "$control_port" \
  -noxdamage -o /run/wefty-computer/x11vnc-control.log &
x11vnc -display "$display" -forever -shared -nopw -viewonly -no6 -rfbport "$view_port" \
  -noxdamage -o /run/wefty-computer/x11vnc-view.log &

websockify --web /usr/share/novnc "$control_web_port" "127.0.0.1:${control_port}" \
  > /run/wefty-computer/web-control.log 2>&1 &
websockify --web /usr/share/novnc "$view_web_port" "127.0.0.1:${view_port}" \
  > /run/wefty-computer/web-view.log 2>&1 &

wait
