FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates chromium dbus-x11 mesa-utils novnc procps sqlite3 \
        tigervnc-tools vainfo websockify x11-utils x11-xserver-utils \
        x11vnc xdotool xfce4 xfce4-terminal xvfb \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 --shell /bin/bash computer

COPY entrypoint.sh home.html /opt/wefty/
RUN chmod 0755 /opt/wefty/entrypoint.sh

ENTRYPOINT ["/opt/wefty/entrypoint.sh"]
