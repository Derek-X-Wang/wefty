# syntax=docker/dockerfile:1.18.0@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

# This is an optional acceptance/example desktop, not a required base image,
# runtime layer, compatibility target, or security profile for tenant images.
ARG DEBIAN_IMAGE=docker.io/library/debian@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

FROM ${DEBIAN_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        chromium \
        dbus-x11 \
        fonts-dejavu-core \
        python3 \
        python3-websockify \
        tini \
        x11-utils \
        x11vnc \
        xfce4-panel \
        xfce4-session \
        xfce4-settings \
        xfdesktop4 \
        xfwm4 \
        xvfb \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 12000 wefty \
    && useradd --uid 12000 --gid 12000 --home-dir /home/wefty --shell /bin/sh --no-create-home wefty \
    && mkdir -p /opt/wefty-computer \
    && ln -s /wefty/service/home/wefty /home/wefty

COPY --chmod=0755 examples/computer/entrypoint.sh /usr/local/bin/wefty-computer-entrypoint
COPY --chmod=0755 examples/computer/rfb-backend.py /usr/local/libexec/wefty-rfb-backend
COPY --chmod=0755 examples/computer/rfb-websocket.py /usr/local/libexec/wefty-rfb-websocket
COPY --chmod=0755 examples/computer/watch-driver.py /usr/local/libexec/wefty-watch-driver
COPY --chmod=0644 examples/computer/oracle.html /opt/wefty-computer/oracle.html

LABEL org.opencontainers.image.source="https://github.com/Derek-X-Wang/wefty" \
      org.opencontainers.image.description="Optional Wefty reference Computer desktop for acceptance and image-author examples"

ENV HOME=/home/wefty \
    DISPLAY=:99 \
    XDG_RUNTIME_DIR=/tmp/wefty-runtime
USER 12000:12000
WORKDIR /
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/wefty-computer-entrypoint"]
