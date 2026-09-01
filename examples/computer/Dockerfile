# syntax=docker/dockerfile:1.18.0@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

# This is an optional acceptance/example desktop, not a required base image,
# runtime layer, compatibility target, or security profile for tenant images.
ARG DEBIAN_IMAGE=docker.io/library/debian@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
ARG SOURCE_DATE_EPOCH=0

FROM ${DEBIAN_IMAGE}

ARG SOURCE_DATE_EPOCH
ENV DEBIAN_FRONTEND=noninteractive
RUN printf '%s\n' \
      'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260827T000000Z bookworm main' \
      > /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/* \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install --yes --no-install-recommends \
        chromium=150.0.7871.100-1~deb12u1 \
        dbus-x11=1.14.10-1~deb12u1 \
        fonts-dejavu-core=2.37-6 \
        python3=3.11.2-1+b1 \
        python3-websockify=0.10.0+dfsg1-4+b1 \
        tini=0.19.0-1+b3 \
        x11-utils=7.7+5 \
        xinput=1.6.3-1 \
        x11vnc=0.9.16-9 \
        xfce4-panel=4.18.2-1 \
        xfce4-session=4.18.1-1 \
        xfce4-settings=4.18.2-1 \
        xfdesktop4=4.18.1-1 \
        xfwm4=4.18.0-1 \
        xvfb=2:21.1.7-3+deb12u12 \
    && rm -f /var/log/dpkg.log /var/log/alternatives.log /var/log/apt/* \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/* /var/cache/fontconfig/* /var/cache/ldconfig/aux-cache \
    && printf '%032d\n' 0 > /etc/machine-id \
    && ln -sf /etc/machine-id /var/lib/dbus/machine-id

RUN groupadd --gid 12000 wefty \
    && useradd --uid 12000 --gid 12000 --home-dir /home/wefty --shell /bin/sh --no-create-home wefty \
    && mkdir -p /opt/wefty-computer \
    && ln -s /wefty/service/home/wefty /home/wefty

COPY --chmod=0755 examples/computer/entrypoint.sh /usr/local/bin/wefty-computer-entrypoint
COPY --chmod=0755 examples/computer/rfb-backend.py /usr/local/libexec/wefty-rfb-backend
COPY --chmod=0755 examples/computer/rfb-websocket.py /usr/local/libexec/wefty-rfb-websocket
COPY --chmod=0755 examples/computer/watch-driver.py /usr/local/libexec/wefty-watch-driver
COPY --chmod=0755 examples/computer/pointer-oracle.py /usr/local/libexec/wefty-pointer-oracle
COPY --chmod=0644 examples/computer/oracle.html /opt/wefty-computer/oracle.html

LABEL org.opencontainers.image.source="https://github.com/Derek-X-Wang/wefty" \
      org.opencontainers.image.description="Optional Wefty reference Computer desktop for acceptance and image-author examples"

ENV HOME=/home/wefty \
    XDG_RUNTIME_DIR=/tmp/wefty-runtime
USER 12000:12000
WORKDIR /
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/wefty-computer-entrypoint"]
