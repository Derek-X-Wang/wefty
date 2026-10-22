# syntax=docker/dockerfile:1.18.0@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

# The single-seat target is an optional reference desktop. The final seats
# target is the default Computer image; neither is a required tenant base.
ARG DEBIAN_IMAGE=docker.io/library/debian@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
ARG SOURCE_DATE_EPOCH=0

FROM ${DEBIAN_IMAGE} AS desktop

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

FROM desktop AS single-seat

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

# The default image has no Seat until the helper adds one to seats.json.
# single-seat above retains the M3.5 reference acceptance image independently.
FROM desktop AS seats
USER root
# Playwright 1.56.1: Debian 12 tools/chromium/firefox/webkit nativeDeps.js union.
RUN apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install --yes --no-install-recommends \
        git curl ca-certificates nodejs npm xauth util-linux procps \
        fonts-freefont-ttf fonts-ipafont-gothic fonts-liberation fonts-noto-color-emoji fonts-tlwg-loma-otf fonts-unifont \
        fonts-wqy-zenhei gstreamer1.0-libav gstreamer1.0-plugins-bad gstreamer1.0-plugins-base gstreamer1.0-plugins-good libasound2 \
        libatk-bridge2.0-0 libatk1.0-0 libatomic1 libatspi2.0-0 libavif15 libcairo-gobject2 \
        libcairo2 libcups2 libdbus-1-3 libdbus-glib-1-2 libdrm2 libegl1 \
        libenchant-2-2 libepoxy0 libevdev2 libevent-2.1-7 libfontconfig1 libfreetype6 \
        libgbm1 libgdk-pixbuf-2.0-0 libgles2 libglib2.0-0 libglx0 libgstreamer-gl1.0-0 \
        libgstreamer-plugins-base1.0-0 libgstreamer1.0-0 libgtk-3-0 libgtk-4-1 libgudev-1.0-0 libharfbuzz-icu0 \
        libharfbuzz0b libhyphen0 libicu72 libjpeg62-turbo liblcms2-2 libmanette-0.2-0 \
        libnotify4 libnspr4 libnss3 libopengl0 libopenjp2-7 libopus0 \
        libpango-1.0-0 libpangocairo-1.0-0 libpng16-16 libproxy1v5 libsecret-1-0 libsoup-3.0-0 \
        libwayland-client0 libwayland-egl1 libwayland-server0 libwebp7 libwebpdemux2 libwoff1 \
        libx11-6 libx11-xcb1 libxcb-shm0 libxcb1 libxcomposite1 libxcursor1 \
        libxdamage1 libxext6 libxfixes3 libxi6 libxkbcommon0 libxml2 \
        libxrandr2 libxrender1 libxslt1.1 libxtst6 xfonts-scalable xvfb \
    && rm -f /var/log/dpkg.log /var/log/alternatives.log /var/log/apt/* \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/* /var/cache/fontconfig/* /var/cache/ldconfig/aux-cache \
    && for n in 1 2 3 4; do \
         groupadd --gid $((2000+n)) seat$n; \
         useradd --no-log-init --uid $((2000+n)) --gid $((2000+n)) \
           --home-dir /wefty/service/seats/$n/home --shell /bin/sh --no-create-home seat$n; \
         chage --lastday 0 seat$n; \
       done \
    && rm -f /etc/passwd- /etc/group- /etc/shadow- /etc/gshadow- \
    && mkdir -p /opt/wefty-computer /wefty/seat-status
COPY --chmod=0755 examples/computer/seat-supervisor.py /usr/local/bin/wefty-seat-supervisor
COPY --chmod=0755 examples/computer/seat-run.sh /usr/local/libexec/wefty-seat-run
COPY --chmod=0755 examples/computer/seat-endpoints.sh /usr/local/libexec/wefty-seat-endpoints
COPY --chmod=0755 examples/computer/rfb-backend.py /usr/local/libexec/wefty-rfb-backend
COPY --chmod=0755 examples/computer/rfb-websocket.py /usr/local/libexec/wefty-rfb-websocket
COPY --chmod=0755 examples/computer/watch-driver.py /usr/local/libexec/wefty-watch-driver
COPY --chmod=0644 examples/computer/seat.html /opt/wefty-computer/seat.html
LABEL org.opencontainers.image.source="https://github.com/Derek-X-Wang/wefty" \
      org.opencontainers.image.description="Default Wefty Computer desktop with four Seat users"
WORKDIR /
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/wefty-seat-supervisor"]
