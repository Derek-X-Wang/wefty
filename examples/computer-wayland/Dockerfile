# syntax=docker/dockerfile:1.18.0@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

# This is a second optional acceptance/example desktop. It is not a required
# base image, runtime layer, compatibility target, or security profile.
ARG DEBIAN_IMAGE=docker.io/library/debian@sha256:f324c7ff54321e8d9c588493a20244965938ce0aa50bbd1022d38010e9ffc4b1
ARG SOURCE_DATE_EPOCH=0

FROM ${DEBIAN_IMAGE} AS neatvnc-build
ARG SOURCE_DATE_EPOCH
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive
RUN case "$TARGETARCH" in amd64) ninja_version=1.12.1-1 ;; arm64) ninja_version=1.12.1-1+b1 ;; *) exit 64 ;; esac \
    && printf '%s\n' \
      'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260827T000000Z trixie main' \
      > /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/* \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install --yes --no-install-recommends \
        build-essential=12.12 \
        ca-certificates=20250419 \
        curl=8.14.1-2+deb13u4 \
        libaml-dev=0.3.0-3 \
        libdrm-dev=2.4.124-2 \
        libgnutls28-dev=3.8.9-3+deb13u4 \
        libpixman-1-dev=0.44.0-3 \
        libxxhash-dev=0.8.3-2 \
        meson=1.7.0-1 \
        nettle-dev=3.10.1-1 \
        ninja-build="$ninja_version" \
        patch=2.8-2 \
        pkgconf=1.8.1-4 \
        xz-utils=5.8.1-1+deb13u1 \
        zlib1g-dev=1:1.3.dfsg+really1.3.1-1+b1 \
    && curl --fail --location --retry 5 \
      --output /tmp/neatvnc.tar.xz \
      https://snapshot.debian.org/archive/debian/20260827T000000Z/pool/main/n/neatvnc/neatvnc_0.9.1%2Bdfsg.orig.tar.xz \
    && printf '%s  %s\n' \
      '46d689818d88e0d65387bf9182f1bd1015c4b28e0347696f0bec7c7d9c829194' \
      /tmp/neatvnc.tar.xz | sha256sum --check -
RUN case "$TARGETARCH" in \
      amd64) mise_arch=x64; mise_sha=7cd12d6002d5b3c83a89cad79023712faf2a36f9e8b2ee2061dac5135b3de0ed ;; \
      arm64) mise_arch=arm64; mise_sha=bc2c447a7e498b0bed0a421cc2101b407fef09a3195670d35a4aa3f43cd868a1 ;; \
      *) exit 64 ;; \
    esac \
    && curl --fail --location --retry 5 --output /tmp/mise \
      "https://github.com/jdx/mise/releases/download/v2026.8.14/mise-v2026.8.14-linux-${mise_arch}" \
    && printf '%s  %s\n' "$mise_sha" /tmp/mise | sha256sum --check - \
    && install --mode=0755 /tmp/mise /opt/mise
COPY examples/computer-wayland/patches/neatvnc-rfb-websocket-v1.patch /tmp/neatvnc.patch
RUN mkdir /tmp/neatvnc \
    && tar --extract --xz --file /tmp/neatvnc.tar.xz --directory /tmp/neatvnc --strip-components=1 \
    && patch --directory /tmp/neatvnc --strip=1 < /tmp/neatvnc.patch \
    && meson setup /tmp/neatvnc/build /tmp/neatvnc \
      --buildtype=release --prefix=/opt/neatvnc --libdir=lib \
      -Dtests=false -Dexamples=false -Dbenchmarks=false \
      -Djpeg=disabled -Dtls=enabled -Dnettle=enabled \
      -Dgbm=disabled -Dh264=disabled -Dwefty_mutation_hooks=false \
    && meson compile -C /tmp/neatvnc/build \
    && meson install -C /tmp/neatvnc/build

FROM ${DEBIAN_IMAGE}
ARG SOURCE_DATE_EPOCH
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive
RUN case "$TARGETARCH" in amd64) swaybg_version=1.2.1-1 ;; arm64) swaybg_version=1.2.1-1+b1 ;; *) exit 64 ;; esac \
    && printf '%s\n' \
      'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/20260827T000000Z trixie main' \
      > /etc/apt/sources.list \
    && rm -f /etc/apt/sources.list.d/* \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install --yes --no-install-recommends \
        chromium=150.0.7871.100-1~deb13u1 \
        dbus-user-session=1.16.2-2 \
        fonts-dejavu-core=2.37-8 \
        foot=1.21.0-2 \
        fuzzel=1.12.0+ds-1 \
        git=1:2.47.3-0+deb13u1 \
        jq=1.7.1-6+deb13u2 \
        lazygit=0.50.0+ds1-1+b2 \
        mako-notifier=1.10.0-1 \
        neovim=0.10.4-8 \
        python3=3.13.5-1 \
        sway=1.10.1-2 \
        swaybg="$swaybg_version" \
        tini=0.19.0-3+b7 \
        wayvnc=0.9.1-1 \
    && rm -f /var/log/dpkg.log /var/log/alternatives.log /var/log/apt/* \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/* /var/cache/fontconfig/* /var/cache/ldconfig/aux-cache \
    && printf '%032d\n' 0 > /etc/machine-id \
    && ln -sf /etc/machine-id /var/lib/dbus/machine-id

COPY --from=neatvnc-build /opt/neatvnc/lib/libneatvnc.so.0.0.0 /usr/local/lib/libneatvnc.so.0.0.0
RUN ln -s libneatvnc.so.0.0.0 /usr/local/lib/libneatvnc.so.0 \
    && case "$TARGETARCH" in amd64) multiarch=x86_64-linux-gnu ;; arm64) multiarch=aarch64-linux-gnu ;; *) exit 64 ;; esac \
    && rm -f "/usr/lib/$multiarch/libneatvnc.so"* \
    && printf '%s\n' /usr/local/lib > /etc/ld.so.conf.d/wefty-neatvnc.conf \
    && ldconfig \
    && ldconfig -p | grep -F 'libneatvnc.so.0' | grep -F '/usr/local/lib/libneatvnc.so.0' \
    && ldd /usr/bin/wayvnc | grep -F 'libneatvnc.so.0 => /usr/local/lib/libneatvnc.so.0' \
    && ! find "/usr/lib/$multiarch" -maxdepth 1 -name 'libneatvnc.so*' -print -quit | grep .
COPY --from=neatvnc-build --chmod=0755 /opt/mise /usr/local/bin/mise

RUN groupadd --gid 12000 wefty \
    && useradd --uid 12000 --gid 12000 --home-dir /home/wefty --shell /bin/sh --no-create-home wefty \
    && mkdir -p /opt/wefty-computer-wayland /opt/wefty-computer-wayland/skills /usr/share/wefty/licenses \
    && ln -s /wefty/service/home/wefty /home/wefty

COPY --chmod=0755 examples/computer-wayland/entrypoint.sh /usr/local/bin/wefty-computer-wayland-entrypoint
COPY --chmod=0755 examples/computer-wayland/surface.py /usr/local/libexec/wefty-wayland-surface
COPY --chmod=0755 examples/computer-wayland/watch-driver.py /usr/local/libexec/wefty-watch-driver
COPY --chmod=0755 examples/computer-wayland/agent-state.sh /usr/local/bin/wefty-agent-state
COPY --chmod=0755 examples/computer-wayland/agent-session-demo.sh /usr/local/bin/wefty-agent-session-demo
COPY --chmod=0755 examples/computer-wayland/crash-briefing.sh /usr/local/bin/wefty-crash-briefing
COPY --chmod=0755 examples/computer-wayland/theme.sh /usr/local/bin/wefty-theme
COPY --chmod=0755 examples/computer-wayland/agent-stub.sh /usr/local/libexec/wefty-agent-stub
RUN ln -s /usr/local/libexec/wefty-agent-stub /usr/local/bin/claude \
    && ln -s /usr/local/libexec/wefty-agent-stub /usr/local/bin/codex
COPY --chmod=0755 examples/computer-wayland/verify-licenses.sh /usr/local/libexec/wefty-verify-licenses
COPY --chmod=0644 examples/computer-wayland/oracle.html /opt/wefty-computer-wayland/oracle.html
COPY --chmod=0644 examples/computer-wayland/sway-config /opt/wefty-computer-wayland/sway-config
COPY --chmod=0644 examples/computer-wayland/skills/self-reconfigure/SKILL.md /opt/wefty-computer-wayland/skills/self-reconfigure/SKILL.md
COPY --chmod=0644 examples/computer-wayland/LICENSES.md /usr/share/doc/wefty-computer-wayland/ATTRIBUTIONS.md
COPY --from=neatvnc-build --chmod=0644 /tmp/neatvnc/COPYING /usr/share/wefty/licenses/neatvnc-ISC.txt
COPY --chmod=0644 examples/computer-wayland/licenses/mise-MIT.txt /usr/share/wefty/licenses/mise-MIT.txt
COPY --chmod=0644 LICENSE /usr/share/wefty/licenses/wefty-Apache-2.0.txt
RUN /usr/local/libexec/wefty-verify-licenses --generate \
      /usr/share/wefty/licenses/debian-packages.tsv /usr/share/wefty/licenses/non-dpkg-components.tsv \
    && /usr/local/libexec/wefty-verify-licenses --check \
      /usr/share/wefty/licenses/debian-packages.tsv /usr/share/wefty/licenses/non-dpkg-components.tsv \
    && chmod 0755 /usr/share/wefty /usr/share/wefty/licenses /usr/share/doc/wefty-computer-wayland \
    && test -s /usr/share/doc/wefty-computer-wayland/ATTRIBUTIONS.md

LABEL org.opencontainers.image.source="https://github.com/Derek-X-Wang/wefty" \
      org.opencontainers.image.description="Optional Wefty GPU-free Wayland reference Computer desktop"

ENV HOME=/home/wefty \
    XDG_RUNTIME_DIR=/tmp/wefty-runtime \
    XDG_CONFIG_HOME=/home/wefty/.config \
    WLR_BACKENDS=headless \
    WLR_RENDERER=pixman \
    WLR_HEADLESS_OUTPUTS=1 \
    WLR_LIBINPUT_NO_DEVICES=1 \
    LIBGL_ALWAYS_SOFTWARE=1 \
    MISE_DATA_DIR=/wefty/service/home/wefty/.local/share/mise
USER 12000:12000
WORKDIR /
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/wefty-computer-wayland-entrypoint"]
