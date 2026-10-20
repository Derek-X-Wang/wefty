# PROTOTYPE (wefty #1028, map #1008): multi-Seat Computer image. Throwaway.
# Based on examples/computer/Dockerfile; same base digest and snapshot, but a
# root supervisor starts one desktop per Seat instead of one desktop per image.
ARG DEBIAN_IMAGE=docker.io/library/debian@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171
FROM ${DEBIAN_IMAGE}
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
        xauth procps iproute2 \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/* \
    && printf '%032d\n' 0 > /etc/machine-id \
    && ln -sf /etc/machine-id /var/lib/dbus/machine-id

# Pre-created Seat users. seat5+ are deliberately NOT created: the supervisor
# can also run a Seat as a bare numeric uid with no passwd entry.
RUN for n in 1 2 3 4; do \
      groupadd --gid $((2000+n)) seat$n; \
      useradd --uid $((2000+n)) --gid $((2000+n)) --home-dir /wefty/service/seats/$n/home \
              --shell /bin/sh --no-create-home seat$n; \
    done

COPY --chmod=0755 seat-supervisor.py /usr/local/bin/seat-supervisor
COPY --chmod=0755 seat-run.sh /usr/local/libexec/seat-run
COPY --chmod=0755 rfb-probe.py /usr/local/libexec/rfb-probe
COPY --chmod=0644 page.html /opt/proto/page.html

USER root
WORKDIR /
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/seat-supervisor"]
