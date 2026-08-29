ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG MUTATION
ENV WEFTY_CONFORMANCE_MUTATION=${MUTATION}
ENV WEFTY_COMPUTER_VIEW_PORT=9
COPY --chmod=0755 examples/computer-wayland/entrypoint-fixture.sh /tmp/wefty-wayland-entrypoint-fixture
COPY --chmod=0755 examples/computer-wayland/watch-driver-fixture.py /tmp/wefty-watch-driver-fixture
USER 0
RUN if [ -d /opt/wefty-computer-wayland ]; then \
      install --mode=0755 /tmp/wefty-wayland-entrypoint-fixture /usr/local/bin/wefty-computer-wayland-entrypoint; \
      install --mode=0755 /tmp/wefty-watch-driver-fixture /usr/local/libexec/wefty-watch-driver; \
    fi \
    && rm -f /tmp/wefty-wayland-entrypoint-fixture /tmp/wefty-watch-driver-fixture
USER 12000:12000
