ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG MUTATION
ENV WEFTY_CONFORMANCE_MUTATION=${MUTATION}
ENV WEFTY_COMPUTER_VIEW_PORT=9
COPY --chmod=0755 examples/computer-wayland/entrypoint-fixture.sh /tmp/wefty-wayland-entrypoint-fixture
COPY --chmod=0755 examples/computer-wayland/watch-driver-fixture.py /tmp/wefty-watch-driver-fixture
COPY --chmod=0755 examples/computer-wayland/view-proxy-fixture.py /tmp/wefty-view-proxy-fixture
COPY --chmod=0755 examples/computer/fixtures/entrypoint.sh /tmp/wefty-xfce-entrypoint-fixture
COPY --chmod=0755 examples/computer/fixtures/watch-driver.py /tmp/wefty-xfce-watch-driver-fixture
COPY --chmod=0755 examples/computer/fixtures/rfb-websocket.py /tmp/wefty-xfce-rfb-websocket-fixture
USER 0
RUN if [ -d /opt/wefty-computer-wayland ]; then \
      install --mode=0755 /tmp/wefty-wayland-entrypoint-fixture /usr/local/bin/wefty-computer-wayland-entrypoint; \
      install --mode=0755 /tmp/wefty-watch-driver-fixture /usr/local/libexec/wefty-watch-driver; \
      install --mode=0755 /tmp/wefty-view-proxy-fixture /usr/local/libexec/wefty-view-proxy; \
    else \
      install --mode=0755 /tmp/wefty-xfce-entrypoint-fixture /usr/local/bin/wefty-computer-entrypoint; \
      install --mode=0755 /tmp/wefty-xfce-watch-driver-fixture /usr/local/libexec/wefty-watch-driver; \
      install --mode=0755 /tmp/wefty-xfce-rfb-websocket-fixture /usr/local/libexec/wefty-rfb-websocket; \
    fi \
    && rm -f /tmp/wefty-wayland-entrypoint-fixture /tmp/wefty-watch-driver-fixture /tmp/wefty-view-proxy-fixture \
      /tmp/wefty-xfce-entrypoint-fixture /tmp/wefty-xfce-watch-driver-fixture /tmp/wefty-xfce-rfb-websocket-fixture
USER 12000:12000
