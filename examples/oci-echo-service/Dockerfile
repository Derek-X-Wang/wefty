# syntax=docker/dockerfile:1.18.0@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf

# This is an acceptance/example image, not a required wefty base image. Both
# build roots are immutable multi-platform indexes so rebuilding one commit
# cannot silently select different toolchain or runtime bytes.
ARG GO_IMAGE=docker.io/library/golang@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83
ARG BUSYBOX_IMAGE=docker.io/library/busybox@sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0
ARG SOURCE_DATE_EPOCH=0

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ARG SOURCE_DATE_EPOCH
WORKDIR /src
COPY go.mod go.sum ./
COPY contract/ ./contract/
COPY cmd/wefty-echo-service/ ./cmd/wefty-echo-service/
RUN CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -trimpath -buildvcs=false -ldflags='-buildid=' \
    -o /out/wefty-echo-service ./cmd/wefty-echo-service \
    && touch -d "@$SOURCE_DATE_EPOCH" /out/wefty-echo-service

FROM ${BUSYBOX_IMAGE}
LABEL org.opencontainers.image.source="https://github.com/Derek-X-Wang/wefty" \
      org.opencontainers.image.description="Wefty multi-architecture OCI acceptance echo service"
COPY --from=build --chmod=0755 /out/wefty-echo-service /usr/local/bin/wefty-echo-service
ENTRYPOINT ["/usr/local/bin/wefty-echo-service"]
