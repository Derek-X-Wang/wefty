# OCI echo acceptance image

This Dockerfile packages the repository's `cmd/wefty-echo-service` program for
`linux/amd64` and `linux/arm64`. It is an acceptance and example artifact, not
a required base image, a runtime layer, or a compatibility target for user
images.

CI is the authority for the published artifact. Pull the commit tag only with
the `sha256:` index digest recorded by the `acceptance-image` workflow; the
workflow artifact contains the same digest, a multi-platform OCI archive, and
per-architecture release install manifests. Do not replace that digest with a
mutable tag when submitting a Job or performing an attended acceptance run.
