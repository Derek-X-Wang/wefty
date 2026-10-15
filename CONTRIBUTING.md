# Contributing

Wefty is an early-stage personal project. Issues and pull requests are welcome,
but there are no response-time promises, and proposed changes may need to wait
until the surrounding contracts settle.

Contributions require a Developer Certificate of Origin sign-off. Create signed
commits with `git commit -s`; the added `Signed-off-by` line records that you
have the right to submit the contribution under the project's license.

Before opening a pull request, run the repository gates:

```sh
test -z "$(gofmt -l .)"
go vet ./...
go test -timeout 30m ./...
bash scripts/check-fabric-boundary.sh

cd workflows/dogfood
npm ci
npm run lint
npm run typecheck
npm test
```

The [v1 design](docs/2026-08-06-wefty-v1-design.md) and accepted
[architecture decisions](docs/adr/) are the decision authority. Changes that
conflict with them should update the governing document explicitly rather than
silently changing the contract in code.

## Moving the Go version

Dependabot does not bump Go itself. Find every pin with one search:

```sh
git grep -nE '^go [0-9]|golang[:@]'
```

- `go.mod`: the `go` line is the version. Every workflow's `actions/setup-go`
  step reads it through `go-version-file: go.mod`; never add a literal
  `go-version:`.
- `examples/oci-echo-service/Dockerfile` and `Dockerfile.service-users`: the
  `ARG GO_IMAGE` digest and the `# golang:<version>-alpine` label above it. The
  image sets `GOTOOLCHAIN=local`, so it must move with `go.mod` or the image
  build fails. Resolve the multi-platform index digest with
  `go run github.com/google/go-containerregistry/cmd/crane@v0.22.0 digest docker.io/library/golang:<version>-alpine`.
  A test in `scripts/` holds the label to `go.mod`.
- `workflows/branch-gates/README.md`: the example `GOLANG_IMAGE` tag.
