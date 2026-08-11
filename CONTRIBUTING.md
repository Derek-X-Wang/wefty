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
go test ./...
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
