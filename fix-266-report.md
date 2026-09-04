# Fix #266 report

## Decision

The primary CLI handle for a Computer connection is the stable, memorable
friendly name that the operator assigned to the Computer. The Fabric-produced
raw connect host remains visible as a secondary field and remains the exact
host accepted by the Fabric dial path. Neither value is identity,
authorization, placement, or scheduling authority.

Starting HEAD was `4a17dc7c3f4f07331f6abd5883812507cf608494`, and
`git merge-base --is-ancestor 4a17dc7 HEAD` exited `0`.

## Surfaces changed

- The person-authorized L1 take-over availability projection now carries the
  Computer's wefty-owned `friendly_name`; its OpenAPI contract requires it.
- `services takeover view` prints and serializes `friendly_name` first and the
  actual dialed `connect_host` second. The private full endpoint and bearer
  remain only in the owner-readable session capability file.
- The take-over client retains the raw host from the validated endpoint while
  continuing to pass that endpoint host unchanged to `Fabric.Dial`.
- An untagged CLI contract test pins human/JSON ordering, and an untagged
  take-over client test pins raw-host input and projection. The tagged real
  front-door acceptance test now proves the same behavior through a routed raw
  host while retaining the existing direct-backend leakage checks.
- `CONTEXT.md`, the Fabric/registration contracts, CLI help, and the OCI node
  runbook now distinguish the primary Friendly name from the secondary Connect
  host.

## Gate evidence

| Gate | Exit code | Result |
| --- | ---: | --- |
| `gofmt -l . \| (! grep .)` equivalent formatting check | 0 | no files reported |
| `bash scripts/check-fabric-boundary.sh` | 0 | Fabric import boundary clean |
| `go vet ./...` | 0 | clean |
| `go test ./...` | 0 | all packages passed |
| `go test -tags=service_acceptance ./...` | 0 | all tagged packages passed |
| `cd workflows/dogfood && npm ci` | 0 | dependencies installed, no vulnerabilities reported |
| `cd workflows/dogfood && npm run lint` | 0 | passed |
| `cd workflows/dogfood && npm run typecheck` | 0 | passed |
| `cd workflows/dogfood && npm test` | 0 | passed |
| `cd workflows/dogfood && npm run build` | 0 | passed |
| `git diff --check` | 0 | clean before report creation |

VERIFIED — all required local gates exited `0`; hosted CI is reported
separately after the pull request is opened.
