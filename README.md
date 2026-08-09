# wefty

Personal compute fabric for agent-built tools — deploy once, run on any machine you own.

**warp** — fixed threads: always-on nodes (cloud, home server).
**weft** — the moving thread: workloads that travel across machines.

Planning happens on the [issue tracker](https://github.com/Derek-X-Wang/wefty/issues) via wayfinder map.

## M0 contract gate

The repository currently contains reviewable contracts rather than service
implementations:

- Go wire types and state tables in `contract/`
- versioned JSON Schemas and valid/invalid fixtures in `contract/schemas/` and
  `contract/testdata/`
- the network abstraction in `fabric/`
- L1 client, L1 agent, and L3 OpenAPI documents in `api/openapi/`
- concurrency and state-machine semantics in `docs/contracts/`

Run the local contract gate with:

```sh
gofmt -w contract/*.go fabric/*.go api/openapi/*.go
go vet ./...
go test ./...
```
