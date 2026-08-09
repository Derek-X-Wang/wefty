# wefty

Personal compute fabric for agent-built tools — deploy once, run on any machine you own.

**warp** — fixed threads: always-on nodes (cloud, home server).
**weft** — the moving thread: workloads that travel across machines.

Planning happens on the [issue tracker](https://github.com/Derek-X-Wang/wefty/issues) via wayfinder map.

## M0 contracts and Fabric seam

The repository contains the reviewable contracts plus the first production
seam implementation:

- Go wire types and state tables in `contract/`
- versioned JSON Schemas and valid/invalid fixtures in `contract/schemas/` and
  `contract/testdata/`
- the network abstraction and its localhost/tsnet implementations in `fabric/`
- L1 client, L1 agent, and L3 OpenAPI documents in `api/openapi/`
- concurrency and state-machine semantics in `docs/contracts/`

Callers address fabric services with `wefty://control-plane` and
`wefty://node/<id>`; concrete transport names are resolved inside the selected
implementation. Tests create an isolated `plain.Network` and inject one
wefty-owned identity per participant so authorization paths remain active.

Run the local contract gate with:

```sh
gofmt -w .
go vet ./...
go test ./...
```

The production fabric smoke test is isolated behind a build tag and skips when
credentials are unavailable:

```sh
TS_AUTHKEY=tskey-auth-... go test -tags=tsnet_smoke ./... -run '^TestTSNetSmoke$'
```

Set `TS_CONTROL_URL` as well when testing against a non-default coordination
server. The auth key must be reusable because the smoke test starts one client
and one server, both ephemeral.
