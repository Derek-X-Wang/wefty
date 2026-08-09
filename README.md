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

## M1 node agent

Run a localhost control plane and one real agent with authoritative routing
tags configured only on the control plane:

```sh
go run ./cmd/wefty-l1 \
  --fabric=plain --listen=127.0.0.1:8787 \
  --db=wefty-l1.sqlite --node-tags=my-node=mac,arm64

go run ./cmd/wefty-agent \
  --fabric=plain --control-plane=127.0.0.1:8787 \
  --node-id=my-node
```

The stable `--node-id` survives agent restarts; each process generates a new
boot-session ID. Heartbeats and attempt lease renewals use independent
cadences. SIGINT or SIGTERM starts a graceful drain: the node stops claiming,
lets its current attempt complete, and exits; a second signal forces local
cancellation. For tsnet, also supply `--fabric-name`, `--state-dir`, and an auth key
whose node identity has the `tag:wefty-agent` principal tag. The credentialed
agent smoke remains env-gated by `TS_AUTHKEY` (and optionally
`TS_AGENT_PRINCIPAL_TAG` when a tailnet uses a different tag).
