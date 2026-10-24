# wefty — agent configuration

## Agent skills

### Issue tracker

Issues live in GitHub Issues (`Derek-X-Wang/wefty`), driven via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Default five-role vocabulary (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at repo root (created lazily by /domain-modeling). See `docs/agents/domain.md`.

## Development gates

Run every gate in `.claude/skills/wefty-dev/SKILL.md` ("Gates") before you claim a change works, whatever your harness. Give `go test` a long limit, for example `go test -timeout 45m ./...`: the `cmd/wefty` package alone can take about 30 minutes on a busy Mac, and Go's default limit is 10 minutes. When you cap CPUs (for example `GOMAXPROCS=2` or `GOFLAGS=-p=1`), use `-timeout 60m` or more.
