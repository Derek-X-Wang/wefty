# Do the Buzz-note gaps still exist on main?

Date: 2026-10-12. Ticket: #858 (map #855). Method: code, contracts and history on `main` at `88674cd2` ([pinned tree][main]); `gh issue view` / `gh pr view` for history; targeted Go tests run on this Mac; CI logs for the one Linux-only real-engine row. No accounts, credentials, Docker, Lima or sudo.

**Answer.** All three gaps that `docs/research/2026-10-04-buzz-on-wefty.md` section 4 found are **closed on main**. Spec #643 ("apps can rely on wefty for headless work") fixed them in eleven tickets (#644–#654, PRs #655–#669), merged 2026-10-05, plus follow-ups (#665 and #671 are the relevant ones). An app that talks only to the L1 client contract (ADR-0006) can now start a container one-shot, run a service that stays stopped after a clean exit, cancel its jobs, and reserve an **instance key** so it never has two live bodies. Gap 3 changed shape. A second start while one is live gets a typed refusal (`409 instance_key_conflict`) that names the live job; it is not a silent no-op. The app must also use a new dispatch key for each new start. Other problems remain that the Buzz note did not list, and an app running a long-lived AI agent will hit them (section 2). The most important: **OCI jobs silently ignore `limits`** (no maximum runtime, no idle guard). **Process one-shots kill a quiet AI agent after 10 minutes** of no output unless the app raises `idle_timeout_seconds`. **Every running attempt dies if L1 is out of reach for longer than the 30-second lease**.

**Evidence legend:** **[VERIFIED]** means observed in the cited code, contract or run output on main. **[UNVERIFIED]** means inference, sizing or recommendation. **NOT-RUN** marks checks not performed.

## 1. The three gaps

| # | Gap in the Buzz note | Status on main | Fixed by |
|---|---|---|---|
| 1 | An app cannot start an OCI one-shot without a run identity that only L3 or a child may name | **Fixed** | #644, #645, #646, #647 (PR #668, merge `006e92b2`) |
| 2 | Services allow only `restart=always`, so a clean exit is restarted | **Fixed** | #648 (PR #656), #649 (PR #660), #665 (PR #672) |
| 3 | A dispatch key returns the original job even after it ended; Buzz needs "no-op while live, fresh body after it ended" | **Fixed, changed shape** | #653 (PR #658), #654 (PR #664) |

All seven merge commits named above are ancestors of `main` (`git merge-base --is-ancestor`). **[VERIFIED]**

### Gap 1: container one-shots without a run identity — fixed

- **[VERIFIED]** The job's server-assigned ID is now the fallback owner. `ExecutionHandoffOwnerKey` returns the run labels if present, else the job ID for any OCI one-shot (and for a process one-shot with no explicit directory). The submitted labels are not changed, so run entitlement and the request hash stay as they were. ([contract/handoff_owner.go:34-49][ho-exec])
- **[VERIFIED]** L1 no longer refuses an unlabelled OCI one-shot. `run_identity_required` now fires only for a *malformed explicit* owner (over 255 bytes or containing NUL), after dispatch replay and entitlement checks. ([contract/handoff_owner.go:60-75][ho-validate], [l1/store.go:2375-2384][store-runid])
- **[VERIFIED]** The node agent keys the helper volume, result capture and upload by that owner. ([agent/execution_handoff.go:22][exec-handoff], [agent/run_mailbox_remote.go:195][mailbox-remote])
- **[VERIFIED]** The published contract says so: "An ordinary client may submit a kind=oci one-shot without run identity or L3." ([api/openapi/l1-client.v1.json:221][openapi-create])
- **[VERIFIED]** Tests on main: an ordinary client's unlabelled OCI one-shot gets `201` ([l1/run_identity_entitlement_test.go:118-126][entitle-test]); a real L1 plus a real node agent returns the OCI result through `GET /v1/jobs/{id}/result` with no L3 ([agent/oci_handoff_result_test.go:26][oci-result-test]). The real-engine row `direct-L1-job-owned-result` ([runner/ocihelper/containerd_engine_realtiming_linux_test.go:757][realtiming-row]) passed on both Ubuntu jobs of CI run 38331599155 at `bd9f4368`, nine commits behind `main`. None of those nine commits touch this path. ([CI run][ci-run])
- Naming a run is still reserved to the run ledger or to a child naming its parent's run (#583). That is correct; an app does not need it now.

### Gap 2: restart policy — fixed

- **[VERIFIED]** The validator accepts `always` (the default when omitted), `on-failure` and `never` for services. Computers must still be explicitly `always`. ([contract/job_spec.go:86-101][jobspec-restart], [contract/types.go:341-343][types-restart]) The CLI accepts the same three values. ([cmd/wefty/service_command.go:115][cli-restart])
- **[VERIFIED]** A clean exit (code 0) under `on-failure` or `never` records a **policy stop**. The job becomes `stopped` and is not restarted. Desired state is not rewritten (ADR-0004). ([l1/restart_policy.go:184-188][rp-clean])
- **[VERIFIED]** An exit that the node agent or its guardian asked for (shutdown, a directive, lost authority) is counted as an interruption, never as a policy stop. `on-failure` requeues it. `never` leaves it failed once the program had started. ([l1/restart_policy.go:177-183][rp-initiator], [l1/restart_policy.go:205-213][rp-never-infra], [l1/restart_policy.go:227-232][rp-never-fail])
- **[VERIFIED]** Lease loss after start does not requeue a `never` service. ([l1/store.go:4771-4782][store-lease])
- **[VERIFIED]** The job view says why the service is not restarting, for example `"policy stop: on-failure payload exited cleanly; use start or restart"`. `PUT …/desired-state running` or `POST …/restart` clears the policy stop. ([l1/operator_routes.go:436-454][status-suppressed], [desired-state contract][openapi-desired])
- **[VERIFIED]** Tests: `TestOnFailure*` and `TestNever*` in `l1/`, `TestServicePolicyStopTransitions` in `contract/`, all passing on main (section 5).
- **[UNVERIFIED assessment]** For a Buzz AI agent, use `on-failure`, not `never`. Under `never`, a node agent upgrade or forced shutdown leaves the AI agent `failed`. Under `on-failure`, that interruption is requeued, while Buzz's own idle self-stop or `!shutdown` (exit 0) stays stopped.

### Gap 3: no duplicate bodies — fixed, with a changed shape

**What exists [VERIFIED]:**

- **The instance key.** A job may carry an optional `instance_key` ([contract/types.go:301][types-ik]). It reserves at most one live job per (namespace, key) through a partial unique index. One-shots hold the key until they reach a terminal state. Services hold it in every state until removal finalization deletes the job row. ([l1/instance_keys.go:16-30][ik-index])
- **Namespaces.** A root submission uses `"fabric:" + <authenticated submitter>`. A child uses `"job:" + <parent job ID>`. ([l1/instance_keys.go:39-44][ik-ns])
- **Order of checks.** Dispatch-key replay is resolved first, then the instance key. ([l1/store.go:2331-2337][store-replay], [l1/store.go:2362-2369][store-ik])
- **The refusal.** A losing start, even a concurrent one, gets `409 instance_key_conflict` with `details.job_id` when the caller may read the holder. ([l1/instance_keys.go:53-74][ik-conflict])
- **Release on end.** A terminal one-shot (succeeded, failed, or canceled) releases the key. A pending cancel keeps it until settlement, which has a 30-second deadline. ([l1/cancel.go:25][cancel-deadline])
- **Tests.** Sixteen concurrent creates across two store handles produce exactly one job ([l1/instance_key_integration_test.go:94][ik-concurrent]). A terminal end releases the key, and an old dispatch-key replay still returns the old job ([l1/instance_key_integration_test.go:242-308][ik-terminal]). A service keeps its key through stop, removal, forget and stall until finalization ([l1/instance_key_integration_test.go:355][ik-removal]).

**How Buzz's rule maps onto it [VERIFIED mechanics; mapping UNVERIFIED]:**

| Buzz wants | What wefty does now | What the adapter must do |
|---|---|---|
| Start while live = no-op | `409 instance_key_conflict` naming the live job | Treat the 409 as success and return `details.job_id` as the `agent_id` |
| Fresh body after it ended (one-shot) | Key is free once the one-shot is terminal; a new dispatch key creates a new job | Use a **new dispatch key for each new start** (for example, one per `deploy` call). Reuse it only for retries of that same call. Reusing an old dispatch key replays the old, finished job |
| Fresh body after it ended (service) | The key stays with the stopped service job; `PUT desired-state running` or `restart` starts a fresh attempt **of the same job, with the same spec** | Start or restart the existing job. A changed spec, such as a new secret, needs remove → wait for removal finalization → recreate, because there is no in-place update |
| Competing launchers | One body per key **per Fabric identity** | All launchers must share one Fabric identity (section 2, item 5) |
| Stop | One-shot: `POST /v1/jobs/{id}/cancel`. Service: `PUT desired-state stopped` (cancel is refused for services with `409 cancel_service`) | Map Buzz stop to one of these |

So the semantics Buzz needs are reachable. The differences are a typed refusal instead of a silent no-op, and a dispatch key per new start. Neither needs a wefty change. **[UNVERIFIED]**

## 2. Other things an app hits when it launches a long-running AI agent

These are on main today. They are general (not Buzz-specific) and none is tracked in an open issue that I found (searched issue titles and bodies for "max_runtime OCI", "idle timeout", "termination grace").

| # | What the app hits | Evidence | Smallest general fix | Size **[UNVERIFIED]** |
|---|---|---|---|---|
| 1 | **OCI jobs ignore `limits`.** `max_runtime_seconds` and `idle_timeout_seconds` are accepted on any job, but only the process runner reads them. The OCI adapter forwards only CPU and memory to the helper and never reads `request.Limits` or `request.IdlePolicy`. An OCI one-shot therefore has no runtime ceiling and no hang guard, and the contract does not say so. | **[VERIFIED by code search]** [runner/process/runner.go:274-294][proc-timeouts] is the only reader of `JobLimits`. [runner/oci/adapter.go:2796-2803][oci-limits] maps only CPU and memory. The agent passes `Limits` and `IdlePolicy` to every runtime ([agent/attempt_lifecycle.go:1092-1100][lifecycle-idle]). The schema advertises `idle_timeout_seconds` default 600 for every kind ([job-spec.schema.json:61-68][schema-limits]). Runtime behaviour NOT-RUN. | Enforce `max_runtime_seconds` in the OCI adapter through the existing TERM → grace → KILL path, or refuse `limits` on `kind=oci` until it is enforced. Say in the contract which limits apply to which kind. | Small (refuse and document); medium (enforce, plus a realtiming row) |
| 2 | **A quiet process one-shot is killed after 10 minutes.** Process one-shots watch for output silence (default 600 s). An AI agent waiting for chat messages is often silent. The idle timer cannot be turned off; the schema minimum is 1, and 0 means the default. Services do not watch for silence. | **[VERIFIED]** [runner/process/runner.go:21][proc-idle-default], [runner/process/runner.go:199-247][proc-idle-loop], [agent/attempt_lifecycle.go:1092-1095][lifecycle-idle] | The app sets a large `idle_timeout_seconds`, or uses a service. Optionally add an explicit "no idle guard" value for one-shots. | None for an app (a setting); small for an opt-out |
| 3 | **Every running attempt ends if L1 is out of reach for longer than the lease** (default 30 s). An independent watchdog cancels the attempt at its local authority deadline, and a sleep gap that uses up the lease also cancels it. One-shots then fail for good. Services under `always` or `on-failure` are requeued. This covers L1 restarts and upgrades, the control-plane machine going to sleep (phase 1 runs it on the Mac), and network loss. | **[VERIFIED]** [docs/contracts/agent-local-lifecycle.md:1013-1021][authority-clock], [l1/types.go:13][lease-default], [docs/contracts/lease-fencing-dispatch.md:645-652][lease-oneshot], [l1/store.go:4771-4782][store-lease]. The lease is a cluster-wide L1 flag ([cmd/wefty-l1/main.go:90][lease-flag]). | No contract change needed. Run long-lived AI agents as `on-failure` services, or as one-shots that the app relaunches. Plan the phase-1 upgrade around it. | None to small (guidance) |
| 4 | **Upgrading the node agent waits for long-lived attempts.** The first SIGTERM drains: no new claims, then it waits for every resident attempt, "bounded only by each attempt's own max runtime". A second signal force-cancels them. With an AI agent resident, an upgrade either hangs or kills it. | **[VERIFIED]** [docs/contracts/agent-local-lifecycle.md:23-33][drain], [cmd/wefty-agent/main.go:937-966][agent-signals] | An upgrade runbook (stop or let services requeue elsewhere), or a bounded drain deadline that then interrupts residents as an infrastructure stop. | Small (runbook); medium (drain deadline) |
| 5 | **The instance-key namespace and cancel authority belong to the Fabric node identity of the submitter.** On tsnet that identity is the node's `StableID`. A short-lived adapter program, such as `buzz-backend-<id>`, gets a new identity each run if it uses an ephemeral or fresh tsnet state. It would then lose duplicate protection and the right to cancel its own earlier jobs. | **[VERIFIED]** [l1/server.go:1204][server-submitter], [fabric/tsnet/tsnet.go:191-199][tsnet-id], [l1/instance_keys.go:39-44][ik-ns], cancel contract (originating submitter or a person admin) | Package the adapter with a persistent, non-ephemeral tsnet state directory, so every launcher on that machine is one identity. App identity beyond that is out of scope (owner ruling in #643). | Small (packaging and docs) |
| 6 | **Stop grace is a fixed 5 s.** Cancel, stop and restart use TERM, then 5 s, then KILL. There is no per-job grace field. Buzz's Kubernetes backend allows 60 s. Cancel settlement has a 30 s deadline. | **[VERIFIED]** [runner/process/runner.go:22][proc-grace], [runner/oci/adapter.go:1379][oci-grace], [agent/attempt_lifecycle.go:1221][lifecycle-grace], [l1/cancel.go:25][cancel-deadline] | An optional bounded `termination_grace_seconds` (for example ≤ 25 s, so it stays within the 30 s settlement). Only if a real AI agent needs a longer clean shutdown. | Medium |
| 7 | **No lookup by instance key.** `GET /v1/jobs` filters only by class, kind, state and `submitter=me`. `GET /v1/dispatch-keys/{key}/job` is reserved for the run ledger. An app that lost its own record finds its live body by trying to create it again (the 409 names the holder) or by listing `submitter=me` and filtering. | **[VERIFIED]** l1-client OpenAPI parameters for `GET /v1/jobs` and the dispatch-key route description ([api/openapi/l1-client.v1.json][openapi]) | Add an exact `instance_key` filter to `GET /v1/jobs`. | Small |
| 8 | **Slots.** By default a Node runs at most 4 one-shots and 2 services. Long-lived AI agents hold slots and can starve dogfood one-shots. Slots can be set per Node with L1 flags. | **[VERIFIED]** [l1/types.go:28-29][slots], [cmd/wefty-l1/main.go:114-115][slot-flags] | Operator configuration; no code. | None |
| 9 | **Log retention.** One-shot logs keep 32 MiB per job for 30 days; services keep 32 MiB for 7 days. A chatty AI agent that runs for days will lose old logs. | **[VERIFIED]** [l1/types.go:19-26][log-retention] | None needed for Buzz (the relay holds the conversation). | None |
| 10 | **Not built yet, and not needed for Buzz:** `POST /v1/jobs/{id}/prompt` still answers 501; there are no lifecycle webhooks or streams (logs and status are polled); a service spec cannot be updated in place. | **[VERIFIED]** OpenAPI operation list ([api/openapi/l1-client.v1.json][openapi]); #643 "Out of Scope" | Leave as is until a consumer needs them. | — |

Unchanged since the Buzz note **[VERIFIED]**: `sensitive_env` reaches the payload and is scrubbed from L1 when a one-shot ends or a service is removed. A process job runs as the node agent's OS user, so it is a trusted process, not an isolation boundary.

## 3. Facts for #862 (job shape)

**[UNVERIFIED comparison]**, built from the verified facts above:

| | OCI one-shot | Process one-shot | Service (`on-failure`) |
|---|---|---|---|
| Closest to Buzz's Kubernetes Pod (`restartPolicy: Never`) | Yes | Yes | No |
| Clean self-stop stays stopped | Yes (it ends) | Yes (it ends) | Yes (policy stop) |
| Survives node agent restart or L1 outage > lease | No; the app relaunches | No; the app relaunches | Yes; it is requeued |
| Fresh body after it ended | New job (new dispatch key, same instance key) | Same | Start or restart the same job; a new spec needs remove + recreate |
| Idle and runtime guards | None today (item 1) | 600 s idle unless raised (item 2) | None |
| Stop | `cancel` | `cancel` | `desired-state stopped` |
| Isolation | Container (Lima on Mac) | None (node agent user) | Either kind |

All three shapes work through L1 alone. L3 is not needed and adds nothing here.

## 4. Open questions for the owner

1. **Should an OCI job that sets `limits` be refused, or should wefty enforce them?** Today they are silently ignored. I recommend refusing them now (small) and enforcing them when a cost ceiling matters.
2. **For phase 1 (daily use), should long-lived AI agents survive a control-plane restart?** If yes, they must run as `on-failure` services, or the lease must be longer than an upgrade takes. This also decides how the node agent upgrade runbook treats resident AI agents.
3. **Is one Fabric identity per launching machine acceptable for the Buzz backend?** That is the boundary of "no duplicates" today. Two desktops with different identities could each start a body for the same AI agent.

## 5. Verification and limits

- **VERIFIED:** I ran the targeted tests on main `88674cd2` on this Mac: `go test ./l1/ ./agent/ ./contract/ -count=1 -run 'TestInstanceKey|TestOnFailure|TestNever|TestOnlyTheRunLedgerMayNameARunOnARootSubmission|TestManagedOneShotOwnerDoesNotEntitleChildRunIdentity|TestOCIHandoffResultWithoutL3ThroughL1|TestCancelOCIHandoffResultThroughDirectApp|TestServicePolicyStopTransitions|TestProcessOneShotMayOmitHandoffDirectory'`. Result: 41 top-level tests passed, 0 failed, and all three packages were `ok`.
- **VERIFIED:** CI run 38331599155 (`bd9f4368`, an ancestor of main) passed `TestNativeLinuxOCIAdapterLifecycle/direct-L1-job-owned-result` on both Ubuntu jobs. The run for `88674cd2` itself was still in progress when I checked.
- **NOT-RUN:** I did not run a runtime test of item 1 (OCI ignoring `limits`); the claim rests on reading the code. I also did not run items 3 and 4 (L1 outage, node agent drain) end to end. They come from the contracts and code cited.
- **Limit:** this note checks wefty only. Buzz-side behaviour is taken from the 2026-10-04 note and was not re-inspected.

## Sources

[main]: https://github.com/Derek-X-Wang/wefty/tree/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8
[ho-exec]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/contract/handoff_owner.go#L34-L49
[ho-validate]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/contract/handoff_owner.go#L60-L75
[store-runid]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/store.go#L2375-L2384
[exec-handoff]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/agent/execution_handoff.go#L22
[mailbox-remote]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/agent/run_mailbox_remote.go#L195
[openapi-create]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/api/openapi/l1-client.v1.json#L221
[openapi]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/api/openapi/l1-client.v1.json
[openapi-desired]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/api/openapi/l1-client.v1.json
[entitle-test]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/run_identity_entitlement_test.go#L118-L126
[oci-result-test]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/agent/oci_handoff_result_test.go#L26
[realtiming-row]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/ocihelper/containerd_engine_realtiming_linux_test.go#L757
[ci-run]: https://github.com/Derek-X-Wang/wefty/actions/runs/38331599155
[jobspec-restart]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/contract/job_spec.go#L86-L101
[types-restart]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/contract/types.go#L341-L343
[types-ik]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/contract/types.go#L301
[cli-restart]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/cmd/wefty/service_command.go#L115
[rp-clean]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/restart_policy.go#L184-L188
[rp-initiator]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/restart_policy.go#L177-L183
[rp-never-infra]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/restart_policy.go#L205-L213
[rp-never-fail]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/restart_policy.go#L227-L232
[store-lease]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/store.go#L4771-L4782
[status-suppressed]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/operator_routes.go#L436-L454
[ik-index]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/instance_keys.go#L16-L30
[ik-ns]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/instance_keys.go#L39-L44
[ik-conflict]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/instance_keys.go#L53-L74
[store-replay]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/store.go#L2331-L2337
[store-ik]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/store.go#L2362-L2369
[cancel-deadline]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/cancel.go#L25
[ik-concurrent]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/instance_key_integration_test.go#L94
[ik-terminal]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/instance_key_integration_test.go#L242-L308
[ik-removal]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/instance_key_integration_test.go#L355
[proc-timeouts]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/process/runner.go#L274-L294
[proc-idle-default]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/process/runner.go#L21
[proc-idle-loop]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/process/runner.go#L199-L247
[proc-grace]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/process/runner.go#L22
[oci-limits]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/oci/adapter.go#L2796-L2803
[oci-grace]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/runner/oci/adapter.go#L1379
[lifecycle-idle]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/agent/attempt_lifecycle.go#L1092-L1100
[lifecycle-grace]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/agent/attempt_lifecycle.go#L1221
[schema-limits]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/contract/schemas/v1/job-spec.schema.json#L61-L68
[authority-clock]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/docs/contracts/agent-local-lifecycle.md#L1013-L1021
[lease-default]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/types.go#L13
[lease-oneshot]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/docs/contracts/lease-fencing-dispatch.md#L645-L652
[lease-flag]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/cmd/wefty-l1/main.go#L90
[drain]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/docs/contracts/agent-local-lifecycle.md#L23-L33
[agent-signals]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/cmd/wefty-agent/main.go#L937-L966
[server-submitter]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/server.go#L1204
[tsnet-id]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/fabric/tsnet/tsnet.go#L191-L199
[slots]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/types.go#L28-L29
[slot-flags]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/cmd/wefty-l1/main.go#L114-L115
[log-retention]: https://github.com/Derek-X-Wang/wefty/blob/88674cd28ca7181d24be5d32e2a0d5f86e4e3fc8/l1/types.go#L19-L26

- Pinned main: [tree at `88674cd2`][main]. History: spec #643 and its resolution comments; tickets #644–#654, #665, #671; PRs #656, #658, #660, #664, #668, #672.
- Prior note: `docs/research/2026-10-04-buzz-on-wefty.md`, section 4 (pinned there to `b1c4f081`).
