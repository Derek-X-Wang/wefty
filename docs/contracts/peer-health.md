# Read-only control-service peer probes

L1 and L3 each serve two additive routes, published in their OpenAPI documents.
Existing routes and principal rules are unchanged.

| Route | Caller | Work |
| --- | --- | --- |
| `GET /v1/peer-health` | Client principal only, without a bearer | Observe the configured peer through this service's own Fabric |
| `GET /v1/peer/ping` | Exact configured peer node ID only | Return `{"ok":true}` without another request |

L1 trusts its configured `RunLedgerNodeID`; L3 trusts its configured
`ControlPlaneNodeID`. Saved fleet startup supplies their enrolled opaque IDs.
Person and agent principals cannot invoke peer health. Person authorization
middleware is not used, so there is no identity-observation write. Run,
Computer and attempt bearer credentials confer no permission on either route.

A health check sends one GET to the other service's peer ping. The complete
check, including dial, identity lookup, headers and body, has a two-second
ceiling independent of individual caller cancellation. It follows no redirects,
reads at most 4097 response bytes including headers, uses a fresh connection
and closes it on completion or the shared deadline. Overlapping calls on one
service share the single active check. Every caller, including the initiating
caller, can cancel its own wait without spoiling another caller's observation.
An already-cancelled caller starts no work. There is no result cache or
background polling.

On an authenticated Fabric, the dial target is always the trusted enrolled
opaque node ID, regardless of a legacy address in service configuration. The
adapter resolves that ID only from authenticated network state; it never
substitutes a logical name or legacy default. The probe also asks Fabric
`WhoIs` for the actual connection's remote address. A lookup failure is
unavailable evidence; a different node ID is refused before any ping is sent.
No HTTP response field or display name substitutes for that identity check.

Plain Fabric uses the configured legacy peer IDs and addresses. Its isolated
in-process Network now binds each outbound connection to the answering
listener's injected identity. Listener replacement during a dial invalidates
that evidence, and closing the connection removes it. This is **self-asserted
development evidence**, never production authentication. There is no new plain
wire handshake: independent plain processes do not gain outbound identity
proof from this in-process registry. Without that evidence the probe reports
unreachable. Plain setup, doctor and status do not invoke these probes.

The health response always has these three fields:

```json
{"reachable":true,"error_class":"","checked_at":"2026-10-21T12:00:00Z"}
```

`checked_at` is the UTC time the observation finished. Unreachable results use
only existing protocol classes: `unavailable` for transport/lookup/time limits
or unsuccessful HTTP status; `forbidden` for a mismatched answering ID or peer
refusal; `internal` for an invalid pong; `not_found` when the peer's ping route
is absent. Reachable results have an empty class. The result contains no peer
IDs, addresses, secrets, response payloads or backend diagnostics.

Neither route opens a transaction, writes a database, takes a lease, renews
anything, or changes authority. A pong proves only an accepted read from the
configured peer. It does not prove scheduling readiness, deny-policy behavior,
owner access, or the node agent's network paths.

Fleet setup readiness and fleet doctor read both service observers as the saved
client through the running client service. Both installation roles report
`L1 → L3 direction` and `L3 → L1 direction`. Reachable is VERIFIED; an
unreachable or invalid 200 observation is FAILED. Doctor reports an unreadable
observer as NOT-RUN with `peer observer could not be read; direction unverified`,
and a client 401/403 refusal as REFUSED. Neither result claims the service ran
a peer probe. Readiness retains FAILED for an unreadable observation. If either
the observer endpoint or its peer ping is absent on an older service, the
result is NOT-RUN with `peer probe unavailable on this version`. No direct
participant fallback exists. An unverified client identity leaves probes
NOT-RUN. A missing-version result does not itself fail readiness or doctor;
a FAILED result does. Doctor retains existing identity/transport exit
precedence when reading an observer fails. Peer reads retain the shared
15-second doctor budget and the three-second per-read limit. If the shared
budget expires, the affected direction is NOT-RUN with
`doctor time budget expired`; no request starts for a remaining direction
after that deadline. No new protocol error code or CLI exit code is introduced.
