# L1 client contract

The supported client surface is [l1-client.v1.json](../../api/openapi/l1-client.v1.json).
L3 uses that HTTP protocol as a reference client (ADR-0006); it does not share
L1 storage or internal authority predicates.

## Current person administrator check

`POST /v1/person-admin-check` is a read available only to the configured run
ledger's Fabric node identity, carrying the L1 client principal. Other clients,
people, agents and attempt credentials cannot call it. It accepts exactly:

```json
{"fabric_id":"issuer","user_id":"person","device_id":"device"}
```

These are Fabric identity evidence authenticated by the ledger, never caller
request fields. The issuer, user and device must satisfy the same stable-person
validation as L1 job cancellation. The response contains only:

```json
{"current_admin":true,"policy_revision":1}
```

L1 uses `requireCurrentAdmin`, the exact predicate used for job cancellation,
in a read-only transaction. Membership and policy revision come from the same
snapshot. A valid non-admin person receives HTTP 200 with `current_admin=false`.
Invalid person evidence receives `401 person_identity_required`; an unidentified
ledger receives `401 unauthorized`; other principals receive `403 forbidden`
or `principal_forbidden`. This read reveals no roster and changes no policy,
audit records, authenticated-person observations or job authority.

For Run cancellation L3 first checks submitting-actor and root-lineage authority.
Otherwise, it asks L1 for a Fabric person's current membership on every request,
including repeats against terminal Runs. Current admins may cancel any Run,
including Computer-submitted roots. L3 never caches the answer: policy reset or
admin removal revokes this arm immediately on the next request (ADR-0003).
Non-admin callers retain the existing `403 forbidden` response. Missing or
invalid local person evidence provides no admin authority.

An unavailable or erroring L1 check yields L3 HTTP 503, `unavailable`,
`retryable=true`, without recording cancellation intent. This error code is
additive. An L1 authentication failure during delivery of job cancellation is
also retryable, surfaced as L3 HTTP 503 `internal`, never the person's 401.

The ledger continues cancelling jobs as their original L1 submitter. L1 job
cancel accepts no person evidence or "cancel on behalf of" authority.
