# Fabric-identity realtiming

The trusted main and scheduled realtiming workflows run three production
Fabric rows independently of the plain-Fabric lifecycle matrix:

| Stable row | Credential | Meaning |
| --- | --- | --- |
| `fabric.machine_dns_acl` | existing tagged `TS_AUTHKEY` | A second ephemeral machine peer resolves and dials the listener through real Fabric DNS and ACL policy, and the listener authenticates a machine identity. |
| `fabric.machine_second_peer_reachability` | existing tagged `TS_AUTHKEY` | Two distinct Fabric peers complete a request/response round trip. |
| `fabric.person_whoami` | untagged `TS_AUTHKEY_CI_TESTER` | L1 accepts an authenticated person and returns its stable Fabric, user, and device IDs through `/v1/whoami`. |

These jobs never run on `pull_request` or `pull_request_target`. Pull-request
receipts keep all three rows as `NOT-RUN` with reason
`pull_request_secretless`; they do not receive either credential. On trusted
main and scheduled runs the machine rows are required to execute with the
already armed `tsnet-smoke` credential. A failed or unexpectedly skipped
machine job fails the result gate.

## Arm the CI test person

The person row is deliberately safe before provisioning. While
`TSNET_CI_TESTER_REQUIRED` is unset or not `true`, the person job is skipped
and the result receipt records `fabric.person_whoami` as `NOT-RUN` with reason
`secret_unarmed`. The workflow remains green when the machine rows and the
rest of realtiming pass; the result is never reported as a fully executed
Fabric PASS.

To arm it, the owner must:

1. Create a dedicated `ci-tester` user in the tailnet. It must be a second
   user, not the owner account and not a tagged machine identity.
2. While acting as that user, mint a reusable, ephemeral, **untagged** auth
   key. A tagged key cannot satisfy the person route and must not be used.
3. Store the key as the GitHub Actions repository secret
   `TS_AUTHKEY_CI_TESTER`.
4. Set the GitHub Actions repository variable
   `TSNET_CI_TESTER_REQUIRED=true`.

The workflow checks that an armed secret is non-empty before running. Missing,
tagged, expired, or unauthorized credentials fail the person job and therefore
the result gate; they are not converted to `NOT-RUN`.

## Receipts and gate

Each executed job uploads a candidate-bound partial receipt. The
`realtiming-result` job combines those partials with the observed GitHub job
conclusions and validates the combined `fabric-identity-receipt.json` using
`scripts/check-fabric-identity-receipt.sh`.

An executed row is `PASS` only when every live assertion succeeded, and its
`dev.plain_fabric_identity` deviation is absent. A row that did not execute
retains that deviation and a typed reason. The checker cross-validates receipt
status against job conclusion, so changing a skipped person job to `PASS`
fails the gate.

Network presentation evidence is taken only from `fabric.ConnectHost()`.
Logical Wefty names are used to dial inside the test but are not written to
receipts. Provider types and provider-specific DNS or service-name fields stay
behind `fabric/tsnet`; `scripts/check-fabric-boundary.sh` and the receipt gate
enforce that boundary.
