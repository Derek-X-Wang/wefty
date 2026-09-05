# Fabric-identity realtiming

The trusted main and scheduled realtiming workflows define three production
Fabric rows independently of the plain-Fabric lifecycle matrix:

| Stable row | Credential | Meaning |
| --- | --- | --- |
| `fabric.machine_dns_acl` | tagged `TS_AUTHKEY` | A second ephemeral machine peer resolves and dials the listener through Fabric DNS, and the listener authenticates a machine identity. Both peers use the same tagged key, so this proves tagged-peer reachability, not ACL differentiation between principals. |
| `fabric.machine_second_peer_reachability` | tagged `TS_AUTHKEY` | A dial reaches a network peer address distinct from the dialer's local address and completes a request/response round trip. |
| `fabric.person_whoami` | untagged `TS_AUTHKEY_CI_TESTER` plus listener `TS_AUTHKEY` | L1 accepts an authenticated person and returns a complete person identity through `/v1/whoami`; tailnet user and device IDs are not published in the public receipt. |

These jobs never run on `pull_request`, `pull_request_target`, or manual
`workflow_dispatch`. They may evaluate credentials only on a `workflow_run`
caused by the main branch's push publication or on the weekly `schedule` for
`refs/heads/main`. Pull-request receipts keep all three rows as `NOT-RUN` with
reason `pull_request_secretless`; manual-dispatch receipts use
`manual_dispatch_secretless`. Neither event receives either credential.

This repository currently has neither credential nor either arming variable.
Trusted main and scheduled receipts therefore keep both machine rows and the
person row as `NOT-RUN` with reason `secret_unarmed`. An unarmed skip is not a
PASS. Once a row group is armed, a failed or skipped job fails the result gate.

## Arm the machine rows

The owner must:

1. Mint a reusable, ephemeral, tagged auth key for the machine Fabric smoke
   peers, with the intended test-tailnet reachability policy.
2. Store the key as the GitHub Actions repository secret `TS_AUTHKEY`.
3. Set the GitHub Actions repository variable `TSNET_SMOKE_REQUIRED=true`.

Until both the secret and variable exist, both machine rows remain
`NOT-RUN(secret_unarmed)`. Setting the variable without a usable secret arms
the contract and makes the machine job fail closed.

## Arm the CI test person

The person row also needs the machine credential for its listener. While
`TSNET_SMOKE_REQUIRED` is unset or not `true`, or while
`TSNET_CI_TESTER_REQUIRED` is unset or not `true`, the person job is skipped
and the result receipt records `fabric.person_whoami` as `NOT-RUN` with reason
`secret_unarmed`. The result is never reported as a fully executed Fabric
PASS.

To arm it, the owner must:

1. Complete the machine-row arming steps above.
2. Create a dedicated `ci-tester` user in the tailnet. It must be a second
   user, not the owner account and not a tagged machine identity.
3. While acting as that user, mint a reusable, ephemeral, **untagged** auth
   key. A tagged key cannot satisfy the person route and must not be used.
4. Store the key as the GitHub Actions repository secret
   `TS_AUTHKEY_CI_TESTER`.
5. Set the GitHub Actions repository variable
   `TSNET_CI_TESTER_REQUIRED=true`.

The workflow checks that armed secrets are non-empty before running. Missing,
tagged, expired, or unauthorized credentials fail the person job and therefore
the result gate; they are not converted to `NOT-RUN`.

## Receipts and gate

Each executed job uploads a candidate-bound partial receipt. The
`realtiming-result` job combines those partials with the observed GitHub job
conclusions and validates the combined `fabric-identity-receipt.json` using
`scripts/check-fabric-identity-receipt.sh`.

An executed row is `PASS` only when its exact row-specific assertion and
evidence schema is present, every required assertion succeeded, and its
`dev.plain_fabric_identity` deviation is absent. A row that did not execute or
did not complete retains that deviation and a typed reason. The checker
cross-validates receipt status against job conclusion, so changing any skipped
row to `PASS` fails the gate.

Network presentation evidence is taken only from `fabric.ConnectHost()` and
must match the Wefty-owned hostname shape. The public receipt omits tailnet
user and device identifiers. Logical Wefty names are used to dial inside the
test but are not written to receipts. Provider types and provider-specific DNS
or service-name fields stay behind `fabric/tsnet`;
`scripts/check-fabric-boundary.sh` and the receipt gate enforce that boundary.
