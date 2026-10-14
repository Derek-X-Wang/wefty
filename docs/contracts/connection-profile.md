# Connection profile

The connection profile is the one file that tells every wefty program on a
machine where the others listen and who each of them is on the plain Fabric.
`wefty setup` writes it at `~/.config/wefty/profile.json` (mode `0600`).
Separate plain-Fabric processes cannot find each other by logical name, and L1
and L3 refuse the built-in `wefty://` defaults in plain mode, so without the
profile every program needs the same endpoint and identity flags.

## Who reads it, and in what order

Every value a program takes from the profile follows one precedence: an
explicit flag, then a non-blank environment variable, then the profile, then
the built-in default. An explicit empty flag (for example `--l3=`) is still an
explicit choice. L1's node admission is replaced whole: any of
`--node-tags`, `--node-max-oneshot-slots` or `--node-max-service-slots` makes
L1 ignore the profile's `node_admission`.

| Program | Finds the profile | Reads |
| --- | --- | --- |
| `wefty` (CLI) | `--profile`, else `$WEFTY_PROFILE`, else `~/.config/wefty/profile.json` when it exists | `fabric`, `plain_fabric_id`, `l1_address` (`--l1`), `l3_address` (`--l3`), `identities.cli`, `identities.person_user_id`, `identities.person_device_id` |
| `wefty-l1` | `--profile`, else `$WEFTY_PROFILE` | `fabric`, `plain_fabric_id`, `l1_address` (`--listen`), `l3_address` (`--run-ledger`), `allow_plain_person_identities`, `node_admission` |
| `wefty-l3` | `--profile`, else `$WEFTY_PROFILE` | `fabric`, `plain_fabric_id`, `l3_address` (`--listen`), `l1_address` (`--control-plane`) |
| `wefty-agent` | `--profile`, else `$WEFTY_PROFILE` | `fabric`, `plain_fabric_id`, `l1_address` (`--control-plane`), `l3_address` (`--run-ledger`), `identities.node` (`--node-id`) |

A named profile must exist. The CLI's default path is optional: when it is
absent the CLI behaves exactly as before setup. The services read a profile
only when one is named, which their LaunchAgents do, so a service started by
hand is unchanged. The CLI reads the profile only for commands that reach the
cluster: `wefty run ...`, `wefty workflow init`, `wefty node ...` and
`wefty setup` never open it, so a job reporting through its run mailbox cannot
fail on it.

## Format

Version 1 is a JSON object. Unknown fields, trailing data, an unsupported
version or fabric, a `plain_fabric_id` without the `plain-` prefix, a node
named twice, negative slots or an invalid tag are refused with the file's path.

```json
{
  "version": 1,
  "fabric": "plain",
  "plain_fabric_id": "plain-wefty-<16 hex>",
  "l1_address": "127.0.0.1:47101",
  "l3_address": "127.0.0.1:47102",
  "identities": {
    "cli": "wefty-cli",
    "person_user_id": "<login name>",
    "person_device_id": "<node ID>",
    "node": "<node ID>"
  },
  "allow_plain_person_identities": true,
  "node_admission": [
    {"node_id": "<node ID>", "tags": ["wefty:node:<node ID>"], "max_oneshot_slots": 4, "max_service_slots": 2}
  ]
}
```

`plain_fabric_id` stays the same across restarts so identities L1 recorded keep
matching. The node ID is chosen once from the host name plus a random suffix
and is recorded in the installed node configuration (`node.json`, which then
has no `control_socket`; see [agent-local-lifecycle](agent-local-lifecycle.md)).

## What setup keeps

A rerun keeps every value the profile already has, including the owner's own
edits, and fills only what is missing. It always sets `identities.node` to the
node configuration's ID, sets `allow_plain_person_identities`, and makes sure
`node_admission` admits that node with its reserved `wefty:node:<ID>` tag.
Each LaunchAgent records a digest of its binary and of exactly the settings
its program reads from the profile, so a change reloads only the services it
affects. A profile that does not parse stops setup; it is never overwritten.

Setup checks everything before its first write: a destination folder reached
through a symbolic link that leads outside the home folder, a destination of
the wrong type, or a skill folder that is a link is refused, and nothing
changes. Every write goes through a handle confined to the home folder. A file
converges on its bytes, its type and its permission bits. A changed service is
unloaded before its new plist is published, so a failed unload is retried on
the next run, and a service that is loaded but stopped is started.
