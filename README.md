# Flow Nexus

Flow Nexus starts, restarts, and resolves flows. `flow-nexus` is the
no-argument long-running process, `flow` is its ordinary client, and
`flow-meta` is its privileged client. The Nexus persists Flow identity and
policy in one Sema store, and its sockets carry only length-prefixed rkyv
Signal archives.

The wire contracts are independent repositories:

- `signal-flow` defines `Start`, provenance-authorized `Restart`, and
  `ResolveRecipient` for Message Nexus routing.
- `meta-signal-flow` defines `Configure` and reset-credit consumption.

The command boundary stamps the caller's Flow and Codex session into every
start request. The ordinary hot paths are intentionally short:

```sh
flow start codex-medium
flow restart <flow-id>
flow resolve <flow-id>
```

`flow-meta reset <idempotency-key>` asks the Codex app-server to consume the
next eligible reset credit. `flow-meta reset <idempotency-key> <credit-id>`
selects a specific credit. Reusing the key makes a retry stable. It sends the
privileged request through the meta socket; the command never talks to Codex
directly.

Existing daemon sessions enter the authoritative registry through typed meta
operations:

```sh
flow-meta register-codex <flow-id> <thread-id>
flow-meta register-claude <flow-id> <session-id>
```

The two sockets are mode `0600`. The meta edge separates ordinary Flow
operations from administrative operations within the owning Unix user's
processes; it is not a security boundary between processes running as that
same user.

By default the Nexus uses:

- store: `/home/li/.local/state/flow/flow.sema`
- ordinary socket: `/run/user/1001/flow/flow.sock`
- meta socket: `/run/user/1001/flow/flow-meta.sock`
- Codex control socket:
  `/home/li/.codex/app-server-control/app-server-control.sock`

The Codex adapter opens `codex app-server proxy`, then sends `initialize`,
`thread/start`, and `turn/start`. The returned thread is owned by the running
app-server and remains visible to remote-control clients. Restart resumes that
thread and starts its next turn only when the caller's provenance Flow ID and
harness session equal the registered target. The daemon injects `FLOW_ID` and
`FLOW_DIRECTORY` into every Codex child and creates its workspace at
`/home/li/primary/flows/<flow-id>`.

For a user service installation, build the workspace in release mode, install
the three binaries into `~/.local/bin`, copy
`deployment/flow-nexus.service` into `~/.config/systemd/user`, then enable
`flow-nexus.service`. Declarative environments should package the same unit
and binaries instead of retaining this local copy.

Run `cargo test --workspace` for the durable contract, store, command, proxy,
failure, timeout, identity-resolution, and reset-adapter witnesses.
