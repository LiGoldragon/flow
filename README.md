# Flow Nexus

`flow-nexus` owns Flow dispatch, identity, recovery, and policy. `flow` is the
ordinary CLI; `flow-meta` is the privileged configuration CLI. Each accepts
exactly one inline Datom value, encodes typed Signal as a length-prefixed rkyv
frame, and connects only to its own Unix socket.

| Variable | Current value | Purpose |
| --- | --- | --- |
| `FLOW_WORKSPACE` | `/home/li/primary/flow` | repository and Nexus working directory |
| `FLOW_NEXUS_STORE` | `/home/li/primary/flow/flow.sema` | Nexus-owned Sema store |
| `FLOW_ORDINARY_SOCKET` | `/tmp/flow-nexus.sock` | ordinary socket; client override: `FLOW_SOCKET` |
| `FLOW_META_SOCKET` | `/tmp/flow-nexus-meta.sock` | privileged socket; client override: `FLOW_META_SOCKET` |
| `CODEX_APP_SERVER_SOCKET` | `/home/li/.codex/app-server-control/app-server-control.sock` | Codex control socket |
| `FLOW_MODEL` | `gpt-5.4` | configured model |
| `FLOW_EFFORT` | `medium` | Codex turn effort |

Build the five-crate workspace, then run the no-argument Nexus from
`FLOW_WORKSPACE`:

```sh
cargo build --workspace
target/debug/flow-nexus
```

It opens `FLOW_NEXUS_STORE`, resumes saved configuration, and serves the two
default sockets. The contracts use `rkyv`; their Datom edge uses
`datom-codec` and `protos`; the store uses the local `sema-engine` checkout
configured in the Nexus manifest.

Use one quoted Datom argument. These are tested contract forms:

```sh
FLOW_SOCKET="$FLOW_ORDINARY_SOCKET" target/debug/flow \
  'Start.{ codex-medium «map the store» { parent session turn } }'
FLOW_SOCKET="$FLOW_ORDINARY_SOCKET" target/debug/flow \
  'Restart.{ flow-0000000000000001 flow-0000000000000001 }'
FLOW_META_SOCKET="$FLOW_META_SOCKET" target/debug/flow-meta \
  'Configure.{ /tmp/flow.sock /tmp/flow-meta.sock }'
```

`Start` accepts predefined `codex-medium` and returns
`Started.{ <flow-id> { <parent> <session> <turn> } }` only after its thread
and first turn are accepted. A restart requires its second ID to equal the
target child Flow ID, returning `Restarted.{ <flow-id> <generation> }`.
Unknown or non-self authority returns `RestartRejected`; an unsupported type
returns `StartRejected`.

`Configure` replies
`Configured.{ { <ordinary-socket> <meta-socket> } NexusRestartRequired }`.
It saves policy but leaves current listeners unchanged until the Nexus restarts.

A live no-argument Nexus smoke created a daemon-owned Codex thread through the
installed `codex app-server proxy`, then returned
`Started.{ flow-0000000000000001 { flow-self session-smoke turn-smoke } }`.
It rejected the parent (`Restart.{ flow-0000000000000001 flow-self }`) and
accepted the child (`Restart.{ flow-0000000000000001 flow-0000000000000001 }`)
as generation 2. The daemon's `thread/list` independently included the first
smoke thread with `/home/li/primary/flow`, `openai/gpt-5.4`, and `medium`.
That first thread reported `systemError`, so this is evidence of daemon
ownership and accepted RPCs, not a completed model turn. Earlier isolated
proxy probes were silent; their cause is not established. The adapter uses
`codex app-server proxy`, a WebSocket upgrade, `initialize`, `thread/start`,
and `turn/start`; it never invokes `codex exec`.

See [DESIGN.md](DESIGN.md) for the lifecycle and component boundaries.
