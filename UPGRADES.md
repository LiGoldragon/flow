# Flow 0.10.0

The Claude launch shape changes; no wire or storage change.

- A Claude first prompt stacks up to five `/name` commands at its head, in the
  launch profile's skill order, and names any skill beyond the fifth for the
  Skill tool. Claude Code loads at most five stacked commands and passes the
  rest as argument text. The observer accepts up to five command records,
  each with its expansion and the same argument, before the body. A 0.9
  journaled Claude attempt with more than one skill no longer matches the
  observer's reconstruction; let it settle before upgrading.
- Start sets the canonical native title `<Aspect>V2.{ <Model> <FlowId> }`
  and the Herdr pane label after the claim and reads both back before any
  prompt. The model display name comes from a fixed copy of the workspace
  model-display map; an unmapped model refuses the Start (`BindingRefused`).
- The Claude pane drops inherited `CLAUDE_JOB_DIR`, `CLAUDE_CODE_SESSION_ID`
  and `CLAUDE_CODE_SESSION_KIND` besides `CLAUDE_CODE_CHILD_SESSION`.
- Claude starts with `--settings '{"permissions":{"defaultMode":"bypassPermissions"}}'`,
  which suppresses the "make auto mode your default permission mode" offer.

# Flow 0.9.0

Flow 0.9.0 speaks `signal-flow` 4.0.0 and `meta-signal-flow` 6.0.0. Both wires
change: upgrade `flow`, `flow-meta`, and `flow-nexus` from one package closure.

- `Replace` launches a successor with its predecessor named. The predecessor
  is recorded `Stopped` and its pane closed before the successor is routable.
- `LaunchStatus` answers a launch request's outcome or its pending phase.
- `Observe.Launch` streams one `LaunchPending` frame per phase change and ends
  with the outcome. The `flow` CLI prints each frame until the Nexus closes.
- `ResolveRecipient` now refuses a `Stopped` flow with `FlowUnavailable`.
- Meta `Configure` carries the source root and both Codex endpoints, and
  `flow-meta` takes it as one inline datom (`flow-meta 'Configure.{ … }'`);
  the two-argument `configure` word command is gone. The `FLOW_SOURCE_ROOT`
  and `FLOW_CODEX_*` deployment overrides still apply at start.

Storage: two new tables (launch outcomes, replacements) are created on open.
The stored socket record keeps its former archive, so an existing store opens
unchanged. A launch settled before 0.9.0 has no stored outcome; its
`LaunchStatus` answers `LaunchPending` with its last phase.

# Flow 0.8.1

A Codex main Flow now receives its system-prompt bundle's text at the top of
its first block, above the `$name` skill lines, in place of the line
`System prompt: read <bundle>`. Codex keeps its stock base instructions, and
native Codex descendants never see the block. The bundle must be non-empty
UTF-8 for a Codex launch.

The Claude remote-control name is now unique per Flow: `flow-` and the first
eight hex digits of the SHA-256 of the launch request ID, instead of the role.
No wire or storage change; a 0.8.0 journaled attempt keeps its stored prompt
digest and is promoted as before.

# Flow 0.8.0

A fresh Flow now receives one prompt. The native harness starts with no
positional startup block; the composed first prompt carries the native skill
invocation (Claude `/name` at its head, Codex `$name` lines beside the typed
skill inputs) and then the body.

The composed prompt carries no launch request ID, hash, or inlined source
text. The receipt line is now `FLOW_LAUNCH_RECEIPT_V2`, with no fields. A
launch attempt journaled by 0.7.x asked for the old receipt line and cannot be
promoted by 0.8.0; resolve or discard such attempts before upgrading. The
Claude remote-control name is now the role, such as `flow-field-high`, instead
of `flow-<launch request ID>`.

# Flow 0.4.0

Flow 0.4.0 adds the privileged `MetaBindExisting` request from
`meta-signal-flow` 4.0.0 while retaining the deployed ordinary
`signal-flow` 2.0.0 contract.

The privileged wire is not archive-compatible with Flow 0.3.0. Upgrade the
`flow`, `flow-meta`, and `flow-nexus` binaries from one package closure. Do not
mix an older meta client with the 0.4.0 server.

This release intentionally has no migration for pre-live Flow rows. Before the
first bootstrap, the authorized executor removes the explicitly selected old
Flow stores and starts Flow 0.4.0 with a fresh store. A privileged caller then
submits one verified `FlowContainer` and its ordered vector of existing typed
flows. Accepted bindings enter the ordinary v2 store as `Pending`; the meta
reply reports `RegisteredUnconfirmed`. No native receipt is inferred, and no
binding becomes `Active` through this request.

The first bootstrap may be assembled by hand. A collector/import tool is a
later change and is not part of this release.
