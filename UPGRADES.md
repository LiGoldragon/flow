# Flow 0.10.5

A patch release with no wire, storage or argv-shape change.

- Codex's `# Flow launch` record no longer repeats `Predecessor:` and
  `Remembered flows:`; the bundle text's trailing section, which opens the
  same block, is their one home.
- A per-launch bundle copy is removed when its launch is refused and the
  outcome is stored, or when the Flow the launch bound is stopped (`Stop`, or
  the reap of a `Replace`). A Started Flow keeps its copy while it runs.
- The launch request short form grows from eight to sixteen hex digits of the
  SHA-256, so the remote-control name is `flow-<16 hex>` and the copy is
  `launch-<16 hex>.md`. Eight digits already collided (`launch-4646` and
  `launch-72333`). Copies written by 0.10.4 under eight-digit names are not
  pruned; remove them by hand from `~/.local/state/flow/launch-bundles/`.

# Flow 0.10.4

A patch release with no wire, storage or argv-shape change. A launch's
predecessor and remembered flows move into the system-prompt bundle, which is
per launch. At Start the Nexus writes each Claude launch its own copy of the
caller's bundle under `~/.local/state/flow/launch-bundles/launch-<short>.md`
(the caller's bytes unchanged, then, when set, a blank line and
`Predecessor: <flow-id>` and `Remembered: <ids>`), passes that copy as
`--system-prompt-file`, and names it in the one-line prompt. Codex receives
the same lines at the end of the bundle text in its first block. The caller's
bundle is never edited. A copy that cannot be written refuses Start as
`CompositionRefused`.

# Flow 0.10.3

A patch release with no wire or storage change. `Replace` tells an unreadable
Herdr from an absent pane. A failed or roster-less snapshot of the
predecessor's session, or its pane ID shown under another binding, refuses the
reap as `ReapRefused.RouteUnavailable` (retryable, nothing closed, successor
held); only a readable snapshot without the pane counts as reaped. 0.10.1
counted a failed snapshot as reaped and could leave the old pane open.

# Flow 0.10.2

The Claude first prompt becomes one line; no wire, storage or argv change.

- A Claude first prompt is one line of at most 800 characters with no line
  break: up to five stacked `/name` commands in profile order, then one
  sentence naming the system-prompt bundle to read, any skills past the fifth
  for the Skill tool, the goal and any source paths, then the receipt request.
  The multi-line `# Flow launch` block is gone from Claude's prompt; it stays
  for Codex. Claude Code wraps a longer line, or four or more lines, as pasted
  content, and a wrapped block expands no command.
- A profile whose Claude line would break or pass 800 characters is refused
  at composition (`ClaudeFirstLineBroken`, `ClaudeFirstLineTooLong`), which
  Start answers as `CompositionRefused`.
- The observer refuses a Claude first turn that arrived as pasted content or
  as plain text with no stacked command loaded. A 0.10.1 journaled Claude
  attempt no longer matches the observer's footer; let it settle before
  upgrading.

# Flow 0.10.1

A patch release with no wire or storage change. `Replace` now treats a
predecessor whose pane is already gone from Herdr as already reaped: the
predecessor is recorded `Stopped`, `Replaced` is recorded, and the successor
routes. `ReapRefused` remains for a close that fails on a pane that exists; a
request refused as `ReapRefused.RouteUnavailable` earlier settles as
`Replaced` when it is sent again. `signal-flow` is pinned at 4.0.1 and
`meta-signal-flow` at 6.0.2, both regenerated on ethos-zero 13.0.0 with the
wire unchanged.

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
