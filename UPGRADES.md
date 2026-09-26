# Flow 0.14.0

A minor release on signal-flow 6.2.0 and meta-signal-flow 8.0.2: the ordinary
wire gains `Exited` and `Retired` flow lifecycles, the privileged wire gains
`Retire`, and the witnessed faults on the Start and List path are fixed
(flows/e167d8, with evidence from flows/88475f and Field Luna).

- A `LaunchSource.SourcePath` is accepted written absolute or relative. One
  rule holds for both: an absolute path is taken as written, a relative one
  under `FLOW_SOURCE_ROOT`, and a source is read exactly when the path it
  resolves to lies inside that root. Anything outside, by `..` or by symlink
  or by absolute spelling, is still `SourceOutsideRoot`. Start no longer
  refuses a profile whose source path is spelled the way
  `system_prompt_bundle_file` requires.
- Once a launch receipt is witnessed, Flow itself continues the seat into its
  brief. The receipt footer asks for the marker and nothing else, which is
  what makes it verifiable and also what ends the seat's first turn: the brief
  the same prompt carries used to sit there until a human sent a second
  prompt. Flow now types one line, "Launch receipt confirmed. Begin the brief
  in your first prompt now.", into the seat's bound pane as soon as the launch
  is Started (or Replaced, once the successor is routable). Receipt
  verification is untouched, and the first prompt is never re-sent. A
  continuation that cannot be typed is logged; the flow is Started either way.
- `List` reports each flow's true lifecycle. A listed flow that is still live
  is reconciled against Herdr: its bound pane present, the route is refreshed
  and the flow is reported `Active`; its bound pane gone, the flow is reported
  `Exited`, a new lifecycle for the seat whose pane went away without Flow
  closing it, with no route and no endpoint. Its row, origin and history stay.
  A Herdr that cannot be read changes nothing either way.
- **`List` writes nothing.** It is a query, and a query does not change what it
  is asked about. Reading the truth and recording it are separate: only a
  command that witnesses a pane's fate persists an ended lifecycle — `Stop` and
  a replacement's reap record `Stopped`, `Retire` records `Retired`, and a
  `Send` whose bound pane Herdr says is gone records `Exited` at the point the
  observation is made.
- **A pane going away never retires a flow.** An exit retains the flow's record
  and says only that the seat is no longer there; `Retired` comes from
  authority — the privileged `Retire` — and no observation ever produces it.
  The three ended states each name who ended the flow: Flow itself (`Stopped`),
  the seat (`Exited`), an owner (`Retired`). None is a deletion.
- `Send`, `ResolveRecipient`, `Stop` and `Replace` treat all three as gone:
  `FlowStopped`, `FlowUnavailable`, `AlreadyStopped` and `PredecessorStopped`.
- `Retire` is new on the privileged contract: one `FlowId`, answered with
  `FlowRetired` carrying the whole row, or `RetireRejected` naming
  `UnknownFlow`, `AlreadyGone` or `StoreRefused`. It is how a seat Flow lost —
  retired elsewhere, or gone without Flow closing it — leaves Flow's receiving
  without its row, origin or history being dropped, and without pretending
  Flow stopped it. Flow had no such verb at all, which is why d8df70 and
  e51411 stayed listed Pending after messenger-clj had retired them
  (flows/88475f, Field Luna's audit).
- A Pending flow becomes Active when it is witnessed live, which is now a
  Presented Send *or* List finding its bound pane present in Herdr. Only a
  Presented Send promoted one before, so a seat bound through
  `MetaBindExisting` or started before Flow Nexus stayed Pending for as long
  as it lived: of fifteen listed flows only four read Active while every Mind
  and Field seat was running. The Send path itself is unchanged — it still
  promotes only on Presented.
- A plain `Start` answers `Started` for a launch that succeeds. It used to
  answer `StartAmbiguous` the instant the first prompt was submitted, leaving
  0.12.2's watcher to promote it later, so every caller of a working launch
  had to handle a non-failure. The ambiguity is not intrinsic — the Nexus is
  already watching the seat's transcript — so Start waits for the launch to
  settle and answers the settled outcome. It waits on the settlement, never on
  a timer; the dispatch gate is released throughout, so the promoter and every
  other query run meanwhile. A seat that never answers at all still reaches
  the bound (three minutes) and is answered `StartAmbiguous`, which the
  watcher and `Observe.Launch` handle as before.

# Flow 0.12.2

A patch: no wire or contract change. A binding of a flow Flow already holds
can give that flow its role.

- `MetaBindExisting` of a FlowId already in the store is no longer refused
  outright. When the stored flow is that binding (not Stopped; the same native
  thread and harness; a route on the same Herdr session, pane and terminal,
  whatever the agent is now named), the binding's role is recorded if the flow
  has none. Nothing else is written: lifecycle, endpoint, route and origin stay
  as stored, and a repeat changes nothing. The reply is
  `Bound.{ <flow> RegisteredUnconfirmed }`, the only bound form the meta
  contract has; for a flow that was already Active it confirms nothing new.
  This lets a roleless registered flow such as 5f38bc (meta `RegisterFlow`)
  take a role without disturbing its binding.
- Any other binding of a held FlowId, or a role different from one already
  recorded, is still refused as `DuplicateFlowId` and writes nothing.
- Stated, with fixtures, not changed: an imported Codex flow is routed through
  its Herdr pane alone. `MetaBindExisting` stores its Codex endpoint as
  `Unavailable`; nothing on the ResolveRecipient or Send path consults the
  endpoint or the Codex runtime configuration. ResolveRecipient reports the
  Herdr route and the endpoint as separate facts, Send types into the pane when
  the route is Available, and a Presented Send promotes Pending to Active with
  the endpoint left `Unavailable`.

# Flow 0.12.1

A patch: no wire or contract change. Opening the store no longer depends on
every launch-attempt row reading in the current shape.

- Each `flow_nexus_launch_attempts` row is read by itself when the store
  opens. A row that does not read in the current shape is moved aside whole
  (stored key, original archive bytes, the shape it was found to have) into the
  new `flow_nexus_quarantined_launch_attempts` table, in one commit with its
  retraction. Nothing is dropped and opening never fails on such a row.
- A row archived before signal-flow 3.0.0 added `SystemPromptBundleFile`
  (Flow 0.6 and earlier) is carried forward into the current shape with an
  empty `SystemPromptBundleFile` and logged as
  `LaunchAttemptMigrated.{ <launch-request> BeforeSystemPromptBundle }`. A carry
  interrupted after the move aside completes at the next open.
- A row no known shape reads stays only in quarantine and is logged as
  `LaunchAttemptQuarantined.{ <launch-request> «<decode error>» }`.
- With every row readable, role adoption reads the launch profiles again: a
  flow bound by a launch takes its role from that launch's profile. On a copy
  of the live store (2026-09-25) the one earlier row
  (`flow06-luna6-20260924-2305`, a Flow 0.6 acceptance launch) is migrated,
  88475f resolves `CallerResolved.{ 88475f Psyche Medium claude-opus-5-5 }`,
  and the ambiguous-launch scan reads again. 5f38bc stays without a role: it
  was registered through the meta `RegisterFlow` and no launch in the store
  bound it.
- The rows that failed were never a 0.10 → 0.11 change: signal-flow 4.0.1 and
  5.x archive `LaunchAttempt` identically, and 0.10.7 logs the same decode
  failure on the live store.

# Flow 0.12.0

An additive wire change through signal-flow 5.1.0 (74a47ed) and
meta-signal-flow 6.0.4 (fbfe897), and a new store table: Flow knows the caller.

- `ResolveCaller.Option<FlowId>` answers `CallerResolved.{ FlowId FlowAspect
  PowerLevel ModelName }`, or `CallerResolutionRejected` with `CallerUnknown`
  or `CallerMismatch.Caller`. The caller is the peer process of the ordinary
  connection (`SO_PEERCRED`), never a field of the request; its Herdr pane is
  read from `HERDR_SESSION` and `HERDR_PANE_ID` in its `/proc` environment,
  or its nearest ancestor's. That pane must hold exactly one routable flow
  whose binding the live snapshot still shows. The optional FlowId is the
  caller's claim; a different one is `CallerMismatch`, carrying the true
  Caller.
- The store keeps each flow's role in a new `flow_nexus_roles` table, written
  by MetaBindExisting (from the binding's aspect, power and model) and by
  Start (from the launch profile). A flow registered through the meta
  `RegisterFlow` has no role and resolves `CallerUnknown`.
- On open, a store written before roles adopts them: from the launch profile
  of the launch that bound the flow, else from the `<aspect>:<power>:<model>`
  flow type MetaBindExisting wrote. The live Pending flows bound by
  MetaBindExisting therefore resolve without rebinding. The live store's
  launch attempts do not decode under the current contract (read on a copy,
  2026-09-25), so adoption reads the flow types alone there; the two flows
  launched by Start (5f38bc, 88475f) keep no role and resolve
  `CallerUnknown` until they are bound again.
- `flow 'ResolveCaller.None'` run inside a bound pane names that pane's flow.
  Send does not use it yet.

# Flow 0.11.0

A wire change through signal-flow 5.0.0 (cf3648f) and meta-signal-flow 6.0.3
(5926ae7): Send's grades are exact, and no marker is typed.

- `SendRejected` always means nothing was typed. `DeliveryRefused` is
  replaced by `NotDelivered`: Herdr refused before any input reached the pane
  (`agent_blocked`, `agent_not_found`, `agent_not_ready`,
  `agent_target_ambiguous`, `empty_agent_prompt`, `agent_prompt_failed`), or
  the prompt could not be spawned.
- `Sent.Uncertain` is new: the input may have been typed but its reaction was
  not observed (a stalled or timed-out wait, a reply naming another pane, or
  any unrecognised failure). It is answered once and never retried.
- `Sent.Presented` now means Herdr's `agent prompt --wait` saw the settled
  (idle or done) recipient react on the exact pane. The receipt is
  `{ FlowId HerdrPaneId PresentationObservedUnixMilliseconds }`; the marker
  and the pane read are gone.
- `Sent.Accepted` answers a prompt queued to a working agent.
- No presentation marker is appended to any prompt: the pane text is
  the `BareInput` byte for byte. A Pending flow becomes Active only on a
  Presented Send. A Send to a working Pending flow is Accepted and leaves it
  Pending, where 0.10.x refused it.
- A Send to a settled Active flow now waits up to five seconds for the
  reaction and answers Presented instead of Accepted.

# Flow 0.10.7

A patch release with no wire or storage change. Three faults of the first
real Claude Start (successor 88475f, launched through 0.10.5):

- A receipt that arrives after `StartAmbiguous` now promotes the launch with
  no subscriber and no second Start. The Nexus runs its own watch over every
  ambiguous launch (on startup too, so a launch left ambiguous by an older
  Nexus is picked up): each launch's transcript is watched, and each change
  re-observes the receipt. It waits on announced changes, never on a timer.
- The Claude receipt observer stops checking once the first turn and every
  selected skill are confirmed. An instruction that loads further skills
  through the Skill tool (88475f loaded thirteen), a harness notice, or plain
  work before the receipt no longer refuses it as "Skill invocation order
  differs from intent". An `isMeta` user row with plain text (the rename
  reminder) is never read as typed input.
- A Herdr route is keyed on the session, pane id and terminal id. The agent
  name is re-read from the snapshot (ResolveRecipient, Send and Stop report
  the current one) and never matched, so a flow that renames its agent
  (88475f: `claude-86b6e54c…` to `psyche-opus-88475f`) stays routable. The
  receipt observer's `agent get` targets the pane id for the same reason.
- Claude's argv no longer carries `--dangerously-skip-permissions` from Flow:
  the installed `claude` wrapper already execs `.claude-wrapped
  --dangerously-skip-permissions "$@"`, and the pane showed it twice. The
  launch's mode is still set by `--settings
  '{"permissions":{"defaultMode":"bypassPermissions"}}'`. A host whose
  `claude` does not add the flag runs in that settings mode without it.

# Flow 0.10.6

A patch release with no wire, storage or argv-shape change. The Herdr route
rule changes:

- A bound agent (name, pane, terminal and harness matching the binding, as
  before) is routable when its Herdr `agent_status` is `idle`, `done` or
  `working`, and presentable (a Send that waits for its marker) when it is
  `idle` or `done`. Herdr 0.8.2 reports a harness at rest after a turn as
  `done`; Flow now treats it exactly as `idle`.
- `interactive_ready` gates the route only when Herdr reports it: an absent
  (or null) flag permits, `true` permits, any other value refuses. Herdr 0.8.2
  omits the flag for every rested Codex pane observed and for some Claude
  panes, so 0.10.5 resolved every such Codex binding `Unavailable`.
- A binding whose agent is missing from the roster, or whose status is
  anything else (`waiting`, …), stays `Unavailable`. Launch-time readiness
  checks for new panes are unchanged.

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
