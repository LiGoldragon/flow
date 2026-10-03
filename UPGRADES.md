# Flow 0.23.0

No wire or storage change. Rebuild and restart the Nexus; `flow`,
`flow-meta` and `flow-hook` come from the same package.

- **FLOW_SOCKET at launch.** Spawn now types `export FLOW_SOCKET=<path>`
  into every Claude launch pane, after unsetting the inherited identity and
  before `FLOW_ID`. The path is the ordinary socket this Nexus serves, read
  from its store's configuration when the Nexus opens (a meta `Configure`
  of the sockets takes effect on restart, as before). The harness and every
  hook it runs inherit it, and `flow-hook`'s `flow` already reads
  `FLOW_SOCKET` before its default, so a launched flow's `Report`s reach the
  Nexus that launched it, whatever `XDG_RUNTIME_DIR` the pane's shell has.
  Until 0.22.0 they went to `$XDG_RUNTIME_DIR/flow/flow.sock` under the
  pane's runtime directory: on a host whose next-slot Nexus serves
  `$XDG_RUNTIME_DIR/flow-next/flow/`, that is the stable Nexus, or nothing.
  The path is exported for an unreserved launch too; it is single-quoted
  for the shell.
- **Unchanged.** A harness started by hand, with no FLOW_SOCKET, reaches the
  default socket under its own runtime directory, as before. Codex panes get
  no FLOW_SOCKET (Codex runs no `flow-hook`). A seat's own `flow` calls now
  also reach its launching Nexus.
- **Deploy.** Install the 0.23.0 package and restart the Nexus. Flows
  launched before the restart keep the environment they started with: their
  hooks report as under 0.22.0 until they are relaunched.

# Flow 0.22.0

Rebuild and restart the Nexus, `flow` and `flow-meta` together. The wire is
unchanged from 0.21.0 (the contracts' generated modules are byte-identical),
and a 0.21.0 store opens unchanged: no table or row format changes.

- **Contracts.** signal-flow 10.0.0 (f95034de) and meta-signal-flow 14.0.0
  (54eb5618), on signal 8.0.0 (f35460de), where 0.21.0 pinned signal-flow
  9.0.0 and meta-signal-flow 13.0.0 on signal 7.0.0. The build reads the
  Operation ethos with ethos-zero 16.0.0 at c2653dd8. The graph holds one
  datom-codec and one protos, 0.32.2.
- **FLOW_ID at launch.** A Claude launch's FlowId is claimed at Reserve,
  before any harness: Flow chooses the Claude session id from the launch
  request id, runs `flow-id claude --flows-root <root> --parent-session
  <session>`, and holds the flow in Memory (its empty events row). Spawn
  exports `FLOW_ID=<FlowId>` in the launch pane and passes `--session-id
  <session>` to Claude, so `flow-hook`'s `Report`s carry the FlowId from
  `SessionStart` on and are answered `Reported`. Bind refuses a harness that
  came up as another session or a claim naming another FlowId
  (`BindingRefused`). A claim that fails at Reserve refuses the launch with
  `StartRejected.BindingRefused` before any pane is opened.
- **Operation root.** `Reserved` carries `{ LaunchAttemptReservation
  Option<FlowId> }`, `PaneLaunch` carries `Option<FlowId>`, `Failed` gains
  `ClaimRefused`. These are the Nexus's own types, not on the wire.
- **Unchanged.** A Codex launch reserves no FlowId and binds as before. A
  harness started without FLOW_ID reports nothing, as before.
- **Deploy.** Install the 0.22.0 package (it carries `flow-hook` next to
  `flow-nexus`) and make sure `flow-id` is on the Nexus's PATH, as Bind
  already required; restart the Nexus. Launches in flight across the
  restart: an attempt reserved by 0.21.0 holds no FlowId and is resumed or
  reported pending as before, never respawned.

# Flow 0.21.0

Wire and storage change. Rebuild and restart the Nexus, `flow` and
`flow-meta` together; an older client and a 0.21.0 Nexus do not read each
other's frames. A 0.20.0 store opens unchanged: the events live in a new
table, and every flow it holds starts with none.

- **Contracts.** signal-flow 9.0.0 (2cc48792) and meta-signal-flow 13.0.0
  (dd7b7df7), still on signal 7.0.0 (66e7b153), where 0.20.0 pinned
  signal-flow 8.0.0 and meta-signal-flow 12.0.0. signal-flow 9.0.0 renames
  the Start reply's payload type `Started` to `Launched` (and
  `Replaced.started` to `launched`); the datom text and archive of each
  8.0.0 value are unchanged.
- **Report.** `Report.{ FlowId Event }` (`Event.[ Started ToolUsed.String
  Stopped ]`) is `Operation::Record(Record.Harness.{ FlowId Event })`, which
  appends the event to the flow's events in the Nexus's Memory (table
  `flow_nexus_flow_events`, one `FlowEvents.{ flow_id event_vector }` row per
  flow, appended under one hold so concurrent tool uses are not lost). A
  held flow answers `Reported`; a FlowId with no flow row answers
  `Refused.UnknownFlow.<FlowId>`, and no row is made for it. A store
  failure is answered the same way and logged: 9.0.0 names no persistence
  refusal for Report. Report does not wait behind a running launch.
- **ReadEvents.** meta `ReadEvents.FlowId` answers `EventsRead.{ FlowId
  [ events, oldest first ] }`, or `ReadEventsRejected.UnknownFlow`.
- **QueueTurnEnd** stays refused (`TurnEndRejected.QueueRefused`).
- **The harness hook.** A new executable, `flow-hook`, beside `flow`: it
  reads a Claude Code hook event on stdin and calls `flow` with
  `Report.{ «FLOW_ID» Started }` (SessionStart), `Report.{ «FLOW_ID»
  ToolUsed.«tool» }` (PostToolUse) or `Report.{ «FLOW_ID» Stopped }` (Stop),
  taking the FlowId from `FLOW_ID` in its environment; with none it sends
  nothing. It always exits 0. Every Claude flow Flow launches now gets it
  in its `--settings` flag settings, on those three events, at the
  `flow-hook` beside the running `flow-nexus`; and the pane preparation now
  unsets an inherited `FLOW_ID`, so a launched flow never reports as
  another. A launched flow does not yet have its own `FLOW_ID` in its
  harness environment (Flow claims the id after the harness starts), so
  until a launch carries it the hook reports nothing there.

# Flow 0.20.0

No wire, storage or deployment change; the Nexus's library surface changes.

- **The Operation root.** `crates/flow-nexus/ethos/operation.ethos` is
  generated by ethos-zero 16.0.0 (0edfc0c3, a new build-dependency) into
  `src/generated/operation.rs`; the build script fails when the committed
  module is stale. Every effect the launch, stop, retire and lifecycle-record
  paths perform is one `Operation`, performed through the new `Performs`
  trait on `RunningNexus` and answered by one `Outcome`.
- **Replaced.** `launching::ContinuesIntoBrief` is gone: the brief
  continuation is `Operation::Continue`, and its line is
  `Performs::BRIEF_CONTINUATION`. `PrunesLaunchBundles::prune_launch_bundle`
  is gone: pruning one launch's copy is `Operation::Prune`.
- `store::LaunchOutcome` and `store::Replacement` derive `Hash` and are
  re-exported at the crate root, where the Operation root imports them as
  `flow_nexus:[ LaunchOutcome Replacement ]`. Their archives are unchanged.

# Flow 0.19.0

Wire change, no storage change. Rebuild and restart the Nexus, `flow` and
`flow-meta` together; an older client and a 0.19.0 Nexus do not read each
other's frames.

- **Contracts.** signal-flow 8.0.0 (c297d987) and meta-signal-flow 12.0.0
  (69f9c146), generated by ethos-zero 16.0.0 and built on signal 7.0.0,
  where 0.18.0 pinned signal-flow 7.0.0 (1c9e4b30) and meta-signal-flow
  11.0.0 (2ac045c2). signal-flow 8.0.0 carries 7.1.0's vocabulary: the
  ordinary `Query` gains `QueueTurnEnd`, the `Response` gains `TurnEndQueued`
  and `TurnEndRejected`. Archives of `Query` and `Response` change; no
  stored record holds either, so the store opens unchanged.
- **QueueTurnEnd is refused.** Flow holds no turn-end queue, so it answers
  `TurnEndRejected.QueueRefused` and enqueues nothing.
- **One-line text.** protos and datom-codec are 0.32.2. The CLIs print
  replies, and the Nexus types a `Message` into a pane, through
  `Compactable::compact`; protos 0.32's `textualize` prints vertically.
- **Not adopted: the exchange layer.** signal 7.0.0's greeting and
  exchanges (`Contracted`, `Handshake`, `Dispatch`, `Delivery`) are not yet
  spoken on Flow's sockets; the frames are 0.18.0's `Signal` frames.
- sema-engine stays at 0.16.0 (516f01fe): the build does not require 0.18.0.

# Flow 0.18.0

Deploy beside Message 0.17.0, or do not deploy at all. No wire or storage
change; deployment and client behaviour change.

- **Configuration reaches the Nexus only over its meta socket.** The
  `FLOW_SOURCE_ROOT` and `FLOW_CODEX_{STABLE,NEXT}_{CLIENT,SOCKET,HOME,MODELS}`
  variables are no longer read. A store that adopted them earlier keeps the
  adopted values; a new store seeds its defaults from `HOME` and must be sent
  `Configure` for anything else. A next-slot Nexus started under
  `HOME=~/.local/state/flow-next` from a fresh store therefore needs a
  `Configure` carrying the real source root and Codex endpoints.
- **One home for the defaults.** The new `flow-defaults` crate derives the
  store, socket, source-root and Codex paths from `HOME` and
  `XDG_RUNTIME_DIR`; the Nexus seeds from it and the clients reach
  `$XDG_RUNTIME_DIR/flow/flow.sock` and `flow-meta.sock` through it, no
  longer the hard-coded `/run/user/1001/flow/`. `FLOW_SOCKET` and
  `FLOW_META_SOCKET` still name another Nexus's socket. `flow-meta
  register-codex` without an endpoint uses the default stable Codex control
  socket under `HOME`, not `/home/li/.codex`.
- **Claude daemon readiness follows Claude's home.** The jobs directory and
  roster are read under `CLAUDE_CONFIG_DIR` (else `$HOME/.claude`), not
  `/home/li/.claude`.
- **Traits first.** Every production method lives in a trait and `fn main()`
  is the only free function; the `no-free-functions` and
  `no-inherent-methods` checks hold both.

# Flow 0.17.4

Deploy beside Message 0.17.0, or do not deploy at all.

- **Plain direct Claude Start prompts are attested.** Claude Code may record
  the composer-selected direct Skill-tool prompt as a plain user row. Flow
  accepts it only when its exact server-composed text matches the persisted
  prompt SHA-256, then still requires every selected Skill call, successful
  result, and source expansion in order before the receipt.

# Flow 0.17.3

Deploy beside Message 0.17.0, or do not deploy at all.

- **Long Claude Start prompts are durable direct Skill-tool prompts.** A
  startup text that Claude Code will represent as pasted content no longer
  fails composition at 800 UTF-16 units. It names every selected skill for
  the Skill tool, and the observer accepts the native wrapper only after its
  unwrapped text matches the persisted prompt SHA-256 and every selected
  skill has been confirmed in order. The native launch intent, transcript
  boundary, and one-shot prompt-delivery intent remain unchanged.
- **Pasted-content parsing is exact.** `<pasted_content` must end at `>` or
  whitespace; near tags such as `<pasted_contention>` and
  `<pasted_content-id>` cannot be normalized as a Claude wrapper.

# Flow 0.17.2

Gate only: no wire, storage or behavior change (meta-signal-flow 11.0.0).
Deploy beside Message 0.17.0, or do not deploy at all.

- **`tests::a_process_in_a_pane_is_found_by_its_own_marks_or_its_ancestors`
  is deterministic.** It spawned its marked sleeper as `sh -c "exec sleep
  30"`, waited for that shell's Herdr marks, and then read them again after
  the shell had `exec`ed. `/proc/<pid>/environ` reads back empty for the
  width of an `execve`, so `caller_pane()` intermittently found no marks and
  walked to an ancestry that has none in a Nix builder: `None`. The sleeper
  is now spawned directly, so the process whose marks were settled never
  execs again. `caller_pane()` itself was not at fault and is unchanged.
- **The Claude retract path has a fixture test.** `Retraction::Key("ctrl+c")`
  was covered only by a live witness. Two fixture tests now drive a Claude
  composer — `❯` and the non-breaking space it renders after the glyph —
  through `esc esc`: a restored letter is emptied by one `ctrl+c` and the
  HardAbrupt lands; a person's draft is left alone and the delivery is
  refused. Both are named Nix checks, as is the deflaked test.

# Flow 0.17.1

A non-breaking fix on the same wire (meta-signal-flow 11.0.0); deploy beside
Message 0.17.0.

- **A letter an interrupt puts back is taken out, and the HardAbrupt lands.**
  In the e167d8 sandbox (fms-9a3e2b, fms-d70a61), a HardAbrupt's `esc esc`
  was pressed before Claude Code's first response to the Soft letter just
  Presented. Claude cancelled that turn and put the letter back into the
  composer. With vim editing, the second `esc` also left the composer in
  NORMAL mode. The transcript holds the letter as submitted. Flow then
  refused the HardAbrupt as `ComposerOccupied`. Message parked it, and the
  restored letter held the pane against every later letter. After an
  interrupt, a composer holding text that opens with a letter head is now
  emptied: one `ctrl+c` for Claude (it works in either vim mode and is
  pressed only onto held text), and line by line for Codex. The HardAbrupt
  is then typed. A person's draft is never touched.
- **Presented only when the letter is seen leaving the composer.** Herdr's
  `agent prompt --wait` answers `agent_prompted` on any lifecycle change, and
  it sends the submitting CR 300 ms after the text. Every placed letter is
  now read out of the composer (twelve reads, 350 ms apart). If it stays,
  the submit key is pressed once, and the letter is graded Transported,
  since no reaction to it was witnessed. If it still stays, it is taken back
  with `ctrl+u` and `backspace` per line, which interrupt nothing. The
  Deliver is then refused `ComposerOccupied`, which Message parks and
  retries under the same DeliveryId. A letter that can be neither submitted
  nor taken back settles `Uncertain`. The pane lease is let go on every path.

# Flow 0.17.0

A breaking release on meta-signal-flow 11.0.0 (2ac045c) that clears three
faults the e167d8 sandbox suite found in 0.16.0. Deploy with Message 0.17.0
(481b579): an older Message cannot decode the new refusals.

- **Start witnesses the receipt of a Claude model that takes no effort.**
  Claude Code 2.1.280 records no `effort` (and `perTurnEffort: null`) for
  Claude Haiku 4.5. The receipt check demanded the requested effort on the
  row, so a seat that answered exactly `FLOW_LAUNCH_RECEIPT_V2` left Start
  `StartAmbiguous`, the promoter never settled it, and the brief continuation
  was never typed. A row that names no effort now passes; a row that names a
  different one is still refused.
- **HardAbrupt and Command.Interrupt press again while the agent works.**
  Claude ignores `esc esc` pressed as its turn begins; the letter then queued
  behind the running command. The interrupt keys are pressed up to three
  times, each given 3 s to show the agent leaving Working, and only while it
  is still seen Working.
- **A gone flow is refused by who ended it.** Deliver and Command to an Exited
  flow answered `FlowStopped`; they now answer `FlowExited`, and a Retired
  flow `FlowRetired`. Stopped stays Flow's own act.

# Flow 0.16.0

A breaking release on meta-signal-flow 10.0.0 that clears the three faults
found in 0.15.0 before it was deployed.

- **A Letter names its MessageId.** The pane text was
  `Soft.{ Flow.e167d8 Text.«…» }`, which told the recipient everything except
  which message it was reading, so it could not `message 'Acknowledge.…'` at
  all. `Letter` gains `MessageId` as its first position and the pane text is
  now `Soft.{ m-7f3a2c Flow.e167d8 Text.«…» }` — one bare token wider. Flow
  never interprets the id; Message mints it and Flow types it. Every consumer
  must repin: `meta_signal_flow::Letter` gained a field, and signal-message
  7.0.0 imports `MessageId` from it rather than declaring a second one.
- **Presented no longer depends on the recipient's agent name.** A delivery
  seen reacting was re-checked against a fresh Herdr snapshot through
  `current_route`, which re-reads the agent's `name` — a label Herdr omits for
  panes that were never named, among them every pane imported with
  `MetaBindExisting`. Those deliveries settled `Uncertain` however plainly the
  recipient reacted. The grade now rests on the observation itself: Herdr's
  own `agent_prompted` reply, after it waited for the reaction, naming the
  pane and terminal the route names. The name was never the evidence.
- **The meta-gate tests are correct in a build sandbox.** Two of them spawned a
  marked process and read its `/proc/<pid>/environ` at once. glibc's
  `posix_spawn` wakes the vfork parent from inside the child's `execve`, before
  the kernel has laid the new environment into the new address space, so for a
  few dozen microseconds the environ reads back *empty* — indistinguishable
  from a scrubbed one, which makes a flow read as the owner. On a loaded
  machine the parent was descheduled past that window and the tests passed
  under plain cargo; in the Nix sandbox they failed every time. Fixture peers
  now wait for their own marks before they are used as peers. The Nexus needs
  no such wait: its peers have already connected to it.

# Flow 0.15.0

A breaking release on signal-flow 7.0.0 and meta-signal-flow 9.0.0: Flow is
the only pane writer (stage S1 of flows/e167d8/reports/message-through-flow-design.md).

- **Ordinary `Send` is removed.** Nothing on the ordinary socket types into a
  pane. `flow 'Send.…'` no longer parses; use
  `flow-meta 'Deliver.{ <id> <flow> MiddleAbrupt.{ Owner Text.«…» } }'`.
  signal-flow 7.0.0 renumbers the variants after the removed ones, so every
  consumer (today: message) must repin before it talks to Flow 0.15.
- **`Deliver`, `Vet`, `Command`, `ResolvePeer`** on the meta socket; see the
  README. The pane lease, the body refusal and the tier preconditions apply
  to every write, including the brief continuation.
- **`Observe.Agent`** on the ordinary socket streams a flow's Herdr agent
  state (Herdr `events.subscribe`), ending on `Gone`.
- **The meta socket is gated.** A flow outside `MetaAspects` (default
  `[ Psyche ]`) is answered `MetaRefused.PeerNotAuthorized`; the owner and
  the configured Message Nexus executable are admitted. Field and Mind seats
  that call `flow-meta` today will be refused.
- `Configuration` gains the harness profiles (command sigils, interrupt and
  submit keys), `MetaAspects` and `MessageNexusPath`. They live in a new
  store record seeded with defaults, so a 0.14 store opens unchanged. Two
  more new tables hold settled deliveries and lease rows.

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
- Start sets the canonical native title `<Aspect>.{ <Model> <FlowId> }`
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
