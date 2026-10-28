# Flow 0.40.0 (unreleased, branch flow-0.40)

Batch 12 removes the Simplified launch path and its reply binding. Every launch uses
one regular shape; a module header may place it in the system or user prompt, and
presentation instructions arrive through Curriculum context modules. The metaflow
declaration family advances to v6 and launch outcomes to v10. Pins signal-flow
38.0.0 and meta-signal-flow 52.0.0.

Batch 10 makes the Address the sole owner of a binding role. Caller roles carry
their exact Layer; PowerLevel and binding role claims are gone. Role and flow
families advance one version, upgrading High, Medium, Low and UltraLow to
Primary, Secondary, Tertiary and Quaternary. Fresh meta aspects are Psyche,
Mind and Field, and an unbound pane is admitted by process. Pins signal-flow
36.0.0 and meta-signal-flow 50.0.0.Batch 13 makes every harness installation configured data. The privileged
contract carries an arbitrary `Harnesses` family and Nexus v3 carries the
flows root, Flow executables and default Herdr session. A fresh store has no
harness; configured model service, executable/home/socket, pane profile and
composer data are read from the committed harness snapshot. The Nexus family
upgrades v2 to v3 and the new harness family is v1. Default Codex endpoints
leave Flow. Pins signal-flow 37.0.0 and meta-signal-flow 48.0.0. The timing
singleton advances from v1 to v2 with `stop_wait`; an archived v1 value
supplies its former `launch_rest_wait` for the new field. Both waits are
range-checked before configuration is committed.
The store migration reads frozen v8 launch outcomes with the corresponding
launch attempts in one typed context snapshot. A unique attempt supplies the
Address for `RequirementUnmet`; a missing or conflicting attempt moves the
exact old outcome archive into the durable quarantined-outcome family with a
typed reason. The staged store is activated only after every family opens.

Batch 9 makes every operational timing and size/count limit in the overnight
audit configured data. Fresh stores seed the `Timings` and `Limits` singleton
families from committed Datom files; existing stores gain those families with
the same seed values on open. `Configure.Timings` and `Configure.Limits` replace
their complete singleton values, and the effective configuration snapshot
carries both. The privileged wire advances to meta-signal-flow 44.0.0.

Batch 6 makes launch dependencies declaration data. `ConfigureMetaflow`
appends `Requires`; archived declarations upgrade with none. Before a new
launch, Flow starts every required Address not Awake and rejects with
`RequirementUnmet.Address` when one cannot start. The launch-outcome family
advances to v9; an archived `NoSecondary` becomes `RequirementUnmet` only
when its historical attempt uniquely supplies the Address;
otherwise it is kept whole in the durable quarantined-outcome family.

Batch 4 adds typed Line, Keys and Clear owner commands and ReadPane. Keys
carry the exact Herdr keys; they are admitted in a Blocked pane. Line and
Clear require Gate.Forced there. Flow pins meta-signal-flow 43.0.0.


Batch 3 adds `Gate.Kept` and `Gate.Forced` to privileged delivery, command,
and retire requests. Kept preserves threshold and continuity refusals; Forced
overrides them. `Vet` applies the same target, body, threshold, pane-state and
composer checks without typing or opening a delivery lease. Existing queued
delivery records upgrade as Kept.

Pins signal-flow 32.0.0 (e5e9d5) and meta-signal-flow 40.0.0 (f0f214), both
on their `flow-0.40` branches, and Curriculum 91acf29 on its `flow-0.40`
branch, whose schema it carries. Every ordinary and meta peer repins, and
Curriculum serves that schema.

- Pins meta-signal-flow 42.0.0. `ReadEvents`, `ReadSendOutcomes`, and
  `ListFlows` return bounded pages with continuation cursors. Owners can
  settle an `Uncertain` pane delivery as `Presented` or `NotPresented`, with
  their reason, through `ResolveDelivery`. Delivery Memory upgrades v1 to
  v2; all three earlier grades retain their exact meanings.
- Refresh threshold configuration names `RefreshTokens`; the threshold family advances
  from v1 to v2 preserving the numeric value. A successor's `Refresh` cause is unit.
- Store-only recovery repins meta-signal-flow 42 and adds owner repairs for
  queued work, metaflow state, launch settlement, successors, lifecycle,
  in-memory holds, context, quarantine, and extended rebind. These operations
  mutate Flow memory without acting on a harness.
- Pins meta-signal-flow 42.0.0. Pane deliveries are complete datoms:
  priority-headed `Letter.{ Kind Sender Body }`, with the sender Address and
  Flow ID resolved at ordinary Send admission. Logical-delivery current
  Memory upgrades v4 to v6. Older queued work remains whole without an
  invented sender identity: a typed `MissingHistoricalSender` disposition
  quarantines it from runnable delivery while later valid work retains its
  order. Queue-delivery requests advance to v6 to retain that disposition.
  The plain CLI message form requires address, priority, kind, and words.



- An abrupt request may use `Delivery.Queue`; it enters the recipient's
  durable queue and answers `SendAccepted` instead of
  `Failed.QueueNotPermitted`. Pins signal-flow 34.0.0. Send outcomes,
  send calls, and logical-delivery current/event tables each advance one
  version; a stored `QueueNotPermitted` reads as
  `NotSent.«queue not permitted»`.
- A Send is never refused for the seat's state or its route. A Wake Send to
  a seat with a queue, an outstanding delivery, a hold or a passed context
  threshold is queued and answers `SendAccepted.{ ... Queued }`; the route
  rule and `OffRoute` are gone, from the ordinary refusals and the Codex
  send tool's. Pins signal-flow 33.0.0 (ddf27d) and meta-signal-flow
  c58cef. The logical-delivery, send-outcome and send-call tables upgrade
  one version, with frozen signal-flow 32 shapes: a stored `OffRoute` reads
  as `Direct.«off route»` (the send tool's, as `Malformed`) and a stored
  `RecipientBusy` as `NotSent.«recipient busy { <address> }»`.
- Flow passes no tool restriction of its own: a seat declared `Regular
  Default` launches with no `--tools` and no `--disallowedTools` flag; a
  declared tool set is passed exactly as declared, a Simplified flow's
  included; Artifact tools are no longer disallowed and
  `CLAUDE_CODE_ARTIFACT` is no longer set. A Simplified flow is no longer
  refused for a tool set other than `Explicit.[]` and no longer gets
  `--strict-mcp-config` or `ENABLE_CLAUDEAI_MCP_SERVERS=false` of its own:
  both come only from configured leave-outs.
- `Restart.FlowId` is gone: `Refresh.{ Psyche core Primary }` ends the flow
  the metaflow is Awake as, without a turn, under the metaflow's lock; a
  Send arriving under the lock queues. It answers `Refreshed.{ Address
  FlowId }`, naming the flow that ended, or `RefreshRejected.[ NotAwake
  Locked CloseRefused PersistenceRefused HerdrUnavailable
  SuccessorRefused.ReplaceRejection Undeclared SuccessorUnconfirmed ]`. A
  Maintained metaflow, or one whose queue holds a waking request, has its
  successor started and confirmed before the ending flow is reaped, and the
  queue goes to it; any other is left Asleep. A Launch addressed to a
  metaflow Awake as a live flow is such a Refresh, its successor the
  Launch's, and answers `Replaced`.
- A Replace whose metaflow a refresh or a waking holds is refused
  `ReplaceRejected.Locked`. A refresh's or a replace's successor is owed in
  the store until it is confirmed, and a Nexus that starts with one owed
  starts it.
- A launch profile names its `Remembering` (`Predecessors.<depth>` or
  `Custom.[ FlowId … ]`) in place of its remembered-flow list; `Launch` is
  `Launch.{ Address Remembering InstructionPrompt }`. Stored launch
  attempts upgrade v3 to v4; an archived list becomes `Custom` of its flows.
- A metaflow declaration appends `Continuity.[ OnDemand Maintained ]`,
  `Purpose` and `Records`. Stored declarations upgrade v1, v2 and v3 to
  v4: { Psyche core Primary }, { Psyche core Secondary }, { Psyche core
  Tertiary }, { Psyche core Quaternary }, { Mind core Primary }, { Mind
  core Secondary }, { Mind core Tertiary }, { Field core Primary }, { Field
  core Secondary } and { Field core Tertiary } open Maintained, every other
  Address OnDemand, whatever a v3 row named; v1 and v2 rows open with no
  purpose and no records. Initializers supply the new positions.
- An OnDemand seat whose turn ends with nothing queued has its harness and
  pane closed and sleeps; a Maintained one keeps its pane and is always
  followed by a successor when refreshed or ended by an exit. The store
  gains `flow_nexus_successor_intents`.
- The meta socket answers `Inspect.Address` with the metaflow whole
  (`Inspected`: its state, Past and queue, the row of its current flow and
  the process bound to its Address, its sender locks, every outstanding
  attempt); `Rebind.{ Address Pane.HerdrPaneId }` records the pane its
  current flow lives in and `Rebind.{ Address Flow.FlowId }` makes that
  flow current, answered `Rebound.Address` or `RebindRejected.[
  UnknownAddress PaneAbsent FlowUnknown NotAwake HerdrUnavailable
  FlowTaken.Address ]`; `Withdraw.{ Address LogicalDeliveryId }` takes one
  request out of the queue, answered `Withdrawn.LogicalDeliveryId` or
  `WithdrawRejected.[ UnknownAddress NotQueued ]`, and its sender sees it
  end `DeliveryState.Withdrawn`. None touches a harness.
- Every stored family whose record shape changed opens under a new label,
  each older row carried into the current shape: launch outcomes v6 to v7
  (signal-flow 30 rejections; a `ContextRefusal` archived before
  `RecordMissing` keeps its meaning, in every older outcome too), flow
  events v3 to v4 (`Ended`, `Cleared`, and `Started` naming its session;
  an older start reads `Started.None`), successor intents v1 to v2
  (`Ordered`, `Replace`), logical deliveries' current and event rows v1 to
  v2, send outcomes v2 to v3 and send calls v4 to v5
  (`DeliveryState.Withdrawn`, `RetryCause.WakeRefused`,
  `Refusal.TopicRefused`, the send tool's `TopicRefused`).
- `Configure.Nexus` names the Curriculum socket in `CurriculumNexusPath`,
  after `MessageNexusBinary`; the Nexus composes every launch's context
  through that socket, read when it opens, and no longer derives it from
  its environment. The stored Nexus family upgrades v1 to v2: an archived
  declaration keeps every value and names
  `$XDG_RUNTIME_DIR/curriculum/curriculum.sock` of the process that opens
  the store, the socket Curriculum serves by default.
- `StartRejected.CompositionRefused` carries its reason
  (`CompositionRefusal`): a profile field that is not
  well formed names itself (`InvalidRemembering` for a `Custom.[]`), a
  model no harness launches exactly is `InexactModel`, a wake whose seat
  has no configured model `Unconfigured`, an unreadable presentation prompt
  `PresentationUnavailable`. A store-refused read of the flows a launch
  remembers answers `LaunchPersistenceRefused`. Launch outcomes upgrade v7
  to v8; a composition refusal archived without its reason reads
  `Unrecorded`, in every older outcome too.
- Locks held by a sender whose flow ends are retracted with the end.
- No pane outlives its flow: `Retire` and a recorded exit close the flow's
  pane where it is still there, as `Stop`, a refresh and a replacement's
  reap do; `Stop` also closes a pane that shows no agent. A Retire whose
  close Herdr refuses answers `RetireRejected.CloseRefused` and records
  nothing; an exit is recorded all the same.
- `flow-hook` reports Claude's SessionStart as
  `Started.Option<NativeSessionId>` and SessionEnd as
  `Ended.Option<NativeSessionId>`; a flow whose own session ended is
  recorded Exited. A SessionEnd with reason `clear` reports `Cleared`: the
  flow has not exited and its pane stays, and the Started after it rebinds
  the flow, its FlowId kept, to the new session.
- A new Primary whose declared Secondary is not Awake has that Secondary
  started first.
- A caller's Start, Replace or Launch with a topic that is not camelCase is
  refused `TopicRefused`, as is a Configure naming one, a Send to one
  (`Refused.TopicRefused`) and a Codex send tool call to one
  (`Refused.TopicRefused.«topic»`); a topic's capitals are never lowered. A
  new Primary needs its Secondary Awake (`NoSecondary`).
- A simplified Claude flow always starts with an explicit empty tool set.
- Curriculum is asked for the remembered flows' raw records; a remembered
  flow that logged none contributes nothing and refuses nothing, and a
  module's dependencies read as `type:subject` or `type-subject`, one ID.
  Deploy the Curriculum Flow pins with it.
- A wake whose start is refused leaves the queue as it was and marks each
  waiting request's delivery `Retryable.WakeRefused.<StartRejection>`, e.g.
  `Retryable.WakeRefused.ContextRefused.RecordMissing`, which the sender's
  `ObserveSends` shows; the next waking request retries the start.
- `Retire` of the flow a Maintained metaflow is Awake as answers
  `RetireRejected.Maintained` and changes nothing. Change that metaflow's
  declaration to `OnDemand` (`Configure.Metaflows`) before retiring it.

# Flow 0.39.1 (unreleased)

- A HardAbrupt reads the recipient's composer before it presses any key.
  A draft there refuses it as `ComposerOccupied` with no interrupt, so the
  recipient's turn goes on; formerly every queued HardAbrupt cancelled the
  recipient's newest turn and was refused all the same, and its next Send
  cancelled the next turn. A prompt the interrupt puts back into a composer
  that was blank is the cancelled turn's own: it is submitted again, never
  deleted, and the letter follows it. Formerly it held the composer, and
  every later letter to the pane was refused.
- Released alone on 0.39.0's contracts as 0.39.2, from the `release-0.39`
  branch.

# Flow 0.39.0

Pins signal-flow 28.0.0 and meta-signal-flow 37.0.0. Every ordinary and meta
peer repins: launch profiles carry FlowKind and ToolSet, metaflow
declarations carry both, and NativeStop carries an optional final answer.

- Declare a simplified Claude judgment with `Simplified Explicit.[]` after
  its system and user module lists. It starts idle with its own system
  prompt and an explicit empty tool set; the queued request is its first
  submitted turn. Its final answer returns as a Result to that request's
  sender, tied to the consumed native request rather than a later sender.
- Existing stored launch profiles and metaflow declarations upgrade to
  `Regular Default`. Event records retain their former data with no final
  answer attached. The reply binding and its completion are durable.
- Claude artifact tools are enabled only for `Psyche book Tertiary`.
- `Psyche core Primary` admits only its own Secondary. Its simplified
  presentation source goes unchanged to `Psyche book Tertiary`, with only
  a notification to Secondary; no duplicate generic result is sent.
- Declare `FLOW_CORE_PRESENTATION_PROMPT` in the service environment for
  core Primary's final literal authored role. Other addresses do not read
  it. Missing or empty content refuses the core presentation launch.
- Initializers must supply the new declaration positions. The managed
  initializer renders regular declarations as `Regular Default` and reads
  `:flow-kind :Simplified :tools []` for a tool-free judgment.

# Flow 0.38.1

Pins signal-flow 26.0.0 (10bc29be) and meta-signal-flow 33.0.0
(2e71b900). Every ordinary-socket peer repins because `Send` gains its
delivery position.

- **The sender chooses whether queued work wakes its recipient.**
  `flow 'Send.{ Address.{ Mind core Secondary } Wake Soft.Order.«...» }'`
  durably queues the request and schedules its recipient. `Queue` in the
  same position leaves the request queued. Priority still controls how a
  message is delivered to an already-live recipient; it does not decide
  whether a queued message wakes one.
- **A sender can deliver everything already queued.**
  `flow 'DeliverAll.{ Mind core Secondary }'` hands over the durable queue
  oldest first. It sends nothing when the queue is empty. `Delivered`
  means the queue is empty afterward; `Queued` means work remains held.
- **Fresh-flow startup contains no queued requests.** A wake composes the
  new flow from its modules with an empty instruction prompt. The durable
  queue remains in Flow and is handed over afterward through the ordinary
  delivery path, one sender-bearing datom at a time.

- **A Codex flow binds its seat from any of its own threads.** A command a
  Codex subagent runs carries the subagent's thread; BindSeat reads that
  thread's rollout, whose `session_meta` names the root thread of its
  session, and binds the seat running the root, under the root's FlowId
  claim. The seat is the one Herdr agent that reports the root as its
  session, or a Codex agent that reports none and whose pane's
  foreground process resumes it (`resume <thread>`, as Herdr's
  `pane process-info` lists the pane's processes). Two such agents, or
  none, leave the caller `Unidentified`.

# Flow 0.38.0

Pins signal-flow 25.0.0 (c67e561f) and meta-signal-flow 32.0.0 (c494af46);
flow-ethos `SentBy` is `[ Sender Unknown Flow ]`. Every peer that reads a
queued request repins. The store moves `flow-nexus-metaflow` to v4,
`flow-nexus-address-hold` to v4, `flow-nexus-queue-delivery-request` to v3
and `flow-nexus-queue-delivery-attempt` to v5 at the first open, each row
carried unchanged; a Flow before 0.38.0 refuses the store from then on.

- **A request Flow raised is sent by Flow.** A landing's orders, its
  result and its failures, and a compaction's investigation, are queued
  with `SentBy.Flow` and read `{ Flow Order.«…» }` in the seat's prompt.
  The landing's report and failures still go to Psyche core Secondary.
- **A seat knows its own FlowId from its first turn.** A Claude launch's
  system prompt ends with its launch section opening `Flow: <id>`, the
  FlowId Reserve claimed before the harness started. A Codex seat is
  given `Flow: <id>` as one developer item (`thread/inject_items`) after
  Bind claims it and before its first turn; the first USER message is
  unchanged for both.
- **`flow-store-check` judges a store before a deploy activates this
  Flow.** `flow-store-check '{ /home/li/.local/state/flow-03521/flow.sema
  }'` opens a private copy of the store as the Nexus would, upgrades
  included, and answers `Opens.<path>`, `Absent.<path>` or
  `Refused.{ <path> «reason» }`, exiting non-zero only on a refusal. The
  store itself is never opened, so a running Nexus is not disturbed.

# Flow 0.37.2

No contract, store or deployment change.

- **A Codex usage with no input is not a measurement.** Codex reports a
  `thread/tokenUsage/updated` whose last call has no input as its thread
  compacts; Flow recorded it as the flow's context, so the investigation of
  that compaction said it happened "at 0 tokens". Such a usage is now
  ignored, and the flow keeps its last real measurement.

# Flow 0.37.1

No contract, store or deployment change.

- **A Codex thread is resumable before its first turn.** codex
  0.161.0-alpha.2 writes a thread's rollout only at its first turn, and a
  TUI's `resume` of a thread with no rollout fails "invalid paginated
  history lineage ... missing source rollout", so Herdr's start timed out
  and every Codex start was refused. After naming the thread and reading
  the name back, Flow sends `thread/section/move` with a null section,
  which materializes the rollout with no turn; the TUI then resumes the
  idle thread.

# Flow 0.37.0

Pins signal-flow 24.0.0 (8796628) and meta-signal-flow 31.0.0 (a5ba4abe).
signal-flow only appends, so a Message built against signal-flow 23.0.0
keeps its wire; meta-signal-flow changes `ConfigureThreshold`, so every
meta-socket peer repins. The store gains
`flow_nexus_meta_metaflow_configuration`,
`flow_nexus_meta_model_threshold_configuration`,
`flow_nexus_meta_investigation_configuration` and `flow_nexus_compactions`
at the first open, and drops `flow_nexus_meta_threshold_configuration`.

- **A flow sends through Flow.** `flow 'Send.{ Address.{ Mind core Secondary }
  Soft.Order.«...» }'` queues a request for the metaflow, named by the
  calling process: the flow bound in its Herdr pane, or, for a command a
  Codex app-server started, the flow of the thread the app-server named in
  that command's environment (the SendServers family names the
  app-servers). `Up` names the layer above. A HardAbrupt or MiddleAbrupt
  message for a live Awake flow is typed into its pane under the lease;
  refused there, it is queued. A Send leaving the routes between aspects is
  `Refused.OffRoute`: across aspects only the same topic at the same layer,
  and Field reaches Psyche only after Psyche sent to it while this Nexus
  runs.
- **A flow is launched from its declaration.** `flow-meta
  'Configure.Metaflows.[ { { Psyche core Secondary } [ ... ] [ ... ] } ]'`
  declares each metaflow's system and user modules; `flow 'Launch.{ {
  Psyche core Secondary } «brief» }'` starts it at the model configured for
  its layer and aspect, at medium effort (low for an OpenAI Quaternary),
  with its default leave-outs; a Launch from the flow the Address is Awake as
  replaces it. An undeclared Address is `StartRejected.Undeclared`. A wake
  of a declared metaflow composes its declaration.
- **A seat binds with its real id.** `flow 'BindSeat.{ Mind tools Secondary
  }'`, run in a seat, reads the seat's harness and native session from
  Herdr, claims its FlowId through `flow-id` (the claim a launch makes; an
  existing claim answers its alias), records the flow live in its pane and
  the metaflow Awake as it: `Bound.47d946`. No synthetic `flow-…` id is
  minted for it.
- **A refresh threshold belongs to the model.** `Configure.Thresholds` holds
  one `{ ModelName Refresh }` per model:
  `Configure.Thresholds.[ { claude-opus-5-5 200000 } { gpt-6.1-sol 150000 } ]`.
  The layer-keyed `{ Layer Handover Refresh }` no longer reads, and its table
  `flow_nexus_meta_threshold_configuration` is dropped at the first open; a
  deployment declares the model thresholds again.
- **A flow past its model's threshold is refreshed.** It is told nothing and
  writes no handover. It is given no new message: a Drain hands it nothing,
  and a meta Deliver into it is refused `DeliveryRejected.PastThreshold`;
  what is queued stays for the metaflow's next flow. At its rest it is ended
  as a seat due a refresh is (0.36.0): its pane closed, the flow Stopped and
  its metaflow Asleep in the same event, and queued work wakes a fresh flow.
- **A Codex flow's context is measured.** Its app-server's
  `thread/tokenUsage/updated` (the last model call's input tokens) is
  recorded as a Claude flow's transcript reading is, so Compact and refresh
  judge both harnesses.
- **A start is one model call.** When the USER modules and the brief do not
  fit one message, the leading modules join the end of the system prompt
  until the rest fits; the system prompt travels in no argument and is no
  longer refused `SystemTooLarge`. A brief that alone does not fit one
  message refuses `ModuleTooLarge`.
- **A presentation is asked for.** Every first prompt, and queued work that
  carries the living's words (`Psyche`, `Psyches`), ends with: "End this
  turn with a presentation: a response that could fit into a book, taking
  its form from your belief modules and this prompt."
- **A compaction during continuous work is investigated.** A Claude
  `compact_boundary` with trigger `auto`, or a Codex `contextCompaction` no
  Compact asked for, is recorded once (`flow_nexus_compactions`), logged at
  warning priority, and given as an Order to the seat
  `Configure.Investigation.{ Psyche compaction Tertiary }` names: launched
  from its `Metaflows` declaration with it as its brief, or queued for it
  when it is Awake (sender `Unknown`). With none configured the warning is logged only.
  A transcript first read in a Nexus's run raises only compactions from ten
  minutes before the Nexus started on.

# Flow 0.36.0

The contracts are unchanged: signal-flow 23.0.0 (c7dcc074) and
meta-signal-flow 29.0.0 (2b03b506). The store gains two tables; the Start
datom gains a position.

- **The Start datom names the landing watch.** `Start` is
  `{ OrdinarySocketPath MetaSocketPath StorePath Option<LandingWatch> }`,
  with `LandingWatch.{ ContextRepository DeployBinary }`. Every caller writes
  the fourth position: `None` runs no approval pipeline (the configure-only
  start writes `None`), and
  `Some.{ /git/github.com/LiGoldragon/psyche-context /nix/store/…/bin/context-deploy }`
  watches that checkout's `main`. A three-position Start is refused
  `Arity { expected: 4, found: 3 }`.
- **The approval pipeline.** Every commit on the context repository's main
  that the context composer does not serve is a landing, recorded in
  `flow_nexus_landings`. Flow runs the deploy executable with
  `Deploy.{ Repository Commit }` for the newest landing and witnesses the
  deploy only by the composer serving a revision that includes it; then it
  marks every live flow whose launch modules (with their dependency closure)
  include a changed path due a refresh (`flow_nexus_refresh_due`), queues
  each changed module as an Order to Mind `<subject>` Secondary (else Mind
  core Secondary) carrying the commit and its changed lines, and queues a
  Result to Psyche core Secondary; a failed step is an Order to Psyche core
  Secondary, and is tried again at the next change of main or the next start.
- **A seat due a refresh is ended at its rest.** At its rest (Herdr shows a
  Claude seat Idle or Done; a Codex thread rests) its pane is closed under
  its lease, it is recorded Stopped and its metaflow Asleep, and work queued
  for the metaflow wakes a fresh flow composed from the deployed modules.
  Its queued work is never typed into it.
- The flow-nexus checks need `git` (the pipeline's tests read a real
  repository); the Nexus's own `git` comes from its PATH.

# Flow 0.35.25

The contracts are unchanged from 0.35.22: signal-flow 23.0.0 (c7dcc074) and
meta-signal-flow 29.0.0 (2b03b506). No store change.

- **A caller in Herdr's default session is named by the socket its server
  serves.** A process marked only with `HERDR_PANE_ID` is placed in the
  one listed session whose socket an ancestor listens on; the Herdr server
  is the process serving that socket, whatever binary it runs and
  whatever `herdr` `PATH` names.
- **A binding ends with its process.** A metaflow Awake as a process
  binding (an id that names no flow row, as `Bind` writes for a peer that
  runs in no flow Flow launched) whose process no longer runs is ended:
  when nothing is queued and no flow row was ever at the Address, the
  metaflow, its binding and any lock are forgotten, so `Current` answers
  `Unknown`; otherwise it is recorded Asleep with its work kept. `Current`
  applies this before it answers, and the Nexus applies it to every
  metaflow at its start.

# Flow 0.35.24

The contracts are unchanged from 0.35.22: signal-flow 23.0.0 (c7dcc074) and
meta-signal-flow 29.0.0 (2b03b506). No store change.

- **A Codex TUI attaches to an idle thread; the first turn starts after.**
  Herdr 0.8.2's `agent start` returns only once a Codex pane is at rest,
  which it is not while a turn runs, so a first turn longer than the start's
  30 s refused the spawn and archived the thread mid-turn. Spawn now opens
  the thread through the app-server, records it, claims its Flow ID and
  names it with its title (read back), then attaches the TUI with
  `resume <thread>`; Submit starts the first part as the thread's first
  turn after the exclusions are applied again and read back, as it does
  every later part. The naming is what stores the thread: codex
  0.161.0-alpha.2 refuses `thread/resume` of a thread with nothing written
  ("no rollout found"), and accepts it once the thread is named. A Codex
  start Herdr refuses archives a thread that had no turn.
- **A pane label is set when Herdr answers it with no output.** Herdr
  0.8.2 answers `pane report-metadata` with an empty reply; the Nexus
  reads its exit status instead of parsing JSON, so a Start no longer ends
  `NativeLaunchRefused.{ Naming PaneLabelRefused }`.

# Flow 0.35.23

The contracts are unchanged: signal-flow 23.0.0 (c7dcc074) and
meta-signal-flow 29.0.0 (2b03b506).

- **An idle flow keeps its pane.** A flow whose turn ends with nothing
  queued stays in its pane and its metaflow stays Awake, whatever its layer
  and harness. Work queued for an Awake seat known resting (a Claude
  transcript whose main turn ended, a Codex thread its app-server shows
  resting) is handed into its live pane at once; a seat in a turn takes it
  at that turn's end.
- **A metaflow is Asleep only once its pane is gone.** Work queued for an
  Awake seat whose pane Herdr shows gone (closed by the living, crashed,
  or replaced) records its flow Exited, which puts the metaflow Asleep, and
  the work wakes a fresh flow as before. No wake resumes a session.
- The Nexus's internal `Operation.Sleep` and `Outcome.Slept` are gone; no
  store family changes.

# Flow 0.35.22

Pins signal-flow 23.0.0 (c7dcc074, contract digest 32765a41a9454b06) and
meta-signal-flow 29.0.0 (2b03b506, contract digest 73ce9b26e5cd4583): every
ordinary- and meta-socket peer repins.

- **Every idle metaflow sleeps.** When a flow's turn ends with nothing
  queued, its metaflow sleeps whatever its layer and harness: a Primary as
  a Secondary, a Codex seat as a Claude seat. A Claude seat's turn end is
  its `Stopped` report; a Codex seat's is its app-server thread coming to
  rest. Sleep closes the pane, records the flow Stopped and the metaflow
  Asleep; the flow's row and native transcript stay.
- **Every wake is fresh.** An Order, a Question or the living's words queued
  for an Asleep metaflow of any layer start a new flow, composed from the
  configured layer model and the modules the metaflow's last launch named,
  on a new native session (Claude) or thread (Codex), its first prompt
  carrying the whole queue oldest first. A sleeping session is never
  resumed; a Result or a Notice still waits for a wake. `Refresh`,
  `Refreshed`, `RefreshRejected` and `RefreshRejection` are gone from the
  meta contract.
- **A topic never dies; it goes idle.** flow-ethos `State` is
  `[ Awake.FlowId Asleep ]`. `Query.End`, `Response.Ended`, `Current.Ended`,
  `Refusal.Ended`, `Refusal.OffRoute`, `StartRejection.AddressEnded` and the
  send tool's `RecipientEnded` are gone: `End.{ Psyche flow Primary }` no
  longer reads.
- **A Primary starts after its Secondary.** A Start whose Address is a
  Primary is refused `StartRejected.NoSecondary`, before anything is
  written, claimed, composed or opened, unless a metaflow is recorded, Awake
  or Asleep, at the Secondary of its aspect and topic.
- **Compaction is refused below the Refresh threshold.** `Command.Compact`
  is refused, before anything reaches the pane, unless the flow's context at
  its last Stop reached the Refresh threshold of its layer, the `Refresh` of
  that layer's `Configure.Thresholds` entry, still one per layer:
  `CommandRejected.CompactionRefused.BelowThreshold.{ 120000 200000 }`,
  `.Unmeasured`, `.NoThreshold`. The context size is still written at each
  Claude Stop; a Codex flow records none, so its Compact answers
  `Unmeasured`.

The store changes, carried in one open by the Nexus: the metaflow family
`flow-nexus-metaflow` v2 (and v1) to v3 and the Address-hold family
`flow-nexus-address-hold` v2 (and v1) to v3, an Ended metaflow carried
Asleep with its past and queue; the launch-outcome family
`flow-nexus-launch-outcome` v5 (and v4, v3, v2) to v6; the send-call family
`flow-nexus-send-call` v1 to v2. A kept launch outcome refused
`AddressEnded`, or a kept send call refused `RecipientEnded`, refuses the
open, naming its key: no current refusal says what it said. Such a row
exists only where a metaflow was once Ended. The same open drops the
fresh-wake marks, `flow_nexus_metaflow_refreshes`
(`flow-nexus-metaflow-refresh` v1), rows and registration; the context
sizes, `flow_nexus_flow_context_sizes`, stay.

# Flow 0.35.21

No contract, store or deployment change.

- **A title shows its topic exactly.** A title is
  `{ <Aspect> <Topic> <Layer> <FlowId> }` with the topic as the metaflow's
  Address holds it; the lowercase coercion is gone, so `flowNexus` and
  `PrimaryCommitting` are shown as written.
- **A Claude seat is titled before its agent runs.** Spawn knows the Claude
  Flow ID, so it sets the pane label, the pane's displayed agent and the
  workspace label to the title before the harness starts; Herdr never shows
  the internal `claude-<hex>` agent name. Herdr 0.8.2 accepts `--label` at
  `workspace create` and `pane report-metadata --display-agent` on a pane
  with no agent. A Codex seat is titled once its thread is named, as before.

# Flow 0.35.20

No contract, store or deployment change.

- **A Claude seat's Remote Control name is its title.** A Claude launch
  passes `--remote-control` the same title it passes `--name`,
  `{ <Aspect> <topic> <Layer> <FlowId> }`, so the Remote Control session list
  shows the flow's title where it showed `flow-` and sixteen hex digits. The
  Flow ID is known before the harness starts because Reserve claims it for
  the session id Flow chooses (`--session-id`). A Claude start without a
  reserved Flow ID has no title and is refused before Herdr is called.

# Flow 0.35.19

Pins signal-flow 22.0.0 (e6ccbddb, contract digest 5aa2992669c14992) and
meta-signal-flow 28.0.0 (efccfb5c, contract digest 809506754cfaf73d): every
ordinary- and meta-socket peer repins. Claude seats launched from this
release carry a UserPromptSubmit hook beside SessionStart, PostToolUse and
Stop.

The store changes, all carried in one open of a 0.35.11 store, by the Nexus
or the configuration-only listener, in this order: the model family
`flow-nexus-meta-model-configuration` v1 to v2, the leave-out family
`flow-nexus-meta-leave-out-configuration` v1 to v2, then the events family
`flow-nexus-flow-events` v1 to v2, each older `Stopped` read as
`Stopped.{ None None }`. The tables `flow_nexus_meta_send_server_configuration`
and the queue-delivery counter (v1), request (v2) and attempt (v4) families
are added holding none; Sema registers each and redb creates its table at
the first row written to it. The whole open lands at once: it runs on a copy
beside the store, which replaces the store by one rename only once every
family is carried. A leave-out default that cannot be carried, any other
failure, or a crash before the rename leaves the 0.35.11 store byte for
byte as it was, every family at v1, so 0.35.11 still opens it.

- **A Herdr workspace is labelled with its flow's title.** A started flow's
  workspace is created unlabelled and, once its Flow ID is claimed, takes the
  title `{ <Aspect> <topic> <Layer> <FlowId> }` beside the pane label; the
  pane's displayed agent is reported as the title, so Herdr never shows the
  internal agent name. Each surface is read back before any prompt. A
  workspace label Herdr refuses is
  `StartRejected.NativeLaunchRefused.{ Naming WorkspaceLabelRefused }`.
- **A launch names an exact native model.** A launch's receipt compares the
  model the native transcript records with the model the launch named,
  string for string, and Claude Code resolves an alias (`haiku`) to a full
  name (`claude-haiku-5-5`) without exposing the resolution. A Start,
  Replace or fresh wake whose model is not exact on its harness is now
  refused at composition, `StartRejected.CompositionRefused`, before any pane
  is opened or prompt typed: a Codex model is exact when a configured
  endpoint lists it; a Claude model is exact when it is `claude-` then
  lowercase letters, digits and hyphens. A fresh wake from such a layer Model
  leaves the metaflow Asleep with its work queued.
- **A model is configured per layer and aspect.** `Configure.Models` holds
  one entry per Layer and Aspect: `{ Secondary Psyche claude-opus-5-5 }`,
  `{ Secondary Mind gpt-6-sol }`. A declaration naming one layer and aspect
  twice is `ConfigureRejected.DuplicateEntry`.
- **A Start naming another model is refused.** A Start, or the successor of
  a Replace, whose model is not the one configured for its Address's layer
  and aspect, compared name for name, is
  `StartRejected.ModelRefused.Differs.<configured>`, or
  `StartRejected.ModelRefused.Unconfigured` when that layer and aspect has
  none (`ReplaceRejected.LaunchRefused.ModelRefused...` for a Replace). It is
  refused before the Nexus writes the store, claims a FlowId, composes, or
  calls Herdr; a Replace is refused before its replacement is recorded. A
  launch request already reserved resumes as it was.
- **A fresh wake takes its seat's model.** A fresh wake launches on the
  model configured for the metaflow's layer and aspect, and with none it
  opens nothing and leaves the metaflow Asleep with its work queued.
- **Store.** The model family moves from v1 (one row per Layer) to v2 (one
  row per Layer and Aspect) when the store opens, by the Nexus or the
  configuration-only listener. Every v1 row was a Claude model, and only
  Psyche runs on Claude, so each is carried whole as that layer's Psyche
  model and re-keyed under its Layer and Aspect in one commit; a crash
  between the upgrade and the re-key is finished at the next open. No Mind
  or Field row is made: until one is declared, every Mind and Field Start
  is `ModelRefused.Unconfigured` and a Mind or Field fresh wake opens
  nothing.
- **A Start takes the default of its own harness.** A Start naming no
  leave-outs takes its metaflow's default for the harness its model derives,
  and none when that harness has no default for its Address. A Codex Start
  of a metaflow with only a Claude default is no longer refused
  `LeaveOutRefused.NotForHarness`. A Start naming its own leave-outs is
  checked against its harness as before, at composition, before any launch
  is reserved or any pane opened.
- **A declared default belongs to its harness.** `Configure.LeaveOuts`
  holds one entry per Address and harness. An entry naming a leave-out of
  another harness than its own is refused `LeaveOutNotForHarness` and the
  whole declaration writes nothing. `recommended_plugins` stays outside the
  leave-out vocabulary.
- **Each stored default is carried under its harness.** Opening a store,
  the full Nexus or the configuration-only listener, carries each v1
  default, kept under its Address alone, whole under the harness every one
  of its leave-outs belongs to, and re-keys it by its Address and that
  harness. A default whose leave-outs belong to both harnesses, or that
  names none, refuses the open with
  `the leave-out default of <Aspect:len:topic:Layer> cannot be carried under
  one harness`, and the store is left unchanged.

- **A Codex thread's send server identifies the flow it serves.** On a shared
  app-server a Codex tool command runs as the app-server's child, in no pane,
  so `Identify` finds no flow. Codex stamps every MCP tool call's `_meta`
  with the calling thread's `threadId`; an MCP send server reads it, and
  Message, which reads that server's process from its socket, asks
  `IdentifyThread.{ Process NativeSessionId }`. Flow answers it only for the
  bound Message process, and only when the kernel reports the process running
  a configured `McpSendServerBinary` whose parent runs the paired
  `CodexAppServerBinary`: else `Refused.NotSendServer.Process`, or
  `Refused.NotConfigured` while no send server is configured. The thread then
  names `Identified.Address`, the metaflow Awake as the one live Codex flow
  whose thread Flow recorded at its Start; an unknown thread, or one whose
  metaflow is not Awake, is `Refused.Unidentified.Process`.
- **Configure the trusted pairs before Message asks.** Each path is the
  executable the kernel reports for the running process, never a wrapper
  script that execs it:
  `flow-meta 'Configure.SendServers.[ { /nix/store/…-codex/bin/codex /nix/store/…/bin/<send-server> } ]'`.
- **An Awake Codex seat’s queue drains on its app-server’s turn completion.** Each connection Flow keeps on a Codex thread reads the thread’s `turn/started`, `turn/completed` and `thread/status/changed`, and a responder’s `thread/resume` answers the thread’s status. When a confirmed seat’s thread rests with work queued, or work is queued for a seat whose thread rests, the oldest request is typed as a Stop would type it, and the next waits for a turn that begins after it. A completion read twice types once; one that arrives while the launch is unconfirmed is settled at confirmation. A seat its app-server shows in a turn is kept (`KeptAtStop::InTurn`) whatever Herdr’s screen shows.
- A Bind that leaves a seat Awake asks the same question a queued request does, so a resting Codex seat drains what waits for it.
- **A Claude launch receipt rebuilds the native paste element.** Claude Code records a pasted part with its native wrapper and exact newline framing. A recorded user turn matches only the exact part bytes or the exact rebuilt element under the turn’s identifier. A different identifier, newline count, or escaping is refused.
- **A resting Codex thread receives queued work through its app-server.** Flow records distinct request, attempt, and JSON-RPC identities before `turn/start`. The exact app-server response is recorded before the queued request is removed. A restart settles a receipt-recorded attempt without another native submission; a receipt-less attempt becomes Uncertain and blocks automatic redelivery until reconciled. The native call and local store are explicitly not one atomic transaction.
- **Turn completion remains the drain trigger.** Duplicate or unrelated events do not repeat delivery, completion during registration is retained until confirmation, and queued sender and order are preserved. Flow never falls back to a pane prompt for this Codex drain.
- **A Claude seat drains on its own turn end.** `flow-hook` reports a Stop
  with the `session_id` and `transcript_path` Claude Code gave it, and Flow
  acts on it only when they are the flow's registered session and that
  session's transcript. The transcript's main `end_turn` confirms it, so a
  seat whose main turn ended while background agents run (Herdr: Working) is
  handed its oldest request. It still sleeps only when Herdr shows it Idle or
  Done. A Stop of another session is recorded and acts on nothing.
- **A typed request settles on its exact receipt.** `flow-hook` reports
  UserPromptSubmit as the SHA-256 of the prompt Claude Code took, its paste
  wrapper taken off, with the session and prompt id. A hand-over settles when
  that digest is exactly its request in the flow's own session, or when the
  transcript after the stored cursor holds the one main user row or
  `queued_command` attachment that took it. A task notification is no
  receipt. The next request is handed over at the next turn end.
- **A Codex seat drains on its app-server's rest, not Herdr's.** Before
  `turn/start` Flow records the thread's newest turn as the attempt's anchor.
- **An Uncertain hand-over is reconciled, never resent.** At each turn end
  and at the Nexus's start Flow reads native history after the anchor. If the
  exact request is found it settles. If history proves it absent, the attempt
  ends Absent with that proof, and a new attempt is authorised. That proof is
  an idle Codex thread with no turn after the anchor carrying it, or a pane
  hand-over Herdr refused before input. Otherwise the attempt stays Uncertain
  and names why. Claude absence after typing is never proven.
  `ReadQueueDeliveries` lists every unsettled attempt with its standing.
- **A Claude turn end recorded before confirmation is honoured.** At
  confirmation the flow's last own Stop or prompt is read, or its transcript's
  main `end_turn`, as a Codex thread's rest already is.
- **The owner resolves a stuck hand-over.** A Claude hand-over whose absence
  cannot be proven stays Uncertain and blocks its address's queue. The owner
  names it by its attempt id: `Delivered` settles it and the request leaves
  the queue; `Absent` authorises a new attempt, which the next turn end hands
  over. Flow records the outcome, the requesting meta peer (its process and,
  when it runs in one, its flow), the reason and the time in the commit that
  settles or authorises. Only an Uncertain attempt that is still the
  address's oldest request resolves; a resolution is never automatic.
- **Deploying.** Declare the Models family whole, every aspect's model for
  every layer by its full native name, before the new Nexus serves Mind or
  Field:
  `Configure.Models.[ { Secondary Psyche claude-opus-5-5 } { Secondary Mind gpt-6-sol } { Secondary Field gpt-6-sol } ]`;
  an entry left out is removed. A declarer of `Configure.LeaveOuts` names
  each entry's harness:
  `Configure.LeaveOuts.[ { { Psyche flow Secondary } Claude [ SkillTool WorkflowTool ] } { { Mind core Primary } Codex [ CodexAgentsMd ] } ]`.
  Configure the trusted send-server pairs before Message asks
  `IdentifyThread`.

# Flow 0.35.11

No wire change. The store adds one table, `flow_nexus_launch_cleanups`; a
store written before opens with it added and holding none.

- **A Codex launch a Nexus left mid-way is reconciled at the next start.**
  Nothing replays such a launch, and its registration waits on a seat at
  rest that a first turn pending on a send call never reaches. Before it
  serves anything, the Nexus takes each unsettled Codex launch whose thread
  is recorded and whose phase is NativeLaunchIntentRecorded or NativeBound.
  It records the launch's cleanup (a thread step and a pane step, both
  Pending), then settles it `NativeLaunchRefused.{ Spawn Unclassified }`.
  The thread step answers the calls the app-server replays on the thread:
  one the earlier Nexus committed is answered from its record, its one
  request left queued from the launch's Address and its target due the wake
  `recover_wakes` takes; any other is refused (`ThreadUnowned`) and nothing
  is queued. It then archives the thread and is Done only once the archive
  succeeded. The pane step takes down its pane and any flow it registered
  and is Done only once nothing of it is left running. A step still Pending
  is taken again at the next start. A launch past its acknowledgement, or
  settled with its cleanup complete, is left alone and is never a sender
  here.
- **A pane is closed only when Herdr shows it the Flow's own.** Every close
  requires the agent at the recorded pane and terminal to report the
  recorded native session, or, for a Codex agent reporting none, a
  foreground process of the pane running `resume <thread>` for the recorded
  thread; a pane with no agent is closed too. A pane running another
  session, one nothing proves, or a Flow with no recorded session is left
  open, and a refused launch's pane so left stays recorded.

# Flow 0.35.10

No wire change and no store change: signal-flow 20.0.0 and meta-signal-flow
24.0.1 stay pinned; no table or family changes (the opened-thread row is
written earlier and also read by thread).

- **A Codex launch serves its first turn's send call.** Herdr's `agent
  start` returns only once the agent is idle or done and interactively
  ready, and a first turn waiting on a send call is neither until the call is
  answered. A Codex launch now records the thread it opened as its own before
  starting the first turn, and keeps the connection that started the thread,
  which is subscribed to its server requests (a `thread/resume` before the
  first turn is refused by the app-server). While it waits for Herdr's start
  and for the bind's rest wait (holding the dispatch gate, as every launch
  does), it reads that connection and answers each call as the gate's
  holder; the connection is closed when the launch ends, whatever it came
  to. When the record fails, no first turn is started and the thread is
  archived.
- **A call before the flow is bound is sent from the launching metaflow.**
  A call's sender is the metaflow Awake as the thread's one live flow, as
  before; failing that, while the Codex launch that opened the thread is
  unsettled and no other flow or launch holds its Address, the launch's
  Address. A thread no flow holds and no unsettled launch opened is
  ThreadUnowned, a refused launch's included.
- Targets a launch's calls queued work for are woken once the launch has
  given the gate back.

# Flow 0.35.8

No wire change and no store change: signal-flow 20.0.0 and meta-signal-flow
24.0.1 stay pinned, no table or family changes.

- **A queued request wakes its Asleep target whether or not the answer is
  written.** A metaflow is due a wake when it is Asleep, not a Primary, and
  holds an Order, a Question or the living's words (or its last flow is still
  live). That is read from the store after every commit that queues a
  request, a send-tool call's or a Deliver's, and after a replayed send call
  is answered from its record; the wake runs after the answer is attempted,
  never only after it succeeded. The Nexus's start wakes every metaflow so
  due. A second ask while a wake runs, or once the target is Awake, wakes
  nothing more.
- A Deliver's connection is closed before the recipient's wake runs.
- The internal `SendAnswer` type is removed: handling a send call yields its
  `SendOutcome`.

# Flow 0.35.7

Wire change on both sockets. Ordinary: signal-flow 20.0.0 (6a496646); its
contract digest changes from baa84beb4f2cc84b to 5bf78e23e2f14c52, so every
ordinary-socket peer (Message, signal-message) repins. Meta:
meta-signal-flow 24.0.1 (a918b7db) only repins signal-flow 20.0.0; its source
and digest 7bbc37359da8843c are unchanged, but a meta-wire peer rebuilds
against it so the shared types have one identity. flow-ethos (this
repository, 9e417bb) changes `Metaflow`'s queue. The store upgrades two
families and adds one table.

- **A queued request carries its sender.** A metaflow's queue holds
  `QueuedRequest.{ SentBy Request }`, `SentBy.[ Sender Unknown ]`. A Deliver
  queues with its Lock's Sender, a send-tool call with the metaflow it is
  attributed to. The metaflows family upgrades v1 to v2, and the Address
  holds family, which keeps a metaflow as found, v1 to v2; every stored
  queued request reads `SentBy Unknown`, every other value as it was. A
  woken or drained flow's prompt shows each request with its sender, one
  datom per line under "Work queued for this metaflow while no flow of it was
  running, oldest first, each with who sent it:", e.g.
  `{ Sender.{ Mind alpha Tertiary } Order.«Carry on.» }` or
  `{ Unknown Notice.kept }`. `Drained` carries `QueuedRequest`.
- **Every Codex thread Flow opens declares the send tool.** `thread/start`
  carries `dynamicTools: [ send ]` on a connection initialized with
  `capabilities.experimentalApi`. Before that connection is opened the
  app-server is probed: a `thread/start` declaring the tool on a connection
  without the experimental API must be refused with -32600 naming
  `dynamicTools` and `experimentalApi`. Any other answer refuses the launch
  `NativeLaunchRefused.{ CodexThread SendToolUnsupported }` (appended last to
  `NativeLaunchFailure`, so stored outcomes read as they were), and a thread
  the probe started is archived at once. No thread is opened without the
  tool. Each Codex launch makes one more app-server connection, the probe.
- **The Nexus answers send-tool calls.** One connection per live Codex flow's
  thread, resumed at start, on each store change that brings a thread, and
  after each reconnect, answers `item/tool/call`: the sender is the metaflow
  Awake as the thread's one live flow, read at handling time; the request is
  queued under the Lock and Deliver rules; the answer is
  `Queued.{ Sender Target }` or `Refused.SendRefusal` as a datom.
- **`flow_nexus_send_calls`** (family `flow-nexus-send-call`, schema
  `flow-nexus-send-call-v1`) records each handled call, keyed by thread, turn
  and call id, with its content digest and outcome; a queued call's row is
  written in the commit that queues it. A store written before 0.35.7 opens
  with the table added and empty.
- A flow that ends or resumes announces a launch change, so watchers re-read.
- The Codex adapter's unused `start_codex`, `start_codex_observed` and
  `resume_codex` paths are removed: they opened threads without the tool.
- A server request whose id equals a pending client request's id is no
  longer taken for its answer; the WebSocket reader answers pings and reads an
  oversized frame through before refusing it.

# Flow 0.35.6

Wire change on both sockets. Ordinary: signal-flow 19.0.0 (37a41014); its
contract digest changes from c573701bba34c200 to baa84beb4f2cc84b, so every
ordinary-socket peer (Message, signal-message) repins it before it can greet
this Flow. Meta: meta-signal-flow 24.0.0 (a0f8d075, pins signal-flow 19.0.0);
its digest changes from 371d348ad5b3fbad to 7bbc37359da8843c, so every
meta-wire peer repins it. The store upgrades its launch outcomes and adds one
table.

- **A refused registration names its stage on the wire.**
  `StartRejected.RegistrationRefused.RegistrationRefusal`: `RouteUnverified`
  (the Herdr route was not verified under the FlowId claim), `RoleConflict`,
  `RoleUnrecorded`, then the stages `accept_registration` names:
  `PaneBindingForeign`, `AcknowledgementMismatch`, `PromptNotCanonical`,
  `NativeTarget.[ HerdrUnreadable PaneMismatch NoNativeSession
  IdentityInexact ]`, `TranscriptBoundary`, `ThreadNotNamed`.
- **A refused native step names its stage and category on the wire.**
  `StartRejected.NativeLaunchRefused.{ NativeLaunchStage NativeLaunchFailure }`,
  stage `Open`, `CodexThread`, `Spawn` (a start Flow did not attempt),
  `Bind`, `Naming`, `Receipt` or `Confirm`; the category is the fixed one the
  Nexus log already printed, no adapter text. The receipt's own:
  `BoundaryMismatch`, `ThreadNotNamed`, `PartDiffers`, `LeaveOutShown`,
  `ModelOrEffortDiffers`; the confirmation's `SessionUnrecorded`. The
  operation outcome is `Failed.LaunchRefused.NativeLaunchRefusal` for Open, a
  Codex thread's opening, an unattempted Spawn, Bind and Title. LaunchStatus
  and a `ReplaceRejected.LaunchRefused` carry the same value.
- **The launch-outcome family upgrades v4 (and v3, v2) to v5.** A stored
  unit `NativeLaunchRefused` reads `{ Unclassified Unclassified }`, a stored
  `RegistrationRefused` reads `RegistrationRefused.Unclassified`; every other
  outcome reads as it was.
- **A Codex launch recovered after its start binds to its thread.** Spawn
  records the thread it opened in `flow_nexus_launch_opened_threads` (keyed by
  launch request) before the TUI attaches; a Start that resumes a Codex
  attempt left at its native intent binds through the recorded thread, the
  recognized pane and the claim, as Bind does. Without a recorded thread it
  is refused `NativeLaunchRefused.{ Bind NativeSessionMissing }`.
- **The wake pane check is Codex-aware.** `holds_native_session` takes the
  flow: a pane whose agent reports the flow's session holds it; for Codex,
  the pane Flow recorded for the flow, in that Herdr session, with Herdr
  recognizing a Codex agent in it and no session reported, holds the opened
  thread; a reported session must equal the thread. A wake of such a flow is
  refused `ConflictingBinding` rather than starting a second seat.
- **The live-seat check is typed.** `own_session` answers `Runs`,
  `NoRecordedPane`, `Unclaimed`, `HerdrUnreadable`, `PaneGone` (the recorded
  pane is gone from Herdr), `OtherSession` or `SessionUnreported` (a Claude
  pane reporting none); a wake adopts a live last flow only on `Runs` and
  logs the reason otherwise. A Codex pane reporting no session runs its
  thread.
- **MetaBindExisting verifies each binding with Herdr.** The pane must be in
  the container's Herdr session with the binding's harness at its workspace,
  pane and terminal (`PaneAbsent`); `herdr pane process-info --pane` must list
  the binding's process in the pane's foreground (`ProcessMismatch`); a session
  Herdr reports must be the binding's (`SessionMismatch`); a Claude pane must
  report one (`SessionUnreported`); a Codex pane reporting none is proven only
  by a rollout whose first record is the `session_meta` naming the session
  (`SessionUnproven`); an unreadable snapshot or process list is
  `HerdrUnreadable`. A refused binding stores nothing.
- **A Codex TUI's terminal title is the thread title alone.** The TUI starts
  with `-c tui.terminal_title=["thread-title"]`; codex-tui 0.161.0-alpha.2
  names its title items in `[tui].terminal_title` (`project-name`,
  `current-dir`, `run-state`, `thread-title`, ...), and its default appended
  the project name (" | primary"). The title check for Codex reads the thread
  name and the exact Herdr pane label, not the terminal title.
- A Start applied to the store alone answers `LaunchPersistenceRefused`.

# Flow 0.35.5

No protocol change; no wire, contract or store change.

- **A Claude launch receipt reads the native paste element.** Claude Code
  2.1.294 records a long submitted message as one user turn of exactly
  `"\n\n<pasted_content id=\"ID\">\n" + part + "\n</pasted_content id=\"ID\">\n"`,
  the part unescaped and the closing tag repeating the opening `id`. A
  recorded user turn matches a part when it is exactly the part's bytes, or
  exactly that element holding exactly the part's bytes. Nothing is trimmed;
  any other wrapper, extra text, a differing closing `id`, a second element or
  other content does not match. The 0.35.4 rule expected a different wrapper
  form and refused the real one (`StartRejected.NativeLaunchRefused`).
- **A Codex registration names its stage and needs no Herdr session.** A
  remote-resumed Codex TUI reports no `agent_session`. The Codex target is
  the thread Flow opened plus the recognized pane (agent, workspace, pane and
  terminal exact); a session Herdr does report must equal that thread
  (`IdentityInexact` otherwise); Claude must still report one. The transcript
  is the authority for the thread: the rollout's first record must be the
  `session_meta` whose `payload.id` is the opened thread, at registration and
  again in the receipt read, and the receipt still matches the first user
  turn's part hashes in that same transcript. `runs_own_session` accepts a
  Codex pane reporting none. `accept_registration` refuses with a typed
  `RegistrationRefusal` (pane binding, acknowledgement, prompt, native target,
  transcript boundary, thread not named) that Nexus logs on stderr; the wire
  answer is still `StartRejected.RegistrationRefused` because signal-flow
  18.0.0 carries no reason there.

# Flow 0.35.4

Wire change on the meta socket only: meta-signal-flow 23.0.0 (dacb3fdd)
makes `Configure` declare one configuration family whole and removes
`Reconfigure`; its contract digest changes from 7d19286cad9199bd to
371d348ad5b3fbad, so every meta-wire peer repins it before it can greet this
Flow. The ordinary socket is unchanged (signal-flow 18.0.0, 59aab725). The
store's tables are unchanged.

- **Each configuration family is its last declaration.**
  `Configure.Models.[ … ]`, `Configure.Thresholds.[ … ]`,
  `Configure.LeaveOuts.[ … ]` and `Configure.Nexus.{ … }` replace that
  family's committed value in one commit; an entry the declaration leaves out
  is removed. The answer is `Configured.Replaced`, or `Configured.Unchanged`
  when nothing differs. `ConfigureRejected.DuplicateEntry` (one Layer or
  Address named twice) and `ConfigureRejected.CodexModelsOverlap` change
  nothing. A declarer sends one `Configure.LeaveOuts` holding every
  metaflow's default leave-outs instead of one `Configure.LeaveOut` per
  metaflow, and treats both `Configured` answers as success; a changed
  default no longer answers `Conflict`. `Reconfigure` is gone:
  `Configure.Nexus` sets or replaces the Nexus, on the configure-only
  listener and on the running Nexus alike.
- **A Claude session is named at launch.** A reserved Claude start passes
  `--name <title>`; `/rename` is a slash command, which a launch leaving out
  the Skill tool (`--disable-slash-commands`) cannot run. The title is read
  back from the terminal title Herdr reports, and the latest transcript title
  record, once written, must equal it. Nothing is typed to title a pane. The
  `rename-refused` title category is gone.
- **A remote-resumed Codex TUI binds to the thread Flow opened.** Herdr
  reports no agent session for it, and the app-server names no client
  attached to a thread, so the pane binds to that thread once Herdr
  recognizes the pane's Codex agent, exact and interactively ready; a session
  Herdr does report must be the thread.

# Flow 0.35.3

Wire change on the ordinary socket: signal-flow 18.0.0 (59aab725) appends
`StartRejection.HarnessSpawnRefused.HarnessSpawnRefusal`, so the ordinary
contract digest changes and every ordinary-socket peer repins it. The meta
socket is unchanged: meta-signal-flow 22.0.1 (5d1ac92f) only repins
signal-flow, and its digest is the same. Store change: the launch-outcome
family upgrades from v3 to v4 (and from v2 to v4) on the first open.

- **A Claude system prompt is never typed.** The composed system text is
  written into an in-memory file (memfd) the Nexus holds, and Claude is
  started with `--system-prompt-file /proc/<nexus pid>/fd/<n>`. The file is
  closed once the session's binding has been observed, or at once when the
  start is refused. Herdr 0.8.2 refuses any `agent start` argument holding a
  control character, so the multiline `--system-prompt` argument Flow 0.35.2
  passed was refused before the pane was touched.
- **A Codex TUI is not given the system prompt.** The thread already holds it
  as its base instructions (given at `thread/start`, given again after the
  TUI attaches), and a TUI remote resume keeps them. The `-c instructions=`
  argument made a typed command of tens of kilobytes; Herdr types the command
  into the pane's shell, a line over about 16 KiB arrived cut short, the
  shell waited at its continuation prompt, and Herdr's readiness wait timed
  out.
- **Flow refuses a command no shell line holds before calling Herdr.** An
  argument holding a control character, or a typed command longer than 4095
  bytes as Herdr quotes it, refuses the start as
  `HarnessSpawnRefused.{ ControlCharacter … }` or `{ CommandTooLong … }`.
- **A refused start keeps Herdr's own words.** `HarnessSpawnRefused` carries
  `Herdr.{ Option<code> message }`: the code and message of Herdr's JSON
  error body, or no code and its whole printed text. LaunchStatus answers the
  same. `NativeLaunchRefused` now names only a failure after the harness
  started.
- **A Codex thread opened before a refused start is accounted for.** The
  refusal carries `Some.{ thread FirstTurn ThreadArchive }`: the first turn
  read once through `thread/turns/list` (`Answered`, `InProgress`,
  `Unanswered`, `Unread`), then the thread archived with `thread/archive`
  (`Archived` or `ArchiveRefused`).
- **`CodexDeveloperInstructions` also removes the multi-agent messages.** The
  leave-out sets `features.multi_agent_v2.root_agent_usage_hint_text` and
  `features.multi_agent_v2.multi_agent_mode_hint_text` to `""` at
  `thread/start`, `thread/resume` and on the TUI, which removes the
  `<multi_agent_role>` and `<multi_agent_mode>` developer messages, and the
  Codex receipt refuses a request carrying either while it is left out. The
  `<recommended_plugins>` message has no witnessed per-thread removal and is
  not part of the leave-out.

# Flow 0.35.2

Wire change on the meta socket only: meta-signal-flow 22.0.0 (4ccf6f0d)
adds `Refresh.Address`, answered `Refreshed.Address` or
`RefreshRejected.[ Unknown Awake Ended NoLaunchProfile StoreRefused ]`. The
ordinary socket is unchanged (signal-flow 17.0.0, b619ea08). The new
variants change meta-signal-flow's contract digest, so every meta-wire peer
must repin it (Message pins e9ec03d) before it can greet this Flow.

- **A Refresh makes one metaflow's next wake fresh.** `flow-meta
  'Refresh.{ Mind flow Secondary }'` is accepted only for an Asleep
  metaflow whose last flow has a recorded launch profile; the wake then
  composes a new native session from that launch's module identities and
  leave-outs instead of resuming, whatever the context size. Models,
  thresholds, the queue and the past are untouched. A repeat while marked
  answers the same `Refreshed`. The queued requests are delivered once, by
  the fresh wake.
- **The mark is a table of its own.** `flow_nexus_metaflow_refreshes`, one
  row per Address, written by `Record.Refresh`; `Record.Awoken` retracts it
  in the write that records the wake, so it governs one wake. A store from
  before opens with the table added and holds no marks.

# Flow 0.35.1

No wire change. Store change: the launch-attempt family upgrades from v1 to
v2 on the first open, so a store Flow 0.34 wrote opens with every row.

- **A 0.34 launch attempt is carried, not set aside.** Each row reads in
  0.34's shape and is written in the current one: what the current shape
  names goes into it (its profile names no module and leaves nothing out,
  its native intent names no system-prompt hash, its one first message is
  its one part, and its Stop count reads 0, which 0.34 did not keep); what
  it named instead is kept beside it as `LaunchContext.BeforeModules`: the
  skill sources, the skill names, the power, the harness, the bundle file,
  the origin clue whole, the native launch intent's skill names, and the
  native skill selections of its delivery intent and receipt. No later seat
  is composed from such a row: a metaflow whose last flow was launched by
  one is not woken from it (its wake is Unplaceable) until a Start names
  its modules. A row older than 0.34's shape is still moved
  whole into the quarantine table. The Flow 0.34.4 production store was
  opened in a disposable copy with this code: every row of every table read,
  and every table held as many rows after as before.
- **A Codex thread is opened lean and given its first turn before its TUI
  attaches.** The Nexus opens the thread through the app-server
  (`thread/start` with the composed system prompt as `baseInstructions` and
  the launch's exclusions in `config`), checks the answer (thread, model,
  reasoning effort, and no `instructionSources` when `CodexAgentsMd` is left
  out), submits the first part as the thread's first turn, and only then
  starts the TUI in the pane as `codex --remote … resume <thread>`. Bind
  requires the session Herdr reports to be that thread.
- **After the TUI attaches, the exclusions are applied again and read
  back.** Before a part is counted delivered, `thread/resume` sends the base
  instructions, the exclusions, the effort and the flow's `FLOW_ID` and
  `FLOW_DIRECTORY` again, and the same readback refuses a differing answer.
- **A Codex receipt refuses a request that carried a left-out block.** The
  receipt reads a Codex thread from its start; an AGENTS.md body, a skill
  listing, permissions, collaboration-mode or environment-context block in a
  request, while its leave-out is in force, refuses the launch.
- `packages.store-proof` is the flow-nexus library test executable, so the
  ignored `a_0_34_4_store_opens_and_every_row_of_every_table_reads` runs by
  name with `FLOW_0_34_STORE` naming a store copy.

# Flow 0.35.0

Wire change: signal-flow 17.0.0, meta-signal-flow 20.0.0. Store change:
launch attempts written before 0.35.0 are set aside as `BeforeModules`; the
flows family upgrades from v5 to v6 and the launch outcomes from v2 to v3,
each carrying its origin as derived; the `flow_nexus_meta_module_configuration`
and `flow_nexus_start_bundles` tables are dropped, rows and registrations, on
the first open.

- **A Start names its context by module identity, and nothing else.**
  `LaunchProfile` carries `SystemModules` and `UserModules`; there is no
  source path, skill name or system-prompt bundle file. A Start datom reads
  `Start.{ id [ spirit-core intent-flow ] [ operation-main-flow ] { Field
  flow Quaternary } haiku medium None [] session None «brief» }`. The `flow`
  CLI takes that one datom; its `--system-modules` flag and its Curriculum
  `ResolveSkills` call are gone.
- **A Start names no origin, power or harness; the Nexus derives them.**
  `Start.LaunchProfile`, `Replace.LaunchProfile`, `Restart.FlowId`. The
  origin is read from the calling process as Identify reads a caller: the
  one flow bound in its Herdr pane, with that flow's native session, is
  `Origin.Flow.{ FlowId SessionId }`; any other caller is `Origin.Unbound`,
  recorded and not refused. `FlowNode`, `Launched` and `LaunchAttempt`
  carry `Origin`. A flow's power is its Address layer's (Primary High,
  Secondary Medium, Tertiary Low, Quaternary UltraLow). Its harness is
  Codex when a configured Codex endpoint serves its model, else Claude;
  `ComposedLaunch` carries that `HarnessKind`.
- **Flow has Curriculum compose the context.** At Start (and at a fresh
  wake and a resume) the Nexus sends `ComposeStartup.{ CallerMetaflow Target
  SystemModules UserModules }` to the Curriculum Nexus at
  `$XDG_RUNTIME_DIR/curriculum/curriculum.sock` (schema
  `crates/flow-nexus/ethos/curriculum.ethos`, Curriculum main
  691852aa), and verifies the answer before any of it reaches a prompt: both
  payload SHA-256s, every requested identity present at its placement, and
  each text exactly the rendering of its modules, each module's slice at its
  rendered SHA-256. A refusal answers `StartRejected.ContextRefused.<why>`.
  Nothing composed is written to disk: the per-launch bundle copies and
  their directory are gone, and so is the Prune operation.
- **The system prompt travels as one argument.** Claude gets
  `--system-prompt <text>`; Codex gets `-c instructions=<text>`. A system
  prompt past 131071 bytes refuses the Start as `SystemTooLarge`.
- **The first USER message is the modules, then the brief, once.** No
  slash command, no Skill-tool instruction, no Codex typed skill item. A
  Claude part is typed through Herdr's socket API (`agent.prompt`, one JSON
  request line of at most 1 MiB), never as a process argument; a Codex part
  is one text item of `turn/start` (at most 1048576 characters), sent in one
  WebSocket frame whose length may take the 64-bit form.
- **A message too large for one part goes in parts.** It is split where
  modules begin; each part opens `Startup message, part k of n.` and every
  part but the last tells the flow to wait for the last; the brief is in the
  last part. The next part is sent once the flow answered the one before
  (Claude: a Stop reported after it and Herdr showing the pane at rest;
  Codex: the turn completed), recorded submitted before it is typed, never
  typed twice. A module that cannot fit one part refuses as
  `ModuleTooLarge`.
- **The receipt is every part, then the first answer.** Each part is seen
  in order as a user turn (Claude's pasted-content wrapper is taken off),
  then the flow's first answer at the intended model and effort. The
  durable intent holds every part's SHA-256 and how many were submitted.
- **Started names the pane.** `Launched` carries the flow's
  `HerdrRouteSelection` (session, agent, pane, terminal), read from the
  flow's row when the reply is made; the outcome row keeps its shape.
- **A fresh wake composes the last launch's identities.** A wake that does
  not resume launches with the module identities and leave-outs the
  metaflow's last launch named, its system prompt composed again; a
  metaflow with no launch of record is `Unplaceable`. 0.34.6's kept Start
  bundle and `Failed(StartBundleMissing)` are gone. `Configure.Module` is
  gone from the meta socket.
- **A Start can leave parts of its harness start out.** `LaunchProfile`
  carries `Option<LeaveOuts>`; `Configure.LeaveOut.{ Address LeaveOuts }`
  names a metaflow's default, which a Start naming its own replaces. Each
  `LeaveOut` becomes the harness's own switch: Claude `--disallowedTools`
  (Agent, Read/Edit/Write, Workflow, ScheduleWakeup, ReportFindings,
  ListAgents, WebFetch/WebSearch, the deferred tools, Skill),
  `--disable-slash-commands`, `--strict-mcp-config`, `--setting-sources
  project`, an exported `ENABLE_CLAUDEAI_MCP_SERVERS=false` or
  `CLAUDE_CODE_DISABLE_{EXPLORE_PLAN_AGENTS,WORKFLOWS,CRON,BUNDLED_SKILLS,CLAUDE_MDS,GIT_INSTRUCTIONS}=1`,
  and empty attribution in the flag settings; Codex `-c` overrides
  (`skills.include_instructions`, `project_doc_max_bytes`,
  `include_collaboration_mode_instructions`, `include_environment_context`,
  `include_permissions_instructions`, `developer_instructions`). Leaving out
  `ToolSearch`, `BashTool` or `UserSettings` (whose settings carry Herdr's
  SessionStart hook, which Flow's binding relies on), `SkillTool` or
  `BundledSkills` with Workflow
  kept, or a part of the other harness answers
  `StartRejected.LeaveOutRefused.<why>`. A Claude receipt is refused when
  the session uses a tool its leave-outs removed or shows a skill listing
  with the Skill tool left out.
- ethos-zero is pinned at 07714b0 (17.0.0); the starter's types and traits
  are generated from `crates/flow-nexus/ethos/starter.ethos`. Every value a
  starter capability reads reaches it through an Ethos trait the concrete
  type implements (`crates/flow-nexus/src/values.rs`); `VerifiedContext`
  alone stays a Rust type, so that its fields stay private.

# Flow 0.34.6

No wire change. The store gains the `flow_nexus_start_bundles` table; a
store written before it opens with the table added and holds no bundles.

- **A fresh wake keeps the metaflow's system prompt.** At Start, Flow records
  the caller's system-prompt bundle bytes for the launch's Address. A fresh
  wake writes exactly those bytes, without any Predecessor or Remembered
  line, as the woken flow's system prompt, so the spirit and intent modules
  the launcher put there survive every wake. The line naming the metaflow and
  why this flow started opens the first prompt instead.
- **A wake without a Start bundle is refused.** An Address no Start kept a
  bundle for is not woken fresh: the wake logs `FreshNotStarted` and answers
  `Failed(StartBundleMissing)`, the work stays queued and the metaflow
  Asleep. There is no one-line fallback.

# Flow 0.34.5

No wire change; no store change.

- **A native title has a lowercase topic.** The title Flow renders for a pane, tab or
  flow is `{ <Aspect> <topic> <Layer> <FlowId> }` with the topic lowercase, e.g.
  `{ Psyche core Tertiary 7c89aa }`. The typed Address and registry keys are
  unchanged. A resumed session whose latest title record is the old
  capitalized spelling is renamed once on its next wake (a `/rename`, no model
  turn); the old spelling is no longer accepted.

# Flow 0.34.4

No wire change; no store change.

- **A resumed session keeping its title is not renamed.** When the
  session's latest `custom-title` transcript record already is the canonical
  title, the title step types no `/rename`; the new pane is still labelled.
- **A rename is read back from its own record.** A typed `/rename` passes
  only when the terminal title shows it and the transcript holds one more
  title record than before the command was typed, the last naming the
  title. A record from the session's first life no longer passes it before
  Claude has taken Herdr's Enter.
- **The work waits for the resumed pane.** Before a wake types its queue,
  Herdr's `agent wait <pane> --until idle --until done` runs, then the
  composer is read until it holds no text (bounded, as a submission read).
- **Refusals say why.** A wake that does not leave its seat Awake logs
  `flow-nexus: wake of <aspect> <topic> <layer> refused: <reason>` with one
  of `StoreUnreadable`, `NotAsleep`, `NothingQueued`, `SessionLive`,
  `HerdrUnreadable`, `Unplaceable`, `BundleMissing`, `PaneNotOpened`,
  `HarnessNotResumed`, `BindingUnread`, `BindingDiffers`,
  `ResumeNotRecorded`, `NotResting`, `HandOver(<reason>)`,
  `FreshNotStarted` or `AwakeNotRecorded`. Work not typed into a pane logs
  `flow-nexus: work not typed into flow <id>: <reason>`, the reason
  `Target(..)`, `Pane(..)` (`ComposerOccupied` among them) or `Refused`.
  The wire answers are unchanged.
- **A seat left live by a failed wake is woken as it is.** When the last
  flow of an Asleep metaflow is Pending or Active and the pane Flow
  recorded for it runs its own native session under its claim, a wake
  records it Awake taking nothing from the queue, and the queue is then
  typed in oldest first; it is no longer refused `ConflictingBinding`. The
  Nexus does this at its start too, for every such metaflow, so a store a
  0.34.3 wake left so needs no hand edit.
- **A wake failing after its pane opened leaves nothing running.** The
  pane is closed and the flow recorded Stopped; the work stays queued and
  the metaflow Asleep for a clean next wake. A pane that cannot be closed
  is logged and its flow left as it is.

# Flow 0.34.3

No wire change; no store change.

- **A default-session pane is found.** Herdr's default session exports
  `HERDR_PANE_ID` and no `HERDR_SESSION`, so a seat Flow launched there was
  answered `Unidentified` by Identify, and ResolveCaller, ResolvePeer and
  Bind did not find its flow. For a process whose nearest mark is the pane
  ID alone, the Nexus walks its ancestry on to the Herdr server: the
  ancestor whose `/proc/<pid>/exe` is the Herdr binary (the `herdr` on the
  Nexus's `PATH`, canonical). The session is the one whose socket in `herdr
  session list --json` that server listens on, read from the server's open
  descriptors and `/proc/<pid>/net/unix`. A server on the sockets of more
  than one session, a different pane ID on the way up, or no such server
  names no pane. Every later check is unchanged.
- **A Herdr server running a replaced binary names no pane.** When the
  `herdr` on `PATH` has moved to a new store path since the default-session
  server started, the server's executable no longer matches and its panes
  are not found until that server restarts.
- **Session sockets are read from JSON.** The agent observer locates a
  session's socket through `herdr session list --json`.
- **Configuration-only startup is unchanged.** It still treats a peer as in
  a pane only when it or an ancestor carries both marks.

# Flow 0.34.2

No wire change; no store change.

- **A Stop waits for the seat to rest.** Before taking the dispatch gate,
  the settling of a `Stopped` report runs Herdr's `agent wait <pane> --until
  idle --until done --timeout 15000`. Claude runs its Stop hook while its
  spinner is on screen, so Herdr showed the seat working at the Stop and it
  never slept.
- **The hook's connection closes first.** A Report is answered and its
  connection closed before what it sets going runs, so no Herdr wait begins
  while the hook is still connected.
- **A Stop before confirmation is settled.** When a launch is confirmed and
  the last event its harness reported is `Stopped`, the Stop is settled once
  the dispatch gate is released and the confirming answer is written.
- **A flow's end puts its metaflow to sleep.** Stop, Retire or Exit of the
  confirmed flow a metaflow is Awake as records the metaflow Asleep in the
  same write, the flow joining Past. A queued request wakes it as usual. The
  Sleep operation now reads that write instead of recording Asleep itself.
- **A retired seat wakes fresh.** A last Past flow that is Retired or Exited
  is never resumed; its seat wakes fresh. Exited is a pane that went away
  without Flow closing it, which `record_resumed` already refused to resume.
- **A metaflow left Awake as an ended flow is Asleep.** Start, Wake,
  Deliver and Identify, and the Nexus at its start, record such a metaflow
  Asleep (`Record.Asleep`) before reading it, logging `flow-nexus: metaflow
  Awake as ended flow <id> was recorded Asleep`. A store an older Nexus
  left needs no hand edit. A metaflow Awake as a Bind flow with no flow row
  is left as it is.
- **A seat kept awake says why.** Every early exit of the Stop settling logs
  `flow-nexus: flow <id> kept at its Stop: <reason>`, the reason one of
  `StoreUnreadable`, `NotAwake`, `Unconfirmed`, `LayerNeverSleeps`,
  `Waking`, `UnknownFlow`, `NotResting(<pane agent>)`, `DrainRefused`,
  `NotResumable`, `LockHeld` or `SleepRefused`.

# Flow 0.34.1

No wire change; no store change.

- **A launch whose flow ended is settled.** A launch attempt with no
  outcome no longer refuses a Start at its Address as `AddressAwake` once
  the flow its native launch bound is Stopped, Retired or Exited. An attempt
  not yet bound, or bound to a flow not yet registered, still refuses it.
  Such an attempt is no longer watched for its receipt.
- **A stale Address hold is given back at Start.** Before reading its
  Address, Start gives back every hold there whose flow has ended, restoring
  the metaflow the hold found. A store that carries such a hold, or an
  unsettled attempt behind a Stopped flow, needs no hand edit.
- **A contradicting receipt rejects the launch.** When the first-turn
  receipt read fails (another model or effort, another first input, an
  unreadable transcript), Start and the ambiguous-launch promotion answer
  `StartRejected.NativeLaunchRefused`, close the pane, record the flow
  Stopped and give back its Address, instead of `StartAmbiguous`. The log
  line is `flow-nexus: launch <request> refused by its receipt: <reason>`.

# Flow 0.34.0

No wire change; no store change.

- **A bind waits for the harness.** Before reading the agent, Bind runs
  Herdr's `agent wait <pane> --until idle --until done --timeout 30000`; it
  no longer checks `interactive_ready` once. A timeout is refused as
  `not-ready` (`flow-nexus: herdr stage=bind category=not-ready`).
- **The first prompt waits for the rename.** After the title (`/rename`
  accepted and read back) and before the first prompt is typed, Flow runs the
  same Herdr wait. A timeout types nothing and is refused as `not-ready`.
  The title step shares this wait.
- **A pane whose harness never appeared is closed.** When Herdr's snapshot
  shows the launch pane and terminal with no agent on it, a refused launch
  closes it (and a Replace's reap closes it) instead of only marking it
  orphaned.

# Flow 0.33.0

No wire change; no store change.

- **A seat's title is its Address and Flow ID.** The native title, set
  before the first prompt, is the Datom `{ <Aspect> <Topic> <Layer>
  <FlowId> }`, e.g. `{ Field flow Quaternary 2712d3 }`. The model no longer
  enters it, so a launch whose model is an alias such as `haiku` is titled;
  the model display table is gone.
- **Claude is waited for before its title.** The title step runs Herdr's
  `agent wait <pane> --until idle --until done`, then reads the agent until
  Herdr reports its official native session and interactive readiness,
  before `/rename`.
- **A refused title is logged by its reason.** The Title stage returns a
  typed refusal; the Nexus log line is
  `flow-nexus: herdr stage=title category=<reason>` (`not-ready`,
  `native-session-missing`, `title-readback-differs`, `herdr-unreadable`, and
  the rest), never adapter text. Herdr's "no official native session" is
  classified `native-session-missing` at every stage.
- **A launch refused after its pane exists leaves nothing running.** Start
  closes the pane and records the flow it registered Stopped (which gives
  back the Address its acknowledgement held), then releases the FlowId
  claim. A created pane with no registered flow is closed when Herdr shows
  its harness there.
- **Stop closes a Pending flow's live pane.** Stop and every pane close now
  require only that a fresh Herdr snapshot shows the flow's recorded binding
  (pane, terminal, harness), not prompt readiness or a FlowId claim. A
  Pending flow left by a refused launch, such as `2712d3`, is stopped with
  `Stop.<flow>`; a pane Herdr does not show still answers
  `StopRejected.RouteUnavailable`.

# Flow 0.32.1

No wire change; no store change.

- **A Claude seat's first prompt no longer tells it to Read its bundle.** The
  bundle already reaches Claude as `--system-prompt-file`, so the line
  «Read <bundle> for your launch mode» is gone from both Claude renderers.
  The first prompt is the stacked `/skill` commands, the skills past the
  stack (or, in the direct form, «Load these skills through the Skill tool
  in this order: …»), then the brief and its sources. Codex is unchanged: its
  first turn never named the bundle. The first-turn digest is taken over the
  new text, so launch confirmation matches as before.

# Flow 0.32.0

No wire change. The store gains one table; every store opens.

- **A launched seat is registered before its first prompt.** The write that
  journals a launch's registration acknowledgement, made once the seat's
  native binding is witnessed (Bind) and its flow registered in its pane,
  records the profile's Address Awake as the launched FlowId, by the same
  rules confirmation used. The first prompt is typed after it, so a seat
  whose first tool call runs `message 'Send.{ … }'` is identified and its
  request delivered, though no answer of the seat is recorded yet.
- **Until confirmation the Address is held.** A row in the new
  `flow_nexus_address_holds` table keeps the metaflow as the launch found
  it. Confirmation keeps the Address and drops the hold in the write that
  makes the flow Active; LaunchStatus and confirmation are otherwise as
  before.
- **A launch that comes to nothing gives the Address back.** A launch
  settled `StartRejected` (or `ReplaceRejected.LaunchRefused`), or a flow
  recorded Stopped, Retired or Exited while it holds its Address, returns a
  metaflow still Awake as it to what the hold kept (Asleep with its Past, a
  replaced predecessor Awake again), keeping requests queued meanwhile; one
  the launch created is removed, or left Asleep when requests were queued.
  No Awake registration of an unconfirmed flow outlives it.
- **A held seat neither sleeps nor drains at a Stop** until confirmation.
- **An Address taken meanwhile** refuses the acknowledgement, and Start
  answers `StartRejected.AddressAwake` or `.AddressEnded`.

To deploy: install 0.32.0 and restart the Nexus. A store written by 0.31.0
opens and gains the table at its first full open. Clients are unchanged.

# Flow 0.31.0

The ordinary wire changes: signal-flow 16.0.0 (229933d9) and
meta-signal-flow 19.0.1 (b6f86546). The Flow CLI and Nexus are rebuilt and
restarted together; an older peer is refused at the greeting.

- **A launch names its seat's Address.** `LaunchProfile` carries the
  `Address` (`{ Aspect Topic Layer }`) where it carried the `Aspect`:
  `Start.{ { id [] [ skills ] { Field flow Quaternary } UltraLow Claude
  haiku medium None [] session bundle «prompt» } { origin session turn } }`.
- **The launch enters the registry.** When a launch is confirmed (its
  receipt observed in the native transcript), the Nexus records that
  Address's metaflow Awake as the launched FlowId in the same write that
  makes the flow Active. A metaflow Awake as a live flow, or bound to a
  live process, is refused; an Ended one is refused; an absent or Asleep
  one, or one Awake as a flow no longer live, is taken, a displaced flow
  joining Past. A replacement takes over its predecessor's metaflow. The
  seat can now sleep at its Stop and be woken by a queued request.
- **A held Address refuses the Start.** A Start whose Address is Awake as a
  live flow, or named by another launch not yet settled, answers
  `StartRejected.AddressAwake`; one whose Address was ended answers
  `StartRejected.AddressEnded`. Nothing is launched.
- **Wakes take the Address the same way.** A fresh wake's launch takes it
  at its confirmation; a resume takes it with `Record.Awoken`, in the write
  that removes the handed-over requests from the queue.
- **A launched seat is identified.** `Identify` names a process bound by
  `Bind`, or descended from one, as before; otherwise a process in the pane
  of a launched flow (found from the Herdr marks it or its nearest marked
  ancestor carries, with the registry and the live Herdr snapshot agreeing
  on the pane's one flow) names the Address that flow is Awake at. Such a
  seat sends through Message:
  `message 'Send.{ Address.{ Mind flow Secondary } Order.«…» }'`.
- **Old launch attempts are set aside.** A launch-attempt row archived
  before 16.0.0 names no topic or layer, so it cannot read as a current
  row. At open it is moved whole into the quarantine table under its shape
  (`BeforeAddress`, or `BeforeSystemPromptBundle` for older rows), logged
  `LaunchAttemptSetAside.{ id Shape }`, and no longer answers LaunchStatus.
  A flow such a launch bound keeps its role: a store written before roles
  adopts it from the kept row. Rows earlier releases carried forward from
  `BeforeSystemPromptBundle` are now set aside as `BeforeAddress`. Every
  store opens; nothing else in it changes.

To deploy: install 0.31.0, restart the Nexus, and rebuild every client of
signal-flow (Message, its CLI and signal-message) against 16.0.0. Seats
launched before this release hold no metaflow until they Bind or are
launched again.

# Flow 0.30.1

No wire or store change.

- **Stop closes the store.** `flow-nexus`, daemon and `--configure-only`,
  takes SIGTERM and SIGINT as a stop: it accepts no new connection, lets
  the requests in flight finish, ends launch and agent subscriptions, removes
  its socket files, and closes the store, so redb's recovery flag is clear
  after `systemctl stop`.
- **A flagged store opens.** A store whose writer was killed carries redb's
  recovery flag, which a read-only open refuses. Recognition now repairs a
  temporary copy beside the store, runs the same checks on it and deletes
  it; only a recognized store reaches the writer open, which repairs the
  original. A flagged foreign or invalid store is refused unchanged.
- **Sockets.** After recognition, the daemon and `--configure-only` remove a
  stale socket file at their socket paths and refuse one that accepts a
  connection; the daemon no longer unlinks a live listener.

To deploy: install 0.30.1 and start the Nexus; a store left flagged by an
earlier kill opens without hand repair.

# Flow 0.30.0

No wire or store change (signal-flow d1358d3 and meta-signal-flow cd8d5ec
unchanged). The Nexus Operation root loses `Continue.FlowId` and its
`Continued` outcome; that root is the Nexus's own and crosses no wire.

- **The first prompt is the brief.** A launched seat's first prompt carries
  the brief and ends with it, and the seat's first answer performs it. The
  composed `first_prompt_text` is `first_prompt_body` exactly, and
  `prompt_sha256` is the digest of the text the harness receives.
- **The receipt is the native transcript.** The watched transcript of the
  bound session shows the launch took: the first turn authenticated by its
  digest, every selected skill loaded in order, and, for Claude, the seat's
  first answer after them, at the intended model and effort. Codex records
  its turn's model and effort before the input, so its confirmed skill loads
  are the receipt. `NativeTargetReceipt.native_turn_id` is the Codex turn or
  the Claude assistant row's uuid; `receipt_sha256` is the SHA-256 of the
  transcript record that completed the receipt, newline included.
- **Started is the end of a launch.** Settling a launch Started, or a
  Replace Replaced, types nothing into the seat's pane.

To deploy: let every launch in `PromptAmbiguous` settle, install 0.30.0 and
restart the Nexus. A launch composed by 0.29.0 and still ambiguous at the
restart is not promoted: its typed text no longer hashes to its stored
body digest.

# Flow 0.29.0

Wire change: meta-signal-flow 19.0.0 (cd8d5ec) adds `Reconfigure`; the meta
contract digest changes, so `flow-nexus`, `flow-meta` and every meta client
move together and an 18.0.0 peer is refused at the greeting. No store
change: no table, family or record shape is added, and a 0.28 store opens
as it is.

- **Reconfigure.** `Reconfigure.ConfigureNexus` replaces the stored Nexus
  value as a whole in one commit. Models, thresholds, modules, flows,
  metaflows, Message's binding and locks are kept. It answers
  `Reconfigured`, or `ReconfigureRejected.Unconfigured` (no Nexus value yet;
  first setup stays `Configure.Nexus`) or
  `ReconfigureRejected.CodexModelsOverlap` (one model names both Codex
  endpoints), and then nothing changed. `Configure.Nexus` still answers
  `ConfigureRejected.Conflict` for a differing value. `flow-nexus
  --configure-only` accepts one `Configure` or one `Reconfigure`.
- **When it applies.** The running Nexus reads `MessageNexusBinary`,
  `MetaAspects`, `Lease`, `HarnessProfiles` and `MessageNexusPath` from the
  store at each use, so those apply at once. `SourceRoot`, `StableCodex` and
  `NextCodex`, the Codex model lists included, are read when the Nexus
  opens: restart the Nexus to serve them.

To deploy: install 0.29.0, restart the Nexus, read the stored value with
`flow-meta Configuration`, send the whole edited value with
`flow-meta 'Reconfigure.{ ... }'`, then restart the Nexus again so it opens
with the replaced Codex endpoints.

# Flow 0.28.1

No wire or store change. Terra is not offered: the stable Codex line is
`gpt-5.6-sol` and `gpt-5.6-luna`, and `gpt-5.6-terra` has no title mapping.
Terra is not used until Terra 6.

# Flow 0.28.0

No wire change (signal-flow d1358d3 and meta-signal-flow 6d07b7b unchanged).
The store gains one table, `flow_nexus_flow_context_sizes`: a 0.27.0
running store is recognized and its first open adds the table; no other
table, family or record shape changes. Rebuild and restart the Nexus;
`flow`, `flow-meta` and `flow-hook` come from the same package.

- **Context size.** When a flow's hook reports `Stopped`, the Nexus reads
  the flow's Claude transcript once (`<claude home>/projects/*/<session>.jsonl`)
  and writes the last main-thread usage entry's `input_tokens +
  cache_read_input_tokens + cache_creation_input_tokens` for that FlowId
  (`Record.Context.{ FlowId Tokens }`). Nothing polls the transcript.
- **Resume or fresh.** A wake resumes the last flow's session when its
  recorded size is under the layer's configured `Threshold` refresh value,
  and launches fresh when the size is at or over it. With no recorded size
  or no configured threshold it resumes.
- **Only a resumable seat sleeps.** A seat sleeps only when it can be
  resumed: a Claude flow with a native session. A Codex seat, or a seat
  without a native session, stays awake whatever it reports. A seat too big
  to resume still sleeps and wakes fresh.
- **Idle drain.** A seat whose Stop finds it resting with work queued is
  handed the oldest request, which then leaves the queue, and stays awake;
  it sleeps at a later Stop with nothing queued. A Primary is unchanged:
  it neither sleeps nor drains.
- **Durable wake.** A wake reads the queue without taking it. The requests
  it handed over leave the queue in the same write that records the
  started flow Awake (`Record.Awoken.{ Address FlowId Delivered }`). A wake
  that fails leaves the queue as it was. At start the Nexus wakes every
  Asleep, non-Primary metaflow whose queue holds an Order, Question, Psyche
  or Psyches.
- **Duplicate guard.** A wake is refused (`Failed.ConflictingBinding`)
  while a pane in the Herdr session it would use, or the last flow's
  session, already runs the last flow's native session; an unreadable
  Herdr refuses it (`Failed.HerdrRefused`).
- **Drain.** `Drain.Address` types the oldest queued request and answers
  it; the drain after a wake repeats it until the queue is empty.

# Flow 0.27.0

No wire change (signal-flow d1358d3 and meta-signal-flow 6d07b7b unchanged).
The store opens as 0.26.0 left it: no table, family or record shape
changes. Rebuild and restart the Nexus; `flow`, `flow-meta` and `flow-hook`
come from the same package.

- **Sleep.** When a flow's Claude Stop hook reports `Stopped`, the Nexus
  answers the hook first, then puts the metaflow Awake as that flow to
  sleep if Herdr shows the seat Idle or Done, its queue is empty, no sender
  holds a lock on it, no wake holds it, and it is not a Primary. Sleeping
  closes the pane (the flow is recorded Stopped), records the metaflow
  Asleep and appends the flow to Past. The flow's row keeps its native
  session and its launch bundle copy, so it can be resumed.
- **Wake.** A Deliver that queues an Order, Question, Psyche or Psyches for
  an Asleep metaflow that is not a Primary wakes it after Message has its
  `Queued` answer, off the dispatch gate. The whole queue goes to the seat,
  oldest first, in one prompt, each Request as its datom. A Claude flow
  with a native session is resumed: a new pane runs Claude with
  `--resume <session>` and FLOW_ID set to the same FlowId, and the binding
  must show the same session and claim or the wake fails. Otherwise the
  seat is launched fresh through Start, from the layer's configured Model
  and the topic's configured modules (as launch sources), in the Herdr
  session the last flow ran in. Results and Notices wait queued. A failed
  wake puts the work back at the head of the queue and the metaflow stays
  Asleep.
- **Drain.** Requests queued while a wake runs are typed into the woken
  flow's pane once it is up; a second request starts no second wake.
- **Bind.** A peer that runs in a flow Flow launched binds its Address
  Awake as that flow's FlowId, instead of an invented `flow-…` id, so that
  flow's Stop can put the seat to sleep.
- **Operations.** `Sleep.{ Address FlowNode }`, `Wake.Address` and
  `Drain.Address`, answered `Slept`, `Woken.FlowId` and
  `Drained.Vector<Request>`; `Record.Asleep` and `Record.Awoken`; `Failed`
  gains `NotAsleep`, `NotAwake` and `NothingQueued`.
- **Unchanged.** Codex flows get no FLOW_ID and send no Stop report, so they
  never sleep; an Asleep Codex flow is not resumed but woken fresh. A wake
  in flight when the Nexus stops is not resumed on restart; its taken
  queue is lost only if the Nexus dies between taking it and handing it
  over or putting it back.
- **Deploy.** `deployment/flow-nexus.service` now passes the one unheaded
  Start argument the binary requires.

# Flow 0.26.0

No wire change from the paired contracts named here: signal-flow
d1358d396e96934795a5b2ae2192499b3eaff9c3 and meta-signal-flow
6d07b7bb077da8755eae7d1146d9dfd64d7b7d7c, with Flow patching the shared
`flow-ethos` leaf to its own workspace definition.

- **Start argument.** Flow Nexus requires one unheaded struct argument,
  `'{ «ordinary-socket» «meta-socket» «store-path» }'`. A `Start.` head is
  refused. `--configure-only` with the same value opens only a recognized
  or fresh store, serves one typed `Configure.Nexus`, and exits.
- **Configuration.** `Configure.Model`, `.Threshold` and `.Module` join
  `.Nexus` in one typed configuration family; an identical value is
  idempotent and a conflicting replacement is refused. Production startup
  requires the Nexus snapshot and uses the explicit store and sockets;
  there is no HOME store fallback.
- **Metaflows.** The ordinary socket carries `Bind`, `Identify`, `Lock`,
  `Deliver`, `Release`, `End`, `Current` and `Metaflows`. `Deliver` queues
  durably and consumes the lock atomically; 0.26.0 neither drains the
  queue nor wakes a seat.
- **Aspect.** One shared `Aspect` type serves addresses, launch profiles
  and meta configuration.

# Flow 0.25.0

The Signal wire and the launch-outcome archive change. Rebuild and restart
the Flow Nexus, its CLI, flow-meta, and flow-hook from one immutable package
closure; an older Signal peer is refused at the contract greeting.

- `signal-flow` is 11.0.0. `LaunchStatus` now returns a `LaunchInspected`
  record with the attempt, optional settled outcome, and current recorded
  `FlowNode` or absence. `Observe.Launch` sends these inspections as phases
  change and sends the settled inspection last. `LaunchPending` remains a
  possible `Start` reply while its original launch runs.
- A view whose database markers changed returns `ReadViewChanged`. An outcome
  without an attempt, or a settled success that disagrees with its recorded
  native binding, returns `InconsistentRecords`. The read is marker-checked,
  not a storage transaction.
- The launch-outcome family advances from v1 to v2. It preserves every v1
  outcome written against `signal-flow` 10.0.0 at
  `f95034de0b203d886ba574fcb513c691c64b8498`. Its source
  `ethos/signal.ethos` declared `BindingRefused` as a unit variant; that
  value becomes typed `BindingRefused.Unclassified` because the stored
  outcome did not retain whether Reserve, Bind, or Title refused. Corrupt
  v1 rows refuse the upgrade without rewriting the predecessor family.
- `StartRejection.BindingRefused` now carries a `BindingRefusal`. A Reserve
  claim refusal is `ClaimUnavailable`; aggregate Bind and Title failures keep
  the broader `NativeLaunchRefused` classification.

Pin `signal-flow` 11.0.0 with `signal` 8.0.0 e0e3c055. Deploy `flow`,
`flow-meta`, and `flow-hook` together, then restart the Nexus before using
the changed CLI.

# Flow 0.24.0

No wire change (signal-flow and meta-signal-flow unchanged). The store opens
as it is: sema-engine moves from 0.16.0 to 0.18.0, whose changes are
additive for Flow (Flow declared no `with_prior`), and a copy of the
production store taken 2026-10-03 opened under 0.24.0 with its
configuration and 26 flow rows read back. Rebuild and restart the Nexus;
`flow`, `flow-meta` and `flow-hook` come from the same package.

- **Session id per Nexus.** The Claude session id Flow chooses for a
  launch is now the SHA-256 of the Nexus's own ordinary socket path and the
  launch request id (`flow-claude-session-v2`), UUIDv5-shaped as before.
  Two Nexuses sharing one `~/.claude` (a sandbox beside production) serve
  different sockets, so one request id never names one session twice, and
  their `flow-id` claims never meet. One Nexus still names one session per
  request, across restarts, as long as its configured socket is unchanged.
- **Refused launches give back what Reserve took.** A Claude launch refused
  after Reserve performs the new Operation `Release.FlowId` (answered
  `Released`): the flow held in Memory is let go (its events row retracted
  unless the flow was registered), and the `flow-id` claim is given back
  under the claim's lock: the empty lane `flows/<FlowId>/` and the marker
  `.<FlowId>.flow-id` are removed. A lane something wrote into keeps its
  claim. A hook `Report` for a released FlowId is refused `UnknownFlow`.
  Reserve also releases its own claim when it cannot hold the flow.
- **Deploy.** Let in-flight Claude launches settle before the restart: a
  launch reserved under 0.23.0 that resumes under 0.24.0 expects a session
  id derived the new way and its Bind is refused. Settled launches and
  running flows are untouched. Held records and claims left by launches
  refused under 0.23.0 are not swept; they stay until removed by hand.
- **Unchanged.** Codex launches still get neither FLOW_ID nor FLOW_SOCKET:
  Codex names its thread inside its app server at thread/start, and a
  Herdr-launched Codex runs its tools in that remote app server, not in the
  pane, so a pane export would not reach them.

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


# Batch 11: reports by session and notifications

The ordinary signal contract is signal-flow 37.0.0 and the privileged contract is meta-signal-flow 51.0.0. A harness report identifies either a Flow or a native session; the Nexus resolves session reporters against the adopted seats it holds. Claude's Notification hook records `Notified` events. The flow-events family upgrades from v4 to v5 without changing any archived event.
