# Flow Nexus

Flow Nexus starts, restarts, delivers to, stops, lists, and resolves flows,
and is the only writer into their panes. `flow-nexus-daemon` is the
long-running process, started with its place; `flow` is its ordinary
client, `flow-meta` its privileged client, and `flow-hook` the
hook Claude Code runs in every Claude flow. The Nexus persists Flow identity
and policy in one Sema store, and its sockets carry only length-prefixed rkyv
Signal archives.

The wire contracts are independent repositories:

- `signal-flow` defines `Start`, `Refresh`, `Stop`,
  `List`, `ResolveRecipient` for Message Nexus routing, `ResolveCaller`,
  which names the flow, aspect and model of the process that calls,
  and `Observe.Agent`, a flow's Herdr agent state on open and on each change.
  A frame that decodes as no Input, an earlier hook's Report among them, is
  answered `FrameRefused` with its caller's process and seat and recorded;
  `Observe.Undecodable` shows every record and each new one.
  `Observe.TurnEnds` sends the last turn end of each harness, then each
  turn end (a Claude Stop in its own session, a Codex turn completed) with
  the metaflow's Address, the harness and the flow.
  `Observe.Metaflows` sends the metaflows as `Metaflows` lists them, then,
  at each commit that changes them, each metaflow that changed as it now
  stands (`MetaflowChanged`) and the Address of each no longer listed
  (`MetaflowGone`); `Metaflows` stays the one-shot read.
  Nothing on the ordinary socket writes into a pane.
- `meta-signal-flow` defines `Configure`, reset-credit consumption, `Retire`,
  and the pane writes: `Deliver`, `Vet`, `Command`, plus `ResolvePeer`.

The ordinary client accepts exactly one inline `Input` Datom from the
`signal-flow` contract. `Start` and `Replace` also require the caller to name
the selected SYSTEM modules explicitly; use an empty vector when none are
selected. This CLI-only list filters those exact names from the resolver's
expanded USER skill vector. It does not change the typed profile or inspect
the supplied precomposed SYSTEM file.

```sh
flow '<one inline Input datom>'
flow --system-modules '[ spirit-core vision-design ]' '<one inline Start or Replace Input datom>'
flow --system-modules '[]' '<one inline Start or Replace Input datom>'
```

`Rehearse` carries the same declared `LaunchRequest` as `Launch`. It reads the
metaflow's configuration and composed context, then returns the one final
native invocation without creating a pane, thread, identity, prompt fd, file,
registration, queue entry, or lifecycle record. Every argv word is
single-quoted, including semantic placeholders such as `'<flow-id>'`,
`'<pane>'`, `'<prompt-fd>'`, and `'<thread>'`; a manual runner supplies those
resources before invoking the result.

Each metaflow may configure a `HarnessFlags.String`. Its words form the final
harness-option tail after effort. Unquoted whitespace separates words; single
quotes preserve their entire interior; double quotes preserve whitespace and
only interpret backslash before dollar, backtick, double quote, backslash, or
newline; outside quotes backslash protects the next character and continues a
line before newline. Quoted empty words are kept. The syntax performs no
expansion, substitution, evaluation, assignment, comment, or operator
interpretation, so `Artifact*`, `#`, `;`, `|`, and `>` are literal words.
Unclosed quotes and incomplete trailing escapes are typed refusals before any
launch effect.

The ordinary pane operations use the same typed edge:

```sh
flow 'Stop.00f95a'
flow 'Metaflows'
flow 'Observe.Agent.00f95a'
flow 'Observe.Undecodable'
flow 'Observe.TurnEnds'
flow 'Observe.Metaflows'
flow-meta 'ObserveConfiguration'
```

A Nexus starts in its place, the one datom it is started with, and reads
nothing else for it: `flow-nexus-daemon '{ main /run/user/1001/nexus/main
/home/li/.local/state/nexus/main }'`, the place name, Runtime (its sockets
`flow.sock` and `flow-meta.sock`) and State (its memory `flow.sema`). A
client takes the datom and nothing else: `flow '<Input>'` or
`flow-meta '<Input>'`, the component's name and the same with `-meta`. The place is their
configuration's, one datom at `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`
(`~/.config/nexus/flow-nexus.datom` when the variable is unset),
`CliConfiguration.{ PlaceName }`, `{ main }`; the place
name resolves under `$XDG_RUNTIME_DIR/nexus/` to that place's socket. A
configuration that is missing, cannot be read or names no place is a typed
refusal naming the file (`Missing.<path>`, `Unreadable.{ <path> <reason> }`,
`Invalid.{ <path> <reason> }`, exit 2), and nothing is sent. No place is
ever typed: a second argument is refused. Two places are two names: the
stable version runs at `main`, a candidate at `next` with its own memory,
and a version becomes main by being started at `main` once the version
there has stopped; a client reaches `next` by a configuration naming it.
The
socket every flow's hook reports to is its launching place's, so a flow
launched at main keeps reporting to whichever version is main.

Every connection greets first, on both sockets. The client writes its
contract's Signal greeting (`signal_flow::Input::greeting()` on `flow.sock`,
`meta_signal_flow::Input::greeting()` on `flow-meta.sock`) and reads the
receipt; it writes its Input only after `Greeted` names its own digest. A
first frame that is not this contract's greeting, an older peer's greeting
or its raw Input among them, is answered `GreetingRefused.ContractMismatch`
with this contract's digest before anything is decoded, the connection
ends, and the refusal is recorded, metadata alone, in `Observe.Undecodable`.
The `flow` CLI, `flow-hook` and `flow-meta` greet; a
refused greeting is the CLI's error (exit 2) and the hook's `2` line.

A place's two directories are shared with every other Nexus at it and are
declared by the environment (CriomOS-home declares them as user tmpfiles
rules): the Nexus finds them, does not start when either is absent, and
makes and removes only its own two sockets there. A socket appears at its
name only once it listens: each is bound under a temporary name beside it,
`.flow.sock.<pid>`, its mode set to `0600`, made to listen, then renamed to
its name, replacing a stale socket a crashed Nexus left there; a peer
waiting for Flow waits on its name moved in (inotify `IN_MOVED_TO`) and
connects at once. Both are placed before the Nexus serves, then it sends
`READY=1` to `$NOTIFY_SOCKET` when it is set, so a systemd unit of
`Type=notify` is started only once both listen. A
fresh memory starts with the configuration built into the executable, so
the Nexus binds and announces readiness before it is configured. Every
meta `Configure` is applied to the running Nexus at once, without a
restart, and `ObserveConfiguration` streams the configuration, then each
change. `Configure.HarnessHook.<path>` names the hook command written into
every Claude flow; unconfigured, it is the `flow-hook` beside the
Nexus.

Every write into a pane goes through the privileged socket, and Flow is the
only writer. `Deliver` carries a typed `Message` whose head is its Priority:

```sh
flow-meta 'Deliver.{ m-7f3a2c:00f95a:0 00f95a Soft.{ m-7f3a2c Owner Text.«continue with the implementation» } }'
flow-meta 'Deliver.{ m-81b0e4:00f95a:0 00f95a HardAbrupt.{ m-81b0e4 Flow.e167d8 Text.«stop the build» } }'
flow-meta 'Command.{ 00f95a Compact }'
```

Compaction is an emergency measure: `Command.{ 00f95a Compact }` is refused,
before anything reaches the pane, unless the flow's context at its last Stop
reached the Refresh threshold configured for its model
(`CommandRejected.CompactionRefused.BelowThreshold.{ 120000 200000 }`,
`.Unmeasured` with no context recorded, `.NoThreshold` with no threshold for
its model).

Flow renders the Message itself, so the pane text always begins
`HardAbrupt.`, `MiddleAbrupt.` or `Soft.`, then the letter's MessageId:

```
Soft.{ m-7f3a2c Owner Text.«continue with the implementation» }
```

That id is the whole of what a recipient needs to answer
`message 'Acknowledge.m-7f3a2c'`, which is the only source of Read. Flow
never interprets it; the DeliveryId beside it is Message's own per-attempt
key.

The body may hold no control character but LF and TAB (`ControlCharacter`
carries the byte offset in the pane text), and a first line that is a harness
command is refused as `HarnessCommand`: use `Command`. Every tier needs the
recipient bound, not Blocked, and its composer blank. A `Soft` Send to a
recipient in its turn waits in its metaflow's queue for the recipient's idle
event, its Stop or its Codex thread's turn completed, and is delivered then
with everything queued for it, oldest first; `HardAbrupt` and `MiddleAbrupt`
are delivered when they arrive. `HardAbrupt` presses the harness profile's interrupt keys when the
recipient is Working and reports whether it was seen leaving Working; it reads
the composer first, and a draft there refuses it before any key. A prompt the
interrupt puts back into the blank composer is the cancelled turn's, and it is
submitted again before the letter. A write
holds the pane's lease from its first key to its last, so two writes never
interleave. `Presented` means the recipient was seen reacting on the exact pane
— Herdr's own `agent_prompted` reply after it waited, naming the pane and
terminal the route names, never the agent's label, which Herdr omits for panes
it was never given a name for. `Transported` means Herdr accepted the text,
`Uncertain` that it may have been typed and was not observed; Uncertain is
never retried, and a delivery a crash left under its lease settles Uncertain
when the Nexus opens. `Deliver` is idempotent on its DeliveryId. A Pending row
becomes Active when the flow is witnessed live: on a `Presented` Deliver, or
when `List` finds its bound pane present in Herdr. Presented is not Read.
`Stop` closes the flow's recorded Herdr pane when a fresh snapshot still
shows its binding there, whatever the seat's readiness and whether its FlowId
claim was given back, so a Pending flow whose launch was refused is stopped
too; it persists the Stopped lifecycle only after `herdr pane close`
succeeds, and answers `RouteUnavailable` when Herdr does not show the pane. `List` returns all durable rows, sorted by Flow ID, and reports each one's
true lifecycle. A row that is still live is reconciled against Herdr before it
is answered: its bound pane present, the route is refreshed and the flow is
reported Active; its bound pane gone, the flow is reported `Exited`, with no
route and no endpoint. A Herdr that cannot be read changes nothing. `List`
writes nothing — it is a query, and a query does not change what it is asked
about; only a command that witnesses a pane's fate persists an ended
lifecycle. The three ended states each name who ended the flow: `Stopped` is
Flow's own act, `Exited` is the seat's, `Retired` is an owner's, through the
privileged `Retire`. A pane going away never retires a flow: an exit retains
the record, and retirement comes from authority, never from an observation.
None of the three is a deletion — each keeps the row, its origin and its
history — and `Deliver`, `ResolveRecipient`, `Stop` and `Replace` treat all three
as gone. `Retire` refuses `AlreadyGone` for a flow that has already ended,
and `Maintained`, changing nothing, for the flow a Maintained metaflow is
Awake as; that metaflow's declaration is changed to `OnDemand` first.

`Start` carries a typed `LaunchProfile` alone. A profile names the module
identities of its system prompt and of its first USER message, the launched
flow's Address (aspect, topic, Model), model, effort, predecessor, remembered
flows, Herdr target session, leave-outs, flow kind, tool set, and first
instruction. The Nexus
derives the rest: the origin from the calling process, as Identify reads a
caller (the one flow bound in its Herdr pane, with that flow's native
session, or `Unbound` for any other caller, which is recorded and not
refused); and the harness from the model, Codex when a configured Codex endpoint serves it and Claude otherwise. Once the seat's native binding is witnessed and its flow
registered, before its first prompt, the launch records that Address's
metaflow Awake as the launched flow, so the seat's first tool call is
identified and sends through Message; confirmation keeps it, so the seat,
of any Model and either harness, stays Awake in its pane when its turn ends
with nothing queued, and work queued for it is handed into that pane; once
its pane is gone (closed by the living, crashed, or replaced) the metaflow is
Asleep and a queued request wakes a fresh flow for it, and a launch refused,
or a flow that ends, before confirmation gives the metaflow back as it found
it; a Start whose Address a live seat holds, or another unsettled launch
names, answers `StartRejected.AddressAwake`. A metaflow is Awake or Asleep:
a topic never dies. A wake whose start is refused leaves its queue as it
was, each waiting request's delivery `Retryable.WakeRefused` with the
refusal, so its sender's `ObserveSends` shows why; the next waking request
tries the start again. No model ranks above another: a Start is admitted by
its own Address alone, waits for no other model's seat, and starts nothing
before itself. Profile producers should use the
generated `signal-flow` Datom types rather than assembling positional text.
The old `flow start ...`, `flow restart ...`, and `flow resolve ...` argument
forms are rejected.

A metaflow declaration sets `FlowKind` (`Regular` or `Simplified`) and
`ToolSet` (`Default` or `Explicit.[ ... ]`). A simplified flow carries its
own system modules. Claude receives a declared tool set exactly as declared
(the empty set as `--tools ""`) and the default as no tool flag at all,
whatever the flow's kind. A simplified flow's first queued request is
submitted after the idle native seat is bound, without a synthetic startup
turn. The Stop
hook's `last_assistant_message` becomes a `Result` to the sender of the
consumed request. The response binding follows that request's native
receipt, rather than whichever sender wrote most recently.

Flow passes no tool restriction of its own: every tool list is
configuration, and a seat declared `Regular Default` launches with every
tool and subagent.

For the `Psyche core Fable` simplified presentation, the witnessed final source is queued
unchanged as one Order to `Psyche book Sonnet`; Psyche core Opus receives only a
notification. Completion and both queues commit atomically, so replayed
Stop hooks create no second publication path.

The core presentation launch reads its final literal authored role from
`FLOW_CORE_PRESENTATION_PROMPT`. The declared service supplies that file;
missing, unreadable or empty content refuses the launch. Its bytes follow
the composed modules and identity metadata, and the final text is hashed.
Claude Code receives the whole authored text through
`--system-prompt-file`; this makes no claim about hidden vendor context.

`flow-meta reset <idempotency-key>` asks the Codex app-server to consume the
next eligible reset credit. `flow-meta reset <idempotency-key> <credit-id>`
selects a specific credit. Reusing the key makes a retry stable. It sends the
privileged request through the meta socket; the command never talks to Codex
directly.

Existing daemon sessions enter the authoritative registry through typed meta
operations:

```sh
flow-meta register-codex <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [control-socket]
flow-meta register-claude <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [daemon-control-socket]
```

The two sockets are mode `0600`. The meta edge separates ordinary Flow
operations from administrative operations within the owning Unix user's
processes; it is not a security boundary between processes running as that
same user.

Claude resolution rechecks the job state, daemon roster, rendezvous socket,
control socket, and worker process on every request. It reports `Parked` when
that evidence no longer matches, when a permission wait is recorded, or when
the lifecycle is terminal. An ordinary `blocked` lifecycle remains routable.

Herdr routes live in a separate table in the same Flow Sema store, keyed by
Flow ID, so the existing v5 Flow rows retain their exact layout. Registration
requires a valid `flow-id` claim marker for the Flow ID and harness, then binds
the current native session to the exact Herdr agent, pane, terminal, and
harness. This keeps imported native sessions valid without inferring identity
from a UUID substring. A repeated registration cannot replace either the
native harness session or the Herdr binding.
Resolution rechecks that binding against `herdr api snapshot`; it returns the
route as available while the exact agent is idle or working and
`interactive_ready`, and otherwise returns `HerdrRouteSelection::Unavailable`.
The Message consumer owns its harness-specific blank-composer guard before it
submits input. Rows written before the route table was added also resolve with
an unavailable Herdr route.

The sockets and the store are the place's. The remaining defaults are
derived from two anchors, `HOME` and `XDG_RUNTIME_DIR` (`flow-defaults` is
their one home):

- source root: `$HOME/primary`
- Codex control socket:
  `$HOME/.codex/app-server-control/app-server-control.sock`

A new store persists these; a populated store resumes what it holds; meta
`Configure` is the only way to change them. Each `Configure` declares one
family whole, `Nexus`, `Models`, `Thresholds` or `Metaflows`, and replaces
the committed family in one commit, so the effective configuration is always
the last declaration of each family: an entry the declaration leaves out is
removed. It answers `Configured` with the change it was given, echoed
whole, also when nothing differs and nothing is written:
`Configured.Models.[ { Sol gpt-6-sol } ]`. `ConfigureRefused.DuplicateEntry`
(one Model, or one Address, named twice),
`ConfigureRefused.CodexModelsOverlap` and
`ConfigureRefused.UnknownFeature` change nothing. `Models` holds one
native model per Model, `{ Sol gpt-6-sol }`; a Start or
Replace naming any other model than the one configured for its Address's
Model, compared name for name, is
`StartRejected.ModelRefused.Differs.<configured>`, or
`ModelRefused.Unconfigured` when none is, before anything is written,
composed or opened, and a fresh wake launches on that model. A model,
declared or named by a launch, is the exact native model: a Codex model a
configured endpoint lists, or a Claude model's full name as its transcript
records it (`claude-haiku-5-5`); a launch naming an alias (`haiku`) or a
suffixed name (`claude-opus-5-5[1m]`) is `StartRejected.CompositionRefused`.
A permaflow is one definition: its declaration (`Configure.Permaflow`, or
an entry of `Metaflows`) carries, after its workspace, the leave-outs its
flows start without, of the harness its Model runs on, then its
`SystemModules`, composed into the system prompt, and its `PromptModules`,
composed into the first prompt,
`{ { Psyche flow Opus } None [ SkillTool WorkflowTool ] [ spirit:core:0 ] [] Regular Default OnDemand «» [] }`;
one naming a leave-out of the other harness is refused
`UnknownFeature.<leave-out>` while that harness is known (the Model's
native model configured, a Nexus declared). A Start takes its own
leave-outs, otherwise its permaflow's; a Start on another harness than
those are of is refused `LeaveOutRefused.NotForHarness`.
`Configure.Amend.{ <Address> <Change> }` changes one value of one
permaflow alone: `Workspace`, `Purpose`, `Kind` or `Continuity` replaced, or
one `SystemModule`, `PromptModule` or `LeaveOut` added with `Add` or taken
out with `Remove`, `Configure.Amend.{ { Psyche core Fable } Add.PromptModule.vision:energy-nexus:1 }`.
It is read and written back in one single-entry commit, the rest of the
store kept, and answers `Configured` with the Amend echoed,
`Configured.Amend.{ { Psyche core Fable } Add.PromptModule.vision:energy-nexus:1 }`;
a change that changes nothing writes nothing and answers the same echo.
An Address with no permaflow is refused `UnknownPermaflow`; a module added
is resolved by Curriculum first (`UnknownModule`, `CurriculumUnavailable`).
Every declaration takes effect at the permaflow's next flow. A
running Nexus serves replaced Codex endpoints from its next start. No `FLOW_` variable reaches the
Nexus's configuration. The source root selects the bounded root used for
exact-byte composition, the Codex native skill catalog working directory, the
Flow workspace root, and the project Claude skill catalog. `CODEX_HOME` and
`CLAUDE_CONFIG_DIR` select native transcript and personal-skill roots, and
`CLAUDE_ENTERPRISE_SKILLS_DIR` optionally adds the highest-precedence Claude
skill catalog (see `NON_IDEAL_AGENTS.md`). Each client invocation is one
typed struct: the target socket followed by its existing Input. The client
sends the Input alone over that socket. Every Claude hook command carries
the launching Nexus's ordinary socket in typed hook configuration.

A fresh Flow receives exactly one prompt. The harness starts with no
positional prompt, and the composed first prompt opens with the native skill
invocation. For Codex, the Nexus resolves every ordered skill name through the
bound app-server's `skills/list`, journals the selected absolute path and
exact source hash, then sends the typed skill inputs and one text, headed by
`$name` lines, in one `turn/start` on the empty Herdr-bound thread. Codex
keeps its stock base instructions; the main Flow's system-prompt bundle text
opens that one text, above the `$name` lines, and native Codex descendants
never receive it. The bundle text ends with the launch section: a
`Predecessor: <flow-id>` line when the profile names a predecessor and a
`Remembered: <ids>` line when it remembers flows, and no line otherwise.
That section is the only place the prompt names them; the `# Flow launch`
record below it carries role, harness, model, effort, Herdr session and
remote control. For Claude, it resolves the ordered enterprise, personal, and project catalogs and
journals the same typed selection. The Claude prompt is one line of at most
800 characters with no line break, because Claude Code wraps a longer line, or
any submission of four or more lines, as pasted content, and a wrapped block
expands no command. The line opens with up to five space-separated
`/<skill>` commands in the profile's order, because Claude reads commands only
at the head of a block and loads at most five stacked ones. One instruction
sentence follows: read the launch's system-prompt bundle at its path, load any
further skills through the Skill tool in order, then the goal and any source
paths. The first prompt is the brief, and the seat's first answer performs
it. At Start the Nexus writes each Claude launch
its own copy of the caller's bundle, under `~/.local/state/flow/launch-bundles/`,
named by the launch request's short form: the caller's bytes unchanged, then,
after a blank line, the same launch section Codex receives. That copy is the
`--system-prompt-file` and the path the line names; the caller's file is never
edited. Predecessor and remembered flows reach Claude through that copy;
role, model, effort and remote control through its argv, not the line. A copy
is kept while its launch can need it: it is removed once the launch is refused
and that outcome is stored, or when the Flow the launch bound is stopped (by
`Stop` or by a `Replace` reap). A Started Flow keeps its copy. A profile whose line would
break or pass 800 characters is refused (`CompositionRefused`), never
truncated. The observer requires one command record and one harness expansion
per stacked command, each carrying the same argument, then native Skill tool
calls, successful results, and expansion evidence for the rest, before
accepting the launch; a first turn wrapped as pasted content, or
recorded as plain text with no command loaded, is refused. Skill bodies are
not pasted into the composed first prompt.

After the Flow ID is claimed and registered, and before any prompt, Start
sets the canonical native title, the seat's Datom
`{ <Aspect> <Topic> <Model> <FlowId> }` (for example
`{ Field flow Terra 2712d3 }`), built from the profile's Address and the
claimed Flow ID; the native model never enters it. Claude is named at launch: it
starts with `--name <title>`, which Claude records in its transcript and shows
in the prompt box, the resume picker and the terminal title. `/rename` is a
slash command, and a launch leaving out the Skill tool starts with
`--disable-slash-commands`, so nothing is typed to title it. After it binds,
Claude is waited for with Herdr's `agent wait --until idle --until done`, then
until Herdr reports its official native session and interactive readiness;
the title is read back from the terminal title Herdr reports, and the
session's latest transcript title record, once one is written, must be the
title. A Claude start without a reserved FlowId carries no name and its title
is refused. Codex is named with `thread/name/set` and
read back with `thread/read`. The Herdr pane label is set to the same title
and read back. A refused title refuses the Start with
`NativeLaunchRefused.{ Naming <category> }`, and the Nexus log names the same
reason (`flow-nexus: herdr stage=title category=<category>`). Every native
step that refuses a Start carries its stage and fixed category the same way
(`Open`, `CodexThread`, `Spawn`, `Bind`, `Naming`, `Receipt`, `Confirm`), and
a refused registration carries its stage
(`RegistrationRefused.NativeTarget.IdentityInexact`); LaunchStatus reports
the same answer. A Codex TUI is started
as `resume <thread>` on the thread Flow opened through the app-server and gave
its first turn. That thread declares Flow's `send` tool, and the Nexus answers
its calls by queueing a request for the named Address from the thread's own
metaflow (see `DESIGN.md`). Herdr reports no agent session for such a TUI, and the
app-server names no client attached to a thread, so the pane binds to that
thread once Herdr recognizes the pane's Codex agent, exact and interactively
ready; a session Herdr does report must be the thread. Spawn records that
thread for the launch request before the TUI attaches, so a launch recovered
after its start binds to it the same way. A wake is refused while a pane runs
the ended flow's session: a pane whose agent reports it, or, for Codex,
the pane Flow recorded for the flow with Herdr recognizing a Codex agent in
it and no other session reported. The launch receipt
then reads that thread's first turn and answer. A launch refused once its
pane exists closes that pane and records the flow it registered Stopped
before it gives back its claim. Before Claude starts, the pane's shell drops inherited
`CLAUDE_CODE_CHILD_SESSION`, `CLAUDE_JOB_DIR`, `CLAUDE_CODE_SESSION_ID` and
`CLAUDE_CODE_SESSION_KIND`, so a new Flow shares no job state, and so no title,
with the process that opened its pane. Claude also starts with
`--settings '{"permissions":{"defaultMode":"bypassPermissions"}}'`: a
flag-settings default mode suppresses Claude's "make auto mode your default"
offer without writing any settings file.

The prompt is lean: no launch request ID, no hash, and no inlined source text.
Sources are named by absolute path after their bytes are checked against the
profile hash. A profile may write a source path absolute or relative: an
absolute path is taken as written, a relative one under the configured source root,
and either is read exactly when what it resolves to lies inside that root. The launch is witnessed in the native transcript of the bound session,
watched for file changes: the first turn authenticated by its body digest,
which stays in the store, every selected skill loaded in order, and, for
Claude, the seat's first answer after them at the intended model and effort
(Codex records its turn's model and effort before the input). That is the
receipt, and no caller and no human has to follow a launch. A Claude launch is remotely controllable under a
name unique to the Flow, `flow-` and the launch request ID's short form (the
first sixteen hex digits of its SHA-256; the bundle copy is named by the same
short form, so two launch requests never share either). The Flow ID is claimed from the native
session only after the harness has started, too late for the start flag.

The Codex adapter opens `codex app-server proxy`, then sends `initialize`,
`thread/start`, and `turn/start`. The returned thread is owned by the running
app-server and remains visible to remote-control clients. The legacy direct Codex starter
injects `FLOW_ID` and `FLOW_DIRECTORY`; typed launches derive their workspace
from the configured source root.

The Flow commands and the `flow-nexus.service` user unit are declared by the
operating system's Home configuration, which pins this repository's package
and writes the unit; this repository carries no installable unit.

`launcher/flow-start.mjs` reports that this package has no launcher until its
external workspace adapter has a packaged typed socket invocation contract.
Launching remains available through an explicit typed Flow client invocation.

This branch is source-published and is not an installed or live-accepted
deployment. Its final compilation and test gate must run on the configured
remote Nix builder; local fallback is not an acceptance path. Nix builds
every crate as its own derivation on the build base
(github:LiGoldragon/build-base) from the committed `Cargo.nix`. Each member
with tests has one check, `checks.<system>.<member>`, which runs the test
binaries its test build compiled and compiles nothing, and fails when no test
ran; `checks.<system>.default` is every member's check, beside `cargo-nix`,
`clippy`, `fmt`, `capsule-witnesses`, the trait-law checks, the
`request-id-migration-process` process check and the immutable contract
pins. After editing a `Cargo.toml` or `Cargo.lock`,
regenerate `Cargo.nix` with
`nix run github:LiGoldragon/build-base#crate2nix -- generate`.

## Harness-event metadata in 0.54.6

Ordinary signal-flow60 Report remains transient transport, including its optional
assistant message. The shared recording boundary projects every Report into
Memory6 RecordedHarnessEvent before append. ReadEvents uses meta-signal-flow76
HarnessEventMetadata: Stop has three positions, session option, transcript-path
option and BackgroundWork. Hook stderr likewise describes metadata and outcome.

Simplified answers still come from the exact bound native transcript and input
receipt; the hook message is never their source. Existing v5 event rows and
supported v4-era Memory upgrade before serving, preserving original row keys,
metadata and order. This is logical removal from current rows, not physical
erasure of historical database pages or external transcripts.
