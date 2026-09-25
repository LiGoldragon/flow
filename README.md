# Flow Nexus

Flow Nexus starts, restarts, sends to, stops, lists, and resolves flows. `flow-nexus` is the
no-argument long-running process, `flow` is its ordinary client, and
`flow-meta` is its privileged client. The Nexus persists Flow identity and
policy in one Sema store, and its sockets carry only length-prefixed rkyv
Signal archives.

The wire contracts are independent repositories:

- `signal-flow` defines `Start`, provenance-authorized `Restart`, `Send`,
  `Stop`, `List`, and `ResolveRecipient` for Message Nexus routing.
- `meta-signal-flow` defines `Configure` and reset-credit consumption.

The ordinary client accepts exactly one inline `Query` Datom from the
`signal-flow` contract:

```sh
flow '<one inline Query datom>'
```

The basic pane operations use the same typed edge:

```sh
flow 'Send.{ 00f95a «continue with the implementation» }'
flow 'Stop.00f95a'
flow 'List.{}'
```

`Send` revalidates the stored Flow claim and exact Herdr agent, pane,
terminal, harness, interactive readiness, and session before prompting that
pane. An Active row returns `Sent` when Herdr accepts the prompt. A Pending
row receives a unique harmless marker in its prompt. Flow waits for that marker
on the exact target pane, reads the pane, requires the marker in the read
output, and revalidates the pane and terminal before promoting the row. The
`Presented` receipt carries the Flow ID, pane ID, exact marker, and pane-read
Unix time. This is presentation evidence; only the target's own later response
can witness Read. A prompt queued to an already working pane cannot confirm a
Pending row. `Stop` persists the
Stopped lifecycle only after `herdr pane close` succeeds for the revalidated
pane. `List` returns all durable rows, sorted by Flow ID, including Pending and
Stopped rows.

`Start` carries a typed `LaunchProfile` plus an `OriginClue`. The origin is a
caller claim; its text does not authenticate the caller. A profile names its
ordered source descriptors and exact SHA-256 values, ordered native skill
names, aspect, power, harness, model, effort, predecessor, remembered flows,
Herdr target session, and first instruction. Profile producers should use the
generated `signal-flow` Datom types rather than assembling positional text.
The old `flow start ...`, `flow restart ...`, and `flow resolve ...` argument
forms are rejected.

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

The current binary uses these store and socket defaults:

- store: `/home/li/.local/state/flow/flow.sema`
- ordinary socket: `/run/user/1001/flow/flow.sock`
- meta socket: `/run/user/1001/flow/flow-meta.sock`
- Codex control socket:
  `/home/li/.codex/app-server-control/app-server-control.sock`

`FLOW_SOURCE_ROOT` is required when starting `flow-nexus`. It selects the
bounded source root used for exact-byte composition, the Codex native skill
catalog working directory, the Flow workspace root, and the project Claude
skill catalog. `CODEX_HOME` and `CLAUDE_CONFIG_DIR` select native transcript
and personal-skill roots. `CLAUDE_ENTERPRISE_SKILLS_DIR` optionally adds the
highest-precedence Claude skill catalog. The ordinary client uses
`FLOW_SOCKET` when set.

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
`Remembered: <ids>` line when it remembers flows, and no line otherwise. For
Claude, it resolves the ordered enterprise, personal, and project catalogs and
journals the same typed selection. The Claude prompt is one line of at most
800 characters with no line break, because Claude Code wraps a longer line, or
any submission of four or more lines, as pasted content, and a wrapped block
expands no command. The line opens with up to five space-separated
`/<skill>` commands in the profile's order, because Claude reads commands only
at the head of a block and loads at most five stacked ones. One instruction
sentence follows: read the launch's system-prompt bundle at its path, load any
further skills through the Skill tool in order, then the goal and any source
paths, then the receipt request. At Start the Nexus writes each Claude launch
its own copy of the caller's bundle, under `~/.local/state/flow/launch-bundles/`,
named by the launch request's short form: the caller's bytes unchanged, then,
after a blank line, the same launch section Codex receives. That copy is the
`--system-prompt-file` and the path the line names; the caller's file is never
edited. Predecessor and remembered flows reach Claude through that copy;
role, model, effort and remote control through its argv, not the line. A profile whose line would
break or pass 800 characters is refused (`CompositionRefused`), never
truncated. The observer requires one command record and one harness expansion
per stacked command, each carrying the same argument, then native Skill tool
calls, successful results, and expansion evidence for the rest, before
accepting the target receipt; a first turn wrapped as pasted content, or
recorded as plain text with no command loaded, is refused. Skill bodies are
not pasted into the composed first prompt.

After the Flow ID is claimed and registered, and before any prompt, Start
sets the canonical native title, `<Aspect>V2.{ <Model> <FlowId> }` (for
example `PsycheV2.{ Fable 38de5b }`), with the model's display name taken from
the exact model identifier and an unmapped identifier refused. Claude is
renamed with its own `/rename` and read back from the terminal title Herdr
reports and the session's transcript title record; Codex is named with
`thread/name/set` and read back with `thread/read`. The Herdr pane label is
set to the same title and read back. A failed readback refuses the Start with
`BindingRefused`. Before Claude starts, the pane's shell drops inherited
`CLAUDE_CODE_CHILD_SESSION`, `CLAUDE_JOB_DIR`, `CLAUDE_CODE_SESSION_ID` and
`CLAUDE_CODE_SESSION_KIND`, so a new Flow shares no job state, and so no title,
with the process that opened its pane. Claude also starts with
`--settings '{"permissions":{"defaultMode":"bypassPermissions"}}'`: a
flag-settings default mode suppresses Claude's "make auto mode your default"
offer without writing any settings file.

The prompt is lean: no launch request ID, no hash, and no inlined source text.
Sources are named by absolute path after their bytes are checked against the
profile hash. The requested receipt is the fixed line
`FLOW_LAUNCH_RECEIPT_V2`; the observer binds it to the launch by native
session, transcript cursor, and the authenticated first turn, whose body
digest stays in the store. A Claude launch is remotely controllable under a
name unique to the Flow, `flow-` and the launch request ID's short form (the
first eight hex digits of its SHA-256). The Flow ID is claimed from the native
session only after the harness has started, too late for the start flag.

The Codex adapter opens `codex app-server proxy`, then sends `initialize`,
`thread/start`, and `turn/start`. The returned thread is owned by the running
app-server and remains visible to remote-control clients. Restart resumes that
thread and starts its next turn only when the caller's provenance Flow ID and
harness session equal the registered target. The legacy direct Codex starter
injects `FLOW_ID` and `FLOW_DIRECTORY`; typed launches derive their workspace
from the configured source root.

For a user service installation, build the workspace in release mode, install
the three binaries into `~/.local/bin`, copy
`deployment/flow-nexus.service` into `~/.config/systemd/user`, then enable
`flow-nexus.service`. Declarative environments should package the same unit
and binaries instead of retaining this local copy.

This branch is source-published and is not an installed or live-accepted
deployment. Its final compilation and test gate must run on the configured
remote Nix builder; local fallback is not an acceptance path. Nix exposes
the full `checks.<system>.default` gate plus focused
`flow-v5-row-preservation`, `flow-herdr-route-durability`,
`flow-stale-route-unavailable`, `flow-conflicting-registration-refusal`,
`flow-herdr-registration-binding`, and
`flow-native-resolution-serialization` checks.
