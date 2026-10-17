# Non-Ideal Agents Registry

Known non-idealities in this repository. Ordinary rules and the intended
shape live in `README.md` and `DESIGN.md`.

## Claude's home and enterprise skill catalog come from the environment

Every value the Nexus runs on comes from its typed default configuration
(`flow-defaults`, anchored on `HOME` and `XDG_RUNTIME_DIR`) and changes only
over the meta socket's `Configure`, with two exceptions read in
`HerdrCli::default`:

- `CLAUDE_CONFIG_DIR` selects Claude's home: its transcript root
  (`projects/`) and its personal skill catalog (`skills/`). Unset, it is
  `$HOME/.claude`.
- `CLAUDE_ENTERPRISE_SKILLS_DIR` adds the highest-precedence Claude skill
  catalog.

Both are Claude Code's own variables, which the Nexus's Claude children read
too, and meta-signal-flow's `Configuration` has no field for either, so no
`Configure` can carry them. No test needs them.

**Proper fix:** meta-signal-flow's `Configuration` gains Claude's home and
its skill catalogs, `flow-defaults` derives their defaults from `HOME`, and
these two reads go.

## Clients choose a Nexus by `FLOW_SOCKET` and `FLOW_META_SOCKET`

The clients reach the default sockets under the caller's
`XDG_RUNTIME_DIR`. `FLOW_SOCKET` and `FLOW_META_SOCKET` name another Nexus's
socket instead: the deployment's next-slot wrappers (`flow-next`,
`flow-next-meta`) select the next Nexus this way, and a Nexus whose sockets
`Configure` moved is reached only so. They configure nothing in the Nexus.
No test needs them.

**Proper fix:** undecided. A wrapper that sets `XDG_RUNTIME_DIR` to its
slot's runtime anchor reaches the same path with no Flow variable, but a
moved socket would then have no way to be named.

## A queued request names no sender

A metaflow's queue holds `Request` values, and `Request` (flow-ethos) has no
sender. A Deliver from Message checks the Lock's Sender and then queues the
request alone, and a send-tool call is queued the same way: the sender Flow
attributed it to is in the call's record and in the tool's answer, but the
recipient reads only `Order.«body»`. No test needs it.

**Proper fix:** the queued item carries its Sender (flow-ethos `Metaflow`
queue of `{ Sender Request }`), which changes the signal-flow and
meta-signal-flow contracts and the metaflow store family.

