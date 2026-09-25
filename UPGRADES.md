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
