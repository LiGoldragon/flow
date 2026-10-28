# Batch 18 configuration-view callgraph

Primary 1838 makes `ConfiguredUse` the owner of one complete committed generation. This receipt classifies every production acquisition returned by:

```
rg -n '\.configured_use\(\)' crates/flow-nexus/src -g'*.rs' | grep -v '/tests/'
```

## Acquisition boundaries

- `lib.rs`: ordinary/meta frame receipt, public query dispatch, socket timeout, startup reconciliation, and native hook entry.
- `performing.rs`: the public `perform` operation boundary. Every nested operation receives `perform_configured`'s view.
- `launching.rs`: interrupted-launch reconciliation and refused-launch lifecycle reconciliation. Start, requirements, confirm, settle, replace and reap have only configured internal forms.
- `waking.rs`: public wake/drain/refresh/replace, native lifecycle event, startup recovery, and each next queued delivery attempt. Nested requirements, fresh launch, hand-over, delivery reconciliation, settlement and successor refresh receive the selected view.
- `sending.rs`: public declared launch, bind, direct-send lease and direct-send completion boundaries. A queued attempt may select the then-current view.
- `send_tool.rs`: one received tool call, one responder item, or one responder lifecycle event.
- `metaflow.rs`: one peer-identification or peer-dispatch request.
- `observe_agent.rs`: one observation request or the next event-driven observation iteration.
- `compacting.rs`: one native compaction event or the next Codex context signal.
- `codex_turns.rs`: one native event or the next observation iteration.

## Removed internal acquisition APIs

The unrestricted launch convenience methods `start`, `settled`, `settle`, `replace`, `reap`, `confirmed`, and `resume_reaping` are removed. The unrestricted requirement and successor-refresh methods are removed. Their internal callgraph accepts `&ConfiguredUse`: requirements → fresh launch → compose/open/spawn/bind/title/submit → confirm → settle → replace/reap; queued drain → target/readiness/placement/reconciliation; lifecycle refresh → successor replace → original-resource close.

A resource view carries the one shared ownership registry. Current configuration selects new I/O; the registry selects the exact original backend and pidfd for an existing resource. Missing or mismatched authority is a typed refusal.

## Herdr-only acquisition boundaries

A second mechanical scan is required:

```
rg -n 'herdr_operation_view\(\)' crates/flow-nexus/src -g'*.rs' | grep -v '/tests/'
```

Its remaining sites are the public caller, delivery, peer and repair effects. Nested paths use their `_with` forms and the `HerdrCli` already owned by the enclosing `ConfiguredUse`; for example metaflow peer identification calls `bound_caller_with(&configured.herdr, ...)` rather than reacquiring a Herdr view.

The provider remains crate-visible while Rust modules hold several external entry implementations. Compile enforcement is presently at the removed unrestricted effect methods and their configured-only nested callgraph; crate-level provider visibility is recorded here rather than represented as a completed stronger visibility boundary.
