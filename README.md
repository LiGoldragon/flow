# Flow Nexus

The Flow Nexus owns typed Flow dispatch, origin identity, and restart authority. The ordinary `flow` client and privileged `flow-meta` client each accept one Datom value and send only length-framed rkyv Signal on their respective sockets. The Codex adapter is deliberately separate: it drives the daemon-owned app-server with `codex app-server proxy`, `thread/start`, then `turn/start`; it never invokes `codex exec`. The supplied app-server survey did not witness proxy framing, so the newline JSON-RPC framing in the adapter remains an integration limitation.
