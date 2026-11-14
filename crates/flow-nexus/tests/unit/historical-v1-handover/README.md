# Historical V1 producer bytes — Opus Order vuvv

Six real persisted stores were produced successfully on Prometheus, then copied here and checked with `sha256sum -c SHA256SUMS` (all six OK). No new upgrader, integration fixture, branch edit, full gate, harness or live store was run. ttkb/6k78/kick remain open.

## Producer provenance

- `4b813404/stores/`: published original Flow V1 producer **4b8134040b2dfc1ecabb333c098042aa07a115f1**, meta66 **2f0a8de1e767929e2c8e1191a8e5a8a67492ae8e**.
- `b0331a84/stores/`: published later still-V1 boundary **b0331a849fc214c1072e1c7a60c7c2a679db958c**, meta68 **91e1f45adced679a006638f1cec1b6456e5a4dc0**.
- Historical archive-producer helpers were compiled, not complete old Nexus executables. They use the original record/descriptor declarations verbatim: the whole historical module for4b813404, and the exact prefix through configuration declarations/free unconfigured function for b0331a84 (before ReadsHandoverConfiguration and its unrelated FlowStore methods). No current-consumer frozen types are used.
- Each helper starts with its producer Cargo.lock and preserves the original workspace flow-ethos path patch to that revision's archived path crate (flow-ethos0.53.2). Comparing resolved package identities found only the added helper itself; no dependency identity was added/updated. This corrects the earlier provenance report's assumption that the nested contracts actually used remote flow-ethos0.53.1: the original workspace patched that edge.
- sema-engine0.18.0 at9884905ff9c660ad97a35c524e42532d5c7bac90; rkyv0.8.18 with original explicit std/bytecheck/little_endian/pointer_width_32/unaligned features. Both helpers built in the original producer's Nix development toolchain on the builder, sharing a target directory.
- Each store has one real V1 configuration table and one record: schema version1; table flow_nexus_meta_handover_configuration; family flow-nexus-handover-configuration; schema label flow-nexus-handover-configuration-v1; record key handover.

## Store contents at each boundary

- `unconfigured.sema`: policies/archive/count all Unconfigured.
- `empty-policy.sema`: Configured empty policy vector; configured archive `/historical/empty policy/archive`; configured count7.
- `nonempty-policy.sema`: configured archive `/historical/nonempty policy/archive`, count13, two policies:
  - historicalModelOne, warning share17, reminder `Historical reminder one: preserve nested text.`, every3 minutes.
  - historicalModelTwo, warning share29, reminder `Historical reminder two: distinct bytes.`, every11 minutes.

Each file is69632 bytes. SHA256 at **both boundaries**, separately checked for every file:

- empty-policy.sema: `1b81a172809ed3b665a30c1b0e8d4cd08bb8ef7bb3f7da600a2937b067431ae8`
- nonempty-policy.sema: `c91eecaa94d5673b683b23e90d8a1e5fe40a73f59ae11a541690a9e78a536346`
- unconfigured.sema: `a0c41ecb8851527b9f525e28c9775964b48bb0e317228d1912f2e11fac17f475`

The two historical boundaries produced identical files for these inputs. This is observed byte equality between old producers, not evidence of new-consumer upgrade success.

`manifest.json` carries per-file paths, hashes, expected values and selected dependency revisions. `SHA256SUMS` lists all six paths separately.

Full scratch sources, original archived trees, helper sources/locks, build logs and originals remain at `prometheus.goldragon.criome:/tmp/flow-72d107-v1-producers-hmc7ok_z/`. Both producer runs exited0. The output stores have only been read for copying/hashing; no consumer opened them.
