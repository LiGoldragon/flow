# Flow branch consolidation, 2026-10-13

This commit joins all inventoried branch ancestry and records semantic resolutions below. It is source integration; Field owns combined compilation, tests and deployment. No production Start or old-store conversion is claimed.

## Current paired contract

Flow requires one `Start.{ ordinary-socket meta-socket store-path }`. `--configure-only` with that same value opens only recognized/fresh Memory and serves one typed `Configure.Nexus` request, then exits. `Configure.Model`, `.Threshold`, and `.Module` use the same typed configuration family. Configuration is idempotent for an identical value and refuses conflicting replacement. Production startup requires the Nexus snapshot and uses the explicit store and sockets; no HOME store fallback. Exact full catalogue and Sema counter layout are checked before writer open. Existing stores outside this cohort refuse, with no migration implied.

Message main c52fc9ccd483964233c2d1e0ba875ec8ade2dc6f takes no arguments and uses its default ordinary/meta sockets and Flow ordinary peer. Its fresh Standard2 store must be selected/accounted separately; existing October3 state is not opened by assumption. Both consumers use signal-flow d1358d396e96934795a5b2ae2192499b3eaff9c3 and meta-signal-flow 6d07b7bb077da8755eae7d1146d9dfd64d7b7d7c. Flow patches the shared leaf to its exact matching workspace definition.

The new ordinary adapter durably queues requests and consumes leases atomically. It does not claim a queue-drain/native-wake implementation or successful native delivery. Existing explicit DeliverMessage/Start paths remain separate.

## Why branches remained apart

The retained order for the typed-store slice said: “Do not silently land branch/main, relabel worker prompts, reuse held old bundle, duplicate passed jobs, or create a source writer.” I failed to close the landing handoff after checks. That order is superseded by the living’s “work on main, merge everything; unmergeable branches are a failure to diagnose.”

For the other historical branches, no exact originating instruction has been established. Commit messages and lock comments are evidence of contents, not a substitute for that missing provenance. The replacement_operation header explicitly said its dispatcher had another owner and it was not wired; that explains the source boundary but does not identify an authorizing instruction. No invented skill quotation is assigned.

Proposed correction for Core’s book, not a skill landing: “A checked change is integrated into main by its existing owner as part of the same task. A check branch is temporary transport, never the completion state. Conflicts are reconciled against current contracts; obsolete behavior is named explicitly, and publication is not deployment.”

## Branch-by-branch resolution

### field-pending-recovery-753e69

Tip `481b6909ef08dd7d2573cf73ae1df1024fb9b7b2`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/pending_start_recovery.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### field/flow-ethos-compile-6aa881f125da

Tip `6aa881f125da085309d2bf50b3ed536fc46e3234`.

Shared leaf producer ancestry already integrated by dfe; current exact Library generation is inherited from StorePath child074. No hand-authored generated Rust or regeneration.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M Cargo.toml; A crates/flow-ethos/Cargo.toml; A crates/flow-ethos/build.rs; A crates/flow-ethos/ethos/library.ethos; A crates/flow-ethos/src/generated/library.rs; A crates/flow-ethos/src/generated/mod.rs; A crates/flow-ethos/src/lib.rs.

### field/flow-ethos-generation-41c6f836f9e9

Tip `a3173348498ecce704359dd42abaea763c09705b`.

Shared leaf producer ancestry already integrated by dfe; current exact Library generation is inherited from StorePath child074. No hand-authored generated Rust or regeneration.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; A crates/flow-ethos/Cargo.toml; A crates/flow-ethos/build.rs; A crates/flow-ethos/ethos/library.ethos; A crates/flow-ethos/src/generated/library.rs; A crates/flow-ethos/src/generated/mod.rs; A crates/flow-ethos/src/lib.rs.

### field/flow-ethos-generation-fc358a5484cc

Tip `fc358a5484cc6466bbf086dbb4f31e0c2149a301`.

Shared leaf producer ancestry already integrated by dfe; current exact Library generation is inherited from StorePath child074. No hand-authored generated Rust or regeneration.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; A crates/flow-ethos/Cargo.toml; A crates/flow-ethos/build.rs; A crates/flow-ethos/ethos/library.ethos; A crates/flow-ethos/src/generated/mod.rs; A crates/flow-ethos/src/lib.rs.

### field/flow-storepath-source-fa8a85c44fba

Tip `6bfbbfb457fcc0cfe258f10beabe82e11b4bc6da`.

Integrate exact generated Library/Memory pair, schema2 StorePath, typed Configure.Nexus/Model/Threshold/Module and exact current producer pins. Port measured engine-counter physical-type correction, typed refusal, SYSTEM consumer and test compatibility. Remove obsolete ConfigureFiles frontend whose producer types no longer exist.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; A crates/flow-ethos/Cargo.toml; A crates/flow-ethos/build.rs; A crates/flow-ethos/ethos/library.ethos; A crates/flow-ethos/src/generated/library.rs; A crates/flow-ethos/src/generated/mod.rs; A crates/flow-ethos/src/lib.rs; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/build.rs; A crates/flow-nexus/ethos/library.ethos; A crates/flow-nexus/src/generated/library.rs; M crates/flow-nexus/src/lib.rs; M crates/flow-nexus/src/main.rs; M crates/flow-nexus/src/peer.rs; M crates/flow-nexus/src/store.rs; M crates/flow-nexus/src/store/delivery.rs; M crates/flow-nexus/src/tests/launching_nexus.rs; M crates/flow-nexus/tests/default_start.rs; M crates/flow/tests/default_socket.rs.

### field/storepath-flow-9bdf3cd3cf64

Tip `074437a66de48a5ef3e7af6a14407652c0ab861d`.

Integrate exact generated Library/Memory pair, schema2 StorePath, typed Configure.Nexus/Model/Threshold/Module and exact current producer pins. Port measured engine-counter physical-type correction, typed refusal, SYSTEM consumer and test compatibility. Remove obsolete ConfigureFiles frontend whose producer types no longer exist.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M Cargo.toml; A crates/flow-ethos/Cargo.toml; A crates/flow-ethos/build.rs; A crates/flow-ethos/ethos/library.ethos; A crates/flow-ethos/src/generated/library.rs; A crates/flow-ethos/src/generated/mod.rs; A crates/flow-ethos/src/lib.rs; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/build.rs; A crates/flow-nexus/ethos/library.ethos; A crates/flow-nexus/src/generated/library.rs; M crates/flow-nexus/src/lib.rs; M crates/flow-nexus/src/main.rs; M crates/flow-nexus/src/peer.rs; M crates/flow-nexus/src/store.rs; M crates/flow-nexus/src/store/delivery.rs; M crates/flow-nexus/src/tests/launching_nexus.rs; M crates/flow-nexus/tests/default_start.rs; M crates/flow/tests/default_socket.rs; M flake.nix.

### flow-ethos-producer-0c85a3

Tip `9245d88a972e11930010fd3e3074ed5395ed544a`.

Shared leaf producer ancestry already integrated by dfe; current exact Library generation is inherited from StorePath child074. No hand-authored generated Rust or regeneration.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; A crates/flow-ethos/Cargo.toml; A crates/flow-ethos/build.rs; A crates/flow-ethos/ethos/library.ethos; A crates/flow-ethos/src/generated/mod.rs; A crates/flow-ethos/src/lib.rs.

### flow/00f95a-system-prompt-start

Tip `aae2b197ce841a41a95509a2e73683a94485927a`.

Resolve the old six-slot prompt_modules assembly in favor of current checked SYSTEM-file composition and resolver expansion/filtering. Its universal Spirit-as-native-USER input contradicts the later explicit SYSTEM-only ruling; it is not restored. Current native binding/transcript adapters remain the source of evidence, rather than the old standalone normalization facade.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M README.md; M crates/flow-nexus/src/claude.rs; M crates/flow-nexus/src/codex.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow/src/main.rs; A result.

### flow/e798f3-launch-adapter

Tip `fd1dae690d4cfb4f2871df5f457eca7117e0289b`.

Resolve the old six-slot prompt_modules assembly in favor of current checked SYSTEM-file composition and resolver expansion/filtering. Its universal Spirit-as-native-USER input contradicts the later explicit SYSTEM-only ruling; it is not restored. Current native binding/transcript adapters remain the source of evidence, rather than the old standalone normalization facade.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M README.md; M crates/flow-nexus/src/claude.rs; M crates/flow-nexus/src/codex.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow/src/main.rs; A result.

### flow/refresh-runtime-47764b

Tip `22c4141be5b4a51774a695f70f687136e38d4ad4`.

Retain current typed Replace/Operation path, kernel peer authorization, persisted replacement, stopped predecessor and successor routing hold. Do not reinstate removed RefreshRequest/CallerProof/RefreshCompletion wire variants or the old privileged flow-meta command. Old retirement archive receipt and SO_PEERPIDFD facade remain historical source, not a claim they are implemented by current Replace. Exact process/pane close and unknown-route refusal remain current behavior; no automatic refresh service is enabled.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M Cargo.toml; M crates/flow-meta/src/main.rs; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/claude.rs; M crates/flow-nexus/src/herdr.rs; A crates/flow-nexus/src/herdr/refresh.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/refresh.rs; A crates/flow-nexus/src/retirement.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs.

### flow/retry-created-pane-00f95a

Tip `e41621af8edbf5f1b8c2e12a9c866119a90ad5d2`.

Port durable created-pane table, one-shot spawn reservation, reopen/orphan test, and same-pane Codex observation to current Operation-based launch. A repeated request never spawns a second seat. Claude retains pending refusal rather than guessing a lost preclaimed identity. The new table is declared in the exact running Memory catalogue and Engine family inputs.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M crates/flow-nexus/src/lib.rs; M crates/flow-nexus/src/store.rs.

### mind-astra-independent-fixture-repair-f5a74e

Tip `d4107dd25ed740e08bd8458a3daceff06d8b44a8`.

Current SYSTEM fixtures already create actual temporary bundle files and assert the SYSTEM handoff; keep those stronger assertions rather than obsolete hard-coded /tmp paths.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M crates/flow-nexus/src/herdr/launch.rs; M crates/flow-nexus/src/lib.rs.

### mind-medium-delivery-2c61af

Tip `b83bfbddc2fd34829d0e3114b9f667339bd90d38`.

Reconcile former delivery-permit/verified-binding API against the current shared leaf and Message Identify/Lock/Deliver/Release protocol. A bound Message executable plus kernel peer is required; queued request and exact lease removal commit atomically. Queued means durable queue only, never a native-delivery or wake claim. Old tester-tier selection is not restored over explicit current profile policy.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/claude.rs; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### mind-medium-delivery-dispatch-2c61af

Tip `6d23cd7ec2148eb08c2bd062be2fd2fd35cc2fe5`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### mind-medium-flow-preacquire-auth-2c61af

Tip `c77ccd937831140783657877882a3373297ceb9a`.

Reconcile former delivery-permit/verified-binding API against the current shared leaf and Message Identify/Lock/Deliver/Release protocol. A bound Message executable plus kernel peer is required; queued request and exact lease removal commit atomically. Queued means durable queue only, never a native-delivery or wake claim. Old tester-tier selection is not restored over explicit current profile policy.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### mind-medium-observe-sessions-2c61af

Tip `d66bbea25c67547386fcb1387e9e1ee1605e8e7f`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M README.md; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow/src/main.rs; A result.

### mind-medium-tester-selector-2c61af

Tip `5afb64f2cdccaa096ffa413cba1f6818506e6f15`.

Reconcile former delivery-permit/verified-binding API against the current shared leaf and Message Identify/Lock/Deliver/Release protocol. A bound Message executable plus kernel peer is required; queued request and exact lease removal commit atomically. Queued means durable queue only, never a native-delivery or wake claim. Old tester-tier selection is not restored over explicit current profile policy.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### mind-medium-typed-input-proposal-2c61af

Tip `4660aa0bbb14e3e403b21276f3e61dd7027387f5`.

Keep the current typed single-Datom CLI, explicit profile/model and SYSTEM bundle contracts. Do not reintroduce the removed delivery BindingRegistration API or old tester model-selection policy. Native input remains assembled from qualified current profile and resolver roots, with SYSTEM modules filtered from USER.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### mind-sol-00f95a-confirm

Tip `18b03f5e8b9cb011467090d25a8c9bfeb334301e`.

Reject the older caller-supplied existing-flow receipt/title confirmation interface. Current MetaBindExisting independently validates process/cwd/pane and leaves registration unconfirmed; ordinary launch confirmation requires its persisted prompt receipt. Do not add removed MetaConfirmExisting producer variants or fabricate a module manifest. The later historical fail-closed correction is retained as history, not downgraded to its unsafe ancestor.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M crates/flow-nexus/src/lib.rs; M crates/flow-nexus/src/store.rs.

### mind-sol-00f95a-confirm-runtime

Tip `d5e1305bf22f8dae3a24457f844ddcd91ad04680`.

Reject the older caller-supplied existing-flow receipt/title confirmation interface. Current MetaBindExisting independently validates process/cwd/pane and leaves registration unconfirmed; ordinary launch confirmation requires its persisted prompt receipt. Do not add removed MetaConfirmExisting producer variants or fabricate a module manifest. The later historical fail-closed correction is retained as history, not downgraded to its unsafe ancestor.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M Cargo.toml; M crates/flow-nexus/src/herdr.rs; M crates/flow-nexus/src/lib.rs; M crates/flow-nexus/src/store.rs.

### mind-sol-00f95a-recovery

Tip `87e41bc664d18e5d7a432f52ce40cf2d6ed05a67`.

Retain recovery-manifest admission and synthetic fixtures as flow-recovery source, structured into traits. It still exits with an explicit missing Lojix adapter; no recovery service is declared or enabled. Unused old Lojix signal pins are not introduced into the current Flow/Message protocol graph.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: A crates/flow-nexus/src/bin/flow-recovery.rs.

### mind-sol-00f95a-recovery-manifest

Tip `279fcdc07a25e3f10f773927ff470ee7f5c8fc2c`.

Retain recovery-manifest admission and synthetic fixtures as flow-recovery source, structured into traits. It still exits with an explicit missing Lojix adapter; no recovery service is declared or enabled. Unused old Lojix signal pins are not introduced into the current Flow/Message protocol graph.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M Cargo.toml; M crates/flow-nexus/Cargo.toml; A crates/flow-nexus/src/bin/flow-recovery.rs.

### native-main-replacement-eb7bae

Tip `5179906c26ef327d40d850fd1601bc6a14ecc434`.

Resolve the experimental, deliberately unwired replacement_operation and removed BindingRegistrationSubmission/VerifiedBindingRegistration types in favor of the current generated Operation/Replace implementation. Keep persisted request identity, predecessor stop, unknown-close refusal and successor unroutability until reaping. Do not introduce a second replacement state machine or caller-authored binding authority.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.lock; M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; A crates/flow-nexus/src/replacement_operation.rs; M crates/flow-nexus/src/store.rs; A crates/flow-nexus/tests/replacement_operation.rs; M crates/flow/src/main.rs; A result.

### night-messaging-0ab019

Tip `e387576f5858192cd4ae7b9b4bccbb38aa742b19`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M README.md; M crates/flow/src/main.rs; A result.

### night-messaging-flow-delivery-store-f72ab7

Tip `92d0694f37f828c303d8daec24f9c4de3350c05d`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### night-messaging-flow-lock-keys-f72ab7

Tip `cd752e0f25f82cf1c2c315041b3496ebaece41f8`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### night-messaging-flow-lock-resolver-f72ab7

Tip `db85d3294367c0357d81dfb1422da6e9d172100a`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; M flake.nix; A nix/resolve-cargo-lock.nix; A result.

### night-messaging-flow-meta-v3-pin-f72ab7

Tip `d9427ec8133c9cd756429199f8a4f0d3b40be7d0`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### night-messaging-flow-producer-pins-f72ab7

Tip `b3592cf6c3f08906ea4da2f7faafe9aa698f8b02`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### night-messaging-flow-runtime-f72ab7

Tip `cc9e7f9d99cc32d6ddd44006a83152c1c898c87c`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### night-messaging-flow-store-ownership-f72ab7

Tip `bb36630b85fdd064963596fd10bb3d7dcf97e115`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### night-messaging-flow-store-ownership-v2-f72ab7

Tip `aa13ff690ce4ab886725180f5067c5c765246874`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.

### night-messaging-flow-tombstone-f72ab7

Tip `87bd74bd4c995d8947440e78d18c0cd6a9ee9597`.

Retain the pure pending-attempt decision helper and its fail-closed replay tests. Port ordinary peer authorization and exact lease consumption into the current Address/Process/Request protocol. Do not restore old permit rows, old meta producer pins, or the tracked result build artifact. Current explicit Memory recognition owns exclusive-writer and foreign-store refusal.

Original instruction/skill: not established, except the typed-store publication hold quoted above. Historical reason for keeping this branch apart: unknown unless explicitly stated above. Original commits remain reachable; no branch deletion.

Changed paths from its common ancestor: M Cargo.toml; M README.md; M crates/flow-nexus/Cargo.toml; M crates/flow-nexus/src/codex.rs; M crates/flow-nexus/src/lib.rs; A crates/flow-nexus/src/prompt_modules.rs; M crates/flow-nexus/src/store.rs; M crates/flow/src/main.rs; A result.
