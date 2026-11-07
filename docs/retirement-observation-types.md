# Retirement observation type packet

Status: proposed until the source commits are immutable.

The ordinary Signal extends its existing `Observe` subscription. It does not add a second stream.

```ethos
ObserveSelection.[ Launch.LaunchRequestId
                   Agent.FlowId
                   Retirement.Address ]

RetirementObserved.RetirementSnapshot
RetirementChanged.RetirementChange
RetirementObservationFailed.RetirementObservationFailure

HandoverGeneration.Integer
HandoverGenerationKey.{ Address HandoverGeneration Predecessor.FlowId Successor.FlowId }
GitCommit.String
GitBlob.String
ArchiveDocumentPath.String
HandoverArchiveIdentity.{ GitCommit ArchiveDocumentPath GitBlob }

RetirementSnapshot.{ Address Vector<HandoverGenerationKey> }
ArchiveCommitted.{ HandoverGenerationKey HandoverArchiveIdentity }
RetirementChange.[ GenerationClosed.HandoverGenerationKey
                    ArchiveCommitted
                    GenerationRemoved.HandoverGenerationKey ]
RetirementObservationFailure.[ Overflow MissingEvidence.String ]
```

`RetirementObserved` is the state at subscription attachment: the closed generation identities then held for the selected Address. It does not claim the history before attachment. Each later committed change is pushed once in FIFO order. `RetirementObservationFailed` is terminal and says the observation is incomplete.

The internal keyed removal outcome is:

```rust
enum HandoverRemovalOutcome {
    Removed,
    AlreadyAbsent,
    IdentityMismatch,
}
```

Only `Removed` emits `GenerationRemoved`. `AlreadyAbsent`, `IdentityMismatch`, a failed durable mutation, or unverifiable archive evidence emits no removal event; an attached observation receives terminal `MissingEvidence` for an incomplete retirement attempt.

The publisher returns the actual Git identity it verified after publication: the commit at `HEAD`, the relative document path, and the blob object resolved by `HEAD:<path>`. It reads the blob back and compares its bytes with the requested immutable document, including when the document already existed tracked and clean.

Subscription attachment and the initial snapshot share `FlowStore::handover_write_gate`. Closing a generation writes it and enqueues `GenerationClosed` under that gate. Git publication runs outside the gate. The completion operation reacquires the gate, rechecks the exact generation identity, and performs archive-evidence enqueue plus keyed removal as one critical section. An observer therefore attaches either before both completion changes or after the generation is absent, never between archive evidence and removal.

Each subscription owns a bounded ephemeral FIFO and a terminal-failure latch. Producers enqueue under the handover gate without blocking; socket delivery removes frames and writes them outside the gate. Capacity loss latches `Overflow`. Disconnect, Nexus restart, partial framing, and EOF are incomplete observations at the CLI and return a nonzero result. No retirement observation row, receipt family, durable history, or Memory migration is added.
