use crate::generated::memory::{LaunchOutcome, StoredLaunchOutcome, SupersededHandover};
use crate::generated::starter::{LaunchedRecord, ReplacedRecord};
use signal_flow as wire;
fn old_rows() -> Vec<StoredLaunchOutcome> {
    use wire::{ReplaceRejection as R, StartRejection as S};
    let address = || wire::Address {
        aspect: wire::Aspect::Mind,
        topic: wire::Topic("archive compatibility".into()),
        model: wire::Model::Sol,
    };
    let id = || wire::FlowId("legacy-flow-long-enough-for-indirection".into());
    let launched = || LaunchedRecord {
        flow_id: id(),
        session_id: wire::SessionId("old-session-id".into()),
        origin: wire::Origin::Unbound,
    };
    let replaced = || ReplacedRecord {
        flow_id: id(),
        launched_record: launched(),
    };
    let mut v = vec![
        LaunchOutcome::Started(launched()),
        LaunchOutcome::Replaced(replaced()),
        LaunchOutcome::ReapPending(replaced()),
        LaunchOutcome::HandoverPending(launched()),
        LaunchOutcome::HandoverSuperseded(SupersededHandover {
            address: address(),
            handover_owner: wire::HandoverOwner(id()),
            launched_record: launched(),
            successor: wire::Successor(id()),
        }),
    ];
    for r in [
        R::PredecessorAbsent,
        R::UnknownPredecessor,
        R::PredecessorStopped,
        R::Locked,
    ] {
        v.push(LaunchOutcome::ReplaceRejected(r));
    }
    for r in [
        S::CompositionRefused(wire::CompositionRefusal::RecoveryPredecessorMismatch(
            wire::RecoveryPredecessorMismatch_Data {
                first_address: address(),
                second_address: address(),
                first_flow_id: id(),
                second_flow_id: id(),
                third_flow_id: id(),
            },
        )),
        S::MalformedHarnessFlags(wire::HarnessFlagsRefusal::TrailingEscape),
        S::LaunchRequestConflict,
        S::LaunchPersistenceRefused,
        S::NativeLaunchRefused(wire::NativeLaunchRefusal {
            native_launch_stage: wire::NativeLaunchStage::Spawn,
            native_launch_failure: wire::NativeLaunchFailure::NativeSessionMissing,
        }),
        S::BindingRefused(wire::BindingRefusal::NotReady),
        S::RegistrationRefused(wire::RegistrationRefusal::TranscriptBoundary),
        S::IntentPersistenceRefused,
        S::OriginUnavailable,
        S::AddressAwake,
        S::ContextRefused(wire::ContextRefusal::ComposerRejected(
            "retained rejection text".into(),
        )),
        S::LeaveOutRefused(wire::LeaveOutRefusal::Indispensable(
            wire::LeaveOut::FileTools,
        )),
        S::HarnessSpawnRefused(wire::HarnessSpawnRefusal {
            spawn_refusal_cause: wire::SpawnRefusalCause::CommandTooLong,
            opened_thread_option: None,
        }),
        S::ModelRefused(wire::ModelRefusal::Differs(wire::ModelName(
            "distinct native model".into(),
        ))),
        S::Undeclared,
        S::TopicRefused(wire::Topic("topic passed through unchanged".into())),
    ] {
        v.push(LaunchOutcome::StartRejected(r.clone()));
        v.push(LaunchOutcome::ReplaceRejected(R::LaunchRefused(r)));
    }
    v.into_iter()
        .enumerate()
        .map(|(n, launch_outcome)| StoredLaunchOutcome {
            launch_request_id: wire::LaunchRequestId(format!("launch-for-archive-{n}")),
            launch_outcome,
        })
        .collect()
}

// The 41 frozen rows were produced with Flow 0c0238f6's stored record
// declarations and signal-flow da01bb0533 (0.69.0), not with this reader.
// Every prior launch and rejection tag has a witness, including nested
// LaunchRefused, the widest composition failure, and out-of-line strings.
#[test]
fn reopens_prior_v11_launch_outcomes_without_shifting_or_rewriting_them() {
    use crate::store::{FlowStore, OpensFlowStore, RecordsLaunchOutcome};
    use base64::Engine as _;
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("v11-launch-outcomes.sema");
    drop(FlowStore::open(&path).unwrap());
    let old = include_str!("fixtures/launch_outcomes_v11_signal_069.base64")
        .lines()
        .map(|s| base64::engine::general_purpose::STANDARD.decode(s).unwrap())
        .collect::<Vec<_>>();
    let expected = old_rows();
    assert_eq!(old.len(), expected.len());
    {
        let db = redb::Database::open(&path).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut rows = tx
                .open_table(redb::TableDefinition::<String, &[u8]>::new(
                    "flow_nexus_launch_outcomes",
                ))
                .unwrap();
            for (bytes, row) in old.iter().zip(&expected) {
                rows.insert(row.launch_request_id.0.clone(), bytes.as_slice())
                    .unwrap();
            }
        }
        tx.commit().unwrap();
    }
    for _ in 0..2 {
        let store = FlowStore::open(&path).unwrap();
        for (bytes, row) in old.iter().zip(&expected) {
            assert_eq!(
                store
                    .launch_outcome(&row.launch_request_id.0)
                    .unwrap()
                    .as_ref(),
                Some(&row.launch_outcome)
            );
            assert_eq!(
                rkyv::to_bytes::<rkyv::rancor::Error>(row)
                    .unwrap()
                    .as_slice(),
                bytes.as_slice()
            );
        }
    }
}
