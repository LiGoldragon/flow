//! Emergency recovery through the running Memory: the intent a successor
//! is owed and the recovery text are read from the Flow store, through the
//! one source a configured Nexus composes with.

use super::configuring::ConfiguredStore;
use super::handover_configuration::APPROVED_RECOVERY_TEXT;
use crate::composition::{
    ComposesInRoot, HandoverRoot, HarnessedProfile, LaunchComposer, OpensLaunchComposer,
};
use crate::generated::memory::{
    EmergencyRefreshWithoutHandover_Data, Predecessor, SuccessionCause, SuccessorIntent,
};
use crate::store::successor::OwesSuccessors;
use crate::store::{FlowStore, OpensFlowStore};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::{LaunchProfile, LeaveOut};
use std::sync::Arc;

fn emergency_intent(
    profile: &LaunchProfile,
    path: Option<signal_flow::TranscriptPath>,
) -> SuccessorIntent {
    let predecessor = profile
        .flow_id_option
        .clone()
        .expect("a successor's profile");
    SuccessorIntent {
        address: profile.address.clone(),
        predecessor: Predecessor(predecessor.0.clone()),
        succession_cause: SuccessionCause::EmergencyRefreshWithoutHandover(
            EmergencyRefreshWithoutHandover_Data {
                flow_id: predecessor,
                transcript_path_option: path,
            },
        ),
        launch_request_id: profile.launch_request_id.clone(),
    }
}

/// The composer a configured Nexus runs: one Memory source for both the
/// handover and the recovery (`ConfiguredRunner`).
fn memory_composer(store: &Arc<FlowStore>) -> LaunchComposer {
    let source = Arc::new(crate::MemoryHandoverSource(store.clone()));
    LaunchComposer::with_context(
        crate::context::ContextComposer(Arc::new(crate::context::FixtureComposer::from(
            &[
                ("spirit:core:0", "Spirit body."),
                ("intent:flow:0", "Intent body."),
                ("operation:main-flow:0", "Main flow body."),
                ("knowledge:flow:0", "Knowledge body."),
            ][..],
        ))),
        source.clone(),
    )
    .with_handover_source(source)
}

fn composed_body(
    composer: &LaunchComposer,
    profile: LaunchProfile,
    root: HandoverRoot<'_>,
) -> String {
    composer
        .compose_in_root(
            HarnessedProfile {
                remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                harness_kind: signal_flow::HarnessKind::Claude,
                launch_profile: profile,
            },
            Vec::<LeaveOut>::new(),
            root,
            &[],
        )
        .expect("the successor composes")
        .first_prompt_payload
        .first_prompt_body
        .0
}

/// Gap 8. Unread is the no-root composition: it reads no handover and so
/// renders no recovery. A predecessor's root that could not be resolved is
/// Unresolved, and through the Memory source, which needs no root, the
/// successor is given the seeded recovery text and its typed provenance,
/// in the regular composition and the simplified one with an empty brief.
#[test]
fn an_unresolved_root_reaches_recovery_through_memory_and_unread_reads_none() {
    let fixture = ConfiguredStore::new();
    let store = Arc::new(FlowStore::open_configured(fixture.path()).expect("configured store"));
    let profile = crate::composition::tests::fixture_profile();
    let intent = emergency_intent(&profile, None);
    store.owe_successor(intent.clone()).expect("intent owed");
    let cause = intent
        .succession_cause
        .datomize(Vec::new())
        .protosize()
        .compact();
    let composer = memory_composer(&store);
    let mut simplified = profile.clone();
    simplified.flow_kind = signal_flow::FlowKind::Simplified;
    simplified.instruction_prompt.0 = String::new();
    for launch in [profile, simplified] {
        let body = composed_body(&composer, launch.clone(), HandoverRoot::Unresolved);
        assert!(body.contains(APPROVED_RECOVERY_TEXT), "{body}");
        assert!(body.contains(&cause), "{body}");
        let body = composed_body(&composer, launch, HandoverRoot::Unread);
        assert!(!body.contains(APPROVED_RECOVERY_TEXT), "{body}");
        assert!(!body.contains(&cause), "{body}");
    }
}

/// Gap 6, through the Flow store's own opener: an emergency intent with a
/// transcript path, owed through the store, reads back whole after each
/// reopen.
#[test]
fn an_owed_emergency_intent_with_its_path_reads_back_whole_across_reopens() {
    let fixture = ConfiguredStore::new();
    let profile = crate::composition::tests::fixture_profile();
    let intent = emergency_intent(
        &profile,
        Some(signal_flow::TranscriptPath(
            "/native/sessions/predecessor transcript.jsonl".into(),
        )),
    );
    {
        let store = FlowStore::open_configured(fixture.path()).expect("configured store");
        store.owe_successor(intent.clone()).expect("intent owed");
    }
    for _ in 0..2 {
        let store = FlowStore::open_configured(fixture.path()).expect("reopened store");
        assert_eq!(
            store
                .successor_intent(&profile.address)
                .expect("intent read"),
            Some(intent.clone())
        );
    }
}
