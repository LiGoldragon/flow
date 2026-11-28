//! Meta `Bind.{ Address TranscriptPath }`: a running Claude session named
//! by its transcript becomes the flow its Address is Awake as, its seat
//! seeded from its Harness Nexus's witness.

use super::NexusFixture;
use super::refresh_and_compaction::declare_seat;
use super::waking::{CLAUDE_SEAT, address, claude_seat};
use crate::Dispatches;
use crate::generated::transitions::{AskedFlow, RecordsTransitions};
use crate::harness_subscription::{HarnessObservation, KeepsHarnessWitnesses};
use crate::store::ReadsFlowRows;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use flow_ethos::State;
use meta_signal_flow::{Bind_Data, BindRefused_Data, Bound_Data, Continuity, Input, Output};
use signal_flow::{FlowId, FlowLifecycle, Metaflow, Model, TranscriptPath};
use std::time::Instant;

const BOUND_SESSION: &str = "b0a7d5e1-0000-4000-8000-000000000007";

/// A Claude transcript of `session` under the fixture, its first row
/// naming it.
fn transcript_of(fixture: &NexusFixture, session: &str) -> TranscriptPath {
    let project = fixture.directory.path().join("bound-project");
    std::fs::create_dir_all(&project).unwrap();
    let path = project.join(format!("{session}.jsonl"));
    std::fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::json!({"type":"user","sessionId":session,"message":{"content":"hello"}})
        ),
    )
    .unwrap();
    TranscriptPath(path.to_string_lossy().into_owned())
}

fn bind(fixture: &NexusFixture, transcript_path: TranscriptPath) -> Output {
    fixture.nexus.dispatch_meta(Input::Bind(Bind_Data {
        address: address(Model::Sol),
        transcript_path,
    }))
}

fn declared() -> NexusFixture {
    let fixture = NexusFixture::new();
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit:core:0".into(), "intent:flow:0".into()],
    );
    fixture
}

/// The Claude Harness Nexus tells the bound session Rested.
fn witnessed_resting(fixture: &NexusFixture, session: &str) {
    fixture.nexus.harness_witnesses.take(
        &meta_signal_flow::HarnessSocket("/run/claude-harness.sock".into()),
        HarnessObservation::Snapshot(vec![harness::Observed {
            flow_id: FlowId(session.chars().take(6).collect()),
            state: harness::State::Flowing(harness::Flow::Rested),
            tokens: harness::Tokens(0),
            background_work_vector: vec![],
        }]),
        Instant::now(),
    );
}

/// No permaflow configured at the Address is UnknownPermaflow; a
/// transcript that cannot be read, or whose rows name no session equal to
/// its stem, is Unreadable. Nothing is bound.
#[test]
fn a_bind_is_refused_for_an_unknown_permaflow_or_an_unreadable_transcript() {
    let undeclared = NexusFixture::new();
    let path = transcript_of(&undeclared, BOUND_SESSION);
    assert_eq!(
        bind(&undeclared, path),
        Output::BindRefused(BindRefused_Data::UnknownPermaflow(address(Model::Sol)))
    );
    let fixture = declared();
    let missing = TranscriptPath(
        fixture
            .directory
            .path()
            .join("absent/none.jsonl")
            .to_string_lossy()
            .into_owned(),
    );
    assert_eq!(
        bind(&fixture, missing.clone()),
        Output::BindRefused(BindRefused_Data::Unreadable(missing))
    );
    let other = fixture
        .directory
        .path()
        .join(format!("{BOUND_SESSION}-renamed.jsonl"));
    std::fs::write(
        &other,
        format!("{}\n", serde_json::json!({"sessionId": BOUND_SESSION})),
    )
    .unwrap();
    let other = TranscriptPath(other.to_string_lossy().into_owned());
    assert_eq!(
        bind(&fixture, other.clone()),
        Output::BindRefused(BindRefused_Data::Unreadable(other))
    );
    assert!(fixture.nexus.store.flow_node("b0a7d5").unwrap().is_none());
}

/// A bound session its Harness Nexus tells idle is Resting at once, its
/// flow the session's first six characters, Active and Claude, and the
/// Address Awake as it; the same Bind again replaces nothing.
#[test]
fn a_bound_idle_session_is_resting_and_its_address_awake() {
    let fixture = declared();
    witnessed_resting(&fixture, BOUND_SESSION);
    let path = transcript_of(&fixture, BOUND_SESSION);
    let flow_id = FlowId("b0a7d5".into());
    assert_eq!(
        bind(&fixture, path.clone()),
        Output::Bound(Bound_Data {
            address: address(Model::Sol),
            flow_id: flow_id.clone(),
            flow_id_option: None,
        })
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .state_of(AskedFlow(flow_id.clone()))
            .unwrap(),
        Some(meta_signal_flow::State::Resting)
    );
    let node = fixture.nexus.store.flow_node("b0a7d5").unwrap().unwrap();
    assert_eq!(node.session_id.0, BOUND_SESSION);
    assert_eq!(node.harness_kind, signal_flow::HarnessKind::Claude);
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&address(Model::Sol))
            .unwrap()
            .unwrap()
            .state,
        State::Awake(flow_id.clone())
    );
    assert_eq!(
        bind(&fixture, path),
        Output::Bound(Bound_Data {
            address: address(Model::Sol),
            flow_id,
            flow_id_option: None,
        }),
        "the same flow again replaces nothing"
    );
}

/// A Bind of an Address Awake as another flow names that flow, retires it
/// in the store and keeps it in Past.
#[test]
fn a_bind_replacing_another_flow_names_and_retires_it() {
    let fixture = declared();
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let path = transcript_of(&fixture, BOUND_SESSION);
    assert_eq!(
        bind(&fixture, path),
        Output::Bound(Bound_Data {
            address: address(Model::Sol),
            flow_id: FlowId("b0a7d5".into()),
            flow_id_option: Some(FlowId(CLAUDE_SEAT.into())),
        })
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Retired
    );
    let metaflow = fixture
        .nexus
        .store
        .metaflow(&address(Model::Sol))
        .unwrap()
        .unwrap();
    assert_eq!(metaflow.state, State::Awake(FlowId("b0a7d5".into())));
    assert_eq!(
        metaflow.past,
        flow_ethos::Past(vec![FlowId(CLAUDE_SEAT.into())])
    );
}
