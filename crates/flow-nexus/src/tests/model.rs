//! A seat runs the model configured for its Address's layer and aspect,
//! named exactly. A Start or a Replace naming any other model is refused
//! before the Nexus writes, claims, composes or opens anything.

use super::{NexusFixture, pane_unopened};
use crate::Dispatches;
use crate::store::metaflow::SleepsMetaflows as _;
use crate::store::{
    ManagesMetaflowState as _, MarksCommits as _, NamesConfiguredModel as _, ReadsFlowRows as _,
    ReadsLaunchAttempt as _, RecordsReplacement as _,
};
use signal_flow::{
    Address, Aspect, LaunchProfile, Layer, ModelRefusal, Query, ReplaceRejection, Response,
    StartRejection,
};
use std::fs;

/// Declares the Models family whole: one model per layer and aspect.
pub(super) fn configure(fixture: &NexusFixture, models: &[(Layer, Aspect, &str)]) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(
            models
                .iter()
                .map(|(layer, aspect, native)| meta_signal_flow::ConfigureModel {
                    layer: layer.clone(),
                    aspect: aspect.clone(),
                    native: (*native).into(),
                })
                .collect(),
        ))
        .expect("models configured");
}

/// A fresh launch of one seat, naming one model.
pub(super) fn seat(
    aspect: Aspect,
    layer: Layer,
    model: &str,
    launch_request_id: &str,
) -> LaunchProfile {
    LaunchProfile {
        launch_request_id: launch_request_id.into(),
        address: Address {
            aspect,
            topic: launch_request_id.into(),
            layer,
        },
        model_name: model.into(),
        flow_id_option: None,
        remembered_flow_vector: Vec::new(),
        herdr_session_name: "fixture-session".into(),
        ..crate::composition::tests::fixture_profile()
    }
}

/// Everything a refused Start must leave as it was: every Herdr call, every
/// FlowId claim, the flows Flow holds, and the store's commit marker, which
/// moves with any write.
#[derive(Debug, PartialEq)]
pub(super) struct Untouched {
    herdr_calls: String,
    flow_id_calls: String,
    flows: Vec<signal_flow::FlowNode>,
    marker: sema_engine::DatabaseMarker,
}

pub(super) fn untouched(fixture: &NexusFixture) -> Untouched {
    let read =
        |name: &str| fs::read_to_string(fixture.directory.path().join(name)).unwrap_or_default();
    Untouched {
        herdr_calls: read("herdr-calls.log"),
        flow_id_calls: read("flow-id-calls"),
        flows: fixture.nexus.store.flow_nodes().expect("flows read"),
        marker: fixture.nexus.store.commit_marker(),
    }
}

/// A Psyche Secondary configured on claude-opus-5-5 refuses a Start naming
/// claude-haiku-5-5, carrying the configured name; a seat whose layer and
/// aspect has no model is refused Unconfigured; a Replace whose successor
/// names another model is refused before the replacement is recorded. None
/// of them calls Herdr (no workspace, pane or harness), claims a FlowId,
/// registers a flow, takes the Address or writes the store.
#[test]
fn a_start_naming_another_model_than_its_seats_is_refused_before_anything_happens() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    fixture.herdr_for_replacement(0);
    configure(
        &fixture,
        &[(Layer::Secondary, Aspect::Psyche, "claude-opus-5-5")],
    );
    let before = untouched(&fixture);

    let differs = seat(
        Aspect::Psyche,
        Layer::Secondary,
        "claude-haiku-5-5",
        "differs-request",
    );
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(differs.clone())),
        Response::StartRejected(StartRejection::ModelRefused(ModelRefusal::Differs(
            "claude-opus-5-5".into()
        )))
    );
    let unconfigured = seat(
        Aspect::Psyche,
        Layer::Tertiary,
        "claude-sonnet-5-5",
        "unconfigured-request",
    );
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(unconfigured.clone())),
        Response::StartRejected(StartRejection::ModelRefused(ModelRefusal::Unconfigured))
    );
    let mut successor = seat(
        Aspect::Psyche,
        Layer::Secondary,
        "claude-haiku-5-5",
        "successor-request",
    );
    successor.flow_id_option = Some("908786".into());
    assert_eq!(
        fixture.nexus.dispatch(Query::Replace(successor.clone())),
        Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
            StartRejection::ModelRefused(ModelRefusal::Differs("claude-opus-5-5".into()))
        ))
    );

    assert_eq!(untouched(&fixture), before);
    for profile in [&differs, &unconfigured, &successor] {
        let id = &profile.launch_request_id;
        assert_eq!(
            fixture.nexus.store.launch_attempt(id).unwrap(),
            None,
            "{id}"
        );
        assert_eq!(fixture.nexus.store.replacement(id).unwrap(), None, "{id}");
        assert_eq!(
            fixture.nexus.store.metaflow(&profile.address).unwrap(),
            None,
            "{id}"
        );
    }
}

/// One layer runs a Claude model for Psyche and a Codex model for Mind. A
/// Psyche Secondary naming the Codex gpt-6-sol is refused, naming the
/// Psyche model; a Mind Secondary naming gpt-6-sol passes the model check
/// and goes on to launch: with Herdr gone it is refused only where the
/// launch pane would open, its attempt reserved.
#[test]
fn a_psyche_seat_on_a_codex_model_is_refused_and_a_mind_secondary_on_sol_launches() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(
        &fixture,
        &[
            (Layer::Secondary, Aspect::Psyche, "claude-opus-5-5"),
            (Layer::Secondary, Aspect::Mind, "gpt-6-sol"),
        ],
    );
    let before = untouched(&fixture);
    let psyche = seat(
        Aspect::Psyche,
        Layer::Secondary,
        "gpt-6-sol",
        "psyche-request",
    );
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(psyche.clone())),
        Response::StartRejected(StartRejection::ModelRefused(ModelRefusal::Differs(
            "claude-opus-5-5".into()
        )))
    );
    assert_eq!(untouched(&fixture), before);
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_attempt("psyche-request")
            .unwrap(),
        None
    );

    let mind = seat(Aspect::Mind, Layer::Secondary, "gpt-6-sol", "mind-request");
    assert_eq!(fixture.nexus.store.model_refusal(&mind).unwrap(), None);
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(mind.clone())),
        Response::StartRejected(pane_unopened())
    );
    let attempt = fixture
        .nexus
        .store
        .launch_attempt("mind-request")
        .unwrap()
        .expect("the accepted Start reserved its attempt");
    assert_eq!(attempt.launch_profile.model_name, "gpt-6-sol");
}
