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

