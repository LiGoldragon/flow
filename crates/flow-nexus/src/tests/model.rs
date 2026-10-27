//! A seat runs the model configured for its Address's layer and aspect,
//! named exactly. A Start or a Replace naming any other model is refused
//! before the Nexus writes, claims, composes or opens anything.

use super::NexusFixture;
use crate::store::ManagesMetaflowState as _;
use signal_flow::{Address, Aspect, LaunchProfile, Layer};

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
                    model_name: (*native).into(),
                    effort: "medium".into(),
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
            // A topic is camelCase: the request id with its hyphens taken out.
            topic: launch_request_id.replace('-', ""),
            layer,
        },
        model_name: model.into(),
        flow_id_option: None,
        remembering: signal_flow::Remembering::Predecessors(0),
        herdr_session_name: "fixture-session".into(),
        ..crate::composition::tests::fixture_profile()
    }
}
