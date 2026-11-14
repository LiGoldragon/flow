//! A seat runs the native model configured for its Address's Model,
//! named exactly. A Start or a Replace naming any other model is refused
//! before the Nexus writes, claims, composes or opens anything.

use super::NexusFixture;
use crate::store::ManagesMetaflowState as _;
use signal_flow::{Address, Aspect, LaunchProfile, Model};

/// Declares the Models family whole: one native model per Model.
pub(super) fn configure(fixture: &NexusFixture, models: &[(Model, &str)]) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(
            models
                .iter()
                .map(|(model, native)| meta_signal_flow::ConfigureModel {
                    model: model.clone(),
                    native: meta_signal_flow::Native((*native).into()),
                })
                .collect(),
        ))
        .expect("models configured");
}

/// A fresh launch of one seat, naming one model.
pub(super) fn seat(
    aspect: Aspect,
    model: Model,
    native: &str,
    launch_request_id: &str,
) -> LaunchProfile {
    LaunchProfile {
        launch_request_id: signal_flow::LaunchRequestId(launch_request_id.into()),
        address: Address {
            aspect,
            // A topic is camelCase: the request id with its hyphens taken out.
            topic: flow_ethos::Topic(launch_request_id.replace('-', "")),
            model,
        },
        model_name: flow_ethos::ModelName(native.into()),
        flow_id_option: None,
        remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        ..crate::composition::tests::fixture_profile()
    }
}
