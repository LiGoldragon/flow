//! Short term, a Primary is not started without its Secondary, and the
//! Secondary starts first. A metaflow sleeps whenever its harness goes
//! idle and a topic never dies, so the Secondary that has started is the
//! one recorded, Awake or Asleep, at the Secondary of the Primary's aspect
//! and topic.

use super::model::{configure, seat, untouched};
use super::{NexusFixture, pane_unopened};
use crate::Dispatches;
use crate::store::ReadsLaunchAttempt as _;
use crate::store::metaflow::{SeedsMetaflow as _, SleepsMetaflows as _};
use signal_flow::{Address, Aspect, Layer, Query, Response, StartRejection};
use std::fs;

/// The metaflow of one seat, Asleep with nothing past or queued.
fn asleep(aspect: Aspect, topic: &str, layer: Layer) -> signal_flow::Metaflow {
    signal_flow::Metaflow {
        address: Address {
            aspect,
            topic: topic.into(),
            layer,
        },
        state: flow_ethos::State::Asleep,
        past: vec![],
        queue: vec![],
    }
}

/// A Secondary, or any layer below the Primary, needs no seat above or
/// beside it to start.
#[test]
fn a_secondary_start_needs_no_other_seat() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Layer::Secondary, Aspect::Mind, "gpt-6-sol")]);
    let secondary = seat(
        Aspect::Mind,
        Layer::Secondary,
        "gpt-6-sol",
        "secondary-request",
    );
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(secondary)),
        Response::StartRejected(pane_unopened())
    );
}
