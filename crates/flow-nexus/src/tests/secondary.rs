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

/// A Start of { Mind topics Primary } is refused NoSecondary before
/// anything is written, claimed or opened while no metaflow is recorded at
/// { Mind topics Secondary }: another topic's Secondary or another
/// aspect's does not count. Once that Secondary is recorded, Asleep, the
/// Start goes on to launch: with Herdr gone it is refused only where the
/// launch pane would open, its attempt reserved.
#[test]
fn a_primary_start_is_refused_until_its_secondary_is_recorded() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Layer::Primary, Aspect::Mind, "gpt-6-sol")]);
    fixture
        .nexus
        .store
        .seed_metaflow(asleep(Aspect::Mind, "other", Layer::Secondary));
    fixture
        .nexus
        .store
        .seed_metaflow(asleep(Aspect::Psyche, "topics", Layer::Secondary));
    let mut primary = seat(Aspect::Mind, Layer::Primary, "gpt-6-sol", "primary-request");
    primary.address.topic = "topics".into();
    let before = untouched(&fixture);

    assert_eq!(
        fixture.nexus.dispatch(Query::Start(primary.clone())),
        Response::StartRejected(StartRejection::NoSecondary)
    );
    assert_eq!(untouched(&fixture), before);
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_attempt("primary-request")
            .unwrap(),
        None
    );
    assert_eq!(
        fixture.nexus.store.metaflow(&primary.address).unwrap(),
        None
    );

    fixture
        .nexus
        .store
        .seed_metaflow(asleep(Aspect::Mind, "topics", Layer::Secondary));
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(primary.clone())),
        Response::StartRejected(pane_unopened())
    );
    assert!(
        fixture
            .nexus
            .store
            .launch_attempt("primary-request")
            .unwrap()
            .is_some(),
        "the accepted Start reserved its attempt"
    );
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
