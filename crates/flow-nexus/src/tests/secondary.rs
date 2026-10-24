//! Short term, a Primary is not started without its Secondary, and the
//! Secondary starts first: Flow starts a declared Secondary itself before a
//! new Primary of its aspect and topic, and refuses the Primary NoSecondary,
//! before anything is written, claimed or opened, only when that Secondary
//! has no declaration and is not Awake as a live flow. A topic is an
//! uncapitalized camelCase identifier, refused by type when it is not.

use super::model::{configure, seat};
use super::{NexusFixture, pane_unopened};
use crate::Dispatches;
use signal_flow::{Aspect, Layer, Query, Response, StartRejection};
use std::fs;

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

fn awake_secondary(fixture: &NexusFixture, topic: &str) {
    use crate::store::metaflow::SeedsMetaflow;
    super::waking::claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
        address: signal_flow::Address {
            aspect: Aspect::Mind,
            topic: topic.into(),
            layer: Layer::Secondary,
        },
        state: flow_ethos::State::Awake(super::waking::CLAUDE_SEAT.into()),
        past: vec![],
        queue: vec![],
    });
}

/// A Start of { Mind topics Primary } whose Secondary has no declaration is
/// refused NoSecondary before anything is written or opened while that
/// Secondary is absent or Asleep:
/// another topic's Secondary or another aspect's does not count. Once that
/// Secondary is Awake as a live flow, the Start goes on to launch.
#[test]
fn a_primary_start_is_refused_until_its_secondary_is_awake() {
    use crate::store::ReadsLaunchAttempt;
    use crate::store::metaflow::SeedsMetaflow;
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Layer::Primary, Aspect::Mind, "gpt-6-sol")]);
    awake_secondary(&fixture, "other");
    let mut primary = seat(Aspect::Mind, Layer::Primary, "gpt-6-sol", "primary-request");
    primary.address.topic = "topics".into();
    for secondary in [None, Some(flow_ethos::State::Asleep)] {
        if let Some(state) = secondary {
            fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
                address: signal_flow::Address {
                    aspect: Aspect::Mind,
                    topic: "topics".into(),
                    layer: Layer::Secondary,
                },
                state,
                past: vec![],
                queue: vec![],
            });
        }
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(primary.clone())),
            Response::StartRejected(StartRejection::NoSecondary)
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_attempt("primary-request")
                .unwrap(),
            None,
            "nothing of the refused Start is written"
        );
    }
    awake_secondary(&fixture, "topics");
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
        "the admitted Start reserved its attempt"
    );
}

/// A topic that is not an uncapitalized camelCase identifier is refused by
/// type, carrying the topic as written, at Start and at Configure, before
/// anything is written; one that is, capitals within, is kept as written.
#[test]
fn a_topic_that_is_not_camel_case_is_refused_at_start_and_configure() {
    use crate::Dispatches;
    use crate::store::ReadsLaunchAttempt;
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Layer::Secondary, Aspect::Mind, "gpt-6-sol")]);
    for topic in [
        "PrimaryCommitting",
        "primary-committing",
        "primary_committing",
        "",
        "9lives",
    ] {
        let mut start = seat(Aspect::Mind, Layer::Secondary, "gpt-6-sol", "topic-request");
        start.address.topic = topic.into();
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(start)),
            Response::StartRejected(StartRejection::TopicRefused(topic.into())),
            "{topic:?}"
        );
        assert_eq!(
            fixture.nexus.store.launch_attempt("topic-request").unwrap(),
            None
        );
        let declared = meta_signal_flow::ConfigureMetaflow {
            address: signal_flow::Address {
                aspect: Aspect::Mind,
                topic: topic.into(),
                layer: Layer::Secondary,
            },
            system_modules: vec!["spirit-core".into()],
            user_modules: vec![],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            availability: meta_signal_flow::Availability::OnDemand,
            purpose: String::new(),
            records: vec![],
        };
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Configure(
                    meta_signal_flow::ConfigurationChange::Metaflows(vec![declared])
                )),
            meta_signal_flow::Response::ConfigureRejected(
                meta_signal_flow::ConfigureRejection::TopicRefused(topic.into())
            ),
            "{topic:?}"
        );
    }
    let mut kept = seat(Aspect::Mind, Layer::Secondary, "gpt-6-sol", "kept-request");
    kept.address.topic = "primaryCommitting".into();
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(kept)),
        Response::StartRejected(pane_unopened())
    );
    let attempt = fixture
        .nexus
        .store
        .launch_attempt("kept-request")
        .unwrap()
        .expect("a camelCase topic is admitted");
    assert_eq!(attempt.launch_profile.address.topic, "primaryCommitting");
}
