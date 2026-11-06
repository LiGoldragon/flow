//! A Start is admitted by its own Address alone: no model ranks above
//! another, so no Start waits for another model's seat and nothing starts
//! first. A topic is an uncapitalized camelCase identifier, refused by type
//! when it is not.

use super::model::{configure, seat};
use super::{NexusFixture, pane_unopened};
use crate::Dispatches;
use signal_flow::{Aspect, Model, Query, Response, StartRejection};
use std::fs;

#[test]
fn a_sol_start_needs_no_other_seat() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Model::Sol, "gpt-6-sol")]);
    let secondary = seat(Aspect::Mind, Model::Sol, "gpt-6-sol", "secondary-request");
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(secondary)),
        Response::StartRejected(pane_unopened())
    );
}

/// A Start of { Mind topics Astra } waits for no other model's seat: with
/// no { Mind topics Sol } metaflow at all it goes on to launch, its attempt
/// reserved, and no Sol metaflow is made or started for it.
#[test]
fn a_start_waits_for_no_other_models_seat() {
    use crate::store::ReadsLaunchAttempt;
    use crate::store::metaflow::SleepsMetaflows;
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Model::Astra, "gpt-6-sol")]);
    let mut astra = seat(Aspect::Mind, Model::Astra, "gpt-6-sol", "astra-request");
    astra.address.topic = "topics".into();
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(astra.clone())),
        Response::StartRejected(pane_unopened())
    );
    assert!(
        fixture
            .nexus
            .store
            .launch_attempt("astra-request")
            .unwrap()
            .is_some(),
        "the admitted Start reserved its attempt"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&signal_flow::Address {
                aspect: Aspect::Mind,
                topic: "topics".into(),
                model: Model::Sol,
            })
            .unwrap(),
        None,
        "no other model's metaflow is made for the Start"
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
    configure(&fixture, &[(Model::Sol, "gpt-6-sol")]);
    for topic in [
        "PrimaryCommitting",
        "primary-committing",
        "primary_committing",
        "",
        "9lives",
    ] {
        let mut start = seat(Aspect::Mind, Model::Sol, "gpt-6-sol", "topic-request");
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
            root_option: None,
            address: signal_flow::Address {
                aspect: Aspect::Mind,
                topic: topic.into(),
                model: Model::Sol,
            },
            system_modules: vec!["spirit-core".into()],
            user_modules: vec![],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: meta_signal_flow::Continuity::OnDemand,
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
    let mut kept = seat(Aspect::Mind, Model::Sol, "gpt-6-sol", "kept-request");
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

/// A Send whose recipient Address has a topic that is not camelCase is
/// refused TopicRefused, carrying the topic as written, before anything is
/// queued; an abrupt one fails with that refusal.
#[test]
fn a_send_to_a_bad_topic_is_topic_refused() {
    use crate::sending::SendsRequests;
    use crate::store::metaflow::SleepsMetaflows;
    use signal_flow::{Address, SendRequest};
    let fixture = NexusFixture::new();
    for topic in ["PrimaryCommitting", "primary-committing", "", "9lives"] {
        let recipient = Address {
            aspect: Aspect::Mind,
            topic: topic.into(),
            model: Model::Sol,
        };
        let sent = fixture.nexus.send_request(
            SendRequest {
                recipient: recipient.clone(),
                sendable_message: signal_flow::SendableMessage::Soft(
                    signal_flow::SendableRequest::Order("Never queued.".into()),
                ),
            },
            None,
        );
        assert_eq!(
            sent.response,
            Response::SendRefused(signal_flow::VisibleRefusal::TopicRefused(topic.into())),
            "{topic:?}"
        );
        assert!(sent.pending.is_none());
        assert_eq!(fixture.nexus.store.metaflow(&recipient).unwrap(), None);
        let abrupt = fixture.nexus.send_request(
            SendRequest {
                recipient,
                sendable_message: signal_flow::SendableMessage::HardAbrupt(
                    signal_flow::SendableRequest::Order("Never typed.".into()),
                ),
            },
            None,
        );
        assert_eq!(
            abrupt.response,
            Response::Failed(signal_flow::VisibleSendFailure {
                visible_send_failure_reason: signal_flow::VisibleSendFailureReason::Refused(
                    signal_flow::VisibleRefusal::TopicRefused(topic.into())
                ),
            }),
            "{topic:?}"
        );
    }
}

/// A Launch naming a topic that is not camelCase is refused TopicRefused,
/// as Start and Configure refuse it.
#[test]
fn launch_with_a_bad_topic_is_topic_refused() {
    use crate::Dispatches;
    use signal_flow::{Address, LaunchRequest, Remembering};
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    configure(&fixture, &[(Model::Sol, "gpt-6-sol")]);
    let response = fixture.nexus.dispatch(Query::Launch(LaunchRequest {
        address: Address {
            aspect: Aspect::Mind,
            topic: "PrimaryCommitting".into(),
            model: Model::Sol,
        },
        remembering: Remembering::Predecessors(0),
        instruction_prompt: String::new(),
    }));
    assert_eq!(
        response,
        Response::StartRejected(StartRejection::TopicRefused("PrimaryCommitting".into())),
    );
}
