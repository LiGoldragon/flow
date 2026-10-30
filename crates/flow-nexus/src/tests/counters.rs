//! Configure.Counters names the next id each family allocates: the next
//! logical delivery, the next queue attempt, the next send outcome. A store
//! that has allocated nothing reads `{ 1 3 1 }`. A Counters is taken only
//! while the store has recorded no logical delivery, no queue attempt and
//! no send outcome, and never below a counter as it stands; the
//! Configuration snapshot ends with the counters, so an operator reads an
//! old store's before carrying them to a new one. Read and written as
//! datom text, as flow-meta does.

use super::NexusFixture;
use super::waking::{CLAUDE_SEAT, address, claude_seat};
use crate::Dispatches;
use crate::store::ManagesMetaflowState;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{
    AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery,
};
use crate::store::send_outcome::RecordsSendOutcomes;
use flow_ethos::State;
use signal_flow::{Delivery, Layer, Message, Metaflow, Request, SendAcceptance, SentBy};

fn ask(fixture: &NexusFixture, text: &str) -> String {
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Compactable, Protosizable, ReaderBudget};
    let mut budget = Budget {
        remaining: 65536,
        reader: ReaderBudget { remaining: 65536 },
        depth: 0,
        maximum_depth: 1024,
    };
    let query = Potential::<meta_signal_flow::Query>::from(text)
        .actualize(&mut budget)
        .unwrap_or_else(|error| panic!("{text}: {error:?}"));
    fixture
        .nexus
        .dispatch_meta(query)
        .datomize(vec![])
        .protosize()
        .compact()
}

/// The fixture store with its Nexus declared, so Configuration answers.
fn configured() -> NexusFixture {
    use crate::store::SeedsConfiguration;
    let fixture = NexusFixture::new();
    let settings = crate::store::DefaultConfiguration {
        home: fixture.directory.path().join("home"),
        runtime_directory: fixture.directory.path().join("runtime"),
    }
    .configuration();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            meta_signal_flow::ConfigureNexus {
                source_root: settings.source_root,
                stable_codex: settings.stable_codex,
                next_codex: settings.next_codex,
                harness_profiles: settings.harness_profile_vector,
                meta_aspects: settings.meta_aspects,
                message_nexus_path_option: Some(settings.message_nexus_path.clone()),
                message_nexus_binary_option: Some("/fixture/message-nexus".into()),

                curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                lease: 30,
            },
        ))
        .expect("Nexus configured");
    fixture
}

fn counters(fixture: &NexusFixture) -> String {
    let snapshot = ask(fixture, "Configuration");
    let open = snapshot
        .rfind(" { ")
        .expect("the snapshot ends with a struct");
    snapshot[open + 1..snapshot.len() - 2].to_owned()
}

/// On a store that has recorded nothing, a Counters at or above each
/// counter is taken and read back; one below any counter is refused
/// Lowered, carrying the counters as they stand, and changes nothing.
#[test]
fn counters_are_declared_on_an_empty_store_and_never_lowered() {
    let fixture = configured();
    assert_eq!(counters(&fixture), "{ 1 3 1 }");
    for lowered in [
        "Configure.Counters.{ 0 3 1 }",
        "Configure.Counters.{ 1 2 1 }",
        "Configure.Counters.{ 1 3 0 }",
        "Configure.Counters.{ -5 3 1 }",
    ] {
        assert_eq!(
            ask(&fixture, lowered),
            "ConfigureRejected.CountersRefused.Lowered.{ 1 3 1 }",
            "{lowered}"
        );
    }
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 1 3 1 }"),
        "Configured.Unchanged"
    );
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 120 45 300 }"),
        "Configured.Replaced"
    );
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 120 45 300 }"),
        "Configured.Unchanged"
    );
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 119 45 300 }"),
        "ConfigureRejected.CountersRefused.Lowered.{ 120 45 300 }"
    );
    assert_eq!(counters(&fixture), "{ 120 45 300 }");
    // Still empty, a raised declaration is taken again.
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 130 50 310 }"),
        "Configured.Replaced"
    );
    assert_eq!(counters(&fixture), "{ 130 50 310 }");
}

/// The declared value is the next id each family allocates. Once any of
/// them is recorded the store is no longer empty and a Counters is
/// refused NotEmpty, however high.
#[test]
fn declared_counters_name_the_next_ids_and_a_record_ends_the_empty_store() {
    let fixture = configured();
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 130 50 310 }"),
        "Configured.Replaced"
    );

    let outcome = fixture
        .nexus
        .store
        .record_send_outcome(
            None,
            None,
            None,
            "digest".into(),
            signal_flow::PrivateSendResponse::Refused(signal_flow::Refusal::Lapsed),
        )
        .expect("send outcome recorded");
    assert_eq!(outcome.send_outcome_id, "send-outcome-0000000000000136");
    assert_eq!(counters(&fixture), "{ 130 50 311 }");
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 500 500 500 }"),
        "ConfigureRejected.CountersRefused.NotEmpty"
    );

    let seat = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat.clone(),
        state: State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let signal_flow::PrivateSendResponse::SendAccepted(SendAcceptance {
        logical_delivery_id,
        ..
    }) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &seat,
            SentBy::Unknown,
            Delivery::Queue,
            Message::Soft(Request::Notice("counted".into())),
            None,
        )
        .expect("admission reads")
    else {
        panic!("the request is queued")
    };
    assert_eq!(logical_delivery_id, "logical-delivery-00000000000000000130");

    claude_seat(&fixture, 40_000);
    let queue = fixture
        .nexus
        .store
        .metaflow(&seat)
        .expect("metaflow reads")
        .expect("metaflow held")
        .queue;
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat.clone(),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue,
    });
    let PrepareQueueDelivery::Prepared(attempt) = fixture
        .nexus
        .store
        .prepare_oldest_delivery(
            &seat,
            CLAUDE_SEAT,
            "c1a0de00-0000-4000-8000-000000000001",
            signal_flow::HarnessKind::Claude,
            NativeHistoryAnchor::CodexTurns(None),
        )
        .expect("prepared")
    else {
        panic!("the oldest request is prepared")
    };
    assert_eq!(attempt.attempt_id, "queue-attempt-0000000000000032");
    // The logical delivery counter is the delivery cursor every lifecycle
    // event draws from: the admission took 130, the hand-over's Attempting
    // took 131.
    assert_eq!(counters(&fixture), "{ 132 51 311 }");
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 500 500 500 }"),
        "ConfigureRejected.CountersRefused.NotEmpty"
    );
    assert_eq!(counters(&fixture), "{ 132 51 311 }");
}
