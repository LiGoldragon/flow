//! Configure.Counters names the next id each family allocates: the next
//! logical delivery, the next queue attempt, the next send outcome. A store
//! that has allocated nothing reads `{ 1 3 1 }`. A Counters is taken only
//! while the store has recorded no logical delivery, no queue attempt and
//! no send outcome, and never below a counter as it stands; the
//! Configuration snapshot carries the counters, so an operator reads an
//! old store's before carrying them to a new one. Read and written as
//! datom text, as flow-meta does.

use super::NexusFixture;
use super::waking::address;
use crate::Dispatches;
use crate::store::ManagesMetaflowState;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::send_outcome::RecordsSendOutcomes;
use flow_ethos::State;
use signal_flow::{Message, Metaflow, Model, Request, SendAcceptance, SentBy};

fn ask(fixture: &NexusFixture, text: &str) -> String {
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Compactable, Protosizable, ReaderBudget};
    let mut budget = Budget {
        remaining: 65536,
        reader: ReaderBudget::try_from(65536).expect("a constant reader budget"),
        depth: 0,
        maximum_depth: 1024,
    };
    let query = Potential::<meta_signal_flow::Input>::from(text)
        .actualize(&mut budget)
        .unwrap_or_else(|error| panic!("{text}: {error:?}"));
    fixture
        .nexus
        .dispatch_meta(query)
        .datomize(vec![])
        .protosize()
        .compact()
}

/// The fixture store's Memory file, byte for byte as it stands.
fn store_bytes(fixture: &NexusFixture) -> Vec<u8> {
    std::fs::read(fixture.directory.path().join("flow.sema")).expect("the store file reads")
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
                source_root: flow_ethos::SourceRoot(settings.source_root),
                first_codex_endpoint: settings.stable_codex,
                second_codex_endpoint: settings.next_codex,
                harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                    settings.message_nexus_path.clone(),
                )),
                message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                    "/fixture/message-nexus".into(),
                )),

                curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                    "/fixture/curriculum.sock".into(),
                ),
                lease: flow_ethos::Lease(30),
                presentation_prompt_option: None,
                herdr_session_name: signal_flow::HerdrSessionName("default".into()),
            },
        ))
        .expect("Nexus configured");
    fixture
}

fn counters(fixture: &NexusFixture) -> String {
    use datom_codec::{Actualizing, Budget, Potential};
    use protos::ReaderBudget;
    let text = ask(fixture, "Configuration");
    let mut budget = Budget {
        remaining: 65536,
        reader: ReaderBudget::try_from(65536).unwrap(),
        depth: 0,
        maximum_depth: 1024,
    };
    let meta_signal_flow::Output::Configuration(snapshot) =
        Potential::<meta_signal_flow::Output>::from(text)
            .actualize(&mut budget)
            .unwrap()
    else {
        panic!("configuration expected")
    };
    let counters = snapshot.configured_counters.0;
    format!(
        "{{ {} {} {} }} {} {}",
        counters.logical_delivery.0,
        counters.queue_attempt.0,
        counters.send_outcome.0,
        snapshot.configured_request_id_draws.0.0,
        snapshot.configured_announcement_tries.0.0
    )
}

/// On a store that has recorded nothing, a Counters at or above each
/// counter is taken and read back; one below any counter is refused
/// Lowered, carrying the counters as they stand, and changes nothing.
#[test]
fn counters_are_declared_on_an_empty_store_and_never_lowered() {
    let fixture = configured();
    assert_eq!(counters(&fixture), "{ 1 3 1 } 64 3");
    for lowered in [
        "Configure.Counters.{ 0 3 1 }",
        "Configure.Counters.{ 1 2 1 }",
        "Configure.Counters.{ 1 3 0 }",
        "Configure.Counters.{ -5 3 1 }",
    ] {
        assert_eq!(
            ask(&fixture, lowered),
            "ConfigureRefused.CountersRefused.Lowered.{ 1 3 1 }",
            "{lowered}"
        );
    }
    // The counters as they stand answer their echo and write nothing.
    let stored = store_bytes(&fixture);
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 1 3 1 }"),
        "Configured.Counters.{ 1 3 1 }"
    );
    assert!(
        store_bytes(&fixture) == stored,
        "the store is byte-identical"
    );
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 120 45 300 }"),
        "Configured.Counters.{ 120 45 300 }"
    );
    let stored = store_bytes(&fixture);
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 120 45 300 }"),
        "Configured.Counters.{ 120 45 300 }"
    );
    assert!(
        store_bytes(&fixture) == stored,
        "the store is byte-identical"
    );
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 119 45 300 }"),
        "ConfigureRefused.CountersRefused.Lowered.{ 120 45 300 }"
    );
    assert_eq!(counters(&fixture), "{ 120 45 300 } 64 3");
    // Still empty, a raised declaration is taken again.
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 130 50 310 }"),
        "Configured.Counters.{ 130 50 310 }"
    );
    assert_eq!(counters(&fixture), "{ 130 50 310 } 64 3");
}

/// The declared value is the next id each family allocates. Once any of
/// them is recorded the store is no longer empty and a Counters is
/// refused NotEmpty, however high.
#[test]
fn declared_counters_name_the_next_ids_and_a_record_ends_the_empty_store() {
    let fixture = configured();
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 130 50 310 }"),
        "Configured.Counters.{ 130 50 310 }"
    );

    let outcome = fixture
        .nexus
        .store
        .record_send_outcome(
            None,
            signal_flow::SentBy::Unknown,
            None,
            "digest".into(),
            signal_flow::PrivateSendResponse::Refused(signal_flow::Refusal::Lapsed),
        )
        .expect("send outcome recorded");
    assert_eq!(
        outcome.send_outcome_id,
        meta_signal_flow::SendOutcomeId("send-outcome-0000000000000136".into())
    );
    assert_eq!(counters(&fixture), "{ 130 50 311 } 64 3");
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 500 500 500 }"),
        "ConfigureRefused.CountersRefused.NotEmpty"
    );

    let seat = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
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
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Notice("counted".into())),
            None,
        )
        .expect("admission reads")
    else {
        panic!("the request is queued")
    };
    assert_eq!(
        logical_delivery_id,
        flow_ethos::LogicalDeliveryId("logical-delivery-00000000000000000130".into())
    );

    // The logical delivery counter is the delivery cursor every lifecycle
    // event draws from: the admission took 130.
    assert_eq!(counters(&fixture), "{ 131 50 311 } 64 3");
    assert_eq!(
        ask(&fixture, "Configure.Counters.{ 500 500 500 }"),
        "ConfigureRefused.CountersRefused.NotEmpty"
    );
    assert_eq!(counters(&fixture), "{ 131 50 311 } 64 3");
}
