use super::NexusFixture;
use crate::handover_observation::ReadsRetirementSubscription;
use crate::store::handover::{HandoverRemovalOutcome, HandoverWriter, HoldsHandovers};
use signal_flow::{
    Address, Aspect, HandoverArchiveIdentity, Model, Response, RetirementChange,
    RetirementObservationFailure, Section, Title, Touch,
};

fn address(topic: &str) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: topic.into(),
        model: Model::Sol,
    }
}

fn section(title: &str, description: &str, body: &str) -> Section {
    Section {
        title: title.into(),
        description: description.into(),
        body: body.into(),
    }
}

fn archive_identity() -> HandoverArchiveIdentity {
    HandoverArchiveIdentity {
        git_commit: "commit".into(),
        archive_document_path: "Mind/topic/Sol/pred.md".into(),
        git_blob: "blob".into(),
    }
}

#[test]
fn keep_appends_and_replaces_in_the_original_position() {
    let fixture = NexusFixture::new();
    let address = address("handoverKeep");
    let store = &fixture.nexus.store;
    store
        .keep_handover(
            &address,
            &address,
            section("one", "d1", "b1"),
            Touch::Unknown,
        )
        .unwrap();
    store
        .keep_handover(
            &address,
            &address,
            section("two", "d2", "b2"),
            Touch::Unknown,
        )
        .unwrap();
    store
        .keep_handover(
            &address,
            &address,
            section("one", "new", "replacement"),
            Touch::Unknown,
        )
        .unwrap();
    assert_eq!(
        store.handover(&address).unwrap(),
        vec![
            section("one", "new", "replacement"),
            section("two", "d2", "b2"),
        ]
    );
    assert_eq!(
        store.handover_memory(&address).unwrap().unwrap().touch,
        Some(Touch::Unknown)
    );
}

#[test]
fn missing_drop_is_atomic_for_content_and_touch() {
    let fixture = NexusFixture::new();
    let address = address("handoverDrop");
    let store = &fixture.nexus.store;
    store
        .keep_handover(&address, &address, section("one", "d", "b"), Touch::Unknown)
        .unwrap();
    let before = store.handover_memory(&address).unwrap();
    assert_eq!(
        store.drop_handover(&address, &address, Title::from("missing"), Touch::Unknown),
        Err(signal_flow::HandoverRefusal::MissingTitle)
    );
    assert_eq!(store.handover_memory(&address).unwrap(), before);
}

#[test]
fn authenticated_drop_before_first_keep_is_missing_title_and_stays_absent() {
    let fixture = NexusFixture::new();
    let address = address("handoverDropBeforeKeep");
    let store = &fixture.nexus.store;
    let writer = HandoverWriter {
        address: address.clone(),
        flow_id: "current-flow".into(),
    };
    assert_eq!(
        store.drop_handover_for_flow(&writer, Title::from("missing"), Touch::Unknown),
        Err(signal_flow::HandoverRefusal::MissingTitle)
    );
    assert!(store.handover_memory(&address).unwrap().is_none());
    assert!(store.handover(&address).unwrap().is_empty());
}

#[test]
fn successor_closes_an_immutable_snapshot_and_starts_with_unknown_touch() {
    let fixture = NexusFixture::new();
    let address = address("handoverSuccessor");
    let store = &fixture.nexus.store;
    store
        .keep_handover(
            &address,
            &address,
            section("one", "d", "before"),
            Touch::Unknown,
        )
        .unwrap();
    store
        .close_handover_for_successor(&address, "pred01", "succ01")
        .unwrap();
    let closed = store.handover_memory(&address).unwrap().unwrap();
    assert_eq!(closed.current, vec![section("one", "d", "before")]);
    assert_eq!(closed.touch, None);
    assert_eq!(closed.closed[0].sections, closed.current);
    store
        .keep_handover(
            &address,
            &address,
            section("one", "d", "after"),
            Touch::Unknown,
        )
        .unwrap();
    let changed = store.handover_memory(&address).unwrap().unwrap();
    assert_eq!(
        changed.closed[0].sections,
        vec![section("one", "d", "before")]
    );
}

#[test]
fn successor_restart_is_idempotent_and_empty_generation_is_archived() {
    let fixture = NexusFixture::new();
    let address = address("handoverEmptyGeneration");
    let store = &fixture.nexus.store;
    store
        .close_handover_for_successor(&address, "pred01", "succ01")
        .unwrap();
    store
        .close_handover_for_successor(&address, "pred01", "succ01")
        .unwrap();
    let row = store.handover_memory(&address).unwrap().unwrap();
    assert_eq!(row.closed.len(), 1);
    assert!(row.closed[0].sections.is_empty());
    assert_eq!(row.current_flow_id.as_deref(), Some("succ01"));
    assert_eq!(row.generation, 1);
    assert_eq!(
        store.handover_for_flow(&address, "pred01").unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        store.handover_for_flow(&address, "succ01").unwrap(),
        Some(Vec::new())
    );
}

#[test]
fn closed_generation_reopens_and_retry_keeps_one_snapshot() {
    use super::configuring::ConfiguredStore;
    use crate::store::OpensFlowStore;
    let fixture = ConfiguredStore::new();
    let address = address("handoverCrashBoundary");
    let store = crate::store::FlowStore::open_configured(fixture.path()).unwrap();
    store.ensure_live_handover(&address, "pred").unwrap();
    store
        .keep_handover(
            &address,
            &address,
            section("old", "d", "body"),
            Touch::Unknown,
        )
        .unwrap();
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    drop(store);
    let reopened = crate::store::FlowStore::open_configured(fixture.path()).unwrap();
    reopened
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let row = reopened.handover_memory(&address).unwrap().unwrap();
    assert_eq!(row.closed.len(), 1);
    assert_eq!(row.current_flow_id.as_deref(), Some("succ"));
    assert_eq!(row.touch, None);
    assert_eq!(row.current, vec![section("old", "d", "body")]);
}

#[test]
fn only_owner_edits_but_read_apis_cross_addresses() {
    let fixture = NexusFixture::new();
    let owner = address("handoverOwner");
    let other = address("handoverOther");
    let store = &fixture.nexus.store;
    store
        .keep_handover(&owner, &owner, section("one", "d", "b"), Touch::Unknown)
        .unwrap();
    let before = store.handover_memory(&owner).unwrap();
    assert_eq!(
        store.keep_handover(&owner, &other, section("two", "d", "b"), Touch::Unknown),
        Err(signal_flow::HandoverRefusal::NotOwner)
    );
    assert_eq!(
        store.drop_handover(&owner, &other, Title::from("one"), Touch::Unknown),
        Err(signal_flow::HandoverRefusal::NotOwner)
    );
    assert_eq!(store.handover_memory(&owner).unwrap(), before);
    assert_eq!(
        store.handover(&owner).unwrap(),
        vec![section("one", "d", "b")]
    );
    assert_eq!(
        store.handover_headings(&owner).unwrap()[0].title,
        Title::from("one")
    );
}

#[test]
fn authenticated_unknown_write_sets_live_owner_atomically() {
    let fixture = NexusFixture::new();
    let address = address("handoverWriter");
    let store = &fixture.nexus.store;
    let writer = HandoverWriter {
        address: address.clone(),
        flow_id: "flow01".into(),
    };
    store
        .keep_handover_for_flow(&writer, section("one", "d", "b"), Touch::Unknown)
        .unwrap();
    assert_eq!(
        store.handover_for_flow(&address, "flow01").unwrap(),
        Some(vec![section("one", "d", "b")])
    );
    assert_eq!(
        store.keep_handover_for_flow(
            &HandoverWriter {
                address,
                flow_id: "stale".into()
            },
            section("two", "d", "b"),
            Touch::Unknown
        ),
        Err(signal_flow::HandoverRefusal::NotOwner)
    );
}

#[test]
fn closed_snapshot_survives_successor_edit_and_reopen_notification() {
    let fixture = NexusFixture::new();
    let address = address("handoverSnapshot");
    let store = &fixture.nexus.store;
    let writer = HandoverWriter {
        address: address.clone(),
        flow_id: "pred".into(),
    };
    store
        .keep_handover_for_flow(&writer, section("one", "d", "before"), Touch::Unknown)
        .unwrap();
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    store
        .keep_handover_for_flow(
            &HandoverWriter {
                address: address.clone(),
                flow_id: "succ".into(),
            },
            section("one", "d", "after"),
            Touch::Unknown,
        )
        .unwrap();
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let row = store.handover_memory(&address).unwrap().unwrap();
    assert_eq!(row.closed.len(), 1);
    assert_eq!(row.closed[0].sections, vec![section("one", "d", "before")]);
    assert_eq!(row.current, vec![section("one", "d", "after")]);
}

#[test]
fn exact_retirement_identity_preserves_new_live_and_closed_rows() {
    let fixture = NexusFixture::new();
    let address = address("handoverRetirementRace");
    let store = &fixture.nexus.store;
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let published = store.closed_handovers(&address, 0).unwrap().pop().unwrap();
    let subscription = store.observe_handover_retirement(&address).unwrap();
    assert_eq!(
        subscription.snapshot.handover_generation_key_vector.len(),
        1
    );
    store
        .keep_handover_for_flow(
            &HandoverWriter {
                address: address.clone(),
                flow_id: "succ".into(),
            },
            section("live", "d", "new"),
            Touch::Unknown,
        )
        .unwrap();
    store
        .close_handover_for_successor(&address, "succ", "next")
        .unwrap();
    assert_eq!(
        store
            .complete_closed_handover_retirement(&address, &published, archive_identity())
            .unwrap(),
        HandoverRemovalOutcome::Removed
    );
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementChanged(RetirementChange::GenerationClosed(_))
    ));
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementChanged(RetirementChange::ArchiveCommitted(_))
    ));
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementChanged(RetirementChange::GenerationRemoved(_))
    ));
    let row = store.handover_memory(&address).unwrap().unwrap();
    assert_eq!(row.current, vec![section("live", "d", "new")]);
    assert_eq!(row.closed.len(), 1);
    assert_eq!(row.closed[0].predecessor, "succ");
}

#[test]
fn changed_published_snapshot_is_retained() {
    let fixture = NexusFixture::new();
    let address = address("handoverRetirementConflict");
    let store = &fixture.nexus.store;
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let mut published = store.closed_handovers(&address, 0).unwrap().pop().unwrap();
    let subscription = store.observe_handover_retirement(&address).unwrap();
    published.successor = "changed".into();
    assert_eq!(
        store
            .complete_closed_handover_retirement(&address, &published, archive_identity())
            .unwrap(),
        HandoverRemovalOutcome::IdentityMismatch
    );
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementObservationFailed(RetirementObservationFailure::MissingEvidence(_))
    ));
    assert_eq!(store.closed_handovers(&address, 0).unwrap().len(), 1);
}

#[test]
fn already_absent_is_explicitly_incomplete_and_never_emits_removed() {
    let fixture = NexusFixture::new();
    let address = address("handoverRetirementAbsent");
    let store = &fixture.nexus.store;
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let closed = store.closed_handovers(&address, 0).unwrap().pop().unwrap();
    assert_eq!(
        store
            .complete_closed_handover_retirement(&address, &closed, archive_identity())
            .unwrap(),
        HandoverRemovalOutcome::Removed
    );
    let subscription = store.observe_handover_retirement(&address).unwrap();
    assert!(
        subscription
            .snapshot
            .handover_generation_key_vector
            .is_empty()
    );
    assert_eq!(
        store
            .complete_closed_handover_retirement(&address, &closed, archive_identity())
            .unwrap(),
        HandoverRemovalOutcome::AlreadyAbsent
    );
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementObservationFailed(RetirementObservationFailure::MissingEvidence(_))
    ));
}

#[test]
fn verified_archive_then_memory_failure_reports_archive_and_incomplete_without_removed() {
    let fixture = NexusFixture::new();
    let address = address("handoverRemovalWriteFailure");
    let store = &fixture.nexus.store;
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let closed = store.closed_handovers(&address, 0).unwrap().pop().unwrap();
    let subscription = store.observe_handover_retirement(&address).unwrap();
    store.fail_next_handover_removal();
    assert!(
        store
            .complete_closed_handover_retirement(&address, &closed, archive_identity())
            .is_err()
    );
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementChanged(RetirementChange::ArchiveCommitted(_))
    ));
    assert!(matches!(
        subscription.next_response().unwrap(),
        Response::RetirementObservationFailed(RetirementObservationFailure::MissingEvidence(_))
    ));
    assert_eq!(store.closed_handovers(&address, 0).unwrap(), vec![closed]);
}

#[test]
fn concurrent_attach_and_close_places_the_generation_exactly_once_in_snapshot_or_fifo() {
    let fixture = NexusFixture::new();
    let address = address("handoverAttachCloseRace");
    let store = fixture.nexus.store.clone();
    let start = std::sync::Arc::new(std::sync::Barrier::new(3));
    let observing = {
        let address = address.clone();
        let store = store.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            store.observe_handover_retirement(&address).unwrap()
        })
    };
    let closing = {
        let address = address.clone();
        let store = store.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            start.wait();
            store
                .close_handover_for_successor(&address, "pred", "succ")
                .unwrap();
        })
    };
    start.wait();
    let subscription = observing.join().unwrap();
    closing.join().unwrap();
    let in_snapshot = subscription
        .snapshot
        .handover_generation_key_vector
        .iter()
        .filter(|key| key.predecessor == "pred" && key.successor == "succ")
        .count();
    assert_eq!(in_snapshot + subscription.queued_change_count(), 1);
    if in_snapshot == 0 {
        assert!(matches!(
            subscription.next_response().unwrap(),
            Response::RetirementChanged(RetirementChange::GenerationClosed(key))
                if key.predecessor == "pred" && key.successor == "succ"
        ));
    }
}

#[test]
fn two_closes_are_observed_in_their_durable_mutation_order() {
    let fixture = NexusFixture::new();
    let address = address("handoverTwoCloseOrder");
    let store = &fixture.nexus.store;
    let subscription = store.observe_handover_retirement(&address).unwrap();
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    store
        .close_handover_for_successor(&address, "succ", "next")
        .unwrap();
    for (generation, predecessor, successor) in [(0, "pred", "succ"), (1, "succ", "next")] {
        assert!(matches!(
            subscription.next_response().unwrap(),
            Response::RetirementChanged(RetirementChange::GenerationClosed(key))
                if key.handover_generation == generation
                    && key.predecessor == predecessor
                    && key.successor == successor
        ));
    }
}

#[test]
fn queued_delivery_completes_while_the_handover_mutation_gate_is_held() {
    let fixture = NexusFixture::new();
    let address = address("handoverDeliveryOutsideGate");
    let store = &fixture.nexus.store;
    let subscription = store.observe_handover_retirement(&address).unwrap();
    store
        .close_handover_for_successor(&address, "pred", "succ")
        .unwrap();
    let gate = store
        .handover_write_gate
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (sent, received) = std::sync::mpsc::channel();
    let delivery = std::thread::spawn(move || {
        sent.send(subscription.next_response()).unwrap();
    });
    assert!(matches!(
        received
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
            .unwrap(),
        Response::RetirementChanged(RetirementChange::GenerationClosed(_))
    ));
    drop(gate);
    delivery.join().unwrap();
}

#[test]
fn concurrent_authenticated_writes_preserve_both_sections() {
    let fixture = NexusFixture::new();
    let address = address("handoverConcurrent");
    let store = fixture.nexus.store.clone();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let mut joins = Vec::new();
    for title in ["one", "two"] {
        let store = store.clone();
        let address = address.clone();
        let barrier = barrier.clone();
        joins.push(std::thread::spawn(move || {
            barrier.wait();
            store
                .keep_handover_for_flow(
                    &HandoverWriter {
                        address,
                        flow_id: "flow".into(),
                    },
                    section(title, "d", title),
                    Touch::Unknown,
                )
                .unwrap();
        }));
    }
    barrier.wait();
    for join in joins {
        join.join().unwrap();
    }
    let row = store.handover_memory(&address).unwrap().unwrap();
    assert_eq!(row.current.len(), 2);
    assert_eq!(row.touch, Some(Touch::Unknown));
}
