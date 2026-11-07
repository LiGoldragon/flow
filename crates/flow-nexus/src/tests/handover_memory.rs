use super::NexusFixture;
use crate::store::handover::{HandoverWriter, HoldsHandovers};
use signal_flow::{Address, Aspect, Model, Section, Title, Touch};

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
    assert!(store.remove_closed_handover(&address, &published).is_ok());
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
    published.successor = "changed".into();
    assert!(matches!(
        store.remove_closed_handover(&address, &published),
        Err(crate::store::StoreError::StateInvariant)
    ));
    assert_eq!(store.closed_handovers(&address, 0).unwrap().len(), 1);
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
