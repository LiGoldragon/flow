use super::NexusFixture;
use crate::store::handover::HoldsHandovers;
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
