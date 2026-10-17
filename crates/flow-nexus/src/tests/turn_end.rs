use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::store::ManagesMetaflowState;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::turn_end::KeepsTurnEnds;
use crate::turn_end::HandlesTurnEnds;
use flow_ethos::State;
use signal_flow::{
    Address, Aspect, FlowLifecycle, Layer, Metaflow, Query, QueuedRequest, Request, Response,
    SentBy, TurnEndRequest,
};
use std::io::Write;

fn queued(sender: &str, body: &str) -> QueuedRequest {
    QueuedRequest {
        sent_by: SentBy::Sender(Address {
            aspect: Aspect::Psyche,
            topic: sender.into(),
            layer: Layer::Secondary,
        }),
        request: Request::Order(body.into()),
    }
}

fn setup(lifecycle: FlowLifecycle) -> (NexusFixture, Address) {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(lifecycle);
    let address = Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        layer: Layer::Secondary,
    };
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake("908786".into()),
        past: Vec::new(),
        queue: vec![
            queued("firstSender", "first"),
            queued("secondSender", "second"),
            queued("thirdSender", "third"),
        ],
    });
    (fixture, address)
}

fn transcript(fixture: &NexusFixture) -> std::path::PathBuf {
    let path = fixture
        .directory
        .path()
        .join("rollout-01a0b22c-e24f-7452-9940-64490878680f.jsonl");
    std::fs::File::create(&path).unwrap();
    path
}

fn request(path: &std::path::Path, turn: &str) -> TurnEndRequest {
    TurnEndRequest {
        session_id: "01a0b22c-e24f-7452-9940-64490878680f".into(),
        turn_id: turn.into(),
        transcript_path_option: Some(path.to_string_lossy().into_owned()),
    }
}

fn append(path: &std::path::Path, turn: &str) {
    writeln!(
        std::fs::OpenOptions::new().append(true).open(path).unwrap(),
        "{}",
        serde_json::json!({"type":"event_msg","payload":{
            "type":"task_complete","turn_id":turn}})
    )
    .unwrap();
}

#[test]
fn exact_completed_turns_drain_one_sender_preserving_entry_in_order() {
    let (fixture, address) = setup(FlowLifecycle::Active);
    let transcript = transcript(&fixture);
    let first = request(&transcript, "turn-1");
    assert_eq!(
        fixture.nexus.dispatch(Query::QueueTurnEnd(first.clone())),
        Response::TurnEndQueued(first.clone())
    );
    assert!(fixture.nexus.complete_visible_turn_ends().is_empty());
    append(&transcript, "unrelated");
    assert!(fixture.nexus.complete_visible_turn_ends().is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .unwrap()
            .queue
            .len(),
        3
    );

    append(&transcript, "turn-1");
    assert_eq!(fixture.nexus.complete_visible_turn_ends(), vec!["908786"]);
    let after_first = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
    assert_eq!(
        after_first.queue,
        vec![
            queued("secondSender", "second"),
            queued("thirdSender", "third"),
        ]
    );
    let typed = fixture.typed().unwrap();
    assert!(typed.contains("firstSender") && typed.contains("first"));
    assert!(!typed.contains("secondSender"));

    assert_eq!(
        fixture.nexus.dispatch(Query::QueueTurnEnd(first.clone())),
        Response::TurnEndQueued(first)
    );
    assert!(fixture.nexus.complete_visible_turn_ends().is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .unwrap()
            .queue
            .len(),
        2,
        "a duplicate completion drains nothing"
    );

    for (turn, remaining, next_sender) in [("turn-2", 1, "thirdSender"), ("turn-3", 0, "")] {
        let next = request(&transcript, turn);
        assert_eq!(
            fixture.nexus.dispatch(Query::QueueTurnEnd(next.clone())),
            Response::TurnEndQueued(next)
        );
        append(&transcript, turn);
        assert_eq!(fixture.nexus.complete_visible_turn_ends(), vec!["908786"]);
        let metaflow = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
        assert_eq!(metaflow.queue.len(), remaining);
        if let Some(oldest) = metaflow.queue.first() {
            assert_eq!(oldest, &queued(next_sender, "third"));
        }
    }
}

#[test]
fn a_first_turn_completion_during_registration_is_kept_for_confirmation() {
    let (fixture, _) = setup(FlowLifecycle::Pending);
    fixture.set_agents(vec![fixture.current_agent()]);
    let transcript = transcript(&fixture);
    let first = request(&transcript, "first-turn");
    assert_eq!(
        fixture.nexus.dispatch(Query::QueueTurnEnd(first.clone())),
        Response::TurnEndQueued(first)
    );
    append(&transcript, "first-turn");
    assert_eq!(fixture.nexus.complete_visible_turn_ends(), vec!["908786"]);
    let records = fixture.nexus.store.pending_turn_ends().unwrap();
    assert!(records.is_empty());
    assert!(fixture.nexus.store.has_completed_turn("908786").unwrap());
}
