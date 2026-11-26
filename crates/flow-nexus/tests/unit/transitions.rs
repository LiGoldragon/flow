//! A flow's state is a record of events (vision:flow-nexus:0-1): every
//! report either harness makes is typed into one Event and recorded as a
//! transition with the cause Reported and the state it leads to; a request
//! delivered is recorded with the cause Arrived; the state the Nexus decides
//! by is the last transition; and a store an earlier Flow wrote, 0.56.2's
//! or 0.58.0's, is given each live flow's state from its recorded reports.

use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, WORKING_CODEX_THREAD, address, append_main_rows, claude_seat,
    codex_seat_in_fresh_world, end_turn, maintain, metaflow, report_claude_stop,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::generated::memory::{Cause, Event, Moment, Transition};
use crate::generated::overriding::{HeedsOverrides, RemembersOverrides, Seat, Transitioned};
use crate::generated::transitions::{AskedFlow, RecordsTransitions};
use crate::reporting::RecordsReports;
use crate::store::context::RecordsContextSize;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::{FlowStore, OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
use crate::waking::HoldsSoftForIdle;
use flow_ethos::State;
use meta_signal_flow as meta;
use signal_flow::{Message, Model, PrivateSendResponse, Request, SentBy, Tokens};

/// The transitions Flow's Memory holds for `flow_id`, oldest first.
fn transitions(fixture: &NexusFixture, flow_id: &str) -> Vec<Transition> {
    fixture
        .nexus
        .store
        .transitions()
        .unwrap()
        .into_iter()
        .find(|flow| flow.flow_id.0 == flow_id)
        .map(|flow| flow.transition_vector)
        .unwrap_or_default()
}

fn last(fixture: &NexusFixture, flow_id: &str) -> Transition {
    transitions(fixture, flow_id)
        .last()
        .cloned()
        .expect("a transition is recorded")
}

/// The Claude seat Awake at the Sol Address, its metaflow Maintained.
fn awake_claude_seat(fixture: &NexusFixture) {
    fixture.accept_pane_operations(
        vec![super::waking::claude_agent(fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(fixture, address(Model::Sol));
}

/// One Claude hook report, as the hook sends it: recorded, answered, then
/// acted on.
fn report(fixture: &NexusFixture, event: signal_flow::Event) {
    let flow_id = flow_ethos::FlowId(CLAUDE_SEAT.into());
    assert_eq!(
        fixture.nexus.report(signal_flow::Report_Data {
            flow_id: flow_id.clone(),
            process_option: None,
            event: event.clone(),
        }),
        signal_flow::Output::Reported
    );
    fixture.nexus.after_report(&flow_id, &event);
}

fn session() -> Option<signal_flow::NativeSessionId> {
    Some(signal_flow::NativeSessionId(CLAUDE_SESSION.into()))
}

fn reported(state: meta::State, event: Event) -> (meta::State, Cause) {
    (state, Cause::Reported(event))
}

fn held(fixture: &NexusFixture, flow_id: &str) -> Vec<(meta::State, Cause)> {
    transitions(fixture, flow_id)
        .into_iter()
        .map(|transition| (transition.state, transition.cause))
        .collect()
}

/// Every Claude hook report is one Event: the fresh session Started, its
/// own prompt and a tool boundary Working, its own Stop Idle with the size
/// its transcript shows, a Started of the same session its rest when the
/// main turn ended, a Cleared nothing, and its own session's end Ended.
/// Each leads to its state.
#[test]
fn each_claude_report_is_one_event_leading_to_its_state() {
    let fixture = NexusFixture::new();
    awake_claude_seat(&fixture);
    let seat = CLAUDE_SEAT;
    assert!(transitions(&fixture, seat).is_empty());

    report(&fixture, signal_flow::Event::Started(session()));
    assert_eq!(
        held(&fixture, seat),
        vec![reported(meta::State::Acting, Event::Started)]
    );

    report(
        &fixture,
        signal_flow::Event::PromptSubmitted(signal_flow::NativePrompt {
            native_session_id_option: session(),
            prompt_id_option: None,
            prompt_sha256: signal_flow::PromptSha256("ab".repeat(32)),
        }),
    );
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(meta::State::Acting, Event::Working(Tokens(0))))
    );

    fixture
        .nexus
        .store
        .record_context_size(seat, 12_000)
        .unwrap();
    report(&fixture, signal_flow::Event::ToolUsed("Bash".into()));
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(
            meta::State::Acting,
            Event::Working(Tokens(12_000))
        )),
        "a tool boundary carries the size Flow last measured"
    );

    report_claude_stop(&fixture, vec![]);
    assert!(
        held(&fixture, seat).contains(&reported(meta::State::Resting, Event::Idle(Tokens(40_000)))),
        "its own Stop is Idle with its transcript's size: {:?}",
        held(&fixture, seat)
    );
    assert_eq!(last(&fixture, seat).state, meta::State::Resting);

    // A Cleared is no event.
    let before = transitions(&fixture, seat).len();
    report(&fixture, signal_flow::Event::Cleared(session()));
    assert_eq!(transitions(&fixture, seat).len(), before);

    // The same session started again past its main turn's end: its rest.
    append_main_rows(&fixture, &[end_turn("ended-turn")]);
    report(&fixture, signal_flow::Event::Started(session()));
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(meta::State::Resting, Event::Idle(Tokens(40_000)))),
        "{:?}",
        held(&fixture, seat)
    );

    // Another session's end is no event of this flow; its own is Ended.
    let before = transitions(&fixture, seat).len();
    report(
        &fixture,
        signal_flow::Event::Ended(Some(signal_flow::NativeSessionId("another".into()))),
    );
    assert_eq!(transitions(&fixture, seat).len(), before);
    report(&fixture, signal_flow::Event::Ended(session()));
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(meta::State::Asleep, Event::Ended))
    );
    let flow = fixture
        .nexus
        .store
        .transitions()
        .unwrap()
        .into_iter()
        .find(|flow| flow.flow_id.0 == seat)
        .unwrap();
    assert_eq!(flow.address, address(Model::Sol));
    assert!(flow.transition_vector.iter().all(|t| t.moment.0 > 0));
}

/// A compaction the Claude harness made is Compacting, keeping the state
/// the flow was in.
#[test]
fn a_claude_compaction_is_compacting_and_keeps_the_state() {
    use std::io::Write;
    let fixture = NexusFixture::new();
    awake_claude_seat(&fixture);
    report_claude_stop(&fixture, vec![]);
    assert_eq!(last(&fixture, CLAUDE_SEAT).state, meta::State::Resting);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(super::waking::claude_transcript(&fixture))
        .unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({
            "type": "system",
            "subtype": "compact_boundary",
            "uuid": "manual-compaction",
            "timestamp": "2999-01-01T00:00:00.000Z",
            "isSidechain": false,
            "content": "Conversation compacted",
            "compactMetadata": {"trigger": "manual", "preTokens": 219_141}
        })
    )
    .unwrap();
    report(&fixture, signal_flow::Event::ToolUsed("Bash".into()));
    let all = held(&fixture, CLAUDE_SEAT);
    assert!(
        all.contains(&reported(meta::State::Resting, Event::Compacting)),
        "{all:?}"
    );
}

/// Every Codex report is one Event: its first is preceded by Started, its
/// thread reaching its rest Idle, a turn starting Working, a token usage
/// Working with its size in a turn, a compaction Compacting with the state
/// kept, and its thread closed Ended.
#[test]
fn each_codex_report_is_one_event_leading_to_its_state() {
    use super::codex_turns::{completed, idle, started};
    use crate::codex_turns::SettlesRestingCodexSeats;
    use crate::compacting::{CodexContextSignal, WatchesCompactions};
    let mut fixture = NexusFixture::new();
    let _world = codex_seat_in_fresh_world(&mut fixture);
    let seat = "abc123";

    fixture
        .nexus
        .codex_events(WORKING_CODEX_THREAD, vec![idle()]);
    assert_eq!(
        held(&fixture, seat),
        vec![
            reported(meta::State::Acting, Event::Started),
            reported(meta::State::Resting, Event::Idle(Tokens(0))),
        ]
    );

    fixture
        .nexus
        .codex_events(WORKING_CODEX_THREAD, vec![started("turn-1")]);
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(meta::State::Acting, Event::Working(Tokens(0))))
    );

    fixture
        .nexus
        .codex_context_signal(WORKING_CODEX_THREAD, CodexContextSignal::TokenUsage(91_000));
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(
            meta::State::Acting,
            Event::Working(Tokens(91_000))
        ))
    );

    fixture
        .nexus
        .codex_events(WORKING_CODEX_THREAD, vec![completed("turn-1")]);
    assert!(
        held(&fixture, seat).contains(&reported(meta::State::Resting, Event::Idle(Tokens(91_000)))),
        "{:?}",
        held(&fixture, seat)
    );
    let state = last(&fixture, seat).state;

    fixture.nexus.codex_context_signal(
        WORKING_CODEX_THREAD,
        CodexContextSignal::Compacted("compaction-1".into()),
    );
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(state, Event::Compacting))
    );

    fixture.nexus.codex_events(
        WORKING_CODEX_THREAD,
        vec![crate::generated::turn_ledger::LedgerEvent::ThreadClosed],
    );
    assert_eq!(
        held(&fixture, seat).last().cloned(),
        Some(reported(meta::State::Asleep, Event::Ended))
    );
}

/// A request delivered to a flow arrives there: its transition, the cause
/// Arrived with its RequestId, leads to Acting.
#[test]
fn a_delivered_request_arrives_at_its_flow() {
    let fixture = NexusFixture::new();
    awake_claude_seat(&fixture);
    report_claude_stop(&fixture, vec![]);
    assert_eq!(last(&fixture, CLAUDE_SEAT).state, meta::State::Resting);
    let PrivateSendResponse::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &address(Model::Sol),
            SentBy::Flow,
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Notice("Read the ruling.".into())),
            None,
        )
        .unwrap()
    else {
        panic!("admitted")
    };
    fixture
        .nexus
        .store
        .transition_logical_delivery(
            &accepted.logical_delivery_id,
            signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                harness_kind: signal_flow::HarnessKind::Claude,
                native_session_id: signal_flow::NativeSessionId(CLAUDE_SESSION.into()),
                native_prompt_id: signal_flow::NativePromptId("prompt-1".into()),
            }),
        )
        .unwrap();
    let arrived = last(&fixture, CLAUDE_SEAT);
    assert_eq!(arrived.state, meta::State::Acting);
    assert_eq!(arrived.cause, Cause::Arrived(accepted.request_id.clone()));
}

/// A request typed into a seat whose receipt its transcript shows only once
/// the turn that took it has ended arrives before that turn's rest: the
/// Stop settles it first, so the seat's last transition is its rest and the
/// next Soft Send is typed into it at once.
#[test]
fn a_request_its_turn_took_arrives_before_that_turns_rest() {
    use super::codex_agent_in;
    use super::waking::{CLAUDE_SESSION, claude_agent, send_soft, soft_recipient, stored};
    let fixture = NexusFixture::new();
    let (_caller, peer) = soft_recipient(&fixture, false);
    let response = send_soft(&fixture, &peer, "Read the ruling.");
    assert!(
        matches!(response, signal_flow::Output::SendAccepted(_)),
        "{response:?}"
    );
    assert!(fixture.typed().is_none(), "held for the seat's idle event");

    // The turn ends; Claude writes the row of what is typed into it only
    // once its prompt is taken, later.
    append_main_rows(&fixture, &[end_turn("first-turn")]);
    fixture.accept_pane_operations(
        vec![
            codex_agent_in("pane-1", "terminal-pane-1"),
            claude_agent(&fixture),
        ],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    report_claude_stop(&fixture, vec![]);
    let typed = fixture.typed().expect("typed at the Stop");
    assert!(typed.contains("Read the ruling."), "{typed}");

    // The seat takes it, its hook naming a digest of the text as Claude
    // shows it; its row and its turn's end land in its transcript.
    report(
        &fixture,
        signal_flow::Event::PromptSubmitted(signal_flow::NativePrompt {
            native_session_id_option: session(),
            prompt_id_option: None,
            prompt_sha256: signal_flow::PromptSha256("0".repeat(64)),
        }),
    );
    assert_eq!(last(&fixture, CLAUDE_SEAT).state, meta::State::Acting);
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,
                "uuid":"late-receipt","message":{"content":typed}}),
            end_turn("second-turn"),
        ],
    );
    report_claude_stop(&fixture, vec![]);

    let held = held(&fixture, CLAUDE_SEAT);
    let arrived = held
        .iter()
        .rposition(|(_, cause)| matches!(cause, Cause::Arrived(_)))
        .expect("the request arrived");
    let rested = held
        .iter()
        .rposition(|(state, _)| *state == meta::State::Resting)
        .expect("the seat rested");
    assert!(arrived < rested, "its arrival precedes its rest: {held:?}");
    assert_eq!(last(&fixture, CLAUDE_SEAT).state, meta::State::Resting);
    assert!(fixture.nexus.rests_now(CLAUDE_SEAT));
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());

    // The next Soft Send is typed at once, with no hand Deliver.
    fixture.accept_pane_operations(
        vec![
            codex_agent_in("pane-1", "terminal-pane-1"),
            claude_agent(&fixture),
        ],
        PromptFixture::Prompted("w1:p3"),
    );
    send_soft(&fixture, &peer, "The next ruling.");
    let typed = fixture.typed().expect("typed into the resting seat");
    assert!(typed.contains("The next ruling."), "{typed}");
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
}

/// The fold rule: an arrival whose receipt lies in a turn that has
/// already ended keeps the state before, and none when none was held; one
/// whose turn runs leads to Acting, so a seat genuinely in its turn still
/// waits for its idle event.
#[test]
fn an_arrival_in_an_ended_turn_keeps_the_state_before() {
    use crate::generated::transitions::{Arrival, Caused, LeadsToState, ReceiptTurn};
    let arrival = |receipt_turn| Arrival {
        caused: Caused {
            seat: Seat {
                flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
                address: address(Model::Sol),
            },
            cause: Cause::Arrived(flow_ethos::RequestId("r3c1".into())),
            moment: Moment(0),
        },
        receipt_turn,
    };
    for before in [
        None,
        Some(meta::State::Resting),
        Some(meta::State::Acting),
        Some(meta::State::Asleep),
    ] {
        assert_eq!(
            arrival(ReceiptTurn::Ended).leads_to(before.clone()),
            before,
            "an ended turn's arrival keeps {before:?}"
        );
        assert_eq!(
            arrival(ReceiptTurn::Running).leads_to(before.clone()),
            Some(meta::State::Acting),
            "a running turn's arrival from {before:?}"
        );
    }
}

/// Native activity alone changes the decision row. Audit entries survive
/// but cannot manufacture a state in a pre-takeover store.
#[test]
fn the_current_decision_state_is_not_an_audit_transition() {
    let fixture = NexusFixture::new();
    awake_claude_seat(&fixture);
    append_main_rows(&fixture, &[end_turn("ended-turn")]);
    let seat = Seat {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        address: address(Model::Sol),
    };
    let state = |fixture: &NexusFixture| {
        fixture
            .nexus
            .store
            .state_of(AskedFlow(flow_ethos::FlowId(CLAUDE_SEAT.into())))
            .unwrap()
    };
    fixture
        .nexus
        .store
        .record_observation(CLAUDE_SEAT, None)
        .unwrap();
    assert_eq!(state(&fixture), None);
    assert!(
        !fixture.nexus.rests_now(CLAUDE_SEAT),
        "a transcript alone is no state"
    );
    assert_eq!(fixture.nexus.state_now(seat.clone()), None);
    fixture
        .nexus
        .store
        .record_observation(
            CLAUDE_SEAT,
            Some(crate::generated::memory::SeatStanding::Alive(
                crate::generated::memory::SeatActivity::Working,
            )),
        )
        .unwrap();

    report_claude_stop(&fixture, vec![]);
    assert_eq!(state(&fixture), Some(meta::State::Resting));
    assert!(fixture.nexus.rests_now(CLAUDE_SEAT));
    assert_eq!(
        fixture.nexus.state_now(seat.clone()),
        Some(meta::State::Resting)
    );

    report(&fixture, signal_flow::Event::ToolUsed("Bash".into()));
    assert_eq!(state(&fixture), Some(meta::State::Acting));
    assert!(!fixture.nexus.rests_now(CLAUDE_SEAT));
    assert_eq!(
        fixture.nexus.state_now(seat.clone()),
        Some(meta::State::Acting)
    );

    use crate::Dispatches;
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta::Input::SetState(meta::SetState_Data {
                flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
                state: meta::State::Resting,
            })),
        meta::Output::StateSet(meta::StateSet_Data {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            state: meta::State::Resting,
        })
    );
    assert!(!fixture.nexus.rests_now(CLAUDE_SEAT));
    assert_eq!(fixture.nexus.state_now(seat), Some(meta::State::Acting));
}

/// The store closed, optionally rewritten as 0.56.2 left it (no
/// transitions and no switch families), and opened again.
fn reopen(fixture: &mut NexusFixture, as_before_the_panel: bool) {
    let placeholder = FlowStore::open(&fixture.directory.path().join("placeholder.sema"))
        .expect("placeholder store");
    drop(std::mem::replace(
        &mut fixture.nexus.store,
        placeholder.into(),
    ));
    let path = fixture.directory.path().join("flow.sema");
    if as_before_the_panel {
        let database = redb::Database::open(&path).expect("the store opens raw");
        let write = database.begin_write().unwrap();
        {
            let mut catalog = write
                .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                    "__sema_engine_catalog",
                ))
                .unwrap();
            for family in ["flow_nexus_flow_transitions", "flow_nexus_hold"] {
                catalog.remove(family).unwrap();
            }
        }
        for family in ["flow_nexus_flow_transitions", "flow_nexus_hold"] {
            let _ = write.delete_table(redb::TableDefinition::<String, &[u8]>::new(family));
        }
        write.commit().unwrap();
    }
    let mut reopened = FlowStore::open(&path).expect("the earlier store opens");
    reopened.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
        host_id: "isolated-test-host".into(),
        boot_id: "isolated-test-boot".into(),
    });
    fixture.nexus.store = reopened.into();
}

/// The Claude seat with its Stop recorded as an earlier Flow recorded it,
/// a harness event and no transition, and its context size held.
fn earlier_seat(fixture: &NexusFixture) {
    awake_claude_seat(fixture);
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 151_000)
        .unwrap();
    use crate::store::events::RecordsHarnessEvents;
    assert!(matches!(
        fixture
            .nexus
            .store
            .events(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .last(),
        Some(crate::store::events::RecordedHarnessEvent::Stopped(_))
    ));
}

/// A 0.56.2 store, which holds neither the transitions family nor the
/// switch, opens refusing nothing: both are registered, the Nexus acts on
/// its own, and each live flow is given the transition its last recorded
/// report maps to, with the size Memory holds. Opened again, nothing more
/// is given.
#[test]
fn a_0_56_2_store_opens_with_each_live_flow_given_its_reported_state() {
    let mut fixture = NexusFixture::new();
    earlier_seat(&fixture);
    reopen(&mut fixture, true);
    assert!(matches!(
        fixture.nexus.store.holding(),
        Ok(crate::generated::overriding::Holding::Acting)
    ));
    assert_eq!(
        held(&fixture, CLAUDE_SEAT),
        vec![reported(meta::State::Resting, Event::Idle(Tokens(151_000)))]
    );
    assert!(fixture.nexus.rests_now(CLAUDE_SEAT));
    reopen(&mut fixture, false);
    assert_eq!(transitions(&fixture, CLAUDE_SEAT).len(), 1);
}

/// A 0.58.0 store, whose transitions are the override panel's alone,
/// opens refusing nothing: a flow the panel recorded is given its reported
/// transition first, at its first override's moment, so the owner's word
/// stays its state.
#[test]
fn a_0_58_0_store_opens_with_the_reported_state_before_the_overrides() {
    let mut fixture = NexusFixture::new();
    earlier_seat(&fixture);
    let seat = Seat {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        address: address(Model::Sol),
    };
    fixture
        .nexus
        .store
        .record_transition(Transitioned {
            seat,
            transition: Transition {
                state: meta::State::Crystallizing,
                cause: Cause::Overridden,
                moment: Moment(1_000),
            },
        })
        .unwrap();
    reopen(&mut fixture, false);
    assert_eq!(
        transitions(&fixture, CLAUDE_SEAT),
        vec![
            Transition {
                state: meta::State::Resting,
                cause: Cause::Reported(Event::Idle(Tokens(151_000))),
                moment: Moment(1_000),
            },
            Transition {
                state: meta::State::Crystallizing,
                cause: Cause::Overridden,
                moment: Moment(1_000),
            },
        ]
    );
    reopen(&mut fixture, false);
    assert_eq!(transitions(&fixture, CLAUDE_SEAT).len(), 2);
}
