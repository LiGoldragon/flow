//! The meta socket's repairs. `Inspect` answers a metaflow whole: its
//! state and queue, the flow it is Awake as with its pane, the sender lock
//! held on it, and the attempts outstanding for it. `Rebind` records where
//! its flow lives, a pane or another flow, and touches no harness.
//! `Withdraw` takes one queued request out and closes the attempt that
//! holds it back.

use super::refresh_and_compaction::{SUCCESSOR_REQUEST, staged_refresh_successor};
use super::waking::{CLAUDE_SEAT, address, claude_agent, claude_seat};
use super::{ControlsHerdrSnapshot, NexusFixture};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{MutatesMetaflows, SeedsMetaflow, SleepsMetaflows};
use crate::store::queue_delivery::{
    AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery, QueueDeliveryPhase,
};
use crate::store::{ManagesMetaflowState, ReadsFlowRoles, ReadsFlowRows, RegistersFlowIdentity};
use crate::{Dispatches, DispatchesSerially};
use flow_ethos::State;
use meta_signal_flow::{
    Attempt, InspectRejected_Data, Query as MetaQuery, QueueDeliveryStanding, Rebind_Data,
    RebindRejected_Data, Rebinding, Response as MetaResponse, Withdraw_Data, WithdrawRejected_Data,
};
use signal_flow::{
    Address, Aspect, Delivery, DeliveryState, HarnessKind, HerdrRoute, HerdrRouteSelection, Layer,
    Lock_Data, Message, Metaflow, QueueEntry, Recipient, Request, Response, SentBy,
};
use std::fs;

fn seat_address() -> Address {
    address(Layer::Secondary)
}

fn elsewhere() -> Address {
    Address {
        aspect: Aspect::Field,
        topic: "nowhere".into(),
        layer: Layer::Tertiary,
    }
}

fn entry(id: &str) -> QueueEntry {
    QueueEntry {
        logical_delivery_id: id.into(),
        sent_by: SentBy::Sender(seat_address()),
        delivery: Delivery::Queue,
        message: Message::Soft(Request::Order(format!("Order {id}."))),
    }
}

/// The Claude seat Awake at the Secondary address, holding `queue`.
fn awake_seat(fixture: &NexusFixture, queue: Vec<QueueEntry>) {
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue,
    });
}

fn stored(fixture: &NexusFixture) -> Metaflow {
    fixture
        .nexus
        .store
        .metaflow(&seat_address())
        .expect("metaflow readable")
        .expect("metaflow held")
}

/// Prepares the hand-over of the seat's oldest request, as a drain does
/// before typing it.
fn prepared(fixture: &NexusFixture) -> String {
    let PrepareQueueDelivery::Prepared(attempt) = fixture
        .nexus
        .store
        .prepare_oldest_delivery(
            &seat_address(),
            CLAUDE_SEAT,
            "c1a0de00-0000-4000-8000-000000000001",
            HarnessKind::Claude,
            NativeHistoryAnchor::CodexTurns(None),
        )
        .expect("prepared")
    else {
        panic!("the oldest request is prepared")
    };
    attempt.attempt_id
}

fn inspect(fixture: &NexusFixture, at: Address) -> MetaResponse {
    fixture.nexus.dispatch_meta(MetaQuery::Inspect(at))
}

fn herdr_closed_anything(fixture: &NexusFixture) -> bool {
    ["herdr-operations.log", "herdr-calls.log"]
        .into_iter()
        .any(|name| {
            fs::read_to_string(fixture.directory.path().join(name))
                .unwrap_or_default()
                .contains("pane close")
        })
}

/// Inspect answers the metaflow as Flow holds it: its state and queue, the
/// row of the flow it is Awake as with that flow's pane, the sender lock
/// held on it, the hand-over that holds its oldest request back and the
/// launch at its Address not settled. An Asleep metaflow has no flow; an
/// Address Flow holds no metaflow for is refused UnknownAddress.
#[test]
fn inspect_answers_the_metaflow_whole() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture, vec![entry("inspect-1"), entry("inspect-2")]);
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
        layer: Layer::Secondary,
    };
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: sender.clone(),
        state: State::Awake("aa034f".into()),
        past: vec![],
        queue: vec![],
    });
    // A lock lasts the configured lease.
    let root = fixture.directory.path().to_string_lossy().into_owned();
    let endpoint = |name: &str| meta_signal_flow::CodexEndpoint {
        client_path: format!("{root}/{name}"),
        home: format!("{root}/{name}-home"),
        control_socket_path: format!("{root}/{name}.sock"),
        model_name_vector: vec![format!("{name}-model")],
    };
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            meta_signal_flow::ConfigureNexus {
                source_root: root.clone(),
                flows_root: "/tmp/flows".into(),
                meta_aspects: vec![Aspect::Psyche],
                message_nexus_path: format!("{root}/message.sock"),
                message_nexus_binary: format!("{root}/message-nexus"),
                curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                lease: 60,
                executables: meta_signal_flow::Executables {
                    herdr: "herdr".into(),
                    flow_id_claimer: "flow-id".into(),
                    hook: "flow-hook".into(),
                },
                default_herdr_session: "default".into(),
            },
        ))
        .expect("Nexus configured");
    let lock = match fixture
        .nexus
        .store
        .lock_metaflow(
            Lock_Data {
                sender,
                recipient: Recipient::Address(seat_address()),
            },
            1_790_000_000,
        )
        .expect("lock recorded")
    {
        Response::Locked(lock) => lock,
        other => panic!("the sender locks the seat: {other:?}"),
    };
    let attempt_id = prepared(&fixture);

    let MetaResponse::Inspected(inspected) = inspect(&fixture, seat_address()) else {
        panic!("the metaflow is inspected")
    };
    assert_eq!(inspected.metaflow, stored(&fixture));
    assert_eq!(inspected.metaflow.state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(
        inspected.binding.flow_node_option,
        fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap(),
        "the binding is the row of the flow it is Awake as"
    );
    assert!(inspected.binding.flow_node_option.is_some());
    assert_eq!(inspected.binding.process_option, None);
    assert_eq!(inspected.lock_vector, vec![lock]);
    assert!(
        matches!(
            inspected.attempt_vector.as_slice(),
            [Attempt::Queued(delivery)]
                if delivery.queue_attempt_id == attempt_id
                    && delivery.queue_delivery_standing == QueueDeliveryStanding::Prepared
        ),
        "{:?}",
        inspected.attempt_vector
    );

    // A launch at the Address, not confirmed, holds the Address as its
    // flow and is outstanding beside the hand-over.
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    let MetaResponse::Inspected(launching) = inspect(&fixture, seat_address()) else {
        panic!("the metaflow is inspected")
    };
    let State::Awake(launched) = &launching.metaflow.state else {
        panic!("the launch holds the Address")
    };
    assert_eq!(
        launching.binding.flow_node_option,
        fixture.nexus.store.flow_node(launched).unwrap()
    );
    assert!(
        matches!(
            launching.attempt_vector.as_slice(),
            [Attempt::Queued(delivery), Attempt::Launch(launch)]
                if delivery.queue_attempt_id == attempt_id
                    && launch.launch_request_id == SUCCESSOR_REQUEST
        ),
        "{:?}",
        launching.attempt_vector
    );

    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Asleep,
        past: vec![CLAUDE_SEAT.into()],
        queue: vec![],
    });
    let MetaResponse::Inspected(asleep) = inspect(&fixture, seat_address()) else {
        panic!("an Asleep metaflow is inspected")
    };
    assert_eq!(asleep.binding.flow_node_option, None);

    assert_eq!(
        inspect(&fixture, elsewhere()),
        MetaResponse::InspectRejected(InspectRejected_Data::UnknownAddress)
    );
}

/// Rebind.Pane records the pane the metaflow's flow lives in now, with the
/// terminal and agent Herdr shows there, so the flow is reached and
/// identified in that pane and no longer in the old one. A pane Herdr does
/// not show with the flow's harness is refused PaneAbsent; a Herdr whose
/// session cannot be read, HerdrUnavailable; an Asleep metaflow, NotAwake;
/// an Address Flow holds no metaflow for, UnknownAddress. Nothing is closed
/// or typed.
#[test]
fn rebind_pane_moves_the_current_flow_to_its_new_pane() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture, vec![]);
    let mut moved = claude_agent(&fixture);
    moved["pane_id"] = "w1:p9".into();
    moved["terminal_id"] = "term_moved".into();
    moved["name"] = "mind-flow-moved".into();
    fixture.set_agents(vec![moved]);
    let rebind = |rebinding: Rebinding, at: Address| {
        fixture.nexus.dispatch_meta(MetaQuery::Rebind(Rebind_Data {
            address: at,
            rebinding,
        }))
    };

    assert_eq!(
        rebind(Rebinding::Pane("w1:p9".into()), seat_address()),
        MetaResponse::Rebound(seat_address())
    );
    let node = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap().unwrap();
    assert_eq!(
        node.herdr_route_selection,
        HerdrRouteSelection::Available(HerdrRoute {
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: "mind-flow-moved".into(),
            herdr_pane_id: "w1:p9".into(),
            herdr_terminal_id: "term_moved".into(),
        })
    );
    let in_pane = |pane: &str| {
        fixture
            .nexus
            .store
            .flows_in_pane("messaging-build", pane)
            .unwrap()
            .into_iter()
            .map(|node| node.flow_id)
            .collect::<Vec<_>>()
    };
    assert!(in_pane("w1:p9").contains(&CLAUDE_SEAT.to_owned()));
    assert!(!in_pane("w1:p3").contains(&CLAUDE_SEAT.to_owned()));

    assert_eq!(
        rebind(Rebinding::Pane("w1:p404".into()), seat_address()),
        MetaResponse::RebindRejected(RebindRejected_Data::PaneAbsent)
    );
    assert_eq!(
        rebind(Rebinding::Pane("w1:p9".into()), elsewhere()),
        MetaResponse::RebindRejected(RebindRejected_Data::UnknownAddress)
    );
    // Herdr lists its sessions, but the snapshot of the flow's own fails.
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    assert!(script.contains("|| exit 64"), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        "|| exit 64",
        "&& { echo 'herdr: server unreachable' >&2; exit 1; }",
        1,
    ));
    assert_eq!(
        rebind(Rebinding::Pane("w1:p9".into()), seat_address()),
        MetaResponse::RebindRejected(RebindRejected_Data::HerdrUnavailable)
    );
    let mut asleep = stored(&fixture);
    asleep.state = State::Asleep;
    fixture.nexus.store.seed_metaflow(asleep);
    assert_eq!(
        rebind(Rebinding::Pane("w1:p9".into()), seat_address()),
        MetaResponse::RebindRejected(RebindRejected_Data::FlowNotLive)
    );
    assert!(!herdr_closed_anything(&fixture));
    assert!(fixture.typed().is_none());
}

/// Rebind.Flow makes another flow the metaflow's current flow and moves the
/// one it was Awake as to Past, keeping its queue, with no harness action.
/// A flow Flow holds no row for is refused FlowUnknown; a flow another
/// metaflow is Awake as, FlowTaken naming that metaflow; an Asleep
/// metaflow, NotAwake.
#[test]
fn rebind_flow_makes_another_flow_current() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture, vec![entry("rebind-1")]);
    fixture
        .nexus
        .store
        .register_flow(fixture.node())
        .expect("the other flow registered");
    let rebind = |flow_id: &str| {
        fixture.nexus.dispatch_meta(MetaQuery::Rebind(Rebind_Data {
            address: seat_address(),
            rebinding: Rebinding::Flow(flow_id.into()),
        }))
    };

    assert_eq!(
        rebind("ffffff"),
        MetaResponse::RebindRejected(RebindRejected_Data::FlowUnknown)
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));

    // The other flow is another metaflow's current flow: it is not taken.
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: elsewhere(),
        state: State::Awake("908786".into()),
        past: vec![],
        queue: vec![],
    });
    assert_eq!(
        rebind("908786"),
        MetaResponse::RebindRejected(RebindRejected_Data::FlowTaken(elsewhere()))
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: elsewhere(),
        state: State::Asleep,
        past: vec!["908786".into()],
        queue: vec![],
    });

    assert_eq!(rebind("908786"), MetaResponse::Rebound(seat_address()));
    let metaflow = stored(&fixture);
    assert_eq!(metaflow.state, State::Awake("908786".into()));
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
    assert_eq!(metaflow.queue, vec![entry("rebind-1")]);
    assert!(!herdr_closed_anything(&fixture));
    assert!(fixture.typed().is_none());

    let mut asleep = metaflow;
    asleep.state = State::Asleep;
    fixture.nexus.store.seed_metaflow(asleep);
    assert_eq!(rebind(CLAUDE_SEAT), MetaResponse::Rebound(seat_address()));
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
}

/// Withdraw keeps a prepared cohort indivisible. Once any member may have
/// reached the seat, the whole cohort stays recorded Uncertain, leaves the
/// retry queue, and is never handed over again. A request
/// not queued is refused NotQueued; an Address Flow holds no metaflow for,
/// UnknownAddress.
#[test]
fn withdraw_takes_one_request_out_and_closes_its_attempt() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture, vec![entry("withdraw-1")]);
    let Response::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &seat_address(),
            meta_signal_flow::Sender {
                address: seat_address(),
                flow_id: "repair-sender".into(),
            },
            Delivery::Queue,
            Message::Soft(Request::Order("Never handed over.".into())),
            None,
        )
        .expect("accepted")
    else {
        panic!("the request is queued")
    };
    let never = accepted.logical_delivery_id;
    let withdraw = |id: &str, at: Address| {
        fixture.nexus.dispatch_meta_serially(
            MetaQuery::Withdraw(Withdraw_Data {
                address: at,
                logical_delivery_id: id.into(),
            }),
            None,
        )
    };

    // The oldest is handed over and nothing is seen of it since.
    let attempt_id = prepared(&fixture);

    assert_eq!(
        withdraw(&never, seat_address()),
        MetaResponse::Withdrawn(never.clone())
    );
    assert!(stored(&fixture).queue.is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .logical_delivery_snapshot(&never)
            .unwrap()
            .expect("the sender's delivery is kept")
            .delivery_state,
        DeliveryState::Uncertain
    );

    assert_eq!(
        withdraw("withdraw-1", seat_address()),
        MetaResponse::WithdrawRejected(WithdrawRejected_Data::NotQueued)
    );
    assert!(stored(&fixture).queue.is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .queue_delivery_attempt(&attempt_id)
            .unwrap()
            .expect("the attempt is kept")
            .phase,
        QueueDeliveryPhase::Uncertain(None),
        "a hand-over that may have reached the seat is never taken for absent"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .outstanding_delivery(&seat_address())
            .unwrap(),
        None,
        "the attempt is closed"
    );
    let MetaResponse::Inspected(inspected) = inspect(&fixture, seat_address()) else {
        panic!("the metaflow is inspected")
    };
    assert_eq!(inspected.attempt_vector, vec![]);
    assert!(fixture.typed().is_none(), "nothing is handed over again");

    assert_eq!(
        withdraw("withdraw-1", elsewhere()),
        MetaResponse::WithdrawRejected(WithdrawRejected_Data::UnknownAddress)
    );
}

/// Store-only queue and metaflow repairs mutate Memory without touching Herdr.
#[test]
fn owner_repairs_queue_sleep_and_forget_without_harness_action() {
    use meta_signal_flow::{Enqueue_Data, Requeue_Data};
    let fixture = NexusFixture::new();
    awake_seat(&fixture, vec![entry("first")]);

    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Enqueue(Enqueue_Data {
                address: seat_address(),
                queue_entry: entry("second"),
            })),
        MetaResponse::Enqueued("second".into())
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Requeue(Requeue_Data {
                address: seat_address(),
                logical_delivery_id: "second".into(),
                position: 0,
            })),
        MetaResponse::Requeued("second".into())
    );
    assert_eq!(
        stored(&fixture).queue,
        vec![entry("second"), entry("first")]
    );

    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Sleep(seat_address())),
        MetaResponse::Slept(seat_address())
    );
    assert_eq!(stored(&fixture).state, State::Asleep);
    assert!(!herdr_closed_anything(&fixture));

    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Forget(seat_address())),
        MetaResponse::Forgotten(seat_address())
    );
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&seat_address())
            .unwrap()
            .is_none()
    );
    assert!(!herdr_closed_anything(&fixture));
}
