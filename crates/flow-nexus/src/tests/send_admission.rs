use crate::sending::{AdmitsSendPayload, MeasuresSendPayload, MAX_SEND_PAYLOAD_BYTES};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::ReadsFlowMemory;
use flow_ethos::{Address, Aspect, Model, Request};

fn sender() -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: "send-admission-test".into(),
        model: Model::Astra,
    }
}

#[test]
fn rendered_native_prompt_budget_accepts_exact_boundary_and_refuses_one_byte_over() {
    let sender = sender();
    let overhead = Request::Order("a".into()).native_prompt_bytes(&sender) - 1;
    let exact = Request::Order("a".repeat(MAX_SEND_PAYLOAD_BYTES - overhead));
    assert_eq!(exact.native_prompt_bytes(&sender), MAX_SEND_PAYLOAD_BYTES);
    assert_eq!(exact.oversized_refusal(&sender), None);

    let over = Request::Order("a".repeat(MAX_SEND_PAYLOAD_BYTES - overhead + 1));
    assert_eq!(
        over.native_prompt_bytes(&sender),
        MAX_SEND_PAYLOAD_BYTES + 1
    );
    assert!(matches!(
        over.oversized_refusal(&sender),
        Some(signal_flow::Refusal::Oversized(signal_flow::SendPayloadSize {
            actual_bytes,
            maximum_bytes,
        })) if actual_bytes == (MAX_SEND_PAYLOAD_BYTES + 1) as i64
            && maximum_bytes == MAX_SEND_PAYLOAD_BYTES as i64
    ));
}

#[test]
fn rendered_native_prompt_budget_counts_multibyte_utf8_bytes() {
    let sender = sender();
    let overhead = Request::Order("a".into()).native_prompt_bytes(&sender) - 1;
    let payload_bytes = MAX_SEND_PAYLOAD_BYTES - overhead;
    let at_boundary = Request::Order("a".repeat(payload_bytes - 2) + "é");
    assert_eq!(
        at_boundary.native_prompt_bytes(&sender),
        MAX_SEND_PAYLOAD_BYTES
    );
    assert_eq!(at_boundary.oversized_refusal(&sender), None);

    let over = Request::Order("a".repeat(payload_bytes - 1) + "é");
    assert_eq!(
        over.native_prompt_bytes(&sender),
        MAX_SEND_PAYLOAD_BYTES + 1
    );
    assert!(over.oversized_refusal(&sender).is_some());
}

#[test]
fn an_existing_sleeping_recipient_keeps_admitted_work_when_its_wake_is_unconfigured() {
    use super::{
        bind_existing_caller, codex_agent_in, ControlsHerdrSnapshot, MarkedProcess, NexusFixture,
    };
    use crate::sending::SendsRequests;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::FlowStore;
    use signal_flow::{Message, Metaflow, Response, SendRequest};
    let fixture = NexusFixture::new();
    bind_existing_caller(&fixture, "a001", "pane-1");
    fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
    let target = Address {
        aspect: Aspect::Mind,
        topic: "directTarget".into(),
        model: Model::Sol,
    };
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: flow_ethos::State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let answer = fixture.nexus.send_request(
        SendRequest {
            recipient: target.clone(),
            sendable_message: super::sendable_message(Message::MiddleAbrupt(Request::Order(
                "wake when configured".into(),
            ))),
        },
        Some(&peer),
    );
    assert!(
        matches!(
            answer.response,
            Response::Failed(signal_flow::VisibleSendFailure::Admitted(
                signal_flow::VisibleSendFailure_Admitted_Data {
                    visible_send_failure_reason: signal_flow::VisibleSendFailureReason::NotSent(_),
                    ..
                }
            ))
        ),
        "{:?}",
        answer.response
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&target)
            .unwrap()
            .unwrap()
            .queue
            .len(),
        1,
        "wake configuration failure never discards admitted work"
    );
    let unknown = Address {
        topic: "unknownDirectTarget".into(),
        ..target.clone()
    };
    let answer = fixture.nexus.send_request(
        SendRequest {
            recipient: unknown.clone(),
            sendable_message: signal_flow::SendableMessage::HardAbrupt(
                signal_flow::SendableRequest::Notice("no hidden queue".into()),
            ),
        },
        Some(&peer),
    );
    assert!(matches!(
        answer.response,
        Response::Failed(signal_flow::VisibleSendFailure::Unadmitted(
            signal_flow::VisibleSendFailureReason::RecipientUnavailable(_)
        ))
    ));
    assert!(fixture.nexus.store.metaflow(&unknown).unwrap().is_none());
    let queued = fixture.nexus.send_request(
        SendRequest {
            recipient: target.clone(),
            sendable_message: signal_flow::SendableMessage::Soft(
                signal_flow::SendableRequest::Notice("explicit durable queue".into()),
            ),
        },
        Some(&peer),
    );
    assert!(matches!(queued.response, Response::SendAccepted(_)));
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&target)
            .unwrap()
            .unwrap()
            .queue
            .len(),
        2
    );
}

#[test]
fn an_admitted_send_holds_archive_until_its_answer_object_is_released() {
    use super::{
        bind_existing_caller, codex_agent_in, ControlsHerdrSnapshot, MarkedProcess, NexusFixture,
    };
    use crate::sending::SendsRequests;
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::queue_delivery::AccountsQueueDelivery;
    use crate::store::FlowStore;
    use signal_flow::{Message, Metaflow, PrivateSendResponse, SendRequest};

    let fixture = NexusFixture::new();
    bind_existing_caller(&fixture, "a001", "pane-1");
    fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
    let target = Address {
        aspect: Aspect::Mind,
        topic: "answerHoldTarget".into(),
        model: Model::Sol,
    };
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: flow_ethos::State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let answer = fixture.nexus.send_request(
        SendRequest {
            recipient: target.clone(),
            sendable_message: super::sendable_message(Message::Soft(Request::Notice(
                "hold the admission answer".into(),
            ))),
        },
        Some(&peer),
    );
    let PrivateSendResponse::SendAccepted(accepted) = &answer.private_response else {
        panic!("the Send is admitted")
    };
    let accepted = accepted.clone();
    assert_eq!(
        fixture
            .nexus
            .store
            .withdraw_queued(&target, &accepted.logical_delivery_id)
            .unwrap(),
        Ok(())
    );
    let sender = Address {
        aspect: Aspect::Mind,
        topic: "core".into(),
        model: Model::Sol,
    };
    assert_eq!(
        fixture
            .nexus
            .store
            .archive_request(&accepted.request_id, &sender)
            .unwrap(),
        Err(signal_flow::ArchiveRejection::NotYet)
    );
    drop(answer);
    assert_eq!(
        fixture
            .nexus
            .store
            .archive_request(&accepted.request_id, &sender)
            .unwrap(),
        Ok(())
    );
}

#[test]
fn an_active_abrupt_send_returns_sent_only_after_exact_native_receipt() {
    use super::waking::{
        append_main_rows, claude_agent, claude_seat, claude_transcript, end_turn, CLAUDE_SEAT,
        CLAUDE_SESSION,
    };
    use super::{
        bind_existing_caller, codex_agent_in, ControlsHerdrSnapshot, MarkedProcess, NexusFixture,
        PromptFixture,
    };
    use crate::sending::SendsRequests;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::FlowStore;
    use signal_flow::{Message, Metaflow, Response, SendRequest};
    use std::io::Write;
    use std::time::{Duration, Instant};
    for (hard, duplicate, privileged) in [
        (true, false, false),
        (false, false, false),
        (true, true, false),
        (true, false, true),
        (false, false, true),
    ] {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        claude_seat(&fixture, 40_000);
        fixture.accept_pane_operations(
            vec![
                codex_agent_in("pane-1", "terminal-pane-1"),
                claude_agent(&fixture),
            ],
            if duplicate {
                PromptFixture::Prompted("w1:p3")
            } else {
                PromptFixture::PromptedUnrecorded("w1:p3")
            },
        );
        let target = Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            model: Model::Sol,
        };
        fixture.nexus.store.seed_metaflow(Metaflow {
            address: target.clone(),
            state: flow_ethos::State::Awake(CLAUDE_SEAT.into()),
            past: vec![],
            queue: vec![],
        });
        append_main_rows(&fixture, &[end_turn("direct-before")]);
        let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
        let transcript = claude_transcript(&fixture);
        let typed = fixture.directory.path().join("pane-typed.txt");
        let writer = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(12);
            while !std::fs::read_to_string(&typed)
                .unwrap_or_default()
                .contains("real direct receipt")
            {
                assert!(Instant::now() < deadline, "direct message never submitted");
                std::thread::sleep(Duration::from_millis(10));
            }
            // The seat records the very text typed: the message's datom.
            let typed_text = std::fs::read_to_string(&typed).unwrap();
            let row = serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"direct-native-receipt", "message":{"content":typed_text}});
            writeln!(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(transcript)
                    .unwrap(),
                "{row}"
            )
            .unwrap();
        });
        let request = Request::Notice("real direct receipt".into());
        let message = if hard {
            Message::HardAbrupt(request)
        } else {
            Message::MiddleAbrupt(request)
        };
        let answer = if privileged {
            use crate::metaflow::DispatchesMetaflowPeer;
            let (_message_child, message_peer) = super::waking::message_peer(&fixture);
            let Response::Locked(lock) = fixture
                .nexus
                .dispatch_metaflow_peer(
                    signal_flow::Query::Lock(signal_flow::Lock_Data {
                        sender: Address {
                            aspect: Aspect::Mind,
                            topic: "core".into(),
                            model: Model::Sol,
                        },
                        recipient: target.clone(),
                    }),
                    Some(&message_peer),
                )
                .response
            else {
                panic!("authenticated Message lease")
            };
            fixture
                .nexus
                .dispatch_metaflow_peer(
                    signal_flow::Query::Deliver(signal_flow::Deliver_Data {
                        lock,
                        deliver_key: super::deliver_key(
                            i64::from(hard) * 4 + i64::from(duplicate) * 2 + i64::from(privileged),
                        ),
                        sendable_message: super::sendable_message(message),
                    }),
                    Some(&message_peer),
                )
                .response
        } else {
            let answer = fixture.nexus.send_request(
                SendRequest {
                    recipient: target.clone(),
                    sendable_message: super::sendable_message(message),
                },
                Some(&peer),
            );
            assert!(answer.pending.is_none());
            answer.response
        };
        writer.join().unwrap();
        if duplicate {
            let Response::SendAccepted(accepted) = answer else {
                panic!("uncertain admitted send: {answer:?}")
            };
            assert_eq!(
                accepted.visible_delivery_state,
                signal_flow::VisibleDeliveryState::Uncertain
            );
            use crate::store::send_outcome::RecordsSendOutcomes;
            let outcomes = fixture.nexus.store.read_send_outcomes().unwrap();
            let meta_signal_flow::OrdinarySendOutcome::Accepted(durable) =
                &outcomes.last().unwrap().ordinary_send_outcome
            else {
                panic!("uncertain send remains an acceptance")
            };
            assert_eq!(durable.delivery_cursor, accepted.delivery_cursor);
            assert_eq!(
                durable.delivery_state,
                signal_flow::DeliveryState::Uncertain
            );
            continue;
        }
        if privileged {
            let Response::SendAccepted(accepted) = answer else {
                panic!("Deliver answers its admission: {answer:?}")
            };
            use crate::store::logical_delivery::{MintsRequestIds, RecordsLogicalDeliveries};
            let binding = fixture
                .nexus
                .store
                .request_binding(&accepted.request_id)
                .unwrap()
                .expect("the admitted request remains bound");
            let snapshot = fixture
                .nexus
                .store
                .logical_delivery_snapshot(&binding.logical_delivery_id)
                .unwrap()
                .expect("the admitted delivery remains observable");
            assert!(matches!(
                snapshot.delivery_state,
                signal_flow::DeliveryState::Delivered(_)
            ));
            assert!(snapshot.delivery_cursor > accepted.delivery_cursor);
            assert!(fixture
                .nexus
                .store
                .metaflow(&target)
                .unwrap()
                .unwrap()
                .queue
                .is_empty());
            continue;
        }
        let Response::Sent(receipt) = answer else {
            use crate::store::queue_delivery::AccountsQueueDelivery;
            use crate::waking::{ReadsClaudeDelivery, RendersAttemptInput};
            let attempts = fixture.nexus.store.unsettled_deliveries(&target).unwrap();
            let histories: Vec<_> = attempts
                .iter()
                .map(|attempt| {
                    fixture
                        .nexus
                        .claude_delivery_history(attempt, &attempt.prompt_text())
                })
                .collect();
            panic!(
                "direct reply {:?}; attempts {attempts:?}; native {histories:?}",
                answer
            )
        };
        assert_eq!(receipt.len(), 4);
        assert!(fixture
            .nexus
            .store
            .metaflow(&target)
            .unwrap()
            .unwrap()
            .queue
            .is_empty());
    }
}

#[test]
fn direct_start_preflight_materializes_only_configured_working_state() {
    use super::NexusFixture;
    use crate::sending::CompletesDirectSends;
    use crate::store::ManagesMetaflowState;
    let fixture = NexusFixture::new();
    let target = Address {
        aspect: Aspect::Mind,
        topic: "newConfiguredDirect".into(),
        model: Model::Sol,
    };
    assert!(fixture.nexus.direct_send_lease(&target).is_err());
    assert!(fixture.nexus.store.metaflow(&target).unwrap().is_none());
    let declaration = meta_signal_flow::ConfigureMetaflow {
        root_option: None,
        address: target.clone(),
        system_modules: vec!["spirit-core".into()],
        user_modules: vec![],
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        continuity: meta_signal_flow::Continuity::OnDemand,
        purpose: String::new(),
        records: vec![],
    };
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                model: Model::Sol,
                native: "gpt-6-sol".into(),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
            declaration.clone(),
        ]))
        .unwrap();
    let lease = fixture
        .nexus
        .direct_send_lease(&target)
        .expect("configured address can start");
    let working = fixture.nexus.store.metaflow(&target).unwrap().unwrap();
    assert_eq!(working.state, flow_ethos::State::Asleep);
    assert!(working.queue.is_empty());
    use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
    assert_eq!(
        fixture.nexus.store.metaflow_declaration(&target).unwrap(),
        Some(declaration)
    );
    drop(lease);
}

#[test]
fn a_task_is_admitted_only_with_minutes_above_zero_and_a_reachable_deadline() {
    use super::NexusFixture;
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::SeedsMetaflow;
    use signal_flow::{Message, Metaflow, MinutesRefusal, Refusal, SentBy};
    let fixture = NexusFixture::new();
    let target = Address {
        aspect: Aspect::Mind,
        topic: "minutesTarget".into(),
        model: Model::Sol,
    };
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: flow_ethos::State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let admit = |minutes: i64| {
        fixture
            .nexus
            .store
            .accept_logical_delivery(
                &target,
                SentBy::Sender(sender()),
                flow_ethos::Delivery::Queue,
                Message::Soft(Request::Task(flow_ethos::AnswerableTask {
                    minutes,
                    string: "Draft the proposal.".into(),
                })),
                None,
            )
            .unwrap()
    };
    for (minutes, refusal) in [
        (0, MinutesRefusal::Zero),
        (-1, MinutesRefusal::Negative),
        (i64::MIN, MinutesRefusal::Negative),
        (i64::MAX, MinutesRefusal::Unreachable),
        (i64::MAX / 60, MinutesRefusal::Unreachable),
    ] {
        assert_eq!(
            admit(minutes),
            signal_flow::PrivateSendResponse::Refused(Refusal::MinutesRefused(refusal)),
            "{minutes} minutes"
        );
    }
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&target)
            .unwrap()
            .unwrap()
            .queue
            .is_empty(),
        "a refused Task is never queued"
    );
    assert!(matches!(
        admit(30),
        signal_flow::PrivateSendResponse::SendAccepted(_)
    ));
}

/// The world a CLI send runs in: the flow CLI built beside this test, the
/// sending seat 5e4de7 Awake as `{ Field flow Terra }` in pane w5:p1, which
/// Herdr shows, and the recipient `{ Mind flow Sol }` Asleep; the Nexus
/// serving its ordinary socket, and its meta socket, in the fixture
/// directory.
struct CliWorld {
    fixture: &'static super::NexusFixture,
    /// The directory the flow and flow-meta CLIs are built in.
    binaries: std::path::PathBuf,
    socket: std::path::PathBuf,
    meta_socket: std::path::PathBuf,
}

trait ServesCliWorld: Sized {
    fn serving() -> Self;
    /// The flow CLI run in the sender's pane, its pane ID alone marked:
    /// its one line of output.
    fn flow(&self, text: &str) -> String;
    /// The flow-meta CLI run in no pane, the owner: its one line of output.
    fn flow_meta(&self, text: &str) -> String;
}

impl ServesCliWorld for CliWorld {
    fn serving() -> Self {
        use super::waking::{ended_claude, metaflow};
        use super::NexusFixture;
        use crate::fixture_executable::{FixtureExecutable, InstallsScript};
        use crate::store::metaflow::{SeedsMetaflow, TakesLaunchAddress};
        use crate::store::RegistersFlowIdentity;
        use crate::{ServesMeta, ServesOrdinary};
        use signal_flow::{
            EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind, HerdrRoute,
            HerdrRouteSelection, Origin,
        };

        // The flow CLI is built in the profile directory this test
        // executable lies under: `target/<profile>/flow`, whatever depth
        // cargo gives the test itself.
        let executable = std::env::current_exe().expect("test executable");
        let binaries = executable
            .ancestors()
            .skip(1)
            .find(|directory| directory.join("flow").is_file())
            .unwrap_or_else(|| {
                panic!(
                    "no flow CLI above {}: cargo test --workspace builds it",
                    executable.display()
                )
            })
            .to_path_buf();

        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        ended_claude(fixture);
        // The sending seat: a flow Flow launched into pane w5:p1, Awake as
        // Field flow Terra.
        let sender_address = Address {
            aspect: Aspect::Field,
            topic: "flow".into(),
            model: Model::Terra,
        };
        fixture
            .nexus
            .store
            .register_flow_in_role(
                FlowNode {
                    flow_id: "5e4de7".into(),
                    session_id: "5e4de700-0000-4000-8000-000000000001".into(),
                    harness_kind: HarnessKind::Claude,
                    endpoint_selection: EndpointSelection::Unavailable,
                    herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                        herdr_session_name: "messaging-build".into(),
                        herdr_agent_name: "sender-agent".into(),
                        herdr_pane_id: "w5:p1".into(),
                        herdr_terminal_id: "term-sender".into(),
                    }),
                    origin: Origin::Flow(FlowOrigin {
                        flow_id: "da2038".into(),
                        session_id: "field-session".into(),
                    }),
                    flow_lifecycle: FlowLifecycle::Active,
                },
                signal_flow::Caller {
                    flow_id: "5e4de7".into(),
                    aspect: Aspect::Field,
                    model_name: "haiku".into(),
                },
            )
            .expect("sender registered");
        assert!(matches!(
            fixture
                .nexus
                .store
                .address_taking(&sender_address, "5e4de7", None, 0)
                .unwrap(),
            crate::store::metaflow::AddressTaking::Takes(_)
        ));
        assert!(fixture
            .nexus
            .store
            .awaken_metaflow(&sender_address, "5e4de7", 0)
            .unwrap());
        // Herdr shows the sender's pane.
        let script = std::fs::read_to_string(&fixture.snapshot_program).unwrap();
        let sender_agent = r#"{"agent":"claude","agent_status":"working","name":"sender-agent","pane_id":"w5:p1","terminal_id":"term-sender","workspace_id":"w5"}"#;
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&script.replace(r#""agents":[]"#, &format!(r#""agents":[{sender_agent}]"#)));
        // The recipient: Mind flow Sol, Asleep.
        fixture.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            flow_ethos::State::Asleep,
            vec!["abc123"],
            vec![],
        ));

        let socket = fixture.directory.path().join("ordinary.sock");
        let meta_socket = fixture.directory.path().join("meta.sock");
        let (served, meta_served) = (socket.clone(), meta_socket.clone());
        std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
        std::thread::spawn(move || fixture.nexus.serve_meta(&meta_served));
        for path in [&socket, &meta_socket] {
            for _ in 0..200 {
                if std::os::unix::net::UnixStream::connect(path).is_ok() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        CliWorld {
            fixture,
            binaries,
            socket,
            meta_socket,
        }
    }

    fn flow(&self, text: &str) -> String {
        let output = std::process::Command::new(self.binaries.join("flow"))
            .env_clear()
            .env("HERDR_PANE_ID", "w5:p1")
            .arg(format!("{{ «{}» {text} }}", self.socket.to_string_lossy()))
            .output()
            .expect("flow runs");
        assert!(output.status.success(), "{text}: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    fn flow_meta(&self, text: &str) -> String {
        let output = std::process::Command::new(self.binaries.join("flow-meta"))
            .env_clear()
            .arg(format!(
                "{{ «{}» {text} }}",
                self.meta_socket.to_string_lossy()
            ))
            .output()
            .expect("flow-meta runs");
        assert!(output.status.success(), "{text}: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }
}

/// A Task whose Minutes the Nexus refuses makes the whole trip: the flow
/// CLI, standing in a flow's pane, reads the datom text and sends it over
/// the ordinary socket; the Nexus names the sender by its pane, refuses
/// the Minutes at admission, and the CLI prints the refusal as datom text.
/// The refused Task is never queued.
#[test]
fn a_zero_minutes_task_from_the_cli_is_refused_minutes_refused_by_the_nexus() {
    use super::waking::address;
    let world = CliWorld::serving();
    let recipient = address(Model::Sol);
    assert_eq!(
        world.flow("Send.{ { Mind flow Sol } Soft.Task.{ 0 «Draft the proposal.» } }"),
        "SendRefused.MinutesRefused.Zero"
    );
    assert_eq!(
        world.flow("Send.{ { Mind flow Sol } Soft.Task.{ -5 «Draft the proposal.» } }"),
        "SendRefused.MinutesRefused.Negative"
    );
    assert!(
        world
            .fixture
            .nexus
            .store
            .metaflow(&recipient)
            .unwrap()
            .unwrap()
            .queue
            .is_empty(),
        "a refused Task is never queued"
    );
    // The same CLI and Nexus admit a Task whose Minutes stand.
    assert!(world
        .flow("Send.{ { Mind flow Sol } Soft.Task.{ 30 «Draft the proposal.» } }")
        .starts_with("SendAccepted."),);
}

/// A Send whose text holds a nested guillemet pair, and one holding an
/// escaped unbalanced guillemet, round-trips byte for byte: the flow CLI in
/// the sender's pane reads the datom text and sends it on the ordinary
/// socket; the Nexus names the sender, admits the Send and queues it for
/// the recipient, Asleep; the queue holds the string as written; and the
/// flow-meta CLI, reading the recipient over the meta socket, prints each
/// message back as the very text sent.
#[test]
fn a_send_with_nested_and_escaped_guillemets_round_trips_through_the_cli_and_the_nexus() {
    use super::waking::address;
    let world = CliWorld::serving();
    let sent = [
        (
            "Soft.Order.«Title «The Belief Nexus made small»»",
            "Title «The Belief Nexus made small»",
        ),
        (
            "Soft.Order.«an unbalanced \\» inside»",
            "an unbalanced » inside",
        ),
    ];
    for (message, _) in sent {
        let answer = world.flow(&format!("Send.{{ {{ Mind flow Sol }} {message} }}"));
        assert!(answer.starts_with("SendAccepted."), "{message}: {answer}");
    }
    let queued: Vec<signal_flow::Message> = world
        .fixture
        .nexus
        .store
        .metaflow(&address(Model::Sol))
        .unwrap()
        .unwrap()
        .queue
        .into_iter()
        .map(|entry| entry.message)
        .collect();
    assert_eq!(
        queued,
        sent.iter()
            .map(|(_, order)| signal_flow::Message::Soft(Request::Order((*order).into())))
            .collect::<Vec<_>>(),
        "the Nexus holds each string as written"
    );
    let inspected = world.flow_meta("Inspect.{ Mind flow Sol }");
    for (message, words) in sent {
        let needle = if words.starts_with("Title") {
            "Belief Nexus"
        } else {
            "unbalanced"
        };
        assert!(
            inspected.contains(needle),
            "{message} is printed back as sent: {inspected}"
        );
    }
}
