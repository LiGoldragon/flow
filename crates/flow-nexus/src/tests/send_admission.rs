use crate::sending::{AdmitsSendPayload, MAX_SEND_PAYLOAD_BYTES, MeasuresSendPayload};
use crate::store::ReadsFlowMemory;
use crate::store::metaflow::SleepsMetaflows;
use flow_ethos::{Address, Aspect, Layer, Request};

fn sender() -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: "send-admission-test".into(),
        layer: Layer::Primary,
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
fn direct_send_to_sleeping_or_unknown_recipient_fails_without_queueing() {
    use super::{
        ControlsHerdrSnapshot, MarkedProcess, NexusFixture, bind_existing_caller, codex_agent_in,
    };
    use crate::sending::SendsRequests;
    use crate::store::FlowStore;
    use crate::store::metaflow::SeedsMetaflow;
    use signal_flow::{
        Delivery, Message, Metaflow, Recipient, Response, SendFailureReason, SendRequest,
    };
    let fixture = NexusFixture::new();
    bind_existing_caller(&fixture, "a001", "pane-1");
    fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
    let target = Address {
        aspect: Aspect::Mind,
        topic: "directTarget".into(),
        layer: Layer::Secondary,
    };
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: flow_ethos::State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    for message in [
        Message::HardAbrupt(Request::Order("direct hard".into())),
        Message::MiddleAbrupt(Request::Order("direct middle".into())),
    ] {
        let answer = fixture.nexus.send_request(
            SendRequest {
                recipient: Recipient::Address(target.clone()),
                delivery: Delivery::Wake,
                message,
            },
            Some(&peer),
        );
        assert!(
            matches!(
                answer.response,
                Response::Failed(signal_flow::SendFailure {
                    logical_delivery_id_option: None,
                    send_failure_reason: SendFailureReason::RecipientUnavailable(_),
                })
            ),
            "{:?}",
            answer.response
        );
        assert!(answer.pending.is_none());
        assert!(
            fixture
                .nexus
                .store
                .metaflow(&target)
                .unwrap()
                .unwrap()
                .queue
                .is_empty()
        );
    }
    let unknown = Address {
        topic: "unknown-direct-target".into(),
        ..target.clone()
    };
    let answer = fixture.nexus.send_request(
        SendRequest {
            recipient: Recipient::Address(unknown.clone()),
            delivery: Delivery::Wake,
            message: Message::HardAbrupt(Request::Notice("no hidden queue".into())),
        },
        Some(&peer),
    );
    assert!(matches!(answer.response, Response::Failed(_)));
    assert!(fixture.nexus.store.metaflow(&unknown).unwrap().is_none());
    for message in [
        Message::HardAbrupt(Request::Notice("hard queued".into())),
        Message::MiddleAbrupt(Request::Notice("middle queued".into())),
    ] {
        let answer = fixture.nexus.send_request(
            SendRequest {
                recipient: Recipient::Address(target.clone()),
                delivery: Delivery::Queue,
                message,
            },
            Some(&peer),
        );
        assert!(matches!(
            answer.response,
            Response::Failed(signal_flow::SendFailure {
                send_failure_reason: SendFailureReason::QueueNotPermitted,
                ..
            })
        ));
        assert!(answer.pending.is_none());
    }
    let queued = fixture.nexus.send_request(
        SendRequest {
            recipient: Recipient::Address(target.clone()),
            delivery: Delivery::Queue,
            message: Message::Soft(Request::Notice("explicit durable queue".into())),
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
        1
    );
}

#[test]
fn an_active_abrupt_send_returns_sent_only_after_exact_native_receipt() {
    use super::waking::{
        CLAUDE_SEAT, CLAUDE_SESSION, append_main_rows, claude_agent, claude_seat,
        claude_transcript, end_turn,
    };
    use super::{
        ControlsHerdrSnapshot, MarkedProcess, NexusFixture, PromptFixture, bind_existing_caller,
        codex_agent_in,
    };
    use crate::sending::SendsRequests;
    use crate::store::FlowStore;
    use crate::store::metaflow::SeedsMetaflow;
    use signal_flow::{Delivery, Message, Metaflow, Recipient, Response, SendRequest};
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
            layer: Layer::Secondary,
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
                            layer: Layer::Secondary,
                        },
                        recipient: Recipient::Address(target.clone()),
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
                        delivery: Delivery::Wake,
                        message,
                    }),
                    Some(&message_peer),
                )
                .response
        } else {
            let answer = fixture.nexus.send_request(
                SendRequest {
                    recipient: Recipient::Address(target.clone()),
                    delivery: Delivery::Wake,
                    message,
                },
                Some(&peer),
            );
            assert!(answer.pending.is_none());
            answer.response
        };
        writer.join().unwrap();
        if duplicate {
            assert!(
                matches!(
                    answer,
                    Response::Failed(signal_flow::SendFailure {
                        send_failure_reason: signal_flow::SendFailureReason::ReceiptUncertain,
                        ..
                    })
                ),
                "{:?}",
                answer
            );
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
        assert_eq!(receipt.native_receipt.native_session_id, CLAUDE_SESSION);
        assert_eq!(
            receipt.native_receipt.native_prompt_id,
            "direct-native-receipt"
        );
        assert!(!receipt.logical_delivery_id.is_empty());
        assert!(
            fixture
                .nexus
                .store
                .metaflow(&target)
                .unwrap()
                .unwrap()
                .queue
                .is_empty()
        );
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
        layer: Layer::Secondary,
    };
    assert!(fixture.nexus.direct_send_lease(&target).is_err());
    assert!(fixture.nexus.store.metaflow(&target).unwrap().is_none());
    let declaration = meta_signal_flow::ConfigureMetaflow {
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
                layer: Layer::Secondary,
                aspect: Aspect::Mind,
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
    use signal_flow::{Delivery, Message, Metaflow, MinutesRefusal, Refusal, Response, SentBy};
    let fixture = NexusFixture::new();
    let target = Address {
        aspect: Aspect::Mind,
        topic: "minutesTarget".into(),
        layer: Layer::Secondary,
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
                Delivery::Queue,
                Message::Soft(Request::Task(flow_ethos::Task_Data {
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
            Response::Refused(Refusal::MinutesRefused(refusal)),
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
    assert!(matches!(admit(30), Response::SendAccepted(_)));
}

/// A Task whose Minutes the Nexus refuses makes the whole trip: the flow
/// CLI, standing in a flow's pane, reads the datom text and sends it over
/// the ordinary socket; the Nexus names the sender by its pane, refuses
/// the Minutes at admission, and the CLI prints the refusal as datom text.
/// The refused Task is never queued.
#[test]
fn a_zero_minutes_task_from_the_cli_is_refused_minutes_refused_by_the_nexus() {
    use super::NexusFixture;
    use super::waking::{address, ended_claude, metaflow};
    use crate::ServesOrdinary;
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::store::RegistersFlowIdentity;
    use crate::store::metaflow::{SeedsMetaflow, TakesLaunchAddress};
    use signal_flow::{
        EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind, HerdrRoute,
        HerdrRouteSelection, Origin, PowerLevel,
    };

    // The flow CLI is built in the profile directory this test executable
    // lies under: `target/<profile>/flow`, whatever depth cargo gives the
    // test itself.
    let executable = std::env::current_exe().expect("test executable");
    let cli = executable
        .ancestors()
        .skip(1)
        .map(|directory| directory.join("flow"))
        .find(|path| path.is_file())
        .unwrap_or_else(|| {
            panic!(
                "no flow CLI above {}: cargo test --workspace builds it",
                executable.display()
            )
        });

    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    ended_claude(fixture);
    // The sending seat: a flow Flow launched into pane w5:p1, Awake as
    // Field flow Quaternary.
    let sender_address = Address {
        aspect: Aspect::Field,
        topic: "flow".into(),
        layer: Layer::Quaternary,
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
                power_level: PowerLevel::UltraLow,
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
    assert!(
        fixture
            .nexus
            .store
            .awaken_metaflow(&sender_address, "5e4de7", 0)
            .unwrap()
    );
    // Herdr shows the sender's pane.
    let script = std::fs::read_to_string(&fixture.snapshot_program).unwrap();
    let sender_agent = r#"{"agent":"claude","agent_status":"working","name":"sender-agent","pane_id":"w5:p1","terminal_id":"term-sender","workspace_id":"w5"}"#;
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replace(r#""agents":[]"#, &format!(r#""agents":[{sender_agent}]"#)));
    // The recipient: Mind flow Secondary, Asleep.
    let recipient = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        flow_ethos::State::Asleep,
        vec!["abc123"],
        vec![],
    ));

    let socket = fixture.directory.path().join("ordinary.sock");
    let served = socket.clone();
    std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
    for _ in 0..200 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // The CLI runs in the sender's pane, its pane ID alone marked.
    let flow = |text: &str| -> String {
        let output = std::process::Command::new(&cli)
            .env_clear()
            .env("FLOW_SOCKET", &socket)
            .env("HERDR_PANE_ID", "w5:p1")
            .arg(text)
            .output()
            .expect("flow runs");
        assert!(output.status.success(), "{text}: {output:?}");
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    };
    assert_eq!(
        flow("Send.{ Address.{ Mind flow Secondary } Wake Soft.Task.{ 0 «Draft the proposal.» } }"),
        "Refused.MinutesRefused.Zero"
    );
    assert_eq!(
        flow(
            "Send.{ Address.{ Mind flow Secondary } Queue Soft.Task.{ -5 «Draft the proposal.» } }"
        ),
        "Refused.MinutesRefused.Negative"
    );
    assert!(
        fixture
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
    assert!(
        flow(
            "Send.{ Address.{ Mind flow Secondary } Queue Soft.Task.{ 30 «Draft the proposal.» } }"
        )
        .starts_with("SendAccepted."),
    );
}
