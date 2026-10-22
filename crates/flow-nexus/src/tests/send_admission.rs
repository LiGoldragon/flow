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
        topic: "direct-target".into(),
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
    for (hard, duplicate) in [(true, false), (false, false), (true, true)] {
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
            let row = serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"direct-native-receipt", "message":{"content":"real direct receipt"}});
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
        let answer = fixture.nexus.send_request(
            SendRequest {
                recipient: Recipient::Address(target.clone()),
                delivery: Delivery::Wake,
                message: if hard {
                    Message::HardAbrupt(request)
                } else {
                    Message::MiddleAbrupt(request)
                },
            },
            Some(&peer),
        );
        writer.join().unwrap();
        if duplicate {
            assert!(
                matches!(
                    answer.response,
                    Response::Failed(signal_flow::SendFailure {
                        send_failure_reason: signal_flow::SendFailureReason::ReceiptUncertain,
                        ..
                    })
                ),
                "{:?}",
                answer.response
            );
            continue;
        }
        let Response::Sent(receipt) = answer.response else {
            use crate::store::queue_delivery::AccountsQueueDelivery;
            use crate::waking::ReadsClaudeDelivery;
            let attempts = fixture.nexus.store.unsettled_deliveries(&target).unwrap();
            let histories: Vec<_> = attempts
                .iter()
                .map(|attempt| fixture.nexus.claude_delivery_history(attempt))
                .collect();
            panic!(
                "direct reply {:?}; attempts {attempts:?}; native {histories:?}",
                answer.response
            )
        };
        assert_eq!(receipt.native_receipt.native_session_id, CLAUDE_SESSION);
        assert_eq!(
            receipt.native_receipt.native_prompt_id,
            "direct-native-receipt"
        );
        assert!(!receipt.logical_delivery_id.is_empty());
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
}
