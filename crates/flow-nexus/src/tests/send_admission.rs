use crate::sending::{AdmitsSendPayload, MAX_SEND_PAYLOAD_BYTES, MeasuresSendPayload};
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
    let base = Request::Order(String::new()).native_prompt_bytes(&sender);
    let exact = Request::Order("a".repeat(MAX_SEND_PAYLOAD_BYTES - base));
    assert_eq!(exact.native_prompt_bytes(&sender), MAX_SEND_PAYLOAD_BYTES);
    assert_eq!(exact.oversized_refusal(&sender), None);

    let over = Request::Order("a".repeat(MAX_SEND_PAYLOAD_BYTES - base + 1));
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
    let base = Request::Order(String::new()).native_prompt_bytes(&sender);
    let at_boundary = Request::Order("a".repeat(MAX_SEND_PAYLOAD_BYTES - base - 2) + "é");
    assert_eq!(
        at_boundary.native_prompt_bytes(&sender),
        MAX_SEND_PAYLOAD_BYTES
    );
    assert_eq!(at_boundary.oversized_refusal(&sender), None);

    let over = Request::Order("a".repeat(MAX_SEND_PAYLOAD_BYTES - base - 1) + "é");
    assert_eq!(
        over.native_prompt_bytes(&sender),
        MAX_SEND_PAYLOAD_BYTES + 1
    );
    assert!(over.oversized_refusal(&sender).is_some());
}
