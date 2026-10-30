#![cfg(feature = "datom")]

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_ethos::message::{Message, Request, Sender, Waking};
use protos::{Compactable, Protosizable, ReaderBudget};

// The waking of the ruled contract (bie3-1 2.5): four inputs, oldest first,
// one top-level vector; the same Address under two FlowIds, the living, and
// Flow itself.
const WAKING: &str = "[ { logical-delivery-7 Flow.{ { Psyche vision Primary } f94581 } Soft.Order.{ 30 «Draft the proposal.» } } { logical-delivery-8 Living Soft.Psyche.[ { «a context» «the living said { braces }» } ] } { logical-delivery-9 Flow.{ { Psyche vision Primary } e3a1b2 } MiddleAbrupt.Result.{ logical-delivery-7 «Done.» } } { logical-delivery-10 Nexus HardAbrupt.Notice.«a datom inside: { x }» } ]";

fn budget() -> Budget {
    Budget {
        remaining: 4096,
        reader: ReaderBudget { remaining: 4096 },
        depth: 0,
        maximum_depth: 1024,
    }
}

fn waking() -> Waking {
    Potential::<Waking>::from(WAKING)
        .actualize(&mut budget())
        .unwrap()
}

#[test]
fn a_waking_datom_composes_and_prints_back_unchanged() {
    assert_eq!(waking().datomize(vec![]).protosize().compact(), WAKING);
}

#[test]
fn a_waking_keeps_each_sender_as_admitted() {
    let senders: Vec<String> = waking()
        .iter()
        .map(|input| match &input.sender {
            Sender::Flow(flow) => flow.flow_id.clone(),
            Sender::Living => "Living".to_owned(),
            Sender::Nexus => "Nexus".to_owned(),
        })
        .collect();
    assert_eq!(senders, ["f94581", "Living", "e3a1b2", "Nexus"]);
}

#[test]
fn an_order_carries_its_minutes_and_a_result_names_its_order() {
    let inputs = waking();
    let Message::Soft(Request::Order(order)) = &inputs[0].message else {
        panic!("the first input is a Soft Order");
    };
    assert_eq!(order.minutes, 30);
    let Message::MiddleAbrupt(Request::Result(result)) = &inputs[2].message else {
        panic!("the third input is a MiddleAbrupt Result");
    };
    assert_eq!(result.logical_delivery_id, inputs[0].logical_delivery_id);
}

#[test]
fn a_waking_round_trips_through_its_archive() {
    let inputs = waking();
    let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&inputs).unwrap();
    assert_eq!(
        rkyv::from_bytes::<Waking, rkyv::rancor::Error>(&archive).unwrap(),
        inputs
    );
}
