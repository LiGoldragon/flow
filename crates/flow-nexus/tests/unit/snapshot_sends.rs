use super::{GreetsOrdinarySocket, NexusFixture};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::{CarriesSignalFrames, Connection, ServesConnection};
use flow_ethos::{Aspect, Delivery, Model, Topic};
use signal_flow::{
    Address, DeliveryCursor, Input, Message, Output, Request, SentBy, VisibleSender,
};
use std::os::unix::net::UnixStream;
use std::time::Duration;

fn address(aspect: Aspect, topic: &str, model: Model) -> Address {
    Address {
        aspect,
        topic: Topic(topic.into()),
        model,
    }
}

fn accept(
    fixture: &NexusFixture,
    sender: &Address,
    recipient: &Address,
    text: &str,
) -> signal_flow::SendAcceptance {
    let signal_flow::PrivateSendResponse::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            recipient,
            SentBy::Sender(sender.clone()),
            Delivery::Queue,
            Message::Soft(Request::Order(text.into())),
            None,
        )
        .expect("logical delivery accepted")
    else {
        panic!("send is admitted")
    };
    accepted
}

fn snapshot_over_socket(fixture: &NexusFixture, cursor: Option<DeliveryCursor>) -> Output {
    let (peer, mut client) = UnixStream::pair().expect("snapshot socket pair");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            Connection::accepted(peer)
                .unwrap()
                .serve_ordinary(&fixture.nexus)
        });
        client.greet_ordinary();
        client.write_query(&Input::SnapshotSends(cursor)).unwrap();
        let response = client.read_response().expect("one snapshot frame");
        assert!(server.join().unwrap().is_ok());
        assert!(
            client.read_response().is_err(),
            "SnapshotSends closes after exactly one reply"
        );
        response
    })
}

#[test]
fn snapshot_sends_is_a_cursor_filtered_public_snapshot_and_then_eof() {
    let fixture = NexusFixture::new();
    let sender = address(Aspect::Field, "snapshotSender", Model::Luna);
    let recipient = address(Aspect::Mind, "snapshotRecipient", Model::Sol);
    use crate::store::metaflow::SeedsMetaflow;
    fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
        address: recipient.clone(),
        state: flow_ethos::State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let first = accept(&fixture, &sender, &recipient, "first");
    let second = accept(&fixture, &sender, &recipient, "second");

    let Output::SendsSnapshotted(initial) = snapshot_over_socket(&fixture, None) else {
        panic!("SnapshotSends answers SendsSnapshotted")
    };
    assert_eq!(initial.visible_delivery_snapshot_vector.len(), 2);
    assert!(initial.visible_delivery_snapshot_vector.iter().all(|row| {
        row.address == recipient && row.visible_sender == VisibleSender::Sender(sender.clone())
    }));

    let Output::SendsSnapshotted(after_first) =
        snapshot_over_socket(&fixture, Some(first.delivery_cursor))
    else {
        panic!("cursor snapshot answers SendsSnapshotted")
    };
    assert_eq!(
        after_first
            .visible_delivery_snapshot_vector
            .iter()
            .map(|row| row.request_id.clone())
            .collect::<Vec<_>>(),
        vec![second.request_id]
    );
    assert_eq!(after_first.delivery_cursor, second.delivery_cursor);
}
