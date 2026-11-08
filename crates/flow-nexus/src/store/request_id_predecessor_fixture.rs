//! Injected only into the exact accepted f7 predecessor by the migration
//! check. It uses that source's production FlowStore capabilities to write a
//! nonempty Memory; it is not compiled into the current Nexus.

use super::*;
use crate::heartbeat::DeliveryPurpose;
use crate::store::logical_delivery::{RecordsLogicalDeliveries, SimplifiedReplyBinding};
use crate::store::metaflow::SeedsMetaflow;
use crate::store::queue_delivery::AccountsQueueDelivery;
use signal_flow::{
    Address, Aspect, DeliveryState, Message, Metaflow, Model, NativeReceipt, Request, SentBy,
};

#[test]
#[ignore = "the exact predecessor derivation supplies the private output path"]
fn writes_nonempty_request_id_predecessor_memory() {
    let output = std::path::PathBuf::from(
        std::env::var_os("FLOW_PREDECESSOR_FIXTURE_OUT")
            .expect("FLOW_PREDECESSOR_FIXTURE_OUT is required"),
    );
    std::fs::create_dir_all(&output).expect("fixture output directory creates");
    let store_path = output.join("flow.sema");
    let store = <FlowStore as OpensFlowStore>::open_configured(&store_path)
        .expect("the exact predecessor process configured Memory before fixture writes");
    let sender = Address {
        aspect: Aspect::Mind,
        topic: "migrationSender".into(),
        model: Model::Sol,
    };
    let recipient = Address {
        aspect: Aspect::Field,
        topic: "migrationRecipient".into(),
        model: Model::Sol,
    };
    for address in [&sender, &recipient] {
        store.seed_metaflow(Metaflow {
            address: address.clone(),
            state: flow_ethos::State::Asleep,
            past: Vec::new(),
            queue: Vec::new(),
        });
    }
    let admit = |address: &Address, sent_by: SentBy, message: Message| {
        let signal_flow::PrivateSendResponse::SendAccepted(value) = store
            .accept_logical_delivery(address, sent_by, flow_ethos::Delivery::Queue, message, None)
            .expect("predecessor admission commits")
        else {
            panic!("predecessor admission is accepted")
        };
        value
    };
    let uncertain_order = admit(
        &recipient,
        SentBy::Sender(sender.clone()),
        Message::Soft(Request::Order("singleton answered order".into())),
    );
    store
        .transition_logical_delivery(
            &uncertain_order.logical_delivery_id,
            DeliveryState::Uncertain,
        )
        .expect("uncertain Order persists");
    let historical_result = admit(
        &sender,
        SentBy::Sender(recipient.clone()),
        Message::Soft(Request::Result(flow_ethos::ResultPayload::Answered(
            flow_ethos::ResultPayload_Answered_Data {
                answerable: flow_ethos::Answerable::Order("singleton answered order".into()),
                string: "historical answer".into(),
            },
        ))),
    );
    store
        .transition_logical_delivery(
            &historical_result.logical_delivery_id,
            DeliveryState::Delivered(NativeReceipt {
                harness_kind: flow_ethos::HarnessKind::Codex,
                native_session_id: "historical-answer-session".into(),
                native_prompt_id: "historical-answer-turn".into(),
            }),
        )
        .expect("historical Result delivered");
    store
        .withdraw_queued(&sender, &historical_result.logical_delivery_id)
        .expect("historical Result queue reads")
        .expect("historical Result leaves its queue");
    let open_order = admit(
        &recipient,
        SentBy::Sender(sender.clone()),
        Message::Soft(Request::Order("open order".into())),
    );
    let delivered_order = admit(
        &recipient,
        SentBy::Sender(sender.clone()),
        Message::Soft(Request::Order("delivered order".into())),
    );
    store
        .transition_logical_delivery(
            &delivered_order.logical_delivery_id,
            DeliveryState::Delivered(NativeReceipt {
                harness_kind: flow_ethos::HarnessKind::Codex,
                native_session_id: "delivered-order-session".into(),
                native_prompt_id: "delivered-order-turn".into(),
            }),
        )
        .expect("delivered Order persists");
    store
        .withdraw_queued(&recipient, &delivered_order.logical_delivery_id)
        .expect("delivered Order queue reads")
        .expect("delivered Order leaves its queue");
    let open_task = admit(
        &recipient,
        SentBy::Sender(sender.clone()),
        Message::Soft(Request::Task(flow_ethos::AnswerableTask {
            minutes: 5,
            string: "open task".into(),
        })),
    );
    let open_reply_input = admit(
        &sender,
        SentBy::Sender(recipient.clone()),
        Message::Soft(Request::Question("open simplified input".into())),
    );
    let completed_reply_input = admit(
        &sender,
        SentBy::Sender(recipient.clone()),
        Message::Soft(Request::Question("completed simplified input".into())),
    );
    store
        .bind_simplified_reply(SimplifiedReplyBinding {
            flow_id: "open-simplified".into(),
            input_delivery_id: open_reply_input.logical_delivery_id.clone(),
            source: sender.clone(),
            recipient: recipient.clone(),
            completed: false,
        })
        .expect("open reply binding persists");
    store
        .bind_simplified_reply(SimplifiedReplyBinding {
            flow_id: "completed-simplified".into(),
            input_delivery_id: completed_reply_input.logical_delivery_id.clone(),
            source: sender.clone(),
            recipient: recipient.clone(),
            completed: true,
        })
        .expect("completed reply binding persists");
    store
        .replace_heartbeat_standings(vec![StoredHeartbeatStanding {
            address: recipient.clone(),
            baseline_awake: 1,
            baseline_working: None,
            baseline_working_complete: true,
            baseline_sent: 2,
            baseline_queue: 3,
            latest_awake: 4,
            latest_working: Some(5),
            latest_working_complete: true,
            latest_sent: 6,
            latest_queue: 7,
            outstanding: None,
        }])
        .expect("heartbeat standing persists");
    let signal_flow::PrivateSendResponse::SendAccepted(heartbeat) = store
        .accept_logical_delivery_for(
            &recipient,
            SentBy::Flow,
            flow_ethos::Delivery::Wake,
            Message::Soft(Request::Notice("heartbeat predecessor".into())),
            None,
            DeliveryPurpose::Heartbeat,
        )
        .expect("heartbeat admission commits")
    else {
        panic!("heartbeat admission accepted")
    };
    drop(store);

    std::fs::write(
        output.join("fixture.ids"),
        format!(
            "sender=Mind:migrationSender:Sol\nrecipient=Field:migrationRecipient:Sol\nuncertain_order={}\nhistorical_result={}\nopen_order={}\ndelivered_order={}\nopen_task={}\nopen_reply={}\ncompleted_reply={}\nheartbeat={}\n",
            uncertain_order.logical_delivery_id,
            historical_result.logical_delivery_id,
            open_order.logical_delivery_id,
            delivered_order.logical_delivery_id,
            open_task.logical_delivery_id,
            open_reply_input.logical_delivery_id,
            completed_reply_input.logical_delivery_id,
            heartbeat.logical_delivery_id,
        ),
    )
    .expect("fixture identity manifest writes");
}
