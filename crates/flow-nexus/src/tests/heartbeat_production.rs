use super::NexusFixture;
use crate::heartbeat::{DeliveryPurpose, RunsHeartbeat};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::{StoredHeartbeatStanding, TracksHeartbeat};
use signal_flow::{Address, Aspect, DeliveryState, Layer, Message, Metaflow, Request, SentBy};

trait HeartbeatFixture {
    fn recipient(topic: &str) -> Address;
    fn seed(&self, address: &Address, state: flow_ethos::State);
    fn ordinary(&self, address: &Address, body: &str) -> String;
    fn standing(&self, address: &Address) -> StoredHeartbeatStanding;
}

impl HeartbeatFixture for NexusFixture {
    fn recipient(topic: &str) -> Address {
        Address {
            aspect: Aspect::Field,
            topic: topic.into(),
            layer: Layer::Secondary,
        }
    }

    fn seed(&self, address: &Address, state: flow_ethos::State) {
        self.nexus.store.seed_metaflow(Metaflow {
            address: address.clone(),
            state,
            past: Vec::new(),
            queue: Vec::new(),
        });
    }

    fn ordinary(&self, address: &Address, body: &str) -> String {
        let signal_flow::PrivateSendResponse::SendAccepted(accepted) = self
            .nexus
            .store
            .accept_logical_delivery(
                address,
                SentBy::Flow,
                flow_ethos::Delivery::Wake,
                Message::Soft(Request::Notice(body.into())),
                None,
            )
            .expect("ordinary delivery is admitted")
        else {
            panic!("ordinary delivery accepted")
        };
        accepted.logical_delivery_id
    }

    fn standing(&self, address: &Address) -> StoredHeartbeatStanding {
        self.nexus
            .store
            .heartbeat_standings()
            .unwrap()
            .into_iter()
            .find(|row| row.address == *address)
            .expect("recipient standing")
    }
}

#[test]
fn durable_worker_path_is_quiet_coalesces_and_excludes_its_own_traffic() {
    let fixture = NexusFixture::new();
    let awake = NexusFixture::recipient("heartbeatAwake");
    let sleeping = NexusFixture::recipient("heartbeatSleeping");
    let unknown = NexusFixture::recipient("heartbeatUnknown");
    fixture.seed(&awake, flow_ethos::State::Awake("awake-flow".into()));
    fixture.seed(&sleeping, flow_ethos::State::Asleep);

    let evidence = fixture
        .nexus
        .heartbeat_evidence(&[awake.clone(), sleeping, unknown])
        .unwrap();
    assert_eq!(
        evidence
            .recipients
            .iter()
            .map(|(address, _)| address)
            .collect::<Vec<_>>(),
        vec![&awake],
        "only a running recipient has authoritative own evidence"
    );

    fixture.nexus.sync_heartbeat(std::slice::from_ref(&awake));
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&awake)
            .unwrap()
            .unwrap()
            .queue
            .is_empty()
    );
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    assert!(fixture.nexus.store.delivery_purposes().unwrap().is_empty());

    fixture.ordinary(&awake, "ordinary one");
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    let purposes = fixture.nexus.store.delivery_purposes().unwrap();
    assert_eq!(
        purposes
            .iter()
            .filter(|row| row.purpose == DeliveryPurpose::Ordinary)
            .count(),
        1
    );
    assert_eq!(
        purposes
            .iter()
            .filter(|row| row.purpose == DeliveryPurpose::Heartbeat)
            .count(),
        1
    );
    let first_heartbeat = fixture.standing(&awake).outstanding.unwrap();
    let after_generated = fixture
        .nexus
        .heartbeat_evidence(std::slice::from_ref(&awake))
        .unwrap();
    assert_eq!(after_generated.recipients[0].1.ordinary_messages_sent, 1);
    assert_eq!(after_generated.recipients[0].1.ordinary_queue_length, 1);

    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    assert_eq!(fixture.nexus.store.delivery_purposes().unwrap().len(), 2);
    fixture.ordinary(&awake, "ordinary two");
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    let coalesced = fixture.standing(&awake);
    assert_eq!(
        coalesced.outstanding.as_deref(),
        Some(first_heartbeat.as_str())
    );
    assert_eq!(coalesced.latest_sent, 2);
    assert_eq!(fixture.nexus.store.delivery_purposes().unwrap().len(), 3);

    fixture
        .nexus
        .store
        .transition_logical_delivery(
            &first_heartbeat,
            DeliveryState::Failed(signal_flow::Refusal::Direct("terminal fixture".into())),
        )
        .unwrap();
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    let followup = fixture.standing(&awake).outstanding.unwrap();
    assert_ne!(followup, first_heartbeat);
    assert_eq!(fixture.nexus.store.delivery_purposes().unwrap().len(), 4);

    fixture
        .nexus
        .store
        .transition_logical_delivery(
            &followup,
            DeliveryState::Failed(signal_flow::Refusal::Direct("terminal fixture".into())),
        )
        .unwrap();
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    assert_eq!(fixture.nexus.store.delivery_purposes().unwrap().len(), 4);

    let mut unknown_receipt = fixture.standing(&awake);
    unknown_receipt.outstanding = Some("unknown-heartbeat".into());
    fixture
        .nexus
        .store
        .replace_heartbeat_standings(vec![unknown_receipt])
        .unwrap();
    fixture.ordinary(&awake, "ordinary three");
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    let held = fixture.standing(&awake);
    assert_eq!(held.outstanding.as_deref(), Some("unknown-heartbeat"));
    assert_eq!(held.latest_sent, 3);
    assert_eq!(fixture.nexus.store.delivery_purposes().unwrap().len(), 5);
}

#[test]
fn refused_heartbeat_admission_retains_its_durable_baseline() {
    let fixture = NexusFixture::new();
    let absent = NexusFixture::recipient("heartbeatAbsent");
    let standing = StoredHeartbeatStanding {
        address: absent.clone(),
        baseline_awake: 1,
        baseline_working: None,
        baseline_working_complete: false,
        baseline_sent: 2,
        baseline_queue: 3,
        latest_awake: 4,
        latest_working: None,
        latest_working_complete: false,
        latest_sent: 5,
        latest_queue: 6,
        outstanding: None,
    };
    fixture
        .nexus
        .store
        .replace_heartbeat_standings(vec![standing.clone()])
        .unwrap();
    assert!(matches!(
        fixture.nexus.store.accept_logical_delivery_for(
            &absent,
            SentBy::Flow,
            flow_ethos::Delivery::Wake,
            Message::Soft(Request::Notice("heartbeat".into())),
            None,
            DeliveryPurpose::Heartbeat,
        ),
        Ok(signal_flow::PrivateSendResponse::Refused(_))
    ));
    assert_eq!(fixture.standing(&absent), standing);
}

#[test]
fn configuration_churn_preserves_an_unknown_outstanding_delivery() {
    let fixture = NexusFixture::new();
    let awake = NexusFixture::recipient("heartbeatReselected");
    fixture.seed(&awake, flow_ethos::State::Awake("awake-flow".into()));
    fixture.nexus.sync_heartbeat(std::slice::from_ref(&awake));
    let mut standing = fixture.standing(&awake);
    standing.outstanding = Some("unknown-heartbeat".into());
    fixture
        .nexus
        .store
        .replace_heartbeat_standings(vec![standing.clone()])
        .unwrap();

    fixture.nexus.sync_heartbeat(&[]);
    assert_eq!(fixture.standing(&awake), standing);
    fixture.nexus.sync_heartbeat(std::slice::from_ref(&awake));
    assert_eq!(fixture.standing(&awake), standing);
    fixture.nexus.heartbeat_tick(std::slice::from_ref(&awake));
    assert_eq!(fixture.standing(&awake), standing);
    assert!(fixture.nexus.store.delivery_purposes().unwrap().is_empty());
}
