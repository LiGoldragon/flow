//! Observe.Metaflows: the subscriber receives the metaflows as Metaflows
//! lists them on open, then each metaflow that changes, as it now stands,
//! and the Address of each no longer listed, each once.

use super::NexusFixture;
use crate::Dispatches;
use crate::observe_metaflows::ObservesMetaflows;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use flow_ethos::State;
use signal_flow::{Address, Aspect, Metaflow, Model, ObserveSelection, Query, Response};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

/// An Address no metaflow declaration names: listed while Awake, history
/// once asleep.
fn undeclared(topic: &str) -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic(topic.into()),
        model: Model::Haiku,
    }
}

fn awake(address: &Address, flow_id: &str) -> Metaflow {
    Metaflow {
        address: address.clone(),
        state: State::Awake(flow_ethos::FlowId(flow_id.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    }
}

/// The observer's peer going: dropped, it ends the observer, so a failed
/// assertion ends the test rather than leaving the observer waiting.
struct ObserverGoes<'a> {
    disconnected: &'a AtomicBool,
    fixture: &'a NexusFixture,
}

impl Drop for ObserverGoes<'_> {
    fn drop(&mut self) {
        self.disconnected.store(true, Ordering::Release);
        crate::store::AnnouncesLaunchChanges::announce(&self.fixture.nexus.store.metaflow_changes);
    }
}

/// The subscriber opens on the list, then gets a metaflow that appears as
/// it stands, and the Address of one that goes asleep undeclared; a commit
/// that leaves the list as it was sends nothing.
#[test]
fn a_metaflow_subscriber_receives_the_list_on_open_then_each_change_once() {
    let fixture = NexusFixture::new();
    let first = undeclared("observedFirst");
    let second = undeclared("observedSecond");
    fixture.nexus.store.seed_metaflow(awake(&first, "c1a0de"));
    let Response::Listed(listed) = fixture.nexus.dispatch(Query::Metaflows) else {
        panic!("Metaflows lists the metaflows");
    };
    assert!(listed.contains(&awake(&first, "c1a0de")), "{listed:?}");
    let disconnected = AtomicBool::new(false);
    let (frames, received) = mpsc::channel();
    std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            fixture
                .nexus
                .observe_metaflows(&disconnected, &mut |frame| {
                    frames
                        .send(frame.clone())
                        .map_err(|error| error.to_string())
                })
        });
        let goes = ObserverGoes {
            disconnected: &disconnected,
            fixture: &fixture,
        };
        // A frame that never comes fails the test at this bound rather than
        // hanging it; every frame here is sent as its commit is announced.
        let next = || {
            received
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("the observer sends")
        };

        assert_eq!(next(), Response::MetaflowsObserved(listed.clone()));

        // A sleep refused (another flow) commits nothing; the seed after it
        // is the next frame.
        assert!(
            !fixture
                .nexus
                .store
                .sleep_metaflow(&first, "0ther0")
                .unwrap()
        );
        fixture.nexus.store.seed_metaflow(awake(&second, "5ec0d1"));
        assert_eq!(next(), Response::MetaflowChanged(awake(&second, "5ec0d1")));

        assert!(
            fixture
                .nexus
                .store
                .sleep_metaflow(&first, "c1a0de")
                .unwrap()
        );
        assert_eq!(next(), Response::MetaflowGone(first.clone()));

        drop(goes);
        assert!(observer.join().expect("the observer ends").is_err());
        assert_eq!(
            received.try_iter().collect::<Vec<_>>(),
            vec![],
            "nothing more was sent"
        );
    });
    // A direct Observe answers the opening frame: the list as it now is.
    let Response::Listed(now) = fixture.nexus.dispatch(Query::Metaflows) else {
        panic!("Metaflows lists the metaflows");
    };
    assert!(!now.iter().any(|metaflow| metaflow.address == first));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Observe(ObserveSelection::Metaflows)),
        Response::MetaflowsObserved(now)
    );
}
