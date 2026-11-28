//! The Harness Nexuses' witness of each flow against a fake subscription
//! socket: the daemon configuration beside the place file; a Subscribed then
//! each Changed held as the flow's witness; a dropped connection dialed
//! again, its fresh Subscribed replacing what the socket had told; and a
//! disagreement with Flow's own state kept once its grace has passed, none
//! when the two agree. Nothing here rests, drains, wakes or closes a flow.

use super::waking::{CLAUDE_SEAT, claude_agent, claude_seat};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::StopsNexus;
use crate::generated::memory::{SeatActivity, SeatStanding};
use crate::generated::startup::{
    DaemonConfiguration, DaemonConfigurationRefusal, DisagreementCapacity, GraceSeconds,
    HarnessSubscriptionPath,
};
use crate::harness_subscription::{
    HarnessObservation, JudgesHarnessAgreement, JudgesHarnessWitnesses, KeepsHarnessWitnesses,
    LocatesDaemonConfiguration, ObservesHarness, SubscribesToHarnesses,
};
use crate::store::AnnouncesLaunchChanges;
use crate::store::current_seat::RecordsCurrentSeats;
use meta_signal_flow::{HarnessDisagreement, HarnessSocket, HarnessTokens, HarnessWitness, Seen};
use signal_flow::FlowId;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::{Duration, Instant};
use translator::place_file::{ConfigurationHome, LocatesConfigurationHome};
use translator::placement::PlaceName;

const SOCKET: &str = "/run/user/1001/nexus/main/claude-harness-subscription.sock";

fn configuration(sockets: &[&str], grace: i64, capacity: i64) -> DaemonConfiguration {
    DaemonConfiguration {
        harness_subscription_path_vector: sockets
            .iter()
            .map(|socket| HarnessSubscriptionPath((*socket).into()))
            .collect(),
        grace_seconds: GraceSeconds(grace),
        disagreement_capacity: DisagreementCapacity(capacity),
    }
}

/// A Claude seat Flow holds, resting, with the daemon configuration given.
fn seat_witnessed_by(sockets: &[&str], grace: i64, capacity: i64) -> NexusFixture {
    let mut fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    fixture.nexus.harness_witnesses.configuration = Some(configuration(sockets, grace, capacity));
    fixture
}

fn seat() -> FlowId {
    FlowId(CLAUDE_SEAT.into())
}

fn socket() -> HarnessSocket {
    HarnessSocket(SOCKET.into())
}

fn changed(event: harness::Event, state: harness::State) -> HarnessObservation {
    HarnessObservation::Changed(signal_harness::Changed {
        flow_id: seat(),
        event,
        state,
    })
}

fn flowing(flow: harness::Flow) -> harness::State {
    harness::State::Flowing(flow)
}

fn observed(flow_id: &str, state: harness::State, tokens: i64) -> harness::Observed {
    harness::Observed {
        flow_id: FlowId(flow_id.into()),
        state,
        tokens: harness::Tokens(tokens),
        background_work_vector: vec![],
    }
}

fn set_seat(fixture: &NexusFixture, activity: SeatActivity) {
    fixture
        .nexus
        .store
        .record_observation(CLAUDE_SEAT, Some(SeatStanding::Alive(activity)))
        .unwrap();
}

/// No file is no configuration; a file names its sockets, grace and
/// capacity; a relative socket, a socket named twice, a negative grace or
/// capacity and a file that holds no configuration are refused Invalid,
/// and one that cannot be read Unreadable, each naming the file.
#[test]
fn the_daemon_configuration_lies_beside_the_place_file_and_is_absent_without_it() {
    let configuration_home = tempfile::tempdir().expect("configuration home");
    let home = ConfigurationHome::under(Some(configuration_home.path().as_os_str()), None)
        .expect("configuration home");
    let main = PlaceName("main".into());
    assert_eq!(main.daemon_configuration_under(&home), Ok(None));
    let file = configuration_home
        .path()
        .join("nexus/main/flow-nexus-daemon.datom");
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(
        &file,
        "{ [ «/run/user/1001/nexus/main/claude-harness-subscription.sock» «/run/user/1001/nexus/main/codex-harness-subscription.sock» ] 30 64 }\n",
    )
    .unwrap();
    assert_eq!(
        main.daemon_configuration_under(&home),
        Ok(Some(configuration(
            &[
                "/run/user/1001/nexus/main/claude-harness-subscription.sock",
                "/run/user/1001/nexus/main/codex-harness-subscription.sock",
            ],
            30,
            64
        )))
    );
    std::fs::write(&file, "{ [] 0 0 }").unwrap();
    assert_eq!(
        main.daemon_configuration_under(&home),
        Ok(Some(configuration(&[], 0, 0)))
    );
    for refused in [
        "{ [ «claude-harness-subscription.sock» ] 30 64 }",
        "{ [ «/run/a.sock» «/run/a.sock» ] 30 64 }",
        "{ [ «/run/a.sock» ] -1 64 }",
        "{ [ «/run/a.sock» ] 30 -1 }",
        "{ [ «/run/a.sock» ] 30 }",
        "{ main }",
    ] {
        std::fs::write(&file, refused).unwrap();
        match main.daemon_configuration_under(&home) {
            Err(DaemonConfigurationRefusal::Invalid(fault)) => {
                assert_eq!(fault.path.0, file.to_string_lossy(), "{refused}")
            }
            other => panic!("{refused}: {other:?}"),
        }
    }
    std::fs::remove_file(&file).unwrap();
    std::fs::create_dir(&file).unwrap();
    match main.daemon_configuration_under(&home) {
        Err(DaemonConfigurationRefusal::Unreadable(fault)) => {
            assert_eq!(fault.path.0, file.to_string_lossy())
        }
        other => panic!("a directory is no readable file: {other:?}"),
    }
}

/// With no configuration the part spawns nothing and nothing is told.
#[test]
fn with_no_configuration_nothing_is_subscribed_to() {
    let fixture = NexusFixture::new();
    assert!(fixture.nexus.harness_witnesses.configuration.is_none());
    std::thread::scope(|scope| fixture.nexus.witness_harnesses(scope));
    assert_eq!(
        fixture.nexus.harness_observed(),
        Ok(meta_signal_flow::HarnessObserved_Data {
            harness_witness_vector: vec![],
            harness_disagreement_vector: vec![],
        })
    );
}

/// The agreement of each harness state with each of Flow's states.
#[test]
fn harness_states_agree_with_flow_states_as_the_design_rules() {
    use meta_signal_flow::State::{Acting, Asleep, Crystallizing, Resting};
    let table = [
        (flowing(harness::Flow::Working), [true, false, true, false]),
        (flowing(harness::Flow::Waiting), [true, false, true, false]),
        (flowing(harness::Flow::Blocked), [true, false, true, false]),
        (flowing(harness::Flow::Rested), [false, true, true, false]),
        (harness::State::Ended, [false, false, false, true]),
    ];
    for (state, agreements) in table {
        for (flow_state, agrees) in [Acting, Resting, Crystallizing, Asleep]
            .into_iter()
            .zip(agreements)
        {
            assert_eq!(
                state.agrees_with(&flow_state),
                Some(agrees),
                "{state:?} against {flow_state:?}"
            );
        }
    }
    for flow_state in [Acting, Resting, Crystallizing, Asleep] {
        assert_eq!(harness::State::Dry.agrees_with(&flow_state), None);
    }
}

/// The fake Harness Nexus's frames: a length, then the archive.
trait FakeHarnessFrames {
    fn read_input(&mut self) -> signal_harness::Input;
    fn write_output(&mut self, output: &signal_harness::Output);
}

impl FakeHarnessFrames for UnixStream {
    fn read_input(&mut self) -> signal_harness::Input {
        let mut length = [0_u8; 4];
        self.read_exact(&mut length).expect("a frame's length");
        let mut bytes = vec![0_u8; u32::from_le_bytes(length) as usize];
        self.read_exact(&mut bytes).expect("a frame");
        rkyv::from_bytes::<signal_harness::Input, rkyv::rancor::Error>(&bytes).expect("an Input")
    }

    fn write_output(&mut self, output: &signal_harness::Output) {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(output).expect("archived");
        self.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
        self.write_all(&bytes).unwrap();
        self.flush().unwrap();
    }
}

/// Waits, on the witnesses' change announcements, until `holds`.
trait AwaitsWitnesses {
    fn witnessed_until(&self, holds: &mut dyn FnMut(&NexusFixture) -> bool) -> bool;
}

impl AwaitsWitnesses for NexusFixture {
    fn witnessed_until(&self, holds: &mut dyn FnMut(&NexusFixture) -> bool) -> bool {
        self.nexus
            .harness_witnesses
            .changes
            .until_holds(Instant::now() + Duration::from_secs(20), &mut || {
                holds(self)
            })
    }
}

/// Meta `ObserveHarness` asked through the meta dispatch, its answer as
/// the meta client prints it.
trait AsksObserveHarness {
    fn observe_harness_as_printed(&self) -> String;
}

impl AsksObserveHarness for NexusFixture {
    fn observe_harness_as_printed(&self) -> String {
        use crate::Dispatches;
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};
        self.nexus
            .dispatch_meta(meta_signal_flow::Input::ObserveHarness)
            .datomize(vec![])
            .protosize()
            .compact()
    }
}

fn witness_of(fixture: &NexusFixture, flow_id: &str) -> Option<harness::State> {
    fixture
        .nexus
        .harness_witnesses
        .witnesses()
        .into_iter()
        .find(|(_, told, _)| told.0 == flow_id)
        .map(|(_, _, witnessed)| witnessed.state)
}

/// The part, started before its socket exists, subscribes once the socket
/// is placed. Its Subscribed gives the seat's witness and one of a flow
/// Flow does not hold, which ObserveHarness does not list; each Changed
/// moves the seat's witness, a ContextSize its tokens. A flow told Ended
/// stays. The connection dropped, the part dials again at once, and the
/// fresh Subscribed replaces every witness the socket had told.
#[test]
fn a_subscription_s_witnesses_follow_its_changes_and_a_redial_replaces_them() {
    let directory = tempfile::tempdir().expect("a socket directory");
    let path = directory.path().join("harness/subscription.sock");
    let socket_name = path.to_string_lossy().into_owned();
    let fixture = seat_witnessed_by(&[&socket_name], 3_600, 8);
    let socket = HarnessSocket(socket_name.clone());
    std::thread::scope(|scope| {
        fixture.nexus.witness_harnesses(scope);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&path).expect("the fake subscription socket");
        let (mut first, _) = listener.accept().expect("the part subscribes");
        assert_eq!(first.read_input(), signal_harness::Input::Subscribe);
        first.write_output(&signal_harness::Output::Subscribed(vec![
            observed(CLAUDE_SEAT, flowing(harness::Flow::Working), 40_000),
            observed("ffffff", harness::State::Dry, 0),
        ]));
        assert!(fixture.witnessed_until(&mut |fixture| witness_of(fixture, "ffffff").is_some()));
        assert_eq!(
            fixture
                .nexus
                .harness_observed()
                .unwrap()
                .harness_witness_vector,
            vec![HarnessWitness {
                flow_id: seat(),
                harness_socket: socket.clone(),
                harness_event: harness::Event::InFlight(vec![]),
                harness_state: flowing(harness::Flow::Working),
                harness_tokens: HarnessTokens(40_000),
                state_option: Some(meta_signal_flow::State::Resting),
            }],
            "only the flow Flow holds is listed, with Flow's own state"
        );
        first.write_output(&signal_harness::Output::Changed(signal_harness::Changed {
            flow_id: seat(),
            event: harness::Event::ContextSize(harness::Tokens(41_234)),
            state: flowing(harness::Flow::Working),
        }));
        first.write_output(&signal_harness::Output::Changed(signal_harness::Changed {
            flow_id: seat(),
            event: harness::Event::TurnEnded,
            state: flowing(harness::Flow::Rested),
        }));
        assert!(fixture.witnessed_until(&mut |fixture| {
            witness_of(fixture, CLAUDE_SEAT) == Some(flowing(harness::Flow::Rested))
        }));
        assert_eq!(
            fixture
                .nexus
                .harness_observed()
                .unwrap()
                .harness_witness_vector,
            vec![HarnessWitness {
                flow_id: seat(),
                harness_socket: socket.clone(),
                harness_event: harness::Event::TurnEnded,
                harness_state: flowing(harness::Flow::Rested),
                harness_tokens: HarnessTokens(41_234),
                state_option: Some(meta_signal_flow::State::Resting),
            }]
        );
        assert_eq!(
            fixture.observe_harness_as_printed(),
            format!(
                "HarnessObserved.{{ [ {{ {CLAUDE_SEAT} {socket_name} TurnEnded Flowing.Rested 41234 Some.Resting }} ] [] }}"
            ),
            "ObserveHarness as the meta client prints its answer"
        );
        first.write_output(&signal_harness::Output::Changed(signal_harness::Changed {
            flow_id: seat(),
            event: harness::Event::Ended(harness::EndReason::Exited),
            state: harness::State::Ended,
        }));
        assert!(fixture.witnessed_until(&mut |fixture| {
            witness_of(fixture, CLAUDE_SEAT) == Some(harness::State::Ended)
        }));
        drop(first);
        // The part dials again at once; until the fresh Subscribed the
        // socket's witnesses stand as it last told them.
        let (mut second, _) = listener.accept().expect("the part dials again");
        assert_eq!(second.read_input(), signal_harness::Input::Subscribe);
        assert_eq!(
            witness_of(&fixture, CLAUDE_SEAT),
            Some(harness::State::Ended),
            "a flow told Ended stays until the next Subscribed"
        );
        second.write_output(&signal_harness::Output::Subscribed(vec![observed(
            "abcdef",
            flowing(harness::Flow::Rested),
            12,
        )]));
        assert!(fixture.witnessed_until(&mut |fixture| witness_of(fixture, "abcdef").is_some()));
        assert_eq!(
            fixture
                .nexus
                .harness_witnesses
                .witnesses()
                .into_iter()
                .map(|(told_by, flow_id, _)| (told_by, flow_id.0))
                .collect::<Vec<_>>(),
            vec![(socket.clone(), "abcdef".to_owned())],
            "the fresh Subscribed replaces the socket's witnesses whole"
        );
        // The scope joins every thread the part spawned: each ends when
        // the Nexus stops.
        fixture.nexus.stop(&[]);
    });
    assert!(
        fixture.nexus.harness_witnesses.disagreements().is_empty(),
        "no grace passed"
    );
}

/// A Changed moving the seat to Working while Flow holds it Resting is a
/// disagreement once the grace has passed, not before; a Changed moving it
/// to Rested agrees, and keeps none; a comparison whose state was moved on
/// before it fell due is not judged; the ring keeps the newest entries.
#[test]
fn a_disagreement_is_kept_after_its_grace_and_none_when_the_states_agree() {
    let fixture = seat_witnessed_by(&[SOCKET], 30, 2);
    let witnesses = &fixture.nexus.harness_witnesses;
    let start = Instant::now();
    let at = |seconds: u64| start + Duration::from_secs(seconds);
    witnesses.take(
        &socket(),
        HarnessObservation::Snapshot(vec![observed(
            CLAUDE_SEAT,
            flowing(harness::Flow::Rested),
            40_000,
        )]),
        at(0),
    );
    assert_eq!(
        witnesses.next_due(),
        None,
        "a Subscribed sets no comparison"
    );
    witnesses.take(
        &socket(),
        changed(
            harness::Event::PromptSubmitted,
            flowing(harness::Flow::Working),
        ),
        at(0),
    );
    assert_eq!(witnesses.next_due(), Some(at(30)));
    fixture.nexus.judge_due_comparisons(at(29), 1_000);
    assert!(
        witnesses.disagreements().is_empty(),
        "the grace has not passed"
    );
    fixture.nexus.judge_due_comparisons(at(30), 1_030);
    assert_eq!(
        witnesses.disagreements(),
        vec![HarnessDisagreement {
            flow_id: seat(),
            harness_socket: socket(),
            seen: Seen(1_030),
            harness_state: flowing(harness::Flow::Working),
            state_option: Some(meta_signal_flow::State::Resting),
        }]
    );
    assert_eq!(witnesses.next_due(), None, "a comparison is judged once");

    // Rested agrees with Resting: nothing more is kept.
    witnesses.take(
        &socket(),
        changed(harness::Event::TurnEnded, flowing(harness::Flow::Rested)),
        at(40),
    );
    fixture.nexus.judge_due_comparisons(at(70), 1_070);
    assert_eq!(witnesses.disagreements().len(), 1);

    // A Changed that leaves the state as it was sets no comparison.
    witnesses.take(
        &socket(),
        changed(
            harness::Event::ContextSize(harness::Tokens(50_000)),
            flowing(harness::Flow::Rested),
        ),
        at(80),
    );
    assert_eq!(witnesses.next_due(), None);

    // Working, then Rested before its grace: the Working comparison is not
    // judged, and Rested agrees.
    witnesses.take(
        &socket(),
        changed(
            harness::Event::PromptSubmitted,
            flowing(harness::Flow::Working),
        ),
        at(100),
    );
    witnesses.take(
        &socket(),
        changed(harness::Event::TurnEnded, flowing(harness::Flow::Rested)),
        at(110),
    );
    fixture.nexus.judge_due_comparisons(at(140), 1_140);
    fixture.nexus.judge_due_comparisons(at(150), 1_150);
    assert_eq!(witnesses.disagreements().len(), 1);

    // Working, Rested, then Working again: only the last move's comparison
    // stands, due its own grace later; the first is not judged at its time.
    for (state, moment) in [
        (flowing(harness::Flow::Working), 160),
        (flowing(harness::Flow::Rested), 165),
        (flowing(harness::Flow::Working), 170),
    ] {
        witnesses.take(
            &socket(),
            changed(harness::Event::PromptSubmitted, state),
            at(moment),
        );
    }
    assert_eq!(witnesses.next_due(), Some(at(200)));
    fixture.nexus.judge_due_comparisons(at(190), 1_190);
    assert_eq!(
        witnesses.disagreements().len(),
        1,
        "the first move stood no grace"
    );
    witnesses.take(
        &socket(),
        changed(harness::Event::TurnEnded, flowing(harness::Flow::Rested)),
        at(195),
    );
    fixture.nexus.judge_due_comparisons(at(230), 1_230);
    assert_eq!(witnesses.disagreements().len(), 1);

    // Flow now holds the seat Acting: Ended, then Rested, disagree; the
    // ring of two keeps the newest two, oldest first.
    set_seat(&fixture, SeatActivity::Working);
    witnesses.take(
        &socket(),
        changed(
            harness::Event::Ended(harness::EndReason::Exited),
            harness::State::Ended,
        ),
        at(200),
    );
    fixture.nexus.judge_due_comparisons(at(230), 1_230);
    witnesses.take(
        &socket(),
        changed(harness::Event::Resumed, flowing(harness::Flow::Rested)),
        at(240),
    );
    fixture.nexus.judge_due_comparisons(at(270), 1_270);
    assert_eq!(
        witnesses
            .disagreements()
            .into_iter()
            .map(|disagreement| (disagreement.seen, disagreement.harness_state))
            .collect::<Vec<_>>(),
        vec![
            (Seen(1_230), harness::State::Ended),
            (Seen(1_270), flowing(harness::Flow::Rested)),
        ]
    );
    // Dry is not judged, and a flow Flow records no state for is not
    // compared.
    witnesses.take(
        &socket(),
        changed(harness::Event::Started, harness::State::Dry),
        at(300),
    );
    witnesses.take(
        &socket(),
        HarnessObservation::Changed(signal_harness::Changed {
            flow_id: FlowId("ffffff".into()),
            event: harness::Event::PromptSubmitted,
            state: flowing(harness::Flow::Working),
        }),
        at(300),
    );
    fixture.nexus.judge_due_comparisons(at(330), 1_330);
    assert_eq!(witnesses.disagreements().len(), 2);
    assert_eq!(
        fixture
            .nexus
            .harness_observed()
            .unwrap()
            .harness_disagreement_vector
            .len(),
        2,
        "ObserveHarness reads the ring"
    );
}

/// The judge waits on the deadline as an event: a disagreement told over
/// the socket is kept only after its grace of one second has passed.
#[test]
fn the_judge_keeps_a_disagreement_once_its_grace_has_passed() {
    let directory = tempfile::tempdir().expect("a socket directory");
    let path = directory.path().join("subscription.sock");
    let socket_name = path.to_string_lossy().into_owned();
    let fixture = seat_witnessed_by(&[&socket_name], 1, 4);
    let listener = UnixListener::bind(&path).expect("the fake subscription socket");
    std::thread::scope(|scope| {
        fixture.nexus.witness_harnesses(scope);
        let (mut stream, _) = listener.accept().expect("the part subscribes");
        assert_eq!(stream.read_input(), signal_harness::Input::Subscribe);
        stream.write_output(&signal_harness::Output::Subscribed(vec![]));
        let told = Instant::now();
        stream.write_output(&signal_harness::Output::Changed(signal_harness::Changed {
            flow_id: seat(),
            event: harness::Event::Ended(harness::EndReason::Died),
            state: harness::State::Ended,
        }));
        assert!(fixture.witnessed_until(&mut |fixture| {
            witness_of(fixture, CLAUDE_SEAT) == Some(harness::State::Ended)
        }));
        assert!(fixture.witnessed_until(&mut |fixture| {
            !fixture.nexus.harness_witnesses.disagreements().is_empty()
        }));
        assert!(
            told.elapsed() >= Duration::from_secs(1),
            "kept only after the grace"
        );
        let observed = fixture.nexus.harness_observed().unwrap();
        assert_eq!(
            observed
                .harness_disagreement_vector
                .iter()
                .map(|disagreement| (
                    disagreement.flow_id.clone(),
                    disagreement.harness_state.clone(),
                    disagreement.state_option.clone()
                ))
                .collect::<Vec<_>>(),
            vec![(
                seat(),
                harness::State::Ended,
                Some(meta_signal_flow::State::Resting)
            )]
        );
        // The scope joins every thread the part spawned: each ends when
        // the Nexus stops.
        fixture.nexus.stop(&[]);
    });
}
