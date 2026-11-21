//! The Claude Harness subscription against a fake subscription socket: its
//! configuration beside the place file; a rested turn end it tells resting
//! the flow with the context size the Harness Nexus told, recording no
//! harness event; and the one source of a Claude flow's rest, the
//! subscription when one is configured, the Stop hook when none is. The rest
//! is a session's State becoming Flowing.Rested, whatever Event brought it
//! there, and the subscription's State is the judgement of the flow's turn
//! that closing it at rest asks.

use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, address, append_main_rows, claude_agent, claude_prompt_taken,
    claude_seat, end_turn, maintain, metaflow, report_claude_stop, stored,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::HoldsStopRequest;
use crate::generated::memory::RecordedHarnessEvent;
use crate::generated::startup::{
    ClaudeHarnessSubscriptionPath, DaemonConfiguration, DaemonConfigurationRefusal,
};
use crate::generated::transitions::{AskedFlow, RecordsTransitions};
use crate::harness_subscription::{
    ClaudeHarnessSubscriber, ClaudeHarnessSubscription, HarnessObservation,
    HoldsHarnessSubscription, KeepsHarnessSessions, LocatesDaemonConfiguration,
    SubscribesToClaudeHarness,
};
use crate::store::context::RecordsContextSize;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SeedsMetaflow;
use crate::waking::JudgesClosingRest;
use flow_ethos::State;
use signal_flow::Model;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use translator::place_file::{ConfigurationHome, LocatesConfigurationHome};
use translator::placement::PlaceName;

/// The seat's own Stops, as Memory records them.
fn recorded_stops(fixture: &NexusFixture) -> Vec<crate::generated::memory::RecordedStop> {
    fixture
        .nexus
        .store
        .events(CLAUDE_SEAT)
        .unwrap()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|event| match event {
            RecordedHarnessEvent::Stopped(stop) => Some(stop),
            _ => None,
        })
        .collect()
}

/// An idle Maintained Claude seat, its transcript showing a context of
/// 151,000, its own rest recorded.
fn maintained_claude_seat() -> NexusFixture {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 151_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));
    fixture
}

fn the_seat_s_state(fixture: &NexusFixture) -> Option<meta_signal_flow::State> {
    fixture
        .nexus
        .store
        .state_of(AskedFlow(flow_ethos::FlowId(CLAUDE_SEAT.into())))
        .unwrap()
}

/// No file is every option unset; a file names the socket; a relative
/// socket or a file that holds no configuration is refused.
#[test]
fn the_daemon_configuration_lies_beside_the_place_file_and_is_unset_without_it() {
    let configuration = tempfile::tempdir().expect("configuration home");
    let home = ConfigurationHome::under(Some(configuration.path().as_os_str()), None)
        .expect("configuration home");
    let main = PlaceName("main".into());
    assert_eq!(
        main.daemon_configuration_under(&home),
        Ok(DaemonConfiguration {
            claude_harness_subscription_path_option: None,
        })
    );
    let file = configuration
        .path()
        .join("nexus/main/flow-nexus-daemon.datom");
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(
        &file,
        "{ Some.«/run/user/1001/nexus/main/claude-harness-subscription.sock» }\n",
    )
    .unwrap();
    assert_eq!(
        main.daemon_configuration_under(&home),
        Ok(DaemonConfiguration {
            claude_harness_subscription_path_option: Some(ClaudeHarnessSubscriptionPath(
                "/run/user/1001/nexus/main/claude-harness-subscription.sock".into()
            )),
        })
    );
    std::fs::write(&file, "{ None }").unwrap();
    assert_eq!(
        main.daemon_configuration_under(&home),
        Ok(DaemonConfiguration {
            claude_harness_subscription_path_option: None,
        })
    );
    for refused in ["{ Some.«claude-harness-subscription.sock» }", "{ main }"] {
        std::fs::write(&file, refused).unwrap();
        assert!(
            matches!(
                main.daemon_configuration_under(&home),
                Err(DaemonConfigurationRefusal::Invalid(_))
            ),
            "{refused}"
        );
    }
}

/// The fake Claude Harness Nexus's frames: a length, then the archive.
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

/// With no subscription configured the flow's own Stop hook is the source
/// of its rest: the Stop is recorded, and rests the flow, its context size
/// the one its transcript shows.
#[test]
fn with_no_subscription_the_stop_hook_rests_the_flow() {
    let fixture = maintained_claude_seat();
    assert!(fixture.nexus.claude_harness_subscription.is_none());
    report_claude_stop(&fixture, vec![]);
    assert_eq!(recorded_stops(&fixture).len(), 2);
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(151_000),
        "the hook's Stop wrote the transcript's size"
    );
    assert_eq!(
        the_seat_s_state(&fixture),
        Some(meta_signal_flow::State::Resting)
    );
}

/// With a subscription configured it is the one source of the rest: the
/// flow's own Stop is still recorded, but rests nothing, and the judgement
/// of its turn is the subscription's: while the subscription has told
/// nothing of the session its turn is not known to have ended, and once it
/// tells the session Rested nothing is in flight.
#[test]
fn with_a_subscription_the_stop_hook_is_recorded_and_rests_nothing() {
    let mut fixture = maintained_claude_seat();
    fixture.nexus.claude_harness_subscription = Some(ClaudeHarnessSubscription::from(
        ClaudeHarnessSubscriptionPath(
            "/run/user/1001/nexus/main/claude-harness-subscription.sock".into(),
        ),
    ));
    report_claude_stop(&fixture, vec![]);
    assert_eq!(recorded_stops(&fixture).len(), 2, "the Stop is recorded");
    assert!(
        !fixture.nexus.background_settled(CLAUDE_SEAT),
        "the recorded Stop is not the judgement of the turn"
    );
    fixture
        .nexus
        .claude_harness_subscription
        .as_ref()
        .unwrap()
        .keep(&HarnessObservation::Snapshot(vec![observed(
            harness::Flow::Rested,
            41_234,
            vec![],
        )]));
    assert!(
        fixture.nexus.background_settled(CLAUDE_SEAT),
        "the subscription's Rested is the judgement of the turn"
    );
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        None,
        "no context size is written"
    );
    assert_ne!(
        the_seat_s_state(&fixture),
        Some(meta_signal_flow::State::Resting),
        "no rest is recorded"
    );
}

/// The part, started before its socket exists, subscribes once the socket
/// is placed, and after a connection that ends unanswered, once it is placed
/// anew. Its Subscribed snapshot gives the seat's session a context of
/// 41,234 tokens, where the seat's transcript shows 40,000; a rested turn
/// end of that session then rests the OnDemand seat with nothing queued:
/// the harness's size is written, its rest recorded, and it sleeps. No
/// Stop is recorded by the part. When the connection ends the part
/// subscribes again.
#[test]
fn a_rested_turn_end_rests_the_flow_with_the_harness_s_context_size() {
    let mut fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    // Its successor composes, so it may close.
    super::refresh_and_compaction::declare_seat(
        &fixture,
        meta_signal_flow::Continuity::OnDemand,
        vec!["spirit:core:0".into(), "intent:flow:0".into()],
    );
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    assert_eq!(
        recorded_stops(&fixture).len(),
        1,
        "the seat's own rest is recorded first"
    );
    assert_eq!(fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(), None);

    let socket = fixture
        .directory
        .path()
        .join("claude-harness/claude-harness-subscription.sock");
    fixture.nexus.claude_harness_subscription = Some(ClaudeHarnessSubscription::from(
        ClaudeHarnessSubscriptionPath(socket.to_string_lossy().into_owned()),
    ));
    std::thread::scope(|scope| {
        let part = scope.spawn(|| fixture.nexus.keep_subscribing_to_claude_harness());
        std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&socket).expect("the fake subscription socket");
        // A connection that ends before its Subscribed is not tried again
        // until the socket is placed anew: the part waits on its watch.
        let (mut unanswered, _) = listener.accept().expect("the part subscribes");
        assert_eq!(unanswered.read_input(), signal_harness::Input::Subscribe);
        drop(unanswered);
        drop(listener);
        std::fs::remove_file(&socket).unwrap();
        let listener = UnixListener::bind(&socket).expect("the socket placed anew");
        let (mut first, _) = listener.accept().expect("the part subscribes once cued");
        assert_eq!(first.read_input(), signal_harness::Input::Subscribe);
        let session = flow_ethos::FlowId(CLAUDE_SESSION[..6].into());
        first.write_output(&signal_harness::Output::Subscribed(vec![
            harness::Observed {
                flow_id: session.clone(),
                state: harness::State::Flowing(harness::Flow::Working),
                tokens: harness::Tokens(41_234),
                background_work_vector: vec![],
            },
        ]));
        first.write_output(&signal_harness::Output::Changed(signal_harness::Changed {
            flow_id: session,
            event: harness::Event::TurnEnded,
            state: harness::State::Flowing(harness::Flow::Rested),
        }));
        drop(first);
        // The part reads the end only after it has taken the Changed, so
        // its second Subscribe follows the rest.
        let (mut second, _) = listener.accept().expect("the part subscribes again");
        assert_eq!(second.read_input(), signal_harness::Input::Subscribe);
        fixture.nexus.stopping.request();
        part.join().expect("the part ends when the Nexus stops");
    });

    assert_eq!(
        recorded_stops(&fixture).len(),
        1,
        "the part records no Stop"
    );
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234),
        "the context size is the harness's, not the transcript's"
    );
    let slept = stored(&fixture, Model::Sol);
    assert_eq!(slept.state, State::Asleep, "the resting seat sleeps");
    assert_eq!(
        slept.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
    );
    assert!(
        std::fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
            .unwrap_or_default()
            .contains("pane close w1:p3"),
        "the seat's harness is closed"
    );
}

/// The seat's session as a snapshot shows it.
fn observed(
    flow: harness::Flow,
    tokens: i64,
    background_work_vector: Vec<harness::BackgroundWork>,
) -> harness::Observed {
    harness::Observed {
        flow_id: seat_session(),
        state: harness::State::Flowing(flow),
        tokens: harness::Tokens(tokens),
        background_work_vector,
    }
}

/// The FlowId the Harness Nexus names the seat's session by.
fn seat_session() -> flow_ethos::FlowId {
    flow_ethos::FlowId(CLAUDE_SESSION[..6].into())
}

/// A change of the seat's session, leaving it in `flow`.
fn changed(event: harness::Event, flow: harness::Flow) -> HarnessObservation {
    HarnessObservation::Changed(signal_harness::Changed {
        flow_id: seat_session(),
        event,
        state: harness::State::Flowing(flow),
    })
}

fn shell_work() -> harness::BackgroundWork {
    harness::BackgroundWork {
        work_id: harness::WorkId("b1".into()),
        work_kind: harness::WorkKind::Shell,
        description: harness::Description("sleep 60".into()),
    }
}

/// The fixture with a subscription configured, each observation taken as
/// the subscriber takes it from its socket.
trait TakesObservations {
    fn subscribe(&mut self);
    fn take_all(&self, observations: Vec<HarnessObservation>);
}

impl TakesObservations for NexusFixture {
    fn subscribe(&mut self) {
        self.nexus.claude_harness_subscription = Some(ClaudeHarnessSubscription::from(
            ClaudeHarnessSubscriptionPath(
                "/run/user/1001/nexus/main/claude-harness-subscription.sock".into(),
            ),
        ));
    }

    fn take_all(&self, observations: Vec<HarnessObservation>) {
        let subscription = self
            .nexus
            .claude_harness_subscription
            .as_ref()
            .expect("a subscription configured");
        let mut subscriber = ClaudeHarnessSubscriber {
            nexus: &self.nexus,
            subscription,
            socket: std::path::PathBuf::from(&subscription.socket.0),
            unheld: Default::default(),
        };
        for observation in observations {
            subscriber.take(observation);
        }
    }
}

/// An OnDemand Claude seat with nothing queued, whose successor composes,
/// its transcript showing 40,000, its own session having taken a prompt
/// after its last Stop: no Stop hook marks its turn's end.
fn on_demand_seat_in_its_turn() -> NexusFixture {
    let mut fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    claude_prompt_taken(&fixture);
    super::refresh_and_compaction::declare_seat(
        &fixture,
        meta_signal_flow::Continuity::OnDemand,
        vec!["spirit:core:0".into(), "intent:flow:0".into()],
    );
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    fixture.subscribe();
    fixture
}

fn slept(fixture: &NexusFixture) -> bool {
    let metaflow = stored(fixture, Model::Sol);
    metaflow.state == State::Asleep
        && metaflow.past == flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
        && std::fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
            .unwrap_or_default()
            .contains("pane close w1:p3")
}

/// A turn that ends with background work in flight leaves the session
/// Waiting: no rest, and the seat stays awake. The WorkEnded that ends the
/// last of that work brings it to Rested: that is the flow's rest, and the
/// OnDemand seat with nothing queued sleeps.
#[test]
fn a_work_end_that_brings_waiting_to_rested_rests_the_flow() {
    let fixture = on_demand_seat_in_its_turn();
    fixture.take_all(vec![
        HarnessObservation::Snapshot(vec![observed(
            harness::Flow::Working,
            41_234,
            vec![shell_work()],
        )]),
        changed(harness::Event::TurnEnded, harness::Flow::Waiting),
    ]);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        "a turn ended Waiting is no rest"
    );
    assert_eq!(fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(), None);
    assert!(!fixture.nexus.background_settled(CLAUDE_SEAT));
    fixture.take_all(vec![changed(
        harness::Event::WorkEnded(harness::WorkId("b1".into())),
        harness::Flow::Rested,
    )]);
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234),
        "the rest wrote the harness's size"
    );
    assert!(slept(&fixture), "the resting seat sleeps");
    assert_eq!(recorded_stops(&fixture).len(), 1, "no Stop is recorded");
}

/// Escape ends a turn with no Stop hook: the Harness Nexus tells it
/// Interrupted, Flowing.Rested. That rests the flow, and the subscription's
/// Rested is the judgement that nothing is in flight, so the OnDemand seat
/// with nothing queued sleeps as it would after a hook Stop.
#[test]
fn an_interrupted_turn_with_no_stop_hook_rests_and_sleeps_an_on_demand_seat() {
    let fixture = on_demand_seat_in_its_turn();
    fixture.take_all(vec![
        HarnessObservation::Snapshot(vec![observed(harness::Flow::Working, 41_234, vec![])]),
        changed(harness::Event::Interrupted, harness::Flow::Rested),
    ]);
    assert_eq!(recorded_stops(&fixture).len(), 1, "no Stop hook ran");
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234)
    );
    assert!(slept(&fixture), "the resting seat sleeps");
}

/// A Changed that leaves a Rested session Rested is no rest: a ContextSize
/// or a Responded after the rest rests nothing again, though the size is
/// kept for the next rest, which writes it.
#[test]
fn a_context_size_while_rested_does_not_rest_again() {
    let mut fixture = maintained_claude_seat();
    fixture.subscribe();
    fixture.take_all(vec![
        HarnessObservation::Snapshot(vec![observed(harness::Flow::Working, 41_234, vec![])]),
        changed(harness::Event::TurnEnded, harness::Flow::Rested),
    ]);
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234),
        "the rest wrote the harness's size"
    );
    fixture.take_all(vec![
        changed(
            harness::Event::ContextSize(harness::Tokens(52_000)),
            harness::Flow::Rested,
        ),
        changed(
            harness::Event::Responded(harness::Response("done".into())),
            harness::Flow::Rested,
        ),
    ]);
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234),
        "no rest wrote the size told while Rested"
    );
    fixture.take_all(vec![
        changed(harness::Event::PromptSubmitted, harness::Flow::Working),
        changed(harness::Event::TurnEnded, harness::Flow::Rested),
    ]);
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(52_000),
        "the next rest writes the size kept"
    );
}

/// A session a snapshot shows Rested is the flow's rest when Flow's own
/// record does not show the flow Resting, a rest missed while no
/// connection was open; when it does, the rest was taken already and is not
/// taken again.
#[test]
fn a_session_found_rested_rests_a_flow_not_yet_resting_once() {
    let mut fixture = maintained_claude_seat();
    fixture.subscribe();
    assert_ne!(
        the_seat_s_state(&fixture),
        Some(meta_signal_flow::State::Resting)
    );
    fixture.take_all(vec![HarnessObservation::Snapshot(vec![observed(
        harness::Flow::Rested,
        41_234,
        vec![],
    )])]);
    assert_eq!(
        the_seat_s_state(&fixture),
        Some(meta_signal_flow::State::Resting),
        "the missed rest is taken"
    );
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234)
    );
    fixture.take_all(vec![HarnessObservation::Snapshot(vec![observed(
        harness::Flow::Rested,
        52_000,
        vec![],
    )])]);
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(41_234),
        "a flow already Resting is not rested again"
    );
}
