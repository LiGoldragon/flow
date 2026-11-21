//! The Claude Harness subscription against a fake subscription socket: its
//! configuration beside the place file; a rested turn end it tells resting
//! the flow with the context size the Harness Nexus told, recording no
//! harness event; and the one source of a Claude flow's rest, the
//! subscription when one is configured, the Stop hook when none is.

use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, address, append_main_rows, claude_agent, claude_seat, end_turn,
    maintain, metaflow, report_claude_stop, stored,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::HoldsStopRequest;
use crate::generated::memory::RecordedHarnessEvent;
use crate::generated::startup::{
    ClaudeHarnessSubscriptionPath, DaemonConfiguration, DaemonConfigurationRefusal,
};
use crate::generated::transitions::{AskedFlow, RecordsTransitions};
use crate::harness_subscription::{LocatesDaemonConfiguration, SubscribesToClaudeHarness};
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
/// flow's own Stop is still recorded, and is its own turn end for what reads
/// the recorded Stops, but rests nothing.
#[test]
fn with_a_subscription_the_stop_hook_is_recorded_and_rests_nothing() {
    let mut fixture = maintained_claude_seat();
    fixture.nexus.claude_harness_subscription = Some(ClaudeHarnessSubscriptionPath(
        "/run/user/1001/nexus/main/claude-harness-subscription.sock".into(),
    ));
    report_claude_stop(&fixture, vec![]);
    assert_eq!(recorded_stops(&fixture).len(), 2, "the Stop is recorded");
    assert!(
        fixture.nexus.background_settled(CLAUDE_SEAT),
        "the recorded Stop is the flow's own turn end"
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
    fixture.nexus.claude_harness_subscription = Some(ClaudeHarnessSubscriptionPath(
        socket.to_string_lossy().into_owned(),
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
