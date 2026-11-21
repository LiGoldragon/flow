//! The Claude Harness subscription against a fake subscription socket: its
//! configuration beside the place file, and a rested turn end it tells
//! reaching Memory as the flow's own Stop.

use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, address, claude_agent, claude_seat, claude_transcript, maintain,
    metaflow,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::HoldsStopRequest;
use crate::generated::memory::RecordedHarnessEvent;
use crate::generated::startup::{
    ClaudeHarnessSubscriptionPath, DaemonConfiguration, DaemonConfigurationRefusal,
};
use crate::harness_subscription::{LocatesDaemonConfiguration, SubscribesToClaudeHarness};
use crate::store::context::RecordsContextSize;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SeedsMetaflow;
use flow_ethos::State;
use signal_flow::Model;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use translator::place_file::{ConfigurationHome, LocatesConfigurationHome};
use translator::placement::PlaceName;

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

/// The part, started before its socket exists, subscribes once the socket
/// is placed, and after a connection that ends unanswered, once it is placed
/// anew; the snapshot maps to nothing, and a rested turn end of the
/// session the seat is registered with is recorded as the seat's own Stop,
/// naming its session and transcript, and acted on: its context size is
/// written. When the connection ends the part subscribes again.
#[test]
fn a_rested_turn_end_reaches_memory_as_the_flow_s_own_stop() {
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
    let stops = || {
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
            .collect::<Vec<_>>()
    };
    assert_eq!(stops().len(), 1, "the seat's own rest is recorded first");
    assert_eq!(fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(), None);

    let socket = fixture
        .directory
        .path()
        .join("claude-harness/claude-harness-subscription.sock");
    let configuration = DaemonConfiguration {
        claude_harness_subscription_path_option: Some(ClaudeHarnessSubscriptionPath(
            socket.to_string_lossy().into_owned(),
        )),
    };
    std::thread::scope(|scope| {
        let part = scope.spawn(|| {
            fixture
                .nexus
                .keep_subscribing_to_claude_harness(&configuration)
        });
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
        first.write_output(&signal_harness::Output::Subscribed(vec![
            harness::Observed {
                flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
                state: harness::State::Flowing(harness::Flow::Working),
                tokens: harness::Tokens(151_000),
                background_work_vector: vec![],
            },
        ]));
        first.write_output(&signal_harness::Output::Changed(signal_harness::Changed {
            flow_id: flow_ethos::FlowId(CLAUDE_SESSION[..6].into()),
            event: harness::Event::TurnEnded,
            state: harness::State::Flowing(harness::Flow::Rested),
        }));
        drop(first);
        // The part reads the end only after it has taken the Changed, so
        // its second Subscribe follows the report.
        let (mut second, _) = listener.accept().expect("the part subscribes again");
        assert_eq!(second.read_input(), signal_harness::Input::Subscribe);
        fixture.nexus.stopping.request();
        part.join().expect("the part ends when the Nexus stops");
    });

    let stops = stops();
    assert_eq!(stops.len(), 2, "{stops:?}");
    let stop = stops.last().unwrap();
    assert_eq!(
        stop.native_session_id_option,
        Some(signal_flow::NativeSessionId(CLAUDE_SESSION.into()))
    );
    assert_eq!(
        stop.transcript_path_option,
        Some(signal_flow::TranscriptPath(
            claude_transcript(&fixture).to_string_lossy().into_owned()
        ))
    );
    assert!(stop.background_work.0.is_empty());
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(151_000),
        "the mapped Stop was acted on as a hook's"
    );
}
