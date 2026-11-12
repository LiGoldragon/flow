//! Wake and drain against the fixture Herdr: an idle seat of any model and
//! either harness keeps its pane and stays Awake, and work queued for it is
//! handed into that pane; a seat whose pane is gone is Asleep, and a queued
//! request wakes a fresh flow composed from the metaflow's last launch;
//! queued work remains durable until native history receipts it.

use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::RunningNexus;
use crate::composition::{ComposesLaunch, FreshSeat, RendersQueuedWork};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::operation::Failed_Data;
use crate::generated::operation::{Operation, Outcome};
use crate::generated::starter::ProfilesFreshSeat;
use crate::herdr::launch::ChecksComposedLaunch;
use crate::herdr::pane::PaneAgent;
use crate::herdr::reservation::ChoosesNativeSession;
use crate::live::HoldsLiveValue;
use crate::performing::Performs;
use crate::reporting::RecordsReports;
use crate::store::ManagesMetaflowState;
use crate::store::context::RecordsContextSize;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::{
    OpensFlowStore, ReadsFlowRows, RecordsFlowLifecycle, RecordsNativeLaunchBinding,
    RecordsNativeLaunchIntent, RegistersFlowIdentity, ReservesLaunchAttempt,
};
use crate::waking::{
    AnswersWakeRefusal, AtStop, FreshContext, HoldsWakingMetaflows, IdentifiesWakingLease,
    KeptAtStop, PerformsWakeAndDrain, SleepsAndWakesSeats, WakePlan, WakePolicy, WakeRefusal,
};
use flow_ethos::{Delivery, State};
use signal_flow::{
    Address, Aspect, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind,
    HerdrRoute, HerdrRouteSelection, LaunchProfile, Message, Metaflow, Model, NativeLaunchBinding,
    NativeLaunchIntent, Origin, QueueEntry, Request, SentBy,
};
use std::fs;

#[test]
fn a_wake_refusal_notice_keeps_the_missing_module_identity() {
    let refusal = signal_flow::ContextRefusal::ModuleMissing(Some(signal_flow::ModuleIdentity(
        "vision:absent".into(),
    )));
    assert_eq!(
        WakeRefusal::StartRejected(signal_flow::StartRejection::ContextRefused(refusal.clone()))
            .failure(),
        Failed_Data::ContextRefused(refusal)
    );
}

pub(super) fn address(model: Model) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("flow".into()),
        model,
    }
}

pub(super) fn metaflow(
    model: Model,
    state: State,
    past: Vec<&str>,
    queue: Vec<Request>,
) -> Metaflow {
    Metaflow {
        address: address(model),
        state,
        past: flow_ethos::Past(
            past.into_iter()
                .map(|flow| flow_ethos::FlowId(flow.into()))
                .collect(),
        ),
        queue: flow_ethos::Queue(queue.unsent()),
    }
}

/// A request as it is queued with no sender kept: what seeding a queue in
/// these tests stands for, and what a request queued before Flow kept
/// senders reads as.
trait Unsent {
    type Queued;
    fn unsent(self) -> Self::Queued;
}

impl Unsent for Request {
    type Queued = QueueEntry;
    fn unsent(self) -> QueueEntry {
        use sha2::Digest;
        let logical_delivery_id = format!(
            "test-{:x}",
            sha2::Sha256::digest(format!("{self:?}").as_bytes())
        );
        QueueEntry {
            request_id: flow_ethos::RequestId(logical_delivery_id[5..9].into()),
            logical_delivery_id: flow_ethos::LogicalDeliveryId(logical_delivery_id),
            sent_by: SentBy::Unknown,
            delivery: flow_ethos::Delivery::Wake,
            message: Message::MiddleAbrupt(self),
        }
    }
}

impl Unsent for Vec<Request> {
    type Queued = Vec<QueueEntry>;
    fn unsent(self) -> Vec<QueueEntry> {
        let mut ids = std::collections::HashSet::new();
        self.into_iter()
            .enumerate()
            .map(|(index, request)| {
                let mut entry = request.unsent();
                if !ids.insert(entry.logical_delivery_id.clone()) {
                    entry.logical_delivery_id.0.push_str(&format!("-{index}"));
                    ids.insert(entry.logical_delivery_id.clone());
                    use sha2::Digest;
                    entry.request_id = flow_ethos::RequestId(
                        format!(
                            "{:x}",
                            sha2::Sha256::digest(entry.logical_delivery_id.0.as_bytes())
                        )[..4]
                            .into(),
                    );
                }
                entry
            })
            .collect()
    }
}

/// Declares the metaflow at `address` Maintained: it always has a flow, and
/// an idle seat keeps its harness.
pub(super) fn maintain(fixture: &NexusFixture, address: Address) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
            meta_signal_flow::ConfigureMetaflow {
                root_option: None,
                address,
                system_modules: signal_flow::SystemModules(vec![
                    signal_flow::ModuleIdentity("spirit-core".into()),
                    signal_flow::ModuleIdentity("intent-flow".into()),
                ]),
                user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                    "operation-main-flow".into(),
                )]),
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                continuity: meta_signal_flow::Continuity::Maintained,
                purpose: meta_signal_flow::Purpose(String::new()),
                records: meta_signal_flow::Records(vec![]),
            },
        ]))
        .expect("metaflow declared Maintained");
}

pub(super) fn stored(fixture: &NexusFixture, model: Model) -> Metaflow {
    fixture
        .nexus
        .store
        .metaflow(&address(model))
        .expect("metaflow readable")
        .expect("metaflow held")
}

fn closes(fixture: &NexusFixture) -> bool {
    fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
        .unwrap_or_default()
        .contains("pane close w1:p3")
}

/// A Claude seat `c1a0de` resting in pane w1:p3 of the fixture Herdr, its
/// claim written, its native transcript showing a context of `tokens`, its
/// last turn event its own Stop with nothing in flight.
pub(super) const CLAUDE_SEAT: &str = "c1a0de";
pub(super) const CLAUDE_SESSION: &str = "c1a0de00-0000-4000-8000-000000000001";

pub(super) fn claude_agent(fixture: &NexusFixture) -> serde_json::Value {
    let mut agent = fixture.settled_agent();
    agent["agent"] = serde_json::Value::String("claude".into());
    agent["agent_session"] = serde_json::json!({
        "source":"herdr:claude","agent":"claude","kind":"id","value":CLAUDE_SESSION
    });
    agent
}

/// An agent that reports no native session, as a remote-resumed Codex TUI
/// shows in Herdr.
fn quiet_agent(fixture: &NexusFixture) -> serde_json::Value {
    let mut agent = fixture.settled_agent();
    if let Some(fields) = agent.as_object_mut() {
        fields.remove("agent_session");
    }
    agent
}

pub(super) fn claude_seat(fixture: &NexusFixture, tokens: i64) {
    let mut node = fixture.node();
    node.flow_id = flow_ethos::FlowId(CLAUDE_SEAT.into());
    node.session_id = signal_flow::SessionId(CLAUDE_SESSION.into());
    node.harness_kind = HarnessKind::Claude;
    node.endpoint_selection = EndpointSelection::Unavailable;
    fixture
        .nexus
        .store
        .register_flow(node)
        .expect("Claude seat registered");
    fs::write(
        fixture
            .directory
            .path()
            .join(format!("flows/.{CLAUDE_SEAT}.flow-id")),
        format!(
            "version=1\nharness=claude\nidentity={}\nalias={CLAUDE_SEAT}\nuuid-version=uuid-v4\n",
            CLAUDE_SESSION.replace('-', "")
        ),
    )
    .unwrap();
    let project = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join(format!("{CLAUDE_SESSION}.jsonl")),
        format!(
            "{}\n",
            serde_json::json!({"type":"assistant","message":{"usage":{
                "input_tokens": 3,
                "cache_read_input_tokens": tokens - 13,
                "cache_creation_input_tokens": 10,
                "output_tokens": 50
            }}})
        ),
    )
    .unwrap();
    use crate::store::events::RecordsHarnessEvents;
    fixture
        .nexus
        .store
        .record_event(CLAUDE_SEAT, claude_stop(fixture))
        .expect("the seat's rest recorded");
}

/// The Stop the seat's own hook reports when its turn ends with
/// `background_work` in flight.
pub(super) fn claude_stop_with(
    fixture: &NexusFixture,
    background_work: Vec<signal_flow::BackgroundTask>,
) -> signal_flow::Event {
    signal_flow::Event::Stopped(signal_flow::NativeStop {
        native_session_id_option: Some(signal_flow::NativeSessionId(CLAUDE_SESSION.into())),
        transcript_path_option: Some(signal_flow::TranscriptPath(
            claude_transcript(fixture).to_string_lossy().into_owned(),
        )),
        last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
        background_work: signal_flow::BackgroundWork(background_work),
    })
}

/// That Stop as the hook sends it: recorded, answered, then acted on.
pub(super) fn report_claude_stop(
    fixture: &NexusFixture,
    background_work: Vec<signal_flow::BackgroundTask>,
) {
    let stop = claude_stop_with(fixture, background_work);
    assert_eq!(
        fixture.nexus.report(signal_flow::Report_Data {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            process_option: None,
            event: stop.clone(),
        }),
        signal_flow::Response::Reported
    );
    fixture.nexus.after_report(
        &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
        &stop,
    );
}

/// `flow_id`'s own Stop with nothing in flight, recorded as its last turn
/// event: the flow rests.
pub(super) fn rested_at_stop(fixture: &NexusFixture, flow_id: &str) {
    use crate::store::events::RecordsHarnessEvents;
    use crate::waking::ReadsClaudeDelivery;
    let node = fixture
        .nexus
        .store
        .flow_node(flow_id)
        .unwrap()
        .expect("the flow is held");
    let transcript = fixture
        .nexus
        .claude_transcript(&node)
        .expect("the flow's transcript");
    fixture
        .nexus
        .store
        .record_event(
            flow_id,
            signal_flow::Event::Stopped(signal_flow::NativeStop {
                native_session_id_option: Some(signal_flow::NativeSessionId(
                    node.session_id.0.clone(),
                )),
                transcript_path_option: Some(signal_flow::TranscriptPath(
                    transcript.to_string_lossy().into_owned(),
                )),
                last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
                background_work: signal_flow::BackgroundWork(vec![]),
            }),
        )
        .expect("the flow's rest recorded");
}

/// The seat's own session took a prompt after its last Stop: it is in its
/// turn.
pub(super) fn claude_prompt_taken(fixture: &NexusFixture) {
    use crate::store::events::RecordsHarnessEvents;
    fixture
        .nexus
        .store
        .record_event(
            CLAUDE_SEAT,
            signal_flow::Event::PromptSubmitted(signal_flow::NativePrompt {
                native_session_id_option: Some(signal_flow::NativeSessionId(CLAUDE_SESSION.into())),
                prompt_id_option: None,
                prompt_sha256: signal_flow::PromptSha256("0".repeat(64)),
            }),
        )
        .expect("the seat's prompt recorded");
}

/// One task in flight, as Claude Code names it in a Stop.
pub(super) fn background_task(
    id: &str,
    kind: signal_flow::BackgroundTaskKind,
) -> signal_flow::BackgroundTask {
    signal_flow::BackgroundTask {
        background_task_id: signal_flow::BackgroundTaskId(id.into()),
        background_task_kind: kind,
        background_task_status: signal_flow::BackgroundTaskStatus("running".into()),
        description: signal_flow::Description(format!("task {id}")),
    }
}

/// The seat's own Claude transcript.
pub(super) fn claude_transcript(fixture: &NexusFixture) -> std::path::PathBuf {
    fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary")
        .join(format!("{CLAUDE_SESSION}.jsonl"))
}

/// The Stop the seat's own hook reports: its registered session and that
/// session's transcript.
pub(super) fn claude_stop(fixture: &NexusFixture) -> signal_flow::Event {
    signal_flow::Event::Stopped(signal_flow::NativeStop {
        native_session_id_option: Some(signal_flow::NativeSessionId(CLAUDE_SESSION.into())),
        transcript_path_option: Some(signal_flow::TranscriptPath(
            claude_transcript(fixture).to_string_lossy().into_owned(),
        )),
        last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
        background_work: signal_flow::BackgroundWork(vec![]),
    })
}

/// An idle Maintained Claude flow of any model keeps its pane and stays
/// Awake: its Stop writes its context size and, with nothing queued, leaves
/// the seat as it is.
#[test]
fn an_idle_maintained_claude_flow_keeps_its_pane_and_stays_awake() {
    use crate::store::NamesLiveFlow;
    for model in [Model::Astra, Model::Sol, Model::Luna] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![claude_agent(&fixture)],
            PromptFixture::Prompted("w1:p3"),
        );
        claude_seat(&fixture, 151_000);
        fixture.nexus.store.seed_metaflow(metaflow(
            model.clone(),
            State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
            vec![],
            vec![],
        ));
        maintain(&fixture, address(model.clone()));

        fixture.nexus.after_report(
            &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
            &claude_stop(&fixture),
        );
        assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);

        assert_eq!(
            fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
            Some(151_000),
            "the context size is written at the Stop"
        );
        assert!(!closes(&fixture), "{model:?}: the idle seat keeps its pane");
        let kept = stored(&fixture, model.clone());
        assert_eq!(
            kept.state,
            State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
        );
        assert!(kept.past.0.is_empty());
        let node = fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .expect("flow row kept");
        assert!(node.flow_lifecycle.is_live(), "{:?}", node.flow_lifecycle);
    }
}

/// An idle Maintained Codex flow keeps its pane and stays Awake: its thread
/// coming to rest with nothing queued settles nothing, and a turn end read
/// then leaves the seat as it is.
#[test]
fn an_idle_maintained_codex_flow_keeps_its_pane_and_stays_awake() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("908786".into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));
    assert_eq!(
        fixture.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::ThreadNotResting)
    );

    // Test 5: the thread's coming to rest settles the seat itself, once,
    // and a Maintained seat with nothing queued stays Idle.
    let thread = fixture.node().session_id;
    assert_eq!(
        fixture
            .nexus
            .codex_events(&thread.0, vec![TurnEvent::Status(ThreadStatus::Idle)]),
        Some(AtStop::Idle)
    );
    assert!(!closes(&fixture), "the idle seat keeps its pane");
    let kept = stored(&fixture, Model::Sol);
    assert_eq!(
        kept.state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
    assert!(kept.past.0.is_empty());
    let node = fixture
        .nexus
        .store
        .flow_node("908786")
        .unwrap()
        .expect("flow row kept");
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Active);
}

/// An idle OnDemand Claude flow, its turn ended with nothing queued, sleeps:
/// its harness is closed, its flow recorded Stopped and its metaflow Asleep
/// with the flow in Past, its Address and the flow's row and transcript
/// kept. A metaflow no declaration names is OnDemand.
#[test]
fn an_idle_on_demand_claude_flow_closes_its_harness_and_keeps_its_address() {
    let fixture = NexusFixture::new();
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
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));

    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Slept(address(Model::Sol))
    );
    assert!(closes(&fixture), "the idle seat's harness is closed");
    assert!(fixture.typed().is_none(), "nothing is typed");
    let slept = stored(&fixture, Model::Sol);
    assert_eq!(slept.state, State::Asleep);
    assert_eq!(
        slept.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
    );
    let node = fixture
        .nexus
        .store
        .flow_node(CLAUDE_SEAT)
        .unwrap()
        .expect("the flow's row is kept");
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Stopped);
    assert_eq!(
        node.session_id.0, CLAUDE_SESSION,
        "its transcript stays named"
    );
    assert!(claude_transcript(&fixture).exists(), "its transcript stays");
}

/// An idle OnDemand Codex flow sleeps once its app-server shows its thread
/// resting with nothing queued.
#[test]
fn an_idle_on_demand_codex_flow_closes_its_harness() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    // Its successor composes, so it may close.
    super::refresh_and_compaction::declare_seat(
        &fixture,
        meta_signal_flow::Continuity::OnDemand,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("908786".into())),
        vec![],
        vec![],
    ));
    // Test 4: the thread's rest settles the seat itself; it sleeps.
    let thread = fixture.node().session_id;
    assert_eq!(
        fixture
            .nexus
            .codex_events(&thread.0, vec![TurnEvent::Status(ThreadStatus::Idle)]),
        Some(AtStop::Slept(address(Model::Sol)))
    );
    assert!(closes(&fixture), "the idle seat's harness is closed");
    let slept = stored(&fixture, Model::Sol);
    assert_eq!(slept.state, State::Asleep);
    assert_eq!(
        slept.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("908786".to_owned())])
    );
}

/// An Order queued for an idle Awake Claude flow is handed into its live
/// pane at once: no Stop is awaited, no pane is opened, and the metaflow
/// stays Awake as the same flow.
#[test]
fn a_request_to_an_idle_awake_claude_flow_is_delivered_into_its_pane() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    append_main_rows(&fixture, &[end_turn("idle-turn")]);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));
    assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);
    assert!(fixture.typed().is_none(), "nothing typed while idle");

    let mut idle = stored(&fixture, Model::Sol);
    idle.queue
        .0
        .push(Request::Order("Read the new ruling.".into()).unsent());
    fixture.nexus.store.seed_metaflow(idle);
    fixture.nexus.wake_due(address(Model::Sol));

    let typed = fixture.typed().expect("typed into the live pane");
    assert!(typed.contains("Read the new ruling."), "{typed}");
    assert!(!closes(&fixture));
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        "the same flow, still Awake"
    );
}

/// A Maintained Claude seat Awake at the Sol Address, and a caller bound in
/// pane-1 to send it work. Returns the caller, killed on drop, and its
/// process.
pub(super) fn soft_recipient(
    fixture: &NexusFixture,
    resting: bool,
) -> (super::MarkedProcess, signal_flow::Process) {
    use crate::store::{FlowStore, ReadsFlowMemory};
    super::bind_existing_caller(fixture, "a001", "pane-1");
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(fixture, address(Model::Sol));
    soft_recipient_shows(fixture, resting);
    let caller = super::MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    (caller, peer)
}

/// Shows the seat in its turn, Herdr showing it Working and its transcript
/// recording no end of its main turn, or resting, its transcript recording
/// that end and Herdr showing it Idle.
pub(super) fn soft_recipient_shows(fixture: &NexusFixture, resting: bool) {
    if resting {
        append_main_rows(fixture, &[end_turn("turn-ended")]);
    }
    let seat = if resting {
        claude_agent(fixture)
    } else {
        working_claude_agent(fixture)
    };
    fixture.accept_pane_operations(
        vec![super::codex_agent_in("pane-1", "terminal-pane-1"), seat],
        PromptFixture::Prompted("w1:p3"),
    );
}

/// Sends `body` Soft from `peer` to the seat as a flow's own Send is
/// answered: a request left pending is scheduled again once answered.
pub(super) fn send_soft(
    fixture: &NexusFixture,
    peer: &signal_flow::Process,
    body: &str,
) -> signal_flow::Response {
    use crate::sending::SendsRequests;
    let answer = fixture.nexus.send_request(
        signal_flow::SendRequest {
            address: address(Model::Sol),
            sendable_message: super::sendable_message(Message::Soft(Request::Order(body.into()))),
        },
        Some(peer),
    );
    if let Some(pending) = answer.pending {
        fixture.nexus.wake_due(pending.address);
    }
    answer.response
}

/// A Soft request to a seat in its turn is held in its queue, and the
/// seat's Stop, its idle event, hands it over.
#[test]
fn a_held_soft_is_delivered_at_its_recipients_idle_event() {
    let fixture = NexusFixture::new();
    let (_caller, peer) = soft_recipient(&fixture, false);
    let response = send_soft(&fixture, &peer, "Read the ruling when you rest.");
    assert!(
        matches!(response, signal_flow::Response::SendAccepted(_)),
        "{response:?}"
    );
    assert!(fixture.typed().is_none(), "nothing typed during the turn");
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1, "held");

    soft_recipient_shows(&fixture, true);
    fixture.nexus.after_report(
        &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
        &claude_stop(&fixture),
    );

    let typed = fixture.typed().expect("typed at the Stop");
    assert!(typed.contains("Read the ruling when you rest."), "{typed}");
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
}

/// Several Soft requests held during one turn are handed over together at
/// its one idle event, oldest first.
#[test]
fn several_held_soft_messages_are_delivered_in_order_at_one_idle_event() {
    let fixture = NexusFixture::new();
    let (_caller, peer) = soft_recipient(&fixture, false);
    for body in ["First held.", "Second held.", "Third held."] {
        let response = send_soft(&fixture, &peer, body);
        assert!(
            matches!(response, signal_flow::Response::SendAccepted(_)),
            "{response:?}"
        );
    }
    assert!(fixture.typed().is_none(), "nothing typed during the turn");
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 3, "all held");

    soft_recipient_shows(&fixture, true);
    fixture.nexus.after_report(
        &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
        &claude_stop(&fixture),
    );

    let typed = fixture.typed().expect("typed at the Stop");
    let at = |body: &str| {
        typed
            .find(body)
            .unwrap_or_else(|| panic!("{body}: {typed}"))
    };
    assert!(
        at("First held.") < at("Second held.") && at("Second held.") < at("Third held."),
        "oldest first: {typed}"
    );
    assert_eq!(
        super::prompts(&fixture.directory.path().join("herdr-operations.log")).len(),
        1,
        "one idle event, one hand-over"
    );
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
}

/// A hand-over types every queued message as its own NativeInput under
/// its own sender: a Task from one flow, the reminder Flow raised for it
/// and a Done from another flow reach the pane as one top-level vector,
/// oldest first, each naming who sent it, none folded into another's
/// Notice. What is typed is exactly the text whose receipt settles them.
#[test]
fn an_abrupt_cohort_is_typed_as_one_vector_each_input_naming_its_sender() {
    use datom_codec::{Actualizing, Potential};
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    append_main_rows(&fixture, &[end_turn("idle-turn")]);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));
    assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);

    let from = |aspect: Aspect, model: Model| {
        SentBy::Sender(Address {
            aspect,
            topic: flow_ethos::Topic("flow".into()),
            model,
        })
    };
    let task = QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId(
            "logical-delivery-00000000000000000001".into(),
        ),
        request_id: flow_ethos::RequestId("t001".into()),
        sent_by: from(Aspect::Psyche, Model::Astra),
        delivery: flow_ethos::Delivery::Wake,
        message: Message::MiddleAbrupt(Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Draft the proposal.".into(),
        })),
    };
    let reminder = QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-delivery-00000000000000000002".into()),
        request_id: flow_ethos::RequestId("t002".into()),
        sent_by: SentBy::Flow,
        delivery: flow_ethos::Delivery::Wake,
        message: Message::Soft(Request::Notice(
            "Reminder 1 of 4: the task of 30 minutes, Draft the proposal., to Psyche flow Secondary has no Done after 30 minutes."
                .into(),
        )),
    };
    let done = QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId(
            "logical-delivery-00000000000000000003".into(),
        ),
        request_id: flow_ethos::RequestId("t003".into()),
        sent_by: from(Aspect::Mind, Model::Sol),
        delivery: flow_ethos::Delivery::Wake,
        message: Message::Soft(Request::Done(flow_ethos::DonePayload::Historical(
            flow_ethos::Historical_Data {
                logical_delivery_id: flow_ethos::LogicalDeliveryId(
                    "logical-delivery-00000000000000000000".into(),
                ),
                string: "Proposal drafted.".into(),
            },
        ))),
    };
    let cohort = vec![task, reminder, done];
    let mut idle = stored(&fixture, Model::Sol);
    idle.queue = flow_ethos::Queue(cohort.clone());
    fixture.nexus.store.seed_metaflow(idle);
    fixture.nexus.wake_due(address(Model::Sol));

    let typed = fixture.typed().expect("typed into the live pane");
    let mut budget = datom_codec::Budget {
        remaining: 65536,
        reader: protos::ReaderBudget::try_from(65536).expect("a constant reader budget"),
        depth: 0,
        maximum_depth: 1024,
    };
    let inputs = Potential::<Vec<signal_flow::VisibleNativeInput>>::from(typed.clone())
        .actualize(&mut budget)
        .unwrap_or_else(|error| panic!("{typed}: {error:?}"));
    use crate::message_projection::ProjectsQueueEntry;
    let expected = cohort
        .iter()
        .map(|entry| entry.visible_input(&address(Model::Sol)))
        .collect::<Vec<_>>();
    assert_eq!(inputs, expected, "{typed}");
    assert_eq!(
        typed,
        cohort.prompt_text(&address(Model::Sol)),
        "the receipt text is what was typed"
    );
}

/// A flow whose pane is gone, closed by the living or crashed, is Asleep
/// once work for it finds the pane gone: its flow is recorded Exited and
/// joins Past, and the Order wakes a fresh flow on a new native session.
/// The queued Order remains durable until native history receipts it.
#[test]
fn a_flow_whose_pane_is_gone_is_asleep_and_wakes_fresh() {
    let mut fixture = NexusFixture::new();
    let at = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    // Live in Flow's records; the fixture Herdr lists no agent: its pane is
    // gone.
    let seat = launched_claude_ended_by(&fixture, |_| true);
    let mut awake = stored(&fixture, Model::Sol);
    assert_eq!(
        awake.state,
        State::Awake(flow_ethos::FlowId("abc123".into()))
    );
    let queue = vec![Request::Order("Carry on.".into())];
    awake.queue = flow_ethos::Queue(queue.clone().unsent());
    fixture.nexus.store.seed_metaflow(awake);

    fixture.nexus.drain_awake(&at);
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(
        asleep.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node("abc123")
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Exited
    );
    assert!(fixture.nexus.wake_is_due(&asleep));

    let world = fixture.fresh_claude_world();
    fixture.nexus.wake_due(at);
    let woken = stored(&fixture, Model::Sol);
    let State::Awake(flow_id) = &woken.state else {
        panic!("woken fresh: {woken:?}; Herdr calls: {}", world.calls());
    };
    assert_ne!(flow_id.0, "abc123", "a new flow");
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_ne!(world.session(), seat.session, "a new native session");
    assert!(
        woken.queue.0.is_empty(),
        "the accepted first prompt took the queue"
    );
}

/// An Order queued for an Asleep Primary wakes it as it wakes any model:
/// a fresh flow on the Primary's configured model, on a new native
/// session. The queued Order is its accepted first prompt. Its Secondary is
/// recorded as every Primary starts.
#[test]
fn an_order_wakes_an_asleep_primary_fresh() {
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Astra);
    let queue = vec![Request::Order("Carry the design on.".into())];
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["5ec0d0"], vec![]));
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Astra,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    let world = fixture.fresh_claude_world();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                model: Model::Astra,
                native: meta_signal_flow::Native("claude-fixture".into()),
            },
        ]))
        .expect("the Primary's model configured");
    assert!(fixture.nexus.wake_is_due(&stored(&fixture, Model::Astra)));

    fixture.nexus.wake_due(at.clone());

    let woken = stored(&fixture, Model::Astra);
    let State::Awake(flow_id) = &woken.state else {
        panic!(
            "the Primary wakes: {woken:?}; Herdr calls: {}",
            world.calls()
        );
    };
    assert_ne!(flow_id.0, "abc123");
    assert!(
        woken.queue.0.is_empty(),
        "the accepted first prompt took the queue"
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_ne!(world.session(), seat.session);
    let node = fixture.nexus.store.flow_node(&flow_id.0).unwrap().unwrap();
    assert_eq!(node.session_id, signal_flow::SessionId(world.session()));
}

/// A seat that goes idle with work queued hands over its whole ordered
/// cohort and removes it after the exact native receipt.
#[test]
fn a_resting_seat_with_work_queued_is_handed_the_oldest_request_and_stays_awake() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    let queued = vec![
        Request::Notice("first".into()),
        Request::Result(flow_ethos::ResultPayload::Historical("second".into())),
    ]
    .unsent();
    let expected_prompt = queued.clone().prompt_text(&address(Model::Sol));
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(queued),
    });

    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Drained(_)
    ));
    let kept = stored(&fixture, Model::Sol);
    assert_eq!(
        kept.state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
    assert!(
        kept.queue.0.is_empty(),
        "the exact receipt settles the cohort"
    );
    assert!(!closes(&fixture), "it keeps its pane");
    assert_eq!(
        fixture.typed().as_deref(),
        Some(expected_prompt.as_str()),
        "the resting seat receives the whole ordered cohort"
    );
}

/// An explicit all-queue delivery settles the whole ordered cohort on its
/// exact native receipt.
#[test]
fn explicit_delivery_hands_over_every_queued_request_in_order() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    let first = Request::Notice("first".into()).unsent();
    let second = Request::Result(flow_ethos::ResultPayload::Historical("second".into())).unsent();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![first.clone(), second.clone()]),
    });

    assert_eq!(
        fixture.nexus.drain_all(address(Model::Sol)),
        Outcome::Drained(vec![first, second])
    );
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
}

/// A Claude agent Herdr shows Working, as its screen reads while its main
/// turn runs or while background agents run after it ended.
pub(super) fn working_claude_agent(fixture: &NexusFixture) -> serde_json::Value {
    let mut agent = fixture.current_agent();
    agent["agent"] = serde_json::Value::String("claude".into());
    agent["agent_session"] = serde_json::json!({
        "source":"herdr:claude","agent":"claude","kind":"id","value":CLAUDE_SESSION
    });
    agent
}

/// Appends rows of the seat's own main thread to its transcript.
pub(super) fn append_main_rows(fixture: &NexusFixture, rows: &[serde_json::Value]) {
    use std::io::Write;
    let mut transcript = fs::OpenOptions::new()
        .append(true)
        .open(claude_transcript(fixture))
        .unwrap();
    for row in rows {
        writeln!(transcript, "{row}").unwrap();
    }
}

pub(super) fn end_turn(uuid: &str) -> serde_json::Value {
    serde_json::json!({"type":"assistant","sessionId":CLAUDE_SESSION,"uuid":uuid,
        "message":{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}})
}

#[test]
fn a_flow_that_stopped_while_working_or_awaited_is_kept() {
    // Herdr still shows the seat Working.
    let working = NexusFixture::new();
    working.accept_pane_operations(
        vec![working_claude_agent(&working)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&working, 40_000);
    working.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    assert!(matches!(
        working.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(KeptAtStop::NotResting(PaneAgent::Present {
            agent_state: signal_flow::AgentState::Working,
            ..
        }))
    ));
    // Its main turn ended while a background agent still runs, as its Stop
    // names: Herdr still shows Working, and the seat, with nothing queued,
    // stays idle in its pane.
    append_main_rows(&working, &[end_turn("main-ended")]);
    {
        use crate::store::events::RecordsHarnessEvents;
        working
            .nexus
            .store
            .record_event(
                CLAUDE_SEAT,
                claude_stop_with(
                    &working,
                    vec![background_task(
                        "a1",
                        signal_flow::BackgroundTaskKind::Subagent,
                    )],
                ),
            )
            .expect("the Stop recorded");
    }
    assert_eq!(working.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);
    assert_eq!(
        stored(&working, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
    assert!(!closes(&working));

    // Resting and unawaited, but a wake holds its metaflow.
    let waking = NexusFixture::new();
    waking.accept_pane_operations(
        vec![waking.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    waking.register_with(FlowLifecycle::Active);
    waking.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("908786".into())),
        vec![],
        vec![],
    ));
    let _lease = waking.nexus.waking.hold(&address(Model::Sol));
    assert_eq!(
        waking.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::Waking)
    );
    assert!(!closes(&waking));
}

/// An explicit wake applies to queued work without judging its request kind.
#[test]
fn abrupt_priority_makes_a_wake_due_at_every_layer() {
    assert!(RunningNexus::rests_at(&signal_flow::AgentState::Idle));
    assert!(RunningNexus::rests_at(&signal_flow::AgentState::Done));
    assert!(!RunningNexus::rests_at(&signal_flow::AgentState::Working));

    let fixture = NexusFixture::new();
    for model in [Model::Astra, Model::Sol, Model::Luna, Model::Terra] {
        assert!(
            fixture.nexus.wake_is_due(&metaflow(
                model.clone(),
                State::Asleep,
                vec![],
                vec![Request::Notice("fyi".into())],
            )),
            "queued work can be explicitly woken at {model:?}"
        );
    }
}

#[test]
fn the_store_sleeps_and_awakens_removing_only_the_confirmed_startup_batch() {
    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let at = address(Model::Luna);
    store.seed_metaflow(metaflow(
        Model::Luna,
        State::Awake(flow_ethos::FlowId("aaaaaa".into())),
        vec![],
        vec![],
    ));
    assert!(
        !store.sleep_metaflow(&at, "bbbbbb").unwrap(),
        "not Awake as it"
    );
    assert!(store.sleep_metaflow(&at, "aaaaaa").unwrap());
    assert!(
        !store.sleep_metaflow(&at, "aaaaaa").unwrap(),
        "already Asleep"
    );
    let mut asleep = store.metaflow(&at).unwrap().unwrap();
    asleep.queue = flow_ethos::Queue(
        vec![
            Request::Order("A".into()),
            Request::Order("B".into()),
            Request::Order("C".into()),
        ]
        .unsent(),
    );
    store.seed_metaflow(asleep);
    assert!(
        !store.awaken_metaflow(&at, "aaaaaa", 4).unwrap(),
        "more delivered than queued"
    );
    assert!(!store.awaken_metaflow(&at, "bbbbbb", 2).unwrap());
    assert!(store.awaken_metaflow(&at, "bbbbbb", 0).unwrap());
    let startup = store.metaflow(&at).unwrap().unwrap().queue.0[..2].to_vec();
    let receipt = signal_flow::NativeReceipt {
        harness_kind: HarnessKind::Claude,
        native_session_id: signal_flow::NativeSessionId("legacy-session-receipt".into()),
        native_prompt_id: signal_flow::NativePromptId("legacy-prompt-receipt".into()),
    };
    assert!(
        store
            .settle_wake_startup_receipts(&at, "bbbbbb", &startup, Some(&receipt))
            .unwrap()
    );
    let woken = store.metaflow(&at).unwrap().unwrap();
    assert_eq!(
        woken.state,
        State::Awake(flow_ethos::FlowId("bbbbbb".into()))
    );
    assert_eq!(
        woken.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("aaaaaa".to_owned())]),
        "the flow that slept stays in Past; the woken flow is current"
    );
    assert_eq!(
        woken.queue,
        flow_ethos::Queue(vec![Request::Order("C".into())].unsent()),
        "the later arrival remains behind the confirmed startup batch"
    );
    for entry in startup {
        assert!(matches!(
            store
                .logical_delivery_snapshot(&entry.logical_delivery_id)
                .unwrap()
                .unwrap()
                .delivery_state,
            signal_flow::DeliveryState::Delivered(ref delivered) if delivered == &receipt
        ));
    }
    assert!(
        store
            .remove_oldest_queued(&at, &Request::Order("C".into()).unsent())
            .unwrap()
    );
    assert!(store.metaflow(&at).unwrap().unwrap().queue.0.is_empty());
    assert_eq!(
        store.metaflow_awake_as("bbbbbb").unwrap().unwrap().address,
        at
    );
}

#[test]
fn a_wake_receipt_settles_its_logical_prefix_atomically_and_preserves_the_suffix() {
    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let at = address(Model::Luna);
    store.seed_metaflow(metaflow(
        Model::Luna,
        State::Awake(flow_ethos::FlowId("bbbbbb".into())),
        vec![],
        vec![],
    ));
    let sender = address(Model::Terra);
    for body in ["A", "B", "C"] {
        let accepted = store
            .accept_logical_delivery(
                &at,
                SentBy::Sender(sender.clone()),
                Delivery::Wake,
                Message::MiddleAbrupt(Request::Order(body.into())),
                None,
            )
            .unwrap();
        assert!(matches!(
            accepted,
            signal_flow::PrivateSendResponse::SendAccepted(_)
        ));
    }
    let queued = store.metaflow(&at).unwrap().unwrap().queue;
    let startup = queued.0[..2].to_vec();
    let later = queued.0[2].clone();
    let receipt = signal_flow::NativeReceipt {
        harness_kind: HarnessKind::Claude,
        native_session_id: signal_flow::NativeSessionId("session-receipt".into()),
        native_prompt_id: signal_flow::NativePromptId("prompt-receipt".into()),
    };

    assert!(
        !store
            .settle_wake_startup_receipts(&at, "notbbb", &startup, Some(&receipt))
            .unwrap(),
        "a different flow changes neither lifecycle nor queue"
    );
    assert_eq!(store.metaflow(&at).unwrap().unwrap().queue, queued);
    assert!(
        !store
            .settle_wake_startup_receipts(&at, "bbbbbb", &startup[1..], Some(&receipt))
            .unwrap(),
        "a non-prefix changes neither lifecycle nor queue"
    );
    assert_eq!(store.metaflow(&at).unwrap().unwrap().queue, queued);
    assert!(
        store
            .settle_wake_startup_receipts(&at, "bbbbbb", &startup, None)
            .is_err(),
        "logical deliveries require an exact receipt"
    );
    assert_eq!(store.metaflow(&at).unwrap().unwrap().queue, queued);

    // Reconcile the partial shape an older split settlement could leave:
    // one exact receipt is durable while its queue entry remains.
    store
        .transition_logical_delivery(
            &startup[0].logical_delivery_id,
            signal_flow::DeliveryState::Delivered(receipt.clone()),
        )
        .unwrap();
    let conflicting = signal_flow::NativeReceipt {
        harness_kind: HarnessKind::Claude,
        native_session_id: signal_flow::NativeSessionId("other-session".into()),
        native_prompt_id: signal_flow::NativePromptId("other-prompt".into()),
    };
    assert!(
        store
            .settle_wake_startup_receipts(&at, "bbbbbb", &startup, Some(&conflicting))
            .is_err(),
        "a conflicting receipt cannot rewrite evidence or drain the queue"
    );
    assert_eq!(store.metaflow(&at).unwrap().unwrap().queue, queued);
    assert!(matches!(
        store
            .logical_delivery_snapshot(&startup[1].logical_delivery_id)
            .unwrap()
            .unwrap()
            .delivery_state,
        signal_flow::DeliveryState::Queued
    ));
    assert!(
        store
            .settle_wake_startup_receipts(&at, "bbbbbb", &startup, Some(&receipt))
            .unwrap()
    );
    let settled = store.metaflow(&at).unwrap().unwrap();
    assert_eq!(settled.queue, flow_ethos::Queue(vec![later.clone()]));
    for entry in &startup {
        assert!(matches!(
            store
                .logical_delivery_snapshot(&entry.logical_delivery_id)
                .unwrap()
                .unwrap()
                .delivery_state,
            signal_flow::DeliveryState::Delivered(ref delivered) if delivered == &receipt
        ));
    }
    assert!(matches!(
        store
            .logical_delivery_snapshot(&later.logical_delivery_id)
            .unwrap()
            .unwrap()
            .delivery_state,
        signal_flow::DeliveryState::Queued
    ));
    let NexusFixture {
        directory, nexus, ..
    } = fixture;
    let path = directory.path().join("flow.sema");
    drop(nexus);
    let reopened = crate::store::FlowStore::open(&path).unwrap();
    assert_eq!(
        reopened.metaflow(&at).unwrap().unwrap().queue,
        flow_ethos::Queue(vec![later])
    );
    for entry in startup {
        assert!(matches!(
            reopened
                .logical_delivery_snapshot(&entry.logical_delivery_id)
                .unwrap()
                .unwrap()
                .delivery_state,
            signal_flow::DeliveryState::Delivered(ref delivered) if delivered == &receipt
        ));
    }
}

/// The context a wake composes a fresh flow from: the module identities and
/// leave-outs of the metaflow's last launch, whatever that flow's end, in
/// the Herdr session the Nexus declares.
fn last_launch_context() -> WakePlan {
    WakePlan::Fresh(FreshContext {
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        system_modules: signal_flow::SystemModules(vec![
            signal_flow::ModuleIdentity("spirit-core".into()),
            signal_flow::ModuleIdentity("intent-flow".into()),
        ]),
        user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
            "operation-main-flow".into(),
        )]),
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        leave_outs_option: None,
        purpose: crate::generated::starter::Purpose(String::new()),
    })
}

/// Every wake is fresh: a metaflow whose last flow Stopped, Exited or was
/// Retired wakes as a new flow composed from that flow's launch.
#[test]
fn every_wake_composes_a_fresh_flow_from_the_last_launch_whatever_its_end() {
    let asleep = metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]);
    let stopped = NexusFixture::new();
    ended_claude(&stopped);
    assert_eq!(stopped.nexus.wake_plan(&asleep), last_launch_context());

    let exited = NexusFixture::new();
    launched_claude_ended_by(&exited, |store| store.record_exited("abc123").unwrap());
    assert_eq!(exited.nexus.wake_plan(&asleep), last_launch_context());

    let retired = NexusFixture::new();
    launched_claude_ended_by(&retired, |store| {
        store.record_active("abc123").unwrap() && store.record_retired("abc123").unwrap()
    });
    assert_eq!(retired.nexus.wake_plan(&asleep), last_launch_context());
}

#[test]
fn a_fresh_wake_composes_modules_and_queued_work_in_the_first_prompt() {
    let fixture = NexusFixture::new();
    let queue = [
        Request::Order("first".into()),
        Request::Question("second".into()),
    ];
    let text = queue.to_vec().unsent().prompt_text(&address(Model::Sol));
    let seat = FreshSeat {
        address: address(Model::Sol),
        launch_request_id: "wake-0-1".into(),
        model_name: "claude-fixture".into(),
        effort: "medium".into(),
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        herdr_session_name: "fixture-session".into(),
        system_modules: signal_flow::SystemModules(vec![
            signal_flow::ModuleIdentity("spirit-core".into()),
            signal_flow::ModuleIdentity("intent-flow".into()),
        ]),
        user_modules: signal_flow::UserModules(vec![
            signal_flow::ModuleIdentity("operation-main-flow".into()),
            signal_flow::ModuleIdentity("knowledge-flow".into()),
        ]),
        leave_outs_option: None,
        prompt: text.clone(),
    };
    let profile = seat.fresh_profile();
    assert_eq!(profile.address, address(Model::Sol));
    assert_eq!(profile.instruction_prompt.0, text);
    assert_eq!(profile.user_modules, seat.user_modules);
    let composed = fixture
        .nexus
        .composer
        .current()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                harness_kind: HarnessKind::Claude,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap();
    // The same full module text a Start of these identities carries, with
    // the fixed queue batch in that first prompt.
    let body = composed.first_prompt_payload.first_prompt_body;
    assert!(
        body.0
            .starts_with("These are your skills, given here; do not reload them.")
    );
    assert!(body.0.contains("Main flow body.") && body.0.contains("Knowledge body."));
    assert!(
        composed
            .system_prompt
            .system_prompt_text
            .0
            .contains("Spirit body.")
    );
    assert!(body.0.contains(&text));
    assert!(!body.0.contains("seat"));
}

#[test]
fn an_asleep_seat_no_launch_of_record_names_is_unplaceable() {
    // A flow Flow holds with no launch of record (registered, not launched)
    // names no context to wake its metaflow with.
    let fixture = NexusFixture::new();
    fixture.set_agents(vec![fixture.current_agent()]);
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture
            .nexus
            .wake_plan(&metaflow(Model::Sol, State::Asleep, vec!["908786"], vec![],)),
        WakePlan::Unplaceable
    );
    assert_eq!(
        fixture
            .nexus
            .wake_plan(&metaflow(Model::Sol, State::Asleep, vec![], vec![])),
        WakePlan::Unplaceable
    );
}

/// A short-lived process standing as a peer that Bind binds: no flow Flow
/// launched runs it, so its Address is Awake as an id that names no flow.
struct BoundPeer(std::process::Child);

impl BoundPeer {
    fn spawn() -> Self {
        Self(
            std::process::Command::new("sleep")
                .arg("30")
                .spawn()
                .expect("peer process"),
        )
    }

    fn process(&self) -> signal_flow::Process {
        let pid = self.0.id();
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).expect("peer stat");
        let (_, fields) = stat.rsplit_once(") ").expect("stat fields");
        signal_flow::Process {
            pid: flow_ethos::Pid(i64::from(pid)),
            started: flow_ethos::Started(
                fields
                    .split_whitespace()
                    .nth(19)
                    .and_then(|field| field.parse().ok())
                    .expect("start time"),
            ),
        }
    }

    fn bind(&self, fixture: &NexusFixture, address: Address) {
        use crate::store::metaflow::MutatesMetaflows;
        let process = self.process();
        assert!(matches!(
            fixture.nexus.store.bind_metaflow(
                signal_flow::Bind_Data {
                    address,
                    process: process.clone(),
                },
                None,
            ),
            Ok(signal_flow::Response::Bound(_))
        ));
    }

    fn end(mut self) {
        self.0.kill().expect("peer killed");
        self.0.wait().expect("peer reaped");
    }
}

fn current(fixture: &NexusFixture, address: Address) -> signal_flow::Response {
    use crate::Dispatches;
    fixture.nexus.dispatch(signal_flow::Query::Current(address))
}

/// An Address Flow holds only through a binding is forgotten once the bound
/// process has ended: Flow can neither deliver to it nor wake it, and the
/// seat behind it is no flow Flow holds.
#[test]
fn an_address_held_only_through_an_ended_binding_is_forgotten() {
    let fixture = NexusFixture::new();
    let peer = BoundPeer::spawn();
    peer.bind(&fixture, address(Model::Sol));
    assert!(matches!(
        current(&fixture, address(Model::Sol)),
        signal_flow::Response::Current(signal_flow::Current_Data::Awake(_))
    ));
    peer.end();
    assert_eq!(
        current(&fixture, address(Model::Sol)),
        signal_flow::Response::Current(signal_flow::Current_Data::Unknown)
    );
    assert_eq!(
        fixture.nexus.store.metaflow(&address(Model::Sol)).unwrap(),
        None
    );
}

/// An ended binding whose Address holds work is not forgotten: the metaflow
/// is rested Asleep, its work still queued.
#[test]
fn an_ended_binding_holding_work_rests_asleep() {
    let fixture = NexusFixture::new();
    let peer = BoundPeer::spawn();
    peer.bind(&fixture, address(Model::Sol));
    let mut bound = fixture
        .nexus
        .store
        .metaflow(&address(Model::Sol))
        .unwrap()
        .expect("bound metaflow");
    bound.queue = flow_ethos::Queue(vec![Request::Order("Kept.".into()).unsent()]);
    fixture.nexus.store.seed_metaflow(bound);
    peer.end();
    assert_eq!(
        current(&fixture, address(Model::Sol)),
        signal_flow::Response::Current(signal_flow::Current_Data::Asleep)
    );
    let rested = fixture
        .nexus
        .store
        .metaflow(&address(Model::Sol))
        .unwrap()
        .expect("rested metaflow");
    assert_eq!(
        rested.queue,
        flow_ethos::Queue(vec![Request::Order("Kept.".into()).unsent()])
    );
}

/// The Claude flow `abc123` Flow launched at the Mind flow Secondary, its
/// launch of record kept, in pane w1:p3 of `fixture-session`.
pub(super) struct LaunchedClaude {
    pub(super) profile: LaunchProfile,
    pub(super) session: String,
}

/// The launched Claude flow abc123, Stopped: the flow a metaflow that
/// slept was last Awake as.
pub(super) fn ended_claude(fixture: &NexusFixture) -> LaunchedClaude {
    launched_claude_ended_by(fixture, |store| store.record_stopped("abc123").unwrap())
}

fn ended_simplified_claude(fixture: &NexusFixture) -> LaunchedClaude {
    launched_claude_ended_by_with(fixture, signal_flow::FlowKind::Simplified, "", |store| {
        store.record_stopped("abc123").unwrap()
    })
}

/// The launched Claude flow abc123, Simplified and still Active.
fn active_simplified_claude(fixture: &NexusFixture) -> LaunchedClaude {
    launched_claude_ended_by_with(fixture, signal_flow::FlowKind::Simplified, "", |_| true)
}

/// The launched Claude flow abc123, ended by `end`. The fixture Herdr logs
/// every call and lists no agent; a wake replaces it with the fresh world.
pub(super) fn launched_claude_ended_by(
    fixture: &NexusFixture,
    end: impl FnOnce(&crate::store::FlowStore) -> bool,
) -> LaunchedClaude {
    launched_claude_ended_by_with(
        fixture,
        signal_flow::FlowKind::Regular,
        "fixture instruction",
        end,
    )
}

fn launched_claude_ended_by_with(
    fixture: &NexusFixture,
    flow_kind: signal_flow::FlowKind,
    instruction_prompt: &str,
    end: impl FnOnce(&crate::store::FlowStore) -> bool,
) -> LaunchedClaude {
    // The launch of record opened in the session the Nexus declares.
    fixture.declare_launch_session("fixture-session");
    let profile = LaunchProfile {
        launch_request_id: signal_flow::LaunchRequestId("launched-request".into()),
        system_modules: signal_flow::SystemModules(vec![
            signal_flow::ModuleIdentity("spirit-core".into()),
            signal_flow::ModuleIdentity("intent-flow".into()),
        ]),
        user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
            "operation-main-flow".into(),
        )]),
        flow_kind: flow_kind.clone(),
        tool_set: if flow_kind == signal_flow::FlowKind::Simplified {
            signal_flow::ToolSet::Explicit(Vec::new())
        } else {
            signal_flow::ToolSet::Default
        },
        address: signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("flow".into()),
            model: signal_flow::Model::Sol,
        },
        model_name: flow_ethos::ModelName("claude-fixture".into()),
        effort: signal_flow::Effort("medium".into()),
        flow_id_option: None,
        remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        leave_outs_option: None,
        instruction_prompt: signal_flow::InstructionPrompt(instruction_prompt.into()),
    };
    // The launch that bound abc123.
    let composed = fixture
        .nexus
        .composer
        .current()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                harness_kind: HarnessKind::Claude,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .expect("composes");
    let session = fixture
        .nexus
        .herdr
        .reserved_native_session_id(&composed)
        .expect("a Claude launch has a Flow-chosen session");
    let origin = Origin::Flow(FlowOrigin {
        flow_id: flow_ethos::FlowId("caller".into()),
        session_id: signal_flow::SessionId("caller-session".into()),
    });
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, origin.clone())
        .expect("reserved");
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Claude,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: composed
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
            })
            .unwrap()
    );
    let old_pane = signal_flow::HerdrPaneBinding {
        launch_request_id: profile.launch_request_id.clone(),
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        herdr_agent_name: signal_flow::HerdrAgentName(composed.launch_agent_name()),
        herdr_workspace_id: signal_flow::HerdrWorkspaceId("w1".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-old".into()),
    };
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_binding(NativeLaunchBinding {
                launch_request_id: profile.launch_request_id.clone(),
                flow_id: flow_ethos::FlowId("abc123".into()),
                native_session_id: signal_flow::NativeSessionId(session.clone()),
                harness_kind: HarnessKind::Claude,
                herdr_pane_binding: old_pane,
            })
            .unwrap()
    );
    fixture
        .nexus
        .store
        .register_flow(FlowNode {
            flow_id: flow_ethos::FlowId("abc123".into()),
            session_id: signal_flow::SessionId(session.clone()),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                herdr_agent_name: signal_flow::HerdrAgentName(composed.launch_agent_name()),
                herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("term-old".into()),
            }),
            origin,
            flow_lifecycle: FlowLifecycle::Active,
        })
        .expect("registered");
    assert!(end(&fixture.nexus.store));
    fs::write(
        fixture.directory.path().join("flows/.abc123.flow-id"),
        format!(
            "version=1\nharness=claude\nidentity={}\nalias=abc123\nuuid-version=uuid-v5\n",
            session.replace('-', "")
        ),
    )
    .unwrap();
    let calls = fixture.directory.path().join("herdr-calls.log");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&format!(
        "#!/bin/sh\n{sessions}\nprintf '%s\\n' \"$*\" >> '{calls}'\ncase \"$*\" in\n  *\"api snapshot\"*) printf '%s\\n' '{{\"id\":\"cli:api:snapshot\",\"result\":{{\"snapshot\":{{\"agents\":[],\"protocol\":20,\"version\":\"0.8.2\"}},\"type\":\"session_snapshot\"}}}}' ;;\n  *\"pane close\"*) printf '%s\\n' '{{\"result\":{{}}}}' ;;\n  *) exit 64 ;;\nesac\n",
        sessions = fixture.session_listing(),
        calls = calls.display(),
    ));
    // The flow's own transcript, its last turn ended: it stays as it was.
    let transcript = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary")
        .join(format!("{session}.jsonl"));
    fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    fs::write(
        transcript,
        format!(
            "{}\n",
            serde_json::json!({
                "type": "assistant",
                "sessionId": session,
                "uuid": "fixture-end-turn",
                "message": { "stop_reason": "end_turn" }
            })
        ),
    )
    .unwrap();
    LaunchedClaude { profile, session }
}

/// The fresh world a wake of the Mind flow Secondary runs in, with its
/// model's Claude model configured and FlowIds claimed as `flow-id claude`
/// claims them.
pub(super) trait WakesFreshClaude {
    fn fresh_claude_world(&mut self) -> FreshWorld;
}

impl WakesFreshClaude for NexusFixture {
    fn fresh_claude_world(&mut self) -> FreshWorld {
        self.claims_flow_ids();
        configure_secondary_model(self, "claude-fixture");
        self.fresh_world("unused")
    }
}

/// What the fresh world saw.
pub(super) trait ReadsFreshWorld {
    /// Every Herdr call, one per line.
    fn calls(&self) -> String;
    /// The Claude harness starts, each on a session Flow chose.
    fn claude_starts(&self) -> usize;
    /// The native session of the last harness started.
    fn session(&self) -> String;
    /// The text of every user turn the last started Claude session's
    /// transcript holds, oldest first.
    fn user_turns(&self, fixture: &NexusFixture) -> Vec<String>;
}

impl ReadsFreshWorld for FreshWorld {
    fn calls(&self) -> String {
        fs::read_to_string(self.state.join("calls")).unwrap_or_default()
    }

    fn claude_starts(&self) -> usize {
        self.calls()
            .lines()
            .filter(|line| line.contains("agent start") && line.contains("--session-id"))
            .count()
    }

    fn session(&self) -> String {
        fs::read_to_string(self.state.join("session")).unwrap_or_default()
    }

    fn user_turns(&self, fixture: &NexusFixture) -> Vec<String> {
        fs::read_to_string(
            fixture
                .directory
                .path()
                .join("native-transcripts/claude/-home-li-primary")
                .join(format!("{}.jsonl", self.session())),
        )
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|row| row["type"] == "user")
        .filter_map(|row| {
            row.pointer("/message/content")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
    }
}

/// A queued Order wakes a fresh Claude flow, never the session that slept:
/// a new FlowId on a new native session Flow chose, its first prompt
/// carrying the fixed oldest-first queue batch. What arrives while the wake
/// holds the metaflow is handed to the woken flow after,
/// and the flow that slept stays Stopped on its own session, in Past.
#[test]
fn a_queued_order_wakes_fresh_then_drains_the_whole_queue() {
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    let queue = vec![
        Request::Order("A".into()),
        Request::Notice("B".into()),
        Request::Question("C".into()),
    ];
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    let world = fixture.fresh_claude_world();
    let lease = fixture.nexus.waking.hold(&at).expect("the wake holds it");
    assert!(
        fixture.nexus.waking.hold(&at).is_none(),
        "a second request starts no second wake"
    );

    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!(
            "the wake starts a fresh flow; Herdr calls: {}",
            world.calls()
        );
    };
    assert_ne!(flow_id.0, "abc123", "a new flow, never the one that slept");
    // D arrives while the wake holds the metaflow.
    let mut woken = stored(&fixture, Model::Sol);
    assert!(
        woken.queue.0.is_empty(),
        "the startup batch left on acceptance"
    );
    woken.queue.0.push(Request::Order("D".into()).unsent());
    fixture.nexus.store.seed_metaflow(woken);
    fixture.nexus.drain_woken(&at, lease);
    assert!(!fixture.nexus.waking.is_waking(&at), "the hold is released");

    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let new_session = world.session();
    assert_ne!(new_session, seat.session, "a new native session");
    assert!(
        world
            .calls()
            .contains(&format!("--session-id {new_session}")),
        "{}",
        world.calls()
    );
    assert!(!world.calls().contains(&seat.session), "{}", world.calls());
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_id.clone()));
    assert_eq!(
        woken.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    assert!(
        woken.queue.0.is_empty(),
        "the later arrival is delivered immediately"
    );
    let turns = world.user_turns(&fixture);
    assert!(
        turns.first().is_some_and(|turn| turn.contains("Order.{")
            && turn.contains(" A }")
            && turn.contains("Notice.{")
            && turn.contains(" B }")
            && turn.contains("Question.{")
            && turn.contains(" C }")),
        "the fixed batch is the first turn: {turns:?}"
    );
    assert_eq!(
        turns.len(),
        2,
        "the later arrival is a second prompt: {turns:?}"
    );
    assert!(
        turns[1].contains("Order.{") && turns[1].contains(" D }"),
        "the later prompt carries D: {turns:?}"
    );
    let node = fixture.nexus.store.flow_node(&flow_id.0).unwrap().unwrap();
    assert_eq!(node.session_id.0, new_session);
    assert_eq!(node.harness_kind, HarnessKind::Claude);
    let old = fixture.nexus.store.flow_node("abc123").unwrap().unwrap();
    assert_eq!(old.session_id.0, seat.session);
    assert_eq!(old.flow_lifecycle, FlowLifecycle::Stopped);
}

/// A lease is a capability of its owning waking registry, not merely an
/// address-shaped token that another registry may reuse.
#[test]
fn waking_lease_requires_its_exact_registry_owner() {
    let first = crate::waking::WakingMetaflows::default();
    let second = crate::waking::WakingMetaflows::default();
    let at = address(Model::Sol);
    let lease = first.hold(&at).expect("first registry owns the address");
    assert!(lease.owned_by(&first));
    assert!(!lease.owned_by(&second));
}

#[test]
fn a_simplified_wake_sends_the_first_abrupt_request_without_an_empty_turn() {
    let mut fixture = NexusFixture::new();
    ended_simplified_claude(&fixture);
    let at = address(Model::Sol);
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Opus,
    };
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: sender.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let signal_flow::PrivateSendResponse::SendAccepted(input) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &at,
            SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Wake,
            Message::MiddleAbrupt(Request::Order("judge this".into())),
            None,
        )
        .unwrap()
    else {
        panic!("the waking request is accepted")
    };
    let world = fixture.fresh_claude_world();

    fixture.nexus.wake_due(at.clone());

    // The woken OnDemand seat answers its one request and, with nothing
    // queued, sleeps at its rest: its flow is the metaflow's last.
    let woken = stored(&fixture, Model::Sol);
    let flow_id = match &woken.state {
        State::Awake(flow_id) => flow_id.clone(),
        State::Asleep => match woken.past.0.last() {
            Some(flow_id) if flow_id.0 != "abc123" => flow_id.clone(),
            _ => panic!("simplified flow was woken: {woken:?}; {}", world.calls()),
        },
    };
    assert!(woken.queue.0.is_empty(), "the first request was submitted");
    let turns = world.user_turns(&fixture);
    assert_eq!(turns.len(), 1, "no synthetic empty startup turn: {turns:?}");
    assert!(
        turns[0].contains("judge this"),
        "actual request is the first turn: {turns:?}"
    );
    let crate::store::logical_delivery::SimplifiedReplyStanding::Received(binding, receipt) =
        fixture
            .nexus
            .store
            .simplified_reply_receipt(&flow_id.0)
            .unwrap()
    else {
        panic!("the exact first input receipt binds its sender")
    };
    assert_eq!(binding.input_delivery_id, input.logical_delivery_id.0);
    assert_eq!(binding.recipient, sender);
    assert_eq!(
        receipt.native_session_id,
        signal_flow::NativeSessionId(world.session())
    );
}

/// A Simplified startup has one reply binding, so its fixed first-prompt
/// batch is its oldest request. Later work stays ordered for its next turn.
#[test]
fn a_simplified_startup_leaves_its_second_request_ordered() {
    let mut fixture = NexusFixture::new();
    ended_simplified_claude(&fixture);
    let at = address(Model::Sol);
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Opus,
    };
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: sender.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let accept = |body: &str| {
        let signal_flow::PrivateSendResponse::SendAccepted(accepted) = fixture
            .nexus
            .store
            .accept_logical_delivery(
                &at,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Wake,
                Message::MiddleAbrupt(Request::Order(body.into())),
                None,
            )
            .unwrap()
        else {
            panic!("the request is accepted")
        };
        accepted
    };
    let first = accept("judge the first");
    let second = accept("judge the second");
    let world = fixture.fresh_claude_world();

    fixture.nexus.wake_due(at.clone());

    let seat = stored(&fixture, Model::Sol);
    let State::Awake(flow_id) = seat.state else {
        panic!("the simplified seat is awake; {}", world.calls())
    };
    assert_eq!(
        seat.queue
            .0
            .iter()
            .map(|entry| entry.logical_delivery_id.0.as_str())
            .collect::<Vec<_>>(),
        vec![second.logical_delivery_id.0.as_str()],
        "only the request with the startup reply binding is submitted"
    );
    let turns = world.user_turns(&fixture);
    assert_eq!(turns.len(), 1, "one first turn: {turns:?}");
    assert!(turns[0].contains("judge the first"), "{turns:?}");
    assert!(!turns[0].contains("judge the second"), "{turns:?}");
    let crate::store::logical_delivery::SimplifiedReplyStanding::Received(binding, _) = fixture
        .nexus
        .store
        .simplified_reply_receipt(&flow_id.0)
        .unwrap()
    else {
        panic!("the first input alone owns the reply binding")
    };
    assert_eq!(binding.input_delivery_id, first.logical_delivery_id.0);
}

/// A Simplified flow already owes one reply. Further ordinary Sends are
/// durably admitted in order and answered with their original acceptance;
/// waiting for that native turn is not a failed delivery attempt.
#[test]
fn a_simplified_reply_wait_returns_each_durable_acceptance_without_native_input() {
    use crate::sending::SendsRequests;
    use crate::store::logical_delivery::SimplifiedReplyBinding;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{DeliveryState, Response, SendRequest};

    let fixture = NexusFixture::new();
    super::bind_existing_caller(&fixture, "a001", "pane-1");
    let launched = active_simplified_claude(&fixture);
    let at = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: at.clone(),
        state: State::Awake(flow_ethos::FlowId("abc123".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let mut recipient_agent = fixture.settled_agent();
    recipient_agent["agent"] = serde_json::Value::String("claude".into());
    recipient_agent["agent_session"] = serde_json::json!({
        "source":"herdr:claude", "agent":"claude", "kind":"id",
        "value": launched.session
    });
    fixture.accept_pane_operations(
        vec![
            super::codex_agent_in("pane-1", "terminal-pane-1"),
            recipient_agent,
        ],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    fixture
        .nexus
        .store
        .bind_simplified_reply(SimplifiedReplyBinding {
            flow_id: "abc123".into(),
            input_delivery_id: "already-in-turn".into(),
            source: at.clone(),
            recipient: Address {
                aspect: Aspect::Mind,
                topic: flow_ethos::Topic("core".into()),
                model: Model::Sol,
            },
            completed: false,
        })
        .unwrap();
    let caller = super::MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let send = |body: &str| {
        fixture.nexus.send_request(
            SendRequest {
                address: at.clone(),
                sendable_message: super::sendable_message(Message::Soft(Request::Order(
                    body.into(),
                ))),
            },
            Some(&peer),
        )
    };

    let first = send("first waits");
    let second = send("second waits behind it");
    let accepted = [first, second].map(|answer| {
        assert!(matches!(answer.response, Response::SendAccepted(_)));
        assert!(answer.pending.is_some(), "waiting remains scheduled");
        let signal_flow::PrivateSendResponse::SendAccepted(acceptance) = answer.private_response
        else {
            panic!("waiting is answered with its durable acceptance")
        };
        let snapshot = fixture
            .nexus
            .store
            .logical_delivery_snapshot(&acceptance.logical_delivery_id)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.delivery_cursor, acceptance.delivery_cursor);
        assert_eq!(snapshot.delivery_state, DeliveryState::Queued);
        acceptance.logical_delivery_id
    });
    assert_ne!(accepted[0], accepted[1]);
    let queued = fixture.nexus.store.metaflow(&at).unwrap().unwrap().queue;
    assert_eq!(
        queued
            .0
            .iter()
            .map(|entry| entry.logical_delivery_id.0.as_str())
            .collect::<Vec<_>>(),
        accepted.iter().map(|id| id.0.as_str()).collect::<Vec<_>>()
    );
    assert!(
        fixture.typed().unwrap_or_default().is_empty(),
        "waiting before a native attempt types nothing"
    );
}

/// An address lease never holds admission or another recipient: the held
/// recipient answers promptly with its acceptance while the other dispatches.
#[test]
fn a_held_address_accepts_while_an_independent_recipient_dispatches() {
    use crate::sending::SendsRequests;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{Response, SendRequest};

    let fixture = NexusFixture::new();
    super::bind_existing_caller(&fixture, "a001", "pane-1");
    let held = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: held.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let mut independent = address(Model::Sol);
    independent.topic = flow_ethos::Topic("independent".into());
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: independent.clone(),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture.accept_pane_operations(
        vec![
            super::codex_agent_in("pane-1", "terminal-pane-1"),
            claude_agent(&fixture),
        ],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    let caller = super::MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let lease = fixture.nexus.waking.hold(&held).expect("address held");
    let first = fixture.nexus.send_request(
        SendRequest {
            address: held.clone(),
            sendable_message: super::sendable_message(Message::MiddleAbrupt(Request::Order(
                "held admission".into(),
            ))),
        },
        Some(&peer),
    );
    assert!(
        matches!(first.response, Response::SendAccepted(_)),
        "held: {:?}",
        first.response
    );
    assert!(first.pending.is_some());
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
    fixture
        .nexus
        .store
        .accept_logical_delivery(
            &independent,
            SentBy::Sender(address(Model::Luna)),
            Delivery::Wake,
            Message::Soft(Request::Order("independent dispatch".into())),
            None,
        )
        .unwrap();
    assert!(matches!(
        fixture.nexus.perform(Operation::Drain(independent)),
        Outcome::Drained(_)
    ));
    assert!(
        fixture
            .typed()
            .unwrap_or_default()
            .contains("independent dispatch"),
        "the other address dispatches while the first lease remains held"
    );
    drop(lease);
}

#[test]
fn a_wake_that_cannot_start_puts_the_work_back_and_stays_asleep() {
    let fixture = NexusFixture::new();
    let at = address(Model::Sol);
    let queue = vec![Request::Order("A".into())];
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec![], queue.clone()));
    assert!(matches!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(_)
    ));
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.queue, flow_ethos::Queue(queue.clone().unsent()));
}

/// A wake the last Nexus did not finish left its work queued: the next
/// Nexus wakes the seat at its start, once, as a fresh flow, and the work
/// remains queued until native history receipts it. A
/// metaflow holding only a Result is not due; a Primary holding an Order is
/// due as any model is, and with no launch of record to compose from it
/// keeps its work queued.
#[test]
fn a_nexus_start_wakes_a_seat_whose_wake_did_not_finish() {
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    let queue = vec![Request::Notice("B".into()), Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    // Only a Result is queued for this one: nothing wakes it.
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Luna,
        State::Asleep,
        vec![],
        vec![Request::Result(flow_ethos::ResultPayload::Historical(
            "r".into(),
        ))],
    ));
    // An Order queued for a Primary no launch names.
    let primary = metaflow(
        Model::Astra,
        State::Asleep,
        vec![],
        vec![Request::Order("p".into())],
    );
    assert!(fixture.nexus.wake_is_due(&primary));
    fixture.nexus.store.seed_metaflow(primary);
    let world = fixture.fresh_claude_world();

    fixture.nexus.recover_wakes();
    // A second pass, as a second start would make, finds nothing due.
    fixture.nexus.recover_wakes();

    let woken = stored(&fixture, Model::Sol);
    let State::Awake(flow_id) = &woken.state else {
        panic!("the seat wakes: {woken:?}; Herdr calls: {}", world.calls());
    };
    assert_ne!(flow_id.0, "abc123");
    assert!(
        woken.queue.0.is_empty(),
        "the recovered wake's batch was accepted"
    );
    assert_eq!(world.claude_starts(), 1, "woken once: {}", world.calls());
    assert_eq!(stored(&fixture, Model::Luna).state, State::Asleep);
    let primary = stored(&fixture, Model::Astra);
    assert_eq!(primary.state, State::Asleep);
    assert_eq!(
        primary.queue,
        flow_ethos::Queue(vec![Request::Order("p".into())].unsent())
    );
}

/// A wake is refused while a pane already runs the sleeping flow's native
/// session; the seat stays Asleep with its work queued.
#[test]
fn a_wake_is_refused_while_a_pane_already_runs_the_session() {
    let fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    let running = serde_json::json!({
        "agent": "claude",
        "agent_status": "idle",
        "name": "by-hand",
        "pane_id": "w4:p2",
        "terminal_id": "term-by-hand",
        "workspace_id": "w4",
        "agent_session": {"source": "herdr:claude", "agent": "claude", "kind": "id", "value": seat.session}
    });
    fixture.set_agents(vec![running]);

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(crate::generated::operation::Failed_Data::ConflictingBinding)
    );
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.queue, flow_ethos::Queue(queue.clone().unsent()));
}

/// A Codex flow left in its recorded pane by a remote-resumed TUI: Herdr
/// recognizes the pane's Codex agent and reports no agent session.
fn codex_left_running(fixture: &NexusFixture) {
    let mut node = fixture.node();
    node.flow_lifecycle = FlowLifecycle::Active;
    fixture
        .nexus
        .store
        .register_flow(node)
        .expect("Codex seat registered");
    fixture
        .nexus
        .store
        .record_stopped("908786")
        .expect("Codex seat recorded Stopped");
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["908786"],
        vec![Request::Order("A".into())],
    ));
}

/// The wake check is Codex-aware: the pane Flow recorded for a Codex flow,
/// with Herdr recognizing a Codex agent in it and no session reported,
/// runs the flow's opened thread, so the wake is refused and the seat stays
/// Asleep with its work queued.
#[test]
fn a_wake_is_refused_while_a_remote_resumed_codex_tui_runs_the_thread() {
    let fixture = NexusFixture::new();
    codex_left_running(&fixture);
    fixture.set_agents(vec![fixture.settled_agent()]);
    assert_eq!(
        fixture.nexus.perform(Operation::Wake(address(Model::Sol))),
        Outcome::Failed(crate::generated::operation::Failed_Data::ConflictingBinding)
    );
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(
        asleep.queue,
        flow_ethos::Queue(vec![Request::Order("A".into())].unsent())
    );
}

/// A session Herdr does report for the recorded pane must be the flow's
/// thread: another thread there does not run the flow, so the wake is not
/// refused as a live session (it then has no launch of record to place).
#[test]
fn a_recorded_codex_pane_reporting_another_thread_does_not_hold_the_flow() {
    let fixture = NexusFixture::new();
    codex_left_running(&fixture);
    let mut other = fixture.settled_agent();
    other["agent_session"] = serde_json::json!({
        "source":"herdr:codex","agent":"codex","kind":"id",
        "value":"01a0b22c-0000-7000-8000-000000000000"
    });
    fixture.set_agents(vec![other]);
    assert_eq!(
        fixture.nexus.perform(Operation::Wake(address(Model::Sol))),
        Outcome::Failed(crate::generated::operation::Failed_Data::HerdrRefused)
    );
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
}

/// The pane check itself: a Codex flow is held by its recorded pane's
/// Codex agent reporting no session or its own thread, and by any pane
/// reporting its thread; not by its recorded pane reporting another
/// thread, by a Codex agent without a session in another pane, by its
/// recorded pane in another Herdr session, or, for a Claude flow, by a
/// pane reporting no session.
#[test]
fn the_wake_pane_check_binds_a_codex_flow_by_pane_agent_and_thread() {
    use crate::herdr::ReadsHerdrPanes;
    let fixture = NexusFixture::new();
    let node = fixture.node();
    let held = |agents: Vec<serde_json::Value>, node: &FlowNode, session: &str| {
        fixture.set_agents(agents);
        fixture.nexus.herdr.holds_native_session(session, node)
    };
    let quiet = quiet_agent(&fixture);
    let reporting = |value: &str| {
        let mut agent = fixture.settled_agent();
        agent["agent_session"] = serde_json::json!({
            "source":"herdr:codex","agent":"codex","kind":"id","value":value
        });
        agent
    };
    let mut elsewhere = quiet_agent(&fixture);
    elsewhere["pane_id"] = serde_json::Value::String("w1:p9".into());
    elsewhere["terminal_id"] = serde_json::Value::String("term-elsewhere".into());
    let mut elsewhere_reporting = reporting(&node.session_id.0);
    elsewhere_reporting["pane_id"] = serde_json::Value::String("w1:p9".into());
    elsewhere_reporting["terminal_id"] = serde_json::Value::String("term-elsewhere".into());

    assert_eq!(
        held(vec![quiet.clone()], &node, "messaging-build"),
        Some(true)
    );
    assert_eq!(
        held(
            vec![reporting(&node.session_id.0)],
            &node,
            "messaging-build"
        ),
        Some(true)
    );
    assert_eq!(
        held(vec![elsewhere_reporting], &node, "messaging-build"),
        Some(true)
    );
    assert_eq!(
        held(
            vec![reporting("01a0b22c-0000-7000-8000-000000000000")],
            &node,
            "messaging-build"
        ),
        Some(false)
    );
    assert_eq!(held(vec![elsewhere], &node, "messaging-build"), Some(false));
    assert_eq!(
        held(vec![quiet.clone()], &node, "other-session"),
        Some(false)
    );
    let mut claude = node.clone();
    claude.harness_kind = HarnessKind::Claude;
    let mut claude_agent = quiet;
    claude_agent["agent"] = serde_json::Value::String("claude".into());
    assert_eq!(
        held(vec![claude_agent], &claude, "messaging-build"),
        Some(false)
    );
}

/// A launch takes its Address by Bind's rules: an Address Awake as a live
/// flow is refused unless that flow is the predecessor the launch replaces;
/// one Awake as a flow no longer live, or Asleep, is taken, a displaced
/// flow joining Past. A flow that takes an Address is never also in its
/// Past.
#[test]
fn a_launch_takes_its_address_by_binds_rules() {
    use crate::store::metaflow::{AddressTaking, TakesLaunchAddress};
    use signal_flow::StartRejection;
    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let at = address(Model::Luna);
    let taken = |predecessor: Option<&str>, flow_id: &str| match store
        .address_taking(&at, flow_id, predecessor, 0)
        .unwrap()
    {
        AddressTaking::Takes(_) => Ok(()),
        AddressTaking::Refused(rejection) => Err(rejection),
    };

    // No metaflow yet: taken, and recorded only by the write.
    assert_eq!(taken(None, "aaaaaa"), Ok(()));
    assert!(store.awaken_metaflow(&at, "aaaaaa", 0).unwrap());
    assert_eq!(
        stored(&fixture, Model::Luna).state,
        State::Awake(flow_ethos::FlowId("aaaaaa".into()))
    );

    // Awake as a flow with no live row: taken over, the old flow Past.
    assert!(store.awaken_metaflow(&at, "bbbbbb", 0).unwrap());
    let over = stored(&fixture, Model::Luna);
    assert_eq!(
        over.state,
        State::Awake(flow_ethos::FlowId("bbbbbb".into()))
    );
    assert_eq!(
        over.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("aaaaaa".to_owned())])
    );

    // Awake as a live flow: refused, but not to its successor.
    store
        .register_flow(fixture.node())
        .expect("live flow registered");
    store.seed_metaflow(metaflow(
        Model::Luna,
        State::Awake(flow_ethos::FlowId("908786".into())),
        vec![],
        vec![],
    ));
    assert_eq!(taken(None, "cccccc"), Err(StartRejection::AddressAwake));
    assert!(!store.awaken_metaflow(&at, "cccccc", 0).unwrap());
    assert_eq!(taken(Some("908786"), "cccccc"), Ok(()));
    assert_eq!(
        taken(None, "908786"),
        Ok(()),
        "a flow keeps its own Address"
    );

    // Asleep: taken; a flow taking it is never also in its Past.
    store.seed_metaflow(metaflow(
        Model::Luna,
        State::Asleep,
        vec!["dddddd"],
        vec![Request::Order("A".into())],
    ));
    assert!(!store.awaken_metaflow(&at, "dddddd", 1).unwrap());
    assert!(store.awaken_metaflow(&at, "dddddd", 0).unwrap());
    let taken_by = stored(&fixture, Model::Luna);
    assert_eq!(
        taken_by.state,
        State::Awake(flow_ethos::FlowId("dddddd".into()))
    );
    assert!(taken_by.past.0.is_empty());
    assert_eq!(
        taken_by.queue,
        flow_ethos::Queue(vec![Request::Order("A".into())].unsent())
    );
}

/// A seat Flow launched sends through Message: Message names the sending
/// process, which runs in the launched seat's pane, and Flow identifies it
/// as the Address its launch took. Its Order to an Asleep seat is queued
/// and wakes that seat, which is handed the Order.
/// Message, running as its own process and an ancestor of no seat, bound
/// at Field message Primary as the configured Message Nexus binary.
pub(super) fn message_peer(fixture: &NexusFixture) -> (super::MarkedProcess, signal_flow::Process) {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{Bind_Data, Response};
    // Message runs as its own process, an ancestor of no seat.
    let message_child = super::MarkedProcess::spawn("sleep", &["30"], "w0:p0");
    let message_executable = fs::canonicalize(format!("/proc/{}/exe", message_child.0.id()))
        .unwrap()
        .to_string_lossy()
        .into_owned();
    use crate::store::SeedsConfiguration;
    let settings = crate::store::DefaultConfiguration {
        home: fixture.directory.path().join("home"),
        runtime_directory: fixture.directory.path().join("runtime"),
    }
    .configuration();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            meta_signal_flow::ConfigureNexus {
                source_root: flow_ethos::SourceRoot(
                    fixture.directory.path().to_string_lossy().into_owned(),
                ),
                first_codex_endpoint: settings.stable_codex,
                second_codex_endpoint: settings.next_codex,
                harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                    settings.message_nexus_path.clone(),
                )),
                message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                    message_executable,
                )),
                curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                    "/fixture/curriculum.sock".into(),
                ),
                lease: flow_ethos::Lease(30),
                presentation_prompt_option: None,
                herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
            },
        ))
        .expect("Nexus configured");
    let message = FlowStore::process_identity_for_pid(message_child.0.id()).unwrap();
    let message_address = Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("message".into()),
        model: Model::Astra,
    };
    assert!(matches!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Bind(Bind_Data {
                    address: message_address,
                    process: message.clone(),
                }),
                Some(&message),
            )
            .response,
        Response::Bound(_)
    ));
    (message_child, message)
}

#[test]
fn a_launched_seat_identifies_through_its_pane_and_its_order_wakes_an_asleep_seat() {
    identifies_and_wakes_by_an_order(State::Asleep);
}

/// A metaflow an older Nexus left Awake as its Stopped flow is read as
/// Asleep: the Order queued for it is recorded against an Asleep metaflow
/// and wakes it, as a fresh flow.
#[test]
fn a_metaflow_left_awake_as_an_ended_flow_is_woken_by_an_order() {
    identifies_and_wakes_by_an_order(State::Awake(flow_ethos::FlowId("abc123".into())));
}

fn identifies_and_wakes_by_an_order(recipient: State) {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::sending::MeasuresSendPayload;
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::TakesLaunchAddress;
    use crate::store::send_outcome::RecordsSendOutcomes;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{Deliver_Data, Lock_Data, Response};

    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    let (_message_child, message) = message_peer(&fixture);

    // The sending seat: a flow Flow launched into pane w5:p1, its launch
    // having taken the Address Field flow Quaternary.
    let sender_address = Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Terra,
    };
    let sender_route = HerdrRoute {
        herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
        herdr_agent_name: signal_flow::HerdrAgentName("sender-agent".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("w5:p1".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-sender".into()),
    };
    fixture
        .nexus
        .store
        .register_flow_in_role(
            FlowNode {
                flow_id: flow_ethos::FlowId("5e4de7".into()),
                session_id: signal_flow::SessionId("5e4de700-0000-4000-8000-000000000001".into()),
                harness_kind: HarnessKind::Claude,
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Available(sender_route),
                origin: Origin::Flow(FlowOrigin {
                    flow_id: flow_ethos::FlowId("da2038".into()),
                    session_id: signal_flow::SessionId("field-session".into()),
                }),
                flow_lifecycle: FlowLifecycle::Active,
            },
            signal_flow::CallerIdentity {
                flow_id: flow_ethos::FlowId("5e4de7".into()),
                aspect: Aspect::Field,
                model_name: flow_ethos::ModelName("haiku".into()),
            },
        )
        .expect("sender registered");
    assert!(matches!(
        fixture
            .nexus
            .store
            .address_taking(&sender_address, "5e4de7", None, 0)
            .unwrap(),
        crate::store::metaflow::AddressTaking::Takes(_)
    ));
    assert!(
        fixture
            .nexus
            .store
            .awaken_metaflow(&sender_address, "5e4de7", 0)
            .unwrap()
    );
    // Herdr shows the sender's pane.
    let script = fs::read_to_string(&fixture.snapshot_program).unwrap();
    let sender_agent = r#"{"agent":"claude","agent_status":"working","name":"sender-agent","pane_id":"w5:p1","terminal_id":"term-sender","workspace_id":"w5"}"#;
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replace(r#""agents":[]"#, &format!(r#""agents":[{sender_agent}]"#)));

    // The process that sends runs in the sender's pane, its pane ID alone
    // marked, as in Herdr's default session.
    let sender = super::MarkedProcess::spawn("sleep", &["30"], "w5:p1");
    let sender_process = FlowStore::process_identity_for_pid(sender.0.id()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(sender_process.clone()),
                Some(&message)
            )
            .response,
        Response::Identified(sender_address.clone())
    );
    // A process in no launched seat's pane names no one.
    let stranger = super::MarkedProcess::spawn("sleep", &["30"], "w8:p8");
    let stranger_process = FlowStore::process_identity_for_pid(stranger.0.id()).unwrap();
    assert!(matches!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(stranger_process),
                Some(&message)
            )
            .response,
        Response::Refused(signal_flow::Refusal::Unidentified(_))
    ));

    // The seat the Order is for: Asleep, or left Awake as its Stopped flow.
    let at = address(Model::Sol);
    let past = if recipient == State::Asleep {
        vec!["abc123"]
    } else {
        vec![]
    };
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, recipient, past, vec![]));
    let Response::Locked(lock) = fixture
        .nexus
        .dispatch_metaflow_peer(
            signal_flow::Query::Lock(Lock_Data {
                first_address: sender_address.clone(),
                second_address: at.clone(),
            }),
            Some(&message),
        )
        .response
    else {
        panic!("the identified sender locks the recipient")
    };
    let oversized = Request::Order("x".repeat(crate::sending::MAX_SEND_PAYLOAD_BYTES + 1));
    let expected_size = oversized.native_prompt_bytes(&sender_address);
    let refusal = Response::SendRefused(signal_flow::VisibleRefusal::Oversized(
        signal_flow::SendPayloadSize {
            actual_bytes: signal_flow::ActualBytes(expected_size as i64),
            maximum_bytes: signal_flow::MaximumBytes(crate::sending::MAX_SEND_PAYLOAD_BYTES as i64),
        },
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Deliver(Deliver_Data {
                    lock: lock.clone(),
                    deliver_key: super::deliver_key(10),
                    sendable_message: super::sendable_message(Message::Soft(oversized)),
                }),
                Some(&message),
            )
            .response,
        refusal
    );
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&at)
            .unwrap()
            .unwrap()
            .queue
            .0
            .is_empty()
    );
    assert!(
        fixture
            .nexus
            .store
            .stored_metaflow_lock(&at)
            .unwrap()
            .is_none()
    );
    let send_outcomes = fixture.nexus.store.read_send_outcomes().unwrap();
    assert!(matches!(
        send_outcomes.last().map(|record| &record.ordinary_send_outcome),
        Some(meta_signal_flow::OrdinarySendOutcome::Refused(
            signal_flow::Refusal::Oversized(signal_flow::SendPayloadSize {
                actual_bytes,
                maximum_bytes,
            })
        )) if *actual_bytes == signal_flow::ActualBytes(expected_size as i64)
            && *maximum_bytes == signal_flow::MaximumBytes(crate::sending::MAX_SEND_PAYLOAD_BYTES as i64)
    ));
    let Response::Locked(lock) = fixture
        .nexus
        .dispatch_metaflow_peer(
            signal_flow::Query::Lock(Lock_Data {
                first_address: sender_address.clone(),
                second_address: at.clone(),
            }),
            Some(&message),
        )
        .response
    else {
        panic!("the rejected request releases its verified lock")
    };
    let order = Request::Order("Run the tier tests.".into());
    let world = fixture.fresh_claude_world();
    let deliver = Deliver_Data {
        lock,
        deliver_key: super::deliver_key(11),
        sendable_message: super::sendable_message(Message::MiddleAbrupt(order.clone())),
    };
    let answer = fixture
        .nexus
        .dispatch_metaflow_peer(signal_flow::Query::Deliver(deliver.clone()), Some(&message));
    let Response::SendAccepted(visible) = &answer.response else {
        panic!("Deliver has one public admission answer")
    };
    let signal_flow::PrivateSendResponse::SendAccepted(accepted) = &answer.private_response else {
        panic!("Deliver answers its durable admission")
    };
    let accepted = accepted.clone();
    assert_eq!(visible.request_id, accepted.request_id);
    assert_eq!(visible.delivery_cursor, accepted.delivery_cursor);
    let delivered = fixture
        .nexus
        .store
        .logical_delivery_snapshot(&accepted.logical_delivery_id)
        .expect("delivery state reads")
        .expect("admitted delivery remains observable");
    assert!(matches!(
        delivered.delivery_state,
        signal_flow::DeliveryState::Delivered(_)
    ));
    assert!(delivered.delivery_cursor > accepted.delivery_cursor);
    let message_opening = fixture
        .nexus
        .store
        .read_message_delivery_snapshots(None)
        .expect("Message delivery opening reads");
    assert_eq!(message_opening.delivery_snapshot_vector.len(), 1);
    assert_eq!(
        message_opening.delivery_snapshot_vector[0].logical_delivery_id,
        accepted.logical_delivery_id
    );
    drop(answer);
    let replay = fixture
        .nexus
        .dispatch_metaflow_peer(signal_flow::Query::Deliver(deliver.clone()), Some(&message));
    let signal_flow::PrivateSendResponse::SendAccepted(replayed) = &replay.private_response else {
        panic!("a lost Deliver answer is recovered from its key alone")
    };
    assert_eq!(*replayed, accepted, "replay returns the exact admission");
    let mut changed = deliver.clone();
    changed.sendable_message =
        super::sendable_message(Message::MiddleAbrupt(Request::Order("Different".into())));
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(signal_flow::Query::Deliver(changed), Some(&message))
            .response,
        Response::DeliverKeyReused
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Acknowledge(vec![deliver.deliver_key.clone()]),
                None,
            )
            .response,
        Response::Acknowledged,
        "an Acknowledge is taken from any caller"
    );
    assert!(
        fixture
            .nexus
            .store
            .deliver_call(&deliver.deliver_key)
            .expect("Deliver key reads")
            .is_none(),
        "the Acknowledge retracted the replay row"
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Acknowledge(vec![
                    deliver.deliver_key.clone(),
                    super::deliver_key(99),
                ]),
                Some(&message),
            )
            .response,
        Response::Acknowledged
    );
    assert!(
        fixture
            .nexus
            .store
            .deliver_call(&deliver.deliver_key)
            .expect("Deliver key reads")
            .is_none(),
        "Acknowledge retracts the durable key"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .read_message_delivery_snapshots(None)
            .expect("Message delivery provenance survives Acknowledge")
            .delivery_snapshot_vector,
        message_opening.delivery_snapshot_vector
    );
    let woken = stored(&fixture, Model::Sol);
    let State::Awake(flow_id) = &woken.state else {
        panic!(
            "the Order wakes the seat: {woken:?}; Herdr calls: {}",
            world.calls()
        );
    };
    assert_ne!(flow_id.0, "abc123", "a fresh flow");
    // The woken flow's accepted first prompt carries the Order with the
    // Sender its Lock named.
    let sent = QueueEntry {
        logical_delivery_id: accepted.logical_delivery_id.clone(),
        request_id: accepted.request_id.clone(),
        sent_by: SentBy::Sender(sender_address.clone()),
        delivery: flow_ethos::Delivery::Wake,
        message: Message::MiddleAbrupt(order.clone()),
    };
    assert!(woken.queue.0.is_empty());
    let turns = world.user_turns(&fixture);
    assert!(
        turns
            .iter()
            .any(|turn| turn.contains(&[sent.clone()].prompt_text(&address(Model::Sol)))),
        "{turns:?}"
    );
    assert!(
        turns.iter().all(|turn| !turn.contains("testMessageMemory")),
        "the Message-private key never enters the recipient pane: {turns:?}"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .archive_request(&accepted.request_id.0)
            .expect("archive reads while the Deliver answer is held"),
        Err(signal_flow::ArchiveRejection::NotYet),
        "the replayed Deliver answer holds archive after Acknowledge removed its key"
    );
    drop(replay);
    assert_eq!(
        fixture
            .nexus
            .store
            .archive_request(&accepted.request_id.0)
            .expect("archive reads after the Deliver answer is released"),
        Ok(())
    );
}

#[test]
fn deliver_all_reports_empty_and_retained_queue_truthfully() {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::sending::SendsRequests;
    use crate::store::metaflow::TakesLaunchAddress;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{
        CallerIdentity, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind,
        HerdrRoute, HerdrRouteSelection, Origin, Response,
    };

    let fixture = NexusFixture::new();
    let (_message_child, message) = message_peer(&fixture);
    let sender_address = Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Terra,
    };
    let route = HerdrRoute {
        herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
        herdr_agent_name: signal_flow::HerdrAgentName("sender-agent".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("w5:p1".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-sender".into()),
    };
    fixture
        .nexus
        .store
        .register_flow_in_role(
            FlowNode {
                flow_id: flow_ethos::FlowId("deliver-all-sender".into()),
                session_id: signal_flow::SessionId("deliver-all-sender-session".into()),
                harness_kind: HarnessKind::Claude,
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Available(route),
                origin: Origin::Flow(FlowOrigin {
                    flow_id: flow_ethos::FlowId("parent".into()),
                    session_id: signal_flow::SessionId("parent-session".into()),
                }),
                flow_lifecycle: FlowLifecycle::Active,
            },
            CallerIdentity {
                flow_id: flow_ethos::FlowId("deliver-all-sender".into()),
                aspect: Aspect::Field,
                model_name: flow_ethos::ModelName("haiku".into()),
            },
        )
        .expect("sender registered");
    fixture
        .nexus
        .store
        .address_taking(&sender_address, "deliver-all-sender", None, 0)
        .unwrap();
    fixture
        .nexus
        .store
        .awaken_metaflow(&sender_address, "deliver-all-sender", 0)
        .unwrap();
    fixture.set_agents(vec![serde_json::json!({
        "agent":"claude",
        "agent_status":"working",
        "name":"sender-agent",
        "pane_id":"w5:p1",
        "terminal_id":"term-sender",
        "workspace_id":"w5"
    })]);
    let sender = super::MarkedProcess::spawn("sleep", &["30"], "w5:p1");
    let sender_process = FlowStore::process_identity_for_pid(sender.0.id()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(sender_process.clone()),
                Some(&message)
            )
            .response,
        Response::Identified(sender_address),
    );

    let empty = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec![], vec![]));
    assert_eq!(
        fixture.nexus.deliver_all(empty.clone()),
        Response::Drained(signal_flow::VisibleDrainOutcome {
            address: empty.clone(),
            visible_drain_state: signal_flow::VisibleDrainState::Delivered,
        })
    );

    // With no launch record the sleeping seat cannot be woken. DeliverAll
    // must preserve its queued work and report that it remains queued.
    let queued = address(Model::Luna);
    let request = Request::Order("retain this request".into());
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Luna,
        State::Asleep,
        vec![],
        vec![request.clone()],
    ));
    assert_eq!(
        fixture.nexus.deliver_all(queued.clone()),
        Response::Drained(signal_flow::VisibleDrainOutcome {
            address: queued.clone(),
            visible_drain_state: signal_flow::VisibleDrainState::Held,
        })
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&queued)
            .unwrap()
            .unwrap()
            .queue,
        flow_ethos::Queue(vec![request.clone().unsent()]),
    );
}

/// A process spawned for a test, killed when it is dropped.
pub(super) struct SpawnedProcess(pub(super) std::process::Child);

impl Drop for SpawnedProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The child whose pid its parent wrote at `written`, read at each change
/// in that file's directory until it is there, bounded.
fn child_written_at(written: &std::path::Path) -> signal_flow::Process {
    use crate::event_wait::AwaitsDirectory;
    use crate::store::{FlowStore, ReadsFlowMemory};
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let child = || {
        fs::read_to_string(written)
            .ok()?
            .trim()
            .parse::<u32>()
            .ok()
            .and_then(FlowStore::process_identity_for_pid)
    };
    let directory = written.parent().expect("the pid file's directory");
    assert!(
        directory.until_holds(deadline, &mut || child().is_some()),
        "the child started"
    );
    child().expect("the child started")
}

/// A Codex thread on a shared app-server sends through an MCP send server
/// the app-server runs for it: a process in no pane, whose only account of
/// its flow is the thread id Codex stamped on the call. Message names that
/// process and the thread; Flow trusts it only as the configured send
/// server under its paired app-server (here the test process itself), and
/// names the metaflow Awake as the Codex flow it recorded on that thread.
#[test]
fn a_trusted_send_server_identifies_the_awake_flow_of_its_codex_thread() {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::store::metaflow::TakesLaunchAddress;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{IdentifyThread_Data, Response};

    let fixture = NexusFixture::new();
    let (_message_child, message) = message_peer(&fixture);
    let thread = "01a0fdcc-c1a1-7f43-8c23-06d41fa34438";
    let identify = |process: &signal_flow::Process, thread: &str, peer: &signal_flow::Process| {
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::IdentifyThread(IdentifyThread_Data {
                    process: process.clone(),
                    native_session_id: signal_flow::NativeSessionId(thread.into()),
                }),
                Some(peer),
            )
            .response
    };

    // The send server: a child of the app-server, in no pane.
    let send_server = SpawnedProcess(
        std::process::Command::new("sleep")
            .arg("30")
            .env_remove("HERDR_PANE_ID")
            .env_remove("HERDR_SESSION")
            .spawn()
            .unwrap(),
    );
    let send_server_process = FlowStore::process_identity_for_pid(send_server.0.id()).unwrap();
    let send_server_executable = fs::canonicalize(format!("/proc/{}/exe", send_server.0.id()))
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let app_server_executable = fs::canonicalize(std::env::current_exe().unwrap())
        .unwrap()
        .to_string_lossy()
        .into_owned();

    // Before Flow records a flow on the thread, the thread names no one,
    // whether or not a send server is configured.
    super::assert_unidentified(
        identify(&send_server_process, thread, &message),
        &send_server_process,
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::SendServers(vec![
                meta_signal_flow::ConfigureSendServer {
                    codex_app_server_binary: meta_signal_flow::CodexAppServerBinary(
                        app_server_executable
                    ),
                    mcp_send_server_binary: meta_signal_flow::McpSendServerBinary(
                        send_server_executable
                    ),
                },
            ]))
            .unwrap(),
        meta_signal_flow::ConfigureOutcome::Replaced
    );

    // The Codex flow Flow recorded on the thread, Awake at its Address. Its
    // TUI runs in a pane; the send server runs under the app-server, in none.
    let sender_address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Luna,
    };
    let registration = fixture
        .nexus
        .store
        .register_flow_in_role(
            FlowNode {
                flow_id: flow_ethos::FlowId("41fa34".into()),
                session_id: signal_flow::SessionId(thread.into()),
                harness_kind: HarnessKind::Codex,
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                    herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                    herdr_agent_name: signal_flow::HerdrAgentName("codex-agent".into()),
                    herdr_pane_id: signal_flow::HerdrPaneId("w6:p1".into()),
                    herdr_terminal_id: signal_flow::HerdrTerminalId("term-codex".into()),
                }),
                origin: Origin::Unbound,
                flow_lifecycle: FlowLifecycle::Active,
            },
            signal_flow::CallerIdentity {
                flow_id: flow_ethos::FlowId("41fa34".into()),
                aspect: Aspect::Mind,
                model_name: flow_ethos::ModelName("gpt-6.1-luna".into()),
            },
        )
        .unwrap();
    assert!(
        matches!(registration, crate::store::FlowRegistration::Registered(_)),
        "the Codex flow is registered: {registration:?}"
    );
    assert!(matches!(
        fixture
            .nexus
            .store
            .address_taking(&sender_address, "41fa34", None, 0)
            .unwrap(),
        crate::store::metaflow::AddressTaking::Takes(_)
    ));
    assert!(
        fixture
            .nexus
            .store
            .awaken_metaflow(&sender_address, "41fa34", 0)
            .unwrap()
    );

    // The trusted send server names the flow of its thread.
    assert_eq!(
        identify(&send_server_process, thread, &message),
        Response::Identified(sender_address.clone())
    );
    // A thread Flow recorded for no flow names no one.
    super::assert_unidentified(
        identify(
            &send_server_process,
            "01a0fdcc-0000-7000-8000-000000000000",
            &message,
        ),
        &send_server_process,
    );
    super::assert_unidentified(
        identify(&send_server_process, "", &message),
        &send_server_process,
    );
    // The right thread from another executable names the same flow: the
    // thread is asked about, not the process. The shell writes its child's
    // pid once the child has started.
    let child_pid = tempfile::tempdir().unwrap();
    let child_pid_file = child_pid.path().join("child.pid");
    let shell = SpawnedProcess(
        std::process::Command::new("sh")
            .args(["-c", "sleep 30 & printf '%s' \"$!\" > \"$0\"; wait; :"])
            .arg(&child_pid_file)
            .spawn()
            .unwrap(),
    );
    let shell_process = FlowStore::process_identity_for_pid(shell.0.id()).unwrap();
    assert_eq!(
        identify(&shell_process, thread, &message),
        Response::Identified(sender_address.clone())
    );
    let foreign_child = child_written_at(&child_pid_file);
    assert_eq!(
        identify(&foreign_child, thread, &message),
        Response::Identified(sender_address.clone())
    );
    // Any caller may ask, the Message process or another.
    assert_eq!(
        identify(&send_server_process, thread, &send_server_process),
        Response::Identified(sender_address.clone())
    );
    // The send server runs in no pane: pane identification names no one.
    super::assert_unidentified(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(send_server_process.clone()),
                Some(&message),
            )
            .response,
        &send_server_process,
    );

    // Once its metaflow sleeps, the thread names no one.
    assert!(
        fixture
            .nexus
            .store
            .sleep_metaflow(&sender_address, "41fa34")
            .unwrap()
    );
    super::assert_unidentified(
        identify(&send_server_process, thread, &message),
        &send_server_process,
    );
}

/// A seat launched into the pane Message's marked processes name: its
/// binding witnessed and acknowledged, its first prompt typed, and no answer
/// of its yet recorded.
fn launched_unanswered(fixture: &NexusFixture, launch_request_id: &str) -> super::StagedLaunch {
    let mut launch = fixture.staged_launch(launch_request_id, None);
    launch.pane.herdr_session_name = signal_flow::HerdrSessionName("messaging-build".into());
    launch.pane.herdr_pane_id = signal_flow::HerdrPaneId("w5:p1".into());
    launch.pane.herdr_agent_name = signal_flow::HerdrAgentName("sender-agent".into());
    launch.pane.herdr_terminal_id = signal_flow::HerdrTerminalId("term-sender".into());
    launch.binding.herdr_pane_binding = launch.pane.clone();
    launch.stage_to_ambiguity(&fixture.nexus);
    fixture.set_agents(vec![serde_json::json!({
        "agent":"codex","agent_status":"working","name":"sender-agent",
        "pane_id":"w5:p1","terminal_id":"term-sender","workspace_id":"w5"
    })]);
    launch
}

/// The living's rule is that a seat's first turn does real work: a seat
/// whose first tool call sends, before any answer of its is recorded, is
/// already identified at its Address and its Order is queued.
#[test]
fn a_launched_seat_sends_an_order_from_its_first_tool_call_before_any_answer() {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::store::{FlowStore, ReadsFlowMemory, ReadsLaunchAttempt};
    use signal_flow::{Deliver_Data, LaunchAttemptPhase, Lock_Data, Response};

    let fixture = NexusFixture::new();
    let (_message_child, message) = message_peer(&fixture);
    let launch = launched_unanswered(&fixture, "first-turn-request");
    let sender_address = launch.profile.address.clone();
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_attempt("first-turn-request")
            .unwrap()
            .unwrap()
            .launch_attempt_phase,
        LaunchAttemptPhase::PromptAmbiguous,
        "no answer of the seat is recorded"
    );

    let first_tool_call = super::MarkedProcess::spawn("sleep", &["30"], "w5:p1");
    let process = FlowStore::process_identity_for_pid(first_tool_call.0.id()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(signal_flow::Query::Identify(process), Some(&message))
            .response,
        Response::Identified(sender_address.clone())
    );
    let at = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    let Response::Locked(lock) = fixture
        .nexus
        .dispatch_metaflow_peer(
            signal_flow::Query::Lock(Lock_Data {
                first_address: sender_address.clone(),
                second_address: at.clone(),
            }),
            Some(&message),
        )
        .response
    else {
        panic!("the identified seat locks its recipient")
    };
    let order = Request::Order("Run the tier tests.".into());
    let response = fixture.nexus.dispatch_metaflow_peer(
        signal_flow::Query::Deliver(Deliver_Data {
            lock,
            deliver_key: super::deliver_key(12),
            sendable_message: super::sendable_message(Message::Soft(order.clone())),
        }),
        Some(&message),
    );
    let signal_flow::PrivateSendResponse::SendAccepted(accepted) = response.private_response else {
        panic!("the identified sender's message is admitted")
    };
    let metaflow = stored(&fixture, Model::Sol);
    let [entry] = metaflow.queue.0.as_slice() else {
        panic!("one logical entry is retained")
    };
    assert_eq!(entry.logical_delivery_id, accepted.logical_delivery_id);
    assert_eq!(entry.sent_by, SentBy::Sender(sender_address));
    assert_eq!(entry.delivery, flow_ethos::Delivery::Queue);
    assert_eq!(entry.message, Message::Soft(order));
}

/// A launch refused before it was confirmed gives the Address back as it
/// found it, keeping what was queued meanwhile; a flow that ends unconfirmed
/// gives back an Address that had no metaflow by removing it.
#[test]
fn an_unconfirmed_launch_that_fails_or_ends_leaves_no_awake_registration() {
    use crate::launching::LaunchesFlows;
    use crate::store::address_hold::HoldsLaunchAddress;
    use signal_flow::Response;

    let fixture = NexusFixture::new();
    // An Address that was Asleep: refused, it is Asleep again.
    let mut asleep = fixture.staged_launch("asleep-request", None);
    asleep.profile.address = address(Model::Sol);
    asleep.composed.launch_profile.address = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        vec![Request::Order("A".into())],
    ));
    asleep.stage_to_ambiguity(&fixture.nexus);
    let held = stored(&fixture, Model::Sol);
    assert_eq!(
        held.state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
    assert_eq!(
        held.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    let mut queued = held.clone();
    queued.queue.0.push(Request::Notice("B".into()).unsent());
    fixture.nexus.store.seed_metaflow(queued);

    assert_eq!(
        fixture
            .nexus
            .settle("asleep-request", Response::StartRejected(naming_refused())),
        Response::StartRejected(naming_refused())
    );
    assert_eq!(
        stored(&fixture, Model::Sol),
        metaflow(
            Model::Sol,
            State::Asleep,
            vec!["abc123"],
            vec![Request::Order("A".into()), Request::Notice("B".into())],
        )
    );
    assert_eq!(
        fixture.nexus.store.metaflow_awake_as("908786").unwrap(),
        None
    );
    assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);

    // An Address with no metaflow: the flow's pane goes before any answer.
    let fixture = NexusFixture::new();
    let mut absent = fixture.staged_launch("absent-request", None);
    let at = absent.profile.address.clone();
    absent.stage_to_ambiguity(&fixture.nexus);
    assert!(
        fixture
            .nexus
            .store
            .metaflow_awake_as("908786")
            .unwrap()
            .is_some()
    );
    assert!(fixture.nexus.store.record_exited("908786").unwrap());
    assert_eq!(fixture.nexus.store.metaflow(&at).unwrap(), None);
    assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
}

/// A Herdr stand-in for the Claude seat in pane w1:p3 that shows it working
/// until an `agent wait` has run, and resting (`done`) after: Claude runs
/// its Stop hook while its spinner is still on screen. The wait first
/// waits, boundedly, for the `released` latch to open. Every call is
/// logged.
fn herdr_working_until_waited(
    fixture: &NexusFixture,
    released: &crate::fixture_executable::Latch,
) -> std::path::PathBuf {
    let log = fixture.directory.path().join("herdr-operations.log");
    let waited = fixture.directory.path().join("herdr-waited");
    let snapshot = |status: &str| {
        let mut agent = claude_agent(fixture);
        agent["agent_status"] = serde_json::Value::String(status.into());
        serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":[agent],"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        })
    };
    let body = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$3 $4\" in\n  \"api snapshot\") if [ -e '{waited}' ]; then printf '%s\\n' '{done}'; else printf '%s\\n' '{working}'; fi ;;\n  \"agent wait\") if timeout 10 sh -c ': < \"$0\"' '{released}'; then echo 'wait released by the closed hook' >> '{log}'; else echo 'wait timed out' >> '{log}'; fi; : > '{waited}' ;;\n  \"pane close\") ;;\n  \"agent read\") printf '%s\\n' '❯ ' ;;\n  *) exit 64 ;;\nesac\n",
        log = log.display(),
        waited = waited.display(),
        released = released.path.display(),
        done = snapshot("done"),
        working = snapshot("working"),
    );
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&body);
    log
}

/// The live defect: a Claude seat's Stop hook runs while Herdr still shows
/// it working, and it shows done only a moment later, with no event. The
/// Stop waits, boundedly, for the seat to rest; idle, it keeps its pane.
#[test]
fn a_seat_herdr_shows_working_at_its_stop_is_idle_once_herdr_shows_it_done() {
    let fixture = NexusFixture::new();
    use crate::fixture_executable::{HoldsLatch, Latch};
    let released = Latch::closed(fixture.directory.path().join("released"));
    released.release();
    let log = herdr_working_until_waited(&fixture, &released);
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));

    assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
    let calls = fs::read_to_string(log).unwrap();
    assert!(
        calls.contains("agent wait w1:p3 --until idle --until done --timeout"),
        "the Stop waited for the seat to rest: {calls}"
    );
    assert!(!calls.contains("pane close"), "the pane is kept: {calls}");
}

/// Every early exit names its reason: a seat still working after the wait
/// says so, as does a seat nothing holds Awake.
#[test]
fn a_stop_that_leaves_its_seat_names_why() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![{
            let mut agent = claude_agent(&fixture);
            agent["agent_status"] = serde_json::Value::String("working".into());
            agent
        }],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(KeptAtStop::NotAwake)
    );
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(KeptAtStop::NotResting(PaneAgent::Present {
            agent_state: signal_flow::AgentState::Working,
            ..
        }))
    ));
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
}

/// The hook's Report is answered and its connection closed before the
/// settling waits on Herdr: the harness waits on its hook, so a wait on
/// the harness while the hook is connected would wait on itself. Here the
/// wait cannot finish until the hook has seen its answer and the close.
#[test]
fn the_hook_has_its_answer_and_its_connection_closed_before_the_rest_wait_ends() {
    use crate::{CarriesSignalFrames, ServesOrdinary};
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    use crate::fixture_executable::{HoldsLatch, Latch};
    let released = Latch::closed(fixture.directory.path().join("hook-closed"));
    let log = herdr_working_until_waited(fixture, &released);
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    let socket = fixture.directory.path().join("ordinary.sock");
    let served = socket.clone();
    std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
    // The name is moved in only once the socket listens.
    assert!(
        crate::event_wait::AwaitsPath::appeared_by(
            socket.as_path(),
            Instant::now() + Duration::from_secs(2)
        ),
        "ordinary socket listens"
    );
    let mut hook = UnixStream::connect(&socket).expect("ordinary socket listens");
    hook.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    super::GreetsOrdinarySocket::greet_ordinary(&mut hook);
    let marker = "SOCKET-HOOK-assistant-marker-雪";
    let mut event = claude_stop(fixture);
    let signal_flow::Event::Stopped(stop) = &mut event else {
        unreachable!()
    };
    stop.last_assistant_message_option =
        signal_flow::LastAssistantMessageOption(Some(marker.repeat(4096)));
    hook.write_query(&signal_flow::Query::Report(signal_flow::Report_Data {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        process_option: None,
        event,
    }))
    .unwrap();
    assert_eq!(
        hook.read_response().expect("the hook has its answer"),
        signal_flow::Response::Reported
    );
    let mut rest = Vec::new();
    assert_eq!(
        hook.read_to_end(&mut rest)
            .expect("the connection closes while the wait still runs"),
        0
    );
    use datom_codec::Datomizable;
    use protos::{Compactable, Protosizable};
    let metadata = fixture
        .nexus
        .read_events(flow_ethos::FlowId(CLAUDE_SEAT.into()))
        .datomize(vec![])
        .protosize()
        .compact();
    assert!(
        !metadata.contains(marker),
        "real Report socket stores and serves metadata alone"
    );
    released.release();

    let deadline = Instant::now() + Duration::from_secs(20);
    let calls = || fs::read_to_string(&log).unwrap_or_default();
    let directory = log.parent().expect("the log's directory");
    assert!(
        crate::event_wait::AwaitsDirectory::until_holds(directory, deadline, &mut || {
            let calls = calls();
            calls.contains("wait released") || calls.contains("wait timed out")
        }),
        "the rest wait never ended"
    );
    let calls = calls();
    assert!(
        calls.contains("wait released by the closed hook"),
        "{calls}"
    );
}

/// A topic never dies: the Stop of the flow its metaflow is Awake as
/// records the metaflow Asleep in the same write, the flow in Past, so a
/// wake composes a fresh flow from its launch and a fresh Start may take
/// the Address.
#[test]
fn the_stop_of_a_confirmed_seat_leaves_its_metaflow_asleep_to_wake_fresh() {
    use crate::store::metaflow::TakesLaunchAddress;
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    let launched = ended_claude(&fixture);
    let slept = stored(&fixture, Model::Sol);
    assert_eq!(slept.state, State::Asleep);
    assert_eq!(
        slept.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    assert_eq!(fixture.nexus.wake_plan(&slept), last_launch_context());
    let mut fresh = launched.profile.clone();
    fresh.launch_request_id = signal_flow::LaunchRequestId("fresh-after-stop".into());
    assert_eq!(
        fixture.nexus.store.launch_address_refusal(&fresh).unwrap(),
        None,
        "a fresh Start may take the Address"
    );
}

/// A Retired flow joins Past as provenance, its metaflow Asleep: the queued
/// Order wakes the seat fresh, as every wake is.
#[test]
fn a_retired_seat_sleeps_its_metaflow_and_its_order_wakes_it_fresh() {
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    launched_claude_ended_by(&fixture, |store| {
        store.record_active("abc123").unwrap() && store.record_retired("abc123").unwrap()
    });
    let mut retired = stored(&fixture, Model::Sol);
    assert_eq!(retired.state, State::Asleep);
    assert_eq!(
        retired.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    retired
        .queue
        .0
        .push(Request::Order("Carry on.".into()).unsent());
    fixture.nexus.store.seed_metaflow(retired.clone());
    assert!(fixture.nexus.wake_is_due(&retired));
    assert_eq!(fixture.nexus.wake_plan(&retired), last_launch_context());
}

/// At the Nexus's start every metaflow left Awake as an ended flow is
/// recorded Asleep; one Awake as a flow Flow holds no row for, as Bind
/// names, is left as it is.
#[test]
fn a_nexus_start_puts_every_metaflow_left_awake_as_an_ended_flow_to_sleep() {
    let fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Luna,
        State::Awake(flow_ethos::FlowId("flow-0000000000000001".into())),
        vec![],
        vec![],
    ));
    fixture.nexus.recover_wakes();
    let rested = stored(&fixture, Model::Sol);
    assert_eq!(rested.state, State::Asleep);
    assert_eq!(
        rested.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    assert_eq!(
        stored(&fixture, Model::Luna).state,
        State::Awake(flow_ethos::FlowId("flow-0000000000000001".into()))
    );
}

fn naming_refused() -> signal_flow::StartRejection {
    signal_flow::StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
        native_launch_stage: signal_flow::NativeLaunchStage::Naming,
        native_launch_failure: signal_flow::NativeLaunchFailure::NotReady,
    })
}

/// A whole fresh launch run by a wake, against fakes of everything outside
/// the Nexus: a Herdr that opens one pane (w7:p1), starts the harness the
/// Nexus asks for in it and reports it as Herdr 0.8.2 does (a Claude pane
/// with its session, a remote-resumed Codex pane with none), labels it and
/// lists it, and takes later work typed into a Claude pane as the user turn
/// it becomes in the session's transcript; a Claude-side Herdr server
/// socket that, given the typed first part, writes the user turn and the
/// flow's answer to the Claude transcript; and a Codex app-server that
/// opens one new thread, names it and reads its settings back.
pub(super) struct FreshWorld {
    pub(super) state: std::path::PathBuf,
    codex_invocations: std::path::PathBuf,
}

const FRESH_THREAD: &str = "01a0b22c-f00d-7000-8000-00000000f7e5";
const FRESH_CODEX_FLOW: &str = "f7e5a1";

/// One server-to-client WebSocket text frame, as `printf %b` octal escapes.
fn websocket_frame(json: &str) -> String {
    let mut bytes = vec![0x81];
    match u8::try_from(json.len()) {
        Ok(short) if short < 126 => bytes.push(short),
        _ => {
            bytes.push(126);
            bytes.extend_from_slice(&u16::try_from(json.len()).unwrap().to_be_bytes());
        }
    }
    bytes.extend_from_slice(json.as_bytes());
    bytes.iter().map(|byte| format!("\\{byte:03o}")).collect()
}

trait BuildsFreshWorld {
    /// Installs the fake Herdr, the Claude-side Herdr server socket for the
    /// session `fixture-session`, and the Codex app-server, all with empty
    /// state.
    fn fresh_world(&mut self, codex_title: &str) -> FreshWorld;
}

impl BuildsFreshWorld for NexusFixture {
    fn fresh_world(&mut self, codex_title: &str) -> FreshWorld {
        use crate::herdr::ConfiguresHerdrCli;
        let root = self.directory.path().to_path_buf();
        let state = root.join("fresh-world");
        fs::create_dir_all(&state).unwrap();
        let claude_root = root.join("native-transcripts/claude/-home-li-primary");
        fs::create_dir_all(&claude_root).unwrap();
        let herdr = format!(
            r##"#!/bin/sh
{sessions}
{submitted}
S='{state}'
printf '%s\n' "$*" >> "$S/calls"
# The first pane is w7:p1 on terminal term-fresh; each later one is the next
# workspace, w8:p1 on term-fresh-8 and so on.
ws=$(cat "$S/workspace" 2>/dev/null || printf 7); pane="w$ws:p1"
term=term-fresh; [ "$ws" = 7 ] || term="term-fresh-$ws"
agent() {{
  harness=$(cat "$S/harness"); title=$(cat "$S/title" 2>/dev/null)
  session=''
  reported=$(cat "$S/report-session" 2>/dev/null || cat "$S/session")
  [ "$harness" = claude ] && session=$(printf ',"agent_session":{{"source":"herdr:claude","agent":"claude","kind":"id","value":"%s"}}' "$reported")
  printf '{{"name":"%s","agent":"%s","agent_status":"idle","workspace_id":"w%s","tab_id":"w%s:t1","pane_id":"%s","terminal_id":"%s","interactive_ready":true,"terminal_title_stripped":"%s"%s}}' "$(cat "$S/name")" "$harness" "$ws" "$ws" "$pane" "$term" "$title" "$session"
}}
# A restart world also shows the seat a restart replaces (OLD_AGENT, its
# harness OLD_PID in pane OLD_PANE) and lists the panes it made, so a pane
# with no agent yet is shown; a pane it closes is gone.
agents() {{
  list=; [ -f "$S/old-agent" ] && list=$(cat "$S/old-agent")
  [ -f "$S/harness" ] && list="${{list:+$list,}}$(agent)"
  printf '%s' "$list"
}}
panes() {{
  [ -f "$S/restart-world" ] && [ -f "$S/workspace" ] && [ ! -f "$S/closed-$ws" ] && printf '{{"pane_id":"%s","terminal_id":"%s"}}' "$pane" "$term"
}}
# A process's end, awaited on its pidfd (util-linux waitpid): one already
# ended, a zombie included, is not waited for.
gone() {{
  waitpid --exited --timeout 30 "$1" > /dev/null 2>&1 || :
}}
case "$*" in
  *"workspace create"*)
    if [ -f "$S/workspace" ]; then ws=$((ws + 1)); fi; printf '%s' "$ws" > "$S/workspace"
    term=term-fresh; [ "$ws" = 7 ] || term="term-fresh-$ws"
    printf '{{"result":{{"workspace":{{"workspace_id":"w%s"}},"root_pane":{{"pane_id":"w%s:p1","terminal_id":"%s"}}}}}}\n' "$ws" "$ws" "$term" ;;
  *"pane run"*) sh -c "$6" > "$S/marker" ;;
  *"pane wait-output"*) line=$(grep -F FLOW_CLAUDE_ENV_READY_ "$S/marker"); printf '{{"result":{{"pane_id":"%s","matched_line":"%s"}}}}\n' "$pane" "$line" ;;
  *"agent start"*)
    printf '%s' "$5" > "$S/name"; harness=codex; prev=; resumed=
    for a in "$@"; do
      case "$prev" in
        --session-id) harness=claude; printf '%s' "$a" > "$S/session" ;;
        --resume) harness=claude; resumed=1; printf '%s' "$a" > "$S/session" ;;
        --name) printf '%s' "$a" > "$S/title" ;;
        resume) printf '%s' "$a" > "$S/session" ;;
      esac
      prev=$a
    done
    # A resume HOLD_START holds is held until RELEASE_START, START_HELD
    # telling the test it is held; RESUME_REFUSED refuses it.
    if [ -n "$resumed" ] && [ -f "$S/hold-start" ]; then
      : > "$S/start-held"
      timeout 20 sh -c ': < "$0"' "$S/release-start" || true
    fi
    if [ -n "$resumed" ] && [ -f "$S/resume-refused" ]; then printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"agent_start_failed","message":"resume refused"}}}}'; exit 1; fi
    # RESUME_REFUSED_ONCE refuses the next resume only.
    if [ -n "$resumed" ] && [ -f "$S/resume-refused-once" ]; then rm -f "$S/resume-refused-once"; printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"agent_start_failed","message":"resume refused once"}}}}'; exit 1; fi
    # REPORT_SESSION_ONCE is the session the next resumed pane reports; a
    # later one reports its own.
    if [ -n "$resumed" ] && [ -f "$S/reported-once" ]; then rm -f "$S/reported-once" "$S/report-session"; fi
    if [ -n "$resumed" ] && [ -f "$S/report-session-once" ]; then mv "$S/report-session-once" "$S/report-session"; : > "$S/reported-once"; fi
    # A resume started while an earlier resumed harness, or the old one,
    # still runs is written to OVERLAP.
    if [ -n "$resumed" ]; then
      for p in $(cat "$S/resumed-pids" "$S/old-pid" 2>/dev/null); do
        if [ -e "/proc/$p" ] && ! grep -q ') Z' "/proc/$p/stat" 2>/dev/null; then printf '%s\n' "$p" >> "$S/overlap"; fi
      done
    fi
    printf '%s' "$harness" > "$S/harness"
    # A resumed Claude runs `--resume <session>` in the pane's foreground,
    # marked as Herdr marks a pane's processes; its hook (HOOK_PID) is a
    # child whose environment is scrubbed.
    if [ -n "$resumed" ]; then
      HERDR_PANE_ID="$pane" HERDR_SESSION=fixture-session sh -c 's=$(command -v sleep); env -i "$s" 120 & printf "%s" "$!" > "$0"; sleep 120; :' "$S/hook-pid" --resume "$(cat "$S/session")" < /dev/null > /dev/null 2>&1 &
      printf '%s' "$!" > "$S/pid"
      printf '%s\n' "$!" >> "$S/resumed-pids"
    fi
    # REFUSED_AFTER_START refuses every resume once its harness runs in the
    # pane; REFUSED_AFTER_START_ONCE the next one only.
    if [ -n "$resumed" ] && [ -f "$S/refused-after-start" ]; then printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"timeout","message":"refused after the harness started"}}}}'; exit 1; fi
    if [ -n "$resumed" ] && [ -f "$S/refused-after-start-once" ]; then rm -f "$S/refused-after-start-once"; printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"timeout","message":"refused once after the harness started"}}}}'; exit 1; fi
    # The Codex TUI runs `resume <thread>` in the pane's foreground.
    if [ "$harness" = codex ]; then
      sh -c 'sleep 120; :' resume "$(cat "$S/session")" < /dev/null > /dev/null 2>&1 &
      printf '%s' "$!" > "$S/pid"
    fi
    if [ "$harness" = claude ]; then
      printf '{{"type":"custom-title","customTitle":"%s","sessionId":"%s"}}\n' "$(cat "$S/title")" "$(cat "$S/session")" >> '{claude_root}/'"$(cat "$S/session")"'.jsonl'
    fi
    # As Herdr 0.8.2 does, a Codex start returns only once the agent is idle:
    # with START_WAITS, a start that meets a running turn (TURN_STARTED, the
    # test having seen a turn/start reach the app-server) waits for its end,
    # which no fixture turn has, and times out as Herdr does. START_REFUSED
    # then refuses the start.
    if [ "$harness" = codex ] && [ -f "$S/start-waits" ] && [ -f "$S/turn-started" ]; then
      printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"timeout","message":"timed out waiting for agent startup"}}}}'; exit 1
    fi
    if [ "$harness" = codex ] && [ -f "$S/start-refused" ]; then printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"agent_start_failed","message":"refused after the first turn"}}}}'; exit 1; fi
    printf '{{"result":{{"agent":{{"name":"%s"}}}}}}\n' "$5" ;;
  *"agent wait"*)
    # A pane whose turn runs is not at rest.
    if [ -f "$S/turn-started" ]; then printf '%s\n' '{{"id":"cli:agent:wait","error":{{"code":"timeout","message":"timed out"}}}}'; exit 1; fi
    printf '%s\n' '{{"result":{{"status":"idle"}}}}' ;;
  *"agent get"*) printf '{{"result":{{"agent":%s}}}}\n' "$(agent)" ;;
  *"api snapshot"*) if [ -f "$S/restart-world" ]; then printf '{{"id":"cli:api:snapshot","result":{{"snapshot":{{"agents":[%s],"panes":[%s],"protocol":20,"version":"0.8.2"}},"type":"session_snapshot"}}}}\n' "$(agents)" "$(panes)"; else a=; [ -f "$S/harness" ] && a=$(agent); printf '{{"id":"cli:api:snapshot","result":{{"snapshot":{{"agents":[%s],"protocol":20,"version":"0.8.2"}},"type":"session_snapshot"}}}}\n' "$a"; fi ;;
  *"pane rename "*) printf '%s' "$6" > "$S/label"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane report-metadata "*) printf '%s' "$9" > "$S/display-agent"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane get "*) printf '{{"result":{{"pane":{{"pane_id":"%s","terminal_id":"%s","label":"%s","display_agent":"%s"}}}}}}\n' "$pane" "$term" "$(cat "$S/label")" "$(cat "$S/display-agent")" ;;
  *"workspace rename "*) printf '%s' "$6" > "$S/workspace-label-$5"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"workspace get "*) printf '{{"result":{{"workspace":{{"workspace_id":"%s","label":"%s"}}}}}}\n' "$5" "$(cat "$S/workspace-label-$5" 2>/dev/null)" ;;
  *"pane process-info"*)
    if [ -f "$S/old-pane" ] && [ "$6" = "$(cat "$S/old-pane")" ]; then
      printf '{{"result":{{"process_info":{{"pane_id":"%s","foreground_processes":[{{"pid":%s}}]}}}}}}\n' "$6" "$(cat "$S/old-pid")"
    else
      printf '{{"result":{{"process_info":{{"pane_id":"%s","foreground_processes":[{{"pid":%s}}]}}}}}}\n' "$pane" "$(cat "$S/pid" 2>/dev/null || printf 1)"
    fi ;;
  *"pane close"*)
    # CLOSE_REFUSED refuses every close. A restart world's close takes the
    # pane and its harness down; with ORPHAN_ON_CLOSE the pane goes and the
    # harness of a restart's pane outlives it.
    if [ -f "$S/close-refused" ]; then printf '%s\n' '{{"error":{{"code":"pane_close_failed","message":"refused"}}}}' >&2; exit 1; fi
    if [ -f "$S/old-pane" ] && [ "$5" = "$(cat "$S/old-pane")" ]; then rm -f "$S/old-agent"; p=$(cat "$S/old-pid" 2>/dev/null); [ -n "$p" ] && kill "$p" 2>/dev/null && gone "$p"; fi
    if [ -f "$S/restart-world" ] && [ "$5" = "$pane" ]; then rm -f "$S/harness"; : > "$S/closed-$ws"; p=$(cat "$S/pid" 2>/dev/null); [ -n "$p" ] && [ ! -f "$S/orphan-on-close" ] && kill "$p" 2>/dev/null && gone "$p"; rm -f "$S/pid"; fi
    printf '%s\n' '{{"result":{{}}}}' ;;
  *"agent read"*) printf '%s\n' '❯ ' ;;
  *"agent prompt"*)
    s=$(cat "$S/session"); t='{claude_root}/'"$s"'.jsonl'
    escaped=$(printf '%s' "$6" | sed ':a;N;$!ba;s/\\/\\\\/g;s/"/\\"/g;s/\n/\\n/g')
    # With rows-later, Claude has not yet written the user row: it is held
    # for the test to append once the prompt's hook has reported.
    row=$(printf '{{"type":"user","sessionId":"%s","uuid":"typed-%s","message":{{"content":"%s"}}}}' "$s" "$(wc -l < "$t")" "$escaped")
    if [ -f "$S/rows-later" ]; then printf '%s\n' "$row" > "$S/held-row"; else printf '%s\n' "$row" >> "$t"; fi
    submitted "$5" "$6"
    printf '{{"id":"cli:agent:prompt","result":{{"type":"agent_prompted","agent":%s}}}}\n' "$(agent)" ;;
  *) exit 64 ;;
esac
"##,
            sessions = self.session_listing(),
            submitted = self.submission_report(),
            state = state.display(),
            claude_root = claude_root.display(),
        );
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&herdr);

        // The Claude-side Herdr server: the typed part becomes the user turn,
        // and the flow answers it, in the session's own transcript.
        let socket = root.join("fresh-herdr.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let served_state = state.clone();
        let served_root = claude_root.clone();
        std::thread::spawn(move || {
            use std::io::{BufRead, Write};
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut line = String::new();
                if std::io::BufReader::new(&stream)
                    .read_line(&mut line)
                    .is_err()
                {
                    continue;
                }
                let request: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
                // A title readback's subscription to the pane's events is
                // started and announces nothing: the fresh seat shows its
                // title at the first read.
                if request.get("method").and_then(serde_json::Value::as_str)
                    == Some("events.subscribe")
                {
                    let _ = stream.write_all(
                        b"{\"id\":\"flow-nexus:title\",\"result\":{\"type\":\"subscription_started\"}}\n",
                    );
                    continue;
                }
                let text = request
                    .pointer("/params/text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let session = fs::read_to_string(served_state.join("session")).unwrap_or_default();
                let rows = [
                    serde_json::json!({"type":"user","sessionId":session,"message":{"role":"user","content":text}}),
                    serde_json::json!({"type":"assistant","sessionId":session,"uuid":"fresh-answer","message":{"model":"claude-fixture","content":[{"type":"text","text":"Woken."}]}}),
                ];
                let _ = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(served_root.join(format!("{session}.jsonl")))
                    .and_then(|mut file| rows.iter().try_for_each(|row| writeln!(file, "{row}")));
                let _ = stream.write_all(
                    b"{\"id\":\"flow:prompt\",\"result\":{\"type\":\"agent_prompted\"}}\n",
                );
            }
        });
        self.list_herdr_sessions(&[("fixture-session", socket)]);

        // The Codex app-server: the send-tool probe, the opening (which
        // names the thread), the Title's naming and the settings readback
        // with the first turn a fresh launch makes, in that order, each its
        // own proxy; with START_REFUSED, the account of the thread whose
        // start was refused in place of the naming. Each connection's client
        // frames are kept (record-N), and its start is logged among the
        // Herdr calls. The opening connection is subscribed to the thread it
        // started and stays open; every connection is held until the client
        // closes it.
        let invocations = state.join("codex-invocations");
        let opened = |id: u64| {
            websocket_frame(
                &serde_json::json!({"id":id,"result":{"thread":{"id":FRESH_THREAD},"model":"fixture-model","reasoningEffort":"medium","instructionSources":[]}})
                    .to_string(),
            )
        };
        let initialized = websocket_frame(r#"{"id":1,"result":{}}"#);
        let probe = [
            initialized.clone(),
            websocket_frame(
                r#"{"id":2,"error":{"code":-32600,"message":"thread/start.dynamicTools requires experimentalApi capability"}}"#,
            ),
        ]
        .concat();
        let named = |id: u64| {
            websocket_frame(
                &serde_json::json!({"id":id,"result":{"thread":{"id":FRESH_THREAD,"name":codex_title}}})
                    .to_string(),
            )
        };
        let opening = [
            initialized.clone(),
            opened(2),
            websocket_frame(r#"{"id":3,"result":{}}"#),
            named(4),
            websocket_frame(r#"{"id":5,"result":{}}"#),
        ]
        .concat();
        let naming = [
            initialized.clone(),
            websocket_frame(r#"{"id":2,"result":{}}"#),
            named(3),
        ]
        .concat();
        // The readback, then the FlowId told the seat, then its first turn.
        let readback = [
            initialized.clone(),
            opened(2),
            websocket_frame(r#"{"id":3,"result":{}}"#),
        ]
        .concat();
        let turn = websocket_frame(r#"{"id":4,"result":{"turn":{"id":"turn-fresh"}}}"#);
        let account = [
            initialized,
            websocket_frame(r#"{"id":2,"result":{"data":[]}}"#),
            websocket_frame(r#"{"id":3,"result":{}}"#),
        ]
        .concat();
        let app_server = root.join("fresh-codex");
        FixtureExecutable {
            path: app_server.clone(),
        }
        .install(&format!(
            "#!/bin/sh\nS='{state}'\nn=$(cat '{counter}' 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > '{counter}'\nprintf 'app-server connection %s\\n' \"$n\" >> \"$S/calls\"\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nexec 3<&0; cat <&3 > \"$S/record-$n\" &\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\ncase \"$n\" in\n  1) printf '%b' '{probe}' ;;\n  2) printf '%b' '{opening}' ;;\n  3) if [ -f \"$S/start-refused\" ]; then printf '%b' '{account}'; else printf '%b' '{naming}'; fi ;;\n  4) printf '%b' '{readback}{turn}' ;;\n  *) exit 1 ;;\nesac\nwait\n",
            state = state.display(),
            counter = invocations.display(),
        ));
        self.nexus.codex_endpoints.edit().stable.client_path = app_server;
        // A Codex TUI is started only against an absolute endpoint socket.
        self.nexus.codex_endpoints.edit().stable.socket =
            root.join("codex.sock").to_string_lossy().into_owned();
        let endpoints = (*self.nexus.codex_endpoints.current()).clone();
        let flows = root.join("flows");
        let herdr = std::mem::replace(
            &mut self.nexus.herdr,
            crate::herdr::HerdrCli::at(self.snapshot_program.clone(), flows),
        );
        self.nexus.herdr = herdr.with_codex_endpoints(endpoints);
        FreshWorld {
            state,
            codex_invocations: invocations,
        }
    }
}

fn configure_secondary_model(fixture: &NexusFixture, native: &str) {
    use crate::store::SeedsConfiguration;
    // A wake reads its model's model from the committed configuration,
    // which exists once the Nexus family is declared.
    let settings = crate::store::DefaultConfiguration {
        home: fixture.directory.path().join("home"),
        runtime_directory: fixture.directory.path().join("runtime"),
    }
    .configuration();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            meta_signal_flow::ConfigureNexus {
                source_root: flow_ethos::SourceRoot(
                    fixture.directory.path().to_string_lossy().into_owned(),
                ),
                first_codex_endpoint: settings.stable_codex,
                second_codex_endpoint: settings.next_codex,
                harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                    settings.message_nexus_path.clone(),
                )),
                message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                    "/fixture/message-nexus".into(),
                )),

                curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                    "/fixture/curriculum.sock".into(),
                ),
                lease: flow_ethos::Lease(30),
                presentation_prompt_option: None,
                herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
            },
        ))
        .expect("Nexus configured");
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                model: Model::Sol,
                native: meta_signal_flow::Native(native.into()),
            },
        ]))
        .expect("model configured");
}

/// A sleeping Codex address wakes fresh: the Nexus opens a new
/// thread through the app-server, attaches a TUI to it in a new pane, binds
/// the flow to that thread although Herdr reports no session, names it,
/// reads its settings back, reads the receipt from the new rollout, and
/// records the address Awake as the new flow. The old thread and its flow
/// are untouched: its rollout is byte-identical, nothing addresses it, and
/// the old flow stays Stopped on it.
#[test]
fn a_sleeping_codex_address_wakes_fresh_to_a_new_thread_and_binds_to_it() {
    use crate::title::{ShowsNativeTitle, TitlesFlow};
    let mut fixture = NexusFixture::new();
    // The old Codex flow 908786 on thread OLD, Stopped, its launch of record.
    let old = fixture.node();
    let old_thread = old.session_id.0.clone();
    let profile = LaunchProfile {
        launch_request_id: signal_flow::LaunchRequestId("old-codex-request".into()),
        system_modules: signal_flow::SystemModules(vec![
            signal_flow::ModuleIdentity("spirit-core".into()),
            signal_flow::ModuleIdentity("intent-flow".into()),
        ]),
        user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
            "operation-main-flow".into(),
        )]),
        address: address(Model::Sol),
        model_name: flow_ethos::ModelName("fixture-model".into()),
        effort: signal_flow::Effort("medium".into()),
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        flow_id_option: None,
        remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        leave_outs_option: None,
        instruction_prompt: signal_flow::InstructionPrompt("fixture instruction".into()),
    };
    let composed = fixture
        .nexus
        .composer
        .current()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                harness_kind: HarnessKind::Codex,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap();
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, Origin::Unbound)
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: composed.part_hashes(),
            })
            .unwrap()
    );
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_binding(NativeLaunchBinding {
                launch_request_id: profile.launch_request_id.clone(),
                flow_id: old.flow_id.clone(),
                native_session_id: signal_flow::NativeSessionId(old_thread.clone()),
                harness_kind: HarnessKind::Codex,
                herdr_pane_binding: signal_flow::HerdrPaneBinding {
                    launch_request_id: profile.launch_request_id.clone(),
                    herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                    herdr_agent_name: signal_flow::HerdrAgentName(composed.launch_agent_name()),
                    herdr_workspace_id: signal_flow::HerdrWorkspaceId("w1".into()),
                    herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
                    herdr_terminal_id: signal_flow::HerdrTerminalId("term-old".into()),
                },
            })
            .unwrap()
    );
    let mut old_node = old.clone();
    old_node.herdr_route_selection = HerdrRouteSelection::Available(HerdrRoute {
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        herdr_agent_name: signal_flow::HerdrAgentName(composed.launch_agent_name()),
        herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-old".into()),
    });
    fixture.nexus.store.register_flow(old_node).unwrap();
    assert!(fixture.nexus.store.record_stopped(&old.flow_id.0).unwrap());
    let rollouts = fixture.directory.path().join("native-transcripts/codex");
    fs::create_dir_all(&rollouts).unwrap();
    let old_rollout = rollouts.join(format!("rollout-{old_thread}.jsonl"));
    fs::write(
        &old_rollout,
        format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":old_thread}})
        ),
    )
    .unwrap();
    let old_bytes = fs::read(&old_rollout).unwrap();
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec![old.flow_id.0.as_str()],
        queue.clone(),
    ));
    configure_secondary_model(&fixture, "fixture-model");
    let at = address(Model::Sol);

    // The fresh seat the wake composes, and so the first turn the new thread
    // holds, and the title it is named with.
    let fresh = FreshSeat {
        address: at.clone(),
        launch_request_id: "any".into(),
        model_name: "fixture-model".into(),
        effort: "medium".into(),
        herdr_session_name: "fixture-session".into(),
        system_modules: profile.system_modules.clone(),
        user_modules: profile.user_modules.clone(),
        leave_outs_option: None,
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        prompt: queue.clone().unsent().prompt_text(&address(Model::Sol)),
    }
    .fresh_profile();
    let title = fresh.native_title(FRESH_CODEX_FLOW).unwrap();
    let first_part = fixture
        .nexus
        .composer
        .current()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                harness_kind: HarnessKind::Codex,
                launch_profile: fresh,
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap()
        .first_prompt_payload
        .prompt_part_vector[0]
        .prompt_part_text
        .clone();
    let world = fixture.fresh_world(title.as_str());
    let rows = [
        serde_json::json!({"type":"session_meta","payload":{"id":FRESH_THREAD}}),
        serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-fresh"}}),
        serde_json::json!({"type":"event_msg","payload":{"thread_id":FRESH_THREAD,"turn_id":"turn-fresh","item":{"type":"UserMessage","content":[{"type":"text","text":first_part.0}]}}}),
        serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Woken."}]}}),
    ];
    fs::write(
        rollouts.join(format!("rollout-{FRESH_THREAD}.jsonl")),
        rows.iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    // The new thread's FlowId claim, as `flow-id codex` makes it.
    FixtureExecutable {
        path: fixture.directory.path().join("native-transcripts/flow-id"),
    }
    .install(&format!(
        "#!/bin/sh\n[ \"$1\" = codex ] && [ \"$CODEX_SESSION_ID\" = '{FRESH_THREAD}' ] || exit 2\nprintf 'version=1\\nharness=codex\\nidentity={identity}\\nalias={FRESH_CODEX_FLOW}\\n' > \"$3/.{FRESH_CODEX_FLOW}.flow-id\"\nprintf '{FRESH_CODEX_FLOW}\\n'\n",
        identity = FRESH_THREAD.replace('-', ""),
    ));

    let outcome = fixture.nexus.perform(Operation::Wake(at.clone()));
    assert_eq!(
        outcome,
        Outcome::Woken(flow_ethos::FlowId(FRESH_CODEX_FLOW.into())),
        "Herdr calls: {}; app-server invocations: {:?}",
        fs::read_to_string(world.state.join("calls")).unwrap_or_default(),
        fs::read_to_string(&world.codex_invocations).ok()
    );

    let woken = stored(&fixture, Model::Sol);
    assert_eq!(
        woken.state,
        State::Awake(flow_ethos::FlowId(FRESH_CODEX_FLOW.into()))
    );
    assert!(woken.queue.0.is_empty());
    assert_eq!(woken.past, flow_ethos::Past(vec![old.flow_id.clone()]));
    let new_node = fixture
        .nexus
        .store
        .flow_node(FRESH_CODEX_FLOW)
        .unwrap()
        .unwrap();
    assert_eq!(new_node.session_id.0, FRESH_THREAD);
    assert_eq!(new_node.harness_kind, HarnessKind::Codex);
    assert_eq!(new_node.flow_lifecycle, FlowLifecycle::Active);
    let old_after = fixture
        .nexus
        .store
        .flow_node(&old.flow_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(old_after.session_id.0, old_thread);
    assert_eq!(old_after.flow_lifecycle, FlowLifecycle::Stopped);
    assert_eq!(fs::read(&old_rollout).unwrap(), old_bytes);
    // Exactly the probe, the opening, the naming and the readback reached
    // the app-server; the naming and the readback each read back the new
    // thread, so none addressed another.
    assert_eq!(fs::read_to_string(&world.codex_invocations).unwrap(), "4");
    let calls = fs::read_to_string(world.state.join("calls")).unwrap();
    assert!(calls.contains(&format!("resume {FRESH_THREAD}")), "{calls}");
    assert!(!calls.contains(&old_thread), "{calls}");
    assert_eq!(
        fs::read_to_string(world.state.join("label")).unwrap(),
        title.as_str()
    );
}

/// The Claude equivalent: a sleeping Claude address wakes fresh to a new
/// native session Flow chose, never the old one; the new flow binds to that
/// session and the old flow keeps its own.
#[test]
fn a_sleeping_claude_address_wakes_fresh_to_a_new_session_and_binds_to_it() {
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    let at = address(Model::Sol);
    let world = fixture.fresh_claude_world();

    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!(
            "the fresh Claude wake starts; Herdr calls: {}",
            fs::read_to_string(world.state.join("calls")).unwrap_or_default()
        );
    };
    assert_ne!(flow_id.0, "abc123");
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_id.clone()));
    assert!(woken.queue.0.is_empty());
    let new_node = fixture.nexus.store.flow_node(&flow_id.0).unwrap().unwrap();
    let new_session = fs::read_to_string(world.state.join("session")).unwrap();
    assert_eq!(new_node.session_id.0, new_session);
    assert_ne!(new_session, seat.session);
    assert_eq!(new_node.harness_kind, HarnessKind::Claude);
    assert_eq!(new_node.flow_lifecycle, FlowLifecycle::Active);
    let old_after = fixture.nexus.store.flow_node("abc123").unwrap().unwrap();
    assert_eq!(old_after.session_id.0, seat.session);
    assert_eq!(old_after.flow_lifecycle, FlowLifecycle::Stopped);
    let calls = fs::read_to_string(world.state.join("calls")).unwrap();
    assert!(
        calls.contains(&format!("--session-id {new_session}")),
        "{calls}"
    );
    assert!(!calls.contains(&seat.session), "{calls}");
}

/// A model Model declared as a Claude alias is not an exact native model: a
/// fresh wake from it is refused `InexactModel` before any pane is opened,
/// so no native session receives the queued work, and the work stays queued
/// with the metaflow Asleep. Declared as the full name, the same wake starts and
/// takes the work.
#[test]
fn a_fresh_wake_from_a_claude_alias_opens_nothing_and_keeps_its_work() {
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture.claims_flow_ids();
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        vec![Request::Order("A".into())],
    ));
    configure_secondary_model(&fixture, "haiku");
    let at = address(Model::Sol);
    let world = fixture.fresh_world("unused");

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(
            crate::generated::operation::Failed_Data::CompositionRefused(
                signal_flow::CompositionRefusal::InexactModel
            )
        )
    );
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(
        asleep.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("abc123".to_owned())])
    );
    assert_eq!(asleep.queue.0.len(), 1);
    let calls = fs::read_to_string(world.state.join("calls")).unwrap_or_default();
    assert!(!calls.contains("--session-id"), "{calls}");

    configure_secondary_model(&fixture, "claude-fixture");
    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!(
            "the exact Claude model wakes; Herdr calls: {}",
            fs::read_to_string(world.state.join("calls")).unwrap_or_default()
        );
    };
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_id));
    assert!(woken.queue.0.is_empty());
}

/// A wake whose start is refused keeps every request queued, never lost,
/// and marks each one's delivery `Retryable.WakeRefused` with the refusal,
/// which the sender's observation shows; the next wake retries the start and
/// the woken flow takes the same queue in its accepted first prompt.
#[test]
fn a_refused_wake_keeps_its_queue_and_each_delivery_names_why() {
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture.claims_flow_ids();
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    configure_secondary_model(&fixture, "haiku");
    let at = address(Model::Sol);
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Opus,
    };
    let accepted: Vec<_> = ["A", "B"]
        .into_iter()
        .map(|body| {
            match fixture
                .nexus
                .store
                .accept_logical_delivery(
                    &at,
                    SentBy::Sender(sender.clone()),
                    flow_ethos::Delivery::Queue,
                    Message::Soft(Request::Order(body.into())),
                    None,
                )
                .unwrap()
            {
                signal_flow::PrivateSendResponse::SendAccepted(accepted) => {
                    accepted.logical_delivery_id
                }
                other => panic!("the send is accepted: {other:?}"),
            }
        })
        .collect();
    let world = fixture.fresh_world("unused");

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(
            crate::generated::operation::Failed_Data::CompositionRefused(
                signal_flow::CompositionRefusal::InexactModel
            )
        )
    );
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(
        asleep
            .queue
            .0
            .iter()
            .map(|entry| entry.logical_delivery_id.clone())
            .collect::<Vec<_>>(),
        accepted,
        "every request stays queued, in order"
    );
    let refused = signal_flow::DeliveryState::Retryable(signal_flow::RetryCause::WakeRefused(
        signal_flow::StartRejection::CompositionRefused(
            signal_flow::CompositionRefusal::InexactModel,
        ),
    ));
    let observed = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&sender, None)
        .unwrap();
    assert_eq!(
        observed
            .delivery_snapshot_vector
            .iter()
            .map(|snapshot| (
                snapshot.logical_delivery_id.clone(),
                snapshot.delivery_state.clone()
            ))
            .collect::<Vec<_>>(),
        accepted
            .iter()
            .map(|id| (id.clone(), refused.clone()))
            .collect::<Vec<_>>(),
        "the sender sees why each request waits"
    );
    let calls = fs::read_to_string(world.state.join("calls")).unwrap_or_default();
    assert!(!calls.contains("--session-id"), "{calls}");

    configure_secondary_model(&fixture, "claude-fixture");
    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!(
            "the retried wake starts; Herdr calls: {}",
            fs::read_to_string(world.state.join("calls")).unwrap_or_default()
        );
    };
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_id));
    assert!(
        woken.queue.0.is_empty(),
        "the first prompt takes the same queue"
    );
}

#[test]
fn a_missing_module_wake_keeps_its_identity_in_the_caller_outcome_and_queue_state() {
    let fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture.claims_flow_ids();
    let at = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    configure_secondary_model(&fixture, "claude-fixture");
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
            meta_signal_flow::ConfigureMetaflow {
                root_option: None,
                address: at.clone(),
                system_modules: signal_flow::SystemModules(vec![signal_flow::ModuleIdentity(
                    "vision:absent".into(),
                )]),
                user_modules: signal_flow::UserModules(vec![]),
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                continuity: meta_signal_flow::Continuity::Maintained,
                purpose: meta_signal_flow::Purpose(String::new()),
                records: meta_signal_flow::Records(vec![]),
            },
        ]))
        .expect("the missing module is a valid declaration");
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Opus,
    };
    let signal_flow::PrivateSendResponse::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &at,
            SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Order("A".into())),
            None,
        )
        .unwrap()
    else {
        panic!("the request is accepted")
    };
    let refusal = signal_flow::ContextRefusal::ModuleMissing(Some(signal_flow::ModuleIdentity(
        "vision:absent".into(),
    )));

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at)),
        Outcome::Failed(Failed_Data::ContextRefused(refusal.clone()))
    );
    let observed = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&sender, None)
        .unwrap();
    assert_eq!(
        observed.delivery_snapshot_vector[0].logical_delivery_id,
        accepted.logical_delivery_id
    );
    assert_eq!(
        observed.delivery_snapshot_vector[0].delivery_state,
        signal_flow::DeliveryState::Retryable(signal_flow::RetryCause::WakeRefused(
            signal_flow::StartRejection::ContextRefused(refusal)
        ))
    );
}

/// A fresh wake takes the native model configured for its seat's Model,
/// never another Model's: a Mind Sol seat while only Opus has a native
/// model opens nothing and keeps its work queued Asleep; once Sol's native
/// model is declared beside Opus's, the same wake launches on Sol's.
#[test]
fn a_fresh_wake_takes_the_model_of_its_layer_and_aspect() {
    use crate::store::ReadsLaunchAttempt as _;
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture.claims_flow_ids();
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        vec![Request::Order("A".into())],
    ));
    configure_secondary_model(&fixture, "claude-fixture");
    let models = |fixture: &NexusFixture, declared: &[(Model, &str)]| {
        fixture
            .nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(
                declared
                    .iter()
                    .map(|(model, native)| meta_signal_flow::ConfigureModel {
                        model: model.clone(),
                        native: meta_signal_flow::Native((*native).into()),
                    })
                    .collect(),
            ))
            .expect("models configured");
    };
    models(&fixture, &[(Model::Opus, "claude-fixture")]);
    let at = address(Model::Sol);
    assert_eq!(at.aspect, Aspect::Mind);
    let world = fixture.fresh_world("unused");

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(
            crate::generated::operation::Failed_Data::CompositionRefused(
                signal_flow::CompositionRefusal::Unconfigured
            )
        )
    );
    let asleep = stored(&fixture, Model::Sol);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.queue.0.len(), 1);
    let calls = fs::read_to_string(world.state.join("calls")).unwrap_or_default();
    assert!(!calls.contains("--session-id"), "{calls}");

    models(
        &fixture,
        &[
            (Model::Opus, "claude-other"),
            (Model::Sol, "claude-fixture"),
        ],
    );
    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!(
            "Sol's native model wakes the seat; Herdr calls: {}",
            fs::read_to_string(world.state.join("calls")).unwrap_or_default()
        );
    };
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_id.clone()));
    assert!(woken.queue.0.is_empty());
    let launched = fixture
        .nexus
        .store
        .launch_requests_bound_to(&flow_id.0)
        .expect("launches read");
    let attempt = fixture
        .nexus
        .store
        .launch_attempt(launched.last().expect("the wake's launch"))
        .expect("attempt reads")
        .expect("attempt");
    assert_eq!(
        attempt.launch_profile.model_name,
        flow_ethos::ModelName("claude-fixture".into())
    );
}

/// The first-turn send of a fresh Codex launch, against the fresh world: a
/// Codex seat at SENDER whose first turn calls the send tool while the
/// launch submits it, and the sleeping Claude seat abc123 the call orders.
struct FirstTurnSend {
    world: FreshWorld,
    sender: LaunchProfile,
    recipient: Address,
}

/// The id of the first turn's tool call as the app-server sends it.
const FIRST_CALL_REQUEST: u64 = 77;
const FIRST_CALL_BODY: &str = "Wake and reply.";

fn first_call_key() -> crate::generated::send_tool::ToolCallKey {
    crate::generated::send_tool::ToolCallKey {
        thread_id: crate::generated::send_tool::ThreadId(FRESH_THREAD.into()),
        turn_id: crate::generated::send_tool::TurnId("turn-fresh".into()),
        call_id: crate::generated::send_tool::CallId("call-first".into()),
    }
}

trait SetsUpFirstTurnSend {
    /// The fresh world with the sender's first turn calling `send` towards
    /// `recipient`, Herdr's start and wait refusing a seat whose turn runs,
    /// the rollout and the FlowId claims a fresh Codex and a fresh Claude
    /// launch read.
    fn first_turn_send(&mut self, recipient: Address) -> FirstTurnSend;
}

impl SetsUpFirstTurnSend for NexusFixture {
    fn first_turn_send(&mut self, recipient: Address) -> FirstTurnSend {
        use crate::store::SeedsConfiguration;
        use crate::title::{ShowsNativeTitle, TitlesFlow};
        let sender = LaunchProfile {
            launch_request_id: signal_flow::LaunchRequestId("first-turn-send".into()),
            system_modules: signal_flow::SystemModules(vec![
                signal_flow::ModuleIdentity("spirit-core".into()),
                signal_flow::ModuleIdentity("intent-flow".into()),
            ]),
            user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                "operation-main-flow".into(),
            )]),
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            address: Address {
                aspect: Aspect::Field,
                topic: flow_ethos::Topic("sendProof".into()),
                model: Model::Luna,
            },
            model_name: flow_ethos::ModelName("fixture-model".into()),
            effort: signal_flow::Effort("medium".into()),
            flow_id_option: None,
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
            herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
            leave_outs_option: None,
            instruction_prompt: signal_flow::InstructionPrompt("Call the send tool once.".into()),
        };
        let settings = crate::store::DefaultConfiguration {
            home: self.directory.path().join("home"),
            runtime_directory: self.directory.path().join("runtime"),
        }
        .configuration();
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: flow_ethos::SourceRoot(
                        self.directory.path().to_string_lossy().into_owned(),
                    ),
                    first_codex_endpoint: settings.stable_codex,
                    second_codex_endpoint: settings.next_codex,
                    harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                    meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                    message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                        settings.message_nexus_path.clone(),
                    )),
                    message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                        "/fixture/message-nexus".into(),
                    )),

                    curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                        "/fixture/curriculum.sock".into(),
                    ),
                    lease: flow_ethos::Lease(30),
                    presentation_prompt_option: None,
                    herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                },
            ))
            .expect("Nexus configured");
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    model: Model::Sol,
                    native: meta_signal_flow::Native("claude-fixture".into()),
                },
                meta_signal_flow::ConfigureModel {
                    model: Model::Luna,
                    native: meta_signal_flow::Native("fixture-model".into()),
                },
            ]))
            .expect("models configured");
        let first_part = self
            .nexus
            .composer
            .current()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: sender.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap()
            .first_prompt_payload
            .prompt_part_vector[0]
            .prompt_part_text
            .clone();
        let title = sender.native_title(FRESH_CODEX_FLOW).unwrap();
        let world = self.fresh_world(title.as_str());
        // Herdr 0.8.2: `agent start` returns once the agent is idle, and
        // times out while a turn runs.
        fs::write(world.state.join("start-waits"), "").unwrap();
        let call = serde_json::json!({
            "id": FIRST_CALL_REQUEST,
            "method": "item/tool/call",
            "params": {
                "threadId": FRESH_THREAD,
                "turnId": "turn-fresh",
                "callId": "call-first",
                "tool": "send",
                "arguments": {
                    "target": {
                        "aspect": "Mind",
                        "topic": recipient.topic.0,
                        "model": format!("{:?}", recipient.model)
                    },
                    "priority": "MiddleAbrupt",
                    "kind": "Order",
                    "body": FIRST_CALL_BODY
                }
            }
        });
        fs::write(
            world.state.join("first-call"),
            websocket_frame(&call.to_string()),
        )
        .unwrap();
        let rollouts = self.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&rollouts).unwrap();
        let rows = [
            serde_json::json!({"type":"session_meta","payload":{"id":FRESH_THREAD}}),
            serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-fresh"}}),
            serde_json::json!({"type":"event_msg","payload":{"thread_id":FRESH_THREAD,"turn_id":"turn-fresh","item":{"type":"UserMessage","content":[{"type":"text","text":first_part.0}]}}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Queued."}]}}),
        ];
        fs::write(
            rollouts.join(format!("rollout-{FRESH_THREAD}.jsonl")),
            rows.iter()
                .map(|row| format!("{row}\n"))
                .collect::<String>(),
        )
        .unwrap();
        // `flow-id codex` for the new thread, `flow-id claude` for a fresh
        // Claude session.
        FixtureExecutable {
            path: self.directory.path().join("native-transcripts/flow-id"),
        }
        .install(&format!(
            r#"#!/bin/sh
if [ "$1" = codex ]; then
  [ "$CODEX_SESSION_ID" = '{FRESH_THREAD}' ] || exit 2
  printf 'version=1\nharness=codex\nidentity={identity}\nalias={FRESH_CODEX_FLOW}\n' > "$3/.{FRESH_CODEX_FLOW}.flow-id"
  printf '{FRESH_CODEX_FLOW}\n'
  exit 0
fi
[ "$1" = claude ] && [ "$2" = --flows-root ] && [ "$4" = --parent-session ] || exit 2
identity=$(printf '%s' "$5" | tr -d -)
alias=$(printf '%s' "$identity" | cut -c1-6)
: > "$3/.$alias.flow-id.lock"
mkdir -p "$3/$alias"
printf 'version=1\nharness=claude\nidentity=%s\nalias=%s\nuuid-version=uuid-v5\n' "$identity" "$alias" > "$3/.$alias.flow-id"
printf '%s\n' "$alias"
"#,
            identity = FRESH_THREAD.replace('-', ""),
        ));
        FirstTurnSend {
            world,
            sender,
            recipient,
        }
    }
}

/// Stands for the app-server's view of the first turn: once any connection
/// has carried a `turn/start`, TURN_STARTED is written, which the fake Herdr
/// reads as a turn running. Returns the connection that carried it.
fn turn_started_when_requested(
    state: std::path::PathBuf,
) -> std::thread::JoinHandle<Option<std::path::PathBuf>> {
    use super::send_tool::ReadsClientFrames;
    std::thread::spawn(move || {
        use crate::event_wait::AwaitsDirectory;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let started = || {
            (1..=6)
                .map(|n| state.join(format!("record-{n}")))
                .find(|record| {
                    record.client_messages().iter().any(|message| {
                        message.get("method").and_then(serde_json::Value::as_str)
                            == Some("turn/start")
                    })
                })
        };
        // Each frame a connection records is a write in the state directory.
        if !state.until_holds(deadline, &mut || started().is_some()) {
            return None;
        }
        let record = started();
        fs::write(state.join("turn-started"), "").unwrap();
        record
    })
}

/// The order of a fresh Codex launch as the app-server and Herdr saw it:
/// the connection that opened the thread is logged before the Codex
/// `agent start`, and the connection that carried the first `turn/start`
/// after it; the opening connection named the thread and started no turn.
trait ShowsCodexLaunchOrder {
    fn shows_thread_start_then_agent_start_then_turn_start(
        &self,
        turn_record: Option<std::path::PathBuf>,
    );
}

impl ShowsCodexLaunchOrder for FirstTurnSend {
    fn shows_thread_start_then_agent_start_then_turn_start(
        &self,
        turn_record: Option<std::path::PathBuf>,
    ) {
        use super::send_tool::ReadsClientFrames;
        let state = &self.world.state;
        assert_eq!(turn_record, Some(state.join("record-4")));
        let calls = fs::read_to_string(state.join("calls")).unwrap_or_default();
        let line_of = |needle: &str| {
            calls
                .lines()
                .position(|line| line.contains(needle))
                .unwrap_or_else(|| panic!("{needle} not among the calls: {calls}"))
        };
        let opening = line_of("app-server connection 2");
        let attach = line_of(&format!("resume {FRESH_THREAD}"));
        let turn = line_of("app-server connection 4");
        assert!(opening < attach && attach < turn, "{calls}");
        let methods = |record: &str| {
            state
                .join(record)
                .client_messages()
                .iter()
                .filter_map(|message| message.get("method")?.as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            methods("record-2"),
            [
                "initialize",
                "initialized",
                "thread/start",
                "thread/name/set",
                "thread/read",
                "thread/section/move"
            ]
        );
        assert_eq!(
            methods("record-4"),
            [
                "initialize",
                "initialized",
                "thread/resume",
                "thread/inject_items",
                "turn/start"
            ]
        );
    }
}

/// A first turn that runs far longer than Herdr's start waits refuses
/// nothing: Herdr's start, waiting for an idle seat as Herdr 0.8.2 does,
/// meets the opened thread before any turn, and the bind before it too;
/// the turn starts after both and never ends here, and the launch is
/// Started all the same.
#[test]
fn a_codex_first_turn_starts_after_the_tui_attached_and_a_long_turn_refuses_nothing() {
    use crate::DispatchesSerially;
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    let recipient = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec![], vec![]));
    let setup = fixture.first_turn_send(recipient);
    // The turn calls nothing and never ends.
    fs::remove_file(setup.world.state.join("first-call")).unwrap();
    let started = turn_started_when_requested(setup.world.state.clone());

    let response = fixture.nexus.dispatch_serially(
        signal_flow::Query::Start(setup.sender.clone()),
        Origin::Unbound,
    );

    let calls = fs::read_to_string(setup.world.state.join("calls")).unwrap_or_default();
    let signal_flow::Response::Started(started_flow) = &response else {
        panic!("the launch completes: {response:?}; Herdr calls: {calls}");
    };
    assert_eq!(started_flow.flow_id.0, FRESH_CODEX_FLOW);
    setup.shows_thread_start_then_agent_start_then_turn_start(started.join().unwrap());
}

/// Herdr refuses the start: the launch is refused, and the thread it
/// opened, named and idle, is accounted for and archived; no turn was
/// ever started on it, and nothing was queued from it.
#[test]
fn a_refused_codex_start_archives_its_idle_thread_and_starts_no_turn() {
    use super::send_tool::ReadsClientFrames;
    use crate::DispatchesSerially;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let recipient = address(Model::Astra);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Astra, State::Asleep, vec![], vec![]));
    let setup = fixture.first_turn_send(recipient.clone());
    fs::write(setup.world.state.join("start-refused"), "").unwrap();
    fs::remove_file(setup.world.state.join("first-call")).unwrap();

    let response = fixture.nexus.dispatch_serially(
        signal_flow::Query::Start(setup.sender.clone()),
        Origin::Unbound,
    );

    let signal_flow::Response::StartRejected(signal_flow::StartRejection::HarnessSpawnRefused(
        refusal,
    )) = &response
    else {
        panic!("the start is refused: {response:?}");
    };
    // The thread had no turn to read: the account names none.
    assert_eq!(
        refusal.opened_thread_option,
        Some(signal_flow::OpenedThread {
            native_session_id: signal_flow::NativeSessionId(FRESH_THREAD.into()),
            first_turn: signal_flow::FirstTurn::Unread,
            thread_archive: signal_flow::ThreadArchive::Archived,
        })
    );
    let account = setup.world.state.join("record-3").client_messages();
    assert!(
        account.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/archive")
        }),
        "{account:?}"
    );
    for n in 1..=4 {
        let record = setup
            .world
            .state
            .join(format!("record-{n}"))
            .client_messages();
        assert!(
            !record
                .iter()
                .any(|message| message["method"] == "turn/start"),
            "record-{n}: {record:?}"
        );
    }
    assert_eq!(
        fixture.nexus.store.send_call(&first_call_key()).unwrap(),
        None
    );
    assert!(stored(&fixture, Model::Astra).queue.0.is_empty());
    // After the refusal the thread is no launch's: a call now sends as
    // Unbound.
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        signal_flow::SentBy::Unbound
    );
}

/// A Codex launch as a Nexus that exited mid-way left it: reserved, its
/// native intent recorded, its pane created and its thread recorded, its
/// first turn pending on a send call; nothing settled. Returns the
/// sender's profile and the state directory of the fakes.
fn interrupted_launch(fixture: &mut NexusFixture) -> FirstTurnSend {
    use crate::store::RecordsCreatedLaunchPane;
    use crate::store::opened_thread::RecordsOpenedThreads;
    let recipient = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec![], vec![]));
    let setup = fixture.first_turn_send(recipient);
    let composed = fixture
        .nexus
        .composer
        .current()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                harness_kind: HarnessKind::Codex,
                launch_profile: setup.sender.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap();
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, Origin::Unbound)
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: setup.sender.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: setup.sender.model_name.clone(),
                effort: setup.sender.effort.clone(),
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: composed.part_hashes(),
            })
            .unwrap()
    );
    assert!(
        fixture
            .nexus
            .store
            .record_created_launch_pane(signal_flow::HerdrPaneBinding {
                launch_request_id: setup.sender.launch_request_id.clone(),
                herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                herdr_agent_name: signal_flow::HerdrAgentName(composed.launch_agent_name()),
                herdr_workspace_id: signal_flow::HerdrWorkspaceId("w7".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("w7:p1".into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("term-fresh".into()),
            })
            .unwrap()
    );
    assert!(
        fixture
            .nexus
            .store
            .record_opened_thread(&setup.sender.launch_request_id.0, FRESH_THREAD)
            .unwrap()
    );
    // The app-server the restarted Nexus meets: the send-tool probe; the
    // connection that resumes the thread, to which the pending calls are
    // replayed after its answer, then the barrier's answer, then the
    // calls again (the barrier is itself a resume), then the answer to the
    // read that shows the answers read; and the account that archives the
    // thread.
    let initialized = websocket_frame(r#"{"id":1,"result":{}}"#);
    let probe = [
        initialized.clone(),
        websocket_frame(
            r#"{"id":2,"error":{"code":-32600,"message":"thread/start.dynamicTools requires experimentalApi capability"}}"#,
        ),
    ]
    .concat();
    let resumed = [
        initialized.clone(),
        websocket_frame(r#"{"id":2,"result":{"thread":{}}}"#),
    ]
    .concat();
    let barrier = [
        websocket_frame(r#"{"id":3,"result":{"thread":{}}}"#),
        websocket_frame(r#"{"id":4,"result":{"thread":{}}}"#),
    ];
    let (barrier, answers_read) = (&barrier[0], &barrier[1]);
    let account = [
        initialized.clone(),
        websocket_frame(r#"{"id":2,"result":{"data":[{"status":"inProgress"}]}}"#),
        websocket_frame(r#"{"id":3,"result":{}}"#),
    ]
    .concat();
    let archive_refused = [
        initialized,
        websocket_frame(r#"{"id":2,"result":{"data":[{"status":"inProgress"}]}}"#),
        websocket_frame(r#"{"id":3,"error":{"code":-32603,"message":"archive refused"}}"#),
    ]
    .concat();
    let app_server = fixture.directory.path().join("restarted-codex");
    FixtureExecutable {
        path: app_server.clone(),
    }
    .install(&format!(
        "#!/bin/sh\nS='{state}'\nn=$(cat \"$S/restart-invocations\" 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > \"$S/restart-invocations\"\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nexec 3<&0; cat <&3 > \"$S/restart-record-$n\" &\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\ncase \"$((n % 3))\" in\n  1) printf '%b' '{probe}' ;;\n  2) printf '%b' '{resumed}'; replay() {{ for c in first-call second-call; do [ -e \"$S/$c\" ] && printf '%b' \"$(cat \"$S/$c\")\"; done; }}; replay; printf '%b' '{barrier}'; replay; printf '%b' '{answers_read}' ;;\n  0) if [ -e \"$S/archive-refused\" ]; then printf '%b' '{archive_refused}'; else printf '%b' '{account}'; fi ;;\nesac\nwait\n",
        state = setup.world.state.display(),
    ));
    fixture.nexus.codex_endpoints.edit().stable.client_path = app_server;
    setup
}

/// What the restarted Nexus wrote on the connection `record` that resumed
/// the interrupted thread, once it holds the read that shows the answers
/// read: the Nexus writes that read after the answers, and the fixture
/// copies the connection in order.
fn resumed_connection(state: &std::path::Path, record: &str) -> Vec<serde_json::Value> {
    use super::send_tool::ReadsClientFrames;
    use crate::event_wait::AwaitsDirectory;
    let path = state.join(record);
    let shows_read = |messages: &[serde_json::Value]| {
        messages.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/read")
                && message.get("id") == Some(&serde_json::json!(4))
        })
    };
    let held = state.until_holds(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        &mut || shows_read(&path.client_messages()),
    );
    let messages = path.client_messages();
    assert!(held, "the answers are shown read: {messages:?}");
    messages
}

/// The calls an app-server replays on resume are all taken, however many:
/// the Nexus resumes the thread a second time as the replay's end (the
/// app-server answers each resume of a running thread only after the
/// replay of the one before it), answers each call once, after that end,
/// and closes the connection only once a read's answer shows its answers
/// read. No window is waited out: the calls the barrier's own resume
/// replays again after its answer are not answered twice.
#[test]
fn every_call_replayed_before_the_resume_barrier_is_answered_once() {
    use crate::launching::ReconcilesInterruptedLaunches;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();
    let second = serde_json::json!({
        "id": FIRST_CALL_REQUEST + 1,
        "method": "item/tool/call",
        "params": {
            "threadId": FRESH_THREAD,
            "turnId": "turn-fresh",
            "callId": "call-second",
            "tool": "send",
            "arguments": {
                "target": {
                    "aspect": "Mind",
                    "topic": setup.recipient.topic.0,
                    "model": format!("{:?}", setup.recipient.model)
                },
                "priority": "Soft",
                "kind": "Order",
                "body": "a second call"
            }
        }
    });
    fs::write(
        state.join("second-call"),
        websocket_frame(&second.to_string()),
    )
    .unwrap();

    fixture.nexus.reconcile_interrupted_launches();

    let written = resumed_connection(&state, "restart-record-2");
    let position = |holds: &dyn Fn(&serde_json::Value) -> bool| {
        written
            .iter()
            .position(holds)
            .unwrap_or_else(|| panic!("not written: {written:?}"))
    };
    let request = |id: u64, method: &'static str| {
        move |message: &serde_json::Value| {
            message.get("id") == Some(&serde_json::json!(id))
                && message.get("method").and_then(serde_json::Value::as_str) == Some(method)
        }
    };
    let resumed = position(&request(2, "thread/resume"));
    let barrier = position(&request(3, "thread/resume"));
    let read = position(&request(4, "thread/read"));
    for call in [FIRST_CALL_REQUEST, FIRST_CALL_REQUEST + 1] {
        let answers = written
            .iter()
            .enumerate()
            .filter(|(_, message)| {
                message.get("id") == Some(&serde_json::json!(call))
                    && message.get("method").is_none()
            })
            .collect::<Vec<_>>();
        assert_eq!(answers.len(), 1, "call {call} answered once: {written:?}");
        let (at, answer) = answers[0];
        assert!(
            resumed < barrier && barrier < at && at < read,
            "{written:?}"
        );
        assert!(answer.to_string().contains("ThreadUnowned"), "{answer}");
    }
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
}

/// A Nexus restarted after a Codex launch had durably opened its thread
/// and its first turn's send call was pending: the launch is reconciled at
/// the start. The replayed call is refused and nothing is queued, the
/// thread is archived, and the launch is settled refused; no second launch
/// is made, a replayed Start answers the settlement, and a second start
/// finds nothing to reconcile.
#[test]
fn a_launch_interrupted_with_its_first_call_pending_is_refused_archived_and_settled_at_restart() {
    use super::send_tool::ReadsClientFrames;
    use crate::DispatchesSerially;
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::RecordsLaunchOutcome;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();

    fixture.nexus.reconcile_interrupted_launches();

    let replayed = resumed_connection(&state, "restart-record-2");
    let answer = replayed
        .iter()
        .find(|message| message.get("id") == Some(&serde_json::json!(FIRST_CALL_REQUEST)))
        .unwrap_or_else(|| panic!("the replayed call is answered: {replayed:?}"));
    assert_eq!(
        answer.pointer("/result/success"),
        Some(&serde_json::json!(false))
    );
    assert!(answer.to_string().contains("ThreadUnowned"), "{answer}");
    let archived = state.join("restart-record-3").client_messages();
    assert!(
        archived.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/archive")
        }),
        "{archived:?}"
    );
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
    assert_eq!(
        fixture.nexus.store.send_call(&first_call_key()).unwrap(),
        None
    );
    let refused =
        signal_flow::StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Spawn,
            native_launch_failure: signal_flow::NativeLaunchFailure::Unclassified,
        });
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_outcome(&setup.sender.launch_request_id.0)
            .unwrap(),
        Some(crate::store::LaunchOutcome::StartRejected(refused.clone()))
    );
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        signal_flow::SentBy::Unbound
    );
    // No second launch: a replayed Start answers the settlement, and a
    // second start reconciles nothing.
    assert_eq!(
        fixture.nexus.dispatch_serially(
            signal_flow::Query::Start(setup.sender.clone()),
            Origin::Unbound
        ),
        signal_flow::Response::StartRejected(refused)
    );
    fixture.nexus.reconcile_interrupted_launches();
    assert_eq!(
        fs::read_to_string(state.join("restart-invocations")).unwrap(),
        "3"
    );
    let calls = fs::read_to_string(state.join("calls")).unwrap_or_default();
    assert!(!calls.contains("agent start"), "{calls}");
    assert!(!calls.contains("workspace create"), "{calls}");
}

/// While a launch runs, its thread's calls are sent from its Address only
/// while no other flow holds that Address; otherwise, and once the launch
/// is settled, they are sent as Unbound.
#[test]
fn a_launch_whose_address_another_flow_holds_sends_as_unbound() {
    use crate::store::RecordsLaunchOutcome;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        signal_flow::SentBy::Sender(setup.sender.address.clone()),
        "an unsettled launch whose Address is free sends from it"
    );
    let owner = fixture.node();
    fixture.nexus.store.register_flow(owner.clone()).unwrap();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: setup.sender.address.clone(),
        state: State::Awake(owner.flow_id.clone()),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        signal_flow::SentBy::Unbound,
        "another live flow holds the Address"
    );
    let mut settled = NexusFixture::new();
    let setup = interrupted_launch(&mut settled);
    settled
        .nexus
        .store
        .record_launch_outcome(
            &setup.sender.launch_request_id.0,
            crate::store::LaunchOutcome::StartRejected(
                signal_flow::StartRejection::LaunchPersistenceRefused,
            ),
        )
        .unwrap();
    assert_eq!(
        settled.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        signal_flow::SentBy::Unbound,
        "a settled launch is no sender"
    );
}

/// A Herdr for the pane an interrupted launch created, w7:p1 on terminal
/// term-fresh of `fixture-session`: its snapshot shows the agents written to
/// `agents` beside that pane, its process information shows the pid written
/// to `pid` in the pane's foreground, and every call is logged to `calls`.
fn launch_pane_herdr(fixture: &NexusFixture) -> std::path::PathBuf {
    let directory = fixture.directory.path().join("launch-pane-herdr");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("agents"), "[]").unwrap();
    fs::write(directory.join("pid"), "1").unwrap();
    let script = r#"#!/bin/sh
LISTING
D='DIRECTORY'
printf '%s\n' "$*" >> "$D/calls"
case "$3 $4" in
  "api snapshot") printf '{"id":"cli:api:snapshot","result":{"snapshot":{"agents":%s,"panes":[{"pane_id":"w7:p1","terminal_id":"term-fresh"}],"protocol":20,"version":"0.8.2"},"type":"session_snapshot"}}\n' "$(cat "$D/agents")" ;;
  "pane process-info") printf '{"result":{"process_info":{"pane_id":"%s","foreground_processes":[{"pid":%s}]}}}\n' "$6" "$(cat "$D/pid")" ;;
  "pane close") printf '%s\n' '{"result":{}}' ;;
  *) exit 64 ;;
esac
"#
    .replace("LISTING", &fixture.session_listing())
    .replace("DIRECTORY", &directory.display().to_string());
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script);
    directory
}

/// The Codex agent Herdr shows at the launch's pane, reporting `session`
/// when one is given.
fn launch_pane_agent(session: Option<&str>) -> serde_json::Value {
    let mut agent = serde_json::json!({
        "agent":"codex","agent_status":"idle","name":"fixture-agent",
        "pane_id":"w7:p1","terminal_id":"term-fresh","workspace_id":"w7"
    });
    if let Some(session) = session {
        agent["agent_session"] = serde_json::json!({
            "source":"herdr:codex","agent":"codex","kind":"id","value":session
        });
    }
    agent
}

/// A process whose command line resumes `thread`, as a Codex TUI attached
/// to it runs; killed when dropped.
struct ResumingProcess(std::process::Child);

impl Drop for ResumingProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

trait SpawnsResumingProcess {
    fn resuming(thread: &str) -> ResumingProcess;
}

impl SpawnsResumingProcess for ResumingProcess {
    fn resuming(thread: &str) -> ResumingProcess {
        ResumingProcess(
            std::process::Command::new("/bin/sh")
                .args(["-c", "sleep 60; :", "resume", thread])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("resuming process"),
        )
    }
}

/// Teardown closes only a pane proven the launch's own. An interrupted
/// launch's pane, recorded for recovery, now runs a replacement session:
/// Herdr reports another session there, then reports none while the pane's
/// foreground process resumes another thread. Neither restart closes
/// anything; the launch is settled, its pane stays recorded and its pane
/// step Pending. Once the pane's foreground process resumes the launch's
/// own thread, the next start closes it and the cleanup completes.
#[test]
fn an_interrupted_launch_pane_running_a_replacement_session_is_never_closed() {
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::launch_cleanup::{
        CleanupStep, CompletesCleanup, KeepsLaunchCleanups, LaunchCleanup,
    };
    use crate::store::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane, RecordsLaunchOutcome};
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let launch = setup.sender.launch_request_id.clone();
    fixture
        .nexus
        .store
        .request_harness_start(&launch.0)
        .unwrap();
    let herdr = launch_pane_herdr(&fixture);
    let cleanup = |fixture: &NexusFixture| -> LaunchCleanup {
        fixture
            .nexus
            .store
            .launch_cleanup(&launch.0)
            .unwrap()
            .expect("the cleanup is recorded")
    };

    // A replacement session reported at the recorded pane and terminal.
    fs::write(
        herdr.join("agents"),
        serde_json::json!([launch_pane_agent(Some("replacement-thread"))]).to_string(),
    )
    .unwrap();
    fixture.nexus.reconcile_interrupted_launches();
    let calls = fs::read_to_string(herdr.join("calls")).unwrap_or_default();
    assert!(!calls.contains("pane close"), "{calls}");
    assert!(
        fixture
            .nexus
            .store
            .launch_outcome(&launch.0)
            .unwrap()
            .is_some(),
        "the launch is settled whatever its cleanup came to"
    );
    assert_eq!(cleanup(&fixture).thread_step, CleanupStep::Done);
    assert_eq!(cleanup(&fixture).pane_step, CleanupStep::Pending);
    assert!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch.0)
            .unwrap()
            .is_some()
    );

    // A replacement reporting no session, its foreground process resuming
    // another thread.
    let other = ResumingProcess::resuming("another-thread");
    fs::write(
        herdr.join("agents"),
        serde_json::json!([launch_pane_agent(None)]).to_string(),
    )
    .unwrap();
    fs::write(herdr.join("pid"), other.0.id().to_string()).unwrap();
    fixture.nexus.reconcile_interrupted_launches();
    let calls = fs::read_to_string(herdr.join("calls")).unwrap_or_default();
    assert!(calls.contains("pane process-info"), "{calls}");
    assert!(!calls.contains("pane close"), "{calls}");
    assert_eq!(cleanup(&fixture).pane_step, CleanupStep::Pending);
    drop(other);

    // The pane's foreground process resumes the launch's own thread.
    let own = ResumingProcess::resuming(FRESH_THREAD);
    fs::write(herdr.join("pid"), own.0.id().to_string()).unwrap();
    fixture.nexus.reconcile_interrupted_launches();
    let calls = fs::read_to_string(herdr.join("calls")).unwrap_or_default();
    assert!(calls.contains("pane close w7:p1"), "{calls}");
    assert!(cleanup(&fixture).is_complete());
    assert_eq!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch.0)
            .unwrap(),
        None
    );
    // The app-server was met once: the thread step was not taken again.
    assert_eq!(
        fs::read_to_string(setup.world.state.join("restart-invocations")).unwrap(),
        "3"
    );
}

/// The launch's settlement and its cleanup are apart. A restart whose
/// archive of the thread is refused, and whose Herdr cannot be read,
/// settles the launch but leaves both cleanup steps Pending; the next
/// start resumes them (the replayed call refused again, nothing queued),
/// the settlement unchanged, and only then is the cleanup complete.
#[test]
fn an_interrupted_launch_whose_cleanup_fails_is_settled_and_its_cleanup_resumed() {
    use super::send_tool::ReadsClientFrames;
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::launch_cleanup::{CleanupStep, CompletesCleanup, KeepsLaunchCleanups};
    use crate::store::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane, RecordsLaunchOutcome};
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();
    let launch = setup.sender.launch_request_id.clone();
    fixture
        .nexus
        .store
        .request_harness_start(&launch.0)
        .unwrap();
    fs::write(state.join("archive-refused"), "").unwrap();
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install("#!/bin/sh\nexit 64\n");

    fixture.nexus.reconcile_interrupted_launches();

    let settled = fixture.nexus.store.launch_outcome(&launch.0).unwrap();
    assert!(settled.is_some(), "the launch is settled");
    let cleanup = fixture
        .nexus
        .store
        .launch_cleanup(&launch.0)
        .unwrap()
        .unwrap();
    assert_eq!(cleanup.thread_step, CleanupStep::Pending, "archive refused");
    assert_eq!(cleanup.pane_step, CleanupStep::Pending, "Herdr unreadable");
    assert!(!cleanup.is_complete());
    assert!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch.0)
            .unwrap()
            .is_some()
    );

    // The next start: the archive succeeds, and Herdr's snapshot shows
    // neither the pane nor its binding.
    fs::remove_file(state.join("archive-refused")).unwrap();
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&format!(
        "#!/bin/sh\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{{\"id\":\"cli:api:snapshot\",\"result\":{{\"snapshot\":{{\"agents\":[],\"panes\":[],\"protocol\":20,\"version\":\"0.8.2\"}},\"type\":\"session_snapshot\"}}}}' ;;\n  *) exit 64 ;;\nesac\n",
        fixture.session_listing()
    ));
    fixture.nexus.reconcile_interrupted_launches();

    assert_eq!(
        fixture.nexus.store.launch_outcome(&launch.0).unwrap(),
        settled
    );
    let replayed = resumed_connection(&state, "restart-record-5");
    assert!(
        replayed
            .iter()
            .any(|message| message.to_string().contains("ThreadUnowned")),
        "{replayed:?}"
    );
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
    let archived = state.join("restart-record-6").client_messages();
    assert!(
        archived.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/archive")
        }),
        "{archived:?}"
    );
    let cleanup = fixture
        .nexus
        .store
        .launch_cleanup(&launch.0)
        .unwrap()
        .unwrap();
    assert!(cleanup.is_complete(), "{cleanup:?}");
    assert_eq!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch.0)
            .unwrap(),
        None
    );
    // Complete and settled: a further start does nothing.
    fixture.nexus.reconcile_interrupted_launches();
    assert_eq!(
        fs::read_to_string(state.join("restart-invocations")).unwrap(),
        "6"
    );
}

/// A send call the earlier Nexus committed with a logical identity before it
/// could answer, then the Nexus exited: the restarted Nexus answers the
/// replay from its record and queues nothing more.
#[test]
fn a_send_committed_before_its_reply_keeps_its_one_request_and_wake_across_a_restart() {
    use crate::generated::send_tool::{HandlesSendCall, ReadsToolCall, SendOutcome, ToolCall};
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();
    let recipient = setup.recipient.clone();
    let params = serde_json::json!({
        "threadId": FRESH_THREAD,
        "turnId": "turn-fresh",
        "callId": "call-first",
        "tool": "send",
        "arguments": {
            "target": {
                "aspect": "Mind",
                "topic": recipient.topic.0,
            "model": format!("{:?}", recipient.model)
            },
            "priority": "MiddleAbrupt",
            "kind": "Order",
            "body": FIRST_CALL_BODY
        }
    });
    let call = ToolCall::read_tool_call(params).expect("the call reads");
    let _held = fixture
        .nexus
        .waking
        .hold(&recipient)
        .expect("the interrupted launch holds delivery before its answer");
    // The earlier Nexus commits the call; its answer never leaves.
    let committed = fixture.nexus.handle_send_call(call);
    let SendOutcome::Accepted(accepted) = &committed else {
        panic!("the call is committed with a logical identity: {committed:?}")
    };
    assert_eq!(
        accepted.sent_by,
        signal_flow::SentBy::Sender(setup.sender.address.clone())
    );
    let logical_delivery_id = accepted.send_acceptance.logical_delivery_id.clone();
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);

    // The Nexus exits: the store as it lies on disk opens in a new one.
    let app_server = fixture
        .nexus
        .codex_endpoints
        .current()
        .stable
        .client_path
        .clone();
    let written = fixture.directory.path().join("flow.sema");
    let mut restarted = NexusFixture::with_store(|store| {
        fs::copy(&written, store).expect("the store as written");
    });
    restarted.nexus.codex_endpoints.edit().stable.client_path = app_server;

    restarted.nexus.reconcile_interrupted_launches();

    let replayed = resumed_connection(&state, "restart-record-2");
    let answer = replayed
        .iter()
        .find(|message| message.get("id") == Some(&serde_json::json!(FIRST_CALL_REQUEST)))
        .unwrap_or_else(|| panic!("the replayed call is answered: {replayed:?}"));
    assert_eq!(
        answer.pointer("/result/success"),
        Some(&serde_json::json!(true))
    );
    assert!(answer.to_string().contains("Accepted"), "{answer}");
    assert_eq!(
        restarted
            .nexus
            .store
            .send_call(&first_call_key())
            .unwrap()
            .map(|call| call.send_outcome),
        Some(committed.clone())
    );
    let target = stored(&restarted, Model::Sol);
    assert_eq!(target.queue.0.len(), 1, "exactly one enqueue");
    assert_eq!(
        target.queue.0[0].sent_by,
        SentBy::Sender(setup.sender.address.clone()),
        "its sender is kept"
    );
    assert_eq!(target.queue.0[0].logical_delivery_id, logical_delivery_id);
    assert_eq!(
        target.queue.0[0].message,
        Message::MiddleAbrupt(Request::Order(FIRST_CALL_BODY.into()))
    );
    assert!(
        restarted.nexus.wake_is_due(&target),
        "the target is due the wake the start's recover_wakes takes"
    );
}

/// The sender seat c1a0de, Awake at { Mind flow Sol }, holding a
/// lock on { Mind flow Astra }.
fn sender_holding_a_lock(fixture: &NexusFixture) -> Address {
    use crate::store::SeedsConfiguration;
    use crate::store::metaflow::MutatesMetaflows;
    let settings = crate::store::DefaultConfiguration {
        home: fixture.directory.path().join("home"),
        runtime_directory: fixture.directory.path().join("runtime"),
    }
    .configuration();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            meta_signal_flow::ConfigureNexus {
                source_root: flow_ethos::SourceRoot(
                    fixture.directory.path().to_string_lossy().into_owned(),
                ),
                first_codex_endpoint: settings.stable_codex,
                second_codex_endpoint: settings.next_codex,
                harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                    settings.message_nexus_path.clone(),
                )),
                message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                    "/fixture/bin/message-nexus".into(),
                )),

                curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                    "/fixture/curriculum.sock".into(),
                ),
                lease: flow_ethos::Lease(30),
                presentation_prompt_option: None,
                herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
            },
        ))
        .expect("Nexus configured");
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    let recipient = address(Model::Astra);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Astra, State::Asleep, vec![], vec![]));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let locked = fixture
        .nexus
        .store
        .lock_metaflow(
            signal_flow::Lock_Data {
                first_address: address(Model::Sol),
                second_address: recipient.clone(),
            },
            now,
        )
        .expect("lock written");
    assert!(
        matches!(locked, signal_flow::Response::Locked(_)),
        "{locked:?}"
    );
    assert!(
        fixture
            .nexus
            .store
            .stored_metaflow_lock(&recipient)
            .unwrap()
            .is_some()
    );
    recipient
}

/// A lock is retracted in the write that ends its sender's flow, however
/// it ended: Stopped, Exited or Retired. The recipient is never held by a
/// sender that has ended.
#[test]
fn a_terminal_sender_retracts_its_locks() {
    use crate::generated::operation::Record_Data;
    for end in [
        Record_Data::Stopped(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        Record_Data::Exited(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        Record_Data::Retired(flow_ethos::FlowId(CLAUDE_SEAT.into())),
    ] {
        let fixture = NexusFixture::new();
        let recipient = sender_holding_a_lock(&fixture);
        assert_eq!(
            fixture.nexus.perform(Operation::Record(end.clone())),
            Outcome::Recorded,
            "{end:?}"
        );
        assert!(
            fixture
                .nexus
                .store
                .stored_metaflow_lock(&recipient)
                .unwrap()
                .is_none(),
            "{end:?}: the ended sender's lock is retracted"
        );
        assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
    }
}

/// A flow Observe sees Gone, its pane absent from a readable Herdr, is
/// recorded Exited, and the lock it held as a sender goes with it.
#[test]
fn a_sender_observe_reports_gone_is_exited_and_its_lock_released() {
    use crate::observe_agent::ObservesAgent;
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(vec![], PromptFixture::Prompted("w1:p3"));
    let recipient = sender_holding_a_lock(&fixture);

    assert_eq!(
        fixture.nexus.agent_observation(CLAUDE_SEAT),
        signal_flow::Response::AgentObserved(signal_flow::AgentObservation {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            agent_state: signal_flow::AgentState::Gone,
        })
    );
    let node = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap().unwrap();
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Exited);
    assert!(
        fixture
            .nexus
            .store
            .stored_metaflow_lock(&recipient)
            .unwrap()
            .is_none()
    );
}

/// A Maintained metaflow whose flow exits with its pane gone is owed a
/// successor in the write that records the exit, and is due a wake with
/// nothing queued; an OnDemand one is owed none and is not due.
#[test]
fn a_maintained_metaflow_ended_by_an_exit_is_owed_a_successor() {
    use crate::generated::operation::Record_Data;
    use crate::store::successor::{OwesSuccessors, SuccessionCause};
    for maintained in [true, false] {
        let fixture = NexusFixture::new();
        claude_seat(&fixture, 40_000);
        fixture.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
            vec![],
            vec![],
        ));
        if maintained {
            maintain(&fixture, address(Model::Sol));
        }
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Exited(flow_ethos::FlowId(
                    CLAUDE_SEAT.into()
                )))),
            Outcome::Recorded
        );
        let intent = fixture
            .nexus
            .store
            .successor_intent(&address(Model::Sol))
            .unwrap();
        let asleep = stored(&fixture, Model::Sol);
        assert_eq!(asleep.state, State::Asleep);
        if maintained {
            let intent = intent.expect("a successor is owed");
            assert_eq!(intent.predecessor, CLAUDE_SEAT);
            assert_eq!(intent.cause, SuccessionCause::Exit);
            assert!(
                fixture.nexus.wake_is_due(&asleep),
                "due with nothing queued"
            );
        } else {
            assert_eq!(intent, None);
            assert!(!fixture.nexus.wake_is_due(&asleep));
        }
    }
}

/// A Stop the owner asks for is not an unexpected exit: a Maintained
/// metaflow so stopped is owed no successor.
#[test]
fn a_maintained_metaflow_stopped_deliberately_is_owed_no_successor() {
    use crate::generated::operation::Record_Data;
    use crate::store::successor::OwesSuccessors;
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));
    for end in [
        Record_Data::Stopped(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        Record_Data::Retired(flow_ethos::FlowId(CLAUDE_SEAT.into())),
    ] {
        let _ = fixture.nexus.perform(Operation::Record(end));
    }
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&address(Model::Sol))
            .unwrap(),
        None
    );
}

/// A launch's Remembering resolves to flows: the metaflow's lineage, its
/// past then the flow it is Awake as, to the depth, oldest first, or the
/// flows named one by one.
#[test]
fn a_launch_remembers_its_metaflow_s_last_flows_to_its_depth_or_those_it_names() {
    use crate::store::metaflow::ResolvesRemembering;
    use signal_flow::Remembering;
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("c3c3c3".into())),
        vec!["a1a1a1", "b2b2b2"],
        vec![],
    ));
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = address(Model::Sol);
    profile.launch_request_id = signal_flow::LaunchRequestId("remember-request".into());
    for (remembering, expected) in [
        (
            Remembering::Predecessors(signal_flow::RememberingDepth(0)),
            vec![],
        ),
        (
            Remembering::Predecessors(signal_flow::RememberingDepth(2)),
            vec!["b2b2b2", "c3c3c3"],
        ),
        (
            Remembering::Predecessors(signal_flow::RememberingDepth(5)),
            vec!["a1a1a1", "b2b2b2", "c3c3c3"],
        ),
        (
            Remembering::Custom(vec![flow_ethos::FlowId("36b71b".into())]),
            vec!["36b71b"],
        ),
    ] {
        profile.remembering = remembering.clone();
        assert_eq!(
            fixture.nexus.store.remembered_flows(&profile).unwrap(),
            expected
                .into_iter()
                .map(|flow| flow_ethos::FlowId(flow.into()))
                .collect::<Vec<_>>(),
            "{remembering:?}"
        );
    }
}

/// At its start the Nexus takes up every successor owed: one a flow already
/// took the Address for is forgotten, and an Asleep metaflow owed one is due
/// a wake.
#[test]
fn a_nexus_start_forgets_a_successor_already_given_and_wakes_one_owed() {
    use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec!["abc123"],
        vec![],
    ));
    fixture
        .nexus
        .store
        .owe_successor(SuccessorIntent {
            address: address(Model::Sol),
            predecessor: "abc123".into(),
            cause: SuccessionCause::Exit,
            launch_request_id: String::new(),
        })
        .unwrap();
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Astra,
        State::Asleep,
        vec!["def456"],
        vec![],
    ));
    fixture
        .nexus
        .store
        .owe_successor(SuccessorIntent {
            address: address(Model::Astra),
            predecessor: "def456".into(),
            cause: SuccessionCause::Exit,
            launch_request_id: String::new(),
        })
        .unwrap();

    fixture.nexus.recover_wakes();

    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&address(Model::Sol))
            .unwrap(),
        None,
        "the successor Awake as another flow was given"
    );
    let owed = stored(&fixture, Model::Astra);
    assert!(
        fixture.nexus.wake_is_due(&owed),
        "the Asleep metaflow is still owed its successor: {owed:?}"
    );
}

/// Nothing starts first: a Start of { Mind flow Astra } whose declared
/// { Mind flow Sol } sleeps is the Astra's launch alone, whatever it comes
/// to. The Sol metaflow stays Asleep as it was, and no seat is started for
/// it.
#[test]
fn a_start_starts_no_other_models_seat_first() {
    use crate::Dispatches;
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    let world = fixture.fresh_claude_world();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
            meta_signal_flow::ConfigureMetaflow {
                root_option: None,
                address: address(Model::Sol),
                system_modules: signal_flow::SystemModules(vec![
                    signal_flow::ModuleIdentity("spirit-core".into()),
                    signal_flow::ModuleIdentity("intent-flow".into()),
                ]),
                user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                    "operation-main-flow".into(),
                )]),
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                continuity: meta_signal_flow::Continuity::OnDemand,
                purpose: meta_signal_flow::Purpose(String::new()),
                records: meta_signal_flow::Records(vec![]),
            },
        ]))
        .expect("the Sol metaflow is declared");
    let mut primary = crate::composition::tests::fixture_profile();
    primary.launch_request_id = signal_flow::LaunchRequestId("primary-request".into());
    primary.address = address(Model::Astra);
    primary.flow_id_option = None;
    primary.remembering = signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0));

    let response = fixture.nexus.dispatch(signal_flow::Query::Start(primary));

    assert!(
        !matches!(response, signal_flow::Response::Started(_)),
        "the Astra's own launch, not a Sol's, answers: {response:?}"
    );
    assert_eq!(
        stored(&fixture, Model::Sol),
        metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]),
        "the Sol metaflow is left as it was"
    );
    assert_eq!(
        world.claude_starts(),
        0,
        "no seat starts for the Sol: {}",
        world.calls()
    );
}

/// A Claude flow's own SessionEnd, reported by its hook, is its exit as an
/// event: it is recorded Exited without anything looking at its pane, and
/// a Maintained metaflow is owed its successor. A session end naming another
/// session changes nothing.
#[test]
fn a_reported_session_end_exits_the_flow_and_owes_a_maintained_successor() {
    use crate::store::NamesLiveFlow;
    use crate::store::successor::OwesSuccessors;
    for (session, ends) in [("another-session", false), (CLAUDE_SESSION, true)] {
        let fixture = NexusFixture::new();
        claude_seat(&fixture, 40_000);
        fixture.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
            vec![],
            vec![],
        ));
        maintain(&fixture, address(Model::Sol));

        fixture.nexus.after_report(
            &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
            &signal_flow::Event::Ended(Some(signal_flow::NativeSessionId(session.into()))),
        );

        let node = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap().unwrap();
        let intent = fixture
            .nexus
            .store
            .successor_intent(&address(Model::Sol))
            .unwrap();
        if ends {
            assert_eq!(node.flow_lifecycle, FlowLifecycle::Exited);
            assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
            assert_eq!(
                intent.expect("a successor is owed").predecessor,
                CLAUDE_SEAT
            );
        } else {
            assert!(node.flow_lifecycle.is_live(), "{:?}", node.flow_lifecycle);
            assert_eq!(intent, None);
        }
    }
}

/// An owed successor is taken up by the start sequence the daemon runs
/// (main.rs): interrupted launches reconciled, then, while the sockets
/// serve, the owed work taken up.
#[test]
fn start_sequence_takes_up_an_owed_successor() {
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    maintain(&fixture, address(Model::Sol));
    fixture
        .nexus
        .store
        .owe_successor(SuccessorIntent {
            address: address(Model::Sol),
            predecessor: "abc123".into(),
            cause: SuccessionCause::Exit,
            launch_request_id: String::new(),
        })
        .unwrap();
    let world = fixture.fresh_claude_world();

    fixture.nexus.reconcile_interrupted_launches();
    fixture.nexus.recover_wakes();

    let started = stored(&fixture, Model::Sol);
    assert!(
        matches!(started.state, State::Awake(_)),
        "{started:?}; {}",
        world.calls()
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
}

/// The owed successor is taken up by recover_wakes, the start pass that
/// takes up owed work.
#[test]
fn recover_wakes_takes_up_an_owed_successor() {
    use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    maintain(&fixture, address(Model::Sol));
    fixture
        .nexus
        .store
        .owe_successor(SuccessorIntent {
            address: address(Model::Sol),
            predecessor: "abc123".into(),
            cause: SuccessionCause::Exit,
            launch_request_id: String::new(),
        })
        .unwrap();
    let world = fixture.fresh_claude_world();

    fixture.nexus.recover_wakes();

    let started = stored(&fixture, Model::Sol);
    assert!(
        matches!(started.state, State::Awake(_)),
        "{started:?}; {}",
        world.calls()
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
}

/// A Maintained metaflow whose flow's own session end is reported gets its
/// successor started, not only owed.
#[test]
fn reported_session_end_starts_a_maintained_successor() {
    use crate::store::successor::OwesSuccessors;
    let mut fixture = NexusFixture::new();
    let seat = launched_claude_ended_by(&fixture, |_| true);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    maintain(&fixture, address(Model::Sol));
    let world = fixture.fresh_claude_world();

    fixture.nexus.after_report(
        &signal_flow::FlowId("abc123".to_owned()),
        &signal_flow::Event::Ended(Some(signal_flow::NativeSessionId(seat.session.clone()))),
    );

    let woken = stored(&fixture, Model::Sol);
    let State::Awake(flow_id) = &woken.state else {
        panic!("the successor starts: {woken:?}; {}", world.calls());
    };
    assert_ne!(flow_id.0, "abc123", "a fresh flow");
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&address(Model::Sol))
            .unwrap(),
        None,
        "the owed successor is given"
    );
}

/// A metaflow left Awake as a flow that has ended is put Asleep, and the
/// ended flow's locks as a sender are retracted in that write.
#[test]
fn a_metaflow_put_asleep_for_an_ended_flow_retracts_its_sender_locks() {
    use crate::store::metaflow::SleepsMetaflows;
    let fixture = NexusFixture::new();
    let recipient = sender_holding_a_lock(&fixture);
    assert!(
        fixture
            .nexus
            .store
            .sleep_metaflow(&address(Model::Sol), CLAUDE_SEAT)
            .unwrap()
    );
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
    assert!(
        fixture
            .nexus
            .store
            .stored_metaflow_lock(&recipient)
            .unwrap()
            .is_none(),
        "the ended sender's lock is retracted"
    );
}

/// Hook A is transient; the ended answer B belongs to the exact input receipt
/// in the bound native transcript and is delivered once.
#[test]
fn a_simplified_answer_uses_bound_transcript_b_and_never_hook_a() {
    use crate::store::logical_delivery::SimplifiedReplyBinding;
    use crate::waking::{QueuesSimplifiedAnswers, SimplifiedAnswer};
    let fixture = NexusFixture::new();
    let launched = active_simplified_claude(&fixture);
    let at = launched.profile.address.clone();
    let sender = address(Model::Astra);
    for (address, state) in [
        (
            at.clone(),
            State::Awake(flow_ethos::FlowId("abc123".into())),
        ),
        (sender.clone(), State::Asleep),
    ] {
        fixture.nexus.store.seed_metaflow(Metaflow {
            address,
            state,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
    }
    let signal_flow::PrivateSendResponse::SendAccepted(input) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &at,
            SentBy::Sender(sender.clone()),
            Delivery::Wake,
            Message::MiddleAbrupt(Request::Order("input".into())),
            None,
        )
        .unwrap()
    else {
        panic!("input accepted")
    };
    fixture
        .nexus
        .store
        .bind_simplified_reply(SimplifiedReplyBinding {
            flow_id: "abc123".into(),
            input_delivery_id: input.logical_delivery_id.0.clone(),
            source: at.clone(),
            recipient: sender.clone(),
            completed: false,
        })
        .unwrap();
    let startup = fixture.nexus.store.metaflow(&at).unwrap().unwrap().queue.0;
    assert!(
        fixture
            .nexus
            .store
            .settle_wake_startup_receipts(
                &at,
                "abc123",
                &startup,
                Some(&signal_flow::NativeReceipt {
                    harness_kind: HarnessKind::Claude,
                    native_session_id: signal_flow::NativeSessionId(launched.session.clone()),
                    native_prompt_id: signal_flow::NativePromptId("bound-prompt".into()),
                })
            )
            .unwrap()
    );
    let transcript = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary")
        .join(format!("{}.jsonl", launched.session));
    let hook_a = "HOOK-A-assistant-only-marker";
    let transcript_b = "TRANSCRIPT-B-intended-answer";
    // The Stop arrives first. Neither its content nor an unbound old answer is used.
    let stop = signal_flow::Event::Stopped(signal_flow::NativeStop {
        native_session_id_option: Some(signal_flow::NativeSessionId(launched.session.clone())),
        transcript_path_option: Some(signal_flow::TranscriptPath(
            transcript.display().to_string(),
        )),
        last_assistant_message_option: signal_flow::LastAssistantMessageOption(Some(hook_a.into())),
        background_work: signal_flow::BackgroundWork(vec![]),
    });
    assert_eq!(
        fixture.nexus.report(signal_flow::Report_Data {
            flow_id: flow_ethos::FlowId("abc123".into()),
            process_option: None,
            event: stop,
        }),
        signal_flow::Response::Reported
    );
    assert!(matches!(
        fixture.nexus.queue_simplified_answer("abc123"),
        SimplifiedAnswer::TurnUnanswered { .. }
    ));
    let rows = [
        serde_json::json!({"type":"user","sessionId":launched.session,"uuid":"bound-prompt","message":{"content":"input"}}),
        serde_json::json!({"type":"assistant","sessionId":launched.session,"uuid":"answer",
            "message":{"stop_reason":"end_turn","content":[{"type":"text","text":transcript_b}]}}),
    ];
    fs::write(
        &transcript,
        rows.iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    assert!(matches!(
        fixture.nexus.queue_simplified_answer("abc123"),
        SimplifiedAnswer::Queued { .. }
    ));
    assert_eq!(
        fixture.nexus.queue_simplified_answer("abc123"),
        SimplifiedAnswer::NoneOwed
    );
    let queued = fixture
        .nexus
        .store
        .metaflow(&sender)
        .unwrap()
        .unwrap()
        .queue
        .0;
    assert_eq!(queued.len(), 1);
    assert_eq!(
        queued[0].message,
        Message::Soft(Request::Result(flow_ethos::ResultPayload::Historical(
            transcript_b.into()
        )))
    );
    use datom_codec::Datomizable;
    use protos::{Compactable, Protosizable};
    let metadata = fixture
        .nexus
        .read_events(flow_ethos::FlowId("abc123".into()))
        .datomize(vec![])
        .protosize()
        .compact();
    assert!(!metadata.contains(hook_a));
    assert!(
        !metadata.contains(transcript_b),
        "ReadEvents remains metadata-only"
    );
    // A regular seat has no simplified binding and receives no automatic answer.
    assert_eq!(
        fixture.nexus.queue_simplified_answer("regular-seat"),
        SimplifiedAnswer::NoneOwed
    );
}

/// An Awake Codex seat 908786 at Sol with `queue`, declared `continuity`
/// with a successor that composes; its thread named.
fn codex_rest_seat(
    fixture: &NexusFixture,
    continuity: Option<meta_signal_flow::Continuity>,
    queue: Vec<signal_flow::QueueEntry>,
) -> String {
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    if let Some(continuity) = continuity {
        super::refresh_and_compaction::declare_seat(
            fixture,
            continuity,
            vec!["spirit-core".into(), "intent-flow".into()],
        );
    }
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId("908786".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(queue),
    });
    fixture.node().session_id.0
}

/// The Codex thread of the working seat abc123 in the fresh world.
pub(super) const WORKING_CODEX_THREAD: &str = "01a0b22c-c0de-7000-8000-0000000abc12";

/// The Maintained Mind flow Sol, Awake as the Codex flow abc123 on the
/// thread `WORKING_CODEX_THREAD` in pane w1:p3 of `fixture-session`, which
/// Herdr shows until it is closed; its refresh's successor is started in
/// the fresh world, as the Claude model the Address's model is configured
/// to run.
pub(super) fn codex_seat_in_fresh_world(fixture: &mut NexusFixture) -> FreshWorld {
    fixture.declare_launch_session("fixture-session");
    let mut node = fixture.node();
    node.flow_id = flow_ethos::FlowId("abc123".into());
    node.session_id = signal_flow::SessionId(WORKING_CODEX_THREAD.into());
    node.herdr_route_selection = HerdrRouteSelection::Available(HerdrRoute {
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        herdr_agent_name: signal_flow::HerdrAgentName("codex-abc123".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-old".into()),
    });
    node.origin = Origin::Unbound;
    node.flow_lifecycle = FlowLifecycle::Active;
    fixture
        .nexus
        .store
        .register_flow(node)
        .expect("the Codex seat registered");
    fs::write(
        fixture.directory.path().join("flows/.abc123.flow-id"),
        format!(
            "version=1\nharness=codex\nidentity={}\nalias=abc123\n",
            WORKING_CODEX_THREAD.replace('-', "")
        ),
    )
    .unwrap();
    let world = fixture.fresh_claude_world();
    fs::write(world.state.join("restart-world"), "").unwrap();
    fs::write(world.state.join("old-pane"), "w1:p3").unwrap();
    fs::write(
        world.state.join("old-agent"),
        serde_json::json!({
            "name": "codex-abc123",
            "agent": "codex",
            "agent_status": "working",
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "pane_id": "w1:p3",
            "terminal_id": "term-old",
            "agent_session": {
                "source": "herdr:codex", "agent": "codex",
                "kind": "id", "value": WORKING_CODEX_THREAD,
            }
        })
        .to_string(),
    )
    .unwrap();
    maintain(fixture, address(Model::Sol));
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId("abc123".into())),
        vec![],
        vec![],
    ));
    world
}

/// Test 1 (Part A): a Refresh ordered while a Codex turn runs is carried
/// out at the thread's rest with nothing queued.
#[test]
fn a_codex_ordered_refresh_with_nothing_queued_is_carried_out_at_its_rest() {
    use crate::Dispatches;
    use crate::codex_turns::{SettlesRestingCodexSeats, TurnEvent};
    use signal_flow::{Query, Response};
    let fixture = NexusFixture::new();
    let thread = codex_rest_seat(
        &fixture,
        Some(meta_signal_flow::Continuity::OnDemand),
        vec![],
    );
    assert_eq!(
        fixture
            .nexus
            .codex_events(&thread, vec![TurnEvent::Started("turn-1".into())]),
        None,
        "a turn that starts is no rest"
    );
    assert!(matches!(
        fixture.nexus.dispatch(Query::Refresh(address(Model::Sol))),
        Response::RefreshQueued(_)
    ));
    let at_rest = fixture
        .nexus
        .codex_events(&thread, vec![TurnEvent::Completed("turn-1".into())]);
    assert!(
        matches!(&at_rest, Some(AtStop::Refreshed(at)) if *at == address(Model::Sol)),
        "{at_rest:?}"
    );
    assert!(closes(&fixture), "the ordered refresh closes the seat");
}

/// Test 1 (Part A), Maintained: a Refresh ordered while a Maintained
/// Codex seat's turn runs is answered RefreshQueued and starts nothing; at
/// the thread's rest, with nothing queued, the successor is started and
/// confirmed first, the Address receives as it, the predecessor is
/// recorded Stopped and its reap owed to its worker, and the order is
/// spent. The rest answers the reap pending, never a refusal.
#[test]
fn a_codex_ordered_refresh_of_a_maintained_seat_starts_its_successor_at_its_rest() {
    use crate::codex_turns::{SettlesRestingCodexSeats, TurnEvent};
    use crate::store::ordered_replacement::OrdersReplacements;
    use crate::store::pending_reap::RecordsPendingReaps;
    use crate::{Dispatches, store::ReadsFlowRows};
    use signal_flow::{Query, Response};
    let mut fixture = NexusFixture::new();
    let world = codex_seat_in_fresh_world(&mut fixture);
    let at = address(Model::Sol);
    assert_eq!(
        fixture.nexus.codex_events(
            WORKING_CODEX_THREAD,
            vec![TurnEvent::Started("turn-1".into())]
        ),
        None,
        "a turn that starts is no rest"
    );
    let ordered = fixture.nexus.dispatch(Query::Refresh(at.clone()));
    assert!(matches!(ordered, Response::RefreshQueued(_)), "{ordered:?}");
    assert_eq!(world.claude_starts(), 0, "nothing starts while it works");

    let at_rest = fixture.nexus.codex_events(
        WORKING_CODEX_THREAD,
        vec![TurnEvent::Completed("turn-1".into())],
    );
    let Some(AtStop::ReapPending(pending)) = &at_rest else {
        panic!("{at_rest:?}; Herdr calls: {}", world.calls());
    };
    assert_eq!(pending.address, at);
    assert_eq!(pending.predecessor.0.0, "abc123");
    assert_eq!(pending.reap_standing, signal_flow::ReapStanding::Owed);
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let successor = pending.launched.flow_id.clone();
    assert_ne!(successor.0, "abc123");
    let metaflow = stored(&fixture, Model::Sol);
    assert_eq!(metaflow.state, State::Awake(successor));
    assert!(metaflow.queue.0.is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node("abc123")
            .unwrap()
            .expect("the predecessor's row is kept")
            .flow_lifecycle,
        FlowLifecycle::Stopped
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .pending_reap("abc123")
            .unwrap()
            .map(|row| row.reap_standing),
        Some(signal_flow::ReapStanding::Owed)
    );
    assert_eq!(
        fixture.nexus.store.ordered_replacement(&at).unwrap(),
        None,
        "the order is spent"
    );
    assert!(
        !world.calls().contains("pane close w1:p3"),
        "the predecessor's close is its worker's, outside the rest: {}",
        world.calls()
    );
}

/// Test 2 (Part A): a Codex seat past its threshold is refreshed at its
/// rest with nothing queued.
#[test]
fn a_codex_seat_past_its_threshold_is_refreshed_at_its_rest_with_nothing_queued() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    use crate::store::SeedsRole;
    use crate::store::context::RecordsContextSize;
    let fixture = NexusFixture::new();
    let thread = codex_rest_seat(
        &fixture,
        Some(meta_signal_flow::Continuity::OnDemand),
        vec![],
    );
    fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: flow_ethos::FlowId("908786".into()),
        aspect: signal_flow::Aspect::Mind,
        model_name: flow_ethos::ModelName("gpt-sol".into()),
    });
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
            meta_signal_flow::ConfigureThreshold {
                model_name: flow_ethos::ModelName("gpt-sol".into()),
                refresh_threshold: meta_signal_flow::RefreshThreshold(100),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .record_context_size("908786", 500)
        .unwrap();
    let at_rest = fixture
        .nexus
        .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]);
    assert!(
        matches!(&at_rest, Some(AtStop::Refreshed(at)) if *at == address(Model::Sol)),
        "{at_rest:?}"
    );
}

/// Test 3 (Part A), the status half: a status seen twice is no second
/// rest: a second Status(Idle) while resting settles nothing.
#[test]
fn a_second_idle_status_of_a_resting_codex_seat_settles_nothing() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    let fixture = NexusFixture::new();
    let thread = codex_rest_seat(&fixture, None, vec![]);
    maintain(&fixture, address(Model::Sol));
    assert_eq!(
        fixture
            .nexus
            .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]),
        Some(AtStop::Idle)
    );
    assert_eq!(
        fixture
            .nexus
            .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]),
        None,
        "a second Status(Idle) while resting is no new rest"
    );
}

/// Test 3 (Part A), the warning half: an OnDemand Codex seat whose context
/// reaches its warning share below its threshold is sent its handover
/// warning at a rest, typed into its pane, and is not refreshed at that
/// rest; at the rest after the turn the warning opened it is refreshed,
/// though it wrote no handover and never passed its threshold, and a
/// second Status(Idle) then settles nothing.
#[test]
fn a_warned_codex_seat_is_refreshed_at_the_rest_after_its_warning() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    use crate::store::SeedsRole;
    use crate::store::handover_reminders::SchedulesHandoverReminders;
    const WORDS: &str = "Write your handover now.";
    let mut fixture = NexusFixture::new();
    let thread = codex_rest_seat(
        &fixture,
        Some(meta_signal_flow::Continuity::OnDemand),
        vec![],
    );
    fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: flow_ethos::FlowId("908786".into()),
        aspect: signal_flow::Aspect::Mind,
        model_name: flow_ethos::ModelName("gpt-sol".into()),
    });
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
            meta_signal_flow::ConfigureThreshold {
                model_name: flow_ethos::ModelName("gpt-sol".into()),
                refresh_threshold: meta_signal_flow::RefreshThreshold(200_000),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::HandoverPolicies(
            vec![meta_signal_flow::HandoverPolicy {
                model_name: flow_ethos::ModelName("gpt-sol".into()),
                warning_share_left: meta_signal_flow::WarningShareLeft(15),
                reminder_text: meta_signal_flow::ReminderText(WORDS.into()),
                every_minutes: meta_signal_flow::EveryMinutes(5),
            }],
        ))
        .unwrap();
    // Inside the warning share (15 percent before 200,000), below the
    // threshold.
    fixture
        .nexus
        .store
        .record_context_size("908786", 175_000)
        .unwrap();
    use super::send_tool::RunsFakeAppServer;
    let codex = super::send_tool::accepting_codex_endpoint(&mut fixture, "gpt-sol", &thread);

    // The rest that opens the warning: the warning is typed and the seat,
    // handed it, is in the turn it opens; it is not refreshed.
    let warned_at = fixture
        .nexus
        .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]);
    assert_eq!(warned_at, Some(AtStop::Kept(KeptAtStop::InTurn)));
    let reminder = fixture
        .nexus
        .store
        .handover_reminder(&address(Model::Sol), 0)
        .unwrap()
        .expect("the warning opened");
    assert_eq!(reminder.sent, 1);
    let typed = fixture.typed().expect("the warning reached the pane");
    assert!(typed.contains(WORDS), "{typed}");
    assert!(typed.contains("flows/908786/handover.md"), "{typed}");
    assert!(!closes(&fixture), "the warned seat is not refreshed yet");
    assert!(
        codex.queued_turn_texts().is_empty(),
        "the warning is typed, never a native turn"
    );

    // The warning's turn runs and ends: that rest refreshes the seat.
    let refreshed_at = fixture.nexus.codex_events(
        &thread,
        vec![
            TurnEvent::Started("turn-warned".into()),
            TurnEvent::Completed("turn-warned".into()),
        ],
    );
    assert_eq!(refreshed_at, Some(AtStop::Refreshed(address(Model::Sol))));
    assert!(closes(&fixture), "the warned seat is refreshed");
    let refreshed = stored(&fixture, Model::Sol);
    assert_eq!(refreshed.state, State::Asleep);
    assert_eq!(
        refreshed.past,
        flow_ethos::Past(vec![flow_ethos::FlowId("908786".into())])
    );
    assert_eq!(
        fixture
            .nexus
            .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]),
        None,
        "a second Status(Idle) of the refreshed seat settles nothing"
    );
}

/// Test 6 (Part A): a resting Codex seat with work queued is handed the
/// oldest request at its rest: the whole queue is typed into its pane,
/// oldest first, in one hand-over through the app-server's anchor, the
/// queue left empty and no native turn started.
#[test]
fn a_resting_codex_seat_with_work_queued_is_handed_the_oldest_request() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    use crate::store::SeedsRole;
    let mut fixture = NexusFixture::new();
    let queued = |id: &str, text: &str| signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId(format!("codex-rest-{id}")),
        request_id: flow_ethos::RequestId(format!("cr{id}")),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Queue,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(text.into())),
    };
    let oldest = queued("00", "Oldest.");
    let newer = queued("01", "Newer.");
    let thread = codex_rest_seat(&fixture, None, vec![oldest.clone(), newer.clone()]);
    maintain(&fixture, address(Model::Sol));
    // The seat's model selects the endpoint whose app-server anchors the
    // hand-over (`thread/turns/list`) before anything is typed.
    fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: flow_ethos::FlowId("908786".into()),
        aspect: signal_flow::Aspect::Mind,
        model_name: flow_ethos::ModelName("gpt-sol".into()),
    });
    use super::send_tool::RunsFakeAppServer;
    let codex = super::send_tool::accepting_codex_endpoint(&mut fixture, "gpt-sol", &thread);
    assert!(fixture.typed().is_none(), "nothing typed before the rest");

    let at_rest = fixture
        .nexus
        .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]);
    assert_eq!(at_rest, Some(AtStop::Drained(oldest)), "{at_rest:?}");
    let typed = fixture.typed().expect("the queue is typed into the pane");
    assert!(
        typed.find("Oldest.").is_some() && typed.find("Oldest.") < typed.find("Newer."),
        "oldest first: {typed}"
    );
    assert!(
        stored(&fixture, Model::Sol).queue.0.is_empty(),
        "the queue is handed over"
    );
    assert!(
        codex.queued_turn_texts().is_empty(),
        "pane delivery never starts a Codex turn"
    );
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
}

/// Test 8 (Part A): an OnDemand Codex seat whose successor cannot be placed
/// stays Awake, its sleep alone declined, and is told nothing.
#[test]
fn an_on_demand_codex_seat_whose_successor_cannot_compose_stays_awake_untold() {
    use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
    let fixture = NexusFixture::new();
    let thread = codex_rest_seat(&fixture, None, vec![]);
    let at_rest = fixture
        .nexus
        .codex_events(&thread, vec![TurnEvent::Status(ThreadStatus::Idle)]);
    assert_eq!(at_rest, Some(AtStop::Kept(KeptAtStop::SleepUnplaceable)));
    assert!(!closes(&fixture), "the seat stays Awake");
    assert!(fixture.typed().is_none(), "no successor-refused notice");
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
}

/// Test 9 (ruling 1): the first authoritative Resumed(Idle) after a Nexus
/// start settles a Codex seat once; a re-subscription's Idle with no turn
/// seen in between settles nothing; a turn seen running then a Resumed(Idle)
/// does.
#[test]
fn the_first_resumed_idle_after_a_start_settles_a_codex_seat_once() {
    use crate::codex_turns::{CodexTurns, ThreadStatus, TracksCodexTurns, TurnEvent};
    let turns = CodexTurns::default();
    let thread = "resumed-thread";
    assert!(
        turns
            .observe(thread, TurnEvent::Resumed(ThreadStatus::Idle))
            .rested
    );
    assert!(
        !turns
            .observe(thread, TurnEvent::Resumed(ThreadStatus::Idle))
            .rested
    );
    turns.observe(thread, TurnEvent::Resumed(ThreadStatus::Unavailable));
    assert!(
        !turns
            .observe(thread, TurnEvent::Resumed(ThreadStatus::Idle))
            .rested,
        "a reconnect's Idle after Unknown is no new rest"
    );
    turns.observe(thread, TurnEvent::Started("t1".into()));
    assert!(
        turns
            .observe(thread, TurnEvent::Completed("t1".into()))
            .rested
    );
    turns.observe(thread, TurnEvent::Resumed(ThreadStatus::Active));
    assert!(
        turns
            .observe(thread, TurnEvent::Resumed(ThreadStatus::Idle))
            .rested,
        "a turn seen running, then Idle, is a rest"
    );
    // Through the Nexus: the first Resumed(Idle) settles the seat once.
    use crate::codex_turns::SettlesRestingCodexSeats;
    let fixture = NexusFixture::new();
    let seat = codex_rest_seat(&fixture, None, vec![]);
    maintain(&fixture, address(Model::Sol));
    assert_eq!(
        fixture
            .nexus
            .codex_events(&seat, vec![TurnEvent::Resumed(ThreadStatus::Idle)]),
        Some(AtStop::Idle)
    );
    assert_eq!(
        fixture
            .nexus
            .codex_events(&seat, vec![TurnEvent::Resumed(ThreadStatus::Idle)]),
        None
    );
}
