//! The send tool against a fake Codex app-server: each connection Flow
//! opens is one run of a script that answers the WebSocket upgrade, prints
//! the frames prepared for that connection, records what the client wrote,
//! and stays open or closes. A responder's every connection is preceded by
//! the send-tool probe, so connection `n` of a responder is run `2n` of its
//! script, and run `2n - 1` is the probe.

use super::{ControlsHerdrSnapshot, NexusFixture};
use crate::codex::{CodexAdapter, ResponderSignal, SelectsCodexEndpoint};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::send_tool::{
    HandlesSendCall, ReadsToolCall, SendCall, SendOutcome, SendRefusal, ToolCall, ToolCallKey,
};
use crate::send_tool::{DescribesSendInvocation, RespondsToSendCalls, SendTool, ServesSendTool};
use crate::sending::{MAX_SEND_PAYLOAD_BYTES, MeasuresSendPayload};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::send_call::KeepsSendCalls;
use crate::store::{AnnouncesLaunchChanges, RecordsFlowLifecycle, RegistersFlowIdentity};
use crate::{HoldsStopRequest, generated::send_tool::DeclaresSendTool};
use flow_ethos::State;
use meta_signal_flow::QueueEvidence;
use signal_flow::{
    Address, Aspect, Caller, Delivery, FlowNode, HerdrRoute, HerdrRouteSelection, Layer, Message,
    Metaflow, PowerLevel, QueueEntry, Request, SentBy,
};
use std::{
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};

const THREAD_A: &str = "01a0b22c-0000-7000-8000-00000000000a";
const THREAD_B: &str = "01a0b22c-0000-7000-8000-00000000000b";
const TURN: &str = "01a0b22c-0000-7000-8000-0000000000f0";

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestRequest {
    sent_by: SentBy,
    request: Request,
}

fn queue_entry(id: impl Into<String>, queued: TestRequest) -> QueueEntry {
    QueueEntry {
        logical_delivery_id: id.into(),
        sent_by: queued.sent_by,
        delivery: Delivery::Wake,
        message: Message::Soft(queued.request),
    }
}

fn address(topic: &str, layer: Layer) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: topic.into(),
        layer,
    }
}

fn sender_a() -> Address {
    address("alpha", Layer::Tertiary)
}

fn sender_b() -> Address {
    address("beta", Layer::Tertiary)
}

fn target() -> Address {
    address("gamma", Layer::Secondary)
}

/// One server-to-client WebSocket text frame, as `printf %b` octal escapes.
fn frame(json: &serde_json::Value) -> String {
    let json = json.to_string();
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

/// The app-server's item/tool/call server request, sending an Order to
/// the target.
fn tool_call_request(request_id: u64, thread: &str, call: &str, body: &str) -> serde_json::Value {
    send_request(request_id, thread, call, &target(), "Order", body)
}

/// The app-server's item/tool/call server request, sending `kind` to `to`.
fn send_request(
    request_id: u64,
    thread: &str,
    call: &str,
    to: &Address,
    kind: &str,
    body: &str,
) -> serde_json::Value {
    serde_json::json!({
        "method": "item/tool/call",
        "id": request_id,
        "params": {
            "threadId": thread,
            "turnId": TURN,
            "callId": call,
            "namespace": null,
            "tool": "send",
            "arguments": {
                "target": {
                    "aspect": format!("{:?}", to.aspect),
                    "topic": to.topic,
                    "layer": format!("{:?}", to.layer)
                },
            "delivery": "Wake",
            "priority": "Soft",
            "kind": kind,
                "body": body
            }
        }
    })
}

fn tool_call(thread: &str, call: &str, body: &str) -> ToolCall {
    ToolCall {
        tool_call_key: ToolCallKey {
            thread_id: thread.into(),
            turn_id: TURN.into(),
            call_id: call.into(),
        },
        tool_name: SendTool::tool_name(),
        arguments: serde_json::json!({
            "target": { "aspect": "Mind", "topic": "gamma", "layer": "Secondary" },
            "delivery": "Wake",
            "priority": "Soft",
            "kind": "Order",
            "body": body
        })
        .to_string(),
    }
}

/// What one subscribed connection of the fake does: the server requests it
/// sends after the initialize and resume answers, whether it closes once it
/// has sent them, and whether it stops reading first, so the client's
/// answer to them fails to be written.
struct FakeConnection {
    requests: Vec<serde_json::Value>,
    closes: bool,
    deaf: bool,
}

struct FakeAppServer {
    directory: tempfile::TempDir,
    executable: PathBuf,
}

trait RunsFakeAppServer {
    fn install(connections: Vec<FakeConnection>) -> FakeAppServer;
    /// The JSON messages the client wrote on run `run`, waiting until one
    /// satisfies `wanted`.
    fn written_on(
        &self,
        run: u64,
        wanted: impl Fn(&serde_json::Value) -> bool,
    ) -> Vec<serde_json::Value>;
    fn runs(&self) -> u64;
    /// Adds app-server sessions that resume `thread` and authentically
    /// acknowledge any durable queued-turn JSON-RPC id in the test range.
    fn accept_queued_turns(&self, thread: &str, runs: std::ops::RangeInclusive<u64>);
    /// Run `run` answers a reconciliation: `thread/read` with the thread
    /// idle or active, then one page of `turns`, newest first.
    fn serve_turn_history(&self, run: u64, thread: &str, idle: bool, turns: Vec<serde_json::Value>);
    fn queued_turn_texts(&self) -> Vec<String>;
}

impl RunsFakeAppServer for FakeAppServer {
    fn install(connections: Vec<FakeConnection>) -> FakeAppServer {
        let directory = tempfile::tempdir().expect("fake app-server directory");
        let root = directory.path();
        let initialized = frame(&serde_json::json!({ "id": 1, "result": {} }));
        let probe = [
            initialized.clone(),
            frame(&serde_json::json!({ "id": 2, "error": {
                "code": -32600,
                "message": "thread/start.dynamicTools requires experimentalApi capability"
            }})),
        ]
        .concat();
        for (index, connection) in connections.iter().enumerate() {
            let run = 2 * index + 1;
            std::fs::write(root.join(format!("frames-{run}")), &probe).unwrap();
            let frames = [
                initialized.clone(),
                frame(&serde_json::json!({ "id": 2, "result": { "thread": {} } })),
            ];
            std::fs::write(root.join(format!("frames-{}", run + 1)), frames.concat()).unwrap();
            let calls = connection.requests.iter().map(frame).collect::<String>();
            std::fs::write(root.join(format!("calls-{}", run + 1)), calls).unwrap();
            if connection.closes {
                std::fs::write(root.join(format!("close-{}", run + 1)), "").unwrap();
            }
            if connection.deaf {
                std::fs::write(root.join(format!("deaf-{}", run + 1)), "").unwrap();
            }
        }
        let executable = root.join("codex");
        FixtureExecutable {
            path: executable.clone(),
        }
        .install(&format!(
            "#!/bin/sh\nd='{root}'\nn=$(cat \"$d/count\" 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > \"$d/count\"\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nexec 3<&0; cat <&3 > \"$d/record-$n\" &\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\n[ -f \"$d/frames-$n\" ] || exit 1\nprintf '%b' \"$(cat \"$d/frames-$n\")\"\nif [ -f \"$d/deaf-$n\" ]; then sleep 1; kill $! 2>/dev/null; exec 0<&- 3<&-; printf '%b' \"$(cat \"$d/calls-$n\")\"; exit 0; fi\n[ -f \"$d/calls-$n\" ] && printf '%b' \"$(cat \"$d/calls-$n\")\"\n[ -f \"$d/close-$n\" ] && {{ kill $! 2>/dev/null; exit 0; }}\nsleep 30\n",
            root = root.display(),
        ));
        FakeAppServer {
            directory,
            executable,
        }
    }

    fn written_on(
        &self,
        run: u64,
        wanted: impl Fn(&serde_json::Value) -> bool,
    ) -> Vec<serde_json::Value> {
        let path = self.directory.path().join(format!("record-{run}"));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let messages = path.client_messages();
            if messages.iter().any(&wanted) || Instant::now() > deadline {
                return messages;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn runs(&self) -> u64 {
        std::fs::read_to_string(self.directory.path().join("count"))
            .ok()
            .and_then(|count| count.parse().ok())
            .unwrap_or(0)
    }

    fn accept_queued_turns(&self, thread: &str, runs: std::ops::RangeInclusive<u64>) {
        let initialized = frame(&serde_json::json!({ "id": 1, "result": {} }));
        // One answer serves every request a queued turn's connections make
        // as id 2: the anchor's `thread/turns/list` (a thread with no turn
        // yet), and the `thread/resume` before `turn/start`.
        let resumed = frame(&serde_json::json!({
            "id": 2,
            "result": {
                "thread": { "id": thread, "status": { "type": "idle" } },
                "data": [],
                "nextCursor": null
            }
        }));
        let receipts = (3..=64)
            .map(|id| {
                frame(&serde_json::json!({
                    "id": id,
                    "result": { "turn": { "id": format!("queued-turn-{id}") } }
                }))
            })
            .collect::<String>();
        for run in runs {
            std::fs::write(
                self.directory.path().join(format!("frames-{run}")),
                format!("{initialized}{resumed}{receipts}"),
            )
            .unwrap();
        }
    }

    fn serve_turn_history(
        &self,
        run: u64,
        thread: &str,
        idle: bool,
        turns: Vec<serde_json::Value>,
    ) {
        let status = if idle { "idle" } else { "active" };
        let frames = [
            frame(&serde_json::json!({ "id": 1, "result": {} })),
            frame(&serde_json::json!({
                "id": 2,
                "result": { "thread": { "id": thread, "status": { "type": status } } }
            })),
            frame(&serde_json::json!({
                "id": 3,
                "result": { "data": turns, "nextCursor": null }
            })),
        ]
        .concat();
        std::fs::write(self.directory.path().join(format!("frames-{run}")), frames).unwrap();
    }

    fn queued_turn_texts(&self) -> Vec<String> {
        (1..=self.runs())
            .flat_map(|run| {
                self.directory
                    .path()
                    .join(format!("record-{run}"))
                    .client_messages()
            })
            .filter(|message| message["method"] == "turn/start")
            .filter_map(|message| {
                message
                    .pointer("/params/input/0/text")?
                    .as_str()
                    .map(str::to_owned)
            })
            .collect()
    }
}

/// The JSON messages a recorded client stream holds: each masked client
/// frame unmasked (RFC 6455 5.3).
pub(super) trait ReadsClientFrames {
    fn client_messages(&self) -> Vec<serde_json::Value>;
}

impl ReadsClientFrames for Path {
    fn client_messages(&self) -> Vec<serde_json::Value> {
        let bytes = std::fs::read(self).unwrap_or_default();
        let mut messages = Vec::new();
        let mut at = 0;
        while at + 2 <= bytes.len() {
            let (length, header) = match bytes[at + 1] & 0x7f {
                126 if at + 4 <= bytes.len() => (
                    u16::from_be_bytes([bytes[at + 2], bytes[at + 3]]) as usize,
                    4,
                ),
                127 if at + 10 <= bytes.len() => (
                    u64::from_be_bytes(bytes[at + 2..at + 10].try_into().unwrap()) as usize,
                    10,
                ),
                short if short < 126 => (short as usize, 2),
                _ => break,
            };
            let start = at + header + 4;
            if start + length > bytes.len() {
                break;
            }
            let mask = &bytes[at + header..start];
            let payload = bytes[start..start + length]
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4])
                .collect::<Vec<_>>();
            if let Ok(message) = serde_json::from_slice(&payload) {
                messages.push(message);
            }
            at = start + length;
        }
        messages
    }
}

/// The answer the client wrote to server request `request_id`.
fn answer_to(messages: &[serde_json::Value], request_id: u64) -> Option<serde_json::Value> {
    messages
        .iter()
        .find(|message| message["id"] == request_id && message.get("result").is_some())
        .map(|message| message["result"].clone())
}

/// An Order as the target's queue holds it: with the Sender Flow resolved.
fn sent(sender: Address, body: &str) -> TestRequest {
    TestRequest {
        sent_by: SentBy::Sender(sender),
        request: Request::Order(body.into()),
    }
}

fn accepted_text(text: &str) {
    assert!(
        text.starts_with("Accepted."),
        "unexpected tool response: {text}"
    );
}

trait SeatsCodexFlows {
    /// A live Codex flow `flow_id` on `thread`, in pane `pane`, launched
    /// for `model`, its metaflow at `at` Awake as it.
    fn codex_seat(&self, flow_id: &str, thread: &str, pane: &str, model: &str, at: Address);
    /// The target metaflow, Awake as a flow Flow does not hold, so nothing
    /// queued for it wakes anything.
    fn awake_target(&self);
    fn target_queue(&self) -> Vec<TestRequest>;
    /// The adapter of the endpoint `model` selects, its client the fake.
    fn adapter(&mut self, model: &str, fake: &FakeAppServer) -> CodexAdapter;
}

impl SeatsCodexFlows for NexusFixture {
    fn codex_seat(&self, flow_id: &str, thread: &str, pane: &str, model: &str, at: Address) {
        let mut node: FlowNode = self.node();
        node.flow_id = flow_id.into();
        node.session_id = thread.into();
        node.herdr_route_selection = HerdrRouteSelection::Available(HerdrRoute {
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: format!("agent-{flow_id}"),
            herdr_pane_id: pane.into(),
            herdr_terminal_id: format!("terminal-{flow_id}"),
        });
        self.nexus
            .store
            .register_flow_in_role(
                node,
                Caller {
                    flow_id: flow_id.into(),
                    aspect: Aspect::Mind,
                    power_level: PowerLevel::Medium,
                    model_name: model.into(),
                },
            )
            .expect("Codex seat registered");
        self.nexus.store.seed_metaflow(Metaflow {
            address: at,
            state: State::Awake(flow_id.into()),
            past: vec![],
            queue: vec![],
        });
    }

    fn awake_target(&self) {
        self.nexus.store.seed_metaflow(Metaflow {
            address: target(),
            state: State::Awake("c0ffee".into()),
            past: vec![],
            queue: vec![],
        });
    }

    fn target_queue(&self) -> Vec<TestRequest> {
        self.nexus
            .store
            .metaflow(&target())
            .expect("target readable")
            .expect("target held")
            .queue
            .into_iter()
            .map(|entry| TestRequest {
                sent_by: entry.sent_by,
                request: match entry.message {
                    Message::HardAbrupt(request)
                    | Message::MiddleAbrupt(request)
                    | Message::Soft(request) => request,
                },
            })
            .collect()
    }

    fn adapter(&mut self, model: &str, fake: &FakeAppServer) -> CodexAdapter {
        let endpoints = &mut self.nexus.codex_endpoints;
        endpoints.timeout = Duration::from_secs(5);
        if endpoints.stable.model_names.contains(model) {
            endpoints.stable.client_path = fake.executable.clone();
        } else {
            endpoints.next.client_path = fake.executable.clone();
        }
        endpoints
            .adapter_for(model)
            .expect("model selects an endpoint")
    }
}

/// Runs a responder for `thread` until `done` returns, then stops it.
fn responding<T>(
    fixture: &NexusFixture,
    thread: &str,
    adapter: &CodexAdapter,
    done: impl FnOnce() -> T,
) -> T {
    std::thread::scope(|scope| {
        let (signals, inbox) = mpsc::channel();
        let stop = signals.clone();
        let responder =
            scope.spawn(move || fixture.nexus.respond_for(thread, adapter, signals, inbox));
        let result = done();
        stop.send(ResponderSignal::Stop).unwrap();
        responder.join().expect("responder ends when told to stop");
        result
    })
}

#[test]
fn two_threads_are_each_attributed_to_their_own_metaflow() {
    let mut fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.codex_seat("b2b2b2", THREAD_B, "w1:p6", "gpt-6-sol", sender_b());
    fixture.awake_target();
    let fake_a = FakeAppServer::install(vec![FakeConnection {
        requests: vec![tool_call_request(10, THREAD_A, "call-a", "from a")],
        closes: false,
        deaf: false,
    }]);
    let fake_b = FakeAppServer::install(vec![FakeConnection {
        requests: vec![tool_call_request(11, THREAD_B, "call-b", "from b")],
        closes: false,
        deaf: false,
    }]);
    let adapter_a = fixture.adapter("gpt-sol", &fake_a);
    let adapter_b = fixture.adapter("gpt-6-sol", &fake_b);

    let answer_a = responding(&fixture, THREAD_A, &adapter_a, || {
        answer_to(&fake_a.written_on(2, |m| m["id"] == 10), 10)
    })
    .expect("thread A's call answered");
    let answer_b = responding(&fixture, THREAD_B, &adapter_b, || {
        answer_to(&fake_b.written_on(2, |m| m["id"] == 11), 11)
    })
    .expect("thread B's call answered");

    assert_eq!(answer_a["success"], true);
    accepted_text(answer_a["contentItems"][0]["text"].as_str().unwrap());
    accepted_text(answer_b["contentItems"][0]["text"].as_str().unwrap());
    assert_eq!(
        fixture.target_queue(),
        vec![sent(sender_a(), "from a"), sent(sender_b(), "from b")]
    );
    // Each subscribed connection initialized with the experimental API and
    // resumed exactly its own thread.
    let written = fake_a.written_on(2, |_| true);
    assert_eq!(
        written[0]["params"]["capabilities"]["experimentalApi"],
        true
    );
    let resume = written
        .iter()
        .find(|m| m["method"] == "thread/resume")
        .expect("thread A resumed");
    assert_eq!(resume["params"]["threadId"], THREAD_A);
    let recorded = fixture
        .nexus
        .store
        .send_call(&tool_call(THREAD_B, "call-b", "from b").tool_call_key)
        .unwrap()
        .expect("thread B's call recorded");
    assert!(matches!(recorded.send_outcome, SendOutcome::Accepted(_)));
}

#[test]
fn a_call_from_a_thread_flow_does_not_hold_is_refused_and_queues_nothing() {
    let mut fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    let foreign = "01a0b22c-0000-7000-8000-0000000000ff";
    let fake = FakeAppServer::install(vec![FakeConnection {
        requests: vec![tool_call_request(12, foreign, "call-x", "intruder")],
        closes: false,
        deaf: false,
    }]);
    let adapter = fixture.adapter("gpt-sol", &fake);
    let answer = responding(&fixture, THREAD_A, &adapter, || {
        answer_to(&fake.written_on(2, |m| m["id"] == 12), 12)
    })
    .expect("the foreign call answered");
    assert_eq!(answer["success"], false);
    assert_eq!(answer["contentItems"][0]["text"], "Refused.ThreadUnowned");
    assert!(fixture.target_queue().is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .send_call(&tool_call(foreign, "call-x", "intruder").tool_call_key)
            .unwrap(),
        None
    );
}

#[test]
fn a_replayed_call_of_a_retired_or_superseded_flow_is_refused() {
    let fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.codex_seat("b2b2b2", THREAD_B, "w1:p6", "gpt-sol", sender_b());
    fixture.awake_target();
    let call_a = tool_call(THREAD_A, "call-a", "before retirement");
    assert!(matches!(
        fixture.nexus.handle_send_call(call_a.clone()),
        SendOutcome::Accepted(_)
    ));
    // Retired: the replay finds the flow ended, though its call was queued.
    assert!(fixture.nexus.store.record_retired("a1a1a1").unwrap());
    let replayed = fixture.nexus.handle_send_call(call_a);
    assert_eq!(replayed, SendOutcome::Refused(SendRefusal::FlowEnded));
    // Superseded: a fresh wake made the metaflow Awake as a successor.
    let call_b = tool_call(THREAD_B, "call-b", "pending while superseded");
    assert!(
        fixture
            .nexus
            .store
            .sleep_metaflow(&sender_b(), "b2b2b2")
            .unwrap()
    );
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: sender_b(),
        state: State::Awake("b3b3b3".into()),
        past: vec!["b2b2b2".into()],
        queue: vec![],
    });
    assert_eq!(
        fixture.nexus.handle_send_call(call_b),
        SendOutcome::Refused(SendRefusal::NotAwake)
    );
    assert_eq!(
        fixture.target_queue(),
        vec![sent(sender_a(), "before retirement")]
    );
}

#[test]
fn a_call_queued_before_a_crash_is_answered_from_its_record_when_replayed() {
    let mut fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    // The Nexus queued the call and recorded it, then died before answering.
    let handled = fixture
        .nexus
        .handle_send_call(tool_call(THREAD_A, "call-a", "exactly once"));
    assert!(matches!(handled, SendOutcome::Accepted(_)));
    // The app-server replays the pending call to the next subscriber.
    let fake = FakeAppServer::install(vec![FakeConnection {
        requests: vec![tool_call_request(13, THREAD_A, "call-a", "exactly once")],
        closes: false,
        deaf: false,
    }]);
    let adapter = fixture.adapter("gpt-sol", &fake);
    let answer = responding(&fixture, THREAD_A, &adapter, || {
        answer_to(&fake.written_on(2, |m| m["id"] == 13), 13)
    })
    .expect("the replayed call answered");
    accepted_text(answer["contentItems"][0]["text"].as_str().unwrap());
    assert_eq!(
        fixture.target_queue(),
        vec![sent(sender_a(), "exactly once")]
    );
}

/// The launched Claude seat's metaflow, Asleep with nothing queued.
fn asleep_target() -> Address {
    address("flow", Layer::Secondary)
}

trait SeatsAsleepTargets {
    /// The metaflow at `at`, Asleep, its last flow the launched abc123.
    fn asleep_at(&self, at: Address);
    fn stored_at(&self, at: &Address) -> Metaflow;
}

impl SeatsAsleepTargets for NexusFixture {
    fn asleep_at(&self, at: Address) {
        self.nexus.store.seed_metaflow(Metaflow {
            address: at,
            state: State::Asleep,
            past: vec!["abc123".into()],
            queue: vec![],
        });
    }

    fn stored_at(&self, at: &Address) -> Metaflow {
        self.nexus
            .store
            .metaflow(at)
            .expect("metaflow readable")
            .expect("metaflow held")
    }
}

/// The Order a woken seat was handed, counted by its recipient-facing text.
fn handed_orders(fixture: &NexusFixture, world: &super::waking::FreshWorld, body: &str) -> usize {
    use super::waking::ReadsFreshWorld;
    world.user_turns(fixture).concat().matches(body).count()
}

#[test]
fn a_call_key_recorded_with_other_content_is_refused() {
    let fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    fixture
        .nexus
        .handle_send_call(tool_call(THREAD_A, "call-a", "first"));
    assert_eq!(
        fixture
            .nexus
            .handle_send_call(tool_call(THREAD_A, "call-a", "second")),
        SendOutcome::Refused(SendRefusal::KeyReused)
    );
    assert_eq!(fixture.target_queue(), vec![sent(sender_a(), "first")]);
    // The record keeps what the key first asked.
    let recorded: SendCall = fixture
        .nexus
        .store
        .send_call(&tool_call(THREAD_A, "call-a", "first").tool_call_key)
        .unwrap()
        .unwrap();
    assert_eq!(
        recorded.content_sha256,
        crate::generated::send_tool::ReadsSendArguments::content_sha256(&tool_call(
            THREAD_A, "call-a", "first"
        ))
    );
}

#[test]
fn dynamic_send_requires_explicit_delivery_and_priority() {
    use crate::generated::send_tool::{ReadsSendArguments, SendPriority};

    let explicit = tool_call(THREAD_A, "explicit", "typed admission");
    let arguments = explicit.send_arguments().expect("explicit policy is valid");
    assert_eq!(arguments.delivery, Delivery::Wake);
    assert_eq!(arguments.send_priority, SendPriority::Soft);

    let mut old_shape = explicit;
    old_shape.arguments = serde_json::json!({
        "target": { "aspect": "Mind", "topic": "gamma", "layer": "Secondary" },
        "kind": "Order",
        "body": "old implicit policy"
    })
    .to_string();
    assert!(old_shape.send_arguments().is_none());
}

#[test]
fn dynamic_queue_admission_preserves_priority_without_waking() {
    let fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.asleep_at(asleep_target());
    let mut call = tool_call(THREAD_A, "queue-only", "retain this");
    call.arguments = serde_json::json!({
        "target": { "aspect": "Mind", "topic": "flow", "layer": "Secondary" },
        "delivery": "Queue",
        "priority": "MiddleAbrupt",
        "kind": "Order",
        "body": "retain this"
    })
    .to_string();
    let outcome = fixture.nexus.handle_send_call(call);
    assert!(matches!(outcome, SendOutcome::Accepted(_)));
    let held = fixture.stored_at(&asleep_target());
    assert_eq!(held.state, State::Asleep);
    assert_eq!(held.queue.len(), 1);
    assert_eq!(held.queue[0].delivery, Delivery::Queue);
    assert_eq!(
        held.queue[0].message,
        Message::MiddleAbrupt(Request::Order("retain this".into()))
    );
}

/// At the Nexus's start the send tool subscribes to every live Codex thread
/// it holds; a connection that closes is reconnected, and the call the
/// app-server left pending is replayed to the new connection and queued
/// once.
#[test]
fn a_pending_call_is_answered_after_start_and_reconnect() {
    let mut fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    let fake = FakeAppServer::install(vec![
        FakeConnection {
            requests: vec![],
            closes: true,
            deaf: false,
        },
        FakeConnection {
            requests: vec![tool_call_request(14, THREAD_A, "call-a", "after restart")],
            closes: false,
            deaf: false,
        },
    ]);
    fixture.adapter("gpt-sol", &fake);
    let answer = std::thread::scope(|scope| {
        let nexus = &fixture.nexus;
        let server = scope.spawn(move || nexus.serve_send_tool(scope));
        let answer = answer_to(&fake.written_on(4, |m| m["id"] == 14), 14);
        nexus.stopping.request();
        nexus.store.launch_changes.announce();
        server.join().expect("the send tool stops with the Nexus");
        answer
    })
    .unwrap_or_else(|| {
        panic!(
            "the pending call was not answered; runs={}, run2={:?}, run3={:?}, run4={:?}",
            fake.runs(),
            fake.directory.path().join("record-2").client_messages(),
            fake.directory.path().join("record-3").client_messages(),
            fake.directory.path().join("record-4").client_messages(),
        )
    });
    accepted_text(answer["contentItems"][0]["text"].as_str().unwrap());
    assert_eq!(
        fixture.target_queue(),
        vec![sent(sender_a(), "after restart")]
    );
    assert!(fake.runs() >= 4, "probe, connection, probe, connection");
}

#[test]
fn a_malformed_call_is_refused_and_recorded() {
    let fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    let mut call = tool_call(THREAD_A, "call-m", "x");
    call.arguments = serde_json::json!({
        "target": { "aspect": "Mind", "topic": "gamma", "layer": "Secondary" },
        "kind": "Psyche",
        "body": "the living's words are not a flow's to send"
    })
    .to_string();
    assert_eq!(
        fixture.nexus.handle_send_call(call.clone()),
        SendOutcome::Refused(SendRefusal::Malformed)
    );
    assert!(fixture.target_queue().is_empty());
    assert!(
        fixture
            .nexus
            .store
            .send_call(&call.tool_call_key)
            .unwrap()
            .is_some()
    );
}

/// A target whose topic is not an uncapitalized camelCase identifier is
/// refused TopicRefused, carrying the topic as written: a capital is never
/// lowered into the metaflow it would then name. The refusal is the call's
/// recorded answer, and a replay is answered the same.
#[test]
fn a_call_to_a_topic_that_is_not_camel_case_is_refused_as_written() {
    let fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    for (call_id, topic) in [("call-capital", "Gamma"), ("call-snake", "gamma_two")] {
        let mut call = tool_call(THREAD_A, call_id, "x");
        call.arguments = serde_json::json!({
            "target": { "aspect": "Mind", "topic": topic, "layer": "Secondary" },
            "delivery": "Wake",
            "priority": "Soft",
            "kind": "Order",
            "body": "x"
        })
        .to_string();
        let refused = SendOutcome::Refused(SendRefusal::TopicRefused(topic.into()));
        assert_eq!(fixture.nexus.handle_send_call(call.clone()), refused);
        assert!(fixture.target_queue().is_empty(), "nothing reaches gamma");
        assert_eq!(
            fixture
                .nexus
                .store
                .send_call(&call.tool_call_key)
                .unwrap()
                .unwrap()
                .send_outcome,
            refused
        );
        assert_eq!(fixture.nexus.handle_send_call(call), refused);
    }
}

#[test]
fn an_oversized_dynamic_send_is_typed_durable_and_not_queued() {
    let fixture = NexusFixture::new();
    fixture.codex_seat("a1a1a1", THREAD_A, "w1:p5", "gpt-sol", sender_a());
    fixture.awake_target();
    let overhead = Request::Order("x".into()).native_prompt_bytes(&sender_a()) - 1;
    let body = "x".repeat(MAX_SEND_PAYLOAD_BYTES - overhead + 1);
    let request = Request::Order(body.clone());
    assert_eq!(
        request.native_prompt_bytes(&sender_a()),
        MAX_SEND_PAYLOAD_BYTES + 1
    );
    let call = tool_call(THREAD_A, "call-oversized", &body);
    let outcome = fixture.nexus.handle_send_call(call.clone());
    assert!(matches!(
        outcome,
        SendOutcome::Refused(SendRefusal::Oversized(signal_flow::SendPayloadSize {
            actual_bytes,
            maximum_bytes,
        })) if actual_bytes == (MAX_SEND_PAYLOAD_BYTES + 1) as i64
            && maximum_bytes == MAX_SEND_PAYLOAD_BYTES as i64
    ));
    assert!(fixture.target_queue().is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .send_call(&call.tool_call_key)
            .unwrap()
            .unwrap()
            .send_outcome,
        outcome
    );
    assert_eq!(fixture.nexus.handle_send_call(call), outcome);
    assert!(fixture.target_queue().is_empty());
}

#[test]
fn each_dynamic_send_invocation_has_a_unique_typed_content_free_journal_event() {
    let call = tool_call(THREAD_A, "call-audit", "sensitive body text");
    let outcome = SendOutcome::Refused(SendRefusal::Malformed);
    let first = call.send_invocation(outcome.clone());
    let second = call.send_invocation(outcome);
    assert_ne!(first.send_invocation_id, second.send_invocation_id);
    assert_eq!(first.tool_call_key, call.tool_call_key);
    assert_eq!(first.tool_name, SendTool::tool_name());
    assert!(!format!("{first:?}").contains("sensitive body text"));
}

/// Starts a witness launch's first turn, its one part, on the connection
/// that opened its thread, as Submit starts it once the TUI attached.
fn start_first_turn(
    connection: &mut crate::codex::ThreadConnection,
    launch: &signal_flow::ComposedLaunch,
    thread: &str,
) {
    use crate::codex::ExchangesOnSubscription;
    let crate::codex::ThreadConnection {
        subscription,
        inbox,
        ..
    } = connection;
    if subscription
        .exchange(
            inbox,
            5,
            "turn/start",
            serde_json::json!({
                "threadId": thread,
                "effort": launch.launch_profile.effort,
                "model": launch.launch_profile.model_name,
                "input": [{
                    "type": "text",
                    "text": launch.first_prompt_payload.prompt_part_vector[0].prompt_part_text,
                    "text_elements": []
                }],
                "turnTrigger": "flow-nexus"
            }),
        )
        .is_err()
    {
        panic!("the first turn starts");
    }
}

/// The integration witness, against a real scratch Codex app-server named
/// by `FLOW_SEND_TOOL_WITNESS_CODEX` (the codex client) and
/// `FLOW_SEND_TOOL_WITNESS_SOCKET` (its control socket), serving
/// `gpt-6-luna`. Flow's own adapter opens the thread, declaring the send
/// tool, with a first turn asking the model to call it once. The target is
/// Asleep, its last flow a launched Claude seat, woken in the fresh world. A
/// first connection subscribes and receives the call; the Nexus handles
/// it, queueing and recording it, and the connection is closed before it
/// answers or wakes anything, as a Nexus dying there would leave it. The
/// responder then subscribes, the app-server replays the pending call to
/// it, it is answered from its record, and the target is woken once, the
/// request handed to it once with its sender. No repository
/// holds such an app-server, so the test runs only when asked for by name
/// with `--ignored` and both variables set.
#[test]
#[ignore = "needs FLOW_SEND_TOOL_WITNESS_CODEX and FLOW_SEND_TOOL_WITNESS_SOCKET naming a scratch codex app-server"]
fn witness_a_real_call_is_queued_once_across_a_forced_reconnect() {
    use crate::codex::{ExchangesOnSubscription, OpensCodexThread, ProxyEvent, SubscribesThread};
    use crate::composition::ComposesLaunch;
    use crate::composition::tests::{fixture_composer, fixture_profile};
    use sha2::{Digest, Sha256};
    let client = std::env::var_os("FLOW_SEND_TOOL_WITNESS_CODEX").expect("codex client");
    let socket = std::env::var("FLOW_SEND_TOOL_WITNESS_SOCKET").expect("app-server socket");
    let mut fixture = NexusFixture::new();
    fixture.nexus.codex_endpoints.next.client_path = PathBuf::from(client);
    fixture.nexus.codex_endpoints.next.socket = socket;
    fixture.nexus.codex_endpoints.timeout = Duration::from_secs(60);
    let adapter = fixture
        .nexus
        .codex_endpoints
        .adapter_for("gpt-6-luna")
        .expect("gpt-6-luna selects the next endpoint");

    let mut profile = fixture_profile();
    profile.model_name = "gpt-6-luna".into();
    profile.effort = "low".into();
    let mut launch = fixture_composer()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: Vec::new(),
                harness_kind: signal_flow::HarnessKind::Codex,
                launch_profile: profile,
            },
            vec![signal_flow::LeaveOut::CodexAgentsMd],
        )
        .expect("launch composes");
    let instruction = "Call the send tool exactly once with target aspect Mind, topic flow, layer Secondary, kind Order and body witness-order. Then reply with the exact text the tool returned.";
    let digest = format!("{:x}", Sha256::digest(instruction.as_bytes()));
    launch.first_prompt_payload.first_prompt_body = instruction.into();
    launch.first_prompt_payload.prompt_sha256 = digest.clone();
    launch.first_prompt_payload.prompt_part_vector = vec![signal_flow::PromptPart {
        prompt_part_text: instruction.into(),
        prompt_sha256: digest.clone(),
    }];
    launch.target_receipt_request.prompt_sha256 = digest;

    let (thread, mut connection) = adapter
        .open_thread(&launch, &mut |_| {
            Ok("{ Mind flow Secondary witness }".into())
        })
        .expect("the thread opens declaring the send tool");
    start_first_turn(&mut connection, &launch, &thread);
    drop(connection);
    eprintln!("witness thread {thread}");
    super::waking::ended_claude(&fixture);
    let world = super::waking::WakesFreshClaude::fresh_claude_world(&mut fixture);
    fixture.codex_seat("a1a1a1", &thread, "w1:p5", "gpt-6-luna", sender_a());
    fixture.asleep_at(asleep_target());

    // The first connection receives the call, the Nexus queues and records
    // it, and the connection closes unanswered.
    let (signals, inbox) = mpsc::channel();
    let mut first = match adapter.subscribe_thread(&thread, 1, &signals, &inbox) {
        Ok(subscription) => subscription,
        Err(crate::codex::SubscriptionEnd::Failed(error)) => panic!("first subscription: {error}"),
        Err(crate::codex::SubscriptionEnd::Stopped) => panic!("first subscription stopped"),
    };
    let mut pending = std::mem::take(&mut first.early);
    let deadline = Instant::now() + Duration::from_secs(120);
    let call = loop {
        if let Some(index) = pending
            .iter()
            .position(|request| request["method"] == "item/tool/call")
        {
            break pending.swap_remove(index);
        }
        match first.next_event(&inbox, Some(deadline)) {
            Ok(ProxyEvent::Reply(Ok(message))) if message.get("id").is_some() => {
                pending.push(message)
            }
            Ok(_) => {}
            Err(_) => panic!("no tool call reached the first connection"),
        }
    };
    eprintln!("witness call {call}");
    let tool_call = ToolCall::read_tool_call::<serde_json::Value>(call["params"].clone())
        .expect("the call reads");
    let handled = fixture.nexus.handle_send_call(tool_call.clone());
    eprintln!("witness first handling {:?}", handled);
    assert!(matches!(handled, SendOutcome::Accepted(_)));
    first.close();
    drop(signals);
    drop(inbox);

    // The responder subscribes; the pending call is replayed and answered
    // from its record.
    std::thread::scope(|scope| {
        let (signals, inbox) = mpsc::channel();
        let stop = signals.clone();
        let nexus = &fixture.nexus;
        let thread = thread.as_str();
        let adapter = &adapter;
        let responder = scope.spawn(move || nexus.respond_for(thread, adapter, signals, inbox));
        // The answer reaches the app-server; the model's turn then ends.
        // The record and the queue are what Flow witnesses here; the
        // rollout shows the tool's output.
        std::thread::sleep(Duration::from_secs(20));
        stop.send(ResponderSignal::Stop).unwrap();
        responder.join().expect("responder stops");
    });
    let woken = fixture.stored_at(&asleep_target());
    eprintln!("witness target {:?} queue {:?}", woken.state, woken.queue);
    let starts = super::waking::ReadsFreshWorld::claude_starts(&world);
    eprintln!("witness herdr agent starts {starts}");
    let handed = handed_orders(&fixture, &world, "witness-order");
    eprintln!("witness handed orders {handed}");
    assert!(matches!(&woken.state, State::Awake(flow_id) if flow_id != "abc123"));
    assert!(woken.queue.is_empty());
    assert_eq!(starts, 1);
    assert_eq!(handed, 1);
    let recorded = fixture
        .nexus
        .store
        .send_call(&tool_call.tool_call_key)
        .unwrap()
        .expect("the call is recorded");
    eprintln!("witness record {:?}", recorded.send_outcome);
}

/// Witness against a scratch codex app-server: a thread opened as a launch
/// opens it, whose first turn calls the send tool before the turn can end,
/// is served while the dispatch gate is held for the whole wait, as a
/// launch holds it while Herdr waits for its seat to come to rest. The
/// thread is recorded as the launch's before the first turn, and the
/// connection that opened it answers; the call is queued once from the launching
/// metaflow, the turn ends with the tool's answer, and once the gate is
/// given back the sleeping target is woken and handed the order once. The
/// wait stands in for Herdr's: it ends when the rollout under
/// FLOW_SEND_TOOL_WITNESS_HOME shows the turn complete. Runs only by name
/// with `--ignored` and the three variables set.
#[test]
#[ignore = "needs FLOW_SEND_TOOL_WITNESS_CODEX, FLOW_SEND_TOOL_WITNESS_SOCKET and FLOW_SEND_TOOL_WITNESS_HOME naming a scratch codex app-server"]
fn witness_a_first_turn_call_is_served_while_the_launch_holds_the_gate() {
    use crate::codex::OpensCodexThread;
    use crate::composition::ComposesLaunch;
    use crate::composition::tests::{fixture_composer, fixture_profile};
    use crate::send_tool::ServesLaunchCalls;
    use crate::store::opened_thread::RecordsOpenedThreads;
    use crate::store::{RecordsNativeLaunchIntent, ReservesLaunchAttempt};
    use sha2::{Digest, Sha256};
    let client = std::env::var_os("FLOW_SEND_TOOL_WITNESS_CODEX").expect("codex client");
    let socket = std::env::var("FLOW_SEND_TOOL_WITNESS_SOCKET").expect("app-server socket");
    let home = PathBuf::from(std::env::var_os("FLOW_SEND_TOOL_WITNESS_HOME").expect("codex home"));
    let mut fixture = NexusFixture::new();
    fixture.nexus.codex_endpoints.next.client_path = PathBuf::from(client);
    fixture.nexus.codex_endpoints.next.socket = socket;
    fixture.nexus.codex_endpoints.timeout = Duration::from_secs(60);
    let adapter = fixture
        .nexus
        .codex_endpoints
        .adapter_for("gpt-6-luna")
        .expect("gpt-6-luna selects the next endpoint");
    let mut profile = fixture_profile();
    profile.model_name = "gpt-6-luna".into();
    profile.effort = "low".into();
    let sender = profile.address.clone();
    let mut launch = fixture_composer()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: Vec::new(),
                harness_kind: signal_flow::HarnessKind::Codex,
                launch_profile: profile.clone(),
            },
            vec![signal_flow::LeaveOut::CodexAgentsMd],
        )
        .expect("launch composes");
    let instruction = "Call the send tool exactly once with target aspect Mind, topic flow, layer Secondary, kind Order and body first-turn-witness. Then reply with the exact text the tool returned, and end your turn.";
    let digest = format!("{:x}", Sha256::digest(instruction.as_bytes()));
    launch.first_prompt_payload.first_prompt_body = instruction.into();
    launch.first_prompt_payload.prompt_sha256 = digest.clone();
    launch.first_prompt_payload.prompt_part_vector = vec![signal_flow::PromptPart {
        prompt_part_text: instruction.into(),
        prompt_sha256: digest.clone(),
    }];
    launch.target_receipt_request.prompt_sha256 = digest;
    // The launch as Start leaves it before its Spawn: reserved, its native
    // intent recorded.
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&launch, signal_flow::Origin::Unbound)
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(signal_flow::NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: signal_flow::HarnessKind::Codex,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: launch
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
            })
            .unwrap()
    );
    super::waking::ended_claude(&fixture);
    let world = super::waking::WakesFreshClaude::fresh_claude_world(&mut fixture);
    fixture.asleep_at(asleep_target());

    let gate = fixture
        .nexus
        .dispatch_gate
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let nexus = &fixture.nexus;
    let (thread, mut connection) = adapter
        .open_thread(&launch, &mut |thread| {
            assert!(
                nexus
                    .store
                    .record_opened_thread(&profile.launch_request_id, thread)
                    .unwrap()
            );
            eprintln!("witness thread {thread} recorded before its first turn");
            Ok("{ Field flow Secondary witness }".into())
        })
        .expect("the thread opens and is named");
    start_first_turn(&mut connection, &launch, &thread);
    nexus.hold_launch_responder(connection);
    let rollout_shows_turn_complete = || {
        let deadline = Instant::now() + Duration::from_secs(180);
        while Instant::now() < deadline {
            let found = walk(&home.join("sessions"))
                .into_iter()
                .filter(|path| path.to_string_lossy().contains(&thread))
                .filter_map(|path| std::fs::read_to_string(path).ok())
                .find(|rollout| rollout.contains("task_complete"));
            if let Some(rollout) = found {
                return Some(rollout);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        None
    };
    let rollout = nexus.serving_launch_calls(rollout_shows_turn_complete);
    nexus.close_launch_responder();
    drop(gate);
    let rollout = rollout.expect("the first turn completes while the gate is held");
    let outputs = rollout
        .lines()
        .filter(|line| line.contains("tool_call_output") || line.contains("function_call_output"))
        .collect::<Vec<_>>();
    eprintln!("witness tool outputs {outputs:?}");
    assert!(
        outputs.iter().any(|line| line.contains("Accepted")),
        "{outputs:?}"
    );
    // Queued once, from the launching metaflow, before the target woke.
    let held = fixture.stored_at(&asleep_target());
    assert_eq!(held.state, State::Asleep);
    assert_eq!(held.queue.len(), 1);
    assert_eq!(held.queue[0].sent_by, SentBy::Sender(sender.clone()));
    assert_eq!(held.queue[0].delivery, Delivery::Wake);
    assert_eq!(
        held.queue[0].message,
        Message::Soft(Request::Order("first-turn-witness".into()))
    );
    let handed = super::waking::ReadsFreshWorld::user_turns(&world, &fixture)
        .concat()
        .matches("first-turn-witness")
        .count();
    eprintln!("witness typed orders {handed}");
    assert_eq!(handed, 0);
}

/// The first half of a restart witness against a scratch codex app-server:
/// a launch opens its thread as a Codex launch does, the thread recorded
/// as its own, and its first turn calls the send tool; once the rollout
/// shows the call, unanswered, the store is copied to
/// FLOW_INTERRUPTED_LAUNCH_STATE and this process exits at once, as a
/// Nexus that dies mid-launch. The second half restarts on that store.
#[test]
#[ignore = "needs the FLOW_SEND_TOOL_WITNESS variables and FLOW_INTERRUPTED_LAUNCH_STATE"]
fn witness_interrupted_launch_first_half_exits_with_its_call_pending() {
    use crate::codex::OpensCodexThread;
    use crate::composition::ComposesLaunch;
    use crate::composition::tests::{fixture_composer, fixture_profile};
    use crate::store::opened_thread::RecordsOpenedThreads;
    use crate::store::{RecordsNativeLaunchIntent, ReservesLaunchAttempt};
    use sha2::{Digest, Sha256};
    let client = std::env::var_os("FLOW_SEND_TOOL_WITNESS_CODEX").expect("codex client");
    let socket = std::env::var("FLOW_SEND_TOOL_WITNESS_SOCKET").expect("app-server socket");
    let home = PathBuf::from(std::env::var_os("FLOW_SEND_TOOL_WITNESS_HOME").expect("codex home"));
    let state =
        PathBuf::from(std::env::var_os("FLOW_INTERRUPTED_LAUNCH_STATE").expect("state directory"));
    let mut fixture = NexusFixture::new();
    fixture.nexus.codex_endpoints.next.client_path = PathBuf::from(client);
    fixture.nexus.codex_endpoints.next.socket = socket;
    fixture.nexus.codex_endpoints.timeout = Duration::from_secs(60);
    let adapter = fixture
        .nexus
        .codex_endpoints
        .adapter_for("gpt-6-luna")
        .expect("gpt-6-luna selects the next endpoint");
    let mut profile = fixture_profile();
    profile.model_name = "gpt-6-luna".into();
    profile.effort = "low".into();
    let mut launch = fixture_composer()
        .compose(
            crate::composition::HarnessedProfile {
                remembered_flows: Vec::new(),
                harness_kind: signal_flow::HarnessKind::Codex,
                launch_profile: profile.clone(),
            },
            vec![signal_flow::LeaveOut::CodexAgentsMd],
        )
        .expect("launch composes");
    let instruction = "Call the send tool exactly once with target aspect Mind, topic flow, layer Secondary, kind Order and body interrupted-witness. Then reply with the exact text the tool returned, and end your turn.";
    let digest = format!("{:x}", Sha256::digest(instruction.as_bytes()));
    launch.first_prompt_payload.first_prompt_body = instruction.into();
    launch.first_prompt_payload.prompt_sha256 = digest.clone();
    launch.first_prompt_payload.prompt_part_vector = vec![signal_flow::PromptPart {
        prompt_part_text: instruction.into(),
        prompt_sha256: digest.clone(),
    }];
    launch.target_receipt_request.prompt_sha256 = digest;
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&launch, signal_flow::Origin::Unbound)
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(signal_flow::NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: signal_flow::HarnessKind::Codex,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: launch
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
            })
            .unwrap()
    );
    fixture.asleep_at(asleep_target());
    let nexus = &fixture.nexus;
    let (thread, mut connection) = adapter
        .open_thread(&launch, &mut |thread| {
            assert!(
                nexus
                    .store
                    .record_opened_thread(&profile.launch_request_id, thread)
                    .unwrap()
            );
            Ok("{ Field flow Secondary witness }".into())
        })
        .expect("the thread opens and is named");
    start_first_turn(&mut connection, &launch, &thread);
    let deadline = Instant::now() + Duration::from_secs(180);
    let pending = loop {
        assert!(Instant::now() < deadline, "no send call appeared");
        let called = walk(&home.join("sessions"))
            .into_iter()
            .filter(|path| path.to_string_lossy().contains(&thread))
            .filter_map(|path| std::fs::read_to_string(path).ok())
            .find(|rollout| rollout.contains("tools.send"));
        if let Some(rollout) = called {
            break rollout;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(
        !pending.contains("tool_call_output"),
        "the call is unanswered"
    );
    std::fs::copy(
        fixture.directory.path().join("flow.sema"),
        state.join("flow.sema"),
    )
    .expect("store copied");
    std::fs::write(state.join("thread"), &thread).unwrap();
    eprintln!("witness thread {thread} call pending; exiting");
    std::process::exit(0);
}

/// The second half: a Nexus started on the store the first half left
/// reconciles the interrupted launch: the replayed call is refused, the
/// thread archived, the launch settled refused, and nothing queued.
#[test]
#[ignore = "needs the FLOW_SEND_TOOL_WITNESS variables and FLOW_INTERRUPTED_LAUNCH_STATE"]
fn witness_interrupted_launch_second_half_is_reconciled_at_restart() {
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::RecordsLaunchOutcome;
    let client = std::env::var_os("FLOW_SEND_TOOL_WITNESS_CODEX").expect("codex client");
    let socket = std::env::var("FLOW_SEND_TOOL_WITNESS_SOCKET").expect("app-server socket");
    let home = PathBuf::from(std::env::var_os("FLOW_SEND_TOOL_WITNESS_HOME").expect("codex home"));
    let state =
        PathBuf::from(std::env::var_os("FLOW_INTERRUPTED_LAUNCH_STATE").expect("state directory"));
    let thread = std::fs::read_to_string(state.join("thread")).expect("thread");
    let mut fixture = NexusFixture::with_store(|store| {
        std::fs::copy(state.join("flow.sema"), store).expect("store restored");
    });
    fixture.nexus.codex_endpoints.next.client_path = PathBuf::from(client);
    fixture.nexus.codex_endpoints.next.socket = socket;
    fixture.nexus.codex_endpoints.timeout = Duration::from_secs(60);

    fixture.nexus.reconcile_interrupted_launches();

    let outcome = fixture.nexus.store.launch_outcome("launch-42").unwrap();
    eprintln!("witness outcome {outcome:?}");
    assert!(matches!(
        outcome,
        Some(crate::store::LaunchOutcome::StartRejected(_))
    ));
    let target = fixture.stored_at(&asleep_target());
    eprintln!("witness target {:?} queue {:?}", target.state, target.queue);
    assert!(target.queue.is_empty());
    let deadline = Instant::now() + Duration::from_secs(30);
    let rollout = loop {
        let found = walk(&home)
            .into_iter()
            .filter(|path| path.to_string_lossy().contains(&thread))
            .collect::<Vec<_>>();
        if let Some(path) = found
            .iter()
            .find(|path| path.to_string_lossy().contains("archived_sessions"))
        {
            break std::fs::read_to_string(path).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "the thread is not archived: {found:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let outputs = rollout
        .lines()
        .filter(|line| line.contains("tool_call_output") || line.contains("function_call_output"))
        .collect::<Vec<_>>();
    eprintln!("witness archived; tool outputs {outputs:?}");
}

/// Every file under `root`, depth first.
fn walk(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    found
}

/// A woken or drained flow receives only recipient-facing message content.
#[test]
fn a_queued_request_is_rendered_as_recipient_content_only() {
    use crate::composition::RendersQueuedWork;
    let queue = [
        queue_entry("render-1", sent(sender_a(), "Carry on.")),
        QueueEntry {
            logical_delivery_id: "render-2".into(),
            sent_by: SentBy::Unknown,
            delivery: Delivery::Queue,
            message: Message::Soft(Request::Notice("kept".into())),
        },
    ];
    let prompt = queue.prompt_text();
    assert_eq!(prompt, "Carry on.\nkept");
}

/// The fixture's Codex seat 908786: its thread, and the Address its
/// metaflow is Awake at.
const SEAT_THREAD: &str = "01a0b22c-e24f-7452-9940-64490878680f";

fn seat_at() -> Address {
    address("flow", Layer::Secondary)
}

/// Who sends the seat its work: the core Secondary of Psyche.
fn core() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
        layer: Layer::Secondary,
    }
}

fn core_primary() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
        layer: Layer::Primary,
    }
}

#[test]
fn tool_send_to_core_primary_accepts_only_the_core_secondary() {
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: core_primary(),
        state: State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let key = |call: &str| ToolCallKey {
        thread_id: THREAD_A.into(),
        turn_id: TURN.into(),
        call_id: call.into(),
    };
    let authorized = fixture
        .nexus
        .store
        .accept_tool_send(
            &key("authorized"),
            "authorized-content",
            core(),
            &core_primary(),
            Delivery::Wake,
            Message::Soft(Request::Question("source".into())),
            0,
        )
        .unwrap();
    assert!(matches!(authorized, SendOutcome::Accepted(_)));

    let unauthorized_sender = address("not-core", Layer::Secondary);
    let unauthorized = fixture
        .nexus
        .store
        .accept_tool_send(
            &key("unauthorized"),
            "unauthorized-content",
            unauthorized_sender,
            &core_primary(),
            Delivery::Wake,
            Message::Soft(Request::Question("must not enter".into())),
            0,
        )
        .unwrap();
    assert!(matches!(unauthorized, SendOutcome::Accepted(_)));
    let queued = fixture
        .nexus
        .store
        .metaflow(&core_primary())
        .unwrap()
        .unwrap()
        .queue;
    assert_eq!(queued.len(), 2);
    assert_eq!(queued[0].sent_by, SentBy::Sender(core()));
    assert_eq!(
        queued[0].message,
        Message::Soft(Request::Question("source".into()))
    );
    assert_eq!(
        queued[1].message,
        Message::Soft(Request::Question("must not enter".into()))
    );
}

fn from_core(request: Request) -> TestRequest {
    TestRequest {
        sent_by: SentBy::Sender(core()),
        request,
    }
}

/// The app-server's notification of a turn of `thread` starting or
/// completing.
fn turn_notice(method: &str, thread: &str, turn: &str) -> serde_json::Value {
    let status = if method == "turn/completed" {
        "completed"
    } else {
        "inProgress"
    };
    serde_json::json!({
        "method": method,
        "params": {
            "threadId": thread,
            "turn": { "id": turn, "items": [], "status": status, "error": null }
        }
    })
}

/// A call the seat's thread makes to the Awake gamma target: once it is
/// answered, every frame sent before it has been read.
fn fence(request_id: u64) -> serde_json::Value {
    tool_call_request(
        request_id,
        SEAT_THREAD,
        &format!("fence-{request_id}"),
        "fence",
    )
}

trait SeatsAwakeCodexSeat {
    /// The fixture's Codex seat 908786, Active, resting in pane w1:p3 as
    /// Herdr's screen shows it, its metaflow Awake as it at `seat_at`
    /// holding `queue`; gamma Awake as a flow Flow does not hold.
    fn awake_codex_seat(&self, queue: Vec<TestRequest>);
    fn seat_queue(&self) -> Vec<TestRequest>;
    /// How many times `request`, with its sender, was typed into the pane.
    fn typed_count(&self, request: &TestRequest) -> usize;
}

impl SeatsAwakeCodexSeat for NexusFixture {
    fn awake_codex_seat(&self, queue: Vec<TestRequest>) {
        use super::PromptFixture;
        self.accept_pane_operations(vec![self.settled_agent()], PromptFixture::Prompted("w1:p3"));
        let mut node = self.node();
        node.flow_lifecycle = signal_flow::FlowLifecycle::Pending;
        assert!(matches!(
            self.nexus.store.register_flow_in_role(
                node,
                Caller {
                    flow_id: "908786".into(),
                    aspect: Aspect::Mind,
                    power_level: PowerLevel::Medium,
                    model_name: "gpt-sol".into(),
                },
            ),
            Ok(crate::store::FlowRegistration::Registered(_))
        ));
        self.nexus.store.seed_metaflow(Metaflow {
            address: seat_at(),
            state: State::Awake("908786".into()),
            past: vec![],
            queue: queue
                .into_iter()
                .enumerate()
                .map(|(index, queued)| queue_entry(format!("seat-{index}"), queued))
                .collect(),
        });
        self.awake_target();
    }

    fn seat_queue(&self) -> Vec<TestRequest> {
        self.nexus
            .store
            .metaflow(&seat_at())
            .expect("seat readable")
            .expect("seat held")
            .queue
            .into_iter()
            .map(|entry| TestRequest {
                sent_by: entry.sent_by,
                request: match entry.message {
                    Message::HardAbrupt(request)
                    | Message::MiddleAbrupt(request)
                    | Message::Soft(request) => request,
                },
            })
            .collect()
    }

    fn typed_count(&self, request: &TestRequest) -> usize {
        usize::from(!self.seat_queue().contains(request))
    }
}

/// Runs a responder on the seat's thread whose one connection sends
/// `notices` and then the fence, and stops it once the fence is answered.
fn seat_responding(
    fixture: &mut NexusFixture,
    notices: Vec<serde_json::Value>,
    fence_id: u64,
) -> FakeAppServer {
    let mut requests = notices;
    requests.push(fence(fence_id));
    let fake = FakeAppServer::install(vec![FakeConnection {
        requests,
        closes: false,
        deaf: false,
    }]);
    fake.accept_queued_turns(SEAT_THREAD, 3..=12);
    let adapter = fixture.adapter("gpt-sol", &fake);
    let answered = responding(fixture, SEAT_THREAD, &adapter, || {
        answer_to(&fake.written_on(2, |m| m["id"] == fence_id), fence_id)
    });
    assert!(answered.is_some(), "the fence was answered");
    fake
}

/// The seat's first turn completion offers both queued requests to its
/// thread as one payload, in order; both leave the queue, typed once.
#[test]
fn a_codex_first_turn_completion_keeps_requests_until_native_receipts() {
    let mut fixture = NexusFixture::new();
    let psyche = from_core(Request::Psyche(flow_ethos::Said {
        context: "The living".into(),
        verbatim: "Make it so.".into(),
    }));
    let brief = from_core(Request::Order("Your brief.".into()));
    fixture.awake_codex_seat(vec![psyche.clone(), brief.clone()]);

    let fake = seat_responding(
        &mut fixture,
        vec![
            turn_notice("turn/started", SEAT_THREAD, "turn-first"),
            turn_notice("turn/completed", SEAT_THREAD, "turn-first"),
            turn_notice("turn/started", SEAT_THREAD, "turn-psyche"),
            turn_notice("turn/completed", SEAT_THREAD, "turn-psyche"),
        ],
        40,
    );

    assert_eq!(fixture.typed_count(&psyche), 1);
    assert_eq!(fixture.typed_count(&brief), 1);
    let texts = fake.queued_turn_texts();
    assert_eq!(texts.len(), 1, "one payload: {texts:?}");
    let typed = &texts[0];
    assert!(
        typed.find("Make it so.") < typed.find("Your brief."),
        "both, in order: {typed}"
    );
    assert!(!typed.contains("Psyche core Secondary"));
    assert_eq!(fixture.seat_queue(), vec![]);
}

/// A completion read twice cannot settle a request without native history.
#[test]
fn a_duplicated_turn_completed_delivers_nothing_twice() {
    let mut fixture = NexusFixture::new();
    let first = from_core(Request::Order("First.".into()));
    let second = from_core(Request::Order("Second.".into()));
    fixture.awake_codex_seat(vec![first.clone(), second.clone()]);

    let fake = seat_responding(
        &mut fixture,
        vec![
            turn_notice("turn/completed", SEAT_THREAD, "turn-first"),
            turn_notice("turn/completed", SEAT_THREAD, "turn-first"),
            serde_json::json!({
                "method": "thread/status/changed",
                "params": { "threadId": SEAT_THREAD, "status": { "type": "idle" } }
            }),
        ],
        41,
    );

    assert_eq!(fixture.typed_count(&first), 1);
    assert_eq!(fixture.typed_count(&second), 1, "in the same payload");
    assert_eq!(fixture.seat_queue(), vec![]);
    assert_eq!(fake.queued_turn_texts().len(), 1, "no duplicate handover");
}

/// Work queued while the seat is in a turn (here by a send call its own
/// thread makes to it) is held, not typed mid-turn; its turn completion
/// hands it over once.
#[test]
fn work_queued_while_a_codex_turn_runs_is_held_until_its_completion() {
    let mut fixture = NexusFixture::new();
    fixture.awake_codex_seat(vec![]);
    let to_self = send_request(42, SEAT_THREAD, "call-self", &seat_at(), "Order", "later");

    let _first = seat_responding(
        &mut fixture,
        vec![
            turn_notice("turn/started", SEAT_THREAD, "turn-running"),
            to_self,
        ],
        43,
    );
    let queued = TestRequest {
        sent_by: SentBy::Sender(seat_at()),
        request: Request::Order("later".into()),
    };
    assert_eq!(fixture.seat_queue(), vec![queued.clone()], "held queued");
    assert_eq!(fixture.typed_count(&queued), 0, "nothing typed mid-turn");

    let second = seat_responding(
        &mut fixture,
        vec![turn_notice("turn/completed", SEAT_THREAD, "turn-running")],
        44,
    );
    assert_eq!(fixture.typed_count(&queued), 1, "handed over at completion");
    assert_eq!(fixture.seat_queue(), vec![]);
    let texts = second.queued_turn_texts();
    assert_eq!(texts.len(), 1, "completion offers the request once");
    assert!(texts[0].contains("later"), "{texts:?}");
}

/// A seat its app-server shows in a turn is not drained, though Herdr's
/// screen shows it idle and a Stop is reported: a psyche and then a brief
/// stay queued.
#[test]
fn a_working_codex_seat_is_not_drained() {
    let mut fixture = NexusFixture::new();
    let psyche = from_core(Request::Psyche(flow_ethos::Said {
        context: "The living".into(),
        verbatim: "Words.".into(),
    }));
    let brief = from_core(Request::Order("Brief.".into()));
    fixture.awake_codex_seat(vec![psyche.clone(), brief.clone()]);

    seat_responding(
        &mut fixture,
        vec![turn_notice("turn/started", SEAT_THREAD, "turn-working")],
        45,
    );
    use crate::waking::SleepsAndWakesSeats;
    let at_stop = fixture.nexus.settle_stopped("908786");
    fixture.nexus.wake_due(seat_at());

    assert!(
        !matches!(at_stop, crate::waking::AtStop::Drained(_)),
        "{at_stop:?}"
    );
    assert_eq!(fixture.typed_count(&psyche), 0);
    assert_eq!(fixture.seat_queue(), vec![psyche, brief]);
}

/// A turn of the seat's thread as `thread/turns/list` answers it with
/// `itemsView: full`; `text` is its user message, when it has one.
fn history_turn(id: &str, status: &str, text: Option<&str>) -> serde_json::Value {
    let items = text
        .map(|text| {
            vec![serde_json::json!({
                "type": "userMessage", "id": format!("{id}-user"), "clientId": null,
                "content": [{ "type": "text", "text": text, "text_elements": [] }]
            })]
        })
        .unwrap_or_default();
    serde_json::json!({ "id": id, "items": items, "itemsView": "full", "status": status,
        "error": null, "startedAt": 1, "completedAt": null, "durationMs": null })
}

/// The seat's oldest request was prepared with its thread's newest turn as
/// anchor, and the Nexus stopped before `turn/start` answered. The store's
/// recovery makes it Uncertain.
fn crashed_before_receipt(fixture: &NexusFixture) -> TestRequest {
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery,
    };
    let prepared = fixture
        .nexus
        .store
        .prepare_oldest_delivery(
            &seat_at(),
            "908786",
            SEAT_THREAD,
            signal_flow::HarnessKind::Codex,
            NativeHistoryAnchor::CodexTurns(Some("turn-before".into())),
        )
        .unwrap();
    let PrepareQueueDelivery::Prepared(attempt) = prepared else {
        panic!("prepared: {prepared:?}")
    };
    assert_eq!(
        fixture
            .nexus
            .store
            .recover_outstanding_deliveries()
            .unwrap()
            .len(),
        1
    );
    let QueueEvidence::Logical(entry) = attempt.queue_evidence else {
        panic!("current queued request has logical lineage")
    };
    TestRequest {
        sent_by: entry.sent_by,
        request: match entry.message {
            Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request) => request,
        },
    }
}

fn codex_standings(fixture: &NexusFixture) -> Vec<meta_signal_flow::QueueDeliveryStanding> {
    use crate::Dispatches;
    match fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Query::ReadQueueDeliveries(seat_at()))
    {
        meta_signal_flow::Response::QueueDeliveriesRead(read) => read
            .queue_delivery_vector
            .into_iter()
            .map(|delivery| delivery.queue_delivery_standing)
            .collect(),
        other => panic!("not read: {other:?}"),
    }
}

/// The app-server took the turn before the crash: its history shows the
/// one turn after the anchor whose user message is exactly the request.
/// The restart settles it from that history and starts no second turn.
#[test]
fn a_codex_turn_a_crash_left_unacknowledged_is_found_in_thread_history() {
    use crate::composition::RendersQueuedWork;
    use crate::waking::ReconcilesQueueDelivery;
    let mut fixture = NexusFixture::new();
    let first = from_core(Request::Order("First.".into()));
    let second = from_core(Request::Order("Second.".into()));
    fixture.awake_codex_seat(vec![first.clone(), second.clone()]);
    let fake = FakeAppServer::install(vec![]);
    fixture.adapter("gpt-sol", &fake);
    let queued = crashed_before_receipt(&fixture);
    assert_eq!(queued, first);
    let first_text = [queue_entry("seat-0", first.clone())].prompt_text();
    fake.serve_turn_history(
        1,
        SEAT_THREAD,
        true,
        vec![
            history_turn("turn-other", "completed", Some("someone else")),
            history_turn("turn-queued", "completed", Some(&first_text)),
            history_turn("turn-before", "completed", Some(&first_text)),
        ],
    );

    fixture.nexus.recover_deliveries();
    assert_eq!(fixture.seat_queue(), vec![second]);
    assert!(fake.queued_turn_texts().is_empty(), "no second turn/start");
    assert_eq!(codex_standings(&fixture), vec![]);
    let reads = fake.written_on(1, |m| m["method"] == "thread/turns/list");
    assert!(
        reads
            .iter()
            .any(|m| m["method"] == "thread/turns/list" && m["params"]["itemsView"] == "full"),
        "{reads:?}"
    );
}

/// The thread is idle but no native user-message receipt identifies the
/// request. It remains uncertain and must not be resent.
#[test]
fn a_codex_turn_absent_from_an_idle_thread_remains_uncertain() {
    use crate::waking::ReconcilesQueueDelivery;
    let mut fixture = NexusFixture::new();
    let first = from_core(Request::Order("First.".into()));
    fixture.awake_codex_seat(vec![first.clone()]);
    let fake = FakeAppServer::install(vec![]);
    fixture.adapter("gpt-sol", &fake);
    crashed_before_receipt(&fixture);
    fake.serve_turn_history(
        1,
        SEAT_THREAD,
        true,
        vec![history_turn("turn-before", "completed", None)],
    );

    fixture.nexus.recover_deliveries();
    assert_eq!(
        codex_standings(&fixture),
        vec![meta_signal_flow::QueueDeliveryStanding::Uncertain(Some(
            meta_signal_flow::Undecided::AbsenceUnprovable
        ))]
    );
    assert_eq!(fixture.seat_queue(), vec![first.clone()], "still queued");

    assert!(fake.queued_turn_texts().is_empty(), "no unsafe resend");
    assert_eq!(fixture.seat_queue(), vec![first]);
}

/// History that cannot decide keeps the request Uncertain, names why, and
/// starts no turn: a thread still active keeps it outstanding until the
/// next drain; a history without the anchor parks it, so it no longer
/// holds the queue.
#[test]
fn an_undecidable_codex_turn_stays_uncertain_and_is_never_resent() {
    use crate::generated::operation::{Operation, Outcome};
    use crate::performing::Performs;
    use crate::waking::ReconcilesQueueDelivery;
    use meta_signal_flow::{QueueDeliveryStanding, Undecided};
    let mut fixture = NexusFixture::new();
    let first = from_core(Request::Order("First.".into()));
    fixture.awake_codex_seat(vec![first]);
    let fake = FakeAppServer::install(vec![]);
    fixture.adapter("gpt-sol", &fake);
    crashed_before_receipt(&fixture);

    fake.serve_turn_history(
        1,
        SEAT_THREAD,
        false,
        vec![history_turn("turn-before", "completed", None)],
    );
    fixture.nexus.recover_deliveries();
    assert_eq!(
        codex_standings(&fixture),
        vec![QueueDeliveryStanding::Uncertain(Some(
            Undecided::SeatNotResting
        ))]
    );

    fake.serve_turn_history(
        2,
        SEAT_THREAD,
        true,
        vec![history_turn("turn-unrelated", "completed", None)],
    );
    // The undecidable attempt is parked: it names why, is never resent,
    // and no longer holds the queue.
    assert_eq!(
        fixture.nexus.perform(Operation::Drain(seat_at())),
        Outcome::Drained(vec![])
    );
    assert_eq!(
        codex_standings(&fixture),
        vec![QueueDeliveryStanding::Uncertain(Some(
            Undecided::HistoryReplaced
        ))]
    );
    assert!(fake.queued_turn_texts().is_empty(), "never resent");
    assert_eq!(fixture.seat_queue(), vec![]);
}

/// A Nexus start whose reconciliation reads the history of a Codex seat
/// whose app-server takes the connection and never answers still serves
/// Metaflows meanwhile: native history is read off the dispatch gate.
#[test]
fn a_start_reading_an_unanswering_codex_history_still_serves_metaflows() {
    use crate::DispatchesSerially;
    use crate::store::queue_delivery::AccountsQueueDelivery;
    use crate::waking::ReconcilesQueueDelivery;
    let mut fixture = NexusFixture::new();
    fixture.awake_codex_seat(vec![from_core(Request::Order("First.".into()))]);
    let fake = FakeAppServer::install(vec![]);
    // Run 1 takes the connection and never answers it.
    std::fs::write(fake.directory.path().join("frames-1"), "").unwrap();
    fixture.adapter("gpt-sol", &fake);
    crashed_before_receipt(&fixture);
    // The typed payload is parked, as a drain leaves it once typed.
    let attempt = fixture
        .nexus
        .store
        .unsettled_deliveries(&seat_at())
        .unwrap()
        .remove(0);
    assert!(
        fixture
            .nexus
            .store
            .park_uncertain_delivery(&attempt.attempt_id)
            .unwrap()
    );
    std::thread::scope(|scope| {
        let recovery = scope.spawn(|| fixture.nexus.recover_deliveries());
        while fake.runs() == 0 {
            std::thread::yield_now();
        }
        let listed = fixture
            .nexus
            .dispatch_serially(signal_flow::Query::Metaflows, signal_flow::Origin::Unbound);
        assert!(
            matches!(listed, signal_flow::Response::Listed(_)),
            "{listed:?}"
        );
        assert!(
            !recovery.is_finished(),
            "Metaflows answered while the native read still waits"
        );
    });
}

/// A turn completion on another thread is not the seat's: nothing drains.
#[test]
fn a_completion_on_another_thread_drains_nothing() {
    let mut fixture = NexusFixture::new();
    let first = from_core(Request::Order("First.".into()));
    fixture.awake_codex_seat(vec![first.clone()]);
    seat_responding(
        &mut fixture,
        vec![
            turn_notice("turn/started", THREAD_B, "turn-elsewhere"),
            turn_notice("turn/completed", THREAD_B, "turn-elsewhere"),
            serde_json::json!({
                "method": "thread/status/changed",
                "params": { "threadId": THREAD_B, "status": { "type": "idle" } }
            }),
        ],
        47,
    );
    assert_eq!(fixture.typed_count(&first), 0);
    assert_eq!(fixture.seat_queue(), vec![first]);
}

/// An Uncertain Codex hand-over, resolved Absent as the meta socket serves
/// it; `first_event` is the thread's last turn event. Returns the fake
/// app-server and the fixture.
fn codex_resolved_absent(
    first_event: crate::codex_turns::TurnEvent,
) -> (NexusFixture, FakeAppServer, TestRequest) {
    use crate::Dispatches;
    use crate::DispatchesSerially;
    use crate::codex_turns::TracksCodexTurns;
    use meta_signal_flow::{QueueDeliveryResolution, QueueResolutionOutcome};
    let mut fixture = NexusFixture::new();
    let first = from_core(Request::Order("First.".into()));
    fixture.awake_codex_seat(vec![first.clone()]);
    let fake = FakeAppServer::install(vec![]);
    fixture.adapter("gpt-sol", &fake);
    crashed_before_receipt(&fixture);
    let attempt = match fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Query::ReadQueueDeliveries(seat_at()))
    {
        meta_signal_flow::Response::QueueDeliveriesRead(read) => {
            read.queue_delivery_vector[0].queue_attempt_id.clone()
        }
        other => panic!("not read: {other:?}"),
    };
    fixture.nexus.codex_turns.observe(SEAT_THREAD, first_event);
    fake.accept_queued_turns(SEAT_THREAD, 1..=2);
    let response = fixture.nexus.dispatch_meta_serially(
        meta_signal_flow::Query::ResolveQueueDelivery(QueueDeliveryResolution {
            queue_attempt_id: attempt,
            queue_resolution_outcome: QueueResolutionOutcome::Absent,
            queue_resolution_reason: "the owner looked".into(),
        }),
        Some(crate::tests::queue_drain::resolver()),
    );
    assert!(matches!(
        response,
        meta_signal_flow::Response::QueueDeliveryResolved(_)
    ));
    (fixture, fake, first)
}

/// A resting Codex seat may retry an owner-resolved-absent request once,
/// and never twice.
#[test]
fn a_resting_codex_seat_resolved_absent_is_handed_the_request_at_once_exactly_once() {
    use crate::codex_turns::{ThreadStatus, TurnEvent};
    use crate::waking::SleepsAndWakesSeats;
    let (fixture, fake, _) = codex_resolved_absent(TurnEvent::Status(ThreadStatus::Idle));
    assert_eq!(fake.queued_turn_texts().len(), 1, "handed over at once");
    fixture.nexus.drain_awake(&seat_at());
    assert_eq!(fake.queued_turn_texts().len(), 1, "never twice");
    assert_eq!(fixture.seat_queue(), vec![]);
}

/// An idle Awake Codex seat keeps its pane: an Order is handed to its live
/// thread once.
#[test]
fn a_request_to_an_idle_awake_codex_flow_is_delivered_into_its_pane() {
    use crate::codex_turns::{ThreadStatus, TracksCodexTurns, TurnEvent};
    use crate::waking::SleepsAndWakesSeats;
    let mut fixture = NexusFixture::new();
    fixture.awake_codex_seat(vec![]);
    let fake = FakeAppServer::install(vec![]);
    fixture.adapter("gpt-sol", &fake);
    fake.accept_queued_turns(SEAT_THREAD, 1..=2);
    fixture
        .nexus
        .codex_turns
        .observe(SEAT_THREAD, TurnEvent::Status(ThreadStatus::Idle));
    fixture.nexus.wake_due(seat_at());
    assert!(
        fake.queued_turn_texts().is_empty(),
        "nothing to hand while idle"
    );

    let order = from_core(Request::Order("Read the new ruling.".into()));
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_at(),
        state: State::Awake("908786".into()),
        past: vec![],
        queue: vec![QueueEntry {
            logical_delivery_id: "logical-delivery-test".into(),
            sent_by: order.sent_by,
            delivery: Delivery::Wake,
            message: Message::Soft(order.request),
        }],
    });
    fixture.nexus.wake_due(seat_at());
    fixture.nexus.wake_due(seat_at());

    let texts = fake.queued_turn_texts();
    assert_eq!(texts.len(), 1, "handed over once: {texts:?}");
    assert!(texts[0].contains("Read the new ruling."), "{texts:?}");
    assert_eq!(fixture.seat_queue(), vec![]);
    let seat = fixture.nexus.store.metaflow(&seat_at()).unwrap().unwrap();
    assert_eq!(seat.state, State::Awake("908786".into()));
}

/// A Codex seat in a turn waits for its completion.
#[test]
fn a_working_codex_seat_resolved_absent_waits_for_its_turn_end() {
    use crate::codex_turns::TurnEvent;
    let (fixture, fake, first) = codex_resolved_absent(TurnEvent::Started("turn-running".into()));
    assert!(fake.queued_turn_texts().is_empty(), "a turn runs");
    assert_eq!(fixture.seat_queue(), vec![first]);
}
