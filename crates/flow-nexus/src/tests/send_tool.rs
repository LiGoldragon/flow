//! The send tool against a fake Codex app-server: each connection Flow
//! opens is one run of a script that answers the WebSocket upgrade, prints
//! the frames prepared for that connection, records what the client wrote,
//! and stays open or closes. A responder's every connection is preceded by
//! the send-tool probe, so connection `n` of a responder is run `2n` of its
//! script, and run `2n - 1` is the probe.

use super::NexusFixture;
use crate::codex::{CodexAdapter, ResponderSignal, SelectsCodexEndpoint};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::send_tool::{
    HandlesSendCall, Queued_Data, SendCall, SendOutcome, SendRefusal, ToolCall, ToolCallKey,
};
use crate::send_tool::{RespondsToSendCalls, SendTool, ServesSendTool};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::send_call::KeepsSendCalls;
use crate::store::{AnnouncesLaunchChanges, RecordsFlowLifecycle, RegistersFlowIdentity};
use crate::{HoldsStopRequest, generated::send_tool::DeclaresSendTool};
use flow_ethos::State;
use signal_flow::{
    Address, Aspect, Caller, FlowNode, HerdrRoute, HerdrRouteSelection, Layer, Metaflow,
    PowerLevel, Request,
};
use std::{
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};

const THREAD_A: &str = "01a0b22c-0000-7000-8000-00000000000a";
const THREAD_B: &str = "01a0b22c-0000-7000-8000-00000000000b";
const TURN: &str = "01a0b22c-0000-7000-8000-0000000000f0";

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

/// The app-server's item/tool/call server request.
fn tool_call_request(request_id: u64, thread: &str, call: &str, body: &str) -> serde_json::Value {
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
                "target": { "aspect": "Mind", "topic": "gamma", "layer": "Secondary" },
                "kind": "Order",
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
            "kind": "Order",
            "body": body
        })
        .to_string(),
    }
}

/// What one subscribed connection of the fake does: the server requests it
/// sends after the initialize and resume answers, and whether it closes
/// once it has sent them.
struct FakeConnection {
    requests: Vec<serde_json::Value>,
    closes: bool,
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
            let mut frames = vec![
                initialized.clone(),
                frame(&serde_json::json!({ "id": 2, "result": { "thread": {} } })),
            ];
            frames.extend(connection.requests.iter().map(frame));
            std::fs::write(root.join(format!("frames-{}", run + 1)), frames.concat()).unwrap();
            if connection.closes {
                std::fs::write(root.join(format!("close-{}", run + 1)), "").unwrap();
            }
        }
        let executable = root.join("codex");
        FixtureExecutable {
            path: executable.clone(),
        }
        .install(&format!(
            "#!/bin/sh\nd='{root}'\nn=$(cat \"$d/count\" 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > \"$d/count\"\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nexec 3<&0; cat <&3 > \"$d/record-$n\" &\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\n[ -f \"$d/frames-$n\" ] || exit 1\nprintf '%b' \"$(cat \"$d/frames-$n\")\"\n[ -f \"$d/close-$n\" ] && exit 0\nsleep 30\n",
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
        let deadline = Instant::now() + Duration::from_secs(10);
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
}

/// The JSON messages a recorded client stream holds: each masked client
/// frame unmasked (RFC 6455 5.3).
trait ReadsClientFrames {
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

fn queued_text(sender: Address, target: Address) -> String {
    use datom_codec::Datomizable;
    use protos::{Compactable, Protosizable};
    SendOutcome::Queued(Queued_Data { sender, target })
        .datomize(Vec::new())
        .protosize()
        .compact()
}

trait SeatsCodexFlows {
    /// A live Codex flow `flow_id` on `thread`, in pane `pane`, launched
    /// for `model`, its metaflow at `at` Awake as it.
    fn codex_seat(&self, flow_id: &str, thread: &str, pane: &str, model: &str, at: Address);
    /// The target metaflow, Awake as a flow Flow does not hold, so nothing
    /// queued for it wakes anything.
    fn awake_target(&self);
    fn target_queue(&self) -> Vec<Request>;
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

    fn target_queue(&self) -> Vec<Request> {
        self.nexus
            .store
            .metaflow(&target())
            .expect("target readable")
            .expect("target held")
            .queue
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
    }]);
    let fake_b = FakeAppServer::install(vec![FakeConnection {
        requests: vec![tool_call_request(11, THREAD_B, "call-b", "from b")],
        closes: false,
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
    assert_eq!(
        answer_a["contentItems"][0]["text"],
        queued_text(sender_a(), target())
    );
    assert_eq!(
        answer_b["contentItems"][0]["text"],
        queued_text(sender_b(), target())
    );
    assert_eq!(
        fixture.target_queue(),
        vec![
            Request::Order("from a".into()),
            Request::Order("from b".into())
        ]
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
    assert_eq!(
        recorded.send_outcome,
        SendOutcome::Queued(Queued_Data {
            sender: sender_b(),
            target: target()
        })
    );
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
        fixture.nexus.handle_send_call(call_a.clone()).send_outcome,
        SendOutcome::Queued(_)
    ));
    // Retired: the replay finds the flow ended, though its call was queued.
    assert!(fixture.nexus.store.record_retired("a1a1a1").unwrap());
    let replayed = fixture.nexus.handle_send_call(call_a);
    assert_eq!(
        replayed.send_outcome,
        SendOutcome::Refused(SendRefusal::FlowEnded)
    );
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
        fixture.nexus.handle_send_call(call_b).send_outcome,
        SendOutcome::Refused(SendRefusal::NotAwake)
    );
    assert_eq!(
        fixture.target_queue(),
        vec![Request::Order("before retirement".into())]
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
    assert!(matches!(handled.send_outcome, SendOutcome::Queued(_)));
    // The app-server replays the pending call to the next subscriber.
    let fake = FakeAppServer::install(vec![FakeConnection {
        requests: vec![tool_call_request(13, THREAD_A, "call-a", "exactly once")],
        closes: false,
    }]);
    let adapter = fixture.adapter("gpt-sol", &fake);
    let answer = responding(&fixture, THREAD_A, &adapter, || {
        answer_to(&fake.written_on(2, |m| m["id"] == 13), 13)
    })
    .expect("the replayed call answered");
    assert_eq!(
        answer["contentItems"][0]["text"],
        queued_text(sender_a(), target())
    );
    assert_eq!(
        fixture.target_queue(),
        vec![Request::Order("exactly once".into())]
    );
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
            .handle_send_call(tool_call(THREAD_A, "call-a", "second"))
            .send_outcome,
        SendOutcome::Refused(SendRefusal::KeyReused)
    );
    assert_eq!(fixture.target_queue(), vec![Request::Order("first".into())]);
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
        },
        FakeConnection {
            requests: vec![tool_call_request(14, THREAD_A, "call-a", "after restart")],
            closes: false,
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
    .expect("the pending call answered on the second connection");
    assert_eq!(
        answer["contentItems"][0]["text"],
        queued_text(sender_a(), target())
    );
    assert_eq!(
        fixture.target_queue(),
        vec![Request::Order("after restart".into())]
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
        fixture.nexus.handle_send_call(call.clone()).send_outcome,
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

/// The integration witness, against a real scratch Codex app-server named
/// by `FLOW_SEND_TOOL_WITNESS_CODEX` (the codex client) and
/// `FLOW_SEND_TOOL_WITNESS_SOCKET` (its control socket), serving
/// `gpt-6-luna`. Flow's own adapter opens the thread, declaring the send
/// tool, with a first turn asking the model to call it once. A first
/// connection subscribes and receives the call; the Nexus handles it,
/// queueing and recording it, and the connection is closed before it
/// answers, as a Nexus dying there would leave it. The responder then
/// subscribes, the app-server replays the pending call to it, and it is
/// answered from its record: the target holds one request. No repository
/// holds such an app-server, so the test runs only when asked for by name
/// with `--ignored` and both variables set.
#[test]
#[ignore = "needs FLOW_SEND_TOOL_WITNESS_CODEX and FLOW_SEND_TOOL_WITNESS_SOCKET naming a scratch codex app-server"]
fn witness_a_real_call_is_queued_once_across_a_forced_reconnect() {
    use crate::codex::{ExchangesOnSubscription, OpensCodexThread, ProxyEvent, SubscribesThread};
    use crate::composition::ComposesLaunch;
    use crate::composition::tests::{fixture_composer, fixture_profile};
    use crate::generated::send_tool::ReadsToolCall;
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
                harness_kind: signal_flow::HarnessKind::Codex,
                launch_profile: profile,
            },
            vec![signal_flow::LeaveOut::CodexAgentsMd],
        )
        .expect("launch composes");
    let instruction = "Call the send tool exactly once with target aspect Mind, topic gamma, layer Secondary, kind Order and body witness-order. Then reply with the exact text the tool returned.";
    let digest = format!("{:x}", Sha256::digest(instruction.as_bytes()));
    launch.first_prompt_payload.first_prompt_body = instruction.into();
    launch.first_prompt_payload.prompt_sha256 = digest.clone();
    launch.first_prompt_payload.prompt_part_vector = vec![signal_flow::PromptPart {
        prompt_part_text: instruction.into(),
        prompt_sha256: digest.clone(),
    }];
    launch.target_receipt_request.prompt_sha256 = digest;

    let thread = adapter
        .open_thread_with_first_turn(&launch)
        .expect("the thread opens declaring the send tool");
    eprintln!("witness thread {thread}");
    fixture.codex_seat("a1a1a1", &thread, "w1:p5", "gpt-6-luna", sender_a());
    fixture.awake_target();

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
    eprintln!("witness first handling {:?}", handled.send_outcome);
    assert!(matches!(handled.send_outcome, SendOutcome::Queued(_)));
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
    let queue = fixture.target_queue();
    eprintln!("witness target queue {queue:?}");
    assert_eq!(queue, vec![Request::Order("witness-order".into())]);
    let recorded = fixture
        .nexus
        .store
        .send_call(&tool_call.tool_call_key)
        .unwrap()
        .expect("the call is recorded");
    eprintln!("witness record {:?}", recorded.send_outcome);
}
