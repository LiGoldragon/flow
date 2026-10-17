//! The Codex app-server is reached only through `codex app-server proxy`.
//! The proxy is a byte bridge, so this module owns its bounded WebSocket and
//! JSON-RPC conversation; it never falls back to a direct Unix-socket client.

use crate::composition::ValidatesComposedPrompt;
use crate::generated::send_tool::DeclaresSendTool;
use crate::send_tool::SendTool;
use crate::store::{CodexEndpointConfiguration, RuntimeConfiguration};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use signal_flow::{ComposedLaunch, HarnessKind, PromptDeliveryIntent, PromptDeliveryResult};
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct CodexAdapter {
    pub executable: PathBuf,
    pub socket: String,
    pub model: String,
    pub timeout: Duration,
    /// Exact cwd whose native Codex skill catalog is valid for this launch.
    pub workspace_root: PathBuf,
}

#[derive(Clone)]
pub struct CodexEndpoint {
    pub client_path: PathBuf,
    pub home: PathBuf,
    pub socket: String,
    pub transcript_root: PathBuf,
    pub model_names: BTreeSet<String>,
}

#[derive(Clone)]
pub struct CodexEndpoints {
    pub stable: CodexEndpoint,
    pub next: CodexEndpoint,
    pub timeout: Duration,
    pub workspace_root: PathBuf,
}

pub trait SelectsCodexEndpoint {
    const APP_SERVER_TIMEOUT: Duration = Duration::from_secs(10);
    /// Models selected by both endpoints select neither.
    fn overlapping_models(&self) -> BTreeSet<String>;
    fn endpoint_for(&self, model: &str) -> Result<&CodexEndpoint, CodexAdapterUnavailable>;
    fn adapter_for(&self, model: &str) -> Result<CodexAdapter, CodexAdapterUnavailable>;
}

impl SelectsCodexEndpoint for CodexEndpoints {
    fn overlapping_models(&self) -> BTreeSet<String> {
        self.stable
            .model_names
            .intersection(&self.next.model_names)
            .cloned()
            .collect()
    }

    fn endpoint_for(&self, model: &str) -> Result<&CodexEndpoint, CodexAdapterUnavailable> {
        let stable = self.stable.model_names.contains(model);
        let next = self.next.model_names.contains(model);
        match (stable, next) {
            (true, false) => Ok(&self.stable),
            (false, true) => Ok(&self.next),
            _ => Err(CodexAdapterUnavailable::Protocol(
                "Codex model does not select exactly one configured endpoint".into(),
            )),
        }
    }

    fn adapter_for(&self, model: &str) -> Result<CodexAdapter, CodexAdapterUnavailable> {
        let endpoint = self.endpoint_for(model)?;
        Ok(CodexAdapter {
            executable: endpoint.client_path.clone(),
            socket: endpoint.socket.clone(),
            model: model.to_owned(),
            timeout: self.timeout,
            workspace_root: self.workspace_root.clone(),
        })
    }
}

/// The harness a model runs on: Codex when a configured Codex endpoint
/// serves it, else Claude.
impl crate::generated::starter::DerivesHarness for CodexEndpoints {
    fn harness_for<N: crate::generated::starter::Textual>(&self, input: N) -> HarnessKind {
        match self.adapter_for(&input.text()) {
            Ok(_) => HarnessKind::Codex,
            Err(_) => HarnessKind::Claude,
        }
    }
}

impl From<&CodexEndpointConfiguration> for CodexEndpoint {
    fn from(configuration: &CodexEndpointConfiguration) -> Self {
        let home = PathBuf::from(&configuration.home);
        Self {
            client_path: PathBuf::from(&configuration.client_path),
            transcript_root: home.join("sessions"),
            home,
            socket: configuration.socket.clone(),
            model_names: configuration.model_names.iter().cloned().collect(),
        }
    }
}

impl From<&RuntimeConfiguration> for CodexEndpoints {
    fn from(configuration: &RuntimeConfiguration) -> Self {
        Self {
            stable: CodexEndpoint::from(&configuration.stable_codex),
            next: CodexEndpoint::from(&configuration.next_codex),
            timeout: Self::APP_SERVER_TIMEOUT,
            workspace_root: PathBuf::from(&configuration.source_root),
        }
    }
}

#[derive(Debug, Error)]
pub enum CodexAdapterUnavailable {
    #[error("Codex proxy could not start: {0}")]
    Proxy(String),
    #[error("Codex proxy did not upgrade to WebSocket: {0}")]
    Upgrade(String),
    #[error("Codex app-server did not reply before the configured timeout")]
    TimedOut,
    #[error("Codex app-server closed its proxy connection")]
    Closed,
    #[error("Codex app-server refused {method}: {detail}")]
    Refused { method: String, detail: String },
    #[error("Codex app-server protocol error: {0}")]
    Protocol(String),
    /// The deployed app-server was not shown to accept a thread's dynamic
    /// tools behind the experimental API, so no thread is opened without
    /// Flow's send tool.
    #[error("Codex app-server does not support the send tool: {0}")]
    SendToolUnsupported(String),
}

pub trait ConsumesResetCredit {
    fn consume_reset_credit(
        &self,
        request: &meta_signal_flow::ResetRequest,
    ) -> Result<meta_signal_flow::ResetOutcome, CodexAdapterUnavailable>;
}

/// Submits the part of the first USER message the durable intent last
/// counted as submitted, as one native turn of one text item, after
/// applying the launch's base instructions and exclusions to the attached
/// thread again and reading them back. The first part is already the
/// thread's first turn, given when the thread was opened, so for it nothing
/// more is sent; a later part follows the turn the transcript showed
/// completed. Durable one-shot gating belongs to the caller; this never
/// creates or replaces the Herdr-attached thread.
pub trait SubmitsBoundCodexPart {
    fn submit_bound_codex_part(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, CodexAdapterUnavailable>;
}

/// Opens a launch's Codex thread through the app-server before any TUI
/// attaches to it: the thread starts with the composed system prompt as its
/// base instructions and the launch's exclusions in its config, what the
/// app-server answers is read back against them, and the first part of the
/// first USER message (the whole message when it fits one Codex turn) is
/// submitted as the thread's first turn. The answer is the thread's id.
pub trait OpensCodexThread {
    fn open_thread_with_first_turn(
        &self,
        launch: &ComposedLaunch,
    ) -> Result<String, CodexAdapterUnavailable>;
}

/// Shows that the deployed app-server accepts a thread's dynamic tools only
/// behind the experimental API: on a connection initialized without it, a
/// `thread/start` declaring the send tool must be refused for that reason,
/// before any thread exists. Only then does Flow open a connection with the
/// experimental API. An app-server that answers otherwise, or starts the
/// (ephemeral) thread, does not support the tool: `SendToolUnsupported`; a
/// thread it started is archived at once, before the refusal.
pub trait ProbesSendTool {
    fn probe_send_tool(&self) -> Result<(), CodexAdapterUnavailable>;
}

/// Names the exact Herdr-bound thread through the app-server and reads the
/// name back from the same thread.
pub trait NamesBoundCodexThread {
    fn name_bound_thread(
        &self,
        native_session_id: &str,
        name: &str,
    ) -> Result<(), CodexAdapterUnavailable>;
}

struct ProxySession {
    child: Child,
    input: ChildStdin,
    replies: Receiver<ProxyEvent>,
}

/// One running `codex app-server proxy` whose WebSocket upgrade was sent;
/// its reader thread hands each event to the sink it was spawned with.
pub(crate) struct ProxyChild {
    pub(crate) child: Child,
    pub(crate) input: ChildStdin,
}

pub(crate) enum ProxyEvent {
    Upgraded,
    /// A ping the app-server sent; its payload is answered with a pong.
    Ping(Vec<u8>),
    Reply(Result<serde_json::Value, CodexAdapterUnavailable>),
}

/// One WebSocket frame read from the proxy: a text message, or a ping or
/// pong, which carry no JSON.
enum ProxyFrame {
    Text(serde_json::Value),
    Ping(Vec<u8>),
    Pong,
}

trait OpensCodexProxy {
    fn open_proxy(&self) -> Result<ProxySession, CodexAdapterUnavailable>;
}

/// Starts the proxy and sends its WebSocket upgrade; the reader thread
/// hands the upgrade, then every frame, to `sink` until the sink refuses or
/// the proxy closes.
pub(crate) trait SpawnsCodexProxy {
    fn spawn_proxy(
        &self,
        sink: impl FnMut(ProxyEvent) -> bool + Send + 'static,
    ) -> Result<ProxyChild, CodexAdapterUnavailable>;
}

trait ExchangesCodexRpc {
    fn notify(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable>;
    fn request(
        &mut self,
        id: u64,
        method: &str,
        params: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, CodexAdapterUnavailable>;
}

trait StopsCodexProxy {
    fn stop_proxy(&mut self);
}

impl SpawnsCodexProxy for CodexAdapter {
    fn spawn_proxy(
        &self,
        mut sink: impl FnMut(ProxyEvent) -> bool + Send + 'static,
    ) -> Result<ProxyChild, CodexAdapterUnavailable> {
        let mut child = Command::new(&self.executable)
            .args(["app-server", "proxy", "--sock", &self.socket])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|error| CodexAdapterUnavailable::Proxy(error.to_string()))?;
        let mut input = child
            .stdin
            .take()
            .ok_or_else(|| CodexAdapterUnavailable::Proxy("proxy stdin was unavailable".into()))?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| CodexAdapterUnavailable::Proxy("proxy stdout was unavailable".into()))?;
        #[cfg(test)]
        let nonce = *b"the sample nonce";
        #[cfg(not(test))]
        let nonce = {
            let mut nonce = [0; 16];
            std::fs::File::open("/dev/urandom")
                .and_then(|mut source| source.read_exact(&mut nonce))
                .map_err(|error| CodexAdapterUnavailable::Proxy(error.to_string()))?;
            nonce
        };
        let key = STANDARD.encode(nonce);
        let mut digest = Sha1::new();
        digest.update(key.as_bytes());
        digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let accept = STANDARD.encode(digest.finalize());
        let upgrade = format!(
            "GET / HTTP/1.1\r\nHost: flow-nexus\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        if let Err(error) = input
            .write_all(upgrade.as_bytes())
            .and_then(|_| input.flush())
        {
            let _ = child.kill();
            let _ = child.wait();
            return Err(CodexAdapterUnavailable::Proxy(error.to_string()));
        }
        thread::spawn(move || {
            let mut output = BufReader::new(output);
            let outcome = ProxySessionReader {
                output: &mut output,
            }
            .read_upgrade(&accept);
            if let Err(error) = outcome {
                let _ = sink(ProxyEvent::Reply(Err(error)));
                return;
            }
            if !sink(ProxyEvent::Upgraded) {
                return;
            }
            loop {
                let event = match (ProxySessionReader {
                    output: &mut output,
                })
                .read_frame()
                {
                    Ok(ProxyFrame::Text(message)) => ProxyEvent::Reply(Ok(message)),
                    Ok(ProxyFrame::Ping(payload)) => ProxyEvent::Ping(payload),
                    Ok(ProxyFrame::Pong) => continue,
                    Err(error) => ProxyEvent::Reply(Err(error)),
                };
                let closed = matches!(
                    event,
                    ProxyEvent::Reply(Err(CodexAdapterUnavailable::Closed))
                );
                if !sink(event) || closed {
                    return;
                }
            }
        });
        Ok(ProxyChild { child, input })
    }
}

impl OpensCodexProxy for CodexAdapter {
    fn open_proxy(&self) -> Result<ProxySession, CodexAdapterUnavailable> {
        let (sender, replies) = mpsc::channel();
        let ProxyChild { child, input } =
            self.spawn_proxy(move |event| sender.send(event).is_ok())?;
        let mut session = ProxySession {
            child,
            input,
            replies,
        };
        match session.replies.recv_timeout(self.timeout) {
            Ok(ProxyEvent::Reply(Err(error))) => {
                session.stop_proxy();
                Err(error)
            }
            Ok(ProxyEvent::Reply(Ok(_)) | ProxyEvent::Ping(_)) => {
                session.stop_proxy();
                Err(CodexAdapterUnavailable::Protocol(
                    "proxy sent a WebSocket message before completing its upgrade".into(),
                ))
            }
            Ok(ProxyEvent::Upgraded) => Ok(session),
            Err(_) => {
                session.stop_proxy();
                Err(CodexAdapterUnavailable::TimedOut)
            }
        }
    }
}

struct ProxySessionReader<'a, R: BufRead> {
    output: &'a mut R,
}

trait ReadsCodexProxy {
    fn read_upgrade(&mut self, accept: &str) -> Result<(), CodexAdapterUnavailable>;
    /// One whole frame. A frame past the size limit is read through and
    /// refused, so the stream stays at a frame boundary.
    fn read_frame(&mut self) -> Result<ProxyFrame, CodexAdapterUnavailable>;
}

impl<R: BufRead> ReadsCodexProxy for ProxySessionReader<'_, R> {
    fn read_upgrade(&mut self, accept: &str) -> Result<(), CodexAdapterUnavailable> {
        let mut header = Vec::new();
        loop {
            let mut line = Vec::new();
            let read = self
                .output
                .read_until(b'\n', &mut line)
                .map_err(|error| CodexAdapterUnavailable::Upgrade(error.to_string()))?;
            if read == 0 {
                return Err(CodexAdapterUnavailable::Closed);
            }
            header.extend_from_slice(&line);
            if header.len() > MAX_HEADER_BYTES {
                return Err(CodexAdapterUnavailable::Upgrade(
                    "response headers exceed limit".into(),
                ));
            }
            if header.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let text = String::from_utf8(header)
            .map_err(|error| CodexAdapterUnavailable::Upgrade(error.to_string()))?;
        let status = text.lines().next().unwrap_or_default();
        if !status.contains(" 101 ") {
            return Err(CodexAdapterUnavailable::Upgrade(status.into()));
        }
        if !text
            .lines()
            .any(|line| line.eq_ignore_ascii_case("upgrade: websocket"))
        {
            return Err(CodexAdapterUnavailable::Upgrade(
                "missing Upgrade: websocket".into(),
            ));
        }
        if !text
            .lines()
            .any(|line| line.eq_ignore_ascii_case("connection: upgrade"))
            || !text.lines().any(|line| {
                line.trim()
                    .eq_ignore_ascii_case(&format!("sec-websocket-accept: {accept}"))
            })
        {
            return Err(CodexAdapterUnavailable::Upgrade(
                "invalid WebSocket upgrade authentication".into(),
            ));
        }
        Ok(())
    }

    fn read_frame(&mut self) -> Result<ProxyFrame, CodexAdapterUnavailable> {
        let mut first = [0; 2];
        match self.output.read_exact(&mut first) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(CodexAdapterUnavailable::Closed);
            }
            Err(error) => return Err(CodexAdapterUnavailable::Protocol(error.to_string())),
        }
        let opcode = first[0] & 0x0f;
        if first[0] & 0x80 == 0 {
            return Err(CodexAdapterUnavailable::Protocol(
                "fragmented WebSocket frame".into(),
            ));
        }
        let masked = first[1] & 0x80 != 0;
        let mut length = u64::from(first[1] & 0x7f);
        if length == 126 {
            let mut bytes = [0; 2];
            self.output
                .read_exact(&mut bytes)
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
            length = u64::from(u16::from_be_bytes(bytes));
        } else if length == 127 {
            let mut bytes = [0; 8];
            self.output
                .read_exact(&mut bytes)
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
            length = u64::from_be_bytes(bytes);
        }
        let mut mask = [0; 4];
        if masked {
            self.output
                .read_exact(&mut mask)
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
        }
        if length > MAX_MESSAGE_BYTES as u64 {
            std::io::copy(&mut (&mut *self.output).take(length), &mut std::io::sink())
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
            return Err(CodexAdapterUnavailable::Protocol(
                "WebSocket message exceeds limit".into(),
            ));
        }
        let mut bytes = vec![0; length as usize];
        self.output
            .read_exact(&mut bytes)
            .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
        if masked {
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte ^= mask[index % mask.len()];
            }
        }
        match opcode {
            8 => Err(CodexAdapterUnavailable::Closed),
            9 => Ok(ProxyFrame::Ping(bytes)),
            10 => Ok(ProxyFrame::Pong),
            1 => serde_json::from_slice(&bytes)
                .map(ProxyFrame::Text)
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string())),
            _ => Err(CodexAdapterUnavailable::Protocol(
                "non-text WebSocket frame".into(),
            )),
        }
    }
}

impl ExchangesCodexRpc for ProxySession {
    fn notify(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable> {
        self.write_json(serde_json::json!({ "method": method, "params": params }))
    }

    fn request(
        &mut self,
        id: u64,
        method: &str,
        params: serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, CodexAdapterUnavailable> {
        self.write_json(serde_json::json!({ "id": id, "method": method, "params": params }))?;
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(CodexAdapterUnavailable::TimedOut)?;
            let reply = match self
                .replies
                .recv_timeout(remaining)
                .map_err(|_| CodexAdapterUnavailable::TimedOut)?
            {
                ProxyEvent::Reply(reply) => reply?,
                ProxyEvent::Ping(payload) => {
                    self.input.write_frame(0x8a, &payload)?;
                    continue;
                }
                ProxyEvent::Upgraded => {
                    return Err(CodexAdapterUnavailable::Protocol(
                        "duplicate WebSocket upgrade".into(),
                    ));
                }
            };
            // A server request or notification is no reply, whatever its
            // id: the app-server numbers its own requests apart from ours.
            if reply.get("method").is_some() || reply.get("id") != Some(&serde_json::json!(id)) {
                continue;
            }
            if let Some(error) = reply.get("error") {
                return Err(CodexAdapterUnavailable::Refused {
                    method: method.into(),
                    detail: error.to_string(),
                });
            }
            return reply.get("result").cloned().ok_or_else(|| {
                CodexAdapterUnavailable::Protocol(format!("{method} response has no result"))
            });
        }
    }
}

pub(crate) trait WritesCodexProxy {
    /// The masked payload length of a client WebSocket frame (RFC 6455
    /// 5.2): seven bits, else 126 and sixteen bits, else 127 and sixty-four
    /// bits, so one text frame carries a whole part.
    fn masked_length(length: usize) -> Vec<u8> {
        if length < 126 {
            vec![0x80 | length as u8]
        } else if let Ok(short) = u16::try_from(length) {
            let mut bytes = vec![0x80 | 126];
            bytes.extend_from_slice(&short.to_be_bytes());
            bytes
        } else {
            let mut bytes = vec![0x80 | 127];
            bytes.extend_from_slice(&(length as u64).to_be_bytes());
            bytes
        }
    }
    /// One masked client frame: `head` is its first byte (FIN and opcode).
    fn write_frame(&mut self, head: u8, payload: &[u8]) -> Result<(), CodexAdapterUnavailable>;
    fn write_json(&mut self, value: serde_json::Value) -> Result<(), CodexAdapterUnavailable> {
        let payload = serde_json::to_vec(&value)
            .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
        self.write_frame(0x81, &payload)
    }
}

impl WritesCodexProxy for ChildStdin {
    fn write_frame(&mut self, head: u8, payload: &[u8]) -> Result<(), CodexAdapterUnavailable> {
        let mut frame = vec![head];
        frame.extend(Self::masked_length(payload.len()));
        let mut mask = [0; 4];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut mask))
            .map_err(|error| CodexAdapterUnavailable::Proxy(error.to_string()))?;
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % mask.len()]),
        );
        self.write_all(&frame)
            .and_then(|_| self.flush())
            .map_err(|error| CodexAdapterUnavailable::Proxy(error.to_string()))
    }
}

impl WritesCodexProxy for ProxySession {
    fn write_frame(&mut self, head: u8, payload: &[u8]) -> Result<(), CodexAdapterUnavailable> {
        self.input.write_frame(head, payload)
    }
}

impl StopsCodexProxy for ProxySession {
    fn stop_proxy(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub trait LocatesFlowDirectory {
    /// The flow's directory under the configured source root.
    fn flow_directory(&self, flow_id: &str) -> PathBuf;
}

impl LocatesFlowDirectory for CodexAdapter {
    fn flow_directory(&self, flow_id: &str) -> PathBuf {
        self.workspace_root.join("flows").join(flow_id)
    }
}

trait OpensBoundCodexSession {
    fn initialize_bound_session(&self) -> Result<ProxySession, CodexAdapterUnavailable> {
        self.initialize_session(serde_json::Value::Null)
    }
    /// Initializes a connection declaring `capabilities`.
    fn initialize_session(
        &self,
        capabilities: serde_json::Value,
    ) -> Result<ProxySession, CodexAdapterUnavailable>;
    /// The `turn/start` of the part the intent last counted as submitted:
    /// its text as the one input item.
    fn bound_turn_params(
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Option<serde_json::Value>;
    /// The thread config a launch's Codex exclusions and effort make, each
    /// leave-out as the setting its TUI switch names.
    fn exclusion_config(launch: &ComposedLaunch) -> serde_json::Map<String, serde_json::Value>;
    /// Reads back what `thread/start` or `thread/resume` answered for the
    /// thread: its id, model and reasoning effort are the launch's, and no
    /// instruction source (AGENTS.md) is loaded when the launch left
    /// project instructions out. The answer schema (codex app-server v2,
    /// ThreadStartResponse and ThreadResumeResponse) names no field for the
    /// skill listing or the other exclusions; their witness is the receipt,
    /// which refuses a request that carried one.
    fn read_back_thread(
        launch: &ComposedLaunch,
        thread_id: &str,
        method: &str,
        response: &serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable>;
    /// Applies the launch's base instructions, exclusions and flow
    /// environment again once Herdr's TUI has attached to the thread, since
    /// a TUI resume replaces the thread's settings, and reads them back.
    fn configure_bound_thread(
        &self,
        session: &mut ProxySession,
        request_id: u64,
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Result<(), CodexAdapterUnavailable>;
}

impl OpensBoundCodexSession for CodexAdapter {
    fn initialize_session(
        &self,
        capabilities: serde_json::Value,
    ) -> Result<ProxySession, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let mut params = serde_json::json!({
            "clientInfo": {
                "name": "flow-nexus",
                "version": env!("CARGO_PKG_VERSION")
            }
        });
        if !capabilities.is_null() {
            params["capabilities"] = capabilities;
        }
        if let Err(error) = (|| {
            session.request(1, "initialize", params, self.timeout)?;
            session.notify("initialized", serde_json::Value::Null)
        })() {
            session.stop_proxy();
            return Err(error);
        }
        Ok(session)
    }

    fn bound_turn_params(
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Option<serde_json::Value> {
        let index = usize::try_from(intent.parts_submitted.checked_sub(1)?).ok()?;
        let part = launch.first_prompt_payload.prompt_part_vector.get(index)?;
        Some(serde_json::json!({
            "threadId": intent.native_session_id,
            "effort": intent.effort,
            "model": intent.model_name,
            "input": [{
                "type": "text",
                "text": part.prompt_part_text,
                "text_elements": []
            }],
            "turnTrigger": "flow-nexus"
        }))
    }

    fn exclusion_config(launch: &ComposedLaunch) -> serde_json::Map<String, serde_json::Value> {
        let mut config = serde_json::Map::new();
        for leave_out in &launch.leave_outs {
            let (key, value) = match leave_out {
                signal_flow::LeaveOut::CodexAgentsMd => {
                    ("project_doc_max_bytes", serde_json::json!(0))
                }
                signal_flow::LeaveOut::CodexSkillListing => {
                    ("skills.include_instructions", serde_json::json!(false))
                }
                signal_flow::LeaveOut::CodexCollaborationMode => (
                    "include_collaboration_mode_instructions",
                    serde_json::json!(false),
                ),
                signal_flow::LeaveOut::CodexEnvironmentContext => {
                    ("include_environment_context", serde_json::json!(false))
                }
                signal_flow::LeaveOut::CodexPermissions => {
                    ("include_permissions_instructions", serde_json::json!(false))
                }
                signal_flow::LeaveOut::CodexDeveloperInstructions => {
                    // The multi-agent role and mode developer messages
                    // codex 0.161.0-alpha.2 sends a v2 multi-agent model
                    // are its root-agent usage hint and its mode hint; an
                    // empty hint is no message. A per-thread
                    // `features.multi_agent` setting does not remove them.
                    for hint in [
                        "features.multi_agent_v2.root_agent_usage_hint_text",
                        "features.multi_agent_v2.multi_agent_mode_hint_text",
                    ] {
                        config.insert(hint.into(), serde_json::json!(""));
                    }
                    ("developer_instructions", serde_json::json!(""))
                }
                _ => continue,
            };
            config.insert(key.into(), value);
        }
        config.insert(
            "model_reasoning_effort".into(),
            serde_json::json!(launch.launch_profile.effort),
        );
        config
    }

    fn read_back_thread(
        launch: &ComposedLaunch,
        thread_id: &str,
        method: &str,
        response: &serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable> {
        if response
            .pointer("/thread/id")
            .and_then(serde_json::Value::as_str)
            != Some(thread_id)
        {
            return Err(CodexAdapterUnavailable::Protocol(format!(
                "{method} did not return the launch's thread"
            )));
        }
        if response.get("model").and_then(serde_json::Value::as_str)
            != Some(launch.launch_profile.model_name.as_str())
            || response
                .get("reasoningEffort")
                .and_then(serde_json::Value::as_str)
                != Some(launch.launch_profile.effort.as_str())
        {
            return Err(CodexAdapterUnavailable::Protocol(format!(
                "{method} readback differs from the launch's model or effort"
            )));
        }
        if launch
            .leave_outs
            .contains(&signal_flow::LeaveOut::CodexAgentsMd)
        {
            let sources = response
                .get("instructionSources")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    CodexAdapterUnavailable::Protocol(format!(
                        "{method} answered no instruction sources"
                    ))
                })?;
            if !sources.is_empty() {
                return Err(CodexAdapterUnavailable::Protocol(format!(
                    "{method} loaded project instructions the launch left out"
                )));
            }
        }
        Ok(())
    }

    fn configure_bound_thread(
        &self,
        session: &mut ProxySession,
        request_id: u64,
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Result<(), CodexAdapterUnavailable> {
        let mut config = Self::exclusion_config(launch);
        config.insert(
            "shell_environment_policy".into(),
            serde_json::json!({ "inherit": "core", "set": {
                "FLOW_ID": intent.flow_id,
                "FLOW_DIRECTORY": self.flow_directory(&intent.flow_id)
            }}),
        );
        let response = session.request(
            request_id,
            "thread/resume",
            serde_json::json!({
                "threadId": intent.native_session_id,
                "model": intent.model_name,
                "baseInstructions": launch.system_prompt.system_prompt_text,
                "config": config
            }),
            self.timeout,
        )?;
        Self::read_back_thread(
            launch,
            &intent.native_session_id,
            "thread/resume",
            &response,
        )
    }
}

impl OpensCodexThread for CodexAdapter {
    fn open_thread_with_first_turn(
        &self,
        launch: &ComposedLaunch,
    ) -> Result<String, CodexAdapterUnavailable> {
        let first = launch.first_prompt_payload.prompt_part_vector.first();
        let Some(first) = first.filter(|_| {
            launch.has_canonical_first_prompt()
                && launch.harness_kind == HarnessKind::Codex
                && self.model == launch.launch_profile.model_name
        }) else {
            return Err(CodexAdapterUnavailable::Protocol(
                "the composed launch is no Codex launch this endpoint serves".into(),
            ));
        };
        // The send tool is declared behind the experimental API, which is
        // opened only once the app-server is shown to gate the tool on it.
        self.probe_send_tool()?;
        let mut session =
            self.initialize_session(serde_json::json!({ "experimentalApi": true }))?;
        let result = (|| {
            let started = session
                .request(
                    2,
                    "thread/start",
                    serde_json::json!({
                        "model": launch.launch_profile.model_name,
                        "cwd": self.workspace_root,
                        "approvalPolicy": "never",
                        "sandbox": "danger-full-access",
                        "baseInstructions": launch.system_prompt.system_prompt_text,
                        "ephemeral": false,
                        "config": Self::exclusion_config(launch),
                        "dynamicTools": [SendTool::tool_spec()]
                    }),
                    self.timeout,
                )
                .map_err(<CodexAdapterUnavailable as SendToolRefusal>::named)?;
            let thread = started
                .pointer("/thread/id")
                .and_then(serde_json::Value::as_str)
                .filter(|thread| !thread.is_empty())
                .ok_or_else(|| {
                    CodexAdapterUnavailable::Protocol("thread/start returned no thread id".into())
                })?
                .to_owned();
            Self::read_back_thread(launch, &thread, "thread/start", &started)?;
            session.request(
                3,
                "turn/start",
                serde_json::json!({
                    "threadId": thread,
                    "effort": launch.launch_profile.effort,
                    "model": launch.launch_profile.model_name,
                    "input": [{
                        "type": "text",
                        "text": first.prompt_part_text,
                        "text_elements": []
                    }],
                    "turnTrigger": "flow-nexus"
                }),
                self.timeout,
            )?;
            Ok(thread)
        })();
        session.stop_proxy();
        result
    }
}

impl SubmitsBoundCodexPart for CodexAdapter {
    fn submit_bound_codex_part(
        &self,
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, CodexAdapterUnavailable> {
        let part_hashes = launch
            .first_prompt_payload
            .prompt_part_vector
            .iter()
            .map(|part| part.prompt_sha256.clone())
            .collect::<Vec<_>>();
        let params = Self::bound_turn_params(launch, intent);
        if !launch.has_canonical_first_prompt()
            || launch.harness_kind != HarnessKind::Codex
            || intent.harness_kind != HarnessKind::Codex
            || intent.launch_request_id != launch.launch_profile.launch_request_id
            || intent.prompt_sha256 != launch.first_prompt_payload.prompt_sha256
            || intent.prompt_sha256_vector != part_hashes
            || intent.model_name != launch.launch_profile.model_name
            || intent.effort != launch.launch_profile.effort
            || self.model != intent.model_name
        {
            return Err(CodexAdapterUnavailable::Protocol(
                "Codex part intent does not match the composed launch".into(),
            ));
        }
        let Some(params) = params else {
            return Err(CodexAdapterUnavailable::Protocol(
                "no part is counted as submitted".into(),
            ));
        };
        let mut session = self.initialize_bound_session()?;
        let result = (|| {
            // The attached TUI replaced the thread's settings: they are
            // applied again and read back before anything more is sent.
            self.configure_bound_thread(&mut session, 2, launch, intent)?;
            // The first part is the thread's first turn, given before the
            // TUI attached. A later part follows the turn the transcript
            // showed completed.
            if intent.parts_submitted > 1 {
                session.request(3, "turn/start", params, self.timeout)?;
            }
            Ok(PromptDeliveryResult::Ambiguous(intent.clone()))
        })();
        session.stop_proxy();
        result
    }
}

/// Reads an app-server refusal of a `thread/start` that declared the send
/// tool: the refusal codex 0.161.0-alpha.2 gives a connection without the
/// experimental API (JSON-RPC -32600, "thread/start.dynamicTools requires
/// experimentalApi capability") is the gate the probe looks for; any
/// refusal naming the dynamic tools is the tool unsupported.
trait SendToolRefusal {
    const GATE_CODE: i64 = -32600;
    /// Whether a refusal's detail is the experimental-API gate on the tool.
    fn is_tool_gate(detail: &str) -> bool;
    /// A refusal naming the dynamic tools, as `SendToolUnsupported`.
    fn named(error: CodexAdapterUnavailable) -> CodexAdapterUnavailable;
}

impl SendToolRefusal for CodexAdapterUnavailable {
    fn is_tool_gate(detail: &str) -> bool {
        serde_json::from_str::<serde_json::Value>(detail).is_ok_and(|error| {
            error.get("code").and_then(serde_json::Value::as_i64) == Some(Self::GATE_CODE)
                && error
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|message| {
                        message.contains("dynamicTools") && message.contains("experimentalApi")
                    })
        })
    }

    fn named(error: CodexAdapterUnavailable) -> CodexAdapterUnavailable {
        match error {
            CodexAdapterUnavailable::Refused { method, detail }
                if detail.contains("dynamicTools") =>
            {
                CodexAdapterUnavailable::SendToolUnsupported(format!("{method}: {detail}"))
            }
            error => error,
        }
    }
}

impl ProbesSendTool for CodexAdapter {
    fn probe_send_tool(&self) -> Result<(), CodexAdapterUnavailable> {
        let mut session = self.initialize_bound_session()?;
        let probed = session.request(
            2,
            "thread/start",
            serde_json::json!({
                "cwd": self.workspace_root,
                "ephemeral": true,
                "dynamicTools": [SendTool::tool_spec()]
            }),
            self.timeout,
        );
        // An app-server that started the probe's thread is archived of it at
        // once, on the same connection, before the refusal.
        let archived = match &probed {
            Ok(started) => started
                .pointer("/thread/id")
                .and_then(serde_json::Value::as_str)
                .map(|thread| {
                    session
                        .request(
                            3,
                            "thread/archive",
                            serde_json::json!({ "threadId": thread }),
                            self.timeout,
                        )
                        .is_ok()
                }),
            Err(_) => None,
        };
        session.stop_proxy();
        match probed {
            Err(CodexAdapterUnavailable::Refused { detail, .. })
                if <CodexAdapterUnavailable as SendToolRefusal>::is_tool_gate(&detail) =>
            {
                Ok(())
            }
            Err(CodexAdapterUnavailable::Refused { method, detail }) => Err(
                CodexAdapterUnavailable::SendToolUnsupported(format!("{method}: {detail}")),
            ),
            Ok(_) => Err(CodexAdapterUnavailable::SendToolUnsupported(format!(
                "thread/start accepted dynamic tools without the experimental API; its thread {}",
                match archived {
                    Some(true) => "was archived",
                    Some(false) => "was not archived",
                    None => "named no id",
                }
            ))),
            Err(error) => Err(error),
        }
    }
}

impl NamesBoundCodexThread for CodexAdapter {
    fn name_bound_thread(
        &self,
        native_session_id: &str,
        name: &str,
    ) -> Result<(), CodexAdapterUnavailable> {
        let mut session = self.initialize_bound_session()?;
        let result = (|| {
            session.request(
                2,
                "thread/name/set",
                serde_json::json!({ "threadId": native_session_id, "name": name }),
                self.timeout,
            )?;
            let read = session.request(
                3,
                "thread/read",
                serde_json::json!({ "threadId": native_session_id, "includeTurns": false }),
                self.timeout,
            )?;
            let thread = read.get("thread").unwrap_or(&read);
            if thread.get("id").and_then(serde_json::Value::as_str) != Some(native_session_id)
                || thread.get("name").and_then(serde_json::Value::as_str) != Some(name)
            {
                return Err(CodexAdapterUnavailable::Protocol(
                    "Codex thread name readback differs from the set title".into(),
                ));
            }
            Ok(())
        })();
        session.stop_proxy();
        result
    }
}

/// What reaches a send-tool responder: an event of one of its connections,
/// named by the connection's generation, or the order to stop.
pub(crate) enum ResponderSignal {
    Proxy(u64, ProxyEvent),
    Stop,
}

/// Why a responder's connection ended.
pub(crate) enum SubscriptionEnd {
    /// The responder was told to stop.
    Stopped,
    Failed(CodexAdapterUnavailable),
}

/// One connection subscribed to one thread: initialized with the
/// experimental API once the app-server was shown to gate the send tool on
/// it, then `thread/resume`d, which subscribes it to the thread's server
/// requests, a call left pending replayed to it. It starts no turn and
/// carries no model content. `early` holds the server requests that arrived
/// before the resume was answered.
pub(crate) struct ThreadSubscription {
    pub(crate) proxy: ProxyChild,
    pub(crate) generation: u64,
    pub(crate) early: Vec<serde_json::Value>,
    timeout: Duration,
}

pub(crate) trait SubscribesThread {
    fn subscribe_thread(
        &self,
        thread: &str,
        generation: u64,
        signals: &mpsc::Sender<ResponderSignal>,
        inbox: &Receiver<ResponderSignal>,
    ) -> Result<ThreadSubscription, SubscriptionEnd>;
}

pub(crate) trait ExchangesOnSubscription {
    /// The next event of this connection; a ping is answered and skipped,
    /// another generation's event dropped. `deadline` bounds the wait.
    fn next_event(
        &mut self,
        inbox: &Receiver<ResponderSignal>,
        deadline: Option<Instant>,
    ) -> Result<ProxyEvent, SubscriptionEnd>;
    /// One request and its answer; a server request met meanwhile is kept
    /// in `early`, a notification dropped.
    fn exchange(
        &mut self,
        inbox: &Receiver<ResponderSignal>,
        id: u64,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, SubscriptionEnd>;
    /// Answers one server request.
    fn answer(
        &mut self,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable>;
    fn close(&mut self);
}

impl ExchangesOnSubscription for ThreadSubscription {
    fn next_event(
        &mut self,
        inbox: &Receiver<ResponderSignal>,
        deadline: Option<Instant>,
    ) -> Result<ProxyEvent, SubscriptionEnd> {
        loop {
            let signal = match deadline {
                None => inbox.recv().map_err(|_| SubscriptionEnd::Stopped)?,
                Some(deadline) => {
                    let remaining = deadline
                        .checked_duration_since(Instant::now())
                        .ok_or(SubscriptionEnd::Failed(CodexAdapterUnavailable::TimedOut))?;
                    match inbox.recv_timeout(remaining) {
                        Ok(signal) => signal,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            return Err(SubscriptionEnd::Failed(CodexAdapterUnavailable::TimedOut));
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            return Err(SubscriptionEnd::Stopped);
                        }
                    }
                }
            };
            match signal {
                ResponderSignal::Stop => return Err(SubscriptionEnd::Stopped),
                ResponderSignal::Proxy(generation, _) if generation != self.generation => {}
                ResponderSignal::Proxy(_, ProxyEvent::Ping(payload)) => self
                    .proxy
                    .input
                    .write_frame(0x8a, &payload)
                    .map_err(SubscriptionEnd::Failed)?,
                ResponderSignal::Proxy(_, event) => return Ok(event),
            }
        }
    }

    fn exchange(
        &mut self,
        inbox: &Receiver<ResponderSignal>,
        id: u64,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, SubscriptionEnd> {
        self.proxy
            .input
            .write_json(serde_json::json!({ "id": id, "method": method, "params": params }))
            .map_err(SubscriptionEnd::Failed)?;
        let deadline = Instant::now() + self.timeout;
        loop {
            let message = match self.next_event(inbox, Some(deadline))? {
                ProxyEvent::Reply(Ok(message)) => message,
                ProxyEvent::Reply(Err(error)) => return Err(SubscriptionEnd::Failed(error)),
                ProxyEvent::Upgraded | ProxyEvent::Ping(_) => continue,
            };
            if message.get("method").is_some() {
                if message.get("id").is_some() {
                    self.early.push(message);
                }
                continue;
            }
            if message.get("id") != Some(&serde_json::json!(id)) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(SubscriptionEnd::Failed(CodexAdapterUnavailable::Refused {
                    method: method.into(),
                    detail: error.to_string(),
                }));
            }
            return message.get("result").cloned().ok_or_else(|| {
                SubscriptionEnd::Failed(CodexAdapterUnavailable::Protocol(format!(
                    "{method} response has no result"
                )))
            });
        }
    }

    fn answer(
        &mut self,
        id: serde_json::Value,
        result: serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable> {
        self.proxy
            .input
            .write_json(serde_json::json!({ "id": id, "result": result }))
    }

    fn close(&mut self) {
        let _ = self.proxy.child.kill();
        let _ = self.proxy.child.wait();
    }
}

impl SubscribesThread for CodexAdapter {
    fn subscribe_thread(
        &self,
        thread: &str,
        generation: u64,
        signals: &mpsc::Sender<ResponderSignal>,
        inbox: &Receiver<ResponderSignal>,
    ) -> Result<ThreadSubscription, SubscriptionEnd> {
        self.probe_send_tool().map_err(SubscriptionEnd::Failed)?;
        let sink = signals.clone();
        let proxy = self
            .spawn_proxy(move |event| sink.send(ResponderSignal::Proxy(generation, event)).is_ok())
            .map_err(SubscriptionEnd::Failed)?;
        let mut subscription = ThreadSubscription {
            proxy,
            generation,
            early: Vec::new(),
            timeout: self.timeout,
        };
        let subscribed = (|| {
            match subscription.next_event(inbox, Some(Instant::now() + self.timeout))? {
                ProxyEvent::Upgraded => {}
                ProxyEvent::Reply(Err(error)) => return Err(SubscriptionEnd::Failed(error)),
                ProxyEvent::Reply(Ok(_)) | ProxyEvent::Ping(_) => {
                    return Err(SubscriptionEnd::Failed(CodexAdapterUnavailable::Protocol(
                        "proxy sent a WebSocket message before completing its upgrade".into(),
                    )));
                }
            }
            subscription.exchange(
                inbox,
                1,
                "initialize",
                serde_json::json!({
                    "clientInfo": { "name": "flow-nexus", "version": env!("CARGO_PKG_VERSION") },
                    "capabilities": { "experimentalApi": true }
                }),
            )?;
            subscription
                .proxy
                .input
                .write_json(serde_json::json!({ "method": "initialized", "params": null }))
                .map_err(SubscriptionEnd::Failed)?;
            subscription.exchange(
                inbox,
                2,
                "thread/resume",
                serde_json::json!({ "threadId": thread, "excludeTurns": true }),
            )?;
            Ok(())
        })();
        match subscribed {
            Ok(()) => Ok(subscription),
            Err(end) => {
                subscription.close();
                Err(end)
            }
        }
    }
}

/// Accounts for a thread Flow opened before its harness start was refused:
/// what its first turn had come to, read once, and its archiving through
/// the app-server. The account is given whatever the app-server answers.
pub trait AccountsOpenedThread {
    fn account_opened_thread(&self, thread: &str) -> signal_flow::OpenedThread;
}

impl AccountsOpenedThread for CodexAdapter {
    fn account_opened_thread(&self, thread: &str) -> signal_flow::OpenedThread {
        let unread = signal_flow::OpenedThread {
            native_session_id: thread.to_owned(),
            first_turn: signal_flow::FirstTurn::Unread,
            thread_archive: signal_flow::ThreadArchive::ArchiveRefused,
        };
        let Ok(mut session) = self.initialize_bound_session() else {
            return unread;
        };
        let first_turn = session
            .request(
                2,
                "thread/turns/list",
                serde_json::json!({ "threadId": thread, "limit": 1, "sortDirection": "asc" }),
                self.timeout,
            )
            .ok()
            .and_then(|turns| {
                turns
                    .pointer("/data/0/status")
                    .and_then(serde_json::Value::as_str)
                    .map(|status| match status {
                        "completed" => signal_flow::FirstTurn::Answered,
                        "inProgress" => signal_flow::FirstTurn::InProgress,
                        _ => signal_flow::FirstTurn::Unanswered,
                    })
            })
            .unwrap_or(signal_flow::FirstTurn::Unread);
        let thread_archive = match session.request(
            3,
            "thread/archive",
            serde_json::json!({ "threadId": thread }),
            self.timeout,
        ) {
            Ok(_) => signal_flow::ThreadArchive::Archived,
            Err(_) => signal_flow::ThreadArchive::ArchiveRefused,
        };
        session.stop_proxy();
        signal_flow::OpenedThread {
            first_turn,
            thread_archive,
            ..unread
        }
    }
}

impl ConsumesResetCredit for CodexAdapter {
    fn consume_reset_credit(
        &self,
        request: &meta_signal_flow::ResetRequest,
    ) -> Result<meta_signal_flow::ResetOutcome, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let result = (|| {
            session.request(
                1,
                "initialize",
                serde_json::json!({
                    "clientInfo": { "name": "flow-nexus", "version": env!("CARGO_PKG_VERSION") }
                }),
                self.timeout,
            )?;
            session.notify("initialized", serde_json::Value::Null)?;
            let credit_id = match &request.credit_selection {
                meta_signal_flow::CreditSelection::Next => serde_json::Value::Null,
                meta_signal_flow::CreditSelection::Specific(value) => {
                    serde_json::Value::String(value.clone())
                }
            };
            let response = session.request(
                2,
                "account/rateLimitResetCredit/consume",
                serde_json::json!({
                    "idempotencyKey": request.idempotency_key,
                    "creditId": credit_id,
                }),
                self.timeout,
            )?;
            match response.get("outcome").and_then(serde_json::Value::as_str) {
                Some("reset") => Ok(meta_signal_flow::ResetOutcome::Reset),
                Some("nothingToReset") => Ok(meta_signal_flow::ResetOutcome::NothingToReset),
                Some("noCredit") => Ok(meta_signal_flow::ResetOutcome::NoCredit),
                Some("alreadyRedeemed") => Ok(meta_signal_flow::ResetOutcome::AlreadyRedeemed),
                _ => Err(CodexAdapterUnavailable::Protocol(
                    "reset-credit response has an unknown outcome".into(),
                )),
            }
        })();
        session.stop_proxy();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use std::sync::Mutex;

    static FAKE_PROXY_LOCK: Mutex<()> = Mutex::new(());

    struct FakeProxy;

    trait BuildsFakeProxy {
        fn websocket_frame(&self, json: &str) -> String;
        fn executable(&self, frames: &[String]) -> String;
    }

    impl BuildsFakeProxy for FakeProxy {
        fn websocket_frame(&self, json: &str) -> String {
            let mut bytes = vec![0x81];
            match u8::try_from(json.len()) {
                Ok(short) if short < 126 => bytes.push(short),
                _ => {
                    bytes.push(126);
                    bytes.extend_from_slice(&(json.len() as u16).to_be_bytes());
                }
            }
            bytes.extend_from_slice(json.as_bytes());
            bytes.iter().map(|byte| format!("\\{:03o}", byte)).collect()
        }

        fn executable(&self, frames: &[String]) -> String {
            format!(
                "#!/bin/sh\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\nprintf '%b' '{}'\nsleep 2\n",
                frames.join("")
            )
        }
    }

    /// A fake app-server that also keeps every frame the client sent, so a
    /// test reads back the requests a launch made, in order.
    struct RecordingFakeProxy;

    trait InstallsRecordingFakeProxy {
        fn install(&self, frames: &[String]) -> (tempfile::TempDir, PathBuf, PathBuf);
        /// A fake app-server whose first connection is the send-tool probe,
        /// answered as codex 0.161.0-alpha.2 answers it; every later
        /// connection answers `frames` and is recorded.
        fn install_probed(&self, frames: &[String]) -> (tempfile::TempDir, PathBuf, PathBuf);
    }

    impl InstallsRecordingFakeProxy for RecordingFakeProxy {
        fn install(&self, frames: &[String]) -> (tempfile::TempDir, PathBuf, PathBuf) {
            let directory = tempfile::tempdir().unwrap();
            let executable = directory.path().join("codex");
            let record = directory.path().join("requests");
            let script = FakeProxy.executable(frames).replacen(
                "\nprintf '%b' 'HTTP/1.1",
                &format!(
                    "\nexec 3<&0\ncat <&3 > '{}' &\nprintf '%b' 'HTTP/1.1",
                    record.display()
                ),
                1,
            );
            FixtureExecutable {
                path: executable.clone(),
            }
            .install(&script);
            (directory, executable, record)
        }

        fn install_probed(&self, frames: &[String]) -> (tempfile::TempDir, PathBuf, PathBuf) {
            let directory = tempfile::tempdir().unwrap();
            let executable = directory.path().join("codex");
            let record = directory.path().join("requests");
            let counter = directory.path().join("connections");
            let probe = [
                FakeProxy.websocket_frame(r#"{"id":1,"result":{}}"#),
                FakeProxy.websocket_frame(
                    r#"{"id":2,"error":{"code":-32600,"message":"thread/start.dynamicTools requires experimentalApi capability"}}"#,
                ),
            ]
            .concat();
            FixtureExecutable {
                path: executable.clone(),
            }
            .install(&format!(
                "#!/bin/sh\nn=$(cat '{counter}' 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > '{counter}'\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nif [ \"$n\" -gt 1 ]; then exec 3<&0; cat <&3 > '{record}' & fi\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\nif [ \"$n\" -eq 1 ]; then printf '%b' '{probe}'; else printf '%b' '{frames}'; fi\nsleep 2\n",
                counter = counter.display(),
                record = record.display(),
                frames = frames.join(""),
            ));
            (directory, executable, record)
        }
    }

    /// The JSON messages a recorded client stream holds: each masked client
    /// frame unmasked (RFC 6455 5.3), waiting until `count` have arrived.
    trait ReadsRecordedRequests {
        fn requests(&self, count: usize) -> Vec<serde_json::Value>;
    }

    impl ReadsRecordedRequests for std::path::Path {
        fn requests(&self, count: usize) -> Vec<serde_json::Value> {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
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
                    messages.push(serde_json::from_slice(&payload).expect("a JSON request"));
                    at = start + length;
                }
                if messages.len() >= count || Instant::now() > deadline {
                    return messages;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
    }

    trait InstallsFakeProxy {
        fn install(&self, frames: &[String]) -> (tempfile::TempDir, PathBuf);
    }

    impl InstallsFakeProxy for FakeProxy {
        fn install(&self, frames: &[String]) -> (tempfile::TempDir, PathBuf) {
            let directory = tempfile::tempdir().unwrap();
            let executable = directory.path().join("codex");
            FixtureExecutable {
                path: executable.clone(),
            }
            .install(&self.executable(frames));
            (directory, executable)
        }
    }

    fn adapter() -> CodexAdapter {
        CodexAdapter {
            executable: PathBuf::from("/fixture/codex-wrapper"),
            socket: "/tmp/fake-codex.sock".into(),
            model: "gpt-5.6".into(),
            timeout: Duration::from_millis(100),
            workspace_root: std::env::current_dir().unwrap(),
        }
    }

    /// A fake-proxy adapter. Its replies arrive at once, so the wide
    /// timeout costs nothing and keeps a loaded test machine from turning a
    /// slow `sh` start into a false `TimedOut`.
    fn adapter_at(executable: PathBuf) -> CodexAdapter {
        CodexAdapter {
            executable,
            timeout: Duration::from_secs(5),
            ..adapter()
        }
    }

    fn endpoints() -> CodexEndpoints {
        CodexEndpoints {
            stable: CodexEndpoint {
                client_path: PathBuf::from("/profile/bin/codex"),
                home: PathBuf::from("/home/test/.codex"),
                socket: "/home/test/.codex/app-server-control/app-server-control.sock".into(),
                transcript_root: PathBuf::from("/home/test/.codex/sessions"),
                model_names: BTreeSet::from(["gpt-5.6-sol".into()]),
            },
            next: CodexEndpoint {
                client_path: PathBuf::from("/profile/bin/codex-next"),
                home: PathBuf::from("/home/test/.codex-next"),
                socket: "/home/test/.codex-next/app-server-control/app-server-control.sock".into(),
                transcript_root: PathBuf::from("/home/test/.codex-next/sessions"),
                model_names: BTreeSet::from([
                    "gpt-6-astra".into(),
                    "gpt-6-sol".into(),
                    "gpt-6-luna".into(),
                ]),
            },
            timeout: Duration::from_secs(10),
            workspace_root: PathBuf::from("/home/test/primary"),
        }
    }

    #[test]
    fn exact_models_select_one_immutable_codex_endpoint() {
        let endpoints = endpoints();
        assert_eq!(
            endpoints.endpoint_for("gpt-6-sol").unwrap().client_path,
            PathBuf::from("/profile/bin/codex-next")
        );
        assert_eq!(
            endpoints.endpoint_for("gpt-5.6-sol").unwrap().client_path,
            PathBuf::from("/profile/bin/codex")
        );
        assert!(endpoints.endpoint_for("gpt-6-invented").is_err());
    }

    #[test]
    fn overlapping_model_routes_fail_closed() {
        let mut endpoints = endpoints();
        endpoints.stable.model_names.insert("gpt-6-sol".into());
        assert!(endpoints.endpoint_for("gpt-6-sol").is_err());
    }

    #[test]
    fn adapter_uses_the_configured_wrapper_without_path_resolution() {
        let adapter = endpoints().adapter_for("gpt-6-astra").unwrap();
        assert_eq!(adapter.executable, PathBuf::from("/profile/bin/codex-next"));
        assert!(matches!(
            adapter.open_proxy(),
            Err(CodexAdapterUnavailable::Proxy(_))
        ));
    }

    fn malformed_bound_launch() -> (ComposedLaunch, PromptDeliveryIntent) {
        let pane = signal_flow::HerdrPaneBinding {
            launch_request_id: "launch-malformed".into(),
            herdr_session_name: "session".into(),
            herdr_agent_name: "codex-agent".into(),
            herdr_workspace_id: "workspace".into(),
            herdr_pane_id: "pane".into(),
            herdr_terminal_id: "terminal".into(),
        };
        let prompt_sha256 = "230d8358dc8e8890b4c58deeb62912ee2f20357ae92a5cc861b98e68fe31acb5";
        let launch = ComposedLaunch {
            harness_kind: HarnessKind::Codex,
            launch_profile: signal_flow::LaunchProfile {
                launch_request_id: "launch-malformed".into(),
                system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                user_modules: vec![],
                address: signal_flow::Address {
                    aspect: signal_flow::Aspect::Field,
                    topic: "flow".into(),
                    layer: signal_flow::Layer::Secondary,
                },
                model_name: "gpt-5.6".into(),
                effort: "medium".into(),
                flow_id_option: None,
                remembered_flow_vector: vec![],
                herdr_session_name: "session".into(),
                leave_outs_option: None,
                instruction_prompt: "body".into(),
            },
            system_prompt: signal_flow::SystemPrompt {
                system_prompt_text: String::new(),
                system_prompt_sha256:
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            },
            first_prompt_payload: signal_flow::FirstPromptPayload {
                first_prompt_body: "body".into(),
                prompt_sha256: prompt_sha256.into(),
                prompt_part_vector: vec![signal_flow::PromptPart {
                    prompt_part_text: "body with a malformed footer".into(),
                    prompt_sha256: prompt_sha256.into(),
                }],
            },
            target_receipt_request: signal_flow::TargetReceiptRequest {
                launch_request_id: "launch-malformed".into(),
                prompt_sha256: prompt_sha256.into(),
            },
            leave_outs: Vec::new(),
        };
        let intent = PromptDeliveryIntent {
            launch_request_id: "launch-malformed".into(),
            prompt_sha256: prompt_sha256.into(),
            flow_id: "123456".into(),
            native_session_id: "native-thread".into(),
            harness_kind: HarnessKind::Codex,
            model_name: "gpt-5.6".into(),
            effort: "medium".into(),
            prompt_sha256_vector: vec![prompt_sha256.into()],
            parts_submitted: 1,
            stops_reported: 0,
            leave_outs: Vec::new(),
            herdr_pane_binding: pane,
            native_transcript_boundary: signal_flow::NativeTranscriptBoundary::Absent(
                signal_flow::NativeTranscriptAbsence {
                    native_session_id: "native-thread".into(),
                    harness_kind: HarnessKind::Codex,
                    transcript_root_device: "1".into(),
                    transcript_root_inode: "2".into(),
                },
            ),
        };
        (launch, intent)
    }

    #[test]
    fn a_codex_part_is_one_text_item_and_nothing_else() {
        use crate::composition::ComposesLaunch;
        use crate::composition::tests::{fixture_composer, fixture_profile};
        let profile = fixture_profile();
        let launch = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let (_, mut intent) = malformed_bound_launch();
        intent.prompt_sha256 = launch.first_prompt_payload.prompt_sha256.clone();
        intent.prompt_sha256_vector = vec![
            launch.first_prompt_payload.prompt_part_vector[0]
                .prompt_sha256
                .clone(),
        ];
        let params = CodexAdapter::bound_turn_params(&launch, &intent).unwrap();
        let input = params["input"].as_array().unwrap();
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["type"], "text");
        assert_eq!(
            input[0]["text"],
            launch.first_prompt_payload.prompt_part_vector[0].prompt_part_text
        );
        let text = input[0]["text"].as_str().unwrap();
        assert!(text.starts_with("These are your skills, given here; do not reload them."));
        assert!(
            !text.contains("Spirit body."),
            "system text stays out of the USER turn"
        );
        assert!(!input.iter().any(|item| item["type"] == "skill"));
        intent.parts_submitted = 0;
        assert!(CodexAdapter::bound_turn_params(&launch, &intent).is_none());
    }

    #[test]
    fn an_outbound_frame_past_sixty_five_kilobytes_takes_the_sixty_four_bit_length() {
        assert_eq!(ProxySession::masked_length(125), vec![0x80 | 125]);
        assert_eq!(ProxySession::masked_length(126), vec![0x80 | 126, 0, 126]);
        assert_eq!(
            ProxySession::masked_length(65_535),
            vec![0x80 | 126, 0xff, 0xff]
        );
        let long = ProxySession::masked_length(1_048_576);
        assert_eq!(long[0], 0x80 | 127);
        assert_eq!(
            u64::from_be_bytes(long[1..9].try_into().unwrap()),
            1_048_576
        );
        assert_eq!(long.len(), 9);
    }

    #[test]
    fn malformed_full_prompt_is_rejected_before_codex_proxy_open() {
        let (launch, intent) = malformed_bound_launch();
        assert!(matches!(
            adapter().submit_bound_codex_part(&launch, &intent),
            Err(CodexAdapterUnavailable::Protocol(detail))
                if detail.contains("does not match the composed launch")
        ));
    }

    #[test]
    fn fake_proxy_names_the_bound_thread_and_reads_the_name_back() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(r#"{"id":2,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":3,"result":{"thread":{"id":"thread-1","name":"Field.{ Astra 123456 }"}}}"#,
            ),
        ];
        let (_directory, executable) = fake.install(&frames);
        adapter_at(executable)
            .name_bound_thread("thread-1", "Field.{ Astra 123456 }")
            .expect("readback equals the set title");
    }

    #[test]
    fn fake_proxy_thread_name_readback_mismatch_is_refused() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(r#"{"id":2,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":3,"result":{"thread":{"id":"thread-1","name":"Psyche Opus b87854"}}}"#,
            ),
        ];
        let (_directory, executable) = fake.install(&frames);
        assert!(matches!(
            adapter_at(executable).name_bound_thread("thread-1", "Field.{ Astra 123456 }"),
            Err(CodexAdapterUnavailable::Protocol(detail)) if detail.contains("readback differs")
        ));
    }

    #[test]
    fn fake_proxy_consumes_a_reset_credit_as_a_typed_outcome() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(r#"{"id":2,"result":{"outcome":"nothingToReset"}}"#),
        ];
        let (_directory, executable) = fake.install(&frames);
        assert_eq!(
            adapter_at(executable)
                .consume_reset_credit(&meta_signal_flow::ResetRequest {
                    idempotency_key: "attempt-1".into(),
                    credit_selection: meta_signal_flow::CreditSelection::Next,
                })
                .unwrap(),
            meta_signal_flow::ResetOutcome::NothingToReset
        );
    }

    /// A Codex launch the fake app-server serves: the fixture's composed
    /// context, leaving the AGENTS.md body and the skill listing out.
    fn lean_codex_launch() -> ComposedLaunch {
        use crate::composition::ComposesLaunch;
        use crate::composition::tests::{fixture_composer, fixture_profile};
        fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile(),
                },
                vec![
                    signal_flow::LeaveOut::CodexAgentsMd,
                    signal_flow::LeaveOut::CodexSkillListing,
                    signal_flow::LeaveOut::CodexDeveloperInstructions,
                ],
            )
            .unwrap()
    }

    fn lean_adapter_at(executable: PathBuf, launch: &ComposedLaunch) -> CodexAdapter {
        CodexAdapter {
            model: launch.launch_profile.model_name.clone(),
            ..adapter_at(executable)
        }
    }

    /// The thread is opened lean through the app-server, its readback
    /// checked, and the first turn given before any TUI attaches: the
    /// requests are initialize, thread/start with the system prompt and the
    /// exclusions, then turn/start with the whole first part.
    #[test]
    fn a_codex_thread_is_opened_lean_and_given_its_first_turn_before_attach() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":[]}}"#,
            ),
            fake.websocket_frame(r#"{"id":3,"result":{"turn":{"id":"turn-1"}}}"#),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install_probed(&frames);
        assert_eq!(
            lean_adapter_at(executable, &launch)
                .open_thread_with_first_turn(&launch)
                .unwrap(),
            "thread-lean"
        );
        let requests = record.requests(4);
        let methods = requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            ["initialize", "initialized", "thread/start", "turn/start"]
        );
        let start = &requests[2]["params"];
        assert_eq!(
            start["baseInstructions"],
            launch.system_prompt.system_prompt_text
        );
        assert_eq!(start["config"]["project_doc_max_bytes"], 0);
        assert_eq!(start["config"]["skills.include_instructions"], false);
        // The developer-instructions leave-out empties the developer
        // instructions and both multi-agent hints.
        assert_eq!(start["config"]["developer_instructions"], "");
        assert_eq!(
            start["config"]["features.multi_agent_v2.root_agent_usage_hint_text"],
            ""
        );
        assert_eq!(
            start["config"]["features.multi_agent_v2.multi_agent_mode_hint_text"],
            ""
        );
        assert_eq!(start["config"]["model_reasoning_effort"], "medium");
        assert_eq!(start["model"], "gpt-6-astra");
        // The thread declares the send tool, on a connection opened with
        // the experimental API once the probe showed the app-server gates
        // the tool on it.
        assert_eq!(
            start["dynamicTools"],
            serde_json::json!([SendTool::tool_spec()])
        );
        assert_eq!(
            requests[0]["params"]["capabilities"]["experimentalApi"],
            true
        );
        let turn = &requests[3]["params"];
        assert_eq!(turn["threadId"], "thread-lean");
        assert_eq!(
            turn["input"][0]["text"],
            launch.first_prompt_payload.prompt_part_vector[0].prompt_part_text
        );
        assert_eq!(turn["input"].as_array().unwrap().len(), 1);
    }

    /// An app-server that does not refuse the send tool without the
    /// experimental API, by starting the probe's thread or by another
    /// refusal, is not shown to support it: no connection with the
    /// experimental API is opened and no thread is started for the launch.
    #[test]
    fn an_app_server_that_does_not_gate_the_send_tool_opens_no_thread() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        for (probe_answer, archives) in [
            (
                r#"{"id":2,"result":{"thread":{"id":"probe-thread"}}}"#,
                true,
            ),
            (
                r#"{"id":2,"error":{"code":-32602,"message":"unknown field `dynamicTools`"}}"#,
                false,
            ),
        ] {
            let frames = [
                fake.websocket_frame(r#"{"id":1,"result":{}}"#),
                fake.websocket_frame(probe_answer),
                fake.websocket_frame(r#"{"id":3,"result":{}}"#),
            ];
            let (_directory, executable, record) = RecordingFakeProxy.install(&frames);
            assert!(matches!(
                lean_adapter_at(executable, &launch).open_thread_with_first_turn(&launch),
                Err(CodexAdapterUnavailable::SendToolUnsupported(_))
            ));
            let requests = record.requests(if archives { 4 } else { 3 });
            let methods = requests
                .iter()
                .map(|request| request["method"].as_str().unwrap())
                .collect::<Vec<_>>();
            // A started probe thread is archived at once; nothing else is sent.
            if archives {
                assert_eq!(
                    methods,
                    [
                        "initialize",
                        "initialized",
                        "thread/start",
                        "thread/archive"
                    ]
                );
                assert_eq!(requests[3]["params"]["threadId"], "probe-thread");
            } else {
                assert_eq!(methods, ["initialize", "initialized", "thread/start"]);
            }
            assert!(requests[0]["params"].get("capabilities").is_none());
            assert_eq!(requests[2]["params"]["ephemeral"], true);
            assert_eq!(
                requests[2]["params"]["dynamicTools"],
                serde_json::json!([SendTool::tool_spec()])
            );
        }
    }

    /// The whole Codex start against a fake app-server and a fake Herdr that
    /// reports no agent session for the remote-resumed TUI, as Herdr does:
    /// Flow opens the thread and gives its first turn, the TUI attaches to
    /// it, and the pane binds to the thread Flow opened once Herdr
    /// recognizes the pane's Codex agent.
    #[test]
    fn a_remote_resumed_tui_binds_to_the_thread_the_app_server_opened() {
        use crate::herdr::ConfiguresHerdrCli;
        use crate::herdr::launch::{
            ChecksComposedLaunch, CreatesHerdrLaunchPane, ObservesNativeLaunchBinding,
            StartsNativeHerdrHarness,
        };
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let thread = "12345678-1234-4abc-8def-123456789abc";
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(&format!(
                r#"{{"id":2,"result":{{"thread":{{"id":"{thread}"}},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":[]}}}}"#
            )),
            fake.websocket_frame(r#"{"id":3,"result":{"turn":{"id":"turn-1"}}}"#),
        ];
        let (_proxy, executable, record) = RecordingFakeProxy.install_probed(&frames);
        let mut codex_endpoints = endpoints();
        codex_endpoints.next.client_path = executable;
        codex_endpoints.next.socket = "/tmp/fake-codex-next.sock".into();
        codex_endpoints.timeout = Duration::from_secs(5);
        codex_endpoints.workspace_root = std::env::current_dir().unwrap();
        let opened = codex_endpoints
            .adapter_for(&launch.launch_profile.model_name)
            .unwrap()
            .open_thread_with_first_turn(&launch)
            .expect("the app-server opens the thread");
        assert_eq!(opened, thread);
        let methods = record
            .requests(4)
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            ["initialize", "initialized", "thread/start", "turn/start"]
        );

        let (root, herdr) = crate::herdr::launch::tests::fixture_herdr(
            "codex",
            thread,
            "1234567812344abc8def123456789abc",
            &launch.launch_agent_name(),
        );
        let herdr = herdr.with_codex_endpoints(codex_endpoints);
        std::fs::write(root.path().join("never-agent-session"), "absent").unwrap();
        let pane = herdr.create_launch_pane(&launch).expect("created pane");
        herdr
            .attach_codex_harness(&launch, &pane, &opened)
            .expect("the TUI attaches to the opened thread");
        let binding = herdr
            .observe_attached_codex_binding(&launch, &pane, &opened)
            .expect("the pane binds to the opened thread");
        assert_eq!(binding.native_session_id, thread);
        assert_eq!(binding.flow_id, "123456");
        assert_eq!(binding.herdr_pane_binding, pane);
    }

    /// A thread opened before a refused harness start is accounted for: its
    /// first turn read once, then archived through the app-server.
    #[test]
    fn an_opened_thread_is_accounted_for_and_archived() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":2,"result":{"data":[{"id":"turn-1","status":"completed","items":[]}],"nextCursor":null}}"#,
            ),
            fake.websocket_frame(r#"{"id":3,"result":{}}"#),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install(&frames);
        assert_eq!(
            lean_adapter_at(executable, &launch).account_opened_thread("thread-opened"),
            signal_flow::OpenedThread {
                native_session_id: "thread-opened".into(),
                first_turn: signal_flow::FirstTurn::Answered,
                thread_archive: signal_flow::ThreadArchive::Archived,
            }
        );
        let requests = record.requests(4);
        let methods = requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "initialized",
                "thread/turns/list",
                "thread/archive"
            ]
        );
        assert_eq!(requests[2]["params"]["threadId"], "thread-opened");
        assert_eq!(requests[2]["params"]["sortDirection"], "asc");
        assert_eq!(requests[3]["params"]["threadId"], "thread-opened");
    }

    /// A thread whose readback shows project instructions loaded, though
    /// the launch left them out, is refused and given no turn.
    #[test]
    fn a_codex_thread_that_loaded_left_out_instructions_gets_no_turn() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":["/primary/AGENTS.md"]}}"#,
            ),
            fake.websocket_frame(r#"{"id":3,"result":{"turn":{"id":"turn-1"}}}"#),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install_probed(&frames);
        assert!(matches!(
            lean_adapter_at(executable, &launch).open_thread_with_first_turn(&launch),
            Err(CodexAdapterUnavailable::Protocol(detail))
                if detail.contains("loaded project instructions the launch left out")
        ));
        let requests = record.requests(3);
        assert!(
            !requests
                .iter()
                .any(|request| request["method"] == "turn/start")
        );
    }

    fn attached_intent(launch: &ComposedLaunch, parts_submitted: i64) -> PromptDeliveryIntent {
        let (_, mut intent) = malformed_bound_launch();
        intent.launch_request_id = launch.launch_profile.launch_request_id.clone();
        intent.prompt_sha256 = launch.first_prompt_payload.prompt_sha256.clone();
        intent.prompt_sha256_vector = launch
            .first_prompt_payload
            .prompt_part_vector
            .iter()
            .map(|part| part.prompt_sha256.clone())
            .collect();
        intent.model_name = launch.launch_profile.model_name.clone();
        intent.effort = launch.launch_profile.effort.clone();
        intent.native_session_id = "thread-lean".into();
        intent.leave_outs = launch.leave_outs.clone();
        intent.parts_submitted = parts_submitted;
        intent
    }

    /// Once the TUI has attached, the base instructions, exclusions and
    /// flow environment are applied to the thread again and read back; the
    /// first part is already the thread's first turn, so nothing else goes.
    #[test]
    fn after_attach_the_exclusions_are_applied_again_and_read_back() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let intent = attached_intent(&launch, 1);
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":[]}}"#,
            ),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install(&frames);
        assert_eq!(
            lean_adapter_at(executable, &launch)
                .submit_bound_codex_part(&launch, &intent)
                .unwrap(),
            PromptDeliveryResult::Ambiguous(intent.clone())
        );
        let requests = record.requests(3);
        let methods = requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(methods, ["initialize", "initialized", "thread/resume"]);
        let resume = &requests[2]["params"];
        assert_eq!(resume["threadId"], "thread-lean");
        assert_eq!(
            resume["baseInstructions"],
            launch.system_prompt.system_prompt_text
        );
        assert_eq!(resume["config"]["project_doc_max_bytes"], 0);
        assert_eq!(resume["config"]["skills.include_instructions"], false);
        assert_eq!(
            resume["config"]["shell_environment_policy"]["set"]["FLOW_ID"],
            intent.flow_id
        );
    }

    /// A readback after attach that differs from the launch refuses the
    /// part: the attached thread runs another model, or loaded AGENTS.md.
    #[test]
    fn an_attached_thread_whose_readback_differs_is_refused() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let intent = attached_intent(&launch, 1);
        let fake = FakeProxy;
        for (answer, refusal) in [
            (
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-sol","reasoningEffort":"medium","instructionSources":[]}}"#,
                "model or effort",
            ),
            (
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":["/primary/AGENTS.md"]}}"#,
                "loaded project instructions",
            ),
        ] {
            let frames = [
                fake.websocket_frame(r#"{"id":1,"result":{}}"#),
                fake.websocket_frame(answer),
            ];
            let (_directory, executable, _record) = RecordingFakeProxy.install(&frames);
            assert!(matches!(
                lean_adapter_at(executable, &launch).submit_bound_codex_part(&launch, &intent),
                Err(CodexAdapterUnavailable::Protocol(detail)) if detail.contains(refusal)
            ));
        }
    }
}
