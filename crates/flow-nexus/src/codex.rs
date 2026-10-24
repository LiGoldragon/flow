//! The Codex app-server is reached only through `codex app-server proxy`.
//! The proxy is a byte bridge, so this module owns its bounded WebSocket and
//! JSON-RPC conversation; it never falls back to a direct Unix-socket client.

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
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver},
    },
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;
use tungstenite::{
    Message,
    protocol::{Role, WebSocket, WebSocketConfig},
};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

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
///
/// A launch compares the model its native transcript records with the model
/// it named, string for string, so it names only a model that is exact on
/// its harness. A Codex model is exact by the endpoint that lists it. Claude
/// Code takes an alias for the latest model (`haiku`, `opus`) or a model's
/// full name, and exposes no resolution of an alias; its transcript records
/// the full name the API answered with (`claude-haiku-5-5`), which is
/// `claude-` then lowercase letters, digits and hyphens. A Claude model of
/// any other form, an alias or a name carrying a `[1m]` context suffix, is
/// never launched.
impl crate::generated::starter::DerivesHarness for CodexEndpoints {
    fn harness_for<N: crate::generated::starter::Textual>(&self, input: N) -> HarnessKind {
        match self.adapter_for(&input.text()) {
            Ok(_) => HarnessKind::Codex,
            Err(_) => HarnessKind::Claude,
        }
    }

    fn launches_exactly<N: crate::generated::starter::Textual>(&self, input: N) -> bool {
        let model = input.text();
        match crate::generated::starter::DerivesHarness::harness_for(self, model.as_str()) {
            HarnessKind::Codex => true,
            HarnessKind::Claude => model.strip_prefix("claude-").is_some_and(|name| {
                !name.is_empty()
                    && name.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            }),
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
/// thread again and reading them back. The first part is the thread's
/// first turn, started only once the TUI attached to the idle thread and
/// the pane bound; a later part follows the turn the transcript showed
/// completed. Durable one-shot gating belongs to the caller; this never
/// creates or replaces the Herdr-attached thread.
pub trait SubmitsBoundCodexPart {
    fn submit_bound_codex_part(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, CodexAdapterUnavailable>;
}

/// The app-server receipt for one queued request submitted as a new turn.
/// The JSON-RPC id is chosen by the durable delivery attempt; the returned
/// turn id is the native fact that the app-server accepted that attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTurnReceipt {
    pub native_request_id: u64,
    pub thread_id: String,
    pub turn_id: String,
    pub native_receipt: String,
}

/// Submits queued work to an existing Codex thread through the supported
/// app-server protocol. This never types into the attached terminal pane.
pub trait SubmitsQueuedCodexTurn {
    fn submit_queued_turn(
        &self,
        thread_id: &str,
        attempt_id: &str,
        native_request_id: u64,
        text: &str,
    ) -> Result<QueuedTurnReceipt, CodexAdapterUnavailable>;
}

/// What a Codex thread's own history shows of one queued turn, read after
/// the newest turn the thread had before `turn/start`.
///
/// Codex 0.158.0-alpha.9's app-server returns a turn as `{ id, items,
/// itemsView, status, error, startedAt, completedAt, durationMs }`: neither
/// the `turnTrigger` a `turn/start` names nor its JSON-RPC id is kept in
/// thread history (witnessed in `app-server-protocol` v2 `Turn` and
/// `turn_input.rs`, where the trigger only reaches turn metadata). What
/// history does keep is the turn's `userMessage` item, so the provable
/// receipt is the one turn after the anchor whose user input is exactly the
/// queued text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexTurnHistory {
    /// Exactly one turn after the anchor carries the text; `resting` when
    /// that turn is completed and the thread idle.
    Received {
        turn_id: String,
        turn: String,
        resting: bool,
    },
    /// Every turn after the anchor was read and none carries the text;
    /// `resting` is whether `thread/read` answered the thread idle.
    Unseen { resting: bool, observation: String },
    /// The anchor turn is not in the thread's history.
    AnchorMissing,
    /// More than one turn after the anchor carries the text.
    Ambiguous,
}

/// Reads a thread's turns from its app-server, for the anchor a queued
/// turn is prepared with and for its reconciliation.
pub trait ReadsCodexTurnHistory {
    /// How many turns one `thread/turns/list` page asks for.
    const HISTORY_PAGE: u64 = 16;
    /// How many pages a reconciliation reads before it calls the anchor
    /// missing.
    const HISTORY_PAGES: u64 = 16;
    /// The thread's newest turn, None when it has none.
    fn newest_turn(&self, thread_id: &str) -> Result<Option<String>, CodexAdapterUnavailable>;
    /// What the thread's history after `anchor` shows of `text`.
    fn queued_turn_after(
        &self,
        thread_id: &str,
        anchor: Option<&str>,
        text: &str,
    ) -> Result<CodexTurnHistory, CodexAdapterUnavailable>;
}

/// Whether one app-server turn's user input is exactly a text.
pub trait CarriesUserText {
    fn carries_user_text(&self, text: &str) -> bool;
}

impl CarriesUserText for serde_json::Value {
    fn carries_user_text(&self, text: &str) -> bool {
        self.get("items")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("type").and_then(serde_json::Value::as_str) == Some("userMessage")
                        && item
                            .get("content")
                            .and_then(serde_json::Value::as_array)
                            .is_some_and(|content| {
                                matches!(content.as_slice(), [only]
                                    if only.get("type").and_then(serde_json::Value::as_str)
                                        == Some("text")
                                        && only.get("text").and_then(serde_json::Value::as_str)
                                            == Some(text))
                            })
                })
            })
    }
}

/// Opens a launch's Codex thread through the app-server before any TUI
/// attaches to it: the thread starts with the composed system prompt as its
/// base instructions and the launch's exclusions in its config, and what the
/// app-server answers is read back against them. The answer is the thread's
/// id and the connection that opened it.
///
/// `title_for` is given the thread once it exists, for what must hold before
/// anything runs on it (the thread recorded as this launch's, its FlowId
/// claimed), and answers the thread's title. The thread is named with that
/// title and the name read back before the TUI attaches: a thread has no
/// stored rollout until something is written to it, and the TUI's
/// `resume <thread>` of a thread with none is refused (codex 0.161.0-alpha.2:
/// "no rollout found"); the name is that write. When `title_for` or the
/// naming refuses, the thread is archived.
///
/// No turn is started here. Herdr's `agent start` returns only once a Codex
/// pane is at rest, which it is not while a turn runs, so the first turn is
/// started after the TUI attached and the pane bound
/// (`SubmitsBoundCodexPart`). The connection that opened the thread stays
/// subscribed to its server requests and is kept as the launch's responder
/// until the launch ends.
pub(crate) trait OpensCodexThread {
    fn open_thread(
        &self,
        launch: &ComposedLaunch,
        title_for: &mut dyn FnMut(&str) -> Result<String, CodexAdapterUnavailable>,
    ) -> Result<(String, ThreadConnection), CodexAdapterUnavailable>;
}

/// A connection subscribed to one thread's server requests, with the
/// channel its events arrive on; closed when dropped.
pub(crate) struct ThreadConnection {
    pub(crate) subscription: ThreadSubscription,
    /// Kept so the connection's reader always has a sink.
    pub(crate) _signals: mpsc::Sender<ResponderSignal>,
    pub(crate) inbox: Receiver<ResponderSignal>,
}

impl Drop for ThreadConnection {
    fn drop(&mut self) {
        self.subscription.close();
    }
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
    input: Arc<Mutex<ChildStdin>>,
    replies: Receiver<ProxyEvent>,
}

/// One running `codex app-server proxy` whose WebSocket upgrade was sent;
/// its reader thread hands each event to the sink it was spawned with.
pub(crate) struct ProxyChild {
    pub(crate) child: Child,
    pub(crate) input: Arc<Mutex<ChildStdin>>,
}

struct ProxyWebSocketIo<R> {
    reader: R,
    input: Arc<Mutex<ChildStdin>>,
}

impl<R: Read> Read for ProxyWebSocketIo<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buffer)
    }
}

impl<R> Write for ProxyWebSocketIo<R> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let mut input = self
            .input
            .lock()
            .map_err(|_| std::io::Error::other("Codex proxy input lock poisoned"))?;
        input.write_all(buffer)?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.input
            .lock()
            .map_err(|_| std::io::Error::other("Codex proxy input lock poisoned"))?
            .flush()
    }
}

pub(crate) enum ProxyEvent {
    Upgraded,
    #[allow(dead_code)] // WebSocket control frame retained for protocol vocabulary.
    Ping(Vec<u8>),
    Reply(Result<serde_json::Value, CodexAdapterUnavailable>),
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
        let input = Arc::new(Mutex::new(child.stdin.take().ok_or_else(|| {
            CodexAdapterUnavailable::Proxy("proxy stdin was unavailable".into())
        })?));
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
        let upgrade_write = match input.lock() {
            Ok(mut input) => input
                .write_all(upgrade.as_bytes())
                .and_then(|_| input.flush()),
            Err(_) => {
                return Err(CodexAdapterUnavailable::Proxy(
                    "Codex proxy input lock poisoned".into(),
                ));
            }
        };
        if let Err(error) = upgrade_write {
            let _ = child.kill();
            let _ = child.wait();
            return Err(CodexAdapterUnavailable::Proxy(error.to_string()));
        }
        let reader_input = Arc::clone(&input);
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
            let mut config = WebSocketConfig::default();
            config.max_message_size = Some(MAX_MESSAGE_BYTES);
            config.max_frame_size = Some(MAX_MESSAGE_BYTES);
            let mut socket = WebSocket::from_raw_socket(
                ProxyWebSocketIo {
                    reader: output,
                    input: reader_input,
                },
                Role::Client,
                Some(config),
            );
            loop {
                let event = match socket.read() {
                    Ok(Message::Text(message)) => match serde_json::from_str(message.as_ref()) {
                        Ok(message) => ProxyEvent::Reply(Ok(message)),
                        Err(error) => ProxyEvent::Reply(Err(CodexAdapterUnavailable::Protocol(
                            error.to_string(),
                        ))),
                    },
                    Ok(Message::Ping(_)) => match socket.flush() {
                        Ok(()) => continue,
                        Err(error) => ProxyEvent::Reply(Err(CodexAdapterUnavailable::Protocol(
                            error.to_string(),
                        ))),
                    },
                    Ok(Message::Pong(_)) => continue,
                    Ok(Message::Close(_)) => {
                        ProxyEvent::Reply(Err(CodexAdapterUnavailable::Closed))
                    }
                    Ok(Message::Binary(_)) => ProxyEvent::Reply(Err(
                        CodexAdapterUnavailable::Protocol("non-text WebSocket message".into()),
                    )),
                    Ok(Message::Frame(_)) => continue,
                    Err(error) => {
                        ProxyEvent::Reply(Err(CodexAdapterUnavailable::Protocol(error.to_string())))
                    }
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
            input: Arc::clone(&input),
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
                ProxyEvent::Ping(_) => continue,
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

impl WritesCodexProxy for Arc<Mutex<ChildStdin>> {
    fn write_frame(&mut self, head: u8, payload: &[u8]) -> Result<(), CodexAdapterUnavailable> {
        self.lock()
            .map_err(|_| CodexAdapterUnavailable::Proxy("Codex proxy input lock poisoned".into()))?
            .write_frame(head, payload)
    }
}

impl WritesCodexProxy for ProxySession {
    fn write_frame(&mut self, head: u8, payload: &[u8]) -> Result<(), CodexAdapterUnavailable> {
        self.input
            .lock()
            .map_err(|_| CodexAdapterUnavailable::Proxy("Codex proxy input lock poisoned".into()))?
            .write_frame(head, payload)
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
    /// Reads back what `thread/read` answered after `thread/name/set`: the
    /// thread is the one named, and its name is the title set.
    fn read_back_name(
        thread_id: &str,
        name: &str,
        read: &serde_json::Value,
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

    fn read_back_name(
        thread_id: &str,
        name: &str,
        read: &serde_json::Value,
    ) -> Result<(), CodexAdapterUnavailable> {
        let thread = read.get("thread").unwrap_or(read);
        if thread.get("id").and_then(serde_json::Value::as_str) != Some(thread_id)
            || thread.get("name").and_then(serde_json::Value::as_str) != Some(name)
        {
            return Err(CodexAdapterUnavailable::Protocol(
                "Codex thread name readback differs from the set title".into(),
            ));
        }
        Ok(())
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
    fn open_thread(
        &self,
        launch: &ComposedLaunch,
        title_for: &mut dyn FnMut(&str) -> Result<String, CodexAdapterUnavailable>,
    ) -> Result<(String, ThreadConnection), CodexAdapterUnavailable> {
        if launch.harness_kind != HarnessKind::Codex {
            return Err(CodexAdapterUnavailable::Protocol(
                "the composed launch is no Codex launch this endpoint serves".into(),
            ));
        }
        // The send tool is declared behind the experimental API, which is
        // opened only once the app-server is shown to gate the tool on it.
        self.probe_send_tool()?;
        let (signals, inbox) = mpsc::channel();
        let subscription = self
            .connect_experimental(1, &signals, &inbox)
            .map_err(<CodexAdapterUnavailable as EndsSubscription>::ended)?;
        let mut connection = ThreadConnection {
            subscription,
            _signals: signals,
            inbox,
        };
        let ThreadConnection {
            subscription,
            inbox,
            ..
        } = &mut connection;
        let mut ask = |id: u64, method: &str, params: serde_json::Value| {
            subscription
                .exchange(inbox, id, method, params)
                .map_err(<CodexAdapterUnavailable as EndsSubscription>::ended)
        };
        let started = ask(
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
        connection.subscription.thread = Some(thread.clone());
        let ThreadConnection {
            subscription,
            inbox,
            ..
        } = &mut connection;
        let mut ask = |id: u64, method: &str, params: serde_json::Value| {
            subscription
                .exchange(inbox, id, method, params)
                .map_err(<CodexAdapterUnavailable as EndsSubscription>::ended)
        };
        let named = title_for(&thread).and_then(|title| {
            ask(
                3,
                "thread/name/set",
                serde_json::json!({ "threadId": thread, "name": title }),
            )?;
            let read = ask(
                4,
                "thread/read",
                serde_json::json!({ "threadId": thread, "includeTurns": false }),
            )?;
            Self::read_back_name(&thread, &title, &read)?;
            // codex 0.161.0-alpha.2 writes a thread's rollout at its first
            // turn; a TUI's `resume` of a thread with no rollout fails
            // "missing source rollout". Moving the thread out of any section
            // materializes its rollout with no turn.
            ask(
                5,
                "thread/section/move",
                serde_json::json!({ "threadId": thread, "sectionId": null }),
            )?;
            Ok(())
        });
        if let Err(refusal) = named {
            let _ = ask(
                6,
                "thread/archive",
                serde_json::json!({ "threadId": thread }),
            );
            return Err(refusal);
        }
        Ok((thread, connection))
    }
}

/// A connection's end, as the adapter's error.
trait EndsSubscription {
    fn ended(end: SubscriptionEnd) -> CodexAdapterUnavailable;
}

impl EndsSubscription for CodexAdapterUnavailable {
    fn ended(end: SubscriptionEnd) -> CodexAdapterUnavailable {
        match end {
            SubscriptionEnd::Failed(error) => error,
            SubscriptionEnd::Stopped => CodexAdapterUnavailable::Closed,
        }
    }
}

impl SubmitsBoundCodexPart for CodexAdapter {
    fn submit_bound_codex_part(
        &self,
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, CodexAdapterUnavailable> {
        let params = Self::bound_turn_params(launch, intent);
        if launch.harness_kind != HarnessKind::Codex
            || intent.harness_kind != HarnessKind::Codex
            || intent.launch_request_id != launch.launch_profile.launch_request_id
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
            // The seat is told the FlowId Bind claimed before its first
            // model call: one developer item in the thread's history, no
            // turn, the first USER message untouched.
            session.request(
                3,
                "thread/inject_items",
                serde_json::json!({
                    "threadId": intent.native_session_id,
                    "items": [{
                        "type": "message",
                        "role": "developer",
                        "content": [{
                            "type": "input_text",
                            "text": format!("Flow: {}", intent.flow_id)
                        }]
                    }]
                }),
                self.timeout,
            )?;
            // The first part is the thread's first turn, started on the idle
            // thread the TUI attached to; a later part follows the turn the
            // transcript showed completed.
            session.request(4, "turn/start", params, self.timeout)?;
            Ok(PromptDeliveryResult::Ambiguous(intent.clone()))
        })();
        session.stop_proxy();
        result
    }
}

impl SubmitsQueuedCodexTurn for CodexAdapter {
    fn submit_queued_turn(
        &self,
        thread_id: &str,
        attempt_id: &str,
        native_request_id: u64,
        text: &str,
    ) -> Result<QueuedTurnReceipt, CodexAdapterUnavailable> {
        if thread_id.is_empty() || attempt_id.is_empty() || native_request_id < 3 {
            return Err(CodexAdapterUnavailable::Protocol(
                "queued turn identity is incomplete".into(),
            ));
        }
        let mut session = self.initialize_bound_session()?;
        let result = (|| {
            let resumed = session.request(
                2,
                "thread/resume",
                serde_json::json!({ "threadId": thread_id }),
                self.timeout,
            )?;
            if resumed
                .pointer("/thread/id")
                .and_then(serde_json::Value::as_str)
                != Some(thread_id)
            {
                return Err(CodexAdapterUnavailable::Protocol(
                    "thread/resume returned another thread".into(),
                ));
            }
            let accepted = session.request(
                native_request_id,
                "turn/start",
                serde_json::json!({
                    "threadId": thread_id,
                    "input": [{
                        "type": "text",
                        "text": text,
                        "text_elements": []
                    }],
                    "turnTrigger": format!("flow-nexus:{attempt_id}")
                }),
                self.timeout,
            )?;
            let turn_id = accepted
                .pointer("/turn/id")
                .and_then(serde_json::Value::as_str)
                .filter(|turn| !turn.is_empty())
                .ok_or_else(|| {
                    CodexAdapterUnavailable::Protocol("turn/start returned no turn id".into())
                })?
                .to_owned();
            Ok(QueuedTurnReceipt {
                native_request_id,
                thread_id: thread_id.to_owned(),
                turn_id,
                native_receipt: accepted.to_string(),
            })
        })();
        session.stop_proxy();
        result
    }
}

impl ReadsCodexTurnHistory for CodexAdapter {
    fn newest_turn(&self, thread_id: &str) -> Result<Option<String>, CodexAdapterUnavailable> {
        let mut session = self.initialize_bound_session()?;
        let result = session
            .request(
                2,
                "thread/turns/list",
                serde_json::json!({
                    "threadId": thread_id,
                    "limit": 1,
                    "sortDirection": "desc",
                    "itemsView": "notLoaded"
                }),
                self.timeout,
            )
            .and_then(|listed| {
                let turns = listed
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| {
                        CodexAdapterUnavailable::Protocol("thread/turns/list has no data".into())
                    })?;
                match turns.first() {
                    None => Ok(None),
                    Some(turn) => turn
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .filter(|turn| !turn.is_empty())
                        .map(|turn| Some(turn.to_owned()))
                        .ok_or_else(|| {
                            CodexAdapterUnavailable::Protocol("a listed turn has no id".into())
                        }),
                }
            });
        session.stop_proxy();
        result
    }

    fn queued_turn_after(
        &self,
        thread_id: &str,
        anchor: Option<&str>,
        text: &str,
    ) -> Result<CodexTurnHistory, CodexAdapterUnavailable> {
        let mut session = self.initialize_bound_session()?;
        let result = (|| {
            let read = session.request(
                2,
                "thread/read",
                serde_json::json!({ "threadId": thread_id, "includeTurns": false }),
                self.timeout,
            )?;
            if read
                .pointer("/thread/id")
                .and_then(serde_json::Value::as_str)
                != Some(thread_id)
            {
                return Err(CodexAdapterUnavailable::Protocol(
                    "thread/read returned another thread".into(),
                ));
            }
            let status = read.pointer("/thread/status").cloned().unwrap_or_default();
            let resting = crate::codex_turns::ReadsThreadStatus::thread_status(&status)
                == Some(crate::codex_turns::ThreadStatus::Idle);
            let mut matched = Vec::new();
            let mut anchor_seen = anchor.is_none();
            let mut read_turns = 0_u64;
            let mut cursor: Option<String> = None;
            'pages: for page in 0..Self::HISTORY_PAGES {
                let mut params = serde_json::json!({
                    "threadId": thread_id,
                    "limit": Self::HISTORY_PAGE,
                    "sortDirection": "desc",
                    "itemsView": "full"
                });
                if let Some(cursor) = &cursor {
                    params["cursor"] = serde_json::Value::String(cursor.clone());
                }
                let listed =
                    session.request(3 + page, "thread/turns/list", params, self.timeout)?;
                let turns = listed
                    .get("data")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| {
                        CodexAdapterUnavailable::Protocol("thread/turns/list has no data".into())
                    })?;
                for turn in turns {
                    let turn_id = turn
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .filter(|turn| !turn.is_empty())
                        .ok_or_else(|| {
                            CodexAdapterUnavailable::Protocol("a listed turn has no id".into())
                        })?;
                    if Some(turn_id) == anchor {
                        anchor_seen = true;
                        break 'pages;
                    }
                    read_turns += 1;
                    if turn.carries_user_text(text) {
                        let completed = turn.get("status").and_then(serde_json::Value::as_str)
                            == Some("completed");
                        matched.push((turn_id.to_owned(), turn.to_string(), completed));
                    }
                }
                match listed.get("nextCursor").and_then(serde_json::Value::as_str) {
                    Some(next) if !next.is_empty() => cursor = Some(next.to_owned()),
                    _ => break,
                }
            }
            if !anchor_seen {
                return Ok(CodexTurnHistory::AnchorMissing);
            }
            Ok(match matched.len() {
                0 => CodexTurnHistory::Unseen {
                    resting,
                    observation: format!(
                        "thread/read {status}; {read_turns} turn(s) after {} carry no such user message",
                        anchor.unwrap_or("the thread's start")
                    ),
                },
                1 => {
                    let (turn_id, turn, completed) = matched.remove(0);
                    CodexTurnHistory::Received {
                        turn_id,
                        turn,
                        resting: resting && completed,
                    }
                }
                _ => CodexTurnHistory::Ambiguous,
            })
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
            Self::read_back_name(native_session_id, name, &read)
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
/// carries no model content. `early` holds the server requests and
/// notifications that arrived while a request of its own was answered.
/// `thread` is the one thread it is subscribed to, once known; `resumed`
/// is the thread status the app-server answered its `thread/resume` with.
pub(crate) struct ThreadSubscription {
    pub(crate) proxy: ProxyChild,
    pub(crate) generation: u64,
    pub(crate) early: Vec<serde_json::Value>,
    pub(crate) thread: Option<String>,
    pub(crate) resumed: Option<serde_json::Value>,
    timeout: Duration,
}

pub(crate) trait SubscribesThread {
    /// A connection initialized with the experimental API, its events sent
    /// to `signals` under `generation`, subscribed to no thread yet.
    fn connect_experimental(
        &self,
        generation: u64,
        signals: &mpsc::Sender<ResponderSignal>,
        inbox: &Receiver<ResponderSignal>,
    ) -> Result<ThreadSubscription, SubscriptionEnd>;
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
    /// One request and its answer; a server request or a notification met
    /// meanwhile is kept in `early`, in the order it came.
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
                self.early.push(message);
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
    fn connect_experimental(
        &self,
        generation: u64,
        signals: &mpsc::Sender<ResponderSignal>,
        inbox: &Receiver<ResponderSignal>,
    ) -> Result<ThreadSubscription, SubscriptionEnd> {
        let sink = signals.clone();
        let proxy = self
            .spawn_proxy(move |event| sink.send(ResponderSignal::Proxy(generation, event)).is_ok())
            .map_err(SubscriptionEnd::Failed)?;
        let mut subscription = ThreadSubscription {
            proxy,
            generation,
            early: Vec::new(),
            thread: None,
            resumed: None,
            timeout: self.timeout,
        };
        let connected = (|| {
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
                .map_err(SubscriptionEnd::Failed)
        })();
        match connected {
            Ok(()) => Ok(subscription),
            Err(end) => {
                subscription.close();
                Err(end)
            }
        }
    }

    fn subscribe_thread(
        &self,
        thread: &str,
        generation: u64,
        signals: &mpsc::Sender<ResponderSignal>,
        inbox: &Receiver<ResponderSignal>,
    ) -> Result<ThreadSubscription, SubscriptionEnd> {
        self.probe_send_tool().map_err(SubscriptionEnd::Failed)?;
        let mut subscription = self.connect_experimental(generation, signals, inbox)?;
        match subscription.exchange(
            inbox,
            2,
            "thread/resume",
            serde_json::json!({ "threadId": thread, "excludeTurns": true }),
        ) {
            Ok(response) => {
                subscription.thread = Some(thread.to_owned());
                subscription.resumed = response.pointer("/thread/status").cloned();
                Ok(subscription)
            }
            Err(end) => {
                subscription.close();
                Err(end)
            }
        }
    }
}

/// Accounts for a thread Flow opened for a launch that ends refused: what
/// its first turn had come to, read once (a thread whose harness start was
/// refused has none), and its archiving through the app-server. The account is given whatever the app-server answers.
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
                remembering: signal_flow::Remembering::Predecessors(0),
                herdr_session_name: "session".into(),
                leave_outs_option: None,
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
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

    /// A reconciliation reads the thread's status, then its turns newest
    /// first, page by page through the cursor the app-server gives, and
    /// stops at the anchor: only a turn after it can be the queued one.
    #[test]
    fn a_queued_turn_is_read_from_history_after_its_anchor_page_by_page() {
        let _serial = FAKE_PROXY_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let turn = |id: &str, status: &str, text: &str| {
            serde_json::json!({"id": id, "status": status, "items": [{
                "type": "userMessage", "id": "m", "clientId": null,
                "content": [{"type": "text", "text": text, "text_elements": []}]
            }]})
        };
        let frames = [
            FakeProxy.websocket_frame(r#"{"id":1,"result":{}}"#),
            FakeProxy.websocket_frame(
                r#"{"id":2,"result":{"thread":{"id":"thread-q","status":{"type":"idle"}}}}"#,
            ),
            FakeProxy.websocket_frame(
                &serde_json::json!({"id":3,"result":{"data":[
                    turn("turn-3", "completed", "later work")
                ],"nextCursor":"page-2"}})
                .to_string(),
            ),
            FakeProxy.websocket_frame(
                &serde_json::json!({"id":4,"result":{"data":[
                    turn("turn-2", "completed", "the queued text"),
                    turn("turn-1", "completed", "the queued text")
                ],"nextCursor":"page-3"}})
                .to_string(),
            ),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install(&frames);
        let history = adapter_at(executable)
            .queued_turn_after("thread-q", Some("turn-1"), "the queued text")
            .unwrap();
        let CodexTurnHistory::Received {
            turn_id, resting, ..
        } = history
        else {
            panic!("one turn after the anchor carries it: {history:?}")
        };
        assert_eq!(turn_id, "turn-2");
        assert!(resting);
        let requests = record.requests(5);
        let methods = requests
            .iter()
            .filter_map(|request| request["method"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "initialized",
                "thread/read",
                "thread/turns/list",
                "thread/turns/list"
            ]
        );
        assert_eq!(requests[3]["params"]["sortDirection"], "desc");
        assert_eq!(requests[3]["params"]["itemsView"], "full");
        assert_eq!(requests[4]["params"]["cursor"], "page-2");
    }

    #[test]
    fn a_turn_carries_only_its_exact_single_text_user_message() {
        let turn = |content: serde_json::Value| {
            serde_json::json!({"items": [
                {"type": "agentMessage", "text": "the text"},
                {"type": "userMessage", "content": content}
            ]})
        };
        assert!(
            turn(serde_json::json!([{"type": "text", "text": "the text"}]))
                .carries_user_text("the text")
        );
        assert!(
            !turn(serde_json::json!([{"type": "text", "text": "the text\n"}]))
                .carries_user_text("the text")
        );
        assert!(
            !turn(serde_json::json!([
                {"type": "text", "text": "the text"},
                {"type": "localImage", "path": "/a.png"}
            ]))
            .carries_user_text("the text")
        );
        assert!(!serde_json::json!({"items": []}).carries_user_text("the text"));
    }

    #[test]
    fn a_codex_part_is_one_text_item_and_nothing_else() {
        use crate::composition::ComposesLaunch;
        use crate::composition::tests::{fixture_composer, fixture_profile};
        let profile = fixture_profile();
        let launch = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
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

    fn server_frame(fin: bool, opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![(if fin { 0x80 } else { 0 }) | opcode];
        if payload.len() < 126 {
            frame.push(payload.len() as u8);
        } else if let Ok(length) = u16::try_from(payload.len()) {
            frame.push(126);
            frame.extend_from_slice(&length.to_be_bytes());
        } else {
            frame.push(127);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn websocket_parser_accepts_json_above_one_megabyte() {
        use std::io::Cursor;

        let json = format!(r#"{{"data":"{}"}}"#, "x".repeat(1024 * 1024));
        let frame = server_frame(true, 1, json.as_bytes());
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(MAX_MESSAGE_BYTES);
        config.max_frame_size = Some(MAX_MESSAGE_BYTES);
        let mut socket = WebSocket::from_raw_socket(Cursor::new(frame), Role::Client, Some(config));
        let Message::Text(message) = socket.read().unwrap() else {
            panic!("expected text message")
        };
        let value: serde_json::Value = serde_json::from_str(message.as_ref()).unwrap();
        assert_eq!(value["data"].as_str().unwrap().len(), 1024 * 1024);
    }

    #[test]
    fn websocket_parser_reassembles_fragmented_json() {
        use std::io::Cursor;

        let json = br#"{"data":"fragmented"}"#;
        let split = json.len() / 2;
        let mut frames = server_frame(false, 1, &json[..split]);
        frames.extend(server_frame(true, 0, &json[split..]));
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(MAX_MESSAGE_BYTES);
        config.max_frame_size = Some(MAX_MESSAGE_BYTES);
        let mut socket =
            WebSocket::from_raw_socket(Cursor::new(frames), Role::Client, Some(config));
        let Message::Text(message) = socket.read().unwrap() else {
            panic!("expected reassembled text message")
        };
        let value: serde_json::Value = serde_json::from_str(message.as_ref()).unwrap();
        assert_eq!(value["data"], "fragmented");
    }

    /// A Codex launch the fake app-server serves: the fixture's composed
    /// context, leaving the AGENTS.md body and the skill listing out.
    fn lean_codex_launch() -> ComposedLaunch {
        use crate::composition::ComposesLaunch;
        use crate::composition::tests::{fixture_composer, fixture_profile};
        fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
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

    /// The title a test's opened thread is named with.
    const OPENED_TITLE: &str = "{ Field Flow Tertiary 123456 }";

    /// The thread is opened lean through the app-server, its readback
    /// checked, and it is named with its title and the name read back, with
    /// no turn: the requests are initialize, thread/start with the system
    /// prompt and the exclusions, thread/name/set and thread/read, and
    /// never turn/start, so the TUI attaches to an idle thread.
    #[test]
    fn a_codex_thread_is_opened_lean_and_named_with_no_turn_before_attach() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":[]}}"#,
            ),
            fake.websocket_frame(r#"{"id":3,"result":{}}"#),
            fake.websocket_frame(
                &serde_json::json!({"id":4,"result":{"thread":{"id":"thread-lean","name":OPENED_TITLE}}})
                    .to_string(),
            ),
            fake.websocket_frame(r#"{"id":5,"result":{}}"#),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install_probed(&frames);
        let mut given = Vec::new();
        assert_eq!(
            lean_adapter_at(executable, &launch)
                .open_thread(&launch, &mut |thread| {
                    given.push(thread.to_owned());
                    Ok(OPENED_TITLE.into())
                })
                .map(|(thread, _)| thread)
                .unwrap(),
            "thread-lean"
        );
        assert_eq!(given, ["thread-lean"]);
        let requests = record.requests(6);
        let methods = requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "initialized",
                "thread/start",
                "thread/name/set",
                "thread/read",
                "thread/section/move"
            ]
        );
        let start = &requests[2]["params"];
        assert_eq!(
            start["baseInstructions"],
            launch.system_prompt.system_prompt_text
        );
        assert!(start["baseInstructions"].as_str().unwrap().contains(
            "Metaflow: { Field flow Secondary }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\n"
        ));
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
        let named = &requests[3]["params"];
        assert_eq!(named["threadId"], "thread-lean");
        assert_eq!(named["name"], OPENED_TITLE);
        assert_eq!(requests[4]["params"]["threadId"], "thread-lean");
    }

    /// A name read back other than the title set refuses the opening, and
    /// the thread is archived; no turn is started.
    #[test]
    fn an_opened_thread_whose_name_reads_back_otherwise_is_archived() {
        let _guard = FAKE_PROXY_LOCK.lock().expect("fake proxy test lock");
        let launch = lean_codex_launch();
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":2,"result":{"thread":{"id":"thread-lean"},"model":"gpt-6-astra","reasoningEffort":"medium","instructionSources":[]}}"#,
            ),
            fake.websocket_frame(r#"{"id":3,"result":{}}"#),
            fake.websocket_frame(
                r#"{"id":4,"result":{"thread":{"id":"thread-lean","name":"another"}}}"#,
            ),
            fake.websocket_frame(r#"{"id":5,"result":{}}"#),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install_probed(&frames);
        assert!(matches!(
            lean_adapter_at(executable, &launch)
                .open_thread(&launch, &mut |_| Ok(OPENED_TITLE.into()))
                .map(|(thread, _)| thread),
            Err(CodexAdapterUnavailable::Protocol(detail))
                if detail.contains("name readback differs")
        ));
        let requests = record.requests(6);
        let methods = requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "initialized",
                "thread/start",
                "thread/name/set",
                "thread/read",
                "thread/archive"
            ]
        );
        assert_eq!(requests[5]["params"]["threadId"], "thread-lean");
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
                lean_adapter_at(executable, &launch)
                    .open_thread(&launch, &mut |_| Ok(OPENED_TITLE.into()))
                    .map(|(thread, _)| thread),
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
    /// Flow opens the thread and names it, the TUI attaches to it, and the
    /// pane binds to the thread Flow opened once Herdr
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
            fake.websocket_frame(r#"{"id":3,"result":{}}"#),
            fake.websocket_frame(
                &serde_json::json!({"id":4,"result":{"thread":{"id":thread,"name":OPENED_TITLE}}})
                    .to_string(),
            ),
            fake.websocket_frame(r#"{"id":5,"result":{}}"#),
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
            .open_thread(&launch, &mut |_| Ok(OPENED_TITLE.into()))
            .map(|(thread, _)| thread)
            .expect("the app-server opens the thread");
        assert_eq!(opened, thread);
        let methods = record
            .requests(6)
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "initialized",
                "thread/start",
                "thread/name/set",
                "thread/read",
                "thread/section/move"
            ]
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
    /// the launch left them out, is refused, neither named nor given a turn.
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
            lean_adapter_at(executable, &launch)
                .open_thread(&launch, &mut |_| Ok(OPENED_TITLE.into()))
                .map(|(thread, _)| thread),
            Err(CodexAdapterUnavailable::Protocol(detail))
                if detail.contains("loaded project instructions the launch left out")
        ));
        let requests = record.requests(3);
        assert!(!requests.iter().any(|request| {
            request["method"] == "turn/start" || request["method"] == "thread/name/set"
        }));
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
    /// flow environment are applied to the thread again and read back; only
    /// then is the seat told its FlowId and the first part started as the
    /// thread's first turn.
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
            fake.websocket_frame(r#"{"id":3,"result":{}}"#),
            fake.websocket_frame(r#"{"id":4,"result":{"turn":{"id":"turn-1"}}}"#),
        ];
        let (_directory, executable, record) = RecordingFakeProxy.install(&frames);
        assert_eq!(
            lean_adapter_at(executable, &launch)
                .submit_bound_codex_part(&launch, &intent)
                .unwrap(),
            PromptDeliveryResult::Ambiguous(intent.clone())
        );
        let requests = record.requests(5);
        let methods = requests
            .iter()
            .map(|request| request["method"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            methods,
            [
                "initialize",
                "initialized",
                "thread/resume",
                "thread/inject_items",
                "turn/start"
            ]
        );
        let told = &requests[3]["params"];
        assert_eq!(told["threadId"], "thread-lean");
        assert_eq!(told["items"][0]["role"], "developer");
        assert_eq!(
            told["items"][0]["content"][0]["text"],
            format!("Flow: {}", intent.flow_id)
        );
        let turn = &requests[4]["params"];
        assert_eq!(turn["threadId"], "thread-lean");
        assert_eq!(
            turn["input"][0]["text"],
            launch.first_prompt_payload.prompt_part_vector[0].prompt_part_text
        );
        assert_eq!(turn["input"].as_array().unwrap().len(), 1);
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
