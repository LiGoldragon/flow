//! The Codex app-server is reached only through `codex app-server proxy`.
//! The proxy is a byte bridge, so this module owns its bounded WebSocket and
//! JSON-RPC conversation; it never falls back to a direct Unix-socket client.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use signal_flow::{
    ComposedLaunch, HarnessKind, NativeLaunchBinding, NativeSkillSelection, OriginClue,
    PromptDeliveryIntent, PromptDeliveryResult,
};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

pub struct CodexAdapter {
    pub socket: String,
    pub model: String,
    pub timeout: Duration,
    /// Exact cwd whose native Codex skill catalog is valid for this launch.
    pub workspace_root: PathBuf,
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
}

pub trait StartsCodex {
    fn start_codex(
        &self,
        flow_id: &str,
        goal: &str,
        origin: &OriginClue,
    ) -> Result<String, CodexAdapterUnavailable>;
}

pub trait ResumesCodex {
    fn resume_codex(
        &self,
        thread_id: &str,
        goal: &str,
        origin: &OriginClue,
    ) -> Result<(), CodexAdapterUnavailable>;
}

pub trait ConsumesResetCredit {
    fn consume_reset_credit(
        &self,
        request: &meta_signal_flow::ResetRequest,
    ) -> Result<meta_signal_flow::ResetOutcome, CodexAdapterUnavailable>;
}

/// Resolves skills through the native app-server catalog only after proving
/// that the exact Herdr-bound thread exists and still has no turns.
pub trait ResolvesBoundCodexSkills {
    fn resolve_bound_codex_skills(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<Vec<NativeSkillSelection>, CodexAdapterUnavailable>;
}

/// Submits one typed skill vector and the composed text as one first native
/// turn. Durable one-shot gating belongs to the caller; this never creates,
/// resumes, or replaces the Herdr-owned thread.
pub trait SubmitsBoundCodexFirstTurn {
    fn submit_bound_codex_first_turn(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, CodexAdapterUnavailable>;
}

struct ProxySession {
    child: Child,
    input: ChildStdin,
    replies: Receiver<ProxyEvent>,
}

enum ProxyEvent {
    Upgraded,
    Reply(Result<serde_json::Value, CodexAdapterUnavailable>),
}

trait OpensCodexProxy {
    fn open_proxy(&self) -> Result<ProxySession, CodexAdapterUnavailable>;
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

trait BuildsCodexTurn {
    fn turn_params(
        &self,
        thread_id: &str,
        flow_id: &str,
        goal: &str,
        origin: &OriginClue,
    ) -> serde_json::Value;
}

impl OpensCodexProxy for CodexAdapter {
    fn open_proxy(&self) -> Result<ProxySession, CodexAdapterUnavailable> {
        let mut child = Command::new("codex")
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
        input
            .write_all(upgrade.as_bytes())
            .and_then(|_| input.flush())
            .map_err(|error| CodexAdapterUnavailable::Proxy(error.to_string()))?;
        let (sender, replies) = mpsc::channel();
        thread::spawn(move || {
            let mut output = BufReader::new(output);
            let outcome = ProxySessionReader {
                output: &mut output,
            }
            .read_upgrade(&accept);
            if let Err(error) = outcome {
                let _ = sender.send(ProxyEvent::Reply(Err(error)));
                return;
            }
            if sender.send(ProxyEvent::Upgraded).is_err() {
                return;
            }
            loop {
                let result = ProxySessionReader {
                    output: &mut output,
                }
                .read_message();
                let closed = matches!(result, Err(CodexAdapterUnavailable::Closed));
                if sender.send(ProxyEvent::Reply(result)).is_err() || closed {
                    return;
                }
            }
        });
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
            Ok(ProxyEvent::Reply(Ok(_))) => {
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
    fn read_message(&mut self) -> Result<serde_json::Value, CodexAdapterUnavailable>;
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

    fn read_message(&mut self) -> Result<serde_json::Value, CodexAdapterUnavailable> {
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
        if length > MAX_MESSAGE_BYTES as u64 {
            return Err(CodexAdapterUnavailable::Protocol(
                "WebSocket message exceeds limit".into(),
            ));
        }
        let mut mask = [0; 4];
        if masked {
            self.output
                .read_exact(&mut mask)
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
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
        if opcode == 8 {
            return Err(CodexAdapterUnavailable::Closed);
        }
        if opcode != 1 {
            return Err(CodexAdapterUnavailable::Protocol(
                "non-text WebSocket frame".into(),
            ));
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))
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
            let ProxyEvent::Reply(reply) = self
                .replies
                .recv_timeout(remaining)
                .map_err(|_| CodexAdapterUnavailable::TimedOut)?
            else {
                return Err(CodexAdapterUnavailable::Protocol(
                    "duplicate WebSocket upgrade".into(),
                ));
            };
            let reply = reply?;
            if reply.get("id") != Some(&serde_json::json!(id)) {
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

trait WritesCodexProxy {
    fn write_json(&mut self, value: serde_json::Value) -> Result<(), CodexAdapterUnavailable>;
}

impl WritesCodexProxy for ProxySession {
    fn write_json(&mut self, value: serde_json::Value) -> Result<(), CodexAdapterUnavailable> {
        let payload = serde_json::to_vec(&value)
            .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
        if payload.len() > u16::MAX as usize {
            return Err(CodexAdapterUnavailable::Protocol(
                "outbound WebSocket message exceeds limit".into(),
            ));
        }
        let mut frame = vec![0x81];
        if payload.len() < 126 {
            frame.push(0x80 | payload.len() as u8);
        } else {
            frame.push(0x80 | 126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
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
        self.input
            .write_all(&frame)
            .and_then(|_| self.input.flush())
            .map_err(|error| CodexAdapterUnavailable::Proxy(error.to_string()))
    }
}

impl StopsCodexProxy for ProxySession {
    fn stop_proxy(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl BuildsCodexTurn for CodexAdapter {
    fn turn_params(
        &self,
        thread_id: &str,
        flow_id: &str,
        goal: &str,
        origin: &OriginClue,
    ) -> serde_json::Value {
        serde_json::json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": format!("{goal}\n\nFlow identity:\nFLOW_ID={flow_id}\nFLOW_DIRECTORY=/home/li/primary/flows/{flow_id}\n\nOrigin clue:\nflow: {}\nsession: {}\nturn: {}", origin.flow_id, origin.session_id, origin.turn_id) }],
            "model": self.model,
            "effort": "medium",
            "turnTrigger": "flow-nexus"
        })
    }
}

impl CodexAdapter {
    fn initialize_bound_session(&self) -> Result<ProxySession, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        if let Err(error) = (|| {
            session.request(
                1,
                "initialize",
                serde_json::json!({
                    "clientInfo": {
                        "name": "flow-nexus",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
                self.timeout,
            )?;
            session.notify("initialized", serde_json::Value::Null)
        })() {
            session.stop_proxy();
            return Err(error);
        }
        Ok(session)
    }

    fn require_empty_bound_thread(
        &self,
        session: &mut ProxySession,
        request_id: u64,
        native_session_id: &str,
    ) -> Result<(), CodexAdapterUnavailable> {
        let read = session.request(
            request_id,
            "thread/read",
            serde_json::json!({
                "threadId": native_session_id,
                "includeTurns": true
            }),
            self.timeout,
        )?;
        let thread = read.get("thread").unwrap_or(&read);
        if thread.get("id").and_then(serde_json::Value::as_str) != Some(native_session_id) {
            return Err(CodexAdapterUnavailable::Protocol(
                "thread/read did not return the exact Herdr-bound thread".into(),
            ));
        }
        let turns = thread
            .get("turns")
            .or_else(|| thread.pointer("/history/turns"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                CodexAdapterUnavailable::Protocol(
                    "thread/read did not provide an authenticated turn list".into(),
                )
            })?;
        if !turns.is_empty() {
            return Err(CodexAdapterUnavailable::Protocol(
                "Herdr-bound Codex thread already has a turn".into(),
            ));
        }
        Ok(())
    }

    fn native_skill_catalog(
        &self,
        session: &mut ProxySession,
        request_id: u64,
    ) -> Result<Vec<serde_json::Value>, CodexAdapterUnavailable> {
        let workspace = self.workspace_root.canonicalize().map_err(|error| {
            CodexAdapterUnavailable::Protocol(format!(
                "configured Codex workspace is unavailable: {error}"
            ))
        })?;
        if !workspace.is_dir() || workspace != self.workspace_root {
            return Err(CodexAdapterUnavailable::Protocol(
                "configured Codex workspace must be an exact canonical directory".into(),
            ));
        }
        let reply = session.request(
            request_id,
            "skills/list",
            serde_json::json!({ "cwds": [workspace] }),
            self.timeout,
        )?;
        let available = reply
            .get("skills")
            .or_else(|| reply.pointer("/data/skills"))
            .or_else(|| reply.pointer("/data/items"))
            .or_else(|| reply.get("data"))
            .or_else(|| reply.pointer("/result/skills"))
            .unwrap_or(&reply)
            .as_array()
            .ok_or_else(|| {
                CodexAdapterUnavailable::Protocol("skills/list did not return an array".into())
            })?;
        let mut flattened = Vec::new();
        for item in available {
            if let Some(skills) = item.get("skills").and_then(serde_json::Value::as_array) {
                flattened.extend(skills.iter().cloned());
            } else if let Some(skill) = item.get("skill") {
                flattened.push(skill.clone());
            } else {
                flattened.push(item.clone());
            }
        }
        Ok(flattened)
    }

    fn resolve_catalog_skills(
        requested: &[String],
        catalog: &[serde_json::Value],
    ) -> Result<Vec<NativeSkillSelection>, CodexAdapterUnavailable> {
        requested
            .iter()
            .map(|name| {
                let matches = catalog
                    .iter()
                    .filter(|skill| {
                        skill.get("name").and_then(serde_json::Value::as_str) == Some(name.as_str())
                    })
                    .collect::<Vec<_>>();
                if matches.len() != 1 {
                    return Err(CodexAdapterUnavailable::Protocol(format!(
                        "native Codex catalog did not resolve {name} exactly once"
                    )));
                }
                let path = matches[0]
                    .get("path")
                    .and_then(serde_json::Value::as_str)
                    .map(Path::new)
                    .ok_or_else(|| {
                        CodexAdapterUnavailable::Protocol(format!(
                            "native Codex skill {name} has no path"
                        ))
                    })?;
                let metadata = fs::symlink_metadata(path).map_err(|error| {
                    CodexAdapterUnavailable::Protocol(format!(
                        "native Codex skill {name} is unreadable: {error}"
                    ))
                })?;
                if !path.is_absolute() || metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(CodexAdapterUnavailable::Protocol(format!(
                        "native Codex skill {name} is not an absolute regular source"
                    )));
                }
                let canonical = path.canonicalize().map_err(|error| {
                    CodexAdapterUnavailable::Protocol(format!(
                        "native Codex skill {name} cannot be canonicalized: {error}"
                    ))
                })?;
                if canonical != path {
                    return Err(CodexAdapterUnavailable::Protocol(format!(
                        "native Codex skill {name} path is not canonical"
                    )));
                }
                let source = fs::read(path).map_err(|error| {
                    CodexAdapterUnavailable::Protocol(format!(
                        "native Codex skill {name} is unreadable: {error}"
                    ))
                })?;
                Ok(NativeSkillSelection {
                    skill_name: name.clone(),
                    native_skill_path: path.to_string_lossy().into_owned(),
                    native_skill_sha256: format!("{:x}", Sha256::digest(source)),
                })
            })
            .collect()
    }

    fn bound_turn_params(
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> serde_json::Value {
        let mut input = intent
            .native_skill_selection_vector
            .iter()
            .map(|skill| {
                serde_json::json!({
                    "type": "skill",
                    "name": skill.skill_name,
                    "path": skill.native_skill_path
                })
            })
            .collect::<Vec<_>>();
        input.push(serde_json::json!({
            "type": "text",
            "text": launch.first_prompt_payload.first_prompt_text,
            "text_elements": []
        }));
        serde_json::json!({
            "threadId": intent.native_session_id,
            "effort": intent.effort,
            "model": intent.model_name,
            "input": input,
            "turnTrigger": "flow-nexus"
        })
    }
}

impl ResolvesBoundCodexSkills for CodexAdapter {
    fn resolve_bound_codex_skills(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<Vec<NativeSkillSelection>, CodexAdapterUnavailable> {
        if launch.launch_profile.harness_kind != HarnessKind::Codex
            || binding.harness_kind != HarnessKind::Codex
            || binding.launch_request_id != launch.launch_profile.launch_request_id
            || self.model != launch.launch_profile.model_name
        {
            return Err(CodexAdapterUnavailable::Protocol(
                "Codex skill resolution profile does not match its native binding".into(),
            ));
        }
        let mut session = self.initialize_bound_session()?;
        let result = (|| {
            self.require_empty_bound_thread(&mut session, 2, &binding.native_session_id)?;
            let catalog = self.native_skill_catalog(&mut session, 3)?;
            Self::resolve_catalog_skills(&launch.launch_profile.skill_name_vector, &catalog)
        })();
        session.stop_proxy();
        result
    }
}

impl SubmitsBoundCodexFirstTurn for CodexAdapter {
    fn submit_bound_codex_first_turn(
        &self,
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, CodexAdapterUnavailable> {
        if launch.launch_profile.harness_kind != HarnessKind::Codex
            || intent.harness_kind != HarnessKind::Codex
            || intent.launch_request_id != launch.launch_profile.launch_request_id
            || intent.prompt_sha256 != launch.first_prompt_payload.prompt_sha256
            || intent.model_name != launch.launch_profile.model_name
            || intent.effort != launch.launch_profile.effort
            || self.model != intent.model_name
        {
            return Err(CodexAdapterUnavailable::Protocol(
                "Codex first-turn intent does not match the composed launch".into(),
            ));
        }
        let expected_names = intent
            .native_skill_selection_vector
            .iter()
            .map(|skill| skill.skill_name.as_str())
            .collect::<Vec<_>>();
        if expected_names
            != launch
                .launch_profile
                .skill_name_vector
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
        {
            return Err(CodexAdapterUnavailable::Protocol(
                "Codex first-turn skill order differs from the launch profile".into(),
            ));
        }
        let mut session = self.initialize_bound_session()?;
        let result = (|| {
            self.require_empty_bound_thread(&mut session, 2, &intent.native_session_id)?;
            let catalog = self.native_skill_catalog(&mut session, 3)?;
            let selected =
                Self::resolve_catalog_skills(&launch.launch_profile.skill_name_vector, &catalog)?;
            if selected != intent.native_skill_selection_vector {
                return Err(CodexAdapterUnavailable::Protocol(
                    "native Codex skill selection changed after durable intent".into(),
                ));
            }
            session.request(
                4,
                "turn/start",
                Self::bound_turn_params(launch, intent),
                self.timeout,
            )?;
            Ok(PromptDeliveryResult::Ambiguous(intent.clone()))
        })();
        session.stop_proxy();
        result
    }
}

impl StartsCodex for CodexAdapter {
    fn start_codex(
        &self,
        flow_id: &str,
        goal: &str,
        origin: &OriginClue,
    ) -> Result<String, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let result = (|| {
            session.request(1, "initialize", serde_json::json!({ "clientInfo": { "name": "flow-nexus", "version": env!("CARGO_PKG_VERSION") } }), self.timeout)?;
            session.notify("initialized", serde_json::Value::Null)?;
            let flow_directory = if cfg!(test) {
                format!("/tmp/flow-nexus-test-{flow_id}")
            } else {
                format!("/home/li/primary/flows/{flow_id}")
            };
            std::fs::create_dir_all(&flow_directory)
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
            let started = session.request(
                2,
                "thread/start",
                serde_json::json!({
                    "cwd": "/home/li/primary",
                    "model": self.model,
                    "sandbox": "danger-full-access",
                    "approvalPolicy": "never",
                    "ephemeral": false,
                    "threadSource": "flow-nexus",
                    "config": { "shell_environment_policy": { "inherit": "core", "set": {
                        "FLOW_ID": flow_id,
                        "FLOW_DIRECTORY": flow_directory
                    }}}
                }),
                self.timeout,
            )?;
            let thread_id = started
                .pointer("/thread/id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    CodexAdapterUnavailable::Protocol("thread/start returned no thread id".into())
                })?
                .to_owned();
            session.request(
                3,
                "turn/start",
                self.turn_params(&thread_id, flow_id, goal, origin),
                self.timeout,
            )?;
            Ok(thread_id)
        })();
        session.stop_proxy();
        result
    }
}
impl CodexAdapter {
    pub fn start_codex_observed(
        &self,
        flow_id: &str,
        goal: &str,
        origin: &OriginClue,
        observer: impl FnOnce(&str) -> Result<(), CodexAdapterUnavailable>,
    ) -> Result<String, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let result = (|| {
            session.request(1,"initialize",serde_json::json!({"clientInfo":{"name":"flow-nexus","version":env!("CARGO_PKG_VERSION")}}),self.timeout)?;
            session.notify("initialized", serde_json::Value::Null)?;
            let flow_directory = if cfg!(test) {
                format!("/tmp/flow-nexus-test-{flow_id}")
            } else {
                format!("/home/li/primary/flows/{flow_id}")
            };
            std::fs::create_dir_all(&flow_directory)
                .map_err(|e| CodexAdapterUnavailable::Protocol(e.to_string()))?;
            let started = session.request(
                2,
                "thread/start",
                serde_json::json!({
                    "cwd":"/home/li/primary",
                    "model":self.model,
                    "sandbox":"danger-full-access",
                    "approvalPolicy":"never",
                    "ephemeral":false,
                    "threadSource":"flow-nexus",
                    "config": { "shell_environment_policy": { "inherit": "core", "set": {
                        "FLOW_ID": flow_id,
                        "FLOW_DIRECTORY": flow_directory
                    }}}
                }),
                self.timeout,
            )?;
            let thread = started
                .pointer("/thread/id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    CodexAdapterUnavailable::Protocol("thread/start returned no thread id".into())
                })?
                .to_owned();
            observer(&thread)?;
            session.request(
                3,
                "turn/start",
                self.turn_params(&thread, flow_id, goal, origin),
                self.timeout,
            )?;
            Ok(thread)
        })();
        session.stop_proxy();
        result
    }
}

impl ResumesCodex for CodexAdapter {
    fn resume_codex(
        &self,
        thread_id: &str,
        goal: &str,
        origin: &OriginClue,
    ) -> Result<(), CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let result = (|| {
            session.request(1, "initialize", serde_json::json!({ "clientInfo": { "name": "flow-nexus", "version": env!("CARGO_PKG_VERSION") } }), self.timeout)?;
            session.notify("initialized", serde_json::Value::Null)?;
            session.request(
                2,
                "thread/resume",
                serde_json::json!({ "threadId": thread_id }),
                self.timeout,
            )?;
            session.request(
                3,
                "turn/start",
                self.turn_params(thread_id, origin.flow_id.as_str(), goal, origin),
                self.timeout,
            )?;
            Ok(())
        })();
        session.stop_proxy();
        result
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
    use std::{
        ffi::OsString,
        fs,
        os::unix::fs::PermissionsExt,
        sync::{Mutex, OnceLock},
    };

    struct FakeProxy;

    trait BuildsFakeProxy {
        fn websocket_frame(&self, json: &str) -> String;
        fn executable(&self, frames: &[String]) -> String;
    }

    impl BuildsFakeProxy for FakeProxy {
        fn websocket_frame(&self, json: &str) -> String {
            let mut bytes = vec![0x81, json.len() as u8];
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

    struct PathScope {
        previous: Option<OsString>,
    }

    trait InstallsFakeProxy {
        fn install(&self, frames: &[String]) -> (tempfile::TempDir, PathScope);
    }

    impl InstallsFakeProxy for FakeProxy {
        fn install(&self, frames: &[String]) -> (tempfile::TempDir, PathScope) {
            let directory = tempfile::tempdir().unwrap();
            let executable = directory.path().join("codex");
            fs::write(&executable, self.executable(frames)).unwrap();
            fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
            let previous = std::env::var_os("PATH");
            let mut path = OsString::from(directory.path());
            if let Some(value) = &previous {
                path.push(":");
                path.push(value);
            }
            // Tests serialize PATH changes below, and Rust 2024 marks process
            // environment mutation unsafe because other threads could observe it.
            unsafe { std::env::set_var("PATH", path) };
            (directory, PathScope { previous })
        }
    }

    impl Drop for PathScope {
        fn drop(&mut self) {
            match &self.previous {
                Some(path) => unsafe { std::env::set_var("PATH", path) },
                None => unsafe { std::env::remove_var("PATH") },
            }
        }
    }

    fn fake_proxy_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn origin() -> OriginClue {
        OriginClue {
            flow_id: "parent-flow".into(),
            session_id: "session-1".into(),
            turn_id: "turn-1".into(),
        }
    }

    fn adapter() -> CodexAdapter {
        CodexAdapter {
            socket: "/tmp/fake-codex.sock".into(),
            model: "gpt-5.6".into(),
            timeout: Duration::from_millis(100),
        }
    }

    #[test]
    fn turn_brief_carries_assigned_flow_identity_and_origin() {
        let origin = origin();
        let params = adapter().turn_params("thread-1", "flow-0000000000000001", "start", &origin);
        let brief = params
            .pointer("/input/0/text")
            .and_then(serde_json::Value::as_str)
            .unwrap();
        assert!(brief.contains("FLOW_ID=flow-0000000000000001"));
        assert!(brief.contains("FLOW_DIRECTORY=/home/li/primary/flows/flow-0000000000000001"));
        assert!(brief.contains("flow: parent-flow\nsession: session-1\nturn: turn-1"));
    }

    #[test]
    fn fake_proxy_starts_a_thread_after_turn_start_is_accepted() {
        let _guard = fake_proxy_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(r#"{"id":2,"result":{"thread":{"id":"thread-1"}}}"#),
            fake.websocket_frame(r#"{"id":3,"result":{}}"#),
        ];
        let (_directory, _path) = fake.install(&frames);
        assert_eq!(
            adapter()
                .start_codex("flow-test", "start", &origin())
                .unwrap(),
            "thread-1"
        );
    }

    #[test]
    fn fake_proxy_refusal_does_not_report_a_started_thread() {
        let _guard = fake_proxy_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(r#"{"id":2,"error":{"code":-32000,"message":"denied"}}"#),
        ];
        let (_directory, _path) = fake.install(&frames);
        assert!(matches!(
            adapter().start_codex("flow-test", "start", &origin()),
            Err(CodexAdapterUnavailable::Refused { method, .. }) if method == "thread/start"
        ));
    }

    #[test]
    fn fake_proxy_timeout_does_not_report_a_started_thread() {
        let _guard = fake_proxy_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fake = FakeProxy;
        let frames = [fake.websocket_frame(r#"{"id":1,"result":{}}"#)];
        let (_directory, _path) = fake.install(&frames);
        assert!(matches!(
            adapter().start_codex("flow-test", "start", &origin()),
            Err(CodexAdapterUnavailable::TimedOut)
        ));
    }

    #[test]
    fn fake_proxy_consumes_a_reset_credit_as_a_typed_outcome() {
        let _guard = fake_proxy_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fake = FakeProxy;
        let frames = [
            fake.websocket_frame(r#"{"id":1,"result":{}}"#),
            fake.websocket_frame(r#"{"id":2,"result":{"outcome":"nothingToReset"}}"#),
        ];
        let (_directory, _path) = fake.install(&frames);
        assert_eq!(
            adapter()
                .consume_reset_credit(&meta_signal_flow::ResetRequest {
                    idempotency_key: "attempt-1".into(),
                    credit_selection: meta_signal_flow::CreditSelection::Next,
                })
                .unwrap(),
            meta_signal_flow::ResetOutcome::NothingToReset
        );
    }
}
