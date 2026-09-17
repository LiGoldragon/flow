//! The Codex app-server is reached only through `codex app-server proxy`.
//! The proxy is a byte bridge, so this module owns its bounded WebSocket and
//! JSON-RPC conversation; it never falls back to a direct Unix-socket client.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use signal_flow::Origin;
use std::{
    io::{BufRead, BufReader, Read, Write},
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
    fn start_codex(&self, goal: &str, origin: &Origin) -> Result<String, CodexAdapterUnavailable>;
}

pub trait ResumesCodex {
    fn resume_codex(
        &self,
        thread_id: &str,
        goal: &str,
        origin: &Origin,
    ) -> Result<(), CodexAdapterUnavailable>;
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
    fn turn_params(&self, thread_id: &str, goal: &str, origin: &Origin) -> serde_json::Value;
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
    fn turn_params(&self, thread_id: &str, goal: &str, origin: &Origin) -> serde_json::Value {
        serde_json::json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": format!("{goal}\n\nOrigin clue:\nflow: {}\nsession: {}\nturn: {}", origin.parent_flow_id, origin.session, origin.turn) }],
            "model": self.model,
            "effort": "medium",
            "turnTrigger": "flow-nexus"
        })
    }
}

impl StartsCodex for CodexAdapter {
    fn start_codex(&self, goal: &str, origin: &Origin) -> Result<String, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let result = (|| {
            session.request(1, "initialize", serde_json::json!({ "clientInfo": { "name": "flow-nexus", "version": env!("CARGO_PKG_VERSION") } }), self.timeout)?;
            session.notify("initialized", serde_json::Value::Null)?;
            let cwd = std::env::current_dir()
                .map_err(|error| CodexAdapterUnavailable::Protocol(error.to_string()))?;
            let started = session.request(2, "thread/start", serde_json::json!({ "cwd": cwd, "model": self.model, "sandbox": "danger-full-access", "approvalPolicy": "never", "ephemeral": false, "threadSource": "flow-nexus" }), self.timeout)?;
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
                self.turn_params(&thread_id, goal, origin),
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
        goal: &str,
        origin: &Origin,
        observer: impl FnOnce(&str) -> Result<(), CodexAdapterUnavailable>,
    ) -> Result<String, CodexAdapterUnavailable> {
        let mut session = self.open_proxy()?;
        let result = (|| {
            session.request(1,"initialize",serde_json::json!({"clientInfo":{"name":"flow-nexus","version":env!("CARGO_PKG_VERSION")}}),self.timeout)?;
            session.notify("initialized", serde_json::Value::Null)?;
            let cwd = std::env::current_dir()
                .map_err(|e| CodexAdapterUnavailable::Protocol(e.to_string()))?;
            let started=session.request(2,"thread/start",serde_json::json!({"cwd":cwd,"model":self.model,"sandbox":"danger-full-access","approvalPolicy":"never","ephemeral":false,"threadSource":"flow-nexus"}),self.timeout)?;
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
                self.turn_params(&thread, goal, origin),
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
        origin: &Origin,
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
                self.turn_params(thread_id, goal, origin),
                self.timeout,
            )?;
            Ok(())
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

    fn origin() -> Origin {
        Origin {
            parent_flow_id: "parent-flow".into(),
            session: "session-1".into(),
            turn: "turn-1".into(),
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
            adapter().start_codex("start", &origin()).unwrap(),
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
            adapter().start_codex("start", &origin()),
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
            adapter().start_codex("start", &origin()),
            Err(CodexAdapterUnavailable::TimedOut)
        ));
    }
}
