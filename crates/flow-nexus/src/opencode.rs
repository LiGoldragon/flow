//! OpenCode's side of a flow's start, before its TUI attaches: the
//! configuration directory Flow writes for one flow's server, and the two
//! calls Flow makes to that server, the agent looked up and the session
//! opened with the id Flow chose.
//!
//! OpenCode 2.4.1, as a test capsule showed it:
//! - `OPENCODE_CONFIG_DIR` names the server's whole configuration directory
//!   in place of the user's: its `opencode.json` and its `plugins/`, so a
//!   flow's server reads Flow's directory and none of the user's.
//! - `POST /api/session` with an `id` creates that session, and answers the
//!   same session, unchanged, when it already stands: Resume is the same
//!   call as Start. It creates a session for an agent that does not exist.
//! - `GET /api/agent/<name>` answers `AgentNotFoundError` for a few seconds
//!   after the server's first request, while its agents load, then the
//!   agent; an absent agent is answered the same way for good. The lookup
//!   is therefore repeated until the agent stands or the wait passes, and
//!   the last answer is kept whole.
//!
//! Not yet wired into the launch: Flow does not start the server in the
//! pane yet, so this is compiled for tests alone until it does.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::emission::WritesOpenCodeConfiguration;
use crate::generated::emission::{OpenCodeAgent, SystemPromptFile};

/// OpenCode's own names, which Flow does not choose: the variable naming its
/// configuration directory, the user its Basic authentication takes, the files it reads in that
/// directory, and its routes.
pub(crate) trait SpeaksOpenCode {
    const CONFIGURATION_DIRECTORY_VARIABLE: &str = "OPENCODE_CONFIG_DIR";
    const SERVER_USER: &str = "opencode";
    const CONFIGURATION_FILE: &str = "opencode.json";
    const PLUGIN_DIRECTORY: &str = "plugins";
    const SESSION_ROUTE: &str = "/api/session";
    const AGENT_ROUTE: &str = "/api/agent";
}

/// The layout Flow chooses inside a flow's configuration directory: the
/// file its agent's system prompt is read from, and the name the report
/// plugin is placed under.
pub(crate) trait LaysOutOpenCodeFlow {
    const SYSTEM_PROMPT_FILE: &str = "system.md";
    const REPORT_PLUGIN: &str = "flow-report";
}

/// One flow's OpenCode configuration directory, under the configured root
/// (`opencode_configuration_root` of the defaults), named by the flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenCodeFlowDirectory {
    pub(crate) path: PathBuf,
}

impl SpeaksOpenCode for OpenCodeFlowDirectory {}
impl LaysOutOpenCodeFlow for OpenCodeFlowDirectory {}

pub(crate) trait WritesOpenCodeFlow: Sized {
    fn under(root: &Path, flow: &str) -> Self;
    /// The file the agent's system prompt is written to and read from.
    fn system_prompt_file(&self) -> SystemPromptFile;
    /// Writes the system prompt, the configuration naming the agent as the
    /// default, and the report plugin's place: `plugins/flow-report`, a link
    /// to the installed plugin. A second write replaces the first.
    fn write(
        &self,
        agent: &OpenCodeAgent,
        system_prompt: &str,
        report_plugin: &Path,
    ) -> std::io::Result<()>;
    /// The environment the flow's server is started with so that it reads
    /// this directory.
    fn server_environment(&self) -> (&'static str, PathBuf);
}

impl WritesOpenCodeFlow for OpenCodeFlowDirectory {
    fn under(root: &Path, flow: &str) -> Self {
        Self {
            path: root.join(flow),
        }
    }

    fn system_prompt_file(&self) -> SystemPromptFile {
        SystemPromptFile(
            self.path
                .join(Self::SYSTEM_PROMPT_FILE)
                .to_string_lossy()
                .into_owned(),
        )
    }

    fn write(
        &self,
        agent: &OpenCodeAgent,
        system_prompt: &str,
        report_plugin: &Path,
    ) -> std::io::Result<()> {
        let plugins = self.path.join(Self::PLUGIN_DIRECTORY);
        std::fs::create_dir_all(&plugins)?;
        std::fs::write(&agent.system_prompt_file.0, system_prompt)?;
        let configuration = serde_json::to_vec_pretty(&agent.opencode_configuration())
            .map_err(std::io::Error::other)?;
        std::fs::write(self.path.join(Self::CONFIGURATION_FILE), configuration)?;
        let placed = plugins.join(Self::REPORT_PLUGIN);
        match std::fs::symlink_metadata(&placed) {
            Ok(_) => std::fs::remove_file(&placed)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        std::os::unix::fs::symlink(report_plugin, placed)
    }

    fn server_environment(&self) -> (&'static str, PathBuf) {
        (Self::CONFIGURATION_DIRECTORY_VARIABLE, self.path.clone())
    }
}

/// A flow's running OpenCode server, as Flow reaches it: its address, the
/// password it was started with, how long one exchange may take, and how
/// long Flow waits between two lookups of an agent still loading.
#[derive(Debug, Clone)]
pub(crate) struct OpenCodeServer {
    pub(crate) address: SocketAddr,
    pub(crate) password: String,
    pub(crate) exchange_timeout: Duration,
    pub(crate) retry_interval: Duration,
}

impl SpeaksOpenCode for OpenCodeServer {}

/// Why OpenCode did not do what Flow asked: it could not be reached, or it
/// answered otherwise, its words kept whole.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum OpenCodeRefusal {
    #[error("OpenCode server unreachable: {0}")]
    Unreachable(String),
    #[error("OpenCode answered {status}: {words}")]
    Answered { status: u16, words: String },
}

pub(crate) trait AsksOpenCode {
    /// Repeats the lookup of the agent until OpenCode answers it, or the
    /// wait passes; then the last refusal, whole.
    fn await_agent(&self, agent: &str, wait: Duration) -> Result<(), OpenCodeRefusal>;
    /// Opens the session of the chosen id, for the agent: creates it, or
    /// answers it as it stands.
    fn open_session(&self, session: &str, agent: &str) -> Result<(), OpenCodeRefusal>;
}

impl AsksOpenCode for OpenCodeServer {
    fn await_agent(&self, agent: &str, wait: Duration) -> Result<(), OpenCodeRefusal> {
        let route = format!("{}/{agent}", Self::AGENT_ROUTE);
        let deadline = Instant::now() + wait;
        loop {
            let refusal = match self.exchange("GET", &route, None) {
                Ok((200, _)) => return Ok(()),
                Ok((status, words)) => OpenCodeRefusal::Answered { status, words },
                Err(refusal) => refusal,
            };
            if Instant::now() >= deadline {
                return Err(refusal);
            }
            std::thread::sleep(
                self.retry_interval
                    .min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }

    fn open_session(&self, session: &str, agent: &str) -> Result<(), OpenCodeRefusal> {
        let body = serde_json::json!({ "id": session, "agent": agent });
        match self.exchange("POST", Self::SESSION_ROUTE, Some(&body))? {
            (200, words) => {
                let answered: serde_json::Value =
                    serde_json::from_str(&words).map_err(|_| OpenCodeRefusal::Answered {
                        status: 200,
                        words: words.clone(),
                    })?;
                if answered["data"]["id"] == session {
                    Ok(())
                } else {
                    Err(OpenCodeRefusal::Answered { status: 200, words })
                }
            }
            (status, words) => Err(OpenCodeRefusal::Answered { status, words }),
        }
    }
}

impl OpenCodeServer {
    /// One HTTP/1.1 exchange, the connection closed after it; the status
    /// and the body as text.
    fn exchange(
        &self,
        method: &str,
        route: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<(u16, String), OpenCodeRefusal> {
        use base64::Engine;
        let unreachable = |error: std::io::Error| OpenCodeRefusal::Unreachable(error.to_string());
        let body = body.map(serde_json::Value::to_string).unwrap_or_default();
        let credential = base64::engine::general_purpose::STANDARD.encode(format!(
            "{}:{}",
            Self::SERVER_USER,
            self.password
        ));
        let request = format!(
            "{method} {route} HTTP/1.1\r\nHost: {}\r\nAuthorization: Basic {credential}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.address,
            body.len()
        );
        let mut stream = TcpStream::connect_timeout(&self.address, self.exchange_timeout)
            .map_err(unreachable)?;
        stream
            .set_read_timeout(Some(self.exchange_timeout))
            .map_err(unreachable)?;
        stream.write_all(request.as_bytes()).map_err(unreachable)?;
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).map_err(unreachable)?;
        HttpAnswer(answer).read()
    }
}

/// A whole HTTP/1.1 answer, read once its connection closed.
struct HttpAnswer(Vec<u8>);

impl HttpAnswer {
    fn read(self) -> Result<(u16, String), OpenCodeRefusal> {
        let text = String::from_utf8_lossy(&self.0).into_owned();
        let malformed = || OpenCodeRefusal::Unreachable(format!("malformed answer: {text}"));
        let (head, body) = text.split_once("\r\n\r\n").ok_or_else(malformed)?;
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|status| status.parse().ok())
            .ok_or_else(malformed)?;
        let chunked = head.lines().any(|line| {
            line.to_ascii_lowercase()
                .starts_with("transfer-encoding: chunked")
        });
        let body = if chunked {
            Self::unchunked(body).ok_or_else(malformed)?
        } else {
            body.to_owned()
        };
        Ok((status, body))
    }

    fn unchunked(mut body: &str) -> Option<String> {
        let mut whole = String::new();
        loop {
            let (size, rest) = body.split_once("\r\n")?;
            let size = usize::from_str_radix(size.split(';').next()?.trim(), 16).ok()?;
            if size == 0 {
                return Some(whole);
            }
            whole.push_str(rest.get(..size)?);
            body = rest.get(size..)?.strip_prefix("\r\n")?;
        }
    }
}
