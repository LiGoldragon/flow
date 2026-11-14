//! The native harness command as typed words: configured tail parsing,
//! effect-created resource projection, and one shell rendering.

use signal_flow::{HarnessFlagsRefusal, HarnessKind};

/// Parses the configured shell-word grammar without expansion or evaluation.
pub(crate) trait ParsesHarnessFlags {
    fn harness_arguments(&self) -> Result<Vec<String>, HarnessFlagsRefusal>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Quote {
    Unquoted,
    Single,
    Double,
}

impl ParsesHarnessFlags for str {
    fn harness_arguments(&self) -> Result<Vec<String>, HarnessFlagsRefusal> {
        let mut words = Vec::new();
        let mut word = String::new();
        let mut started = false;
        let mut quote = Quote::Unquoted;
        let mut chars = self.chars().peekable();
        while let Some(character) = chars.next() {
            match quote {
                Quote::Unquoted => match character {
                    character if character.is_whitespace() => {
                        if started {
                            words.push(std::mem::take(&mut word));
                            started = false;
                        }
                    }
                    '\'' => {
                        started = true;
                        quote = Quote::Single;
                    }
                    '"' => {
                        started = true;
                        quote = Quote::Double;
                    }
                    '\\' => match chars.next() {
                        Some('\n') => {}
                        Some(next) => {
                            started = true;
                            word.push(next);
                        }
                        None => return Err(HarnessFlagsRefusal::TrailingEscape),
                    },
                    ordinary => {
                        started = true;
                        word.push(ordinary);
                    }
                },
                Quote::Single => {
                    if character == '\'' {
                        quote = Quote::Unquoted;
                    } else {
                        word.push(character);
                    }
                }
                Quote::Double => match character {
                    '"' => quote = Quote::Unquoted,
                    '\\' => match chars.next() {
                        Some('\n') => {}
                        Some(next @ ('$' | '`' | '"' | '\\')) => word.push(next),
                        Some(next) => {
                            word.push('\\');
                            word.push(next);
                        }
                        None => return Err(HarnessFlagsRefusal::TrailingEscape),
                    },
                    ordinary => word.push(ordinary),
                },
            }
        }
        match quote {
            Quote::Single => return Err(HarnessFlagsRefusal::UnclosedSingleQuote),
            Quote::Double => return Err(HarnessFlagsRefusal::UnclosedDoubleQuote),
            Quote::Unquoted => {}
        }
        if started {
            words.push(word);
        }
        Ok(words)
    }
}

/// Every resource word the final invocation needs after launch effects have
/// created or selected it. Rehearsal supplies semantic placeholders instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HarnessCommandResources {
    pub herdr_session: String,
    pub agent: String,
    pub pane: String,
    pub program: String,
    pub codex_socket: Option<String>,
    pub claude_settings: Option<String>,
    pub claude_title: Option<String>,
    pub prompt_fd: Option<String>,
    pub claude_session: Option<String>,
    /// The session a Claude restart resumes: `--resume`, in place of
    /// `--session-id`.
    pub claude_resume: Option<String>,
    pub thread: Option<String>,
}

/// Produces the resource projection used by a rehearsal. It creates nothing.
pub(crate) trait RehearsesHarnessResources {
    fn rehearsal(harness: &HarnessKind) -> Self;
}

impl RehearsesHarnessResources for HarnessCommandResources {
    fn rehearsal(harness: &HarnessKind) -> Self {
        match harness {
            HarnessKind::Claude => Self {
                herdr_session: "<session>".into(),
                agent: "<agent>".into(),
                pane: "<pane>".into(),
                program: "claude".into(),
                codex_socket: None,
                claude_settings: Some("<settings>".into()),
                // The reserved Flow identity determines the native title.
                claude_title: Some("<flow-id>".into()),
                prompt_fd: Some("<prompt-fd>".into()),
                claude_session: Some("<claude-session>".into()),
                claude_resume: None,
                thread: None,
            },
            HarnessKind::Codex => Self {
                herdr_session: "<session>".into(),
                agent: "<agent>".into(),
                pane: "<pane>".into(),
                program: "<codex-client>".into(),
                codex_socket: Some("<codex-socket>".into()),
                claude_settings: None,
                claude_title: None,
                prompt_fd: None,
                claude_session: None,
                claude_resume: None,
                thread: Some("<thread>".into()),
            },
        }
    }
}

/// Renders every argv word with shell quoting, including safe words and
/// placeholders. The renderer performs no expansion or evaluation.
pub(crate) trait RendersShellCommand {
    fn shell_command(&self) -> String;
}

impl RendersShellCommand for [String] {
    fn shell_command(&self) -> String {
        self.iter()
            .map(|word| format!("'{}'", word.replace('\'', "'\\''")))
            .collect::<Vec<_>>()
            .join(" ")
    }
}
