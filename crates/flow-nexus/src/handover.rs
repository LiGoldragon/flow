//! The handover a successor flow is given: its predecessor's handover text,
//! placed whole in the successor's first prompt after the brief, followed
//! only by the requests its permaflow still owes an answer.
//!
//! The composition asks one source, [`YieldsHandover`], for the text of a
//! predecessor and places whatever it yields; where the text lives is the
//! source's alone. Runtime launches use the live Memory source, while
//! [`HandoverFile`] remains the file-backed capability for callers that choose
//! it. A source is selected through
//! [`crate::composition::OpensLaunchComposer::with_handover_source`]; the order
//! of the prompt does not change.

use signal_flow::{Address, FlowId, HandoverFault};
use std::path::Path;

/// What a source holds of a predecessor's handover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Handover {
    /// The predecessor left none: nothing is placed.
    Absent,
    /// The handover text, to be placed whole.
    Text(String),
    /// The source holds one it could not read whole, for this fault. The
    /// launch is refused `HandoverUnreadable` with it.
    Unreadable(HandoverFault),
}

/// Yields the handover of a predecessor flow.
///
/// `launch_root` is the root the successor runs in; a source that is not
/// kept under a root ignores it.
pub trait YieldsHandover: std::fmt::Debug + Send + Sync {
    fn handover(&self, address: &Address, predecessor: &str, launch_root: &Path) -> Handover;
    fn crystallization(&self, _predecessor: &str) -> Handover {
        Handover::Absent
    }
    fn companion_handover(&self, _predecessor: &str) -> Handover {
        Handover::Absent
    }
    fn requires_launch_root(&self) -> bool {
        true
    }
}

/// The file-backed source: `flows/<predecessor FlowId>/handover.md` under
/// the launch root. A missing file, or an empty one, is [`Handover::Absent`];
/// a predecessor that names no flow, a file that cannot be read, or one that
/// is not UTF-8, is [`Handover::Unreadable`].
#[derive(Clone, Copy, Debug, Default)]
pub struct HandoverFile;

impl YieldsHandover for HandoverFile {
    fn handover(&self, _address: &Address, predecessor: &str, launch_root: &Path) -> Handover {
        PredecessorDocument::Handover.read_under(&launch_root.join("flows"), predecessor)
    }
}

pub(crate) enum PredecessorDocument {
    Handover,
    Crystallization,
}

pub(crate) trait ReadsPredecessorDocument {
    fn read_under(self, flows_root: &Path, predecessor: &str) -> Handover;
}

impl ReadsPredecessorDocument for PredecessorDocument {
    fn read_under(self, flows_root: &Path, predecessor: &str) -> Handover {
        // A FlowId names a directory: letters, digits, hyphen and underscore.
        let names_a_flow = !predecessor.is_empty()
            && predecessor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
        if !names_a_flow {
            return Handover::Unreadable(HandoverFault::InvalidPredecessor);
        }
        let filename = match self {
            Self::Handover => "handover.md",
            Self::Crystallization => "crystallization.md",
        };
        let path = flows_root.join(predecessor).join(filename);
        match std::fs::read(&path) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(text) if text.is_empty() => Handover::Absent,
                Ok(text) => Handover::Text(text),
                Err(_) if matches!(self, Self::Crystallization) => Handover::Unreadable(
                    HandoverFault::Unreadable(format!("{}: not UTF-8", path.display())),
                ),
                Err(_) => Handover::Unreadable(HandoverFault::NotUtf8),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Handover::Absent,
            Err(error) => Handover::Unreadable(HandoverFault::Unreadable(format!(
                "{}: {error}",
                path.display()
            ))),
        }
    }
}

/// The line placed directly before the requests a successor's predecessor
/// left open, naming that predecessor by its FlowId.
pub trait FramesOpenRequests {
    fn open_requests_line(&self) -> String;
}

impl FramesOpenRequests for FlowId {
    /// `self` is the predecessor's FlowId.
    fn open_requests_line(&self) -> String {
        format!(
            "Flow {}, which this flow succeeds, left these requests open; answer each by its id.",
            self.0
        )
    }
}
