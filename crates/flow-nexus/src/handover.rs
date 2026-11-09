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

use signal_flow::{Address, HandoverFault};
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
        // A FlowId names a directory: letters, digits, hyphen and underscore.
        let names_a_flow = !predecessor.is_empty()
            && predecessor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
        if !names_a_flow {
            return Handover::Unreadable(HandoverFault::InvalidPredecessor);
        }
        let path = launch_root
            .join("flows")
            .join(predecessor)
            .join("handover.md");
        match std::fs::read(&path) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(text) if text.is_empty() => Handover::Absent,
                Ok(text) => Handover::Text(text),
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
