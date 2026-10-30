//! The line the Nexus logs for each Send it admits: the request's kind,
//! a Task's Minutes and the deadline its first
//! reminder is due at in Unix seconds, and the time of admission in Unix
//! seconds to the millisecond. A reminder is witnessed against the logged
//! deadline to the second.

use flow_ethos::Request;
use std::time::{SystemTime, UNIX_EPOCH};

/// One Send the Nexus admitted.
pub(crate) struct Admission<'admitted> {
    pub(crate) request: &'admitted Request,
    /// When a Task's first reminder is due, in Unix seconds; None for any
    /// other request, and for a Task Flow sets no reminder for.
    pub(crate) deadline: Option<i64>,
    pub(crate) at: SystemTime,
}

pub(crate) trait LogsAdmission {
    fn log_line(&self) -> String;
}

/// The request's variant, as its datom heads it.
trait NamesRequestKind {
    fn kind(&self) -> &'static str;
}

impl NamesRequestKind for Request {
    fn kind(&self) -> &'static str {
        match self {
            Request::Order(_) => "Order",
            Request::Question(_) => "Question",
            Request::Psyche(_) => "Psyche",
            Request::Psyches(_) => "Psyches",
            Request::Result(_) => "Result",
            Request::Notice(_) => "Notice",
            Request::Task(_) => "Task",
            Request::Done(_) => "Done",
        }
    }
}

impl LogsAdmission for Admission<'_> {
    fn log_line(&self) -> String {
        let since = self.at.duration_since(UNIX_EPOCH).unwrap_or_default();
        let at = format!("{}.{:03}", since.as_secs(), since.subsec_millis());
        match self.request {
            Request::Task(task) => format!(
                "flow-nexus: admitted Task minutes {} deadline {} at {at}",
                task.minutes,
                self.deadline
                    .map_or_else(|| "None".to_owned(), |deadline| deadline.to_string())
            ),
            request => format!("flow-nexus: admitted {} at {at}", request.kind()),
        }
    }
}
