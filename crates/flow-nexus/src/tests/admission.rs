//! The Nexus logs each Send it admits on one line: the logical delivery
//! request's kind, a Task's Minutes and the deadline its first
//! reminder is due at, and the time of admission, in Unix seconds to the
//! millisecond, so a reminder can be witnessed against its deadline to the
//! second.

use crate::admission::{Admission, LogsAdmission};
use flow_ethos::{AnswerableTask, DonePayload, Historical_Data, Request};
use std::time::{Duration, UNIX_EPOCH};

#[test]
fn a_task_admission_names_its_minutes_deadline_and_time() {
    let task = Request::Task(AnswerableTask {
        minutes: 1,
        string: "Answer with one word.".into(),
    });
    assert_eq!(
        Admission {
            request: &task,
            deadline: Some(1_793_329_086),
            at: UNIX_EPOCH + Duration::from_millis(1_793_329_026_123),
        }
        .log_line(),
        "flow-nexus: admitted Task minutes 1 deadline 1793329086 at 1793329026.123"
    );
}

/// A Task whose sender Flow does not know sets no reminder, so it has no
/// deadline.
#[test]
fn a_task_with_no_reminder_names_no_deadline() {
    let task = Request::Task(AnswerableTask {
        minutes: 5,
        string: "Answer.".into(),
    });
    assert_eq!(
        Admission {
            request: &task,
            deadline: None,
            at: UNIX_EPOCH + Duration::from_millis(1_793_329_026_004),
        }
        .log_line(),
        "flow-nexus: admitted Task minutes 5 deadline None at 1793329026.004"
    );
}

#[test]
fn every_other_admission_names_its_kind_and_time() {
    for (request, kind) in [
        (Request::Order("Draft it.".into()), "Order"),
        (Request::Notice("Noted.".into()), "Notice"),
        (
            Request::Done(DonePayload::Historical(Historical_Data {
                logical_delivery_id: "logical-delivery-00000000000000000042".into(),
                string: "Done.".into(),
            })),
            "Done",
        ),
    ] {
        assert_eq!(
            Admission {
                request: &request,
                deadline: None,
                at: UNIX_EPOCH + Duration::from_millis(1_793_329_027_500),
            }
            .log_line(),
            format!("flow-nexus: admitted {kind} at 1793329027.500")
        );
    }
}
