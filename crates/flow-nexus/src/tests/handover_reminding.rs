use crate::handover_reminding::DecidesHandoverReminder;
use crate::store::handover_reminders::{HandoverReminder, HandoverReminderRules, OpensHandoverReminder};
use signal_flow::{Address, Aspect, Model};

fn address() -> Address { Address { aspect: Aspect::Field, topic: "core".into(), model: Model::Sol } }

#[test]
fn warning_boundary_is_inclusive_without_rounding() {
    assert!(!HandoverReminder::warning_open(101, 15, 85));
    assert!(HandoverReminder::warning_open(101, 15, 86));
}

#[test]
fn first_notice_is_immediate_and_repeats_are_anchored() {
    let r = HandoverReminder::open_handover_reminder(address(), 3, "opus".into(), 100, 5, "Continue".into()).unwrap();
    assert!(r.notice_if_due(100).is_some());
    assert!(r.notice_if_due(99).is_none());
    assert_eq!(r.advanced_due(), Some(400));
}

#[test]
fn configured_text_and_generation_are_preserved() {
    let r = HandoverReminder::open_handover_reminder(address(), 8, "opus".into(), 10, 1, "configured".into()).unwrap();
    let n = r.notice_if_due(10).unwrap();
    assert_eq!(n.text, "configured");
    assert_eq!(n.generation, 8);
}

#[test]
fn recurrence_advances_from_each_due_instant() {
    let mut r = HandoverReminder::open_handover_reminder(address(), 1, "m".into(), 0, 5, "x".into()).unwrap();
    assert_eq!(r.advanced_due(), Some(300));
    r = r.advanced().unwrap();
    assert_eq!(r.next_due, 300);
    r = r.advanced().unwrap();
    assert_eq!(r.next_due, 600);
    r = r.advanced().unwrap();
    assert_eq!(r.next_due, 900);
}
