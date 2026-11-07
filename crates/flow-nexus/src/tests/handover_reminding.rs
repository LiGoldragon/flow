use crate::handover_reminding::DecidesHandoverReminder;
use crate::store::handover_reminders::{
    HandoverReminder, HandoverReminderRules, OpensHandoverReminder,
};
use signal_flow::{Address, Aspect, Model};

fn address() -> Address {
    Address {
        aspect: Aspect::Field,
        topic: "core".into(),
        model: Model::Sol,
    }
}

#[test]
fn warning_boundary_is_inclusive_without_rounding() {
    assert!(!HandoverReminder::warning_open(101, 15, 85));
    assert!(HandoverReminder::warning_open(101, 15, 86));
}

#[test]
fn first_notice_is_immediate_and_repeats_are_anchored() {
    let r = HandoverReminder::open_handover_reminder(
        address(),
        3,
        "opus".into(),
        100,
        5,
        "Continue".into(),
    )
    .unwrap();
    assert!(r.notice_if_due(100).is_some());
    assert!(r.notice_if_due(99).is_none());
    assert_eq!(r.advanced_due(), Some(400));
}

#[test]
fn configured_text_and_generation_are_preserved() {
    let r = HandoverReminder::open_handover_reminder(
        address(),
        8,
        "opus".into(),
        10,
        1,
        "configured".into(),
    )
    .unwrap();
    let n = r.notice_if_due(10).unwrap();
    assert_eq!(n.text, "configured");
    assert_eq!(n.generation, 8);
}

#[test]
fn recurrence_advances_from_each_due_instant() {
    let mut r =
        HandoverReminder::open_handover_reminder(address(), 1, "m".into(), 0, 5, "x".into())
            .unwrap();
    assert_eq!(r.advanced_due(), Some(300));
    r = r.advanced().unwrap();
    assert_eq!(r.next_due, 300);
    r = r.advanced().unwrap();
    assert_eq!(r.next_due, 600);
    r = r.advanced().unwrap();
    assert_eq!(r.next_due, 900);
}

#[test]
fn window_requires_boundary_qualified_current_flow_touch() {
    use crate::handover_reminding::{DiscoversHandoverWindow, HandoverWindowObservation};
    let mut o = HandoverWindowObservation { address: address(), generation: 1, model_name: "m".into(), refresh_threshold: 101, warning_share_left: 15, context_tokens: 86, current_flow_id: "current".into(), touched_flow_id: Some("current".into()), green: false, reminder_text: "x".into() };
    assert!(o.warning_window_is_current());
    o.green = true;
    assert!(!o.warning_window_is_current());
    o.touched_flow_id = Some("successor".into());
    assert!(!o.warning_window_is_current());
}

#[test]
fn typed_keep_and_drop_commands_are_shell_single_argument_forms() {
    use crate::handover_reminding::{HandoverReminderNotice, RendersHandoverCommands};
    use signal_flow::{Query, Section};
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Compactable, Protosizable, ReaderBudget};
    let notice = HandoverReminderNotice { address: address(), text: "Configured reminder".into(), generation: 1 };
    let section = Section { title: "title".into(), description: "description".into(), body: "body".into() };
    let text = notice.keep_commands(std::slice::from_ref(&section));
    assert!(text.starts_with("flow '") && text.ends_with("'"));
    let payload = text.strip_prefix("flow '").unwrap().strip_suffix(''').unwrap().replace("'\"'\"'", "'");
    let mut budget = Budget { remaining: 4096, reader: ReaderBudget::try_from(4096).unwrap(), depth: 0, maximum_depth: 1024 };
    let query = Potential::<Query>::from(payload.as_str()).actualize(&mut budget).unwrap();
    assert_eq!(query.datomize(vec![]).protosize().compact(), payload);
    assert!(notice.drop_command("title".into()).starts_with("flow '"));
}

#[test]
fn runtime_tick_admits_due_hard_notice_and_wakes_recipient() {
    use super::NexusFixture;
    use crate::handover_reminding::RemindsHandoverCallers;
    use crate::store::handover_reminders::{HandoverReminder, OpensHandoverReminder, SchedulesHandoverReminders};
    use crate::store::metaflow::SeedsMetaflow;
    use flow_ethos::{Metaflow, State};
    let fixture = NexusFixture::new();
    let address = address();
    fixture.nexus.store.seed_metaflow(Metaflow { address: address.clone(), state: State::Asleep, past: vec![], queue: vec![] });
    let reminder = HandoverReminder::open_handover_reminder(address.clone(), 0, "m".into(), 10, 5, "configured".into()).unwrap();
    fixture.nexus.store.hold_handover_reminder(reminder).unwrap();
    fixture.nexus.send_due_handover_reminders(10);
    let held = fixture.nexus.store.handover_reminder(&address, 0).unwrap().unwrap();
    assert_eq!(held.sent, 1);
    assert_eq!(held.next_due, 310);
}
