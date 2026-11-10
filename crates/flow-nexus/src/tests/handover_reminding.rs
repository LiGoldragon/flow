use crate::handover_reminding::RemindsHandoverCallers;
use crate::handover_reminding::{DecidesHandoverReminder, RendersHandoverCommands};
use crate::reporting::RecordsReports;
use crate::store::handover::HoldsHandovers;
use crate::store::handover_reminders::SchedulesHandoverReminders;
use crate::store::handover_reminders::{
    HandoverReminder, HandoverReminderRules, OpensHandoverReminder,
};
use signal_flow::{Address, Aspect, Model};

fn address() -> Address {
    Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("core".into()),
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
fn an_empty_handover_notice_still_has_typed_keep_and_drop_forms() {
    let reminder =
        HandoverReminder::open_handover_reminder(address(), 1, "m".into(), 0, 5, "continue".into())
            .unwrap();
    let notice = reminder.notice_if_due(0).unwrap();
    let text = notice.notice_text(&[]);
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].contains("Keep"), "{text}");
    assert!(lines[1].contains("Drop"), "{text}");
    assert_eq!(lines[2], "continue");
}

#[test]
fn window_requires_boundary_qualified_current_flow_touch() {
    use crate::handover_reminding::{DiscoversHandoverWindow, HandoverWindowObservation};
    let mut o = HandoverWindowObservation {
        address: address(),
        generation: 1,
        model_name: "m".into(),
        refresh_threshold: 101,
        warning_share_left: 15,
        context_tokens: 86,
        current_flow_id: "current".into(),
        touched_flow_id: Some("current".into()),
        green: false,
        reminder_text: "x".into(),
    };
    assert!(o.warning_window_is_current());
    o.green = true;
    assert!(!o.warning_window_is_current());
    o.touched_flow_id = Some("successor".into());
    assert!(!o.warning_window_is_current());
}

#[test]
fn typed_keep_and_drop_commands_are_shell_single_argument_forms() {
    use crate::handover_reminding::{HandoverReminderNotice, RendersHandoverCommands};
    use datom_codec::{Actualizing, Budget, Potential};
    use protos::ReaderBudget;
    use signal_flow::{Query, Section};
    let notice = HandoverReminderNotice {
        address: address(),
        text: "Configured reminder".into(),
        generation: 1,
    };
    let section = Section {
        title: signal_flow::Title("title with ' apostrophe".into()),
        description: signal_flow::Description("description with ' apostrophe".into()),
        body: signal_flow::Body("body with ' apostrophe".into()),
    };
    let parse = |text: String| {
        assert!(text.starts_with("flow-nexus '") && text.ends_with("'"));
        let payload = text
            .strip_prefix("flow-nexus '")
            .unwrap()
            .strip_suffix('\'')
            .unwrap()
            .replace("'\"'\"'", "'");
        let mut budget = Budget {
            remaining: 4096,
            reader: ReaderBudget::try_from(4096).unwrap(),
            depth: 0,
            maximum_depth: 1024,
        };
        Potential::<Query>::from(payload.as_str())
            .actualize(&mut budget)
            .unwrap()
    };
    let keep = parse(notice.keep_commands(std::slice::from_ref(&section)));
    let Query::Keep(keep) = keep else {
        panic!("expected Keep query")
    };
    assert_eq!(keep.section, section);
    let drop = parse(notice.drop_command(section.title.clone()));
    let Query::Drop(drop) = drop else {
        panic!("expected Drop query")
    };
    assert_eq!(drop.title, section.title);
    assert_eq!(drop.address, address());
}

#[test]
fn runtime_tick_admits_due_hard_notice_and_wakes_recipient() {
    use super::NexusFixture;
    use crate::store::context::RecordsContextSize;
    use crate::store::handover_reminders::{
        HandoverReminder, OpensHandoverReminder, SchedulesHandoverReminders,
    };
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
    use crate::store::{ManagesMetaflowState, SeedsRole};
    use flow_ethos::{Metaflow, State};
    let fixture = NexusFixture::new();
    let address = address();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
            meta_signal_flow::ConfigureThreshold {
                model_name: flow_ethos::ModelName("m".into()),
                refresh_threshold: meta_signal_flow::RefreshThreshold(101),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::HandoverPolicies(
            vec![meta_signal_flow::HandoverPolicy {
                model_name: flow_ethos::ModelName("m".into()),
                warning_share_left: meta_signal_flow::WarningShareLeft(15),
                reminder_text: meta_signal_flow::ReminderText("configured".into()),
                every_minutes: meta_signal_flow::EveryMinutes(5),
            }],
        ))
        .unwrap();
    fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: flow_ethos::FlowId("fixture-seat".into()),
        aspect: Aspect::Field,
        model_name: flow_ethos::ModelName("m".into()),
    });
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake(flow_ethos::FlowId("fixture-seat".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, "fixture-seat")
        .unwrap();
    fixture
        .nexus
        .store
        .record_context_size("fixture-seat", 86)
        .unwrap();
    let reminder = HandoverReminder::open_handover_reminder(
        address.clone(),
        0,
        "m".into(),
        10,
        5,
        "configured".into(),
    )
    .unwrap();
    fixture
        .nexus
        .store
        .hold_handover_reminder(reminder)
        .unwrap();
    fixture.nexus.send_due_handover_reminders(10);
    let held = fixture
        .nexus
        .store
        .handover_reminder(&address, 0)
        .unwrap()
        .unwrap();
    assert_eq!(held.sent, 1);
    assert_eq!(held.next_due, 310);
    let metaflow = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
    assert!(metaflow.queue.0.iter().any(|entry| matches!(
        &entry.message,
        flow_ethos::Message::HardAbrupt(flow_ethos::Request::Notice(_))
    )));
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::HandoverPolicies(
            vec![],
        ))
        .unwrap();
    fixture.nexus.send_due_handover_reminders(310);
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_reminder(&address, 0)
            .unwrap()
            .unwrap()
            .sent,
        1
    );
}

/// A scheduled warning is an ordinary queued HardAbrupt delivery: once the
/// recipient is already past its threshold, admission still drains through
/// the real waking path and reaches its pane.  The threshold is a warning
/// observation, not a delivery hold.
#[test]
fn runtime_tick_drains_due_hard_notice_past_threshold_into_the_pane() {
    use super::waking::{CLAUDE_SEAT, claude_agent, claude_seat};
    use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture, prompts};
    use crate::store::context::{JudgesThreshold, RecordsContextSize};
    use crate::store::handover::HoldsHandovers;
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
    use crate::store::queue_delivery::AccountsQueueDelivery;
    use crate::store::{ManagesMetaflowState, SeedsRole};
    use flow_ethos::{Metaflow, State};
    use signal_flow::CallerIdentity;

    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 200);
    let address = signal_flow::Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Sol,
    };
    fixture.nexus.store.seed_role(CallerIdentity {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        aspect: Aspect::Mind,
        model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
    });
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, CLAUDE_SEAT)
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
            meta_signal_flow::ConfigureThreshold {
                model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
                refresh_threshold: meta_signal_flow::RefreshThreshold(101),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::HandoverPolicies(
            vec![meta_signal_flow::HandoverPolicy {
                model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
                warning_share_left: meta_signal_flow::WarningShareLeft(15),
                reminder_text: meta_signal_flow::ReminderText("configured".into()),
                every_minutes: meta_signal_flow::EveryMinutes(5),
            }],
        ))
        .unwrap();
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 200)
        .unwrap();
    assert!(fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap());

    fixture.nexus.send_due_handover_reminders(10);

    let stored = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
    assert!(
        stored.queue.0.is_empty(),
        "scheduled warning drained, not parked"
    );
    assert_eq!(
        fixture.nexus.store.outstanding_delivery(&address).unwrap(),
        None
    );
    let typed = fixture.typed().expect("warning reached pane");
    assert!(typed.contains("configured"), "{typed}");
    assert_eq!(typed.matches("configured").count(), 1);
    assert_eq!(prompts(&log).len(), 1);
    let reminder = fixture
        .nexus
        .store
        .handover_reminder(&address, 0)
        .unwrap()
        .unwrap();
    assert_eq!(reminder.sent, 1);
}

#[test]
fn reminder_admission_waits_for_handover_gate_before_commit() {
    use super::NexusFixture;
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::SeedsMetaflow;
    use signal_flow::{Message, Request};
    let fixture = NexusFixture::new();
    let address = address();
    fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
        address: address.clone(),
        state: flow_ethos::State::Awake(flow_ethos::FlowId("fixture-seat".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, "fixture-seat")
        .unwrap();
    let reminder = HandoverReminder::open_handover_reminder(
        address.clone(),
        0,
        "m".into(),
        10,
        5,
        "configured".into(),
    )
    .unwrap();
    let gate = fixture.nexus.store.handover_write_gate.lock().unwrap();
    let store = fixture.nexus.store.clone();
    let (started, started_receiver) = std::sync::mpsc::channel();
    let (finished_sender, finished) = std::sync::mpsc::channel();
    let join = std::thread::spawn(move || {
        started.send(()).unwrap();
        let result = store
            .accept_handover_reminder(
                &address,
                Message::HardAbrupt(Request::Notice("notice".into())),
                reminder,
            )
            .unwrap();
        finished_sender.send(result).unwrap();
    });
    started_receiver.recv().unwrap();
    assert!(finished.try_recv().is_err());
    drop(gate);
    assert!(join.join().is_ok());
    assert!(finished.try_recv().is_ok());
}

#[test]
fn reminder_snapshot_closed_before_admission_creates_no_notice() {
    use super::NexusFixture;
    use crate::store::handover::HoldsHandovers;
    use crate::store::logical_delivery::{
        HoldsLogicalDeliveryGateForTest, RecordsLogicalDeliveries,
    };
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
    use signal_flow::{Message, Request};
    let fixture = NexusFixture::new();
    let address = address();
    fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
        address: address.clone(),
        state: flow_ethos::State::Awake(flow_ethos::FlowId("predecessor".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, "predecessor")
        .unwrap();
    let reminder = HandoverReminder::open_handover_reminder(
        address.clone(),
        0,
        "m".into(),
        10,
        5,
        "configured".into(),
    )
    .unwrap();
    let store = fixture.nexus.store.clone();
    let address_for_thread = address.clone();
    let result = fixture.nexus.store.with_logical_delivery_gate(|| {
        let (started, started_receiver) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            started.send(()).unwrap();
            store.accept_handover_reminder(
                &address_for_thread,
                Message::HardAbrupt(Request::Notice("stale".into())),
                reminder,
            )
        });
        started_receiver.recv().unwrap();
        fixture
            .nexus
            .store
            .close_handover_for_successor(&address, "predecessor", "successor")
            .unwrap();
        join
    });
    let admitted = result.join().unwrap().unwrap();
    assert!(matches!(
        admitted,
        signal_flow::PrivateSendResponse::Refused(_)
    ));
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .unwrap()
            .queue
            .0
            .is_empty()
    );
    let memory = fixture
        .nexus
        .store
        .handover_memory(&address)
        .unwrap()
        .unwrap();
    assert_eq!(memory.generation, 1);
    assert_eq!(memory.current_flow_id.as_deref(), Some("successor"));
}

#[test]
fn stop_measurement_opens_first_warning_before_the_next_timer_tick() {
    use super::refresh_and_compaction::{declare_seat, seat_metaflow};
    use super::waking::{
        CLAUDE_SEAT, append_main_rows, claude_agent, claude_seat, claude_stop, end_turn,
    };
    use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
    use crate::store::handover::HoldsHandovers;
    use crate::store::handover_reminders::SchedulesHandoverReminders;
    use crate::store::{ManagesMetaflowState, SeedsRole};
    use meta_signal_flow::{ConfigurationChange, ConfigureThreshold, Continuity, HandoverPolicy};
    use signal_flow::{Aspect, Model};
    for tokens in [85_i64, 86_i64] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![claude_agent(&fixture)],
            PromptFixture::Prompted("w1:p3"),
        );
        claude_seat(&fixture, tokens);
        append_main_rows(&fixture, &[end_turn("last-turn")]);
        let address = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("flow".into()),
            model: Model::Sol,
        };
        fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            aspect: Aspect::Mind,
            model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
        });
        declare_seat(
            &fixture,
            Continuity::OnDemand,
            vec!["spirit-core".into(), "intent-flow".into()],
        );
        seat_metaflow(&fixture, vec![]);
        fixture
            .nexus
            .store
            .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
                model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
                refresh_threshold: meta_signal_flow::RefreshThreshold(101),
            }]))
            .unwrap();
        fixture
            .nexus
            .store
            .ensure_live_handover(&address, CLAUDE_SEAT)
            .unwrap();
        fixture
            .nexus
            .store
            .configure_change(ConfigurationChange::HandoverPolicies(vec![
                HandoverPolicy {
                    model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
                    warning_share_left: meta_signal_flow::WarningShareLeft(15),
                    reminder_text: meta_signal_flow::ReminderText("continue".into()),
                    every_minutes: meta_signal_flow::EveryMinutes(5),
                },
            ]))
            .unwrap();
        fixture.nexus.after_report(
            &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
            &claude_stop(&fixture),
        );
        let reminder = fixture.nexus.store.handover_reminder(&address, 0).unwrap();
        if tokens == 85 {
            assert!(reminder.is_none());
        } else {
            let row = reminder.expect("Stop measurement opens the warning");
            assert_eq!(row.sent, 1);
            let opened_at = row.opened_at;
            fixture.nexus.send_due_handover_reminders(opened_at + 1);
            assert_eq!(
                fixture
                    .nexus
                    .store
                    .handover_reminder(&address, 0)
                    .unwrap()
                    .unwrap()
                    .sent,
                1
            );
        }
    }
}

#[test]
fn runtime_discovery_crosses_boundary_once_and_cancels_green() {
    const CLAUDE_MODEL: &str = "fixture-model";
    const CLAUDE_SEAT: &str = "fixture-seat";
    use super::NexusFixture;
    use crate::store::context::RecordsContextSize;
    use crate::store::handover::HoldsHandovers;
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
    use crate::store::{ManagesMetaflowState, SeedsRole};
    use meta_signal_flow::{ConfigurationChange, ConfigureThreshold, HandoverPolicy};
    use signal_flow::{Address, Aspect, CallerIdentity, Metaflow, Model};
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Sol,
    };
    fixture.nexus.store.seed_role(CallerIdentity {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        aspect: Aspect::Mind,
        model_name: flow_ethos::ModelName(CLAUDE_MODEL.into()),
    });
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: flow_ethos::ModelName(CLAUDE_MODEL.into()),
            refresh_threshold: meta_signal_flow::RefreshThreshold(101),
        }]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::HandoverPolicies(vec![
            HandoverPolicy {
                model_name: flow_ethos::ModelName(CLAUDE_MODEL.into()),
                warning_share_left: meta_signal_flow::WarningShareLeft(15),
                reminder_text: meta_signal_flow::ReminderText("fixture".into()),
                every_minutes: meta_signal_flow::EveryMinutes(5),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, CLAUDE_SEAT)
        .unwrap();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: flow_ethos::State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 85)
        .unwrap();
    fixture.nexus.send_due_handover_reminders(10);
    assert!(
        fixture
            .nexus
            .store
            .handover_reminder(&address, 0)
            .unwrap()
            .is_none()
    );
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 86)
        .unwrap();
    fixture.nexus.send_due_handover_reminders(20);
    let row = fixture
        .nexus
        .store
        .handover_reminder(&address, 0)
        .unwrap()
        .unwrap();
    assert_eq!(row.sent, 1);
    let public = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
    assert!(public.queue.0.iter().any(|entry| matches!(
        &entry.message,
        flow_ethos::Message::HardAbrupt(flow_ethos::Request::Notice(_))
    )));
    fixture.nexus.send_due_handover_reminders(319);
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_reminder(&address, 0)
            .unwrap()
            .unwrap()
            .sent,
        1
    );
    fixture.nexus.send_due_handover_reminders(320);
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_reminder(&address, 0)
            .unwrap()
            .unwrap()
            .sent,
        2
    );
}

#[test]
fn green_keep_cancels_open_warning_timer() {
    const CLAUDE_MODEL: &str = "fixture-model";
    const CLAUDE_SEAT: &str = "fixture-seat";
    use super::NexusFixture;
    use crate::store::context::RecordsContextSize;
    use crate::store::handover::{HandoverWriter, HoldsHandovers};
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::{ManagesMetaflowState, SeedsRole};
    use meta_signal_flow::{ConfigurationChange, ConfigureThreshold, HandoverPolicy};
    use signal_flow::{Address, Aspect, CallerIdentity, Known_Data, Metaflow, Model, Touch};
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Sol,
    };
    fixture.nexus.store.seed_role(CallerIdentity {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        aspect: Aspect::Mind,
        model_name: flow_ethos::ModelName(CLAUDE_MODEL.into()),
    });
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: flow_ethos::ModelName(CLAUDE_MODEL.into()),
            refresh_threshold: meta_signal_flow::RefreshThreshold(101),
        }]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::HandoverPolicies(vec![
            HandoverPolicy {
                model_name: flow_ethos::ModelName(CLAUDE_MODEL.into()),
                warning_share_left: meta_signal_flow::WarningShareLeft(15),
                reminder_text: meta_signal_flow::ReminderText("fixture".into()),
                every_minutes: meta_signal_flow::EveryMinutes(5),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, CLAUDE_SEAT)
        .unwrap();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: flow_ethos::State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 86)
        .unwrap();
    fixture.nexus.send_due_handover_reminders(10);
    assert!(
        fixture
            .nexus
            .store
            .handover_reminder(&address, 0)
            .unwrap()
            .is_some()
    );
    fixture
        .nexus
        .store
        .keep_handover_for_flow(
            &HandoverWriter {
                address: address.clone(),
                flow_id: CLAUDE_SEAT.into(),
            },
            signal_flow::Section {
                title: signal_flow::Title("x".into()),
                description: signal_flow::Description("d".into()),
                body: signal_flow::Body("b".into()),
            },
            Touch::Known(Known_Data {
                flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
                tokens: signal_flow::Tokens(86),
                timestamp: signal_flow::Timestamp(10),
            }),
        )
        .unwrap();
    fixture.nexus.send_due_handover_reminders(11);
    assert!(
        fixture
            .nexus
            .store
            .handover_reminder(&address, 0)
            .unwrap()
            .is_none()
    );
}

#[test]
fn atomic_same_expected_reminder_state_rejects_duplicate_admission() {
    use super::NexusFixture;
    use crate::store::handover::HoldsHandovers;
    use crate::store::handover_reminders::{HandoverReminder, OpensHandoverReminder};
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::SeedsMetaflow;
    use flow_ethos::{Message, Metaflow, Request, State};
    use signal_flow::PrivateSendResponse;
    let fixture = NexusFixture::new();
    let address = address();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake(flow_ethos::FlowId("fixture-seat".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .ensure_live_handover(&address, "fixture-seat")
        .unwrap();
    let reminder =
        HandoverReminder::open_handover_reminder(address.clone(), 0, "m".into(), 0, 5, "x".into())
            .unwrap();
    let mut advanced = reminder.clone();
    advanced.sent = 1;
    advanced.next_due = 300;
    let message = Message::HardAbrupt(Request::Notice("flow-nexus 'Notice'".into()));
    let first = fixture
        .nexus
        .store
        .accept_handover_reminder(&address, message.clone(), advanced.clone())
        .unwrap();
    assert!(matches!(first, PrivateSendResponse::SendAccepted(_)));
    let second = fixture
        .nexus
        .store
        .accept_handover_reminder(&address, message, advanced)
        .unwrap();
    assert!(matches!(second, PrivateSendResponse::Refused(_)));
}

#[test]
fn reminder_state_reopens_with_anchored_due_without_duplicate_initial() {
    use super::configuring::ConfiguredStore;
    use crate::store::OpensFlowStore;
    use crate::store::handover::HoldsHandovers;
    use crate::store::handover_reminders::{
        HandoverReminder, OpensHandoverReminder, SchedulesHandoverReminders,
    };
    let configured = ConfiguredStore::new();
    let path = configured.path();
    let store = crate::store::FlowStore::open_configured(path).unwrap();
    let address = address();
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::SeedsMetaflow;
    store.seed_metaflow(flow_ethos::Metaflow {
        address: address.clone(),
        state: flow_ethos::State::Awake(flow_ethos::FlowId("fixture-seat".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    store
        .ensure_live_handover(&address, "fixture-seat")
        .unwrap();
    let reminder = HandoverReminder::open_handover_reminder(
        address.clone(),
        0,
        "m".into(),
        100,
        5,
        "x".into(),
    )
    .unwrap();
    store.hold_handover_reminder(reminder.clone()).unwrap();
    let mut advanced = reminder.clone();
    advanced.sent = 1;
    advanced.next_due = 400;
    let accepted = store
        .accept_handover_reminder(
            &address,
            flow_ethos::Message::HardAbrupt(flow_ethos::Request::Notice("x".into())),
            advanced.clone(),
        )
        .unwrap();
    assert!(matches!(
        accepted,
        signal_flow::PrivateSendResponse::SendAccepted(_)
    ));
    drop(store);
    let reopened = crate::store::FlowStore::open_configured(path).unwrap();
    assert_eq!(
        reopened.handover_reminder(&address, 0).unwrap(),
        Some(advanced.clone())
    );
    let retry = reopened
        .accept_handover_reminder(
            &address,
            flow_ethos::Message::HardAbrupt(flow_ethos::Request::Notice("x".into())),
            advanced.clone(),
        )
        .unwrap();
    assert!(matches!(
        retry,
        signal_flow::PrivateSendResponse::Refused(_)
    ));
    drop(reopened);
    let reopened = crate::store::FlowStore::open_configured(path).unwrap();
    assert_eq!(
        reopened
            .handover_reminder(&address, 0)
            .unwrap()
            .unwrap()
            .next_due,
        400
    );
}
