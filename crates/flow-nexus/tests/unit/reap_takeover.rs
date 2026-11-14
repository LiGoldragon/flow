//! Flow 0.54.7's acceptance tests for who owes a pending reap
//! (plan-0547-takeover-r3.md and r2): the successor's end and every other
//! commit that changes an Address's receiving flow reassign what it owed,
//! in that commit; a later succession takes the reap over; the one close
//! rule every reap worker follows; the start as a bearing event for what
//! no flow owes; the stale worker held at a deterministic barrier; the
//! completion of a worker joined whatever its pass could read; and the
//! pending-reap family at v2.
//!
//! A is the Address, P the predecessor fac697, S its successor 908786, N a
//! later flow that becomes A's receiving flow. "Herdr calls of the
//! predecessor" are the fake `herdr` log's calls on P's session and the
//! events server's subscriptions.

use super::reaping::{
    LogicalState, NamesHerdrFiles, PREDECESSOR, PREDECESSOR_PANE, PaneShown, ReapWorld, SUCCESSOR,
    StagesReap, delivery_rejected, is_reap_pending, vetted, with_supervisor,
};
use crate::Dispatches;
use crate::generated::memory::{
    PendingHandover, PendingReap, Predecessor, ReapResponsibility, Replacement, Responsible,
};
use crate::observation_lifetime::ObservationCancellationCause;
use crate::reaping::{
    ArmedReap, HoldsAtPoint, PaneWatchEnd, PaneWatchLapse, RunsPaneWatchHooks, TakesUpObligations,
};
use crate::store::metaflow::{ReadsSuccession, SleepsMetaflows, Succession};
use crate::store::pending_reap::{ReapOwner, RecordsPendingReaps};
use crate::store::{
    AltersFlowRowsForTests, AnnouncesLaunchChanges, ConfirmsStartedFlow, LaunchOutcome,
    OpensFlowStore, ReadsFlowRows, RecordsLaunchOutcome, RecordsReplacement, RegistersExistingFlow,
};
use crate::test_faults::{CrashPoint, InjectsCrashes, RefusedWrite};
use crate::waking::SleepsAndWakesSeats;
use signal_flow::{FlowId, FlowLifecycle, Input, Output, ReapStanding};
use std::fs;
use std::time::{Duration, Instant};

/// N, the later flow that becomes A's receiving flow.
const LATER: &str = "a7a7a7";

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(20)
}

/// What these tests read and do at the world's Address.
trait TakesReapsOver {
    fn address(&self) -> signal_flow::Address;
    fn row(&self) -> Option<PendingReap>;
    fn owner(&self) -> Option<ReapResponsibility>;
    /// The Replace's outcome.
    fn outcome(&self) -> Option<LaunchOutcome>;
    fn stop(&self, flow: &str) -> Output;
    /// W1 and W2 of a launch `id` of `flow` at A, replacing `predecessor`
    /// when named: `flow` is A's receiving flow, its settlement not run.
    fn confirmed_at_address(
        &self,
        id: &str,
        flow: &str,
        predecessor: Option<&str>,
    ) -> signal_flow::Launched;
    /// The launch's settlement under the gate, as Start runs it.
    fn settled(&self, id: &str, launched: signal_flow::Launched) -> Output;
    /// The flow's rest, under the gate, as `settle_rested` runs it.
    fn rest(&self, flow: &str);
    /// Every generation of A's handover row closed so far.
    fn generations(&self) -> Vec<(String, String)>;
    /// The Herdr calls made on the predecessor's session.
    fn predecessor_calls(&self) -> usize;
    /// The obligation commits attempted from `from` on.
    fn attempts_since(&self, from: usize) -> Vec<String>;
    fn attempt_count(&self) -> usize;
    /// What each spawned worker was armed for.
    fn armed(&self) -> Vec<ArmedReap>;
    /// The successor's pane taken out of every later snapshot, or put back.
    fn successor_gone(&self, gone: bool);
    /// Two supervisor passes, each begun after this call: the second has
    /// read the store as it stands now and drained what completed before.
    fn two_passes(&self);
    /// The predecessor's row and S's own: their flow-id claims, so a
    /// delivery reads S's pane.
    fn claim_successor(&self);
}

impl TakesReapsOver for ReapWorld {
    fn address(&self) -> signal_flow::Address {
        self.launch.profile.address.clone()
    }

    fn row(&self) -> Option<PendingReap> {
        self.fixture.nexus.store.pending_reap(PREDECESSOR).unwrap()
    }

    fn owner(&self) -> Option<ReapResponsibility> {
        self.row().map(|row| row.reap_responsibility)
    }

    fn outcome(&self) -> Option<LaunchOutcome> {
        self.fixture
            .nexus
            .store
            .launch_outcome(&self.launch.profile.launch_request_id.0)
            .unwrap()
    }

    fn stop(&self, flow: &str) -> Output {
        self.fixture
            .nexus
            .dispatch(Input::Stop(FlowId(flow.into())))
    }

    fn confirmed_at_address(
        &self,
        id: &str,
        flow: &str,
        predecessor: Option<&str>,
    ) -> signal_flow::Launched {
        let mut launch = self.fixture.staged_launch(id, predecessor);
        launch.profile.address = self.address();
        launch.composed.launch_profile.address = self.address();
        launch.binding.flow_id = FlowId(flow.into());
        launch.binding.native_session_id = signal_flow::NativeSessionId(format!("session-{flow}"));
        launch.pane.herdr_pane_id = signal_flow::HerdrPaneId(format!("pane-{flow}"));
        launch.binding.herdr_pane_binding = launch.pane.clone();
        launch.reserve(&self.fixture.nexus);
        launch.record_native_intent(&self.fixture.nexus);
        launch.bind(&self.fixture.nexus);
        launch.register_and_acknowledge(&self.fixture.nexus);
        if let Some(predecessor) = predecessor {
            self.fixture
                .nexus
                .store
                .record_replacement(Replacement {
                    launch_request_id: launch.profile.launch_request_id.clone(),
                    predecessor: Predecessor(predecessor.into()),
                })
                .unwrap();
        }
        match self.fixture.nexus.store.confirm_started(flow).unwrap() {
            Output::Started(launched) => launched,
            other => panic!("{flow} confirmed as {other:?}"),
        }
    }

    fn settled(&self, id: &str, launched: signal_flow::Launched) -> Output {
        use crate::launching::LaunchesFlows;
        let _turn = self.fixture.nexus.dispatch_gate.lock().unwrap();
        self.fixture.nexus.settle(id, Output::Started(launched))
    }

    fn rest(&self, flow: &str) {
        let _turn = self.fixture.nexus.dispatch_gate.lock().unwrap();
        self.fixture.nexus.take_up_at_rest(flow);
    }

    fn generations(&self) -> Vec<(String, String)> {
        use crate::store::handover::HoldsHandovers;
        self.fixture
            .nexus
            .store
            .handover_memory(&self.address())
            .unwrap()
            .map(|row| {
                row.closed
                    .into_iter()
                    .map(|closed| (closed.predecessor, closed.successor))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn predecessor_calls(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| {
                call.contains("--session messaging-build") || *call == "events.subscribe"
            })
            .count()
    }

    fn attempts_since(&self, from: usize) -> Vec<String> {
        self.fixture.nexus.store.faults.attempts()[from..].to_vec()
    }

    fn attempt_count(&self) -> usize {
        self.fixture.nexus.store.faults.attempts().len()
    }

    fn armed(&self) -> Vec<ArmedReap> {
        self.fixture
            .nexus
            .pane_watch_hooks
            .armed
            .lock()
            .unwrap()
            .iter()
            .map(|(_, armed)| armed.clone())
            .collect()
    }

    fn successor_gone(&self, gone: bool) {
        let flag = self.fixture.file("successor-gone");
        if gone {
            fs::write(flag, "").unwrap();
        } else {
            let _ = fs::remove_file(flag);
        }
    }

    fn two_passes(&self) {
        let hooks = &self.fixture.nexus.pane_watch_hooks;
        for _ in 0..2 {
            let (passed, pass) = std::sync::mpsc::channel();
            *hooks.between.lock().unwrap() = Some(Box::new(move |_| {
                let _ = passed.send(());
            }));
            self.fixture.nexus.store.launch_changes.announce();
            pass.recv_timeout(Duration::from_secs(20))
                .expect("a supervisor pass");
        }
    }

    fn claim_successor(&self) {
        fs::write(
            self.fixture
                .directory
                .path()
                .join(format!("flows/.{SUCCESSOR}.flow-id")),
            format!(
                "version=1\nharness=codex\nidentity=02b0c33df35f8563aa5175501989791a\nalias={SUCCESSOR}\n"
            ),
        )
        .expect("successor flow claim");
    }
}

/// The Replace's outcome reads `Replaced(P, S)`.
fn replaced_p_by_s(outcome: Option<LaunchOutcome>) -> bool {
    matches!(outcome, Some(LaunchOutcome::Replaced(replaced))
        if replaced.flow_id.0 == PREDECESSOR && replaced.launched_record.flow_id.0 == SUCCESSOR)
}

/// The Replace's outcome still reads `ReapPending(P, S)`.
fn pending_p_by_s(outcome: Option<LaunchOutcome>) -> bool {
    matches!(outcome, Some(LaunchOutcome::ReapPending(replaced))
        if replaced.flow_id.0 == PREDECESSOR && replaced.launched_record.flow_id.0 == SUCCESSOR)
}

fn taken_over_by(flow: &str) -> ReapResponsibility {
    ReapResponsibility::TakenOver(Responsible(FlowId(flow.into())))
}

/// Sol's sequence to its third step: the Replace answers Owed, the worker
/// closes once, Herdr shows the pane gone, and the worker's settlement is
/// refused: the row stands at SettleUnrecorded, owed by S, which has not
/// rested. Runs inside a supervisor.
fn settle_unrecorded(world: &ReapWorld) {
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapSettle);
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    world.until("the close", &mut || world.closes() == 1);
    world.events.emit_gone(PREDECESSOR_PANE);
    world.until("the refused settlement", &mut || {
        world.standing() == Some(ReapStanding::SettleUnrecorded)
    });
    assert_eq!(world.owner(), Some(ReapResponsibility::LaunchSuccessor));
    assert!(
        world
            .fixture
            .nexus
            .pane_watch_hooks
            .workers_joined_by(1, deadline()),
        "the first worker acted and was joined"
    );
}

/// The pane-owed row at its successor's Stop: the close refused, the row
/// at CloseRefused, its first worker waiting; then S stopped, its pane
/// closed and recorded. `shown` is what the predecessor's pane shows from
/// the Stop on. Runs inside a supervisor; the second worker is armed under
/// AddressHeld and has taken its opening snapshot.
fn held_by_the_address(world: &ReapWorld, shown: PaneShown) {
    world.close_exits(1);
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    world.until("the refused close", &mut || {
        world.standing() == Some(ReapStanding::CloseRefused)
    });
    world.two_passes();
    // S's own close at its Stop succeeds.
    world.close_exits(0);
    world.show(shown);
    let snapshots = world.snapshots_of_predecessor();
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::Stopped(FlowId(SUCCESSOR.into()))
    );
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    assert_eq!(world.standing(), Some(ReapStanding::CloseRefused));
    assert!(pending_p_by_s(world.outcome()));
    world.until("the worker armed under the Address", &mut || {
        world
            .armed()
            .iter()
            .any(|armed| armed.reap_responsibility == ReapResponsibility::AddressHeld)
            && world.snapshots_of_predecessor() > snapshots
    });
}

/// Test 1 (Sol's exact sequence): a SettleUnrecorded reap is settled by its
/// successor's Stop, in the Stop's own commit, before any rest and with no
/// later launch: no generation is closed, no Herdr call is made, no worker
/// is armed.
#[test]
fn a_settle_unrecorded_reap_is_settled_by_its_successors_stop() {
    use crate::store::handover::HoldsHandovers;
    let mut world = ReapWorld::staged("stop-settles");
    let ends = with_supervisor(&world.fixture, || {
        settle_unrecorded(&world);
        let handover = world
            .fixture
            .nexus
            .store
            .handover_memory(&world.address())
            .unwrap();
        let calls = world.predecessor_calls();
        let subscriptions = world.events.subscriptions();
        let attempts = world.attempt_count();
        assert_eq!(
            world.stop(SUCCESSOR),
            Output::Stopped(FlowId(SUCCESSOR.into()))
        );
        // The one commit: S Stopped, A Asleep, the row finished.
        assert_eq!(
            world.attempts_since(attempts),
            vec!["ReapsReassigned".to_owned(), "FlowEnded".to_owned()]
        );
        assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Stopped);
        assert_eq!(
            world
                .fixture
                .nexus
                .store
                .metaflow(&world.address())
                .unwrap()
                .unwrap()
                .state,
            flow_ethos::State::Asleep
        );
        assert!(world.row().is_none());
        assert!(replaced_p_by_s(world.outcome()));
        assert_eq!(
            world
                .fixture
                .nexus
                .store
                .handover_memory(&world.address())
                .unwrap(),
            handover,
            "no generation is closed: the handover row is as it was"
        );
        world.two_passes();
        assert_eq!(world.predecessor_calls(), calls, "no Herdr call");
        assert_eq!(world.events.subscriptions(), subscriptions);
    });
    assert_eq!(ends, vec![(PREDECESSOR.to_owned(), PaneWatchEnd::Acted)]);
    let first = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.fixture.restarted(), first, "the start writes nothing");
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::StopRejected(signal_flow::StopRejection::AlreadyStopped)
    );
    assert!(!vetted(&world.vet(PREDECESSOR)));
    assert!(!vetted(&world.vet(SUCCESSOR)));
}

/// Test 2: a later fresh launch at A after the Stop that settled the reap
/// has nothing to take over; and r1's path, S Replaced by N, finishes the
/// row in N's W3 through `superseding`, with no Herdr call and no
/// generation from P to S.
#[test]
fn a_launch_after_the_stop_that_settled_a_reap_has_nothing_to_take_over() {
    let world = ReapWorld::staged("stop-then-launch");
    with_supervisor(&world.fixture, || {
        settle_unrecorded(&world);
        assert_eq!(
            world.stop(SUCCESSOR),
            Output::Stopped(FlowId(SUCCESSOR.into()))
        );
    });
    let attempts = world.attempt_count();
    let launched = world.confirmed_at_address("after-stop", LATER, None);
    let answer = world.settled("after-stop", launched);
    assert!(
        matches!(answer, Output::Started(_) | Output::HandoverPending(_)),
        "{answer:?}"
    );
    let since = world.attempts_since(attempts);
    assert!(
        !since
            .iter()
            .any(|attempt| attempt == "Superseding" || attempt == "ReapsReassigned"),
        "nothing to take over: {since:?}"
    );
    // The one generation N's settlement closes is from the handover's real
    // owner, S, which had received at A, to N; none is the reap's, P to S.
    assert_eq!(
        world.generations(),
        vec![(SUCCESSOR.to_owned(), LATER.to_owned())]
    );
    assert_eq!(
        world
            .fixture
            .nexus
            .store
            .receiving_flow(&world.address())
            .unwrap(),
        Some(FlowId(LATER.into()))
    );
    assert_eq!(
        world.fixture.nexus.store.succession(LATER).unwrap(),
        Succession::Receiving
    );

    // r1's path: S is Replaced by N; N's W3 takes the row over.
    let world = ReapWorld::staged("replaced-again");
    with_supervisor(&world.fixture, || settle_unrecorded(&world));
    let calls = world.predecessor_calls();
    let launched = world.confirmed_at_address("replaces-s", LATER, Some(SUCCESSOR));
    let answer = world.settled("replaces-s", launched);
    assert!(
        matches!(&answer, Output::ReapPending(pending)
            if pending.predecessor.0.0 == SUCCESSOR && pending.reap_standing == ReapStanding::Owed),
        "{answer:?}"
    );
    assert!(world.row().is_none(), "P's row is finished in N's W3");
    assert!(replaced_p_by_s(world.outcome()));
    assert_eq!(world.predecessor_calls(), calls, "no Herdr call");
    assert!(
        !world
            .generations()
            .contains(&(PREDECESSOR.to_owned(), SUCCESSOR.to_owned())),
        "no generation from P to S"
    );
    let owed = world
        .fixture
        .nexus
        .store
        .pending_reap(SUCCESSOR)
        .unwrap()
        .expect("S's own reap is owed");
    assert_eq!(
        owed.reap_responsibility,
        ReapResponsibility::LaunchSuccessor
    );
    assert_eq!(
        world.fixture.nexus.store.reap_owner(&owed).unwrap(),
        Some(ReapOwner::Flow(FlowId(LATER.into())))
    );
}

/// Test 3: a pane-owed reap at its successor's Stop is held by the Address;
/// its first worker is cancelled as reassigned, one worker is armed under
/// the Address, the pane shown Unproven is not closed, and Herdr's Gone
/// settles the row with nothing closing.
#[test]
fn a_pane_owed_reap_at_its_successors_stop_is_held_by_the_address_and_settled_by_the_pane_gone_event()
 {
    let mut world = ReapWorld::staged("address-held");
    let ends = with_supervisor(&world.fixture, || {
        held_by_the_address(&world, PaneShown::SessionUnreported);
        assert_eq!(world.closes(), 1, "only the first worker's refused close");
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.row().is_none());
    });
    assert_eq!(world.closes(), 1, "exactly one close across both workers");
    assert!(replaced_p_by_s(world.outcome()));
    assert!(
        world
            .generations()
            .iter()
            .all(|(from, to)| from != SUCCESSOR && to != SUCCESSOR)
    );
    assert!(matches!(
        ends.first(),
        Some((
            _,
            PaneWatchEnd::Cancelled(ObservationCancellationCause::ObligationReassigned)
        )) | Some((_, PaneWatchEnd::Stale))
    ));
    assert_eq!(
        ends.last(),
        Some(&(PREDECESSOR.to_owned(), PaneWatchEnd::Acted))
    );
    assert_eq!(ends.len(), 2);
    let first = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.fixture.restarted(), first, "the start writes nothing");
}

/// Test 4: a crash after the successor-end commit is recovered from the
/// rows once. (a) The finished row: nothing to do. (b) The Address-held
/// pane-owed row: the first pass arms one worker, its opening snapshot
/// Absent settles it with no close. (c) Ruling cszm, Option A: the start
/// settles an Address-held store-only row once and reads an unknown
/// predecessor once, with no Herdr call and no retry.
#[test]
fn a_crash_after_the_successor_end_commit_is_recovered_from_the_rows_once() {
    // (a)
    let mut world = ReapWorld::staged("crash-end-settled");
    with_supervisor(&world.fixture, || settle_unrecorded(&world));
    world
        .fixture
        .nexus
        .store
        .faults
        .crash_at(CrashPoint::AfterFlowEnded);
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world.stop(SUCCESSOR)));
    assert!(crashed.is_err(), "the Stop ends at the crash");
    let calls = world.predecessor_calls();
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert!(world.row().is_none());
    assert!(replaced_p_by_s(world.outcome()));
    assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Stopped);
    assert!(
        !world
            .fixture
            .nexus
            .store
            .faults
            .attempts()
            .iter()
            .any(|attempt| attempt.starts_with("Reap") || attempt == "Superseding"),
        "the start commits nothing for the reap"
    );
    with_supervisor(&world.fixture, || world.two_passes());
    assert!(
        world
            .fixture
            .nexus
            .pane_watch_hooks
            .spawned
            .lock()
            .unwrap()
            .is_empty(),
        "no worker"
    );
    assert_eq!(world.predecessor_calls(), calls);

    // (b)
    let mut world = ReapWorld::staged("crash-end-held");
    with_supervisor(&world.fixture, || {
        world.close_exits(1);
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the refused close", &mut || {
            world.standing() == Some(ReapStanding::CloseRefused)
        });
    });
    world.close_exits(0);
    world
        .fixture
        .nexus
        .store
        .faults
        .crash_at(CrashPoint::AfterFlowEnded);
    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world.stop(SUCCESSOR)));
    assert!(crashed.is_err(), "the Stop ends at the crash");
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    assert!(
        !world
            .fixture
            .nexus
            .store
            .faults
            .attempts()
            .iter()
            .any(|attempt| attempt.starts_with("Reap")),
        "recover_obligations commits nothing for a pane-owed row"
    );
    world.show(PaneShown::Absent);
    let closes = world.closes();
    with_supervisor(&world.fixture, || {
        world.until("the reap settled", &mut || world.row().is_none());
    });
    assert_eq!(world.armed().len(), 1, "one worker");
    assert_eq!(
        world.armed()[0].reap_responsibility,
        ReapResponsibility::AddressHeld
    );
    assert_eq!(world.closes(), closes, "no close");
    let settled = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    with_supervisor(&world.fixture, || world.two_passes());
    assert!(world.armed().is_empty(), "a second start arms nothing");
    assert_eq!(world.fixture.restarted(), settled, "and writes nothing");

    // (c) A store-only row the Address holds: its worker's settlement was
    // refused.
    let mut world = ReapWorld::staged("start-settles-held");
    with_supervisor(&world.fixture, || {
        held_by_the_address(&world, PaneShown::SessionUnreported);
        world
            .fixture
            .nexus
            .store
            .faults
            .refuse_next(RefusedWrite::ReapSettle);
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the refused settlement", &mut || {
            world.standing() == Some(ReapStanding::SettleUnrecorded)
        });
    });
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    let calls = world.calls().len();
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(
        world
            .fixture
            .nexus
            .store
            .faults
            .attempts()
            .iter()
            .filter(|attempt| *attempt == "Reaped")
            .count(),
        1,
        "exactly one settlement attempt"
    );
    assert!(world.row().is_none());
    assert!(replaced_p_by_s(world.outcome()));
    assert_eq!(world.calls().len(), calls, "no Herdr call at the start");
    let settled = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(
        world.fixture.restarted(),
        settled,
        "a second start writes nothing"
    );

    // (c) An unknown predecessor the Address holds: read once per start; a
    // refused read writes nothing and is not retried.
    let mut world = ReapWorld::staged("start-reads-held");
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    assert!(is_reap_pending(
        &world.replace(),
        ReapStanding::PredecessorUnknown
    ));
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::Stopped(FlowId(SUCCESSOR.into()))
    );
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    assert_eq!(world.standing(), Some(ReapStanding::PredecessorUnknown));
    let calls = world.calls().len();
    let unread = world.fixture.restarted();
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.standing(), Some(ReapStanding::PredecessorUnknown));
    assert_eq!(
        world.fixture.restarted(),
        unread,
        "a refused read writes nothing"
    );
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.standing(), Some(ReapStanding::Owed));
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    assert_eq!(world.calls().len(), calls, "no Herdr call at either start");
    let read = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.fixture.restarted(), read, "read once, then nothing");
}

/// Test 5: a refused successor-end commit leaves the prior state whole, for
/// the next end to re-run: (a) the Stop retried, its pane positively
/// observed Absent, records Stopped with no close (ruling cszm, Option A),
/// while an Unavailable route or a snapshot with no pane list does not;
/// (b) an Exited recorded by a delivery; (c) the start writes nothing for
/// the row, S being its live owner.
#[test]
fn a_refused_successor_end_commit_leaves_the_prior_state_for_the_next_end() {
    for next in ["stop", "delivery", "start"] {
        let mut world = ReapWorld::staged(&format!("refused-end-{next}"));
        with_supervisor(&world.fixture, || settle_unrecorded(&world));
        world
            .fixture
            .nexus
            .store
            .faults
            .refuse_next(RefusedWrite::FlowEnded);
        let changes = world.fixture.nexus.store.launch_changes.current();
        let attempts = world.attempt_count();
        assert_eq!(
            world.stop(SUCCESSOR),
            Output::StopRejected(signal_flow::StopRejection::PersistenceRefused),
            "{next}"
        );
        assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Active, "{next}");
        assert_eq!(
            world
                .fixture
                .nexus
                .store
                .receiving_flow(&world.address())
                .unwrap(),
            Some(FlowId(SUCCESSOR.into()))
        );
        assert_eq!(world.standing(), Some(ReapStanding::SettleUnrecorded));
        assert_eq!(world.owner(), Some(ReapResponsibility::LaunchSuccessor));
        assert!(pending_p_by_s(world.outcome()));
        assert_eq!(
            world
                .attempts_since(attempts)
                .iter()
                .filter(|attempt| *attempt == "FlowEnded")
                .count(),
            1
        );
        assert_eq!(
            world.fixture.nexus.store.launch_changes.current(),
            changes,
            "no announcement of a refused end"
        );
        // S's pane went with the refused Stop's close.
        world.successor_gone(true);
        world.show(PaneShown::Absent);
        let calls = world.calls();
        let closes_of_s = |calls: &[String]| {
            calls
                .iter()
                .filter(|call| call.contains("pane close w1:p1"))
                .count()
        };
        match next {
            "stop" => {
                // No route recorded, or no pane list: no record.
                world.show(PaneShown::NoPaneList);
                assert_eq!(
                    world.stop(SUCCESSOR),
                    Output::StopRejected(signal_flow::StopRejection::RouteUnavailable)
                );
                assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Active);
                world.show(PaneShown::Absent);
                assert_eq!(
                    world.stop(SUCCESSOR),
                    Output::Stopped(FlowId(SUCCESSOR.into()))
                );
                assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Stopped);
                assert_eq!(
                    closes_of_s(&world.calls()),
                    closes_of_s(&calls),
                    "Stopped with no close"
                );
            }
            "delivery" => {
                world.claim_successor();
                let delivered =
                    world
                        .fixture
                        .nexus
                        .dispatch_meta(meta_signal_flow::Input::Deliver(
                            meta_signal_flow::DeliveryRequest {
                                delivery_id: meta_signal_flow::DeliveryId(
                                    "to-the-gone-pane".into(),
                                ),
                                flow_id: FlowId(SUCCESSOR.into()),
                                message: super::middle_abrupt("are you there"),
                            },
                        ));
                assert!(
                    matches!(delivered, meta_signal_flow::Output::DeliveryRejected(_)),
                    "{delivered:?}"
                );
                assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Exited);
            }
            _ => {
                let before = world.fixture.restarted();
                world.fixture.nexus.recover_wakes();
                assert_eq!(
                    world.fixture.restarted(),
                    before,
                    "the start writes nothing for a row its live owner owes"
                );
                assert_eq!(world.owner(), Some(ReapResponsibility::LaunchSuccessor));
                assert_eq!(
                    world.stop(SUCCESSOR),
                    Output::Stopped(FlowId(SUCCESSOR.into()))
                );
            }
        }
        assert!(
            world.row().is_none(),
            "{next}: the next end settles the row"
        );
        assert!(replaced_p_by_s(world.outcome()), "{next}");
        assert_eq!(
            world
                .calls()
                .iter()
                .filter(|call| call.contains("--session messaging-build"))
                .count(),
            calls
                .iter()
                .filter(|call| call.contains("--session messaging-build"))
                .count(),
            "{next}: no Herdr call on the predecessor"
        );
    }

    // A retried Stop whose route is not recorded records nothing.
    let world = ReapWorld::staged("refused-end-unrouted");
    with_supervisor(&world.fixture, || settle_unrecorded(&world));
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowEnded);
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::StopRejected(signal_flow::StopRejection::PersistenceRefused)
    );
    world.fixture.nexus.store.forget_route(SUCCESSOR);
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::StopRejected(signal_flow::StopRejection::RouteUnavailable)
    );
    assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Active);
    assert_eq!(world.standing(), Some(ReapStanding::SettleUnrecorded));
}

/// Every end of the successor reassigns what it owed, in its own commit:
/// Stop, Retire, and Exited as a delivery, a wake and a Refresh find its
/// pane gone.
#[test]
fn every_end_of_the_successor_reassigns_what_it_owed() {
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    for end in ["stop", "retire", "delivery", "wake", "refresh"] {
        let world = ReapWorld::staged(&format!("ended-by-{end}"));
        with_supervisor(&world.fixture, || settle_unrecorded(&world));
        let ended = match end {
            "stop" => {
                assert_eq!(
                    world.stop(SUCCESSOR),
                    Output::Stopped(FlowId(SUCCESSOR.into()))
                );
                FlowLifecycle::Stopped
            }
            "retire" => {
                let retired = world
                    .fixture
                    .nexus
                    .dispatch_meta(meta_signal_flow::Input::Retire(FlowId(SUCCESSOR.into())));
                assert!(
                    matches!(retired, meta_signal_flow::Output::FlowRetired(_)),
                    "{retired:?}"
                );
                FlowLifecycle::Retired
            }
            "delivery" => {
                world.successor_gone(true);
                world.show(PaneShown::Absent);
                world.claim_successor();
                let _ = world
                    .fixture
                    .nexus
                    .dispatch_meta(meta_signal_flow::Input::Deliver(
                        meta_signal_flow::DeliveryRequest {
                            delivery_id: meta_signal_flow::DeliveryId("to-the-gone-pane".into()),
                            flow_id: FlowId(SUCCESSOR.into()),
                            message: super::middle_abrupt("are you there"),
                        },
                    ));
                FlowLifecycle::Exited
            }
            "wake" => {
                world.successor_gone(true);
                world.show(PaneShown::Absent);
                world
                    .fixture
                    .nexus
                    .store
                    .accept_logical_delivery(
                        &world.address(),
                        signal_flow::SentBy::Flow,
                        flow_ethos::Delivery::Queue,
                        signal_flow::Message::Soft(signal_flow::Request::Notice(
                            "queued for S".into(),
                        )),
                        None,
                    )
                    .unwrap();
                world.fixture.nexus.drain_awake(&world.address());
                FlowLifecycle::Exited
            }
            _ => {
                world.successor_gone(true);
                world.show(PaneShown::Absent);
                let refreshed = world
                    .fixture
                    .nexus
                    .dispatch(Input::Refresh(world.address()));
                assert_eq!(
                    refreshed,
                    Output::RefreshRejected(signal_flow::RefreshRejection::NotAwake)
                );
                FlowLifecycle::Exited
            }
        };
        assert_eq!(world.lifecycle(SUCCESSOR), ended, "{end}");
        assert!(world.row().is_none(), "{end}: the row is finished");
        assert!(replaced_p_by_s(world.outcome()), "{end}");
    }
}

/// Test 6: a refused takeover commit leaves the Address-held reap watched
/// by its worker under its identity; the new flow N receives meanwhile;
/// N's rest makes one settlement attempt, which takes the reap over, and
/// one worker is armed under N.
#[test]
fn a_refused_takeover_commit_leaves_the_address_held_reap_watched_and_the_new_flows_rest_takes_it_over()
 {
    for variant in ["rest", "refused-again", "start"] {
        let mut world = ReapWorld::staged(&format!("refused-takeover-{variant}"));
        with_supervisor(&world.fixture, || {
            held_by_the_address(&world, PaneShown::SessionUnreported);
            let faults = &world.fixture.nexus.store.faults;
            faults.refuse_next(RefusedWrite::StartedSettle);
            faults.refuse_next(RefusedWrite::HandoverOwing);
            let launched = world.confirmed_at_address("takes-over", LATER, None);
            let answer = world.settled("takes-over", launched);
            assert!(matches!(answer, Output::HandoverPending(_)), "{answer:?}");
            assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
            assert_eq!(world.standing(), Some(ReapStanding::CloseRefused));
            assert_eq!(
                world
                    .fixture
                    .nexus
                    .store
                    .launch_outcome("takes-over")
                    .unwrap(),
                None
            );
            assert!(vetted(&world.vet(LATER)));
            assert_eq!(
                world.fixture.nexus.store.succession(LATER).unwrap(),
                Succession::Receiving
            );
            let attempts = world.attempt_count();
            for _ in 0..5 {
                world.fixture.nexus.store.launch_changes.announce();
            }
            world.two_passes();
            assert_eq!(
                world.attempt_count(),
                attempts,
                "announcements attempt nothing"
            );
            assert_eq!(
                world.armed().len(),
                2,
                "the Address's worker keeps watching"
            );
            match variant {
                "rest" | "refused-again" => {
                    if variant == "refused-again" {
                        faults.refuse_next(RefusedWrite::StartedSettle);
                        faults.refuse_next(RefusedWrite::HandoverOwing);
                        world.rest(LATER);
                        assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
                    }
                    let attempts = world.attempt_count();
                    world.rest(LATER);
                    assert_eq!(
                        world
                            .attempts_since(attempts)
                            .iter()
                            .filter(|attempt| *attempt == "StartedHandedOver")
                            .count(),
                        1,
                        "one settlement attempt"
                    );
                    assert_eq!(world.owner(), Some(taken_over_by(LATER)));
                    world.until("one worker armed under N", &mut || world.armed().len() == 3);
                    assert_eq!(world.armed()[2].reap_responsibility, taken_over_by(LATER));
                    let attempts = world.attempt_count();
                    world.rest(LATER);
                    assert!(
                        !world
                            .attempts_since(attempts)
                            .iter()
                            .any(|attempt| attempt == "StartedHandedOver"
                                || attempt == "HandoverOwed"),
                        "a second rest attempts no settlement"
                    );
                    // It bears on the pane step N now owes: its worker is
                    // armed again under the same identity.
                    world.until("the rearm", &mut || world.armed().len() == 4);
                    assert_eq!(world.armed()[3].reap_responsibility, taken_over_by(LATER));
                }
                _ => {}
            }
        });
        if variant == "refused-again" {
            assert_eq!(
                world
                    .fixture
                    .nexus
                    .store
                    .faults
                    .attempts()
                    .iter()
                    .filter(|attempt| *attempt == "StartedHandedOver")
                    .count(),
                3,
                "the refused launch, then two rests, one attempt each"
            );
        }
        if variant == "start" {
            world.fixture.restarted();
            world.fixture.nexus.recover_wakes();
            assert_eq!(world.owner(), Some(taken_over_by(LATER)));
            assert!(matches!(
                world
                    .fixture
                    .nexus
                    .store
                    .launch_outcome("takes-over")
                    .unwrap(),
                Some(LaunchOutcome::Started(_) | LaunchOutcome::HandoverPending(_))
            ));
            let settled = world.fixture.restarted();
            world.fixture.nexus.recover_wakes();
            assert_eq!(world.fixture.restarted(), settled, "landed once");
        }
    }
}

/// A world whose row was taken over by N: test 6 to its rest.
fn taken_over(world: &ReapWorld) {
    with_supervisor(&world.fixture, || {
        held_by_the_address(world, PaneShown::SessionUnreported);
        let faults = &world.fixture.nexus.store.faults;
        faults.refuse_next(RefusedWrite::StartedSettle);
        faults.refuse_next(RefusedWrite::HandoverOwing);
        let launched = world.confirmed_at_address("takes-over", LATER, None);
        let _ = world.settled("takes-over", launched);
        world.rest(LATER);
        assert_eq!(world.owner(), Some(taken_over_by(LATER)));
    });
}

/// Test 7: a taken-over reap survives a restart under its owner N: the
/// first pass arms one worker under N, an Unproven pane is not closed, a
/// rearm finds it Owned and closes it once, Herdr's Gone settles it with no
/// generation for S. A store-only row N owes is finished at N's rest, or at
/// N's Stop, with no Herdr call.
#[test]
fn a_taken_over_reap_survives_a_restart_under_its_owner() {
    let mut world = ReapWorld::staged("taken-over-restart");
    taken_over(&world);
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert!(
        !world
            .fixture
            .nexus
            .store
            .faults
            .attempts()
            .iter()
            .any(|attempt| attempt.starts_with("Reap") || attempt == "StartedHandedOver"),
        "recover_obligations commits nothing"
    );
    let closes = world.closes();
    with_supervisor(&world.fixture, || {
        world.until("the worker under N snapshots", &mut || {
            world.armed().len() == 1 && world.snapshots_of_predecessor() >= 1
        });
        assert_eq!(world.armed()[0].reap_responsibility, taken_over_by(LATER));
        world.two_passes();
        assert_eq!(world.closes(), closes, "an Unproven pane is not closed");
        world.show(PaneShown::Present);
        world.rest(LATER);
        world.until("one close of the Owned pane", &mut || {
            world.closes() == closes + 1
        });
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.row().is_none());
    });
    assert_eq!(world.closes(), closes + 1);
    assert!(replaced_p_by_s(world.outcome()));
    assert!(
        !world
            .generations()
            .contains(&(PREDECESSOR.to_owned(), SUCCESSOR.to_owned())),
        "no generation for S from the reap: {:?}",
        world.generations()
    );
    let settled = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.fixture.restarted(), settled);

    for bearer in ["rest", "stop"] {
        let mut world = ReapWorld::staged(&format!("taken-over-settle-{bearer}"));
        taken_over(&world);
        world.fixture.restarted();
        world.show(PaneShown::Absent);
        world
            .fixture
            .nexus
            .store
            .faults
            .refuse_next(RefusedWrite::ReapSettle);
        with_supervisor(&world.fixture, || {
            world.until("the refused settlement", &mut || {
                world.standing() == Some(ReapStanding::SettleUnrecorded)
            });
        });
        assert_eq!(world.owner(), Some(taken_over_by(LATER)));
        let held = world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert_eq!(
            world.fixture.restarted(),
            held,
            "the start leaves N's row to N"
        );
        let calls = world.predecessor_calls();
        match bearer {
            "rest" => world.rest(LATER),
            _ => assert_eq!(
                world.stop(LATER),
                Output::Stopped(FlowId(LATER.into())),
                "N's pane positively gone: Stopped with no close"
            ),
        }
        assert!(world.row().is_none(), "{bearer}");
        assert!(replaced_p_by_s(world.outcome()), "{bearer}");
        assert_eq!(world.predecessor_calls(), calls, "{bearer}: no Herdr call");
    }
}

/// Which commit reassigns the row a held worker was armed for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reassigning {
    /// S's Stop: the row becomes AddressHeld.
    SuccessorStop,
    /// N's rest, after its refused settlement: the row becomes
    /// TakenOver(N).
    LaterRest,
}

/// Test 8: a stale worker held at a deterministic barrier, its cue consumed
/// and before it takes the gate, writes nothing, for both reassigning
/// commits: (a) the identity check wins, the pass held, and it ends Stale;
/// (b) the cancellation wins, and it ends Cancelled(ObligationReassigned).
/// Its writes are read after every worker is joined. Delivery to the
/// receiving flow is held by none of it.
#[test]
fn a_stale_worker_held_at_revalidation_writes_nothing() {
    for reassigning in [Reassigning::SuccessorStop, Reassigning::LaterRest] {
        for identity_first in [true, false] {
            let case = format!("{reassigning:?}, identity first: {identity_first}");
            let world = ReapWorld::staged(&format!(
                "stale-{}-{identity_first}",
                if reassigning == Reassigning::SuccessorStop {
                    "stop"
                } else {
                    "rest"
                }
            ));
            let hooks = &world.fixture.nexus.pane_watch_hooks;
            let mut attempts = 0;
            let mut closes = 0;
            let ends = with_supervisor(&world.fixture, || {
                // The worker to be held waits on its subscription.
                match reassigning {
                    Reassigning::SuccessorStop => {
                        world.close_exits(1);
                        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
                        world.until("the refused close", &mut || {
                            world.standing() == Some(ReapStanding::CloseRefused)
                        });
                        world.close_exits(0);
                    }
                    Reassigning::LaterRest => {
                        held_by_the_address(&world, PaneShown::SessionUnreported);
                        let faults = &world.fixture.nexus.store.faults;
                        faults.refuse_next(RefusedWrite::StartedSettle);
                        faults.refuse_next(RefusedWrite::HandoverOwing);
                        let launched = world.confirmed_at_address("takes-over", LATER, None);
                        let _ = world.settled("takes-over", launched);
                    }
                }
                world.two_passes();
                let held = world.armed().last().cloned().expect("a waiting worker");
                hooks.revalidation_hold.arm();
                world.events.emit_gone(PREDECESSOR_PANE);
                assert!(
                    hooks.revalidation_hold.arrived_by(1, deadline()),
                    "{case}: the worker consumed its cue and holds before the gate"
                );
                assert_eq!(
                    hooks.revalidation_arrivals.lock().unwrap().last(),
                    Some(&held)
                );
                // Delivery is held by none of it.
                assert!(world.fixture.nexus.dispatch_gate.try_lock().is_ok());
                let cancelled = *hooks.reassignments_cancelled.lock().unwrap();
                if identity_first {
                    hooks.pass_hold.arm();
                }
                world.show(PaneShown::Absent);
                closes = world.closes();
                match reassigning {
                    Reassigning::SuccessorStop => assert_eq!(
                        world.stop(SUCCESSOR),
                        Output::Stopped(FlowId(SUCCESSOR.into()))
                    ),
                    Reassigning::LaterRest => {
                        world.rest(LATER);
                        assert!(vetted(&world.vet(LATER)), "{case}");
                    }
                }
                attempts = world.attempt_count();
                if identity_first {
                    assert!(
                        hooks.pass_hold.arrived_by(1, deadline()),
                        "{case}: the pass holds at its entry"
                    );
                    let ended = *hooks.workers_ended.lock().unwrap();
                    hooks.revalidation_hold.release();
                    assert!(
                        hooks.workers_ended_by(ended + 1, deadline()),
                        "{case}: the held worker ends"
                    );
                    hooks.pass_hold.release();
                } else {
                    assert!(
                        hooks.reassignments_cancelled_by(cancelled + 1, deadline()),
                        "{case}: the pass cancels the held worker as reassigned"
                    );
                    hooks.revalidation_hold.release();
                }
                world.until("the new worker settles the row", &mut || {
                    world.row().is_none()
                });
            });
            let held_end = if identity_first {
                PaneWatchEnd::Stale
            } else {
                PaneWatchEnd::Cancelled(ObservationCancellationCause::ObligationReassigned)
            };
            assert_eq!(
                &ends[ends.len() - 2..],
                &[
                    (PREDECESSOR.to_owned(), held_end),
                    (PREDECESSOR.to_owned(), PaneWatchEnd::Acted)
                ],
                "{case}: {ends:?}"
            );
            let since = world.attempts_since(attempts);
            assert_eq!(
                since.iter().filter(|attempt| *attempt == "Reaped").count(),
                1,
                "{case}: only the new worker settles: {since:?}"
            );
            assert!(
                !since.iter().any(|attempt| attempt == "ReapRefused"),
                "{case}: the stale worker writes nothing: {since:?}"
            );
            assert!(world.row().is_none());
            assert!(replaced_p_by_s(world.outcome()), "{case}");
            assert!(
                !world
                    .generations()
                    .contains(&(PREDECESSOR.to_owned(), SUCCESSOR.to_owned())),
                "{case}: no generation for S from the reap: {:?}",
                world.generations()
            );
            assert_eq!(world.closes(), closes, "{case}: no close after the Gone");
            assert!(!vetted(&world.vet(PREDECESSOR)), "{case}");
            assert!(!vetted(&world.vet(SUCCESSOR)), "{case}");
            if reassigning == Reassigning::LaterRest {
                assert!(vetted(&world.vet(LATER)), "{case}");
            }
        }
    }
}

/// Test 9: delivery to a new successor is never held by a predecessor reap:
/// at each point after N's confirmation (its takeover refused, the row
/// taken over with its worker waiting, the takeover landed and settled) N
/// is Vetted at once, P and S are refused, and the dispatch gate is free
/// while the worker waits on Herdr.
#[test]
fn delivery_to_a_new_successor_is_never_held_by_a_predecessor_reap() {
    let world = ReapWorld::staged("never-held");
    let unheld = |point: &str| {
        assert!(vetted(&world.vet(LATER)), "{point}: N is delivered to");
        assert!(
            delivery_rejected(
                &world.vet(SUCCESSOR),
                meta_signal_flow::DeliveryRejection::FlowStopped
            ),
            "{point}"
        );
        assert!(!vetted(&world.vet(PREDECESSOR)), "{point}");
        assert!(
            world.fixture.nexus.dispatch_gate.try_lock().is_ok(),
            "{point}: the gate is free while the worker waits"
        );
        let sent = world
            .fixture
            .nexus
            .dispatch(Input::Send(signal_flow::SendRequest {
                address: world.address(),
                sendable_message: signal_flow::SendableMessage::Soft(
                    signal_flow::SendableRequest::Notice(format!("at {point}")),
                ),
            }));
        assert!(matches!(sent, Output::SendAccepted(_)), "{point}: {sent:?}");
    };
    with_supervisor(&world.fixture, || {
        held_by_the_address(&world, PaneShown::SessionUnreported);
        let faults = &world.fixture.nexus.store.faults;
        faults.refuse_next(RefusedWrite::StartedSettle);
        faults.refuse_next(RefusedWrite::HandoverOwing);
        let launched = world.confirmed_at_address("takes-over", LATER, None);
        let _ = world.settled("takes-over", launched);
        unheld("the takeover refused");
        world.rest(LATER);
        world.until("the worker under N", &mut || world.armed().len() == 3);
        world.two_passes();
        unheld("the row taken over, its worker waiting");
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.row().is_none());
        unheld("the reap settled");
    });
}

/// Test 11: one close rule for every reap worker, whoever owes the reap: a
/// Codex agent that reports no session is closed only when its foreground
/// process resumes the recorded thread; Owned and Bare close once;
/// Foreign, Unproven and Unidentified are never closed; Absent settles
/// with no close; Unknown, a snapshot with no pane list among them, writes
/// HerdrUnavailable and settles nothing.
#[test]
fn one_close_rule_for_every_reap_worker() {
    use super::waking::{ResumingProcess, SpawnsResumingProcess};
    /// What a case leaves.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Judged {
        ClosedOnce,
        NotClosed,
        Settled,
        HerdrUnavailable,
    }
    let thread = "01a0b22c-e24f-7452-9940-64490878680f";
    let cases: Vec<(&str, Judged)> = vec![
        ("owned by its session", Judged::ClosedOnce),
        ("owned by its foreground resume", Judged::ClosedOnce),
        ("resuming another thread", Judged::NotClosed),
        ("unproven", Judged::NotClosed),
        ("another harness", Judged::NotClosed),
        ("another session", Judged::NotClosed),
        ("unidentified", Judged::NotClosed),
        ("bare", Judged::ClosedOnce),
        ("absent", Judged::Settled),
        ("no pane list", Judged::HerdrUnavailable),
        ("unreadable", Judged::HerdrUnavailable),
    ];
    for address_held in [false, true] {
        for (case, judged) in &cases {
            let label = format!("{case}, address held: {address_held}");
            let world = ReapWorld::staged(&format!(
                "close-rule-{}-{address_held}",
                case.replace(' ', "-")
            ));
            let mut resuming: Option<ResumingProcess> = None;
            let set_case = |resuming: &mut Option<ResumingProcess>| match *case {
                "owned by its session" => world.show(PaneShown::Present),
                "owned by its foreground resume" | "resuming another thread" => {
                    let process =
                        ResumingProcess::resuming(if *case == "resuming another thread" {
                            "another-thread"
                        } else {
                            thread
                        });
                    world.foreground(process.0.id());
                    *resuming = Some(process);
                    world.show(PaneShown::SessionUnreported);
                }
                "unproven" => world.show(PaneShown::SessionUnreported),
                "another harness" => world.show(PaneShown::OtherHarness),
                "another session" => world.show(PaneShown::Occupied),
                "unidentified" => {
                    world.fixture.nexus.store.forget_session(PREDECESSOR);
                    world.show(PaneShown::Present);
                }
                "bare" => world.show(PaneShown::Bare),
                "absent" => world.show(PaneShown::Absent),
                "no pane list" => world.show(PaneShown::NoPaneList),
                _ => world.show(PaneShown::Unreadable),
            };
            with_supervisor(&world.fixture, || {
                let snapshots;
                if address_held {
                    // The first worker waits on a pane it may not close;
                    // then S's Stop gives the row to the Address.
                    world.show(PaneShown::Occupied);
                    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
                    world.until("the first worker's snapshot", &mut || {
                        world.snapshots_of_predecessor() >= 1 && world.events.subscriptions() >= 1
                    });
                    world.two_passes();
                    // The pass after the Stop is held until the case is
                    // shown, so the Address's worker reads the case and the
                    // Stop reads S's own pane.
                    let hooks = &world.fixture.nexus.pane_watch_hooks;
                    hooks.pass_hold.arm();
                    assert_eq!(
                        world.stop(SUCCESSOR),
                        Output::Stopped(FlowId(SUCCESSOR.into())),
                        "{label}"
                    );
                    assert!(hooks.pass_hold.arrived_by(1, deadline()), "{label}");
                    set_case(&mut resuming);
                    snapshots = world.snapshots_of_predecessor();
                    hooks.pass_hold.release();
                    world.until("the Address's worker", &mut || world.armed().len() == 2);
                    assert_eq!(
                        world.armed()[1].reap_responsibility,
                        ReapResponsibility::AddressHeld,
                        "{label}"
                    );
                } else {
                    set_case(&mut resuming);
                    snapshots = 0;
                    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
                    world.until("the first worker", &mut || world.armed().len() == 1);
                    assert_eq!(
                        world.armed()[0].reap_responsibility,
                        ReapResponsibility::LaunchSuccessor
                    );
                }
                match judged {
                    Judged::ClosedOnce => {
                        world.until("one close", &mut || world.closes() == 1);
                    }
                    Judged::Settled => {
                        world.until("the reap settled", &mut || world.row().is_none());
                    }
                    Judged::HerdrUnavailable => {
                        world.until("HerdrUnavailable", &mut || {
                            world.standing() == Some(ReapStanding::HerdrUnavailable)
                        });
                    }
                    Judged::NotClosed => {
                        let subscriptions = if address_held { 2 } else { 1 };
                        world.until("the opening snapshot", &mut || {
                            world.snapshots_of_predecessor() > snapshots
                                && world.events.subscriptions() >= subscriptions
                        });
                        world.two_passes();
                    }
                }
            });
            let closes = if *judged == Judged::ClosedOnce { 1 } else { 0 };
            assert_eq!(world.closes(), closes, "{label}");
            match judged {
                Judged::Settled => assert!(replaced_p_by_s(world.outcome()), "{label}"),
                Judged::HerdrUnavailable => {
                    assert!(world.row().is_some(), "{label}: nothing settled");
                }
                Judged::NotClosed | Judged::ClosedOnce => {
                    assert!(world.row().is_some(), "{label}: settled only by Gone");
                }
            }
            drop(resuming);
        }
    }
}

/// Test 12 and the adoption's writer: an adoption of N over a dead S takes
/// over what S owed at A in its one commit: S Exited, the store-only row
/// finished, the pane-owed row TakenOver(N); the pass arms one worker under
/// it. A refused adoption leaves every row, S and A as they stood.
#[test]
fn an_adoption_takes_over_the_reaps_its_dead_predecessor_owed() {
    for refused in [true, false] {
        let world = ReapWorld::staged(&format!("adoption-{refused}"));
        let second = second_row(&world);
        world.fixture.nexus.store.prove_owner_dead(SUCCESSOR);
        let store = &world.fixture.nexus.store;
        let binding = adopted_binding();
        let node = signal_flow::FlowNode {
            flow_id: FlowId(LATER.into()),
            session_id: signal_flow::SessionId(binding.native_session_id.0.clone()),
            harness_kind: signal_flow::HarnessKind::Codex,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                    herdr_agent_name: binding.herdr_agent_name.clone(),
                    herdr_pane_id: binding.herdr_pane_id.clone(),
                    herdr_terminal_id: binding.herdr_terminal_id.clone(),
                },
            ),
            origin: signal_flow::Origin::Unbound,
            flow_lifecycle: FlowLifecycle::Active,
        };
        let role = signal_flow::CallerIdentity {
            flow_id: FlowId(LATER.into()),
            aspect: world.address().aspect,
            model_name: world.launch.profile.model_name.clone(),
        };
        if refused {
            store.faults.refuse_next(RefusedWrite::Adoption);
        }
        let adopted = store.adopt_existing_flow(node, role, world.address(), &binding);
        if refused {
            assert!(adopted.is_err(), "{adopted:?}");
            assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Active);
            assert_eq!(world.owner(), Some(ReapResponsibility::LaunchSuccessor));
            assert_eq!(
                store
                    .pending_reap(&second)
                    .unwrap()
                    .map(|row| row.reap_standing),
                Some(ReapStanding::SettleUnrecorded)
            );
            assert_eq!(
                store.receiving_flow(&world.address()).unwrap(),
                Some(FlowId(SUCCESSOR.into()))
            );
            continue;
        }
        assert!(
            matches!(adopted, Ok(crate::store::ExistingSeatAdoption::Bound(_))),
            "{adopted:?}"
        );
        assert_eq!(world.lifecycle(SUCCESSOR), FlowLifecycle::Exited);
        assert!(store.pending_reap(&second).unwrap().is_none());
        assert!(matches!(
            store.launch_outcome("second-launch").unwrap(),
            Some(LaunchOutcome::Replaced(_))
        ));
        assert_eq!(world.owner(), Some(taken_over_by(LATER)));
        world.show(PaneShown::Occupied);
        with_supervisor(&world.fixture, || {
            world.until("one worker under N", &mut || world.armed().len() == 1);
            world.two_passes();
        });
        assert_eq!(world.armed().len(), 1);
        assert_eq!(world.armed()[0].reap_responsibility, taken_over_by(LATER));
    }
}

/// The binding of N, adopted at A by this test process.
fn adopted_binding() -> meta_signal_flow::FlowBinding {
    use std::os::unix::fs::MetadataExt;
    let process_id = i64::from(std::process::id());
    let metadata = fs::metadata(format!("/proc/{process_id}")).expect("current process");
    let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).expect("process stat");
    let (_, fields) = stat.rsplit_once(") ").expect("process comm boundary");
    meta_signal_flow::FlowBinding {
        flow_id: FlowId(LATER.into()),
        aspect: signal_flow::Aspect::Field,
        model_name: flow_ethos::ModelName("fixture-model".into()),
        harness_kind: signal_flow::HarnessKind::Codex,
        native_session_id: signal_flow::NativeSessionId("session-adopted".into()),
        herdr_workspace_id: signal_flow::HerdrWorkspaceId("workspace".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("pane-adopted".into()),
        herdr_tab_id: meta_signal_flow::HerdrTabId("tab".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("terminal-adopted".into()),
        herdr_agent_name: signal_flow::HerdrAgentName("agent-adopted".into()),
        process_identity: meta_signal_flow::ProcessIdentity {
            process_id: meta_signal_flow::ProcessId(process_id),
            process_user_id: meta_signal_flow::ProcessUserId(i64::from(metadata.uid())),
            process_start_token: meta_signal_flow::ProcessStartToken(
                fields
                    .split_whitespace()
                    .nth(19)
                    .expect("process start token")
                    .into(),
            ),
        },
        working_directory: meta_signal_flow::WorkingDirectory("/tmp".into()),
    }
}

/// A world's first row owed at `Owed` by S (the Replace's W3, no worker
/// run), and a second row S owes at A at SettleUnrecorded, for another
/// predecessor; the second predecessor's key.
fn second_row(world: &ReapWorld) -> String {
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    let store = &world.fixture.nexus.store;
    let second = "5ec0d0".to_owned();
    store
        .reap_owed(
            PendingReap {
                predecessor: Predecessor(second.clone()),
                launch_request_id: signal_flow::LaunchRequestId("second-launch".into()),
                address: world.address(),
                reap_standing: ReapStanding::Owed,
                reap_responsibility: ReapResponsibility::LaunchSuccessor,
            },
            crate::generated::starter::ReplacedRecord {
                flow_id: FlowId(second.clone()),
                launched_record: crate::generated::starter::LaunchedRecord {
                    flow_id: FlowId(SUCCESSOR.into()),
                    session_id: signal_flow::SessionId(String::new()),
                    origin: signal_flow::Origin::Unbound,
                },
            },
        )
        .unwrap();
    assert!(
        store
            .reap_refused(&second, ReapStanding::SettleUnrecorded)
            .unwrap()
    );
    second
}

/// The writers that change A's receiving flow besides an end and a
/// settlement: a Bind, a rebind of the current flow, and a Record.Asleep of
/// an ended flow's metaflow. Each carries the reassignment in its own
/// commit; each refused leaves every row and the metaflow as they stood.
#[test]
fn every_receiver_changing_writer_reassigns_in_its_own_commit() {
    use crate::store::ReadsFlowMemory;
    use crate::store::metaflow::MutatesMetaflows;
    use crate::store::repair::RepairsMetaflows;
    for writer in ["bind", "rebind", "sleep"] {
        for refused in [true, false] {
            let label = format!("{writer}, refused: {refused}");
            let world = ReapWorld::staged(&format!("writer-{writer}-{refused}"));
            let second = second_row(&world);
            let store = &world.fixture.nexus.store;
            let address = world.address();
            let faults = &store.faults;
            let before = store.metaflow(&address).unwrap();
            let receiving_after = match writer {
                "bind" => {
                    if refused {
                        faults.refuse_next(RefusedWrite::MetaflowBind);
                    }
                    let process =
                        crate::store::FlowStore::process_identity_for_pid(std::process::id())
                            .expect("this process");
                    let bound = store.bind_metaflow(
                        signal_flow::Bind_Data {
                            address: address.clone(),
                            process,
                        },
                        None,
                    );
                    match bound {
                        Ok(Output::Bound(flow_id)) => Some(flow_id),
                        Err(_) if refused => None,
                        other => panic!("{label}: {other:?}"),
                    }
                }
                "rebind" => {
                    world.fixture.register_predecessor(LATER);
                    if refused {
                        faults.refuse_next(RefusedWrite::CurrentFlowRebind);
                    }
                    let rebound = store.rebind_current_flow(&address, LATER);
                    match rebound {
                        Ok(Ok(())) => Some(FlowId(LATER.into())),
                        Err(_) if refused => None,
                        other => panic!("{label}: {other:?}"),
                    }
                }
                _ => {
                    // S ended on its own, its metaflow left Awake as it.
                    store.mark_exited(SUCCESSOR);
                    if refused {
                        faults.refuse_next(RefusedWrite::MetaflowSleep);
                    }
                    let slept = store.sleep_metaflow(&address, SUCCESSOR);
                    match slept {
                        Ok(true) => None,
                        Err(_) if refused => None,
                        other => panic!("{label}: {other:?}"),
                    }
                }
            };
            if refused {
                assert_eq!(store.metaflow(&address).unwrap(), before, "{label}");
                assert_eq!(
                    world.owner(),
                    Some(ReapResponsibility::LaunchSuccessor),
                    "{label}"
                );
                assert_eq!(
                    store
                        .pending_reap(&second)
                        .unwrap()
                        .map(|row| row.reap_standing),
                    Some(ReapStanding::SettleUnrecorded),
                    "{label}"
                );
                assert!(pending_p_by_s(world.outcome()), "{label}");
                continue;
            }
            assert_eq!(
                store.receiving_flow(&address).unwrap(),
                receiving_after,
                "{label}"
            );
            let expected = match &receiving_after {
                Some(flow) => taken_over_by(&flow.0),
                None => ReapResponsibility::AddressHeld,
            };
            assert_eq!(world.owner(), Some(expected), "{label}");
            assert!(store.pending_reap(&second).unwrap().is_none(), "{label}");
            assert!(
                matches!(
                    store.launch_outcome("second-launch").unwrap(),
                    Some(LaunchOutcome::Replaced(_))
                ),
                "{label}"
            );
        }
    }
}

/// The typed missing owner: a LaunchSuccessor row whose launch outcome is
/// not ReapPending has no owner (`reap_owner` answers Ok(None)), never the
/// Address. No end, takeover, rest, start or pass moves or settles it, and
/// no worker is armed for it.
#[test]
fn a_missing_owner_is_never_taken_for_the_address() {
    let mut world = ReapWorld::staged("missing-owner");
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    let store = &world.fixture.nexus.store;
    store
        .record_launch_outcome(
            &world.launch.profile.launch_request_id.0,
            LaunchOutcome::StartRejected(signal_flow::StartRejection::LaunchPersistenceRefused),
        )
        .unwrap();
    let row = world.row().expect("the row stands");
    assert_eq!(store.reap_owner(&row).unwrap(), None);
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::Stopped(FlowId(SUCCESSOR.into()))
    );
    assert_eq!(world.row(), Some(row.clone()), "the end leaves it");
    let launched = world.confirmed_at_address("after-missing", LATER, None);
    let _ = world.settled("after-missing", launched);
    world.rest(LATER);
    assert_eq!(
        world.row(),
        Some(row.clone()),
        "the takeover and rest leave it"
    );
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.row(), Some(row), "the start leaves it");
    with_supervisor(&world.fixture, || world.two_passes());
    assert!(world.armed().is_empty(), "no worker for a row nobody owes");
    assert_eq!(world.closes(), 0);
}

/// ea0f: a worker's completion is joined in the pass it wakes even when
/// that pass cannot read the store; its obligation keeps its standing and
/// is not armed again until a bearing event.
#[test]
fn a_completion_during_a_refused_store_read_is_joined_at_once_and_not_rearmed() {
    let world = ReapWorld::staged("refused-pass-read");
    world.show(PaneShown::Occupied);
    let hooks = &world.fixture.nexus.pane_watch_hooks;
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        world.two_passes();
        let faults = &world.fixture.nexus.store.faults;
        faults.refuse_next(RefusedWrite::PassRead);
        world.events.close_streams();
        assert!(
            hooks.workers_joined_by(1, deadline()),
            "joined by its own completion"
        );
        assert!(
            !faults.refused_now(RefusedWrite::PassRead),
            "the pass that joined it read nothing"
        );
        for _ in 0..3 {
            world.fixture.nexus.store.launch_changes.announce();
        }
        world.two_passes();
        assert_eq!(hooks.spawned.lock().unwrap().len(), 1, "not armed again");
        assert_eq!(world.standing(), Some(ReapStanding::Owed));
        world.rest(SUCCESSOR);
        world.until("the bearing event arms it", &mut || {
            hooks.spawned.lock().unwrap().len() == 2
        });
    });
    assert_eq!(
        ends.first(),
        Some(&(
            PREDECESSOR.to_owned(),
            PaneWatchEnd::Lapsed(PaneWatchLapse::StreamClosedPanePresent)
        ))
    );
}

/// jjx9: production-path witnesses of a worker that lapses before it
/// subscribes: Herdr lists no session for the predecessor
/// (`session_socket_until` yields None, `SessionUnknown`), or the session's
/// socket refuses the subscription (`SubscriptionRefused`). Each is joined
/// by its own completion with no other store event, writes nothing, and
/// stays Lapsed until its bearing event rearms it.
#[test]
fn a_worker_that_lapses_before_it_subscribes_is_joined_by_its_own_completion_and_stays_lapsed() {
    for lapse in ["session unknown", "subscription refused"] {
        let world = ReapWorld::staged(&format!("lapse-{}", lapse.replace(' ', "-")));
        match lapse {
            "session unknown" => world.fixture.list_herdr_sessions(&[(
                "fixture-session",
                world.fixture.file("herdr-fixture-session.sock"),
            )]),
            _ => world.fixture.list_herdr_sessions(&[
                (
                    "messaging-build",
                    world.fixture.file("nothing-listens.sock"),
                ),
                (
                    "fixture-session",
                    world.fixture.file("herdr-fixture-session.sock"),
                ),
            ]),
        }
        let hooks = &world.fixture.nexus.pane_watch_hooks;
        let ends = with_supervisor(&world.fixture, || {
            assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
            let attempts = world.attempt_count();
            assert!(
                hooks.workers_joined_by(1, deadline()),
                "{lapse}: joined by its own completion"
            );
            assert_eq!(world.attempt_count(), attempts, "{lapse}: nothing written");
            for _ in 0..3 {
                world.fixture.nexus.store.launch_changes.announce();
            }
            world.two_passes();
            assert_eq!(
                hooks.spawned.lock().unwrap().len(),
                1,
                "{lapse}: stays Lapsed"
            );
            assert_eq!(world.standing(), Some(ReapStanding::Owed));
            world.rest(SUCCESSOR);
            world.until("the bearing event rearms it", &mut || {
                hooks.spawned.lock().unwrap().len() == 2
            });
            assert!(hooks.workers_joined_by(2, deadline()));
        });
        let expected = |end: &PaneWatchEnd| match lapse {
            "session unknown" => *end == PaneWatchEnd::Lapsed(PaneWatchLapse::SessionUnknown),
            _ => matches!(
                end,
                PaneWatchEnd::Lapsed(PaneWatchLapse::SubscriptionRefused(_))
            ),
        };
        assert_eq!(ends.len(), 2, "{lapse}: {ends:?}");
        assert!(
            ends.iter().all(|(_, end)| expected(end)),
            "{lapse}: {ends:?}"
        );
        assert_eq!(world.events.subscriptions(), 0, "{lapse}");
    }
}

/// Test 10: a store whose pending-reap family is registered at v1, as an
/// earlier 0.54.7 candidate build wrote it, is refused at open, typed,
/// through the fixture's open and through the configured open, its logical
/// rows and catalog unchanged.
#[test]
fn a_store_of_pending_reap_v1_is_refused_at_open() {
    // The fixture's store, with a row at v1.
    let mut world = ReapWorld::staged("pending-reap-v1");
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    let path = world.fixture.directory.path().join("flow.sema");
    world.fixture.restarted();
    let scratch = world.fixture.directory.path().join("v1-scratch");
    fs::create_dir_all(&scratch).unwrap();
    let placeholder = crate::store::FlowStore::open(&scratch.join("placeholder.sema")).unwrap();
    drop(std::mem::replace(
        &mut world.fixture.nexus.store,
        placeholder.into(),
    ));
    pending_reaps_at_v1(&path, 1, true, &world.address());
    let before = LogicalState::read(&path);
    for _ in 0..2 {
        let refused = crate::store::FlowStore::open(&path);
        assert!(refused.is_err(), "a v1 family is refused at open");
        assert!(
            format!("{:?}", refused.as_ref().err()).contains("FamilyIdentityMismatch"),
            "typed: {:?}",
            refused.err()
        );
        assert_eq!(LogicalState::read(&path), before);
    }

    // A configured store, its family at v1 and empty.
    let configured = super::configuring::ConfiguredStore::new();
    let path = configured.path().to_path_buf();
    drop(crate::store::FlowStore::open_configured(&path).expect("the running store opens"));
    pending_reaps_at_v1(
        &path,
        crate::store::FLOW_SCHEMA_VERSION,
        false,
        &world.address(),
    );
    let before = LogicalState::read(&path);
    for _ in 0..2 {
        // The configured open's recognition refuses it first, naming the
        // family.
        let refused = crate::store::FlowStore::open_configured(&path);
        assert!(
            matches!(
                refused.as_ref().err(),
                Some(crate::store::StoreError::UnrecognizedMemory { reason, .. })
                    if reason.contains("flow_nexus_pending_reaps")
            ),
            "typed: {:?}",
            refused.as_ref().err()
        );
        drop(refused);
        assert_eq!(LogicalState::read(&path), before);
    }
}

/// The pending-reap family as an earlier 0.54.7 candidate stored it: v1,
/// four fields, no responsibility.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
struct PendingReapV1 {
    predecessor: Predecessor,
    launch_request_id: signal_flow::LaunchRequestId,
    address: signal_flow::Address,
    reap_standing: ReapStanding,
}

impl sema_engine::EngineRecord for PendingReapV1 {
    fn record_key(&self) -> sema_engine::RecordKey {
        sema_engine::RecordKey::new(self.predecessor.0.clone())
    }
}

/// Rewrites the closed store at `path`, of Sema schema `version`, with its
/// pending-reap family registered at the v1 label, holding one row when
/// `with_row`.
fn pending_reaps_at_v1(
    path: &std::path::Path,
    version: u32,
    with_row: bool,
    address: &signal_flow::Address,
) {
    {
        let database = redb::Database::open(path).expect("the store opens raw");
        let write = database.begin_write().unwrap();
        {
            let mut catalog = write
                .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                    "__sema_engine_catalog",
                ))
                .unwrap();
            catalog.remove("flow_nexus_pending_reaps").unwrap();
        }
        let _ = write.delete_table(redb::TableDefinition::<String, &[u8]>::new(
            "flow_nexus_pending_reaps",
        ));
        write.commit().unwrap();
    }
    let mut engine = sema_engine::Engine::open(sema_engine::EngineOpen::new(
        path,
        sema_engine::SchemaVersion::new(version),
    ))
    .unwrap();
    let table = engine
        .register_table(sema_engine::TableDescriptor::<PendingReapV1>::new(
            sema_engine::TableName::new("flow_nexus_pending_reaps"),
            sema_engine::FamilyName::new("flow-nexus-pending-reap"),
            sema_engine::SchemaHash::for_label("flow-nexus-pending-reap-v1"),
        ))
        .unwrap();
    if with_row {
        engine
            .assert(sema_engine::Assertion::new(
                table,
                PendingReapV1 {
                    predecessor: Predecessor(PREDECESSOR.into()),
                    launch_request_id: signal_flow::LaunchRequestId("pending-reap-v1".into()),
                    address: address.clone(),
                    reap_standing: ReapStanding::Owed,
                },
            ))
            .unwrap();
    }
}

/// The 0.54.6-to-0.54.7 upgrade, atomic, through the real configured open:
/// a refused reap of 0.54.6, whose attempt, binding and replacement the
/// store holds, opens as ReapPending with its PendingReap owed as its
/// Address stands (LaunchSuccessor, TakenOver, AddressHeld), its
/// predecessor Stopped, in one evolution; a second open changes nothing.
#[test]
fn a_0_54_6_refused_reap_opens_owed_as_its_address_stands_through_the_configured_open() {
    use crate::store::reap_upgrade::{LaunchOutcomeV9, ReplaceRejectionV9, StoredLaunchOutcomeV9};
    for (stands, expected) in [
        (
            "awake as the successor",
            ReapResponsibility::LaunchSuccessor,
        ),
        ("awake as another", taken_over_by(LATER)),
        ("asleep", ReapResponsibility::AddressHeld),
    ] {
        // The rows a 0.54.6 Replace left: read from a fixture's store.
        let mut world = ReapWorld::staged("configured-upgrade");
        world
            .fixture
            .nexus
            .store
            .faults
            .refuse_next(RefusedWrite::ReapOwing);
        assert!(is_reap_pending(&world.replace(), ReapStanding::Unrecorded));
        let id = world.launch.profile.launch_request_id.0.clone();
        let source = world.fixture.directory.path().join("flow.sema");
        world.fixture.restarted();
        let scratch = world.fixture.directory.path().join("upgrade-scratch");
        fs::create_dir_all(&scratch).unwrap();
        let placeholder = crate::store::FlowStore::open(&scratch.join("placeholder.sema")).unwrap();
        drop(std::mem::replace(
            &mut world.fixture.nexus.store,
            placeholder.into(),
        ));
        let configured = super::configuring::ConfiguredStore::new();
        let path = configured.path().to_path_buf();
        drop(crate::store::FlowStore::open_configured(&path).expect("the running store opens"));
        copy_launch_rows(&source, &path, &world.address(), stands);
        super::reaping::v9_store_at(
            &path,
            vec![StoredLaunchOutcomeV9 {
                launch_request_id: id.clone(),
                launch_outcome: LaunchOutcomeV9::ReplaceRejected(ReplaceRejectionV9::ReapRefused(
                    signal_flow::StopRejection::CloseRefused,
                )),
            }],
            crate::store::FLOW_SCHEMA_VERSION,
        );
        let opened =
            crate::store::FlowStore::open_configured(&path).expect("the 0.54.6 store opens");
        let row = opened
            .pending_reap(PREDECESSOR)
            .unwrap()
            .expect("the reap is owed");
        assert_eq!(row.reap_standing, ReapStanding::CloseRefused, "{stands}");
        assert_eq!(row.reap_responsibility, expected, "{stands}");
        assert!(
            matches!(
                opened.launch_outcome(&id).unwrap(),
                Some(LaunchOutcome::ReapPending(_))
            ),
            "{stands}"
        );
        assert_eq!(
            opened
                .flow_node(PREDECESSOR)
                .unwrap()
                .unwrap()
                .flow_lifecycle,
            FlowLifecycle::Stopped,
            "{stands}"
        );
        drop(opened);
        let upgraded = LogicalState::read(&path);
        drop(crate::store::FlowStore::open_configured(&path).expect("a second open"));
        assert_eq!(LogicalState::read(&path), upgraded, "{stands}: a no-op");
    }
}

/// Copies the flow, attempt, replacement and metaflow rows of the closed
/// store `from` into the closed configured store `to`, raw; the metaflow at
/// `address` as `stands` names.
fn copy_launch_rows(
    from: &std::path::Path,
    to: &std::path::Path,
    address: &signal_flow::Address,
    stands: &str,
) {
    let tables = [
        "flow_nexus_flows",
        "flow_nexus_launch_attempts",
        "flow_nexus_replacements",
        "flow_nexus_herdr_routes",
    ];
    let source = LogicalState::read(from);
    let database = redb::Database::open(to).expect("the configured store opens raw");
    let write = database.begin_write().unwrap();
    for table in tables {
        let mut target = write
            .open_table(redb::TableDefinition::<String, &[u8]>::new(table))
            .unwrap();
        for (key, value) in source.rows(table) {
            target.insert(key.clone(), value.as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    drop(database);
    // The metaflow, written through a store over the copy.
    let store = crate::store::FlowStore::open_configured(to).expect("the copy opens");
    use crate::store::metaflow::SeedsMetaflow;
    store.seed_metaflow(signal_flow::Metaflow {
        address: address.clone(),
        state: match stands {
            "awake as the successor" => flow_ethos::State::Awake(FlowId(SUCCESSOR.into())),
            "awake as another" => flow_ethos::State::Awake(FlowId(LATER.into())),
            _ => flow_ethos::State::Asleep,
        },
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
}

/// Test 13: an ordered launch left orphaned by its flow's end, started by
/// `wake_due` (`start_ordered`, under the dispatch gate), takes the reap the
/// Address holds over in its own settlement: TakenOver by the new flow, one
/// settlement attempt, none refused, and no generation from the reap.
#[test]
fn an_ordered_launch_after_an_end_takes_the_address_held_reap_over() {
    use super::waking::{ReadsFreshWorld, WakesFreshClaude, address, ended_claude};
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::ordered_replacement::{
        OrderedLaunch, OrderedReplacement, OrdersReplacements, ReplacementOrder,
    };
    let mut fixture = super::NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(signal_flow::Model::Sol);
    let held = "0f0f0f";
    // A reap abc123 owed at A, held by the Address since abc123's metaflow
    // was put to sleep.
    {
        let store = &fixture.nexus.store;
        store.seed_metaflow(signal_flow::Metaflow {
            address: at.clone(),
            state: flow_ethos::State::Awake(FlowId("abc123".into())),
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
        store
            .reap_owed(
                PendingReap {
                    predecessor: Predecessor(held.into()),
                    launch_request_id: signal_flow::LaunchRequestId("held-launch".into()),
                    address: at.clone(),
                    reap_standing: ReapStanding::Owed,
                    reap_responsibility: ReapResponsibility::LaunchSuccessor,
                },
                crate::generated::starter::ReplacedRecord {
                    flow_id: FlowId(held.into()),
                    launched_record: crate::generated::starter::LaunchedRecord {
                        flow_id: FlowId("abc123".into()),
                        session_id: signal_flow::SessionId(seat.session.clone()),
                        origin: signal_flow::Origin::Unbound,
                    },
                },
            )
            .unwrap();
        assert!(store.sleep_metaflow(&at, "abc123").unwrap());
        assert_eq!(
            store
                .pending_reap(held)
                .unwrap()
                .map(|row| row.reap_responsibility),
            Some(ReapResponsibility::AddressHeld)
        );
    }
    let world = fixture.fresh_claude_world();
    let mut profile = seat.profile.clone();
    profile.launch_request_id = signal_flow::LaunchRequestId("ordered-launch".into());
    profile.flow_id_option = Some(FlowId("abc123".into()));
    fixture
        .nexus
        .store
        .order_replacement(OrderedReplacement {
            address: at.clone(),
            predecessor: "abc123".into(),
            order: ReplacementOrder::Launch(Box::new(OrderedLaunch {
                launch_profile: profile,
                origin: signal_flow::Origin::Unbound,
                prompt_sha256: String::new(),
            })),
        })
        .unwrap();
    let attempts = fixture.nexus.store.faults.attempts().len();
    fixture.nexus.wake_due(at.clone());
    let store = &fixture.nexus.store;
    let flow_ethos::State::Awake(started) = store.metaflow(&at).unwrap().unwrap().state else {
        panic!("the ordered launch starts; Herdr calls: {}", world.calls());
    };
    assert_ne!(started.0, "abc123");
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_eq!(
        store
            .pending_reap(held)
            .unwrap()
            .map(|row| row.reap_responsibility),
        Some(taken_over_by(&started.0)),
        "the launch's settlement took the reap over"
    );
    let since = store.faults.attempts()[attempts..].to_vec();
    assert_eq!(
        since
            .iter()
            .filter(|attempt| *attempt == "StartedHandedOver" || *attempt == "HandoverOwed")
            .count(),
        1,
        "one settlement attempt: {since:?}"
    );
    assert!(
        since.iter().any(|attempt| attempt == "ReapsReassigned"),
        "{since:?}"
    );
    assert!(
        store.ordered_replacement(&at).unwrap().is_none(),
        "the order is spent"
    );
    use crate::store::handover::HoldsHandovers;
    let generations = store
        .handover_memory(&at)
        .unwrap()
        .map(|row| row.closed)
        .unwrap_or_default();
    assert!(
        !generations
            .iter()
            .any(|closed| closed.predecessor == held && closed.successor == "abc123"),
        "{generations:?}"
    );
}

/// Sol's sequence to its fourth step (4vqk, twl7): W2 confirms S replacing
/// P, W3 is refused, so no row and no outcome stand, and S is Stopped
/// before its rest, its end commit finding no row to reassign. The Replace
/// is left derived, with P still Active.
fn successor_ended_before_w3(world: &ReapWorld) {
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapOwing);
    assert!(is_reap_pending(&world.replace(), ReapStanding::Unrecorded));
    assert_eq!(
        world.stop(SUCCESSOR),
        Output::Stopped(FlowId(SUCCESSOR.into()))
    );
    assert!(world.row().is_none());
    assert_eq!(world.outcome(), None);
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Active);
}

/// The start's obligation recovery, under the gate as `recover_wakes` runs
/// it.
fn recovered(world: &ReapWorld) {
    world.fixture.nexus.recover_obligations();
}

/// How many times `name` was attempted from `from` on.
fn attempted(world: &ReapWorld, from: usize, name: &str) -> usize {
    world
        .attempts_since(from)
        .iter()
        .filter(|attempt| *attempt == name)
        .count()
}

/// so5u, the Address Asleep: the derived reap of a successor that ended
/// before W3 is born held by the Address, never owed by the stopped S. The
/// worker sees P positively absent and its settlement is refused once: the
/// row stands at SettleUnrecorded, still the Address's. The next start
/// settles it once, in the store alone: no close, no Herdr call, no
/// generation to S.
#[test]
fn a_derived_reap_whose_successor_ended_is_born_held_by_the_address_and_settled_by_the_next_start()
{
    let mut world = ReapWorld::staged("derived-asleep");
    successor_ended_before_w3(&world);
    assert_eq!(
        world
            .fixture
            .nexus
            .store
            .receiving_flow(&world.address())
            .unwrap(),
        None
    );
    world.show(PaneShown::Absent);
    world.fixture.restarted();
    recovered(&world);
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    assert_eq!(world.standing(), Some(ReapStanding::Owed));
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
    assert!(pending_p_by_s(world.outcome()));
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapSettle);
    with_supervisor(&world.fixture, || {
        world.until("the refused settlement", &mut || {
            world.standing() == Some(ReapStanding::SettleUnrecorded)
        });
        assert!(
            world
                .fixture
                .nexus
                .pane_watch_hooks
                .workers_joined_by(1, deadline()),
            "the worker acted and was joined"
        );
    });
    assert_eq!(world.owner(), Some(ReapResponsibility::AddressHeld));
    assert_eq!(
        world.closes(),
        0,
        "a positively absent pane is never closed"
    );
    let calls = world.predecessor_calls();
    world.fixture.restarted();
    let from = world.attempt_count();
    recovered(&world);
    assert!(world.row().is_none(), "the start settled it");
    assert!(replaced_p_by_s(world.outcome()));
    assert_eq!(attempted(&world, from, "Reaped"), 1, "settled once");
    assert_eq!(world.closes(), 0, "no second close");
    assert_eq!(world.predecessor_calls(), calls, "no Herdr call");
    assert!(
        !world
            .generations()
            .contains(&(PREDECESSOR.to_owned(), SUCCESSOR.to_owned())),
        "no generation to S: {:?}",
        world.generations()
    );
    world.fixture.restarted();
    let again = world.attempt_count();
    recovered(&world);
    assert_eq!(
        attempted(&world, again, "Reaped"),
        0,
        "nothing left to settle"
    );
}

/// so5u, N receiving: S ended before W3 and N took the Address before the
/// start materialized the derived reap. The row is born owed by N; after
/// the worker's refused settlement it stands at SettleUnrecorded, N's, and
/// N's next rest settles it once, in the store alone, with no generation
/// to S. N's own handover obligation is left as it stood.
#[test]
fn a_derived_reap_whose_successor_ended_is_born_owed_by_the_receiving_flow_and_settled_at_its_rest()
{
    use crate::store::pending_handover::RecordsPendingHandovers;
    let mut world = ReapWorld::staged("derived-later");
    successor_ended_before_w3(&world);
    let launched = world.confirmed_at_address("later-launch", LATER, None);
    let _ = world.settled("later-launch", launched);
    assert_eq!(
        world
            .fixture
            .nexus
            .store
            .receiving_flow(&world.address())
            .unwrap(),
        Some(FlowId(LATER.into()))
    );
    let handover = world
        .fixture
        .nexus
        .store
        .pending_handover(&world.address())
        .unwrap();
    let later_outcome = world
        .fixture
        .nexus
        .store
        .launch_outcome("later-launch")
        .unwrap();
    world.show(PaneShown::Absent);
    world.fixture.restarted();
    recovered(&world);
    assert_eq!(world.owner(), Some(taken_over_by(LATER)));
    assert!(pending_p_by_s(world.outcome()));
    assert_eq!(
        world
            .fixture
            .nexus
            .store
            .pending_handover(&world.address())
            .unwrap(),
        handover,
        "N's handover obligation is left as it stood"
    );
    assert_eq!(
        world
            .fixture
            .nexus
            .store
            .launch_outcome("later-launch")
            .unwrap(),
        later_outcome
    );
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapSettle);
    with_supervisor(&world.fixture, || {
        world.until("the refused settlement", &mut || {
            world.standing() == Some(ReapStanding::SettleUnrecorded)
        });
        assert!(
            world
                .fixture
                .nexus
                .pane_watch_hooks
                .workers_joined_by(1, deadline()),
            "the worker acted and was joined"
        );
    });
    assert_eq!(world.owner(), Some(taken_over_by(LATER)));
    let calls = world.predecessor_calls();
    let from = world.attempt_count();
    world.rest(LATER);
    assert!(world.row().is_none(), "N's rest settled it");
    assert!(replaced_p_by_s(world.outcome()));
    assert_eq!(attempted(&world, from, "Reaped"), 1, "settled once");
    assert_eq!(world.closes(), 0);
    assert_eq!(world.predecessor_calls(), calls, "no Herdr call");
    assert!(
        !world
            .generations()
            .iter()
            .any(|(_, successor)| successor == SUCCESSOR),
        "no generation to S: {:?}",
        world.generations()
    );
    let again = world.attempt_count();
    world.rest(LATER);
    assert_eq!(attempted(&world, again, "Reaped"), 0);
}

/// M, the flow that receives at A after N.
const LAST: &str = "b8b8b8";

/// A fresh launch `id` of `flow` at A whose C1 is refused: `flow` receives
/// there, and its own PendingHandover stands durable under its
/// `HandoverPending` outcome; that row.
fn owes_its_own_handover(world: &ReapWorld, id: &str, flow: &str) -> PendingHandover {
    use crate::store::pending_handover::RecordsPendingHandovers;
    let store = &world.fixture.nexus.store;
    store.faults.refuse_next(RefusedWrite::StartedSettle);
    let launched = world.confirmed_at_address(id, flow, None);
    let answer = world.settled(id, launched);
    assert!(matches!(answer, Output::HandoverPending(_)), "{answer:?}");
    assert_eq!(
        store.receiving_flow(&world.address()).unwrap(),
        Some(FlowId(flow.into()))
    );
    let row = store
        .pending_handover(&world.address())
        .unwrap()
        .expect("its own PendingHandover");
    assert_eq!(row.successor.0.0, flow);
    assert!(
        matches!(store.launch_outcome(id).unwrap(),
            Some(LaunchOutcome::HandoverPending(launched)) if launched.flow_id.0 == flow),
        "{:?}",
        store.launch_outcome(id).unwrap()
    );
    row
}

/// The handover work at A as it stands: the PendingHandover and the
/// outcome of each named launch.
fn handover_work(
    world: &ReapWorld,
    launches: &[&str],
) -> (Option<PendingHandover>, Vec<Option<LaunchOutcome>>) {
    use crate::store::pending_handover::RecordsPendingHandovers;
    let store = &world.fixture.nexus.store;
    (
        store.pending_handover(&world.address()).unwrap(),
        launches
            .iter()
            .map(|id| store.launch_outcome(id).unwrap())
            .collect(),
    )
}

/// Astra vh7l, the exact regression: W3 refused for S, S ended, N receiving
/// with its own durable PendingHandover; the start's recovery of S's
/// derived reap supersedes nothing for the ended S. N's PendingHandover and
/// its `HandoverPending` outcome are unchanged, the reap is born owed by N,
/// delivery goes to N alone, and N's next rest, its next bearing event,
/// settles its own handover.
#[test]
fn a_derived_reap_of_an_ended_successor_leaves_the_receiving_flows_own_handover_as_it_stood() {
    use crate::store::pending_handover::RecordsPendingHandovers;
    let mut world = ReapWorld::staged("derived-keeps-handover");
    successor_ended_before_w3(&world);
    let owed = owes_its_own_handover(&world, "later-launch", LATER);
    let before = handover_work(&world, &["later-launch"]);
    world.show(PaneShown::Absent);
    world.fixture.restarted();
    let from = world.attempt_count();
    recovered(&world);
    assert_eq!(world.owner(), Some(taken_over_by(LATER)));
    assert!(pending_p_by_s(world.outcome()));
    assert_eq!(
        handover_work(&world, &["later-launch"]),
        before,
        "N's PendingHandover and outcome are unchanged"
    );
    assert_eq!(
        attempted(&world, from, "Superseding"),
        0,
        "the ended S supersedes nothing: {:?}",
        world.attempts_since(from)
    );
    assert!(vetted(&world.vet(LATER)), "N is delivered to");
    assert!(delivery_rejected(
        &world.vet(SUCCESSOR),
        meta_signal_flow::DeliveryRejection::FlowStopped
    ));
    assert!(!vetted(&world.vet(PREDECESSOR)));
    let from = world.attempt_count();
    world.rest(LATER);
    assert_eq!(
        attempted(&world, from, "HandedOver"),
        1,
        "{:?}",
        world.attempts_since(from)
    );
    let store = &world.fixture.nexus.store;
    assert_eq!(store.pending_handover(&world.address()).unwrap(), None);
    assert!(
        matches!(store.launch_outcome("later-launch").unwrap(),
            Some(LaunchOutcome::Started(launched)) if launched.flow_id.0 == LATER),
        "{:?}",
        store.launch_outcome("later-launch").unwrap()
    );
    assert!(
        world
            .generations()
            .iter()
            .any(|(_, successor)| successor == LATER),
        "N's handover moved: {:?} (owed by {:?})",
        world.generations(),
        owed.handover_owner
    );
    assert!(
        !world
            .generations()
            .iter()
            .any(|(_, successor)| successor == SUCCESSOR),
        "no generation to S: {:?}",
        world.generations()
    );
}

/// Astra vh7l: a failed read of the current receiver during superseding
/// refuses the derived W3's whole commit. No PendingReap is written (the
/// reap stays derived, P Active), N's PendingHandover and outcome stand as
/// they were; the next start writes the row owed by N, still leaving N's
/// handover work as it stood.
#[test]
fn a_refused_receiver_read_in_superseding_refuses_the_commit_and_leaves_both_rows() {
    let mut world = ReapWorld::staged("derived-superseding-read");
    successor_ended_before_w3(&world);
    let _ = owes_its_own_handover(&world, "later-launch", LATER);
    let before = handover_work(&world, &["later-launch"]);
    world.show(PaneShown::Absent);
    world.fixture.restarted();
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::SupersedingRead);
    let from = world.attempt_count();
    recovered(&world);
    assert_eq!(attempted(&world, from, "ReapOwed"), 1);
    assert!(world.row().is_none(), "no PendingReap was written");
    assert_eq!(world.outcome(), None, "the reap stays derived");
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Active);
    assert_eq!(handover_work(&world, &["later-launch"]), before);
    world.fixture.restarted();
    recovered(&world);
    assert_eq!(world.owner(), Some(taken_over_by(LATER)));
    assert!(pending_p_by_s(world.outcome()));
    assert_eq!(handover_work(&world, &["later-launch"]), before);
}

/// Astra vh7l, stale recovery after another succession: S ended before
/// W3, then N received with its own PendingHandover, then N ended and M
/// received, superseding N's handover with its own. Recovering S's derived
/// reap changes neither N's nor M's handover work, and the reap is owned
/// by M.
#[test]
fn a_stale_derived_reap_after_two_successions_changes_no_handover_work_and_is_owned_by_the_last() {
    let mut world = ReapWorld::staged("derived-two-successions");
    successor_ended_before_w3(&world);
    let _ = owes_its_own_handover(&world, "later-launch", LATER);
    let store = &world.fixture.nexus.store;
    store.mark_exited(LATER);
    assert!(store.sleep_metaflow(&world.address(), LATER).unwrap());
    assert_eq!(store.receiving_flow(&world.address()).unwrap(), None);
    let last = owes_its_own_handover(&world, "last-launch", LAST);
    assert!(
        matches!(store.launch_outcome("later-launch").unwrap(),
            Some(LaunchOutcome::HandoverSuperseded(superseded))
                if superseded.successor.0.0 == LAST),
        "M superseded N's handover: {:?}",
        store.launch_outcome("later-launch").unwrap()
    );
    let before = handover_work(&world, &["later-launch", "last-launch"]);
    assert_eq!(before.0.as_ref(), Some(&last));
    world.show(PaneShown::Absent);
    world.fixture.restarted();
    recovered(&world);
    assert_eq!(world.owner(), Some(taken_over_by(LAST)));
    assert!(pending_p_by_s(world.outcome()));
    assert_eq!(
        handover_work(&world, &["later-launch", "last-launch"]),
        before,
        "neither N's nor M's handover work changed"
    );
}

/// so5u, a transient predecessor read refused during the start's derived
/// recovery: the row is born owed by the Address's reachable current flow
/// N, typed PredecessorUnknown, with no worker and P left as it stood. N's
/// next rest reads P again, records it Stopped, and the worker settles the
/// reap once.
#[test]
fn a_predecessor_read_refused_in_derived_recovery_leaves_the_row_typed_under_the_current_owner() {
    let mut world = ReapWorld::staged("derived-unknown");
    successor_ended_before_w3(&world);
    let launched = world.confirmed_at_address("unknown-launch", LATER, None);
    let _ = world.settled("unknown-launch", launched);
    world.show(PaneShown::Absent);
    world.fixture.restarted();
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    recovered(&world);
    assert_eq!(world.owner(), Some(taken_over_by(LATER)));
    assert_eq!(world.standing(), Some(ReapStanding::PredecessorUnknown));
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Active);
    assert!(pending_p_by_s(world.outcome()));
    with_supervisor(&world.fixture, || {
        world.two_passes();
        assert!(world.armed().is_empty(), "no worker for a typed Unknown");
        world.rest(LATER);
        assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
        world.until("the reap settled", &mut || world.row().is_none());
    });
    assert!(replaced_p_by_s(world.outcome()));
    assert_eq!(world.closes(), 0);
    assert!(
        world
            .armed()
            .iter()
            .all(|armed| armed.reap_responsibility == taken_over_by(LATER))
    );
}

/// A third row S owes at A, for a predecessor that names no flow:
/// `PredecessorMissing`, which stands as typed state; its key.
fn standing_row(world: &ReapWorld) -> String {
    let store = &world.fixture.nexus.store;
    let third = "5ec0d1".to_owned();
    store
        .reap_owed(
            PendingReap {
                predecessor: Predecessor(third.clone()),
                launch_request_id: signal_flow::LaunchRequestId("third-launch".into()),
                address: world.address(),
                reap_standing: ReapStanding::PredecessorMissing,
                reap_responsibility: ReapResponsibility::LaunchSuccessor,
            },
            crate::generated::starter::ReplacedRecord {
                flow_id: FlowId(third.clone()),
                launched_record: crate::generated::starter::LaunchedRecord {
                    flow_id: FlowId(SUCCESSOR.into()),
                    session_id: signal_flow::SessionId(String::new()),
                    origin: signal_flow::Origin::Unbound,
                },
            },
        )
        .unwrap();
    third
}

/// Every row at an Address, keyed by predecessor, as it stands.
fn rows_at(world: &ReapWorld, address: &signal_flow::Address) -> Vec<PendingReap> {
    use crate::store::pending_reap::WritesPendingReap;
    world.fixture.nexus.store.pending_reaps_at(address).unwrap()
}

/// Record.Awoken of A as N over an ended S carries the reassignment of the
/// mixed rows S owed there in its own commit: the pane-owed row TakenOver
/// by N, the store-only row finished, the typed row untouched. Refused, it
/// leaves the metaflow and every row as they stood.
#[test]
fn an_awakening_reassigns_the_mixed_rows_in_its_own_commit() {
    for refused in [true, false] {
        let label = format!("refused: {refused}");
        let world = ReapWorld::staged(&format!("awaken-{refused}"));
        let second = second_row(&world);
        let third = standing_row(&world);
        let store = &world.fixture.nexus.store;
        let address = world.address();
        store.mark_exited(SUCCESSOR);
        let before = store.metaflow(&address).unwrap();
        let rows = rows_at(&world, &address);
        if refused {
            store.faults.refuse_next(RefusedWrite::MetaflowAwaken);
        }
        let awoken = store.awaken_metaflow(&address, LATER, 0);
        if refused {
            assert!(awoken.is_err(), "{label}: {awoken:?}");
            assert_eq!(store.metaflow(&address).unwrap(), before, "{label}");
            assert_eq!(rows_at(&world, &address), rows, "{label}");
            assert!(pending_p_by_s(world.outcome()), "{label}");
            continue;
        }
        assert!(awoken.unwrap(), "{label}");
        assert_eq!(
            store.receiving_flow(&address).unwrap(),
            Some(FlowId(LATER.into()))
        );
        assert_eq!(world.owner(), Some(taken_over_by(LATER)));
        assert!(store.pending_reap(&second).unwrap().is_none());
        assert!(matches!(
            store.launch_outcome("second-launch").unwrap(),
            Some(LaunchOutcome::Replaced(_))
        ));
        let standing = store.pending_reap(&third).unwrap().expect("the typed row");
        assert_eq!(standing.reap_standing, ReapStanding::PredecessorMissing);
        assert_eq!(
            standing.reap_responsibility,
            ReapResponsibility::LaunchSuccessor
        );
    }
}

/// The forgetting of an Address held only through an ended binding carries
/// the reassignment of the mixed rows its bound flow owed there in its own
/// commit: the pane-owed row held by the Address, the store-only row
/// finished, the typed row untouched. Refused, it leaves the metaflow, its
/// binding and every row as they stood.
#[test]
fn forgetting_an_ended_binding_reassigns_the_mixed_rows_in_its_own_commit() {
    use crate::store::metaflow::{ForgetsEndedBindings, MutatesMetaflows};
    use crate::store::{ManagesMetaflowState, ReadsFlowMemory};
    for refused in [true, false] {
        let label = format!("refused: {refused}");
        let world = ReapWorld::staged(&format!("forget-{refused}"));
        let store = &world.fixture.nexus.store;
        let mut address = world.address();
        address.topic = flow_ethos::Topic("forgotten".into());
        let mut peer = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("peer process");
        let process =
            crate::store::FlowStore::process_identity_for_pid(peer.id()).expect("the peer");
        let bound = match store.bind_metaflow(
            signal_flow::Bind_Data {
                address: address.clone(),
                process,
            },
            None,
        ) {
            Ok(Output::Bound(flow_id)) => flow_id,
            other => panic!("{label}: {other:?}"),
        };
        let owed = |predecessor: &str, standing: ReapStanding| {
            store
                .reap_owed(
                    PendingReap {
                        predecessor: Predecessor(predecessor.into()),
                        launch_request_id: signal_flow::LaunchRequestId(format!(
                            "{predecessor}-launch"
                        )),
                        address: address.clone(),
                        reap_standing: standing,
                        reap_responsibility: ReapResponsibility::LaunchSuccessor,
                    },
                    crate::generated::starter::ReplacedRecord {
                        flow_id: FlowId(predecessor.into()),
                        launched_record: crate::generated::starter::LaunchedRecord {
                            flow_id: FlowId("b0b0b0".into()),
                            session_id: signal_flow::SessionId(String::new()),
                            origin: signal_flow::Origin::Unbound,
                        },
                    },
                )
                .unwrap();
        };
        owed("f00001", ReapStanding::Owed);
        owed("f00002", ReapStanding::Owed);
        assert!(
            store
                .reap_refused("f00002", ReapStanding::SettleUnrecorded)
                .unwrap()
        );
        owed("f00003", ReapStanding::PredecessorMissing);
        let held_by_binding = ReapResponsibility::TakenOver(Responsible(bound.clone()));
        for row in rows_at(&world, &address) {
            assert_eq!(
                row.reap_responsibility, held_by_binding,
                "{label}: born owed by the binding"
            );
        }
        peer.kill().expect("peer killed");
        peer.wait().expect("peer reaped");
        let before = store.metaflow(&address).unwrap();
        let rows = rows_at(&world, &address);
        if refused {
            store.faults.refuse_next(RefusedWrite::BindingForget);
        }
        let forgotten = store.forget_ended_binding(&address, &bound.0);
        if refused {
            assert!(forgotten.is_err(), "{label}: {forgotten:?}");
            assert_eq!(store.metaflow(&address).unwrap(), before, "{label}");
            assert!(store.bound_process(&address).unwrap().is_some(), "{label}");
            assert_eq!(rows_at(&world, &address), rows, "{label}");
            continue;
        }
        assert!(forgotten.unwrap(), "{label}");
        assert_eq!(store.metaflow(&address).unwrap(), None);
        assert_eq!(
            store
                .pending_reap("f00001")
                .unwrap()
                .map(|row| row.reap_responsibility),
            Some(ReapResponsibility::AddressHeld)
        );
        assert!(store.pending_reap("f00002").unwrap().is_none());
        assert!(matches!(
            store.launch_outcome("f00002-launch").unwrap(),
            Some(LaunchOutcome::Replaced(_))
        ));
        assert_eq!(
            store
                .pending_reap("f00003")
                .unwrap()
                .map(|row| row.reap_responsibility),
            Some(held_by_binding)
        );
    }
}
