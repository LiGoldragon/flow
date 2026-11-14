//! What a launch owes once its successor is confirmed and receiving: the
//! reap of a replacement's predecessor, and the handover a fresh start
//! takes over. Each is a durable obligation, settled on observed events.
//!
//! A reap has three steps. The launch's own gated call takes the first:
//! W3, one commit that records the predecessor Stopped, the launch's
//! outcome `ReapPending` and the PendingReap at `Owed`, and answers at
//! once. No gated path of a reap calls Herdr. The reap worker (the pane
//! watch, below) takes the second outside the dispatch gate: it subscribes
//! to Herdr's close events, reads the predecessor's pane, closes it once
//! when it is still the predecessor's, and waits for Herdr to show it
//! gone. The third, under the gate after revalidation, closes the
//! handover generation for the successor when the Address still receives
//! as it, records `Replaced` and retracts the row. No worker write is a
//! routing write: which flow an Address receives as is written by the
//! launch's acknowledgement, confirmation, give-back and end alone.
//!
//! A pane is counted gone only on a positively observed absence: the
//! worker's own snapshot, with both its agent roster and its pane list, or
//! Herdr's own close event. A route recorded Unavailable is no
//! observation; it keeps the reap at `HerdrUnavailable` and settles it only
//! on independent evidence that the predecessor's resource is closed or
//! retired, its row `Retired` or `Exited`.
//!
//! One close rule serves every worker, whoever owes its reap: the
//! predecessor's recorded pane is closed only when a fresh read shows it
//! `PaneOwnership::Owned` (its harness reports the recorded session, or a
//! Codex foreground process resumes the recorded thread) or `Bare` (the
//! recorded pane and terminal with no agent on them). `Foreign`, `Unproven`
//! and `Unidentified` are never closed and not counted gone; `Absent`
//! settles; `Unknown` neither closes nor settles. Two boundaries are taken
//! knowingly. The snapshot-to-close race: between the read and the close
//! the pane can change (a new harness started in its shell), and Herdr
//! offers no compare-and-close, so the close is made on the read's
//! evidence, as every other close Flow makes is. The Bare-shell boundary:
//! a Bare pane at the predecessor's own terminal is closed even when a
//! person works in its plain shell, since a harness that exits leaves its
//! pane Bare and Herdr announces no going for it.
//!
//! Who owes a reap is recorded on its row (`ReapResponsibility`) and
//! follows the Address's receiving flow: the commit that ends a flow, and
//! the one that makes another flow the receiving one, reassigns it
//! (store `reaps_reassigned`). A worker is armed for one identity, the
//! row's launch and its responsibility; a reassignment cancels it
//! (`ObligationReassigned`), or, if it revalidates first, finds itself
//! stale (`Stale`) and writes nothing; the pass arms one worker under the
//! new identity.
//!
//! Every action answers an event: a launch, the successor's rest, the
//! receiving flow's own handover write, the Nexus start, a Herdr event,
//! the worker's opening snapshot, or a worker's own completion, on which
//! the supervisor joins that worker. Nothing schedules itself; a refused write
//! is no event; a missing owner or predecessor stands as typed state and
//! no event retries it.

use crate::HoldsStopRequest;
use crate::RunningNexus;
use crate::generated::memory::{Predecessor, ReapResponsibility};
use crate::herdr::{
    PaneClose, PaneOwnership, ReadsHerdrUntilCancelled,
    events::{HerdrCue, HerdrEvents, SubscribesHerdrEvents},
};
use crate::observation_lifetime::{
    CancelsObservation, ObservationCancellation, ObservationCancellationCause,
};
use crate::observe_agent::{PaneEvent, ReadsPaneEvent};
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::handover::{ClosesHandovers, HoldsHandovers, JudgesHandoverOwner};
use crate::store::metaflow::{ReadsSuccession, SleepsMetaflows};
use crate::store::pending_handover::RecordsPendingHandovers;
use crate::store::pending_reap::{
    DerivedReap, JudgesReapStep, NamesReapedRecord, ReapOwner, ReapStep, RecordsPendingReaps,
};
use crate::store::{
    AnnouncesLaunchChanges, LaunchedRecord, NamesLiveFlow, ReadsFlowRows, ReadsLaunchAttempt,
    RecordsFlowLifecycle, RecordsLaunchOutcome, RecordsReplacement, ReplacedRecord, Replacement,
};
use crate::{PendingHandover, PendingReap};
use signal_flow::{
    Address, FlowId, FlowLifecycle, FlowNode, HandoverCloseRefusal, HandoverOwnerRefusal,
    HerdrRoute, HerdrRouteSelection, Launched, Output, ReapStanding, Replaced,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{Scope, ScopedJoinHandle};

/// The three steps of a reap, the first and last under the dispatch gate
/// the caller holds; each touches the store alone.
pub(crate) trait ReapsPredecessors {
    /// Step 1, W3, after the successor's confirmation: answered as soon as
    /// the obligation is durable, `Owed`; `Unrecorded` when its commit is
    /// refused, `PredecessorMissing` or `PredecessorUnknown` as the
    /// predecessor's row reads.
    fn owe_reap(&self, replacement: &Replacement, launched: &Launched) -> Output;
    /// W3 for the reap no row records but the rows imply.
    fn owe_derived_reap(&self, derived: &DerivedReap) -> Output;
    /// The reap of `predecessor` taken up at a bearing event, in the store
    /// alone: W3 for a derived reap, the predecessor read again at
    /// `PredecessorUnknown`, step 3 again where the pane was witnessed
    /// gone, a rearm of the worker where the pane step is owed; a row at
    /// `PredecessorMissing` is answered as it stands.
    fn retry_reap(&self, predecessor: &str) -> Option<Output>;
    /// Step 3: the receiving flow read afresh decides the generation.
    fn settle_reap(&self, predecessor: &str) -> Option<Output>;
    /// The reply a PendingReap reads as.
    fn reap_reply(&self, row: &PendingReap) -> Output;
    /// Asks for the reap worker of `predecessor` to be armed again.
    fn rearm(&self, predecessor: &str);
}

/// The fresh-start handover and its recovery, under the dispatch gate the
/// caller holds.
pub(crate) trait HandsOverGenerations {
    /// The settlement of a fresh launch whose flow is confirmed and
    /// receiving: C1 (`StartedHandedOver`) when the handover's owner has
    /// ended or nothing moves, else, or when C1 is refused, C2
    /// (`HandoverOwed`).
    fn settle_fresh(
        &self,
        launch_request_id: &str,
        address: &Address,
        started: &Launched,
    ) -> Output;
    /// The handover owed at `address` taken up at a bearing event.
    fn retry_handover(&self, address: &Address) -> Option<Output>;
    /// The handover a receiving flow is owed and no row records: the row
    /// at the Address names another owner, no PendingHandover names the
    /// receiving flow, and no reap carries it. The receiving flow and its
    /// launch.
    fn derived_handover(&self, address: &Address) -> Option<(FlowId, String)>;
    /// The flow the Address's lineage names before `flow_id`.
    fn predecessor_before(&self, address: &Address, flow_id: &str) -> Option<String>;
}

/// The bearing events that take obligations up, each once.
pub(crate) trait TakesUpObligations {
    /// The flow's rest, under the gate: each reap whose successor it is,
    /// and the handover at its Address when it is the receiving flow.
    fn take_up_at_rest(&self, flow_id: &str);
    /// The receiving flow's own handover write, under the gate, before the
    /// write: the handover owed at its Address.
    fn take_up_at_handover_write(&self, address: &Address, flow_id: &str);
    /// The Nexus start, one pass under the gate: W3 for each derived reap,
    /// C1/C2 for each receiving flow whose fresh launch has no outcome.
    fn recover_obligations(&self);
}

/// Predecessors whose lapsed or waiting worker a bearing event asked to
/// arm again.
#[derive(Debug, Default)]
pub struct PaneWatchRearms(Mutex<BTreeSet<String>>);

trait HoldsRearms {
    fn ask(&self, predecessor: &str);
    fn taken(&self, predecessor: &str) -> bool;
}

impl HoldsRearms for PaneWatchRearms {
    fn ask(&self, predecessor: &str) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(predecessor.to_owned());
    }

    fn taken(&self, predecessor: &str) -> bool {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(predecessor)
    }
}

/// Independent evidence that a predecessor's resource is closed or retired:
/// its row records it Retired or Exited, written by another event than this
/// reap.
trait EvidencesClosedResource {
    fn closed_elsewhere(&self) -> bool;
}

impl EvidencesClosedResource for FlowNode {
    fn closed_elsewhere(&self) -> bool {
        matches!(
            self.flow_lifecycle,
            FlowLifecycle::Retired | FlowLifecycle::Exited
        )
    }
}

impl ReapsPredecessors for RunningNexus {
    fn owe_reap(&self, replacement: &Replacement, launched: &Launched) -> Output {
        let predecessor = replacement.predecessor.0.clone();
        let launch_request_id = replacement.launch_request_id.clone();
        let address = self
            .store
            .launch_attempt(&launch_request_id.0)
            .ok()
            .flatten()
            .map(|attempt| attempt.launch_profile.address);
        let replaced = ReplacedRecord {
            flow_id: FlowId(predecessor.clone()),
            launched_record: LaunchedRecord::from(launched),
        };
        let Some(address) = address else {
            // A confirmed launch always has its attempt; without it nothing
            // of the reap can be named, and it stands derived.
            return Output::ReapPending(self.store_reap_reply(
                &replaced,
                None,
                ReapStanding::Unrecorded,
            ));
        };
        let standing = match self.store.judged_lifecycle(&predecessor) {
            Ok(Some(_)) => ReapStanding::Owed,
            Ok(None) => ReapStanding::PredecessorMissing,
            Err(_) => ReapStanding::PredecessorUnknown,
        };
        let row = PendingReap {
            predecessor: Predecessor(predecessor.clone()),
            launch_request_id: launch_request_id.clone(),
            address: address.clone(),
            reap_standing: standing.clone(),
            reap_responsibility: ReapResponsibility::LaunchSuccessor,
        };
        match self.store.reap_owed(row, replaced.clone()) {
            Ok(()) => {
                if standing != ReapStanding::Owed {
                    eprintln!(
                        "flow-nexus: the reap of {predecessor} for {} (launch {}) is pending: {standing:?}",
                        launched.flow_id.0, launch_request_id.0
                    );
                }
                Output::ReapPending(signal_flow::ReapPending {
                    address,
                    predecessor: signal_flow::Predecessor(FlowId(predecessor)),
                    launched: launched.clone(),
                    reap_standing: standing,
                })
            }
            Err(error) => {
                eprintln!(
                    "flow-nexus: the reap of {predecessor} for {} (launch {}) is owed and unrecorded: {error}",
                    launched.flow_id.0, launch_request_id.0
                );
                Output::ReapPending(signal_flow::ReapPending {
                    address,
                    predecessor: signal_flow::Predecessor(FlowId(predecessor)),
                    launched: launched.clone(),
                    reap_standing: ReapStanding::Unrecorded,
                })
            }
        }
    }

    fn owe_derived_reap(&self, derived: &DerivedReap) -> Output {
        let replaced = derived.replaced_record();
        let launched = self.launched_now(&replaced.launched_record);
        self.owe_reap(&derived.replacement, &launched)
    }

    fn retry_reap(&self, predecessor: &str) -> Option<Output> {
        let row = match self.store.pending_reap(predecessor) {
            Ok(row) => row,
            Err(error) => {
                eprintln!("flow-nexus: the reap of {predecessor} could not be read: {error}");
                return None;
            }
        };
        let Some(row) = row else {
            let derived = self
                .store
                .derived_reaps()
                .ok()?
                .into_iter()
                .find(|derived| derived.replacement.predecessor.0 == predecessor)?;
            return Some(self.owe_derived_reap(&derived));
        };
        match row.reap_standing.reap_step() {
            // [PSYCHE-2]: stands as typed state; no event retries it.
            ReapStep::Stands => Some(self.reap_reply(&row)),
            ReapStep::PredecessorReading => {
                let standing = match self.store.judged_lifecycle(predecessor) {
                    Ok(Some(lifecycle)) => {
                        if lifecycle.is_live() && self.store.record_stopped(predecessor).is_err() {
                            return Some(self.reap_reply(&row));
                        }
                        ReapStanding::Owed
                    }
                    Ok(None) => ReapStanding::PredecessorMissing,
                    Err(_) => return Some(self.reap_reply(&row)),
                };
                let rearm = standing == ReapStanding::Owed;
                if self.store.reap_refused(predecessor, standing).is_ok() && rearm {
                    self.rearm(predecessor);
                }
                self.current_reap_reply(predecessor)
            }
            // The pane was already witnessed gone: step 3 again, in the
            // store alone.
            ReapStep::StoreSettlement => self.settle_reap(predecessor),
            ReapStep::PaneClose => {
                // A predecessor whose route is not recorded has no pane to
                // watch: only independent evidence settles it.
                if let Ok(Some(node)) = self.store.flow_node(predecessor)
                    && matches!(node.herdr_route_selection, HerdrRouteSelection::Unavailable)
                {
                    if node.closed_elsewhere() {
                        return self.settle_reap(predecessor);
                    }
                    return Some(self.reap_reply(&row));
                }
                self.rearm(predecessor);
                Some(self.reap_reply(&row))
            }
        }
    }

    fn settle_reap(&self, predecessor: &str) -> Option<Output> {
        let row = self.store.pending_reap(predecessor).ok()??;
        let replaced = self.store.reaped_record(&row).ok()??;
        let successor = replaced.launched_record.flow_id.clone();
        // The Address's receiving flow read afresh decides the generation:
        // the successor's only while the Address still receives as it.
        let receiving = match self.store.receiving_flow(&row.address) {
            Ok(receiving) => receiving,
            Err(_) => {
                let _ = self
                    .store
                    .reap_refused(predecessor, ReapStanding::SettleUnrecorded);
                return self.current_reap_reply(predecessor);
            }
        };
        let closing = if receiving.as_ref() == Some(&successor) {
            match self
                .store
                .handover_closing(&row.address, predecessor, &successor.0)
            {
                Ok(closing) => closing,
                Err(refusal) => {
                    if let Err(error) = self
                        .store
                        .reap_refused(predecessor, ReapStanding::HandoverRefused(refusal.clone()))
                    {
                        eprintln!(
                            "flow-nexus: the reap of {predecessor} is pending at {refusal:?}; its rewrite was refused: {error}"
                        );
                    }
                    return self.current_reap_reply(predecessor);
                }
            }
        } else {
            self.store.nothing_closing()
        };
        match self.store.reaped(predecessor, closing) {
            Ok(()) => Some(Output::Replaced(Replaced {
                flow_id: replaced.flow_id.clone(),
                launched: self.launched_now(&replaced.launched_record),
            })),
            Err(error) => {
                eprintln!(
                    "flow-nexus: the reap of {predecessor} for {} is pending: its settlement was refused: {error}",
                    successor.0
                );
                let _ = self
                    .store
                    .reap_refused(predecessor, ReapStanding::SettleUnrecorded);
                self.current_reap_reply(predecessor)
            }
        }
    }

    fn reap_reply(&self, row: &PendingReap) -> Output {
        let replaced = self.store.reaped_record(row).ok().flatten();
        match replaced {
            Some(replaced) => Output::ReapPending(self.store_reap_reply(
                &replaced,
                Some(row.address.clone()),
                row.reap_standing.clone(),
            )),
            None => Output::ReapPending(signal_flow::ReapPending {
                address: row.address.clone(),
                predecessor: signal_flow::Predecessor(FlowId(row.predecessor.0.clone())),
                launched: Launched {
                    flow_id: FlowId(String::new()),
                    session_id: signal_flow::SessionId(String::new()),
                    herdr_route_selection: HerdrRouteSelection::Unavailable,
                    origin: signal_flow::Origin::Unbound,
                },
                reap_standing: row.reap_standing.clone(),
            }),
        }
    }

    fn rearm(&self, predecessor: &str) {
        self.pane_watch_rearms.ask(predecessor);
        self.store.launch_changes.announce();
    }
}

/// The reply pieces a reap reads with.
trait NamesReapReply {
    fn store_reap_reply(
        &self,
        replaced: &ReplacedRecord,
        address: Option<Address>,
        standing: ReapStanding,
    ) -> signal_flow::ReapPending;
    fn current_reap_reply(&self, predecessor: &str) -> Option<Output>;
    /// A launched record made a wire `Launched`, its route as its row
    /// holds it now.
    fn launched_now(&self, record: &LaunchedRecord) -> Launched;
}

impl NamesReapReply for RunningNexus {
    fn store_reap_reply(
        &self,
        replaced: &ReplacedRecord,
        address: Option<Address>,
        standing: ReapStanding,
    ) -> signal_flow::ReapPending {
        use crate::store::NamesPendingReplies;
        let mut reply = self.store.reap_pending_reply(replaced);
        if let Some(address) = address {
            reply.address = address;
        }
        reply.reap_standing = standing;
        reply
    }

    fn current_reap_reply(&self, predecessor: &str) -> Option<Output> {
        // A reap settled meanwhile has no row: its outcome answers.
        self.store
            .pending_reap(predecessor)
            .ok()?
            .map(|row| self.reap_reply(&row))
    }

    fn launched_now(&self, record: &LaunchedRecord) -> Launched {
        use crate::generated::starter::NamesLaunchedRoute;
        self.store.launched(record)
    }
}

impl HandsOverGenerations for RunningNexus {
    fn settle_fresh(
        &self,
        launch_request_id: &str,
        address: &Address,
        started: &Launched,
    ) -> Output {
        let launched = LaunchedRecord::from(started);
        let flow_id = started.flow_id.0.clone();
        let predecessor = self
            .predecessor_before(address, &flow_id)
            .unwrap_or_default();
        let owner = || {
            self.store
                .handover_memory(address)
                .ok()
                .flatten()
                .and_then(|row| row.flow_id_option)
                .map(|flow| flow.0)
                .unwrap_or_else(|| predecessor.clone())
        };
        let refusal = match self.store.handover_closing(address, &predecessor, &flow_id) {
            Ok(closing) => match self.store.started_handed_over(
                launch_request_id,
                address,
                launched.clone(),
                closing,
            ) {
                Ok(()) => return Output::Started(started.clone()),
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the handover at {address:?} for {flow_id} (launch {launch_request_id}) was not settled: {error}"
                    );
                    HandoverCloseRefusal::PersistenceRefused
                }
            },
            Err(refusal) => refusal,
        };
        let owner = match &refusal {
            HandoverCloseRefusal::Owner(
                HandoverOwnerRefusal::Live(owner)
                | HandoverOwnerRefusal::Missing(owner)
                | HandoverOwnerRefusal::Unknown(owner),
            ) => owner.0.clone(),
            HandoverCloseRefusal::PersistenceRefused => owner(),
        };
        let pending = PendingHandover {
            address: address.clone(),
            handover_owner: signal_flow::HandoverOwner(FlowId(owner.clone())),
            successor: signal_flow::Successor(started.flow_id.clone()),
            launch_request_id: signal_flow::LaunchRequestId(launch_request_id.into()),
            handover_close_refusal: refusal.clone(),
        };
        if let Err(error) = self.store.handover_owed(pending, launched) {
            eprintln!(
                "flow-nexus: the handover at {address:?} for {flow_id} (launch {launch_request_id}) is owed and unrecorded: {refusal:?}: {error}"
            );
        }
        Output::HandoverPending(signal_flow::HandoverPending {
            address: address.clone(),
            handover_owner: signal_flow::HandoverOwner(FlowId(owner)),
            launched: started.clone(),
            handover_close_refusal: refusal,
        })
    }

    fn retry_handover(&self, address: &Address) -> Option<Output> {
        use crate::store::NamesPendingReplies;
        let receiving = self.store.receiving_flow(address).ok()??;
        let row = self.store.pending_handover(address).ok()?;
        if let Some(row) = row.filter(|row| row.successor.0 == receiving) {
            // [PSYCHE-2]: a missing owner stands; nothing is written.
            if matches!(
                row.handover_close_refusal,
                HandoverCloseRefusal::Owner(HandoverOwnerRefusal::Missing(_))
            ) {
                return self.handover_reply(&row);
            }
            let predecessor = self
                .predecessor_before(address, &receiving.0)
                .unwrap_or_else(|| row.handover_owner.0.0.clone());
            match self
                .store
                .handover_closing(address, &predecessor, &receiving.0)
            {
                Ok(closing) => match self.store.handed_over(address, closing) {
                    Ok(()) => {
                        return self
                            .store
                            .launch_outcome(&row.launch_request_id.0)
                            .ok()
                            .flatten()
                            .map(|outcome| {
                                crate::store::AnswersLaunchOutcome::response(&outcome, &self.store)
                            });
                    }
                    Err(error) => {
                        eprintln!(
                            "flow-nexus: the handover at {address:?} for {} is pending: its settlement was refused: {error}",
                            receiving.0
                        );
                        let _ = self
                            .store
                            .handover_refused(address, HandoverCloseRefusal::PersistenceRefused);
                    }
                },
                Err(refusal) => {
                    let _ = self.store.handover_refused(address, refusal);
                }
            }
            let row = self.store.pending_handover(address).ok()??;
            return Some(Output::HandoverPending(
                self.store.handover_pending_reply(&LaunchedRecord {
                    flow_id: row.successor.0.clone(),
                    session_id: self
                        .store
                        .flow_node(&row.successor.0.0)
                        .ok()
                        .flatten()
                        .map(|node| node.session_id)
                        .unwrap_or(signal_flow::SessionId(String::new())),
                    origin: signal_flow::Origin::Unbound,
                }),
            ));
        }
        let (flow_id, launch_request_id) = self.derived_handover(address)?;
        let node = self.store.flow_node(&flow_id.0).ok()??;
        let launched = Launched {
            flow_id: node.flow_id.clone(),
            session_id: node.session_id.clone(),
            herdr_route_selection: node.herdr_route_selection.clone(),
            origin: node.origin.clone(),
        };
        Some(self.settle_fresh(&launch_request_id, address, &launched))
    }

    fn derived_handover(&self, address: &Address) -> Option<(FlowId, String)> {
        use crate::store::pending_handover::DerivesOwedHandover;
        self.store
            .derived_handover(address)
            .ok()
            .flatten()
            .map(|(flow_id, launch_request_id, _)| (flow_id, launch_request_id))
    }

    fn predecessor_before(&self, address: &Address, flow_id: &str) -> Option<String> {
        let metaflow = self.store.metaflow(address).ok()??;
        metaflow
            .past
            .0
            .iter()
            .rev()
            .find(|past| past.0 != flow_id)
            .map(|past| past.0.clone())
    }
}

/// The reply a PendingHandover reads as.
trait NamesHandoverReply {
    fn handover_reply(&self, row: &PendingHandover) -> Option<Output>;
}

impl NamesHandoverReply for RunningNexus {
    fn handover_reply(&self, row: &PendingHandover) -> Option<Output> {
        let node = self.store.flow_node(&row.successor.0.0).ok()??;
        Some(Output::HandoverPending(signal_flow::HandoverPending {
            address: row.address.clone(),
            handover_owner: row.handover_owner.clone(),
            launched: Launched {
                flow_id: node.flow_id,
                session_id: node.session_id,
                herdr_route_selection: node.herdr_route_selection,
                origin: node.origin,
            },
            handover_close_refusal: row.handover_close_refusal.clone(),
        }))
    }
}

/// The fresh launch a receiving flow has no outcome for: a crash after its
/// confirmation, or its C1 and C2 both refused.
trait FindsUnsettledFreshLaunch {
    /// The receiving flow of `address` and its fresh launch, when the
    /// launch's outcome, its Replacement and the flow's hold each read
    /// absent and the flow is past Pending; None on any read refused.
    fn unsettled_fresh_launch(&self, address: &Address) -> Option<(String, Launched)>;
}

impl FindsUnsettledFreshLaunch for RunningNexus {
    fn unsettled_fresh_launch(&self, address: &Address) -> Option<(String, Launched)> {
        let Ok(Some(receiving)) = self.store.receiving_flow(address) else {
            return None;
        };
        let Ok(Some(launch_request_id)) = self
            .store
            .launch_requests_bound_to(&receiving.0)
            .map(|mut ids| ids.pop())
        else {
            return None;
        };
        if !matches!(self.store.launch_outcome(&launch_request_id), Ok(None))
            || !matches!(self.store.replacement(&launch_request_id), Ok(None))
            || !matches!(self.store.address_hold(&receiving.0), Ok(None))
        {
            return None;
        }
        let Ok(Some(node)) = self.store.flow_node(&receiving.0) else {
            return None;
        };
        if node.flow_lifecycle == FlowLifecycle::Pending {
            return None;
        }
        Some((
            launch_request_id,
            Launched {
                flow_id: node.flow_id.clone(),
                session_id: node.session_id.clone(),
                herdr_route_selection: node.herdr_route_selection.clone(),
                origin: node.origin.clone(),
            },
        ))
    }
}

/// The predecessors of the pending reaps `owner` owes, as their rows read
/// now; empty when they cannot be read.
trait ReadsOwedReaps {
    fn reaps_owed_by(&self, owner: &ReapOwner, address: Option<&Address>) -> BTreeSet<String>;
}

impl ReadsOwedReaps for RunningNexus {
    fn reaps_owed_by(&self, owner: &ReapOwner, address: Option<&Address>) -> BTreeSet<String> {
        self.store
            .pending_reaps()
            .unwrap_or_default()
            .into_iter()
            .filter(|row| address.is_none_or(|address| &row.address == address))
            .filter(|row| matches!(self.store.reap_owner(row), Ok(Some(found)) if found == *owner))
            .map(|row| row.predecessor.0)
            .collect()
    }
}

impl TakesUpObligations for RunningNexus {
    fn take_up_at_rest(&self, flow_id: &str) {
        let resting = ReapOwner::Flow(FlowId(flow_id.to_owned()));
        // The reaps this flow owed before its own settlement, if any, runs:
        // a row that settlement takes over gets its worker from the
        // takeover itself and is not rearmed here.
        let owed = self.reaps_owed_by(&resting, None);
        // a. Its own unrecorded settlement, once: W3 for its derived reap,
        // or C1/C2 for its fresh launch with no outcome. Either carries the
        // takeover of what its Address owes.
        let mut settled_fresh = false;
        for derived in self.store.derived_reaps().unwrap_or_default() {
            if derived.successor.0 == flow_id {
                let answer = self.owe_derived_reap(&derived);
                Self::log_obligation(flow_id, &answer);
            }
        }
        let awake = self.store.metaflow_awake_as(flow_id).ok().flatten();
        if let Some(metaflow) = &awake
            && let Some((launch_request_id, launched)) =
                self.unsettled_fresh_launch(&metaflow.address)
            && launched.flow_id.0 == flow_id
        {
            settled_fresh = true;
            let answer = self.settle_fresh(&launch_request_id, &metaflow.address, &launched);
            Self::log_obligation(flow_id, &answer);
        }
        // b. Each PendingReap this flow owes: its successor's, or one it
        // took over.
        for predecessor in &owed {
            let still_owed = self
                .store
                .pending_reap(predecessor)
                .ok()
                .flatten()
                .is_some_and(|row| matches!(self.store.reap_owner(&row), Ok(Some(found)) if found == resting));
            if still_owed && let Some(answer) = self.retry_reap(predecessor) {
                Self::log_obligation(flow_id, &answer);
            }
        }
        // c. The handover at its Address, when it is the receiving flow;
        // one settlement attempt per rest, so not after (a) ran one.
        if !settled_fresh
            && let Some(metaflow) = &awake
            && let Some(answer) = self.retry_handover(&metaflow.address)
        {
            Self::log_obligation(flow_id, &answer);
        }
    }

    fn take_up_at_handover_write(&self, address: &Address, flow_id: &str) {
        if !matches!(
            self.store.receiving_flow(address),
            Ok(Some(receiving)) if receiving.0 == flow_id
        ) {
            return;
        }
        if let Some(answer) = self.retry_handover(address) {
            Self::log_obligation(flow_id, &answer);
        }
    }

    fn recover_obligations(&self) {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // The derived owed reaps: a refused W3, or a crash between the
        // successor's confirmation and W3.
        for derived in self.store.derived_reaps().unwrap_or_default() {
            let answer = self.owe_derived_reap(&derived);
            Self::log_obligation(&derived.successor.0, &answer);
        }
        // Each receiving flow whose fresh launch has no outcome: a crash
        // after its confirmation, or a refused C2.
        let metaflows =
            crate::store::ManagesMetaflowState::list_metaflows(&*self.store).unwrap_or_default();
        for metaflow in metaflows {
            let Some((launch_request_id, launched)) =
                self.unsettled_fresh_launch(&metaflow.address)
            else {
                continue;
            };
            let answer = self.settle_fresh(&launch_request_id, &metaflow.address, &launched);
            Self::log_obligation(&launched.flow_id.0, &answer);
        }
        // The Nexus start is the bearing event of what no flow owes: each
        // reap its Address holds is taken up once, in the store alone, with
        // no Herdr call. A store-only settlement (its worker's own commit
        // was refused) is settled from the closure its standing records; a
        // predecessor that could not be read is read once more. A refusal
        // is no event: nothing retries it before a later bearing event, and
        // Unknown and Missing stand unless this read changes them. A row
        // whose pane step is owed is the supervisor's first pass's.
        for predecessor in self.reaps_owed_by(&ReapOwner::Address, None) {
            let Ok(Some(row)) = self.store.pending_reap(&predecessor) else {
                continue;
            };
            let answer = match row.reap_standing.reap_step() {
                ReapStep::StoreSettlement => self.settle_reap(&predecessor),
                ReapStep::PredecessorReading => self.retry_reap(&predecessor),
                ReapStep::PaneClose | ReapStep::Stands => None,
            };
            if let Some(answer) = answer {
                Self::log_obligation(&predecessor, &answer);
            }
        }
    }
}

/// One log line per obligation answer that is not settled.
trait LogsObligation {
    fn log_obligation(flow_id: &str, answer: &Output);
}

impl LogsObligation for RunningNexus {
    fn log_obligation(flow_id: &str, answer: &Output) {
        match answer {
            Output::ReapPending(pending) if pending.reap_standing != ReapStanding::Owed => {
                eprintln!(
                    "flow-nexus: the reap of {} for {flow_id} is pending: {:?}",
                    pending.predecessor.0.0, pending.reap_standing
                );
            }
            Output::HandoverPending(pending) => {
                eprintln!(
                    "flow-nexus: the handover at {:?} for {flow_id} is pending: {:?}",
                    pending.address, pending.handover_close_refusal
                );
            }
            _ => {}
        }
    }
}

/// What a worker was armed for: its identity, the row's launch and who
/// owes it, and the route it watches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ArmedReap {
    pub(crate) launch_request_id: signal_flow::LaunchRequestId,
    /// Who owed the row when the worker was armed.
    pub(crate) reap_responsibility: ReapResponsibility,
    /// The route recorded for the predecessor when the worker was armed;
    /// None when its row records none.
    pub(crate) route: Option<HerdrRoute>,
}

/// Whether a row is the obligation a worker was armed for.
trait IdentifiesArmedReap {
    fn armed_for(&self, row: &PendingReap) -> bool;
}

impl IdentifiesArmedReap for ArmedReap {
    fn armed_for(&self, row: &PendingReap) -> bool {
        row.launch_request_id == self.launch_request_id
            && row.reap_responsibility == self.reap_responsibility
    }
}

/// How a worker ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaneWatchEnd {
    /// It took the gate and acted.
    Acted,
    /// It revalidated and found its obligation settled, retracted or owed
    /// by another: it wrote nothing.
    Stale,
    Cancelled(ObservationCancellationCause),
    Lapsed(PaneWatchLapse),
}

/// Why a worker stopped waiting with its obligation still standing. A
/// lapsed key is armed again only by a bearing event's rearm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaneWatchLapse {
    SessionUnknown,
    SubscriptionRefused(String),
    StreamClosedPanePresent,
    /// The worker acted and its row still stands: its write was refused.
    ActedRowStands,
    /// The predecessor's row names another route than the one watched.
    RouteChanged,
    /// The predecessor's row records no route: nothing to observe.
    RouteUnrecorded,
    /// The worker thread panicked; its join answered Err.
    Panicked,
    /// Its revalidation could not read the row: nothing was written, and
    /// the obligation keeps its standing until a bearing event rearms it.
    StoreUnreadable,
}

/// The serial of one worker the supervisor spawned, counted from zero over
/// the supervisor's life.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PaneWatchSpawnSerial(u64);

/// One worker: the predecessor it watches for and the spawn that made it.
/// Its completion names it, so the supervisor joins that very handle and
/// no later worker armed for the same predecessor.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PaneWatchWorkerIdentity {
    pub(crate) predecessor: String,
    pub(crate) spawn: PaneWatchSpawnSerial,
}

enum PaneWatchEntry<'scope> {
    Watching {
        armed: ArmedReap,
        worker: PaneWatchWorkerIdentity,
        cancellation: Arc<ObservationCancellation>,
        thread: ScopedJoinHandle<'scope, PaneWatchEnd>,
    },
    /// A worker that ended with its obligation standing, and what it was
    /// armed for: armed again only by a bearing event's rearm, or by a
    /// reassignment that gives its row another identity.
    Lapsed {
        armed: ArmedReap,
        lapse: PaneWatchLapse,
    },
}

/// The supervisor's entries, one per obligation, keyed by predecessor, and
/// the queue each worker announces its completion on.
pub(crate) struct ReapedPaneWatches<'scope> {
    watches: BTreeMap<String, PaneWatchEntry<'scope>>,
    /// Each worker's identity, sent as the last thing its thread does.
    completed: mpsc::Sender<PaneWatchWorkerIdentity>,
    completions: mpsc::Receiver<PaneWatchWorkerIdentity>,
    next_spawn: PaneWatchSpawnSerial,
}

impl Default for ReapedPaneWatches<'_> {
    fn default() -> Self {
        let (completed, completions) = mpsc::channel();
        Self {
            watches: BTreeMap::new(),
            completed,
            completions,
            next_spawn: PaneWatchSpawnSerial::default(),
        }
    }
}

/// A worker's completion, announced when its thread leaves its body, by
/// return or by unwind: its identity is queued, then `launch_changes` is
/// announced, the event the supervisor waits on. The queue is filled before
/// the announcement and the supervisor reads its count before it drains the
/// queue, so a completion landing between the supervisor's drain and its
/// wait moves the count past what it saw and its wait returns at once.
struct PaneWatchCompletion<'env> {
    worker: PaneWatchWorkerIdentity,
    completed: mpsc::Sender<PaneWatchWorkerIdentity>,
    nexus: &'env RunningNexus,
}

impl Drop for PaneWatchCompletion<'_> {
    fn drop(&mut self) {
        // The receiver lives until the supervisor has joined every worker.
        let _ = self.completed.send(self.worker.clone());
        self.nexus.store.launch_changes.announce();
        #[cfg(test)]
        self.nexus.pane_watch_hooks.worker_ended();
    }
}

/// The one way a worker's handle is joined: the join is the proof it ended.
trait JoinsPaneWatch {
    fn join_pane_watch(
        &self,
        worker: &PaneWatchWorkerIdentity,
        thread: ScopedJoinHandle<'_, PaneWatchEnd>,
    ) -> PaneWatchEnd;
}

impl JoinsPaneWatch for RunningNexus {
    fn join_pane_watch(
        &self,
        worker: &PaneWatchWorkerIdentity,
        thread: ScopedJoinHandle<'_, PaneWatchEnd>,
    ) -> PaneWatchEnd {
        let end = thread
            .join()
            .unwrap_or(PaneWatchEnd::Lapsed(PaneWatchLapse::Panicked));
        #[cfg(test)]
        self.pane_watch_hooks.worker_joined(worker, &end);
        #[cfg(not(test))]
        let _ = worker;
        end
    }
}

pub trait ServesReapedPaneWatches {
    /// One supervisor for the Nexus's life: arms one worker per PendingReap
    /// whose pane step is owed, woken only by `launch_changes`, which every
    /// obligation write, every rearm and every worker's completion announce.
    fn serve_reaped_pane_watches<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>);
}

/// The worker's body and its gated writes.
pub(crate) trait WatchesReapedPane {
    fn watch_reaped_pane(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
    ) -> PaneWatchEnd;
    /// Takes the gate, revalidates, and writes the standing; whether the
    /// worker goes on.
    fn worker_rewrite(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
        standing: ReapStanding,
    ) -> Result<(), PaneWatchEnd>;
    /// Step 6: takes the gate, revalidates, settles.
    fn worker_act(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
    ) -> PaneWatchEnd;
}

/// One pass of the supervisor and its stop.
pub(crate) trait PassesPaneWatches<'scope> {
    fn pass<'env>(&mut self, nexus: &'env RunningNexus, scope: &'scope Scope<'scope, 'env>)
    where
        'env: 'scope;
    fn stop_all(&mut self, nexus: &RunningNexus);
}

impl<'scope> PassesPaneWatches<'scope> for ReapedPaneWatches<'scope> {
    fn pass<'env>(&mut self, nexus: &'env RunningNexus, scope: &'scope Scope<'scope, 'env>)
    where
        'env: 'scope,
    {
        #[cfg(test)]
        nexus.pane_watch_hooks.at_pass();
        let rows = nexus.store.pending_reaps();
        #[cfg(test)]
        let rows = {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            if nexus.store.faults.refused_now(RefusedWrite::PassRead) {
                Err(crate::store::StoreError::StateInvariant)
            } else {
                rows
            }
        };
        // Each worker that announced its completion is joined first, whether
        // or not the store can be read now: its thread has ended, and its
        // handle is reclaimed at once. A completion naming a handle already
        // joined, by a cancellation, names no entry and joins nothing.
        let completed = self.completions.try_iter().collect::<Vec<_>>();
        for worker in completed {
            let key = worker.predecessor.clone();
            match self.watches.get(&key) {
                Some(PaneWatchEntry::Watching { worker: entry, .. }) if *entry == worker => {}
                _ => continue,
            }
            let Some(PaneWatchEntry::Watching { thread, armed, .. }) = self.watches.remove(&key)
            else {
                continue;
            };
            let end = nexus.join_pane_watch(&worker, thread);
            // A row read absent is settled or retracted; one read standing,
            // or not read at all, keeps its standing as lapsed or unknown
            // until a bearing event.
            let stands = match &rows {
                Ok(rows) => rows.iter().any(|row| row.predecessor.0 == key),
                Err(_) => true,
            };
            let lapse = match end {
                PaneWatchEnd::Cancelled(_) | PaneWatchEnd::Stale => None,
                PaneWatchEnd::Acted if !stands => None,
                PaneWatchEnd::Acted => Some(PaneWatchLapse::ActedRowStands),
                PaneWatchEnd::Lapsed(lapse) => Some(lapse),
            };
            if let Some(lapse) = lapse {
                eprintln!(
                    "flow-nexus: the pane watch of {key} lapsed: {lapse:?}; it is armed again at its owner's next bearing event"
                );
                self.watches
                    .insert(key, PaneWatchEntry::Lapsed { armed, lapse });
            }
        }
        // An unreadable store is no evidence of retraction or reassignment.
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => {
                eprintln!("flow-nexus: the pending reaps could not be read: {error}");
                return;
            }
        };
        // A row whose owner is Missing is nobody's: no worker is armed for
        // it and nothing settles it. An owner that cannot be read is no
        // evidence of anything, as an unreadable store is not.
        let mut wanted: BTreeMap<String, PendingReap> = BTreeMap::new();
        for row in rows {
            if row.reap_standing.reap_step() != ReapStep::PaneClose {
                continue;
            }
            match nexus.store.reap_owner(&row) {
                Ok(Some(_)) => {
                    wanted.insert(row.predecessor.0.clone(), row);
                }
                Ok(None) => {}
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the owner of the reap of {} could not be read: {error}",
                        row.predecessor.0
                    );
                    return;
                }
            }
        }
        // Retraction: a watching entry no longer wanted is cancelled and
        // joined; one whose row names another launch, or is owed by another
        // than it was armed for, is cancelled as reassigned and joined, and
        // the arming below arms one worker under the row's identity now. A
        // lapsed entry no longer wanted, or reassigned, goes too.
        let retracted = self
            .watches
            .iter()
            .filter_map(|(key, entry)| match (entry, wanted.get(key)) {
                (PaneWatchEntry::Watching { .. }, None) => Some((
                    key.clone(),
                    ObservationCancellationCause::ObligationRetracted,
                )),
                (PaneWatchEntry::Watching { armed, .. }, Some(row))
                    if row.launch_request_id != armed.launch_request_id =>
                {
                    Some((
                        key.clone(),
                        ObservationCancellationCause::ObligationRetracted,
                    ))
                }
                (PaneWatchEntry::Watching { armed, .. }, Some(row)) if !armed.armed_for(row) => {
                    Some((
                        key.clone(),
                        ObservationCancellationCause::ObligationReassigned,
                    ))
                }
                (PaneWatchEntry::Lapsed { .. }, None) => Some((
                    key.clone(),
                    ObservationCancellationCause::ObligationRetracted,
                )),
                (PaneWatchEntry::Lapsed { armed, .. }, Some(row)) if !armed.armed_for(row) => {
                    Some((
                        key.clone(),
                        ObservationCancellationCause::ObligationReassigned,
                    ))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        for (key, cause) in retracted {
            match self.watches.remove(&key) {
                Some(PaneWatchEntry::Watching {
                    worker,
                    cancellation,
                    thread,
                    ..
                }) => {
                    cancellation.cancel(cause);
                    #[cfg(test)]
                    if cause == ObservationCancellationCause::ObligationReassigned {
                        nexus.pane_watch_hooks.reassignment_cancelled();
                    }
                    nexus.join_pane_watch(&worker, thread);
                }
                Some(PaneWatchEntry::Lapsed { .. }) | None => {}
            }
        }
        // A rearm asked by a bearing event cancels a waiting worker and
        // arms a lapsed one; a wanted key with no entry gets one worker.
        for (key, row) in &wanted {
            let rearmed = nexus.pane_watch_rearms.taken(key);
            match self.watches.get(key) {
                Some(PaneWatchEntry::Watching { .. }) if rearmed => {
                    if let Some(PaneWatchEntry::Watching {
                        worker,
                        cancellation,
                        thread,
                        ..
                    }) = self.watches.remove(key)
                    {
                        cancellation.cancel(ObservationCancellationCause::Rearmed);
                        nexus.join_pane_watch(&worker, thread);
                    }
                }
                Some(PaneWatchEntry::Lapsed { lapse, .. }) if rearmed => {
                    eprintln!(
                        "flow-nexus: the pane watch of {key}, lapsed {lapse:?}, is armed again"
                    );
                    self.watches.remove(key);
                }
                Some(_) => continue,
                None => {}
            }
            let route = nexus.store.flow_node(key).ok().flatten().and_then(|node| {
                match node.herdr_route_selection {
                    HerdrRouteSelection::Available(route) => Some(route),
                    HerdrRouteSelection::Unavailable => None,
                }
            });
            let armed = ArmedReap {
                launch_request_id: row.launch_request_id.clone(),
                reap_responsibility: row.reap_responsibility.clone(),
                route,
            };
            let cancellation =
                match ObservationCancellation::open(nexus.store.launch_changes.clone()) {
                    Ok(cancellation) => Arc::new(cancellation),
                    Err(error) => {
                        eprintln!(
                            "flow-nexus: the pane watch of {key} has no cancellation: {error}"
                        );
                        continue;
                    }
                };
            let worker = PaneWatchWorkerIdentity {
                predecessor: key.clone(),
                spawn: self.next_spawn,
            };
            self.next_spawn = PaneWatchSpawnSerial(self.next_spawn.0 + 1);
            let worker_cancellation = cancellation.clone();
            let worker_armed = armed.clone();
            let completion = PaneWatchCompletion {
                worker: worker.clone(),
                completed: self.completed.clone(),
                nexus,
            };
            let thread = scope.spawn(move || {
                // Announced once the body has made its end, or as it unwinds.
                let completion = completion;
                let end = nexus.watch_reaped_pane(
                    &completion.worker.predecessor,
                    &worker_armed,
                    &worker_cancellation,
                );
                drop(completion);
                end
            });
            #[cfg(test)]
            nexus.pane_watch_hooks.worker_spawned(&worker, &armed);
            self.watches.insert(
                key.clone(),
                PaneWatchEntry::Watching {
                    armed,
                    worker,
                    cancellation,
                    thread,
                },
            );
        }
    }

    fn stop_all(&mut self, nexus: &RunningNexus) {
        for entry in self.watches.values() {
            if let PaneWatchEntry::Watching { cancellation, .. } = entry {
                cancellation.cancel(ObservationCancellationCause::NexusStopped);
            }
        }
        for entry in std::mem::take(&mut self.watches).into_values() {
            if let PaneWatchEntry::Watching { worker, thread, .. } = entry {
                nexus.join_pane_watch(&worker, thread);
            }
        }
    }
}

impl ServesReapedPaneWatches for RunningNexus {
    fn serve_reaped_pane_watches<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>) {
        let changes = self.store.launch_changes.clone();
        let mut watches = ReapedPaneWatches::default();
        loop {
            let seen = changes.current();
            if self.stopping.requested() {
                break;
            }
            watches.pass(self, scope);
            #[cfg(test)]
            self.pane_watch_hooks.between_check_and_wait(self);
            changes.after_while(seen, &|| !self.stopping.requested());
        }
        watches.stop_all(self);
    }
}

impl WatchesReapedPane for RunningNexus {
    fn watch_reaped_pane(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
    ) -> PaneWatchEnd {
        // 1. A route not recorded has no pane to observe: only independent
        // evidence settles the reap; the record alone settles nothing.
        let Some(route) = armed.route.clone() else {
            if let Ok(Some(node)) = self.store.flow_node(predecessor)
                && node.closed_elsewhere()
            {
                return self.worker_act(predecessor, armed, cancellation);
            }
            return match self.worker_rewrite(
                predecessor,
                armed,
                cancellation,
                ReapStanding::HerdrUnavailable,
            ) {
                Ok(()) => PaneWatchEnd::Lapsed(PaneWatchLapse::RouteUnrecorded),
                Err(end) => end,
            };
        };
        let Ok(Some(node)) = self.store.flow_node(predecessor) else {
            return PaneWatchEnd::Lapsed(PaneWatchLapse::RouteChanged);
        };
        // 2. The session's socket.
        let socket = match self
            .herdr
            .session_socket_until(&route.herdr_session_name.0, cancellation)
        {
            Ok(Some(socket)) => socket,
            Ok(None) => return PaneWatchEnd::Lapsed(PaneWatchLapse::SessionUnknown),
            Err(cause) => return PaneWatchEnd::Cancelled(cause),
        };
        // 3. Subscribe first, so no close is lost between it and the
        // snapshot.
        let mut events = match HerdrEvents::subscribe(
            &socket,
            "flow-nexus:reaped-pane",
            serde_json::json!([{"type": "pane.closed"}, {"type": "pane.exited"}]),
            None,
            Some(cancellation),
        ) {
            Ok(events) => events,
            Err(crate::herdr::events::HerdrSubscriptionFailure::Cancelled(cause)) => {
                return PaneWatchEnd::Cancelled(cause);
            }
            Err(error) => {
                return PaneWatchEnd::Lapsed(PaneWatchLapse::SubscriptionRefused(
                    error.to_string(),
                ));
            }
        };
        // 4. Then the opening snapshot, and one close of a pane still the
        // predecessor's own, by the one close rule (module doc). The close
        // is made on this read's evidence: a pane that changes between the
        // read and the close (the snapshot-to-close race) is closed as read.
        match self.herdr.pane_ownership_until(&node, cancellation) {
            Err(cause) => return PaneWatchEnd::Cancelled(cause),
            Ok(PaneOwnership::Absent) => {
                #[cfg(test)]
                crate::test_faults::InjectsCrashes::crash_if_armed(
                    &self.store.faults,
                    crate::test_faults::CrashPoint::WatchAfterGone,
                );
                return self.worker_act(predecessor, armed, cancellation);
            }
            // A pane not proven the predecessor's own is never closed, nor
            // counted gone: the worker waits for its own going.
            Ok(PaneOwnership::Foreign | PaneOwnership::Unproven | PaneOwnership::Unidentified) => {}
            Ok(PaneOwnership::Unknown) => {
                if let Err(end) = self.worker_rewrite(
                    predecessor,
                    armed,
                    cancellation,
                    ReapStanding::HerdrUnavailable,
                ) {
                    return end;
                }
            }
            // Owned, or Bare: a Bare pane at the predecessor's terminal is
            // closed even when a person works in its plain shell (the
            // Bare-shell boundary), as every other close of a Flow's pane is.
            Ok(PaneOwnership::Owned | PaneOwnership::Bare) => {
                match self.herdr.close_pane_until(&route, cancellation) {
                    Err(cause) => return PaneWatchEnd::Cancelled(cause),
                    Ok(PaneClose::Closed) => {
                        #[cfg(test)]
                        crate::test_faults::InjectsCrashes::crash_if_armed(
                            &self.store.faults,
                            crate::test_faults::CrashPoint::AfterPaneClosed,
                        );
                    }
                    Ok(PaneClose::Refused(reason)) => {
                        eprintln!("flow-nexus: the reap of {predecessor}: {reason}");
                        if let Err(end) = self.worker_rewrite(
                            predecessor,
                            armed,
                            cancellation,
                            ReapStanding::CloseRefused,
                        ) {
                            return end;
                        }
                    }
                }
            }
        }
        // 5. Events: the pane's own going, or the stream's end.
        loop {
            match events.next_cue(&[], None, Some(cancellation)) {
                Err(crate::herdr::events::HerdrSubscriptionFailure::Cancelled(cause)) => {
                    return PaneWatchEnd::Cancelled(cause);
                }
                Err(error) => {
                    return PaneWatchEnd::Lapsed(PaneWatchLapse::SubscriptionRefused(
                        error.to_string(),
                    ));
                }
                Ok(HerdrCue::Event(event)) => {
                    #[cfg(test)]
                    self.pane_watch_hooks.at_event();
                    if event.pane_event(&route.herdr_pane_id.0) == Some(PaneEvent::Gone) {
                        #[cfg(test)]
                        crate::test_faults::InjectsCrashes::crash_if_armed(
                            &self.store.faults,
                            crate::test_faults::CrashPoint::WatchAfterGone,
                        );
                        return self.worker_act(predecessor, armed, cancellation);
                    }
                }
                Ok(HerdrCue::Closed) => {
                    return match self.herdr.pane_ownership_until(&node, cancellation) {
                        Err(cause) => PaneWatchEnd::Cancelled(cause),
                        Ok(PaneOwnership::Absent) => {
                            self.worker_act(predecessor, armed, cancellation)
                        }
                        Ok(_) => PaneWatchEnd::Lapsed(PaneWatchLapse::StreamClosedPanePresent),
                    };
                }
                Ok(HerdrCue::Other | HerdrCue::Passed) => {}
            }
        }
    }

    fn worker_rewrite(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
        standing: ReapStanding,
    ) -> Result<(), PaneWatchEnd> {
        #[cfg(test)]
        self.pane_watch_hooks.before_revalidation(armed);
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.revalidated(predecessor, armed, cancellation)?;
        if let Err(error) = self.store.reap_refused(predecessor, standing.clone()) {
            eprintln!(
                "flow-nexus: the reap of {predecessor} at {standing:?}: its rewrite was refused: {error}"
            );
        }
        Ok(())
    }

    fn worker_act(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
    ) -> PaneWatchEnd {
        #[cfg(test)]
        self.pane_watch_hooks.before_revalidation(armed);
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(end) = self.revalidated(predecessor, armed, cancellation) {
            return end;
        }
        let _ = self.settle_reap(predecessor);
        PaneWatchEnd::Acted
    }
}

/// The revalidation every worker commit takes, under the gate.
trait RevalidatesWorker {
    fn revalidated(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
    ) -> Result<(), PaneWatchEnd>;
}

impl RevalidatesWorker for RunningNexus {
    fn revalidated(
        &self,
        predecessor: &str,
        armed: &ArmedReap,
        cancellation: &ObservationCancellation,
    ) -> Result<(), PaneWatchEnd> {
        // a. The worker's cancellation has not fired.
        if let Some(cause) = cancellation.cause() {
            return Err(PaneWatchEnd::Cancelled(cause));
        }
        // b. The row still stands, names the launch the worker was armed
        // for and is owed by whom it was armed for; otherwise the worker is
        // stale and writes nothing. A row that cannot be read is no
        // evidence either way: the worker lapses, writing nothing.
        match self.store.pending_reap(predecessor) {
            Ok(Some(row)) if armed.armed_for(&row) => {}
            Ok(_) => return Err(PaneWatchEnd::Stale),
            Err(_) => return Err(PaneWatchEnd::Lapsed(PaneWatchLapse::StoreUnreadable)),
        }
        // c. The predecessor's row still names the route watched.
        let recorded = self
            .store
            .flow_node(predecessor)
            .ok()
            .flatten()
            .and_then(|node| match node.herdr_route_selection {
                HerdrRouteSelection::Available(route) => Some(route),
                HerdrRouteSelection::Unavailable => None,
            });
        let same = match (&recorded, &armed.route) {
            (Some(recorded), Some(watched)) => {
                recorded.herdr_pane_id == watched.herdr_pane_id
                    && recorded.herdr_terminal_id == watched.herdr_terminal_id
                    && recorded.herdr_session_name == watched.herdr_session_name
            }
            (None, None) => true,
            _ => false,
        };
        if !same {
            return Err(PaneWatchEnd::Lapsed(PaneWatchLapse::RouteChanged));
        }
        // d. The receiving flow is read afresh by step 3 itself.
        Ok(())
    }
}

/// A hook the supervisor runs once, with the Nexus it serves.
#[cfg(test)]
pub(crate) type SupervisorHook = Box<dyn FnOnce(&RunningNexus) + Send>;

/// One point a test holds a thread at: armed, the next thread to arrive
/// is counted and waits there until it is released. Each wait is bounded
/// by the test's own deadline; nothing sleeps.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct HeldPoint {
    state: Mutex<HeldPointState>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
#[derive(Default)]
struct HeldPointState {
    armed: bool,
    arrivals: usize,
    released: bool,
}

#[cfg(test)]
pub(crate) trait HoldsAtPoint {
    /// The next arrival waits at the point.
    fn arm(&self);
    /// Called by the thread at the point: waits there when armed.
    fn arrive(&self);
    /// Waits, bounded, until `count` threads have arrived armed.
    fn arrived_by(&self, count: usize, deadline: std::time::Instant) -> bool;
    /// Lets the waiting thread go on.
    fn release(&self);
}

#[cfg(test)]
impl HoldsAtPoint for HeldPoint {
    fn arm(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.armed = true;
        state.released = false;
    }

    fn arrive(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.armed {
            return;
        }
        state.armed = false;
        state.arrivals += 1;
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn arrived_by(&self, count: usize, deadline: std::time::Instant) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.arrivals < count {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }

    fn release(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.released = true;
        self.changed.notify_all();
    }
}

/// Test-build hooks the pane-watch races are proved with.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct PaneWatchHooks {
    /// Runs between the supervisor's pass and its wait, once.
    pub(crate) between: Mutex<Option<SupervisorHook>>,
    /// Every worker end the supervisor joined, as it joined it.
    pub(crate) ends: Mutex<Vec<(String, PaneWatchEnd)>>,
    /// Every worker the supervisor spawned, and every one it joined.
    pub(crate) spawned: Mutex<Vec<PaneWatchWorkerIdentity>>,
    pub(crate) joined: Mutex<Vec<PaneWatchWorkerIdentity>>,
    /// What each spawned worker was armed for, in spawn order.
    pub(crate) armed: Mutex<Vec<(PaneWatchWorkerIdentity, ArmedReap)>>,
    pub(crate) worker_join: std::sync::Condvar,
    /// Panics a worker at its next event, once.
    pub(crate) panic_at_event: std::sync::atomic::AtomicBool,
    /// How many workers announced their completion, and its announcement.
    pub(crate) workers_ended: Mutex<usize>,
    pub(crate) worker_end: std::sync::Condvar,
    /// A worker, its cue consumed, before it takes the gate to revalidate;
    /// and what each worker that arrived there was armed for.
    pub(crate) revalidation_hold: HeldPoint,
    pub(crate) revalidation_arrivals: Mutex<Vec<ArmedReap>>,
    /// The supervisor at the entry of its next pass, before it reads.
    pub(crate) pass_hold: HeldPoint,
    /// How many workers the pass cancelled as reassigned, counted after
    /// the cancel and before the join.
    pub(crate) reassignments_cancelled: Mutex<usize>,
    pub(crate) reassignment_cancel: std::sync::Condvar,
}

#[cfg(test)]
pub(crate) trait RunsPaneWatchHooks {
    fn between_check_and_wait(&self, nexus: &RunningNexus);
    fn at_event(&self);
    fn at_pass(&self);
    fn before_revalidation(&self, armed: &ArmedReap);
    fn reassignment_cancelled(&self);
    /// Waits, bounded, until `count` workers were cancelled as reassigned.
    fn reassignments_cancelled_by(&self, count: usize, deadline: std::time::Instant) -> bool;
    fn worker_spawned(&self, worker: &PaneWatchWorkerIdentity, armed: &ArmedReap);
    fn worker_ended(&self);
    fn worker_joined(&self, worker: &PaneWatchWorkerIdentity, end: &PaneWatchEnd);
    /// Waits, bounded, until `count` workers have announced their completion.
    fn workers_ended_by(&self, count: usize, deadline: std::time::Instant) -> bool;
    /// Waits, bounded, until `count` workers have been joined.
    fn workers_joined_by(&self, count: usize, deadline: std::time::Instant) -> bool;
}

#[cfg(test)]
impl RunsPaneWatchHooks for PaneWatchHooks {
    fn between_check_and_wait(&self, nexus: &RunningNexus) {
        let hook = self
            .between
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(hook) = hook {
            hook(nexus);
        }
    }

    fn at_pass(&self) {
        self.pass_hold.arrive();
    }

    fn before_revalidation(&self, armed: &ArmedReap) {
        self.revalidation_arrivals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(armed.clone());
        self.revalidation_hold.arrive();
    }

    fn reassignment_cancelled(&self) {
        *self
            .reassignments_cancelled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
        self.reassignment_cancel.notify_all();
    }

    fn reassignments_cancelled_by(&self, count: usize, deadline: std::time::Instant) -> bool {
        let mut cancelled = self
            .reassignments_cancelled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *cancelled < count {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            cancelled = self
                .reassignment_cancel
                .wait_timeout(cancelled, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }

    fn worker_spawned(&self, worker: &PaneWatchWorkerIdentity, armed: &ArmedReap) {
        self.spawned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(worker.clone());
        self.armed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((worker.clone(), armed.clone()));
    }

    fn worker_joined(&self, worker: &PaneWatchWorkerIdentity, end: &PaneWatchEnd) {
        self.ends
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((worker.predecessor.clone(), end.clone()));
        self.joined
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(worker.clone());
        self.worker_join.notify_all();
    }

    fn workers_joined_by(&self, count: usize, deadline: std::time::Instant) -> bool {
        let mut joined = self
            .joined
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while joined.len() < count {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            joined = self
                .worker_join
                .wait_timeout(joined, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }

    fn worker_ended(&self) {
        *self
            .workers_ended
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
        self.worker_end.notify_all();
    }

    fn workers_ended_by(&self, count: usize, deadline: std::time::Instant) -> bool {
        let mut ended = self
            .workers_ended
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *ended < count {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            ended = self
                .worker_end
                .wait_timeout(ended, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }

    fn at_event(&self) {
        if self
            .panic_at_event
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            // The unwind itself announces the worker's completion.
            panic!("flow-nexus test panic in a pane watch");
        }
    }
}
