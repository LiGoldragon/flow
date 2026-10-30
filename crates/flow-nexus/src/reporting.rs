//! A harness event reaches Flow as `Report.{ FlowId Event }`, sent by the
//! flow's hook through the Flow CLI. It is one Operation, Record.Harness,
//! which appends the event to the events Flow's Memory holds for that flow.
//! A FlowId Flow does not hold is refused as `Refused.UnknownFlow`, never
//! adopted. The owner reads the events back over the meta socket with
//! `ReadEvents`. A `Stopped` event, once answered, lets a launch waiting on
//! it send its next part; when it names the flow's registered session and
//! that session's transcript, it is the flow's turn end: it writes the
//! flow's context size, and may drain its queue. A `PromptSubmitted` event in the flow's registered session whose
//! digest is exactly the request a hand-over typed is that request's
//! receipt, and settles it (`after_report`). An event naming another session
//! is kept and acts on nothing.
//!
//! A Claude session /clear ends is a session change, not an exit: its
//! `Cleared` is kept and records nothing else, the flow stays live and its
//! pane open. The `Started` that follows it names the harness's new
//! session, and the flow is rebound to that session; its FlowId stays.

use crate::RunningNexus;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Harness_Data,
};
use crate::performing::Performs;
use crate::store::AnnouncesLaunchChanges;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{AccountsQueueDelivery, ShowsQueueDelivery};
use crate::store::{ReadsFlowRows, RebindsNativeSession};
use crate::waking::{
    CorrelatesNativeEvents, ParksCohort, QueuesSimplifiedAnswers, ReconcilesQueueDelivery,
    SimplifiedAnswer, SleepsAndWakesSeats,
};
use meta_signal_flow::{
    EventsRead_Data, QueueDeliveriesRead_Data, QueueDeliveryResolution, QueueDeliveryResolved_Data,
    QueueResolutionRecord, ReadEventsRejected_Data, ReadQueueDeliveriesRejected_Data,
    ResolveQueueDeliveryRejection, ResolverIdentity,
};
use signal_flow::{Address, Event, FlowId, Report_Data, Response};

pub trait RecordsReports {
    fn report(&self, report: Report_Data) -> Response;
    /// What a recorded report sets going once it is answered: a `Stopped`
    /// flow's context size is written, and its resting seat is handed its
    /// oldest queued request or sleeps when nothing waits for it (see
    /// `waking`). Runs after the
    /// hook has its answer, so the harness never waits on it.
    fn after_report(&self, flow_id: &FlowId, event: &Event);
    fn read_events(&self, flow_id: FlowId) -> meta_signal_flow::Response;
    /// The owner's resolution of one Uncertain hand-over, recorded with the
    /// requesting peer, the reason and the time. Never run by Flow itself.
    fn resolve_queue_delivery(
        &self,
        resolution: QueueDeliveryResolution,
        resolver: ResolverIdentity,
    ) -> meta_signal_flow::Response;
    /// Every hand-over of the metaflow's queue that did not settle, with
    /// its standing: what an Uncertain one waits on is readable here.
    fn read_queue_deliveries(&self, address: Address) -> meta_signal_flow::Response;
}

impl RecordsReports for RunningNexus {
    fn report(&self, report: Report_Data) -> Response {
        let Report_Data { flow_id, event } = report;
        match self.perform(Operation::Record(Record_Data::Harness(
            Record_Data_Harness_Data {
                flow_id: flow_id.clone(),
                event,
            },
        ))) {
            Outcome::Recorded => Response::Reported,
            Outcome::Failed(Failed_Data::UnknownFlow) => Response::Refused(
                signal_flow::Refusal::Unknown(signal_flow::Unknown_Data::FlowId(flow_id)),
            ),
            // signal-flow 9.0.0 names no persistence refusal for Report, so
            // a store that fails is answered as a flow Flow cannot vouch
            // for, and logged.
            outcome => {
                eprintln!("flow-nexus: Report for {flow_id} not recorded: {outcome:?}");
                Response::Refused(signal_flow::Refusal::Unknown(
                    signal_flow::Unknown_Data::FlowId(flow_id),
                ))
            }
        }
    }

    fn after_report(&self, flow_id: &FlowId, event: &Event) {
        if let Event::Started(Some(session)) = event {
            self.follow_cleared_session(flow_id, session);
        }
        // Every report of a Claude flow has its transcript read for a
        // compaction its harness made on its own; SessionStart follows one
        // at once.
        crate::compacting::WatchesCompactions::watch_claude_compactions(self, flow_id);
        match event {
            // The flow's own Claude session ended: it has exited, its locks
            // go with it, and a Maintained metaflow is owed its successor.
            // Observe's Gone remains the fallback for an exit no hook
            // reports.
            Event::Ended(session) => {
                let own = self
                    .store
                    .flow_node(flow_id)
                    .ok()
                    .flatten()
                    .is_some_and(|node| {
                        node.harness_kind == signal_flow::HarnessKind::Claude
                            && !node.session_id.is_empty()
                            && session.as_deref() == Some(node.session_id.as_str())
                    });
                if own {
                    crate::observe_agent::RecordsWitnessedExit::exited_as_witnessed(self, flow_id);
                } else {
                    eprintln!(
                        "flow-nexus: flow {flow_id} reported a session end not its own: {session:?}"
                    );
                }
            }
            Event::Stopped(stop) => {
                // A launch sending its first message in parts waits on a
                // Stop before its next part.
                self.store.launch_changes.announce();
                if self.correlated_stop(flow_id, stop) {
                    // A simplified seat's answer is read from its own
                    // transcript once its input is received there: the
                    // payloads typed into it settle first, since the
                    // prompt's hook may have reported before Claude wrote
                    // its user row.
                    let answer = {
                        let _turn = self
                            .dispatch_gate
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if let Ok(Some(metaflow)) = self.store.metaflow_awake_as(flow_id) {
                            self.reconcile_typed(&metaflow.address);
                        }
                        self.queue_simplified_answer(flow_id)
                    };
                    let later = "it is looked for again at the seat's next rest or drain";
                    match &answer {
                        SimplifiedAnswer::NoneOwed => {}
                        SimplifiedAnswer::Queued { recipient, .. } => eprintln!(
                            "flow-nexus: simplified answer of {flow_id} queued to {:?} {} {:?}",
                            recipient.aspect, recipient.topic, recipient.layer
                        ),
                        SimplifiedAnswer::InputUnreceived { .. } => eprintln!(
                            "flow-nexus: simplified answer of {flow_id} not queued at its Stop: its input is not yet Delivered; {later}"
                        ),
                        SimplifiedAnswer::TurnUnanswered { .. } => eprintln!(
                            "flow-nexus: simplified answer of {flow_id} not queued at its Stop: its transcript shows no ended answer; {later}"
                        ),
                        SimplifiedAnswer::Undelivered { .. } => eprintln!(
                            "flow-nexus: simplified answer of {flow_id} refused by the store: its recipient is unknown or its receipt changed; {later}"
                        ),
                        SimplifiedAnswer::StoreRefused => eprintln!(
                            "flow-nexus: simplified answer of {flow_id} not queued at its Stop: the store refused; {later}"
                        ),
                    }
                    self.settle_stopped(flow_id);
                } else {
                    eprintln!(
                        "flow-nexus: a Stop reported for flow {flow_id} names session {:?}, not its own; it settles nothing",
                        stop.native_session_id_option
                    );
                }
            }
            Event::PromptSubmitted(prompt) => {
                if self.correlated_prompt(flow_id, prompt) {
                    self.settle_submitted_prompt(flow_id, prompt);
                }
            }
            // A cleared session is not an exit: the harness goes on in the
            // session the next Started names.
            Event::Started(_) | Event::ToolUsed(_) | Event::Cleared(_) => {}
        }
    }

    fn read_events(&self, flow_id: FlowId) -> meta_signal_flow::Response {
        match self.store.events(&flow_id) {
            Ok(Some(event_vector)) => meta_signal_flow::Response::EventsRead(EventsRead_Data {
                flow_id,
                event_vector,
            }),
            Ok(None) => {
                meta_signal_flow::Response::ReadEventsRejected(ReadEventsRejected_Data::UnknownFlow)
            }
            Err(_) => meta_signal_flow::Response::ReadEventsRejected(
                ReadEventsRejected_Data::StoreRefused,
            ),
        }
    }

    fn resolve_queue_delivery(
        &self,
        resolution: QueueDeliveryResolution,
        resolver: ResolverIdentity,
    ) -> meta_signal_flow::Response {
        let rejected = |rejection| {
            meta_signal_flow::Response::ResolveQueueDeliveryRejected(
                crate::message_projection::visible_queue_rejection(rejection),
            )
        };
        let Some(unix_seconds) = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        else {
            return rejected(ResolveQueueDeliveryRejection::StoreRefused);
        };
        let record = QueueResolutionRecord {
            queue_resolution_outcome: resolution.queue_resolution_outcome.clone(),
            resolver_identity: resolver,
            queue_resolution_reason: resolution.queue_resolution_reason.clone(),
            unix_seconds,
        };
        match self
            .store
            .resolve_uncertain_delivery(&resolution.queue_attempt_id, record)
        {
            Ok(Ok(attempt)) => {
                eprintln!(
                    "flow-nexus: a queued delivery was resolved {:?} by the owner",
                    resolution.queue_resolution_outcome
                );
                meta_signal_flow::Response::QueueDeliveryResolved(QueueDeliveryResolved_Data {
                    address: attempt.address,
                    visible_queue_delivery_resolution:
                        crate::message_projection::visible_queue_resolution(&resolution),
                })
            }
            Ok(Err(refusal)) => rejected(refusal),
            Err(_) => rejected(ResolveQueueDeliveryRejection::StoreRefused),
        }
    }

    fn read_queue_deliveries(&self, address: Address) -> meta_signal_flow::Response {
        let rejected =
            |rejection| meta_signal_flow::Response::ReadQueueDeliveriesRejected(rejection);
        match self.store.metaflow(&address) {
            Ok(Some(_)) => {}
            Ok(None) => return rejected(ReadQueueDeliveriesRejected_Data::UnknownAddress),
            Err(_) => return rejected(ReadQueueDeliveriesRejected_Data::StoreRefused),
        }
        let Ok(attempts) = self.store.unsettled_deliveries(&address) else {
            return rejected(ReadQueueDeliveriesRejected_Data::StoreRefused);
        };
        let visible_queue_delivery_vector = attempts
            .into_iter()
            .filter_map(ShowsQueueDelivery::queue_delivery)
            .map(|attempt| crate::message_projection::visible_queue_delivery(&attempt, &address))
            .collect();
        meta_signal_flow::Response::QueueDeliveriesRead(QueueDeliveriesRead_Data {
            address,
            visible_queue_delivery_vector,
        })
    }
}

/// A Claude flow whose harness goes on in a new session after /clear.
trait FollowsClearedSession {
    /// A `Started` naming `session` that follows the flow's own `Cleared`
    /// rebinds the live Claude flow to `session`: the last start or end
    /// reported before it is the clear of the session the flow is bound
    /// to. Any other start rebinds nothing.
    fn follow_cleared_session(&self, flow_id: &FlowId, session: &str);
}

impl FollowsClearedSession for RunningNexus {
    fn follow_cleared_session(&self, flow_id: &FlowId, session: &str) {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return;
        };
        if node.harness_kind != signal_flow::HarnessKind::Claude
            || node.session_id.is_empty()
            || node.session_id == session
        {
            return;
        }
        let Ok(Some(events)) = self.store.events(flow_id) else {
            return;
        };
        let cleared_own = events
            .iter()
            .rev()
            .skip(1)
            .find_map(|earlier| match earlier {
                Event::Cleared(cleared) => {
                    Some(cleared.as_deref() == Some(node.session_id.as_str()))
                }
                Event::Started(_) | Event::Ended(_) => Some(false),
                Event::ToolUsed(_) | Event::Stopped(_) | Event::PromptSubmitted(_) => None,
            })
            .unwrap_or(false);
        if !cleared_own {
            return;
        }
        match self
            .store
            .rebind_native_session(flow_id, &node.session_id, session)
        {
            Ok(true) => crate::compacting::WatchesCompactions::session_rebound(self, flow_id),
            Ok(false) => {}
            Err(error) => eprintln!(
                "flow-nexus: flow {flow_id} was not rebound to its new session {session}: {error:?}"
            ),
        }
    }
}
