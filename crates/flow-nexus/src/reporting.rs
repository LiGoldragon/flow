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

use crate::RunningNexus;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Harness_Data,
};
use crate::performing::Performs;
use crate::store::AnnouncesLaunchChanges;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{AccountsQueueDelivery, QueueDeliveryPhase};
use crate::waking::{CorrelatesNativeEvents, ReconcilesQueueDelivery, SleepsAndWakesSeats};
use meta_signal_flow::{
    EventsRead_Data, QueueDeliveriesRead_Data, QueueDelivery, QueueDeliveryResolution,
    QueueDeliveryResolved_Data, QueueDeliveryStanding, QueueResolutionRecord,
    ReadEventsRejected_Data, ReadQueueDeliveriesRejected_Data, ResolveQueueDeliveryRejected_Data,
    ResolverIdentity,
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
        // Every report of a Claude flow has its transcript read for a
        // compaction its harness made on its own; SessionStart follows one
        // at once.
        crate::compacting::WatchesCompactions::watch_claude_compactions(self, flow_id);
        match event {
            Event::Stopped(stop) => {
                // A launch sending its first message in parts waits on a
                // Stop before its next part.
                self.store.launch_changes.announce();
                if self.correlated_stop(flow_id, stop) {
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
            Event::Started | Event::ToolUsed(_) => {}
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
        let rejected =
            |rejection| meta_signal_flow::Response::ResolveQueueDeliveryRejected(rejection);
        let Some(unix_seconds) = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        else {
            return rejected(ResolveQueueDeliveryRejected_Data::StoreRefused);
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
                    "flow-nexus: queued delivery {} resolved {:?} by the owner",
                    attempt.attempt_id, resolution.queue_resolution_outcome
                );
                meta_signal_flow::Response::QueueDeliveryResolved(QueueDeliveryResolved_Data {
                    address: attempt.address,
                    queue_delivery_resolution: resolution,
                })
            }
            Ok(Err(refusal)) => rejected(refusal),
            Err(_) => rejected(ResolveQueueDeliveryRejected_Data::StoreRefused),
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
        let queue_delivery_vector = attempts
            .into_iter()
            .filter_map(|attempt| {
                let queue_delivery_standing = match attempt.phase {
                    QueueDeliveryPhase::Prepared => QueueDeliveryStanding::Prepared,
                    QueueDeliveryPhase::Uncertain(undecided) => {
                        QueueDeliveryStanding::Uncertain(undecided)
                    }
                    QueueDeliveryPhase::ReceiptRecorded(_) => {
                        QueueDeliveryStanding::ReceiptRecorded
                    }
                    QueueDeliveryPhase::Absent(absence) => {
                        QueueDeliveryStanding::Absent(absence.proof)
                    }
                    QueueDeliveryPhase::Resolved(record) => QueueDeliveryStanding::Resolved(record),
                    QueueDeliveryPhase::Settled(_) => return None,
                };
                Some(QueueDelivery {
                    queue_attempt_id: attempt.attempt_id,
                    flow_id: attempt.flow_id,
                    native_session_id: attempt.thread_id,
                    harness_kind: attempt.harness_kind,
                    queued_request: attempt.queued,
                    queue_delivery_standing,
                })
            })
            .collect();
        meta_signal_flow::Response::QueueDeliveriesRead(QueueDeliveriesRead_Data {
            address,
            queue_delivery_vector,
        })
    }
}
