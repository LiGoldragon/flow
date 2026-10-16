//! A harness event reaches Flow as `Report.{ FlowId Event }`, sent by the
//! flow's hook through the Flow CLI. It is one Operation, Record.Harness,
//! which appends the event to the events Flow's Memory holds for that flow.
//! A FlowId Flow does not hold is refused as `Refused.UnknownFlow`, never
//! adopted. The owner reads the events back over the meta socket with
//! `ReadEvents`. A `Stopped` event, once answered, writes the flow's
//! context size, lets a launch waiting on it send its next part, and may
//! drain its queue or put its metaflow to sleep (`after_report`).

use crate::RunningNexus;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Harness_Data,
};
use crate::performing::Performs;
use crate::store::AnnouncesLaunchChanges;
use crate::store::events::RecordsHarnessEvents;
use crate::waking::SleepsAndWakesSeats;
use meta_signal_flow::{EventsRead_Data, ReadEventsRejected_Data};
use signal_flow::{Event, FlowId, Report_Data, Response};

pub trait RecordsReports {
    fn report(&self, report: Report_Data) -> Response;
    /// What a recorded report sets going once it is answered: a `Stopped`
    /// flow's context size is written, and its resting seat is handed its
    /// oldest queued request or sleeps when nothing waits for it (see
    /// `waking`). Runs after the
    /// hook has its answer, so the harness never waits on it.
    fn after_report(&self, flow_id: &FlowId, event: &Event);
    fn read_events(&self, flow_id: FlowId) -> meta_signal_flow::Response;
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
        if *event == Event::Stopped {
            // A launch sending its first message in parts waits on this
            // Stop before its next part.
            self.store.launch_changes.announce();
            self.settle_stopped(flow_id);
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
}
