//! A harness event reaches Flow as `Report.{ FlowId Event }`, sent by the
//! flow's hook through the Flow CLI. It is one Operation, Record.Harness,
//! which appends the event to the events Flow's Memory holds for that flow.
//! A FlowId Flow does not hold is refused as `Refused.UnknownFlow`, never
//! adopted. The owner reads the events back over the meta socket with
//! `ReadEvents`.

use crate::RunningNexus;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Harness_Data,
};
use crate::performing::Performs;
use crate::store::events::RecordsHarnessEvents;
use meta_signal_flow::{EventsRead_Data, ReadEventsRejected_Data};
use signal_flow::{FlowId, Refusal, Report_Data, Response, Unknown_Data};

pub trait RecordsReports {
    fn report(&self, report: Report_Data) -> Response;
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
            Outcome::Failed(Failed_Data::UnknownFlow) => Response::Refused(Refusal::Unknown(
                Unknown_Data::FlowId(flow_id),
            )),
            Outcome::Failed(Failed_Data::StoreRefused) => {
                Response::Refused(Refusal::Store("Report could not be recorded".into()))
            }
            outcome => {
                eprintln!("flow-nexus: Report for {flow_id} not recorded: {outcome:?}");
                Response::Refused(Refusal::Store(format!(
                    "Report was refused with outcome {outcome:?}"
                )))
            }
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
