//! Flow's migration boundary between value-returning domain calls and a
//! socket lifetime. Generated Answerable and direct snapshots stay intact.
//! Each intercepted Input may move independently to a body-yielded answer.

use super::*;

/// Kernel evidence carried beside a decoded Input, never supplied by it.
struct ConnectionCaller {
    process: Option<Process>,
    peer: Option<CallerProcess>,
}

struct OrdinaryEnvelope {
    input: Input,
    caller: ConnectionCaller,
}

struct MetaEnvelope {
    input: meta_signal_flow::Input,
    caller: ConnectionCaller,
}

struct FlowConnectionAdapter<'a> {
    nexus: &'a RunningNexus,
}

enum ReplyOutput {
    Ordinary(Box<Output>),
    Meta(Box<meta_signal_flow::Output>),
}

struct Reply {
    output: ReplyOutput,
    completion: Completion,
}

/// This is the observed write contract, including the deliberately different
/// W0/WR, W1 and WD policies of the baseline handler table.
enum Completion {
    OnSuccessfulWrite(PostReplyOperation),
    AfterAnyWrite(PostReplyOperation),
    NoPostReplyWork,
}

// Operation payloads are kept separately so completion policy remains visible
// without borrowing the answer or keeping the peer alive.
enum PostReplyOperation {
    SettleStops,
    AttributedReport {
        flow_id: signal_flow::FlowId,
        event: signal_flow::Event,
        acts: bool,
    },
    WakeDue(signal_flow::Address),
    DrainAwake(signal_flow::Address),
    SettleThenBoundExisting(Option<signal_flow::FlowId>),
}

enum Observation {
    Ordinary(ObserveSelection),
    Sends {
        selection: crate::observe_sends::SendSelection,
        cursor: Option<i64>,
    },
    Configuration,
}

enum Serving {
    Reply(Box<Reply>),
    Observation(Observation),
}

trait AdaptsFlowConnection {
    fn ordinary(&self, envelope: OrdinaryEnvelope) -> Serving;
    fn meta(&self, envelope: MetaEnvelope) -> Serving;
}

trait YieldsFlowReply {
    fn reply(self, completion: Completion) -> Serving;
}

trait CompletesReply {
    /// Returns the original write error even if an always-after-write
    /// operation runs. The caller has already dropped the connection.
    fn complete(
        self,
        written: Result<(), String>,
        perform: impl FnOnce(PostReplyOperation),
    ) -> Result<(), String>;
}

trait PerformsPostReply {
    fn perform(self, nexus: &RunningNexus);
}

trait WritesAdaptedReply {
    fn write_reply(
        self,
        reply: Reply,
        perform: impl FnOnce(PostReplyOperation),
    ) -> Result<(), String>;
}

trait ServesAdaptedConnection {
    fn serve_adapted(self, nexus: &RunningNexus, serving: Serving) -> Result<(), String>;
    fn serve_observation(
        self,
        nexus: &RunningNexus,
        observation: Observation,
    ) -> Result<(), String>;
}

impl AdaptsFlowConnection for FlowConnectionAdapter<'_> {
    fn ordinary(&self, envelope: OrdinaryEnvelope) -> Serving {
        let nexus = self.nexus;
        let OrdinaryEnvelope { input, caller } = envelope;
        let response = match input {
            Input::LaunchStatus(id) => nexus.launch_status(&id.0),
            Input::Observe(selection) => {
                return Serving::Observation(Observation::Ordinary(selection));
            }
            Input::ObserveSends(cursor) => {
                return Serving::Observation(Observation::Sends {
                    selection: crate::observe_sends::SendSelection::Every,
                    cursor: cursor.map(|value| value.0),
                });
            }
            Input::ObserveSender(forwarded) => {
                let sender = match nexus.observation_sender(&forwarded) {
                    Ok(sender) => sender,
                    Err(refusal) => return refusal.reply(Completion::NoPostReplyWork),
                };
                return Serving::Observation(Observation::Sends {
                    selection: crate::observe_sends::SendSelection::Sent(sender),
                    cursor: forwarded.delivery_cursor_option.map(|value| value.0),
                });
            }
            Input::ObserveDeliveries(cursor) => {
                return Serving::Observation(Observation::Sends {
                    selection: crate::observe_sends::SendSelection::Message,
                    cursor: cursor.map(|value| value.0),
                });
            }
            Input::Report(report) => {
                use crate::restarting::FencesSeatEvents;
                let operation = PostReplyOperation::AttributedReport {
                    flow_id: report.flow_id.clone(),
                    event: report.event.clone(),
                    acts: nexus.report_acts(&report.flow_id.0, report.process_option.as_ref()),
                };
                let response = nexus.report(report);
                let completion = if response == Output::Reported {
                    Completion::OnSuccessfulWrite(operation)
                } else {
                    Completion::NoPostReplyWork
                };
                return response.reply(completion);
            }
            Input::ResolveCaller(claim) => nexus.resolve_caller(
                caller
                    .peer
                    .and_then(|process| process.caller_pane(&nexus.herdr)),
                claim,
            ),
            query @ (Input::Bind(_)
            | Input::Identify(_)
            | Input::IdentifyThread(_)
            | Input::Lock(_)
            | Input::Release(_)
            | Input::ArchiveRequest(_)
            | Input::Acknowledge(_)) => {
                return nexus
                    .dispatch_metaflow_peer(query, caller.process.as_ref())
                    .response
                    .reply(Completion::NoPostReplyWork);
            }
            Input::Deliver(request) => {
                let address = request.lock.second_address.clone();
                let answer =
                    nexus.dispatch_metaflow_peer(Input::Deliver(request), caller.process.as_ref());
                let completion = if matches!(answer.response, Output::SendAccepted(_)) {
                    Completion::AfterAnyWrite(PostReplyOperation::WakeDue(address))
                } else {
                    Completion::NoPostReplyWork
                };
                return answer.response.reply(completion);
            }
            Input::Send(send) => {
                let answer = nexus.send_request(send, caller.process.as_ref());
                let completion = match answer.pending {
                    Some(pending) => {
                        Completion::AfterAnyWrite(PostReplyOperation::WakeDue(pending.address))
                    }
                    None => Completion::NoPostReplyWork,
                };
                return answer.response.reply(completion);
            }
            Input::DeliverAll(address) => {
                return nexus
                    .deliver_all(address)
                    .reply(Completion::NoPostReplyWork);
            }
            Input::Remind(remind) => {
                use crate::sending::IdentifiesSender;
                let sent_by = match caller.process.as_ref() {
                    Some(process) => nexus.sender_of(process),
                    None => Ok(signal_flow::SentBy::Unbound),
                };
                let response = match sent_by {
                    Ok(sent_by) => nexus.remind(remind, sent_by),
                    Err(refusal) => *refusal,
                };
                return response.reply(Completion::NoPostReplyWork);
            }
            Input::BindSeat(request) => {
                let address = request.address.clone();
                let response = nexus.bind_seat(request);
                let completion = if matches!(response, Output::SeatBound(_)) {
                    Completion::AfterAnyWrite(PostReplyOperation::DrainAwake(address))
                } else {
                    Completion::NoPostReplyWork
                };
                return response.reply(completion);
            }
            Input::Adopt(request) => {
                let address = request.address.clone();
                let response = nexus.adopt(request);
                let completion = if matches!(response, Output::Adopted(_)) {
                    Completion::AfterAnyWrite(PostReplyOperation::DrainAwake(address))
                } else {
                    Completion::NoPostReplyWork
                };
                return response.reply(completion);
            }
            Input::Launch(launch) => {
                let origin = nexus.origin_of_process(caller.process.as_ref());
                let declared = {
                    let _turn = nexus
                        .dispatch_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    nexus.declared_launch(launch)
                };
                match declared {
                    Ok(query) => nexus.dispatch_serially(query, origin),
                    Err(refusal) => *refusal,
                }
            }
            query @ (Input::Start(_) | Input::Replace(_)) => {
                let origin = nexus.origin_of_process(caller.process.as_ref());
                nexus.dispatch_serially(query, origin)
            }
            query => nexus.dispatch_serially(query, Origin::Unbound),
        };
        response.reply(Completion::OnSuccessfulWrite(
            PostReplyOperation::SettleStops,
        ))
    }

    fn meta(&self, envelope: MetaEnvelope) -> Serving {
        if envelope.input == meta_signal_flow::Input::ObserveConfiguration {
            return Serving::Observation(Observation::Configuration);
        }
        let resolver = self.nexus.meta_resolver(envelope.caller.peer);
        let response = self
            .nexus
            .dispatch_meta_serially(envelope.input, Some(resolver));
        let bound = match &response {
            meta_signal_flow::Output::BoundExisting(bound) => match &bound.flow_binding_result {
                meta_signal_flow::FlowBindingResult::Bound(binding) => {
                    Some(binding.flow_id.clone())
                }
                _ => None,
            },
            _ => None,
        };
        response.reply(Completion::OnSuccessfulWrite(
            PostReplyOperation::SettleThenBoundExisting(bound),
        ))
    }
}

impl YieldsFlowReply for Output {
    fn reply(self, completion: Completion) -> Serving {
        Serving::Reply(Box::new(Reply {
            output: ReplyOutput::Ordinary(Box::new(self)),
            completion,
        }))
    }
}

impl YieldsFlowReply for meta_signal_flow::Output {
    fn reply(self, completion: Completion) -> Serving {
        Serving::Reply(Box::new(Reply {
            output: ReplyOutput::Meta(Box::new(self)),
            completion,
        }))
    }
}

impl CompletesReply for Completion {
    fn complete(
        self,
        written: Result<(), String>,
        perform: impl FnOnce(PostReplyOperation),
    ) -> Result<(), String> {
        match self {
            Self::OnSuccessfulWrite(operation) if written.is_ok() => perform(operation),
            Self::AfterAnyWrite(operation) => perform(operation),
            Self::OnSuccessfulWrite(_) | Self::NoPostReplyWork => {}
        }
        written
    }
}

impl PerformsPostReply for PostReplyOperation {
    fn perform(self, nexus: &RunningNexus) {
        match self {
            Self::SettleStops => nexus.settle_stops_at_confirmation(),
            Self::AttributedReport {
                flow_id,
                event,
                acts,
            } => {
                if acts {
                    nexus.after_attributed_report(&flow_id, &event);
                } else {
                    eprintln!(
                        "flow-nexus: a report for flow {flow_id} is not its bound seat's; it acts on nothing",
                        flow_id = flow_id.0
                    );
                }
            }
            Self::WakeDue(address) => nexus.wake_due(address),
            Self::DrainAwake(address) => nexus.drain_awake(&address),
            Self::SettleThenBoundExisting(bound) => {
                nexus.settle_stops_at_confirmation();
                if let Some(flow_id) = bound
                    && let Ok(Some(metaflow)) =
                        crate::store::metaflow::SleepsMetaflows::metaflow_awake_as(
                            &*nexus.store,
                            &flow_id.0,
                        )
                {
                    nexus.drain_awake(&metaflow.address);
                }
            }
        }
    }
}

impl ServesAdaptedConnection for Connection {
    fn serve_adapted(self, nexus: &RunningNexus, serving: Serving) -> Result<(), String> {
        match serving {
            Serving::Reply(reply) => self.write_reply(*reply, |operation| operation.perform(nexus)),
            Serving::Observation(observation) => self.serve_observation(nexus, observation),
        }
    }

    fn serve_observation(
        mut self,
        nexus: &RunningNexus,
        observation: Observation,
    ) -> Result<(), String> {
        match observation {
            Observation::Ordinary(ObserveSelection::Launch(id)) => {
                use crate::observation_lifetime::ServesCancellableObservation;
                self.serve_cancellable_observation(nexus, &mut |cancellation, peer| {
                    nexus.observe_launch(&id.0, cancellation, &mut |response| {
                        peer.write_response(response)
                    })
                })
            }
            Observation::Ordinary(ObserveSelection::Agent(id)) => {
                use crate::observation_lifetime::ServesCancellableObservation;
                self.serve_cancellable_observation(nexus, &mut |cancellation, peer| {
                    nexus.observe_agent(&id.0, cancellation, &mut |response| {
                        peer.write_response(response)
                    })
                })
            }
            Observation::Ordinary(ObserveSelection::Undecodable) => self.serve_undecodable(nexus),
            Observation::Ordinary(ObserveSelection::TurnEnds) => self.serve_turn_ends(nexus),
            Observation::Ordinary(ObserveSelection::Metaflows) => self.serve_metaflows(nexus),
            Observation::Ordinary(ObserveSelection::Retirement(address)) => {
                use crate::store::handover::HoldsHandovers;
                let subscription = match nexus.store.observe_handover_retirement(&address) {
                    Ok(subscription) => subscription,
                    Err(error) => {
                        return self
                            .peer
                            .write_response(&Output::RetirementObservationFailed(
                                signal_flow::RetirementObservationFailure::MissingEvidence(
                                    error.to_string(),
                                ),
                            ));
                    }
                };
                self.serve_retirement(nexus, subscription)
            }
            Observation::Sends { selection, cursor } => self.serve_sends(nexus, &selection, cursor),
            Observation::Configuration => self.serve_configuration(nexus),
        }
    }
}

/// The serving entry point receives decoded values and owns all reply I/O.
pub(super) trait AdaptsDecodedConnection {
    fn serve_decoded_ordinary(self, nexus: &RunningNexus, input: Input) -> Result<(), String>;
    fn serve_decoded_meta(
        self,
        nexus: &RunningNexus,
        input: meta_signal_flow::Input,
    ) -> Result<(), String>;
}

impl AdaptsDecodedConnection for Connection {
    fn serve_decoded_ordinary(self, nexus: &RunningNexus, input: Input) -> Result<(), String> {
        let caller = ConnectionCaller {
            process: self.process.clone(),
            peer: self.peer.peer_process(),
        };
        let serving = FlowConnectionAdapter { nexus }.ordinary(OrdinaryEnvelope { input, caller });
        self.serve_adapted(nexus, serving)
    }

    fn serve_decoded_meta(
        self,
        nexus: &RunningNexus,
        input: meta_signal_flow::Input,
    ) -> Result<(), String> {
        let caller = ConnectionCaller {
            process: self.process.clone(),
            peer: self.peer.peer_process(),
        };
        let serving = FlowConnectionAdapter { nexus }.meta(MetaEnvelope { input, caller });
        self.serve_adapted(nexus, serving)
    }
}

impl WritesAdaptedReply for Connection {
    fn write_reply(
        self,
        reply: Reply,
        perform: impl FnOnce(PostReplyOperation),
    ) -> Result<(), String> {
        let mut connection = self;
        let written = match reply.output {
            ReplyOutput::Ordinary(output) => connection.peer.write_response(&output),
            ReplyOutput::Meta(output) => connection.peer.write_meta_response(&output),
        };
        drop(connection);
        reply.completion.complete(written, perform)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn successful_write_policy_skips_work_and_preserves_failure() {
        let called = Cell::new(false);
        let error = "original write failure".to_owned();
        let result = Completion::OnSuccessfulWrite(PostReplyOperation::SettleStops)
            .complete(Err(error.clone()), |_| called.set(true));
        assert_eq!(result, Err(error));
        assert!(!called.get());
    }

    #[test]
    fn always_policy_runs_work_and_preserves_failure() {
        let called = Cell::new(false);
        let error = "original write failure".to_owned();
        let result = Completion::AfterAnyWrite(PostReplyOperation::SettleStops)
            .complete(Err(error.clone()), |_| called.set(true));
        assert_eq!(result, Err(error));
        assert!(called.get());
    }

    #[test]
    fn both_work_policies_run_once_after_success() {
        for completion in [
            Completion::OnSuccessfulWrite(PostReplyOperation::SettleStops),
            Completion::AfterAnyWrite(PostReplyOperation::SettleStops),
        ] {
            let count = Cell::new(0);
            assert_eq!(
                completion.complete(Ok(()), |_| count.set(count.get() + 1)),
                Ok(())
            );
            assert_eq!(count.get(), 1);
        }
    }

    #[test]
    fn direct_write_has_no_tail_on_either_outcome() {
        for written in [Ok(()), Err("write failed".to_owned())] {
            let expected = written.clone();
            assert_eq!(
                Completion::NoPostReplyWork.complete(written, |_| panic!("unexpected tail")),
                expected
            );
        }
    }

    #[test]
    fn serving_writes_real_reply_and_closes_before_operation() {
        let (peer, mut reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let connection = Connection {
            peer,
            process: None,
        };
        let result = connection.write_reply(
            Reply {
                output: ReplyOutput::Ordinary(Box::new(Output::Reported)),
                completion: Completion::OnSuccessfulWrite(PostReplyOperation::SettleStops),
            },
            |_| {
                use crate::CarriesSignalFrames;
                assert_eq!(reader.read_response().unwrap(), Output::Reported);
                let mut byte = [0];
                assert_eq!(reader.read(&mut byte).unwrap(), 0);
            },
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn serving_failed_write_runs_always_operation_after_close() {
        let (peer, reader) = UnixStream::pair().unwrap();
        drop(reader);
        let called = Cell::new(false);
        let connection = Connection {
            peer,
            process: None,
        };
        let result = connection.write_reply(
            Reply {
                output: ReplyOutput::Ordinary(Box::new(Output::Reported)),
                completion: Completion::AfterAnyWrite(PostReplyOperation::SettleStops),
            },
            |_| {
                called.set(true);
            },
        );
        assert!(result.is_err());
        assert!(called.get());
    }
}
