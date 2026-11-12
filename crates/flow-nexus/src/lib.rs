//! Flow Nexus dispatches typed ordinary and privileged Signal requests.

use crate::store::ManagesMetaflowState;
use crate::store::ReadsFlowMemory;
use crate::store::context::RecordsContextSize;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::send_outcome::RecordsSendOutcomes;
// The generated roots name this crate's types by its name: Operation names
// the Memory root's `flow_nexus::LaunchOutcome`, and Memory the starter's
// `flow_nexus::LaunchedRecord`.
extern crate self as flow_nexus;
use crate::claude::ProjectsClaudeReadiness;
use crate::codex::SelectsCodexEndpoint;
pub use crate::generated::curriculum::{
    ContextProvenance, Outcome as CurriculumOutcome, Query as CurriculumQuery, StartupComposition,
    StartupSelection,
};
pub use crate::generated::memory::{
    LaunchOutcome, PendingHandover, PendingReap, Replacement, SeatBirth, SupersededHandover,
};
pub use crate::generated::operation::Outcome as OperationOutcome;
pub use crate::generated::starter::{LaunchedRecord, ReplacedRecord};
use crate::herdr::ConfiguresHerdrCli;
use crate::herdr::ReadsHerdrPanes;
use crate::live::{HoldsLiveValue, Live};
use crate::store::AnnouncesLaunchChanges;
pub use caller::CallerProcess;
pub use composition::CompositionError;
pub use context::VerifiedContext;
pub use store::StoreError;
pub(crate) mod admission;
pub mod binding;
pub mod caller;
pub mod claude;
pub mod codex;
pub mod codex_turns;
pub mod compacting;
pub mod composition;
pub mod context;
pub mod delivery;
pub mod event_wait;
pub mod lean;
pub mod live;
pub(crate) mod message_projection;
mod metaflow;
pub mod model;
pub mod observation_lifetime;
pub mod protecting;
pub mod queue_operations;
pub mod recovery;
pub mod reminding;
pub mod repairing;
pub mod restarting;
use metaflow::DispatchesMetaflowPeer;
#[cfg(test)]
mod exec_witness;
#[cfg(test)]
mod fixture_executable;
/// Generated from the operation and startup Ethos roots; `build.rs` keeps
/// both outputs fresh. The `datom` feature enables their textual CLI surface.
#[allow(unexpected_cfgs, clippy::large_enum_variant)]
pub mod generated;
pub mod reaping;
#[cfg(test)]
mod test_faults;
#[cfg(feature = "executable-default-configuration")]
mod executable_default_configuration {
    include!(env!("FLOW_NEXUS_DEFAULT_CONFIGURATION_RS"));
}
pub mod handover;
pub mod handover_archive;
mod handover_observation;
pub mod handover_reminding;
pub(crate) mod harness_command;
pub mod heartbeat;
pub mod herdr;
pub mod launching;
pub mod observe_agent;
pub mod observe_configuration;
pub mod observe_metaflows;
pub mod observe_sends;
pub mod peer;
pub mod performing;
pub mod permaflow_inventory;
pub mod place;
pub mod placed_socket;
pub mod reporting;
pub mod send_tool;
pub mod sending;
pub mod store;
pub mod store_check;
pub mod submission;
pub mod title;
pub mod turn_ends;
pub mod undecodable;
pub mod values;
pub mod waking;

use crate::generated::operation::{Operation, Outcome, Record_Data};
use binding::{ChecksFlowContainer, ChecksProcessIdentity, VerifiesAdoptedSeat};
use caller::{IdentifiesPeer, LocatesCallerPane, RecordsOriginOfProcess, ResolvesCaller};
use codex::{CodexEndpoints, ConsumesResetCredit};
use composition::{LaunchComposer, OpensLaunchComposer};
use context::{CurriculumComposer, OpensCurriculumComposer};
use delivery::{DeliversMessages, lease::LeasesPanes};
use handover_observation::{
    ObservesHandoverRetirement, ReadsRetirementSubscription, RetirementSubscription,
};
use launching::{AdmitsLaunch, LaunchesFlows, ObservesLaunch, ReadsHarnessArguments};
use observe_agent::ObservesAgent;
use observe_sends::ObservesLogicalSends;
use peer::{NamesMetaPeer, ResolvesPeer};
use performing::Performs;
use queue_operations::OperatesOnRequestsAndQueues;
use reporting::RecordsReports;
use send_tool::ServesLaunchCalls;
use sending::{BindsSeats, LaunchesDeclared, RehearsesDeclared, SendsRequests};
use signal_flow::{
    EndpointSelection, FlowLifecycle, FlowNode, HerdrRouteSelection, ObserveSelection, Origin,
    Process, Query, Response, StartRejection,
};
use std::{
    fs,
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::{UnixListener, UnixStream},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[cfg(feature = "executable-default-configuration")]
use store::ConfiguresOnlyStore;
use store::logical_delivery::RecordsLogicalDeliveries;
use store::{
    AppliesFlowQuery, FlowStore, NamesLiveFlow, OpensFlowStore, ReadsFlowRows,
    RegistersExistingFlow, RegistersFlowIdentity,
};
use waking::SettlesStopsAtConfirmation;
use waking::SleepsAndWakesSeats;

pub struct RunningNexus {
    pub store: Arc<FlowStore>,
    /// The Codex endpoints as the configuration names them; a Configure
    /// replaces them at once ([`AppliesConfiguration`]).
    pub codex_endpoints: Live<CodexEndpoints>,
    pub herdr: herdr::HerdrCli,
    /// Composes through the Curriculum socket and presentation prompt the
    /// configuration names; a Configure replaces it at once.
    pub composer: Live<LaunchComposer>,
    /// Serializes dispatch; see `dispatch_serially`.
    pub dispatch_gate: Mutex<()>,
    /// The exclusive hold on each pane Flow is writing; see
    /// `delivery::lease`.
    pub pane_leases: delivery::lease::PaneLeases,
    /// The metaflows a wake is running for; see `waking`.
    pub waking: waking::WakingMetaflows,
    /// Flows confirmed after their Stop, awaiting settlement; see `waking`.
    pub stopped_at_confirmation: waking::StoppedAtConfirmation,
    /// Flows kept because their successor was refused; see `waking`.
    pub refresh_refusals: waking::RefreshRefusals,
    /// Predecessors whose reap worker a bearing event asked to arm again;
    /// the reap supervisor takes them (`reaping`).
    pub(crate) pane_watch_rearms: reaping::PaneWatchRearms,
    /// The test build's pane-watch race hooks.
    #[cfg(test)]
    pub(crate) pane_watch_hooks: reaping::PaneWatchHooks,
    /// Set once the Nexus is told to stop; see [`StopsNexus`].
    pub stopping: Stopping,
    /// The send-tool responder of the Codex launch running now; see
    /// `send_tool::ServesLaunchCalls`.
    pub launch_calls: send_tool::LaunchCalls,
    /// Whether each Codex thread is in a turn, as its app-server's events
    /// show it; see `codex_turns`.
    pub codex_turns: codex_turns::CodexTurns,
    /// The open `Observe.TurnEnds` subscriptions; see `turn_ends`.
    pub turn_ends: turn_ends::TurnEndSubscribers,
    /// How far each flow's compactions have been read; see `compacting`.
    pub compactions: compacting::CompactionWatch,
    /// The submissions each seat reported taking; see `submission`.
    pub seat_submissions: Arc<submission::SeatSubmissions>,
    /// Announced each time a Configure replaces a family, and at a stop;
    /// see `observe_configuration`.
    pub configuration_changes: store::LaunchChanges,
}

/// The handover source for fresh successors: the predecessor's sections in
/// the running Memory, or, when Memory holds none, the file the handover
/// warning names, `flows/<predecessor>/handover.md` under the launch root
/// ([`crate::handover::HandoverFile`]). With no root resolved only Memory is
/// read.
#[derive(Clone)]
struct MemoryHandoverSource(Arc<FlowStore>);

impl std::fmt::Debug for MemoryHandoverSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MemoryHandoverSource")
    }
}

impl crate::handover::YieldsHandover for MemoryHandoverSource {
    fn requires_launch_root(&self) -> bool {
        false
    }

    fn handover(
        &self,
        address: &signal_flow::Address,
        predecessor: &str,
        launch_root: &Path,
    ) -> crate::handover::Handover {
        match self.store_handover(address, predecessor) {
            Ok(Some(sections)) if !sections.is_empty() => crate::handover::Handover::Text(
                sections
                    .into_iter()
                    .map(|section| {
                        format!(
                            "## {}\n{}\n{}",
                            section.title.0, section.description.0, section.body.0
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n\n"),
            ),
            Ok(_) if launch_root.as_os_str().is_empty() => crate::handover::Handover::Absent,
            Ok(_) => crate::handover::HandoverFile.handover(address, predecessor, launch_root),
            Err(error) => crate::handover::Handover::Unreadable(
                signal_flow::HandoverFault::Unreadable(error.to_string()),
            ),
        }
    }
}

trait ReadsMemoryHandoverSource {
    fn store_handover(
        &self,
        address: &signal_flow::Address,
        predecessor: &str,
    ) -> Result<Option<Vec<signal_flow::Section>>, StoreError>;
}

impl ReadsMemoryHandoverSource for MemoryHandoverSource {
    fn store_handover(
        &self,
        address: &signal_flow::Address,
        predecessor: &str,
    ) -> Result<Option<Vec<signal_flow::Section>>, StoreError> {
        use crate::store::handover::HoldsHandovers;
        self.0.handover_for_flow(address, predecessor)
    }
}

/// Every query is answered the same to every caller. The origin a caller is
/// read as is recorded on what it launches and changes no answer.
pub trait Dispatches {
    /// A query no calling process stands behind: its origin is Unbound.
    fn dispatch(&self, query: Query) -> Response {
        self.dispatch_from(query, Origin::Unbound)
    }
    /// A query from `origin`, the calling process as the Nexus derived it,
    /// which a launch records.
    fn dispatch_from(&self, query: Query, origin: Origin) -> Response;
    /// The flow whose handover an Address's Keep or Drop writes: the flow
    /// the Address is Awake as.
    fn handover_writer(
        &self,
        address: &signal_flow::Address,
    ) -> Result<crate::store::handover::HandoverWriter, signal_flow::HandoverRefusal>;
    fn keep_handover(&self, request: signal_flow::Keep_Data) -> Response;
    fn drop_handover(&self, request: signal_flow::Drop_Data) -> Response;
    fn glance_handover(&self, address: signal_flow::Address) -> Response;
    fn read_handover(&self, address: signal_flow::Address) -> Response;
    /// A meta query with no peer behind it: one that records who asked
    /// records process 0.
    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response {
        self.dispatch_meta_from(query, None)
    }
    /// A meta query from `resolver`, the meta peer as the Nexus read it.
    fn dispatch_meta_from(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response;
}

trait MeasuresHandoverTouch {
    /// The touch a write to `flow_id`'s handover leaves: that flow's
    /// context now.
    fn handover_touch(
        &self,
        flow_id: &str,
    ) -> Result<signal_flow::Touch, signal_flow::HandoverRefusal>;
}

impl MeasuresHandoverTouch for RunningNexus {
    fn handover_touch(
        &self,
        flow_id: &str,
    ) -> Result<signal_flow::Touch, signal_flow::HandoverRefusal> {
        use crate::reminding::CountsUnixSeconds;
        let tokens = self
            .store
            .context_size(flow_id)
            .map_err(|error| signal_flow::HandoverRefusal::Store(error.to_string()))?;
        let Some(tokens) = tokens else {
            return Ok(signal_flow::Touch::Unknown);
        };
        let timestamp = std::time::SystemTime::now().seconds_since_epoch();
        Ok(signal_flow::Touch::Known(signal_flow::Known_Data {
            flow_id: signal_flow::FlowId(flow_id.to_owned()),
            tokens: signal_flow::Tokens(tokens),
            timestamp: signal_flow::Timestamp(timestamp),
        }))
    }
}

impl Dispatches for RunningNexus {
    /// A query no calling process stands behind, taken as a connection
    /// takes it.
    fn dispatch(&self, query: Query) -> Response {
        match query {
            Query::Start(profile) => {
                if let Err(rejection) = self.harness_arguments(&profile.address) {
                    return Response::StartRejected(rejection);
                }
                self.dispatch_from(Query::Start(profile), Origin::Unbound)
            }
            Query::Launch(launch) => match self.declared_launch(launch) {
                Ok(query) => self.dispatch(query),
                Err(refusal) => *refusal,
            },
            Query::Rehearse(launch) => self.rehearse_declared(launch),
            query => self.dispatch_from(query, Origin::Unbound),
        }
    }

    fn dispatch_from(&self, query: Query, origin: Origin) -> Response {
        match query {
            Query::Acknowledge(keys) => {
                use crate::store::logical_delivery::RecordsLogicalDeliveries;
                match self.store.acknowledge_deliver_calls(&keys) {
                    Ok(()) => Response::Acknowledged,
                    Err(error) => Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                }
            }
            Query::ArchiveRequest(request_id) => {
                use crate::store::RecordsLogicalDeliveries;
                match self.store.archive_request(&request_id.0) {
                    Ok(Ok(())) => Response::RequestArchived(request_id),
                    Ok(Err(rejection)) => Response::ArchiveRejected(rejection),
                    Err(_) => {
                        Response::ArchiveRejected(signal_flow::ArchiveRejection::StoreRefused)
                    }
                }
            }
            Query::Start(profile) => {
                let launch_request_id = profile.launch_request_id.clone();
                if let Some(settled) = self.settled(&profile) {
                    return settled;
                }
                // A caller's new launch is admitted before anything of it is
                // written: a camelCase topic.
                match self.admission_refusal(&profile) {
                    Ok(None) => {}
                    Ok(Some(rejection)) => return Response::StartRejected(rejection),
                    Err(_) => {
                        return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                    }
                }
                let response = self.start(profile, origin);
                self.settle(&launch_request_id.0, response)
            }
            // Off the gate: the replacement takes it under the metaflow's
            // lock.
            Query::Replace(profile) => match self.admission_refusal(&profile) {
                Ok(None) => {
                    use crate::waking::RefreshesMetaflows;
                    self.replace_held(profile, origin)
                }
                Ok(Some(rejection)) => Response::ReplaceRejected(
                    signal_flow::ReplaceRejection::LaunchRefused(rejection),
                ),
                Err(_) => Response::ReplaceRejected(signal_flow::ReplaceRejection::LaunchRefused(
                    StartRejection::LaunchPersistenceRefused,
                )),
            },
            Query::DeliverAll(address) => self.deliver_all(address),
            Query::BindSeat(request) => self.bind_seat(request),
            Query::Adopt(request) => self.adopt(request),
            // An observation streams over a connection; a direct dispatch
            // answers its opening frame once, as a read.
            Query::ObserveSends(_) => {
                use crate::store::logical_delivery::RecordsLogicalDeliveries;
                self.store.read_every_delivery_snapshot(None).map_or_else(
                    |error| Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                    |observation| {
                        Response::SendsObserved(
                            crate::message_projection::ProjectsSendObservation::visible_send_observation(
                                &observation,
                            ),
                        )
                    },
                )
            }
            Query::ObserveSender(forwarded) => {
                use crate::observe_sends::ObservesLogicalSends;
                use crate::store::logical_delivery::RecordsLogicalDeliveries;
                let observed = self.observation_sender(&forwarded).and_then(|sender| {
                    self.store
                        .read_logical_delivery_snapshots(&sender, None)
                        .map_err(|error| {
                            Box::new(Response::Refused(signal_flow::Refusal::Store(
                                error.to_string(),
                            )))
                        })
                });
                match observed {
                    Ok(observation) => Response::SendsObserved(
                        crate::message_projection::ProjectsSendObservation::visible_send_observation(
                            &observation,
                        ),
                    ),
                    Err(refusal) => *refusal,
                }
            }
            Query::ObserveDeliveries(_) => {
                use crate::store::logical_delivery::RecordsLogicalDeliveries;
                self.store.read_message_delivery_snapshots(None).map_or_else(
                    |error| Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                    |observation| {
                        Response::SendsObserved(
                            crate::message_projection::ProjectsSendObservation::visible_send_observation(
                                &observation,
                            ),
                        )
                    },
                )
            }
            Query::Send(send) => self.send_request(send, None).response,
            Query::Launch(launch) => match self.declared_launch(launch) {
                Ok(query) => self.dispatch_from(query, origin),
                Err(refusal) => *refusal,
            },
            Query::Rehearse(launch) => self.rehearse_declared(launch),
            Query::LaunchStatus(launch_request_id) => self.launch_status(&launch_request_id.0),
            // Over a connection Observe streams; a direct dispatch answers the
            // subscription's opening frame.
            Query::Observe(ObserveSelection::Launch(launch_request_id)) => {
                self.launch_status(&launch_request_id.0)
            }
            Query::Observe(ObserveSelection::Agent(flow_id)) => self.agent_observation(&flow_id.0),
            Query::Observe(ObserveSelection::Undecodable) => {
                use crate::store::undecodable_frame::RecordsUndecodableFrames;
                match self.store.undecodable_frames() {
                    Ok(frames) => Response::UndecodableObserved(frames),
                    Err(error) => Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                }
            }
            Query::Observe(ObserveSelection::TurnEnds) => {
                use crate::store::turn_end::RecordsTurnEnds;
                match self.store.last_turn_ends() {
                    Ok(turn_ends) => Response::TurnEndsObserved(turn_ends),
                    Err(error) => Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                }
            }
            Query::Observe(ObserveSelection::Metaflows) => {
                use crate::store::metaflow_watch::WatchesMetaflows;
                match self.store.available_metaflows() {
                    Ok(metaflows) => Response::MetaflowsObserved(metaflows),
                    Err(error) => Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                }
            }
            Query::Observe(ObserveSelection::Retirement(address)) => {
                use crate::store::handover::HoldsHandovers;
                match self.store.observe_handover_retirement(&address) {
                    Ok(subscription) => {
                        let response = Response::RetirementObserved(subscription.snapshot.clone());
                        self.store.handover_observations.detach(&subscription);
                        response
                    }
                    Err(error) => Response::RetirementObservationFailed(
                        signal_flow::RetirementObservationFailure::MissingEvidence(
                            error.to_string(),
                        ),
                    ),
                }
            }
            // Off the gate: the refresh takes it for each of its steps.
            Query::Refresh(address) => {
                use crate::waking::RefreshesMetaflows;
                self.refresh(&address)
            }
            // Off the gate: the restart takes it for each of its steps.
            Query::Restart(flow_id) => {
                use crate::restarting::RestartsFlows;
                self.restart(&flow_id.0)
            }
            Query::Keep(request) => self.keep_handover(request),
            Query::Drop(request) => self.drop_handover(request),
            Query::Glance(address) => self.glance_handover(address),
            Query::Read(address) => self.read_handover(address),
            Query::ResolveRecipient(flow_id) => {
                match self.store.apply(Query::ResolveRecipient(flow_id)) {
                    Ok(Response::RecipientResolved(node)) => Response::RecipientResolved(
                        self.herdr
                            .refresh_route(self.herdr.claude_daemon.refreshed(node)),
                    ),
                    Ok(response) => response,
                    Err(error) => Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                }
            }
            Query::Stop(flow_id) => {
                let node = match self.store.flow_node(&flow_id.0) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        return Response::StopRejected(signal_flow::StopRejection::UnknownFlow);
                    }
                    Err(error) => {
                        return Response::Refused(signal_flow::Refusal::Store(error.to_string()));
                    }
                };
                // A flow under a restart record is stopped as its restart
                // stands: its FlowId, session and queue are kept.
                {
                    use crate::restarting::RestartsFlows;
                    use crate::store::restart::RecordsRestarts;
                    match self.store.restart_record(&flow_id.0) {
                        Ok(None) => {}
                        Ok(Some(record)) => return self.stop_restarting(record),
                        Err(error) => {
                            return Response::Refused(signal_flow::Refusal::Store(
                                error.to_string(),
                            ));
                        }
                    }
                }
                // A Retired seat is gone already, so there is nothing left
                // for Stop to close: AlreadyStopped is what it answers.
                if !node.flow_lifecycle.is_live() {
                    return Response::StopRejected(signal_flow::StopRejection::AlreadyStopped);
                }
                // The route is the flow's recorded Herdr pane: a Pending flow
                // whose launch was refused, or a seat not ready for a prompt,
                // is stopped when Herdr still shows its binding there.
                let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
                    return Response::StopRejected(signal_flow::StopRejection::RouteUnavailable);
                };
                match self.herdr.pane_presence(&node) {
                    herdr::PanePresence::Present | herdr::PanePresence::Bare => {
                        // No write is left half-typed into a pane as it
                        // closes.
                        let closed = {
                            let _lease = self.pane_leases.hold(route);
                            self.perform(Operation::Close(node.clone()))
                        };
                        if closed != Outcome::Closed {
                            return Response::StopRejected(
                                signal_flow::StopRejection::CloseRefused,
                            );
                        }
                    }
                    // Positively gone: the route is recorded and a readable
                    // snapshot, with its roster and its pane list, shows
                    // neither the binding nor the pane. The flow is recorded
                    // Stopped with no close, so a Stop retried after its
                    // record was refused re-runs the end and what it owes.
                    herdr::PanePresence::Absent => {}
                    herdr::PanePresence::Unknown => {
                        return Response::StopRejected(
                            signal_flow::StopRejection::RouteUnavailable,
                        );
                    }
                }
                if self.perform(Operation::Record(Record_Data::Stopped(flow_id.clone())))
                    != Outcome::Recorded
                {
                    return Response::StopRejected(signal_flow::StopRejection::PersistenceRefused);
                }
                Response::Stopped(flow_id)
            }
            // A dispatch without a connection has no peer to read.
            Query::ResolveCaller(claim) => self.resolve_caller(None, claim),
            Query::Report(report) => self.report(report),
            Query::Current(address) => {
                use crate::waking::RestsEndedAwake;
                self.rest_ended_awake(&address);
                self.store
                    .current_metaflow(&address)
                    .unwrap_or_else(|error| {
                        Response::Refused(signal_flow::Refusal::Store(error.to_string()))
                    })
            }
            query @ (Query::Bind(_)
            | Query::Identify(_)
            | Query::IdentifyThread(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_)) => {
                use crate::metaflow::DispatchesMetaflowPeer;
                self.dispatch_metaflow_peer(query, None).response
            }
            Query::Cancel(request_id) => self.cancel(&request_id),
            Query::Silence(request_id) => self.silence(&request_id),
            Query::Clear(address) => self.clear(&address),
            Query::ClearAll => self.clear_all(),
            Query::Push(push) => self.push(&push.address, &push.priority),
            Query::Queues => self.queues(),
            Query::Hot => self.hot(),
            // No calling process stands behind it: Unbound asks.
            Query::Remind(remind) => self.remind(remind, signal_flow::SentBy::Unbound),
            Query::Answer(request_id) => self.answer(&request_id),
            Query::Metaflows => {
                use crate::store::metaflow_watch::WatchesMetaflows;
                self.store.available_metaflows().map_or_else(
                    |error| Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                    Response::Listed,
                )
            }
        }
    }

    fn handover_writer(
        &self,
        address: &signal_flow::Address,
    ) -> Result<crate::store::handover::HandoverWriter, signal_flow::HandoverRefusal> {
        let Some(metaflow) = self
            .store
            .metaflow(address)
            .map_err(|error| signal_flow::HandoverRefusal::Store(error.to_string()))?
        else {
            return Err(signal_flow::HandoverRefusal::UnknownFlow);
        };
        let flow_ethos::PermaflowState::Awake(flow_id) = metaflow.permaflow_state else {
            return Err(signal_flow::HandoverRefusal::UnknownFlow);
        };
        Ok(crate::store::handover::HandoverWriter {
            address: address.clone(),
            flow_id: flow_id.0,
        })
    }

    fn keep_handover(&self, request: signal_flow::Keep_Data) -> Response {
        use crate::store::handover::HoldsHandovers;
        let writer = match self.handover_writer(&request.address) {
            Ok(writer) => writer,
            Err(error) => return Response::KeepRefused(error),
        };
        // [PSYCHE-3] (a): the receiving flow's own handover write takes up
        // the handover owed it first, once, in its own commit.
        crate::reaping::TakesUpObligations::take_up_at_handover_write(
            self,
            &writer.address,
            &writer.flow_id,
        );
        let title = request.section.title.clone();
        let touch = match self.handover_touch(&writer.flow_id) {
            Ok(touch) => touch,
            Err(error) => return Response::KeepRefused(error),
        };
        match self
            .store
            .keep_handover_for_flow(&writer, request.section, touch)
        {
            Ok(touch) => Response::Kept(signal_flow::Kept_Data {
                address: request.address,
                title,
                touch,
            }),
            Err(error) => Response::KeepRefused(error),
        }
    }

    fn drop_handover(&self, request: signal_flow::Drop_Data) -> Response {
        use crate::store::handover::HoldsHandovers;
        let writer = match self.handover_writer(&request.address) {
            Ok(writer) => writer,
            Err(error) => return Response::DropRefused(error),
        };
        // [PSYCHE-3] (a): the receiving flow's own handover write takes up
        // the handover owed it first, once, in its own commit.
        crate::reaping::TakesUpObligations::take_up_at_handover_write(
            self,
            &writer.address,
            &writer.flow_id,
        );
        let title = request.title.clone();
        let touch = match self.handover_touch(&writer.flow_id) {
            Ok(touch) => touch,
            Err(error) => return Response::DropRefused(error),
        };
        match self.store.drop_handover_for_flow(&writer, title, touch) {
            Ok(touch) => Response::Dropped(signal_flow::Dropped_Data {
                address: request.address,
                title: request.title,
                touch,
            }),
            Err(error) => Response::DropRefused(error),
        }
    }

    fn glance_handover(&self, address: signal_flow::Address) -> Response {
        use crate::store::handover::HoldsHandovers;
        match self.store.handover_headings(&address) {
            Ok(value) => Response::Glanced(value),
            Err(error) => {
                Response::GlanceRefused(signal_flow::HandoverRefusal::Store(error.to_string()))
            }
        }
    }

    fn read_handover(&self, address: signal_flow::Address) -> Response {
        use crate::store::handover::HoldsHandovers;
        match self.store.handover(&address) {
            Ok(value) => Response::Handover(value),
            Err(error) => {
                Response::ReadRefused(signal_flow::HandoverRefusal::Store(error.to_string()))
            }
        }
    }

    fn dispatch_meta_from(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response {
        match query {
            meta_signal_flow::Query::DebugDelivery(request_id) => {
                use crate::store::logical_delivery::RecordsLogicalDeliveries;
                match self.store.debug_delivery(&request_id) {
                    Ok(Some(delivery)) => meta_signal_flow::Response::DeliveryDebugged(delivery),
                    Ok(None) => meta_signal_flow::Response::DebugDeliveryRejected(
                        meta_signal_flow::DebugDeliveryRejected_Data::UnknownRequest,
                    ),
                    Err(_) => meta_signal_flow::Response::DebugDeliveryRejected(
                        meta_signal_flow::DebugDeliveryRejected_Data::StoreRefused,
                    ),
                }
            }
            // A direct dispatch of the subscription answers its first frame;
            // a meta connection streams the rest (`serve_meta`).
            meta_signal_flow::Query::Configuration
            | meta_signal_flow::Query::ObserveConfiguration => {
                use observe_configuration::ObservesConfiguration;
                self.configuration_response()
            }
            // A declared family replaces the committed family whole, and
            // this Nexus takes the changed configuration at once.
            meta_signal_flow::Query::Configure(change) => {
                self.configure_meta(store::ConfigurationEdit::Declare(Box::new(change)))
            }
            meta_signal_flow::Query::ConsumeReset(request) => self
                .codex_endpoints
                .current()
                .next
                .model_names
                .first()
                .ok_or(())
                .and_then(|model| {
                    self.codex_endpoints
                        .current()
                        .adapter_for(model)
                        .map_err(|_| ())
                })
                .and_then(|adapter| adapter.consume_reset_credit(&request).map_err(|_| ()))
                .map(meta_signal_flow::Response::ResetConsumed)
                .unwrap_or(meta_signal_flow::Response::ResetRejected(
                    meta_signal_flow::ResetRejection::AdapterUnavailable,
                )),
            // A seat Flow lost. Retire keeps the row, its origin and its
            // history, and takes the flow out of receiving: it is not a
            // delete. No pane outlives its flow: a pane still there, its
            // harness running the flow's own session or none, is closed in
            // the same event, under its lease. A close Herdr refuses
            // answers CloseRefused and records nothing: the flow is not
            // retired while its pane stays. The flow a Maintained metaflow
            // is Awake as is not retired: Retire answers Maintained and
            // changes nothing, and the declaration's continuity is changed
            // first. Privileged, because it changes what Flow holds of a
            // flow without that flow acting.
            meta_signal_flow::Query::Retire(flow_id) => {
                let node = match self.store.flow_node(&flow_id.0) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        return meta_signal_flow::Response::RetireRejected(
                            meta_signal_flow::RetireRejection::UnknownFlow,
                        );
                    }
                    Err(_) => {
                        return meta_signal_flow::Response::RetireRejected(
                            meta_signal_flow::RetireRejection::StoreRefused,
                        );
                    }
                };
                if !node.flow_lifecycle.is_live() {
                    return meta_signal_flow::Response::RetireRejected(
                        meta_signal_flow::RetireRejection::AlreadyGone,
                    );
                }
                // A restart that may still start a harness, or is running,
                // keeps its Address: only a Failed one is retired, and its
                // record goes with the flow.
                {
                    use crate::store::restart::RecordsRestarts;
                    match self.store.restart_record(&flow_id.0) {
                        Ok(None) => {}
                        Ok(Some(record))
                            if matches!(
                                record.restart_phase,
                                meta_signal_flow::RestartPhase::Failed(_)
                            ) => {}
                        Ok(Some(_)) => {
                            return meta_signal_flow::Response::RetireRejected(
                                meta_signal_flow::RetireRejection::RestartUnresolved,
                            );
                        }
                        Err(_) => {
                            return meta_signal_flow::Response::RetireRejected(
                                meta_signal_flow::RetireRejection::StoreRefused,
                            );
                        }
                    }
                }
                {
                    use crate::store::metaflow::SleepsMetaflows;
                    use crate::store::successor::ReadsContinuity;
                    let maintained = self.store.metaflow_awake_as(&flow_id.0).and_then(|awake| {
                        awake
                            .map(|metaflow| self.store.continuity(&metaflow.address))
                            .transpose()
                    });
                    match maintained {
                        Ok(Some(meta_signal_flow::Continuity::Maintained)) => {
                            return meta_signal_flow::Response::RetireRejected(
                                meta_signal_flow::RetireRejection::Maintained,
                            );
                        }
                        Ok(_) => {}
                        Err(_) => {
                            return meta_signal_flow::Response::RetireRejected(
                                meta_signal_flow::RetireRejection::StoreRefused,
                            );
                        }
                    }
                }
                if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection
                    && matches!(
                        self.herdr.pane_presence(&node),
                        herdr::PanePresence::Present | herdr::PanePresence::Bare
                    )
                {
                    let closed = {
                        let _lease = self.pane_leases.hold(route);
                        self.perform(Operation::Close(node.clone()))
                    };
                    if closed != Outcome::Closed {
                        return meta_signal_flow::Response::RetireRejected(
                            meta_signal_flow::RetireRejection::CloseRefused,
                        );
                    }
                }
                if self.perform(Operation::Record(Record_Data::Retired(flow_id.clone())))
                    != Outcome::Recorded
                {
                    return meta_signal_flow::Response::RetireRejected(
                        meta_signal_flow::RetireRejection::StoreRefused,
                    );
                }
                match self.store.flow_node(&flow_id.0) {
                    Ok(Some(retired)) => meta_signal_flow::Response::FlowRetired(retired),
                    Ok(None) | Err(_) => meta_signal_flow::Response::RetireRejected(
                        meta_signal_flow::RetireRejection::StoreRefused,
                    ),
                }
            }
            // The meta surface restarts and refreshes as the ordinary socket
            // does; each runs off the gate and takes it for its steps.
            meta_signal_flow::Query::Restart(flow_id) => {
                use crate::restarting::RestartsFlows;
                self.restart(&flow_id.0).into_meta()
            }
            meta_signal_flow::Query::Refresh(address) => {
                use crate::waking::RefreshesMetaflows;
                self.refresh(&address).into_meta()
            }
            meta_signal_flow::Query::MetaBindExisting(request) => {
                let container = request.flow_container;
                if !container.is_well_formed() || !container.socket_is_live() {
                    return meta_signal_flow::Response::BindExistingRejected(
                        meta_signal_flow::BindExistingRejection::ContainerUnavailable,
                    );
                }
                if !container.process_identity.is_live_process() {
                    return meta_signal_flow::Response::BindExistingRejected(
                        meta_signal_flow::BindExistingRejection::ContainerIdentityMismatch,
                    );
                }

                let binding = request.flow_binding;
                let flow_id = binding.flow_id.clone();
                let refusal = |reason| {
                    meta_signal_flow::FlowBindingResult::Refused(
                        meta_signal_flow::RefusedFlowBinding {
                            flow_id: flow_id.clone(),
                            flow_binding_refusal_reason: reason,
                        },
                    )
                };
                let result = match self.verified_seat_node(
                    &container.herdr_session_name.0,
                    &request.address,
                    &binding,
                ) {
                    Err(reason) => refusal(reason),
                    Ok(node) => {
                        let role = signal_flow::CallerIdentity {
                            flow_id: flow_id.clone(),
                            aspect: binding.aspect.clone(),
                            model_name: binding.model_name.clone(),
                        };
                        match self
                            .store
                            .adopt_existing_flow(node, role, request.address, &binding)
                        {
                            Ok(store::ExistingSeatAdoption::Bound(_)) => {
                                meta_signal_flow::FlowBindingResult::Bound(
                                    meta_signal_flow::BoundFlowBinding {
                                        flow_id,
                                        flow_lifecycle:
                                            meta_signal_flow::FlowLifecycle::AdoptedActive,
                                    },
                                )
                            }
                            Ok(store::ExistingSeatAdoption::Refused(reason)) => refusal(reason),
                            Err(_) => {
                                return meta_signal_flow::Response::BindExistingRejected(
                                    meta_signal_flow::BindExistingRejection::StoreRefused,
                                );
                            }
                        }
                    }
                };
                meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                    flow_container: container,
                    flow_binding_result: result,
                })
            }
            meta_signal_flow::Query::Deliver(request) => self.deliver(request),
            meta_signal_flow::Query::Vet(request) => self.vet(&request),
            meta_signal_flow::Query::Command(request) => self.command(request),
            meta_signal_flow::Query::ResolvePeer(identity) => self.resolve_peer(&identity),
            meta_signal_flow::Query::ReadEvents(flow_id) => self.read_events(flow_id),
            meta_signal_flow::Query::ReadQueueDeliveries(address) => {
                self.read_queue_deliveries(address)
            }
            meta_signal_flow::Query::ObserveLegacyArchive => {
                use store::queue_delivery::AccountsQueueDelivery;
                match self.store.observe_legacy_archive() {
                    Ok(archive) => meta_signal_flow::Response::LegacyArchiveObserved(
                        archive
                            .iter()
                            .map(crate::message_projection::ProjectsLegacyArchiveEntry::visible_legacy_archive_entry)
                            .collect(),
                    ),
                    Err(error) => meta_signal_flow::Response::ObserveLegacyArchiveRejected(
                        meta_signal_flow::StoreRefusal::Store(error.to_string()),
                    ),
                }
            }
            meta_signal_flow::Query::ReadSendOutcomes => match self.store.read_send_outcomes() {
                Ok(records) => meta_signal_flow::Response::SendOutcomesRead(
                    records
                        .iter()
                        .map(crate::message_projection::ProjectsSendOutcomeRecord::visible_send_outcome_record)
                        .collect(),
                ),
                Err(_) => meta_signal_flow::Response::ReadSendOutcomesRejected(
                    meta_signal_flow::ReadSendOutcomesRejected_Data::StoreRefused,
                ),
            },
            meta_signal_flow::Query::Debug(address) => match self.store.debug_snapshot(&address) {
                Ok(snapshot) => meta_signal_flow::Response::Debugged(snapshot),
                Err(error) => meta_signal_flow::Response::Refused(
                    meta_signal_flow::StoreRefusal::Store(error.to_string()),
                ),
            },
            // Whoever resolves it is recorded; a peer that could not be
            // read is process 0.
            meta_signal_flow::Query::ResolveQueueDelivery(resolution) => {
                use crate::peer::NamesMetaPeer;
                let resolver = resolver.unwrap_or_else(|| self.meta_resolver(None));
                self.resolve_queue_delivery(resolution, resolver)
            }
            meta_signal_flow::Query::Remove(removal) => {
                self.configure_meta(store::ConfigurationEdit::Remove(removal))
            }
            meta_signal_flow::Query::Inspect(address) => {
                use crate::repairing::RepairsFromMeta;
                self.inspect(address)
            }
            meta_signal_flow::Query::Rebind(rebind) => {
                use crate::repairing::RepairsFromMeta;
                self.rebind(rebind)
            }
            meta_signal_flow::Query::Withdraw(withdraw) => {
                use crate::repairing::RepairsFromMeta;
                self.withdraw(withdraw)
            }
            meta_signal_flow::Query::RegisterFlow(flow_node) => {
                if !self.herdr.validate_registration(&flow_node) {
                    return meta_signal_flow::Response::FlowRegistrationRejected(
                        meta_signal_flow::FlowRegistrationRejection::UnknownOrUnclaimedIdentity,
                    );
                }
                match self.store.register_flow(flow_node) {
                    Ok(store::FlowRegistration::Registered(node)) => {
                        meta_signal_flow::Response::FlowRegistered(*node)
                    }
                    Ok(store::FlowRegistration::ConflictingBinding) => {
                        meta_signal_flow::Response::FlowRegistrationRejected(
                            meta_signal_flow::FlowRegistrationRejection::ConflictingBinding,
                        )
                    }
                    Err(_) => meta_signal_flow::Response::FlowRegistrationRejected(
                        meta_signal_flow::FlowRegistrationRejection::StoreRefused,
                    ),
                }
            }
        }
    }
}

#[cfg(feature = "executable-default-configuration")]
trait ChecksExecutableDefault {
    fn validate_executable_default_configuration(&self) -> Result<(), store::StoreError>;
}

#[cfg(feature = "executable-default-configuration")]
impl ChecksExecutableDefault for meta_signal_flow::ConfigureNexus {
    fn validate_executable_default_configuration(&self) -> Result<(), store::StoreError> {
        let configuration = self;
        // The first Codex endpoint is the stable one, the second the next.
        let stable_codex = &configuration.first_codex_endpoint;
        let next_codex = &configuration.second_codex_endpoint;
        let required_paths = [
            configuration.source_root.0.as_str(),
            stable_codex.client_path.0.as_str(),
            stable_codex.home.0.as_str(),
            stable_codex.control_socket_path.0.as_str(),
            next_codex.client_path.0.as_str(),
            next_codex.home.0.as_str(),
            next_codex.control_socket_path.0.as_str(),
            configuration.curriculum_nexus_path.0.as_str(),
        ];
        if required_paths
            .iter()
            .any(|path| !Path::new(path).is_absolute())
            || stable_codex.model_name_vector.is_empty()
            || configuration.harness_profiles.0.is_empty()
            || configuration.meta_aspects.0.is_empty()
            || configuration.lease.0 <= 0
        {
            return Err(store::StoreError::MissingDefaultConfiguration);
        }
        Ok(())
    }
}

pub trait OpensRunningNexus {
    /// Opens the Nexus in its place: the memory in State, the socket its
    /// flows report to in Runtime. Durable runner values come from the
    /// committed Meta configuration snapshot; a fresh memory starts with
    /// the configuration built into the executable.
    fn open(place: &place::Place) -> Result<Self, store::StoreError>
    where
        Self: Sized;
}

impl OpensRunningNexus for RunningNexus {
    fn open(place: &place::Place) -> Result<Self, store::StoreError> {
        use place::LaysOutPlace;
        let store_path = place.store();
        let store_path = store_path.as_path();
        #[cfg(feature = "executable-default-configuration")]
        if !store_path.exists() {
            let default = executable_default_configuration::executable_default_configuration();
            default.validate_executable_default_configuration()?;
            let configuration_store = store::ConfigurationOnlyStore::open(store_path)?;
            if configuration_store.configuration_snapshot()?.is_none() {
                configuration_store
                    .configure_change(meta_signal_flow::ConfigurationChange::Nexus(default))?;
            }
            drop(configuration_store);
        }
        // The configured opener checks the typed store path read-only first,
        // reads the committed snapshot, and only then registers runtime tables.
        // It never seeds defaults or settles work, and refuses a store an
        // earlier Flow wrote.
        let store = Arc::new(FlowStore::open_configured(store_path)?);
        let snapshot = store
            .configuration_snapshot()?
            .ok_or(store::StoreError::NotConfigured)?;
        let configured = ConfiguredRunner::from(&snapshot);
        // The socket this Nexus serves is its place's, never the store's.
        let ordinary_socket = place.ordinary_socket();
        Ok(Self {
            store: store.clone(),
            codex_endpoints: Live::holding(configured.codex_endpoints.clone()),
            herdr: herdr::HerdrCli::default()
                .with_source_root(&configured.source_root)
                .with_codex_endpoints(configured.codex_endpoints.clone())
                .with_harness_hook(&snapshot.configured_harness_hook.0)
                .with_ordinary_socket(&ordinary_socket),
            // A launch's context is composed by the Curriculum Nexus at the
            // socket the configuration names.
            composer: Live::holding(configured.composer(store.clone())),
            dispatch_gate: Mutex::new(()),
            pane_leases: delivery::lease::PaneLeases::default(),
            waking: waking::WakingMetaflows::default(),
            stopped_at_confirmation: waking::StoppedAtConfirmation::default(),
            refresh_refusals: waking::RefreshRefusals::default(),
            pane_watch_rearms: Default::default(),
            #[cfg(test)]
            pane_watch_hooks: Default::default(),
            stopping: Stopping::default(),
            launch_calls: send_tool::LaunchCalls::default(),
            codex_turns: codex_turns::CodexTurns::default(),
            turn_ends: turn_ends::TurnEndSubscribers::default(),
            compactions: compacting::CompactionWatch::default(),
            seat_submissions: Arc::default(),
            configuration_changes: store::LaunchChanges::default(),
        })
    }
}

/// What a running Nexus derives from its committed configuration and
/// holds between uses: the Codex endpoints, the source root its flows
/// directory lies under, and the composer's Curriculum socket and
/// presentation prompt.
struct ConfiguredRunner {
    codex_endpoints: CodexEndpoints,
    source_root: PathBuf,
    curriculum_nexus_path: String,
    presentation_prompt_option: Option<String>,
}

impl From<&meta_signal_flow::ConfigurationSnapshot> for ConfiguredRunner {
    fn from(snapshot: &meta_signal_flow::ConfigurationSnapshot) -> Self {
        let runtime = store::RuntimeConfiguration::from(&snapshot.configured_nexus.0);
        let codex_endpoints = CodexEndpoints::from(&runtime);
        let overlap = codex_endpoints.overlapping_models();
        if !overlap.is_empty() {
            eprintln!(
                "flow-nexus: models selected by both Codex endpoints are unavailable: {overlap:?}"
            );
        }
        Self {
            codex_endpoints,
            source_root: PathBuf::from(&runtime.source_root),
            curriculum_nexus_path: snapshot.configured_nexus.0.curriculum_nexus_path.clone().0,
            presentation_prompt_option: snapshot
                .configured_nexus
                .0
                .presentation_prompt_option
                .clone()
                .map(|value| value.0),
        }
    }
}

trait ComposesFromConfiguration {
    fn composer(&self, store: Arc<FlowStore>) -> LaunchComposer;
}

impl ComposesFromConfiguration for ConfiguredRunner {
    fn composer(&self, store: Arc<FlowStore>) -> LaunchComposer {
        LaunchComposer::with_presentation_prompt(
            context::ContextComposer(std::sync::Arc::new(CurriculumComposer::at(
                &self.curriculum_nexus_path,
            ))),
            self.presentation_prompt_option.clone(),
        )
        .with_handover_source(Arc::new(MemoryHandoverSource(store)))
    }
}

/// The running Nexus takes its committed configuration as it changes: a
/// Configure that replaces a family is applied before it is answered, so
/// the next launch, delivery or hook it writes reads the new value. Every
/// other family is read from the store at each use already.
pub trait AppliesConfiguration {
    /// Applies what changed between `before` (none: everything) and the
    /// committed configuration now.
    fn apply_configuration(
        &self,
        before: Option<&meta_signal_flow::ConfigurationSnapshot>,
    ) -> Result<(), store::StoreError>;
}

impl AppliesConfiguration for RunningNexus {
    fn apply_configuration(
        &self,
        before: Option<&meta_signal_flow::ConfigurationSnapshot>,
    ) -> Result<(), store::StoreError> {
        let now = self
            .store
            .configuration_snapshot()?
            .ok_or(store::StoreError::NotConfigured)?;
        let configured = ConfiguredRunner::from(&now);
        let launching_changed = before.is_none_or(|before| {
            before.configured_nexus.0.source_root != now.configured_nexus.0.source_root
                || before.configured_nexus.0.first_codex_endpoint
                    != now.configured_nexus.0.first_codex_endpoint
                || before.configured_nexus.0.second_codex_endpoint
                    != now.configured_nexus.0.second_codex_endpoint
                || before.configured_harness_hook != now.configured_harness_hook
        });
        let composing_changed = before.is_none_or(|before| {
            before.configured_nexus.0.curriculum_nexus_path
                != now.configured_nexus.0.curriculum_nexus_path
                || before.configured_nexus.0.presentation_prompt_option
                    != now.configured_nexus.0.presentation_prompt_option
        });
        if launching_changed {
            self.herdr.reconfigure(
                configured.codex_endpoints.clone(),
                &configured.source_root,
                &now.configured_harness_hook.0,
            );
            self.codex_endpoints
                .replace(configured.codex_endpoints.clone());
        }
        if composing_changed {
            self.composer
                .replace(configured.composer(self.store.clone()));
        }
        Ok(())
    }
}

/// One accepted connection: read one frame, answer it, close — except
/// Observe, which keeps the connection as its subscription until the
/// outcome frame. A malformed frame, an idle peer past the read timeout, or
/// a failed write drops only this connection.
pub struct Connection {
    peer: UnixStream,
    /// PID and kernel start token captured when this socket was accepted.
    /// Privileged ordinary operations recheck both before using the peer.
    process: Option<Process>,
}

pub trait ServesConnection: Sized {
    const READ_TIMEOUT: Duration = Duration::from_secs(5);
    const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
    fn accepted(peer: UnixStream) -> Result<Self, String>;
    /// ObserveSends, ObserveSender and ObserveDeliveries: the selected
    /// sends on open, then each change, until the peer goes.
    fn serve_sends(
        &mut self,
        nexus: &RunningNexus,
        selection: &crate::observe_sends::SendSelection,
        cursor: Option<i64>,
    ) -> Result<(), String>;
    fn serve_retirement(
        &mut self,
        nexus: &RunningNexus,
        subscription: RetirementSubscription,
    ) -> Result<(), String>;
    /// Observe.Undecodable: every recorded undecodable frame, then each new
    /// one, until the peer goes.
    fn serve_undecodable(&mut self, nexus: &RunningNexus) -> Result<(), String>;
    /// Observe.TurnEnds: the last turn end of each harness, then each
    /// turn end, until the peer goes.
    fn serve_turn_ends(&mut self, nexus: &RunningNexus) -> Result<(), String>;
    /// Observe.Metaflows: the metaflows, then each change, until the peer
    /// goes.
    fn serve_metaflows(&mut self, nexus: &RunningNexus) -> Result<(), String>;
    /// ObserveConfiguration on the meta socket: the configuration, then
    /// again at each change, until the peer goes.
    fn serve_configuration(&mut self, nexus: &RunningNexus) -> Result<(), String>;
    fn serve_ordinary(self, nexus: &RunningNexus) -> Result<(), String>;
    fn serve_meta(self, nexus: &RunningNexus) -> Result<(), String>;
}

trait ServesAttachedRetirement {
    fn serve_attached_retirement(
        &mut self,
        subscription: &RetirementSubscription,
    ) -> Result<(), String>;
}

impl ServesConnection for Connection {
    fn accepted(peer: UnixStream) -> Result<Self, String> {
        peer.set_read_timeout(Some(Self::READ_TIMEOUT))
            .and_then(|_| peer.set_write_timeout(Some(Self::WRITE_TIMEOUT)))
            .map_err(|error| error.to_string())?;
        let process = peer
            .peer_process()
            .and_then(|peer| FlowStore::process_identity_for_pid(peer.process_id));
        Ok(Self { peer, process })
    }

    fn serve_sends(
        &mut self,
        nexus: &RunningNexus,
        selection: &crate::observe_sends::SendSelection,
        cursor: Option<i64>,
    ) -> Result<(), String> {
        let mut reader = self.peer.try_clone().map_err(|error| error.to_string())?;
        reader
            .set_read_timeout(None)
            .map_err(|error| error.to_string())?;
        let disconnected = Arc::new(AtomicBool::new(false));
        let watch_disconnected = Arc::clone(&disconnected);
        let changes = nexus.store.launch_changes.clone();
        let watcher = std::thread::Builder::new()
            .name("flow-send-observer-peer".into())
            .spawn(move || {
                let mut bytes = [0_u8; 256];
                loop {
                    match reader.read(&mut bytes) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                watch_disconnected.store(true, Ordering::Release);
                changes.announce();
            })
            .map_err(|error| error.to_string())?;

        let result =
            nexus.observe_delivery_changes(selection, cursor, &disconnected, &mut |response| {
                self.peer.write_response(response)
            });
        let _ = self.peer.shutdown(Shutdown::Both);
        let watcher_result = watcher
            .join()
            .map_err(|_| "peer-disconnect watcher panicked".to_owned());
        result.and(watcher_result)
    }

    fn serve_undecodable(&mut self, nexus: &RunningNexus) -> Result<(), String> {
        use crate::undecodable::ObservesUndecodableFrames;
        let mut reader = self.peer.try_clone().map_err(|error| error.to_string())?;
        reader
            .set_read_timeout(None)
            .map_err(|error| error.to_string())?;
        let disconnected = Arc::new(AtomicBool::new(false));
        let watch_disconnected = Arc::clone(&disconnected);
        let changes = nexus.store.launch_changes.clone();
        let watcher = std::thread::Builder::new()
            .name("flow-undecodable-observer-peer".into())
            .spawn(move || {
                let mut bytes = [0_u8; 256];
                loop {
                    match reader.read(&mut bytes) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                watch_disconnected.store(true, Ordering::Release);
                changes.announce();
            })
            .map_err(|error| error.to_string())?;
        let result = nexus.observe_undecodable(&disconnected, &mut |response| {
            self.peer.write_response(response)
        });
        let _ = self.peer.shutdown(Shutdown::Both);
        let watcher_result = watcher
            .join()
            .map_err(|_| "peer-disconnect watcher panicked".to_owned());
        result.and(watcher_result)
    }

    fn serve_turn_ends(&mut self, nexus: &RunningNexus) -> Result<(), String> {
        use crate::turn_ends::ObservesTurnEnds;
        let mut reader = self.peer.try_clone().map_err(|error| error.to_string())?;
        reader
            .set_read_timeout(None)
            .map_err(|error| error.to_string())?;
        let disconnected = Arc::new(AtomicBool::new(false));
        let watch_disconnected = Arc::clone(&disconnected);
        let changes = nexus.store.launch_changes.clone();
        let watcher = std::thread::Builder::new()
            .name("flow-turn-end-observer-peer".into())
            .spawn(move || {
                let mut bytes = [0_u8; 256];
                loop {
                    match reader.read(&mut bytes) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                watch_disconnected.store(true, Ordering::Release);
                changes.announce();
            })
            .map_err(|error| error.to_string())?;
        let result = nexus.observe_turn_ends(&disconnected, &mut |response| {
            self.peer.write_response(response)
        });
        let _ = self.peer.shutdown(Shutdown::Both);
        let watcher_result = watcher
            .join()
            .map_err(|_| "peer-disconnect watcher panicked".to_owned());
        result.and(watcher_result)
    }

    fn serve_metaflows(&mut self, nexus: &RunningNexus) -> Result<(), String> {
        use crate::observe_metaflows::ObservesMetaflows;
        let mut reader = self.peer.try_clone().map_err(|error| error.to_string())?;
        reader
            .set_read_timeout(None)
            .map_err(|error| error.to_string())?;
        let disconnected = Arc::new(AtomicBool::new(false));
        let watch_disconnected = Arc::clone(&disconnected);
        let changes = nexus.store.metaflow_changes.clone();
        let watcher = std::thread::Builder::new()
            .name("flow-metaflow-observer-peer".into())
            .spawn(move || {
                let mut bytes = [0_u8; 256];
                loop {
                    match reader.read(&mut bytes) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                watch_disconnected.store(true, Ordering::Release);
                changes.announce();
            })
            .map_err(|error| error.to_string())?;
        let result = nexus.observe_metaflows(&disconnected, &mut |response| {
            self.peer.write_response(response)
        });
        let _ = self.peer.shutdown(Shutdown::Both);
        let watcher_result = watcher
            .join()
            .map_err(|_| "peer-disconnect watcher panicked".to_owned());
        result.and(watcher_result)
    }

    fn serve_configuration(&mut self, nexus: &RunningNexus) -> Result<(), String> {
        use crate::observe_configuration::ObservesConfiguration;
        let mut reader = self.peer.try_clone().map_err(|error| error.to_string())?;
        reader
            .set_read_timeout(None)
            .map_err(|error| error.to_string())?;
        let disconnected = Arc::new(AtomicBool::new(false));
        let watch_disconnected = Arc::clone(&disconnected);
        let changes = nexus.configuration_changes.clone();
        let watcher = std::thread::Builder::new()
            .name("flow-configuration-observer-peer".into())
            .spawn(move || {
                let mut bytes = [0_u8; 256];
                loop {
                    match reader.read(&mut bytes) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                watch_disconnected.store(true, Ordering::Release);
                changes.announce();
            })
            .map_err(|error| error.to_string())?;
        let result = nexus.observe_configuration(&disconnected, &mut |response| {
            self.peer.write_meta_response(response)
        });
        let _ = self.peer.shutdown(Shutdown::Both);
        let watcher_result = watcher
            .join()
            .map_err(|_| "peer-disconnect watcher panicked".to_owned());
        result.and(watcher_result)
    }

    fn serve_retirement(
        &mut self,
        nexus: &RunningNexus,
        subscription: RetirementSubscription,
    ) -> Result<(), String> {
        let result = self.serve_attached_retirement(&subscription);
        nexus.store.handover_observations.detach(&subscription);
        result
    }

    fn serve_ordinary(mut self, nexus: &RunningNexus) -> Result<(), String> {
        use crate::undecodable::ReadsOrdinaryFrames;
        // Every ordinary connection greets first: nothing is decoded as a
        // Query before this contract is settled.
        match self.greet::<signal_flow::Query>(nexus) {
            Ok(()) => {}
            Err(GreetingEnd::Refused) => return Err("ordinary greeting refused".into()),
            Err(GreetingEnd::Transport(error)) => return Err(error),
        }
        let frame = self.peer.read_frame()?;
        // A frame that decodes as no Query, an earlier hook's Report among
        // them, is refused in this contract's words and recorded.
        let query =
            match nexus.ordinary_query(&frame, self.process.as_ref(), self.peer.peer_process()) {
                Ok(query) => query,
                Err(refused) => return self.peer.write_response(&refused),
            };
        let response = match query {
            // Reads of launch state never wait behind a running launch.
            Query::LaunchStatus(launch_request_id) => nexus.launch_status(&launch_request_id.0),
            Query::Observe(ObserveSelection::Launch(launch_request_id)) => {
                use crate::observation_lifetime::ServesCancellableObservation;
                return self.serve_cancellable_observation(nexus, &mut |cancellation, peer| {
                    nexus.observe_launch(&launch_request_id.0, cancellation, &mut |response| {
                        peer.write_response(response)
                    })
                });
            }
            Query::Observe(ObserveSelection::Agent(flow_id)) => {
                use crate::observation_lifetime::ServesCancellableObservation;
                return self.serve_cancellable_observation(nexus, &mut |cancellation, peer| {
                    nexus.observe_agent(&flow_id.0, cancellation, &mut |response| {
                        peer.write_response(response)
                    })
                });
            }
            Query::Observe(ObserveSelection::Undecodable) => {
                return self.serve_undecodable(nexus);
            }
            Query::Observe(ObserveSelection::TurnEnds) => {
                return self.serve_turn_ends(nexus);
            }
            Query::Observe(ObserveSelection::Metaflows) => {
                return self.serve_metaflows(nexus);
            }
            Query::Observe(ObserveSelection::Retirement(address)) => {
                use crate::store::handover::HoldsHandovers;
                let subscription = match nexus.store.observe_handover_retirement(&address) {
                    Ok(subscription) => subscription,
                    Err(error) => {
                        return self
                            .peer
                            .write_response(&Response::RetirementObservationFailed(
                                signal_flow::RetirementObservationFailure::MissingEvidence(
                                    error.to_string(),
                                ),
                            ));
                    }
                };
                return self.serve_retirement(nexus, subscription);
            }
            // Every send, whoever sent it.
            Query::ObserveSends(cursor) => {
                return self.serve_sends(
                    nexus,
                    &crate::observe_sends::SendSelection::Every,
                    cursor.map(|value| value.0),
                );
            }
            // The sends of the process or thread the query names.
            Query::ObserveSender(forwarded) => {
                let sender = match nexus.observation_sender(&forwarded) {
                    Ok(sender) => sender,
                    Err(refusal) => return self.peer.write_response(&refusal),
                };
                return self.serve_sends(
                    nexus,
                    &crate::observe_sends::SendSelection::Sent(sender),
                    forwarded.delivery_cursor_option.map(|value| value.0),
                );
            }
            Query::ObserveDeliveries(cursor) => {
                return self.serve_sends(
                    nexus,
                    &crate::observe_sends::SendSelection::Message,
                    cursor.map(|value| value.0),
                );
            }
            // A hook's Report never waits behind a running launch: the
            // launch may itself be waiting on the harness whose hook this is.
            // What the report sets going runs after the hook has its answer
            // and the connection is closed: the harness waits on its hook,
            // and the settling waits on the harness.
            // The Report names the harness that ran its hook by its birth:
            // a seat a restart bound acts only on events of its own harness.
            // The answer is Reported whoever reports.
            Query::Report(report) => {
                use crate::restarting::FencesSeatEvents;
                let (flow_id, event) = (report.flow_id.clone(), report.event.clone());
                let acts = nexus.report_acts(&flow_id.0, report.process_option.as_ref());
                let response = nexus.report(report);
                self.peer.write_response(&response)?;
                drop(self);
                if response == Response::Reported {
                    if acts {
                        nexus.after_attributed_report(&flow_id, &event);
                    } else {
                        eprintln!(
                            "flow-nexus: a report for flow {flow_id} is not its bound seat's; it acts on nothing",
                            flow_id = flow_id.0
                        );
                    }
                }
                return Ok(());
            }
            // The caller is the peer of this connection, read from the kernel.
            Query::ResolveCaller(claim) => nexus.resolve_caller(
                self.peer
                    .peer_process()
                    .and_then(|process| process.caller_pane(&nexus.herdr)),
                claim,
            ),
            query @ (Query::Bind(_)
            | Query::Identify(_)
            | Query::IdentifyThread(_)
            | Query::Lock(_)
            | Query::Release(_)
            | Query::ArchiveRequest(_)
            | Query::Acknowledge(_)) => {
                // The bridge operations, from any caller.
                let answer = nexus.dispatch_metaflow_peer(query, self.process.as_ref());
                return self.peer.write_response(&answer.response);
            }
            Query::Deliver(request) => {
                let address = request.lock.second_address.clone();
                let answer =
                    nexus.dispatch_metaflow_peer(Query::Deliver(request), self.process.as_ref());
                let accepted = matches!(answer.response, Response::SendAccepted(_));
                let written = self.peer.write_response(&answer.response);
                drop(self);
                if accepted {
                    nexus.wake_due(address);
                }
                return written;
            }
            // A Send: its sender is filled in from the calling process, and
            // a caller in no flow sends as Unbound. A queued request's wake
            // is read after the answer, off the gate.
            Query::Send(send) => {
                let answer = nexus.send_request(send, self.process.as_ref());
                let written = self.peer.write_response(&answer.response);
                drop(self);
                if let Some(pending) = answer.pending {
                    nexus.wake_due(pending.address);
                }
                return written;
            }
            Query::DeliverAll(address) => {
                let response = nexus.deliver_all(address);
                return self.peer.write_response(&response);
            }
            // A reminder is sent from whoever asks for it, as a Send is.
            Query::Remind(remind) => {
                use crate::sending::IdentifiesSender;
                let sent_by = match self.process.as_ref() {
                    Some(process) => nexus.sender_of(process),
                    None => Ok(signal_flow::SentBy::Unbound),
                };
                let response = match sent_by {
                    Ok(sent_by) => nexus.remind(remind, sent_by),
                    Err(refusal) => *refusal,
                };
                return self.peer.write_response(&response);
            }
            // An adopted seat resting at its adoption is handed what is
            // queued for it now, as its turn's end would; Flow sees no Stop
            // from a seat it did not start until its next turn ends.
            Query::BindSeat(request) => {
                let address = request.address.clone();
                let response = nexus.bind_seat(request);
                let written = self.peer.write_response(&response);
                drop(self);
                if matches!(response, Response::Bound(_)) {
                    nexus.drain_awake(&address);
                }
                return written;
            }
            Query::Adopt(request) => {
                let address = request.address.clone();
                let response = nexus.adopt(request);
                let written = self.peer.write_response(&response);
                drop(self);
                if matches!(response, Response::Adopted(_)) {
                    nexus.drain_awake(&address);
                }
                return written;
            }
            // A Launch is the Start its declaration names, from the calling
            // process, or, for a metaflow Awake as a live flow, the Replace
            // of that flow under the metaflow's lock.
            Query::Launch(launch) => {
                let origin = nexus.origin_of_process(self.process.as_ref());
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
            // A launch's origin is the calling process, as Identify reads
            // it: its pane's bound flow and that flow's native session,
            // recorded and changing no answer.
            query @ (Query::Start(_) | Query::Replace(_)) => {
                let origin = nexus.origin_of_process(self.process.as_ref());
                nexus.dispatch_serially(query, origin)
            }
            query => nexus.dispatch_serially(query, Origin::Unbound),
        };
        self.peer.write_response(&response)?;
        drop(self);
        nexus.settle_stops_at_confirmation();
        Ok(())
    }

    fn serve_meta(mut self, nexus: &RunningNexus) -> Result<(), String> {
        match self.greet::<meta_signal_flow::Query>(nexus) {
            Ok(()) => {}
            Err(GreetingEnd::Refused) => return Err("meta greeting refused".into()),
            Err(GreetingEnd::Transport(error)) => return Err(error),
        }
        let query = self.peer.read_meta_query()?;
        // Every peer is answered alike. The peer is read from the kernel,
        // only for the records that name who asked; nothing in the query
        // names it.
        if query == meta_signal_flow::Query::ObserveConfiguration {
            return self.serve_configuration(nexus);
        }
        let resolver = nexus.meta_resolver(self.peer.peer_process());
        let response = nexus.dispatch_meta_serially(query, Some(resolver));
        self.peer.write_meta_response(&response)?;
        drop(self);
        nexus.settle_stops_at_confirmation();
        // An adopted seat resting at its adoption is handed its queue now.
        if let meta_signal_flow::Response::BoundExisting(bound) = &response
            && let meta_signal_flow::FlowBindingResult::Bound(binding) = &bound.flow_binding_result
            && let Ok(Some(metaflow)) = crate::store::metaflow::SleepsMetaflows::metaflow_awake_as(
                &*nexus.store,
                &binding.flow_id.0,
            )
        {
            nexus.drain_awake(&metaflow.address);
        }
        Ok(())
    }
}

impl ServesAttachedRetirement for Connection {
    fn serve_attached_retirement(
        &mut self,
        subscription: &RetirementSubscription,
    ) -> Result<(), String> {
        self.peer
            .write_response(&Response::RetirementObserved(subscription.snapshot.clone()))?;
        let mut reader = self.peer.try_clone().map_err(|error| error.to_string())?;
        reader
            .set_read_timeout(None)
            .map_err(|error| error.to_string())?;
        let watched = subscription.clone();
        let watcher = std::thread::Builder::new()
            .name("flow-handover-retirement-observer-peer".into())
            .spawn(move || {
                let mut bytes = [0_u8; 256];
                loop {
                    match reader.read(&mut bytes) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
                watched.disconnect();
            })
            .map_err(|error| error.to_string())?;
        let result = loop {
            match subscription.next_response() {
                Ok(response @ Response::RetirementObservationFailed(_)) => {
                    break self.peer.write_response(&response);
                }
                Ok(response) => {
                    if let Err(error) = self.peer.write_response(&response) {
                        break Err(error);
                    }
                }
                Err(error) => break Err(error),
            }
        };
        let _ = self.peer.shutdown(Shutdown::Both);
        let watcher_result = watcher
            .join()
            .map_err(|_| "retirement peer-disconnect watcher panicked".to_owned());
        result.and(watcher_result)
    }
}

/// A configuration change, one family whole or one declaration alone,
/// committed and applied.
pub trait ConfiguresFromMeta {
    fn configure_meta(&self, edit: store::ConfigurationEdit) -> meta_signal_flow::Response;
}

impl ConfiguresFromMeta for RunningNexus {
    fn configure_meta(&self, edit: store::ConfigurationEdit) -> meta_signal_flow::Response {
        let before = self.store.configuration_snapshot().ok().flatten();
        match self.store.edit_configuration(edit) {
            Ok(outcome) => {
                if outcome == meta_signal_flow::ConfigureOutcome::Replaced {
                    if let Err(error) = self.apply_configuration(before.as_ref()) {
                        eprintln!("flow-nexus: a committed configuration was not applied: {error}");
                    }
                    self.configuration_changes.announce();
                    self.store.reminder_changes.announce();
                }
                meta_signal_flow::Response::Configured(outcome)
            }
            Err(store::StoreError::DuplicateConfigurationEntry) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::DuplicateEntry,
                )
            }
            Err(store::StoreError::CodexModelsOverlap) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::CodexModelsOverlap,
                )
            }
            Err(store::StoreError::LeaveOutNotForHarness) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::LeaveOutNotForHarness,
                )
            }
            Err(store::StoreError::TopicRefused(topic)) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::TopicRefused(flow_ethos::Topic(topic)),
                )
            }
            Err(store::StoreError::MinutesRefused(refusal)) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::MinutesRefused(refusal),
                )
            }
            Err(store::StoreError::CountersRefused(refusal)) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::CountersRefused(refusal),
                )
            }
            Err(store::StoreError::RequestIdDrawsRefused) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::RequestIdDrawsRefused,
                )
            }
            Err(store::StoreError::AnnouncementTriesRefused) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::AnnouncementTriesRefused,
                )
            }
            Err(store::StoreError::NexusUnconfigured) => meta_signal_flow::Response::Unconfigured,
            Err(store::StoreError::InvalidHeartbeatPeriod) => {
                meta_signal_flow::Response::ConfigureRejected(
                    meta_signal_flow::ConfigureRejection::StoreRefused,
                )
            }
            Err(error) => meta_signal_flow::Response::Refused(
                meta_signal_flow::StoreRefusal::Store(error.to_string()),
            ),
        }
    }
}

/// Binds a socket and serves each connection on its own thread.
pub trait ListensOnSocket {
    fn listen(
        &self,
        socket: &Path,
        surface: &str,
        serve: fn(Connection, &Self) -> Result<(), String>,
    ) -> Result<(), String>
    where
        Self: Sync + Sized,
    {
        use placed_socket::{PlacedSocket, PlacesSocket};
        self.listen_on(PlacedSocket::at(socket)?.claim()?, socket, surface, serve)
    }

    /// Serves `listener`, already bound at `socket`, each connection on its
    /// own thread, until a stop; then removes the socket.
    fn listen_on(
        &self,
        listener: UnixListener,
        socket: &Path,
        surface: &str,
        serve: fn(Connection, &Self) -> Result<(), String>,
    ) -> Result<(), String>
    where
        Self: Sync + Sized,
    {
        std::thread::scope(|scope| {
            // A stop requested before serving began found no accept to wake.
            let incoming = listener
                .incoming()
                .take(if self.stopping() { 0 } else { usize::MAX });
            for peer in incoming {
                // A stop wakes this accept with its own connection; whatever
                // was accepted with it is closed unanswered.
                if self.stopping() {
                    break;
                }
                let connection = match peer
                    .map_err(|error| error.to_string())
                    .and_then(Connection::accepted)
                {
                    Ok(connection) => connection,
                    Err(error) => {
                        eprintln!("flow-nexus: {surface} accept failed: {error}");
                        continue;
                    }
                };
                scope.spawn(move || {
                    if let Err(error) = serve(connection, self) {
                        eprintln!("flow-nexus: {surface} connection dropped: {error}");
                    }
                });
            }
        });
        // Every accepted connection has finished: the scope joined them.
        let _ = fs::remove_file(socket);
        if self.stopping() {
            Ok(())
        } else {
            Err(format!("{surface} listener closed"))
        }
    }

    /// Whether the Nexus has been told to stop.
    fn stopping(&self) -> bool;
}

impl ListensOnSocket for RunningNexus {
    fn stopping(&self) -> bool {
        self.stopping.requested()
    }
}

/// The Nexus's stop request: once set it stays set, and its announcement
/// is a descriptor that becomes readable, so a thread blocked on its own
/// descriptors wakes on the stop with them.
pub struct Stopping {
    requested: AtomicBool,
    announced: event_wait::Announcement,
}

impl Default for Stopping {
    fn default() -> Self {
        use event_wait::AnnouncesOnce;
        Self {
            requested: AtomicBool::new(false),
            announced: event_wait::Announcement::open()
                .expect("the kernel opens an eventfd for the stop request"),
        }
    }
}

/// What a stop request can say: whether it was made, making it, and the
/// descriptor that is readable once it is made.
pub trait HoldsStopRequest {
    fn requested(&self) -> bool;
    fn request(&self);
    fn descriptor(&self) -> std::os::fd::BorrowedFd<'_>;
}

impl HoldsStopRequest for Stopping {
    fn requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    fn request(&self) {
        use event_wait::AnnouncesOnce;
        self.requested.store(true, Ordering::SeqCst);
        self.announced.announce();
    }

    fn descriptor(&self) -> std::os::fd::BorrowedFd<'_> {
        use event_wait::AnnouncesOnce;
        self.announced.descriptor()
    }
}

/// Stops a running Nexus so that every thread borrowing it returns and the
/// store closes cleanly: the accept loops stop taking connections and join
/// the ones in flight, the launch promoter and launch observers return, and
/// agent observers wake on the stop's descriptor and end.
pub trait StopsNexus {
    fn stop(&self, sockets: &[&Path]);
}

impl StopsNexus for RunningNexus {
    fn stop(&self, sockets: &[&Path]) {
        self.stopping.request();
        {
            let _gate = self
                .store
                .handover_write_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.store.handover_observations.fail_all(
                signal_flow::RetirementObservationFailure::MissingEvidence(
                    "the Nexus stopped; the retirement observation is incomplete".into(),
                ),
            );
        }
        self.store.launch_changes.announce();
        self.store.reminder_changes.announce();
        self.store.metaflow_changes.announce();
        self.configuration_changes.announce();
        // Each accept loop is blocked in accept; a connection wakes it. A
        // listener not yet bound sees the request once it binds.
        for socket in sockets {
            let _ = UnixStream::connect(socket);
        }
    }
}

/// SIGTERM and SIGINT, taken by one watching thread instead of killing the
/// process, so a stop closes the store. The signals are blocked before any
/// thread starts, so every thread inherits the block, and are read from a
/// signalfd; a spawned child process starts with an empty mask.
pub struct TerminationSignals {
    signals: std::os::fd::OwnedFd,
}

/// Blocks the termination signals, then watches for them.
pub trait WatchesTerminationSignals: Sized {
    /// Blocks SIGTERM and SIGINT in the calling thread and opens the
    /// signalfd they are read from; call before any other thread starts.
    fn block() -> Self;

    /// Blocks until SIGTERM or SIGINT arrives, true, or until `stopping`
    /// is requested by another path, false. Both are events: the
    /// signalfd and the stop's descriptor are waited on together.
    fn watch(&self, stopping: &Stopping) -> bool;
}

impl WatchesTerminationSignals for TerminationSignals {
    fn block() -> Self {
        use std::os::fd::FromRawFd;
        // SAFETY: the set is initialized by sigemptyset before use;
        // pthread_sigmask and signalfd only read it, and the descriptor
        // signalfd returns is owned by nothing else.
        unsafe {
            let mut set = std::mem::zeroed::<libc::sigset_t>();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGTERM);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
            let descriptor = libc::signalfd(-1, &set, libc::SFD_CLOEXEC | libc::SFD_NONBLOCK);
            assert!(
                descriptor >= 0,
                "the kernel opens a signalfd for the termination signals"
            );
            Self {
                signals: std::os::fd::OwnedFd::from_raw_fd(descriptor),
            }
        }
    }

    fn watch(&self, stopping: &Stopping) -> bool {
        use event_wait::AwaitsDescriptors;
        use std::os::fd::{AsFd, AsRawFd};
        loop {
            let ready = match [self.signals.as_fd(), stopping.descriptor()].ready(None) {
                Ok(ready) => ready,
                Err(error) => {
                    eprintln!("flow-nexus: termination signals unwatchable: {error}");
                    return false;
                }
            };
            if ready[0] {
                // SAFETY: signalfd_siginfo is plain integers, valid zeroed.
                let mut information = unsafe { std::mem::zeroed::<libc::signalfd_siginfo>() };
                // SAFETY: the buffer is one signalfd_siginfo, the unit a
                // signalfd read returns, borrowed for the call.
                let read = unsafe {
                    libc::read(
                        self.signals.as_raw_fd(),
                        (&raw mut information).cast::<libc::c_void>(),
                        std::mem::size_of::<libc::signalfd_siginfo>(),
                    )
                };
                let signal = information.ssi_signo as i32;
                if read > 0 && (signal == libc::SIGTERM || signal == libc::SIGINT) {
                    return true;
                }
            }
            if ready[1] || stopping.requested() {
                return false;
            }
        }
    }
}

/// Reports a listed row as what Herdr shows of it, without writing anything.
///
/// A flow's stored row says what Flow last recorded, which is not the same as
/// what is there now, and List used to report the row alone. Two witnessed
/// ways it went wrong: a seat whose pane was gone stayed Pending on the route
/// it was bound to, and a seat that was plainly live stayed Pending because
/// only a Presented Deliver ever promoted one — so a flow bound through the
/// privileged contract, or one started before Flow Nexus, was Pending forever.
/// A listed flow is therefore reconciled against Herdr before it is answered:
///
/// - its bound pane present: the flow is witnessed live, so its route is
///   refreshed and it is reported Active;
/// - its bound pane gone: the flow is reported `Exited`, with no route and no
///   endpoint. `Exited` is the seat's own going, never `Retired` — an exit
///   retains the flow's record and retirement is an act of authority.
///
/// **This writes nothing.** List is a query, and a query does not change what
/// it is asked about: the row keeps whatever Flow recorded, and only a command
/// that witnesses a pane's fate — `Stop`, a replacement's reap, `Retire`, or a
/// `Deliver` that finds the pane gone — persists an ended lifecycle. Reading the
/// truth and recording it are deliberately separate.
///
/// This helper is for internal native-flow reconciliation. Metaflows on the
/// ordinary wire are read from their own durable records and never projected
/// from these native seat rows.
pub trait ReconcilesListedFlows {
    fn reconciled(&self, nodes: Vec<FlowNode>) -> Vec<FlowNode>;
    fn reconciled_node(&self, node: FlowNode) -> FlowNode;
}

impl ReconcilesListedFlows for RunningNexus {
    fn reconciled(&self, nodes: Vec<FlowNode>) -> Vec<FlowNode> {
        nodes
            .into_iter()
            .map(|node| self.reconciled_node(node))
            .collect()
    }

    fn reconciled_node(&self, node: FlowNode) -> FlowNode {
        if !node.flow_lifecycle.is_live() {
            return node;
        }
        match self.herdr.pane_presence(&node) {
            herdr::PanePresence::Present => {
                let mut node = self.herdr.refresh_route(node);
                node.flow_lifecycle = FlowLifecycle::Active;
                node
            }
            herdr::PanePresence::Unknown => node,
            herdr::PanePresence::Absent | herdr::PanePresence::Bare => FlowNode {
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Unavailable,
                flow_lifecycle: FlowLifecycle::Exited,
                ..node
            },
        }
    }
}

/// An ordinary Restart's or Refresh's answer as the meta surface gives it.
trait AnswersOnMeta {
    fn into_meta(self) -> meta_signal_flow::Response;
}

impl AnswersOnMeta for Response {
    fn into_meta(self) -> meta_signal_flow::Response {
        match self {
            Response::Restarted(flow_id) => meta_signal_flow::Response::Restarted(flow_id),
            Response::RestartQueued(flow_id) => meta_signal_flow::Response::RestartQueued(flow_id),
            Response::RestartRefused(refusal) => {
                meta_signal_flow::Response::RestartRefused(refusal)
            }
            Response::Refreshed(refreshed) => meta_signal_flow::Response::Refreshed(refreshed),
            Response::RefreshQueued(queued) => meta_signal_flow::Response::RefreshQueued(queued),
            Response::RefreshRejected(rejection) => {
                meta_signal_flow::Response::RefreshRejected(rejection)
            }
            Response::Refused(refusal) => meta_signal_flow::Response::OrdinaryRefused(refusal),
            // Neither answers anything else.
            other => meta_signal_flow::Response::Refused(meta_signal_flow::StoreRefusal::Store(
                format!("unexpected answer {other:?}"),
            )),
        }
    }
}

trait DispatchesSerially {
    /// Store transitions are read-modify-write; one dispatch runs at a time
    /// across both sockets while frames are read concurrently. A query
    /// that takes the gate for its own steps (a Refresh, a Replace, a
    /// Restart, a bind, a delivery, a Cancel, a Clear, a Push) runs off it,
    /// as a wake does.
    fn dispatch_serially(&self, query: Query, origin: Origin) -> Response;
    fn dispatch_meta_serially(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response;
}

impl DispatchesSerially for RunningNexus {
    fn dispatch_serially(&self, query: Query, origin: Origin) -> Response {
        if let Query::Refresh(_)
        | Query::Replace(_)
        | Query::Restart(_)
        | Query::DeliverAll(_)
        | Query::BindSeat(_)
        | Query::Adopt(_)
        | Query::Bind(_)
        | Query::Identify(_)
        | Query::IdentifyThread(_)
        | Query::Lock(_)
        | Query::Deliver(_)
        | Query::Release(_)
        | Query::Cancel(_)
        | Query::Remind(_)
        | Query::Clear(_)
        | Query::ClearAll
        | Query::Push(_) = &query
        {
            let response = self.dispatch_from(query, origin);
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Err(error) =
                store::request_id_upgrade::RunsRequestIdAnnouncements::run_request_id_announcements(
                    &*self.store,
                )
            {
                eprintln!("flow-nexus: request-id announcement pass failed: {error}");
            }
            return response;
        }
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let response = self.dispatch_from(query, origin);
            if let Err(error) =
                store::request_id_upgrade::RunsRequestIdAnnouncements::run_request_id_announcements(
                    &*self.store,
                )
            {
                eprintln!("flow-nexus: request-id announcement pass failed: {error}");
            }
            response
        };
        self.wake_deferred();
        response
    }

    fn dispatch_meta_serially(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response {
        if let meta_signal_flow::Query::Restart(_) | meta_signal_flow::Query::Refresh(_) = &query {
            let response = self.dispatch_meta_from(query, resolver);
            {
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Err(error) = store::request_id_upgrade::RunsRequestIdAnnouncements::run_request_id_announcements(&*self.store) {
                    eprintln!("flow-nexus: request-id announcement pass failed: {error}");
                }
            }
            self.wake_deferred();
            return response;
        }
        let withdrawing = match &query {
            meta_signal_flow::Query::Withdraw(withdraw) => Some(withdraw.address.clone()),
            _ => None,
        };
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let response = self.dispatch_meta_from(query, resolver);
            if let Err(error) =
                store::request_id_upgrade::RunsRequestIdAnnouncements::run_request_id_announcements(
                    &*self.store,
                )
            {
                eprintln!("flow-nexus: request-id announcement pass failed: {error}");
            }
            response
        };
        // A resolution that committed leaves the oldest request queued (Absent)
        // or the next one oldest (Delivered), and a withdrawal may leave
        // another oldest: a seat already resting gets no turn end, so it is
        // handed that request now, the gate given back.
        match (&response, withdrawing) {
            (meta_signal_flow::Response::QueueDeliveryResolved(resolved), _) => {
                self.drain_awake(&resolved.address);
            }
            (meta_signal_flow::Response::Withdrawn, Some(address)) => {
                self.drain_awake(&address);
            }
            _ => {}
        }
        response
    }
}

pub trait ServesOrdinary {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String>;
    /// Serves the ordinary socket already bound at `socket`.
    fn serve_ordinary_on(&self, listener: UnixListener, socket: &Path) -> Result<(), String>;
}

impl ServesOrdinary for RunningNexus {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String> {
        self.listen(socket, "ordinary", Connection::serve_ordinary)
    }

    fn serve_ordinary_on(&self, listener: UnixListener, socket: &Path) -> Result<(), String> {
        self.listen_on(listener, socket, "ordinary", Connection::serve_ordinary)
    }
}

pub trait ServesMeta {
    fn serve_meta(&self, socket: &Path) -> Result<(), String>;
    /// Serves the meta socket already bound at `socket`.
    fn serve_meta_on(&self, listener: UnixListener, socket: &Path) -> Result<(), String>;
}

impl ServesMeta for RunningNexus {
    fn serve_meta(&self, socket: &Path) -> Result<(), String> {
        self.listen(socket, "meta", Connection::serve_meta)
    }

    fn serve_meta_on(&self, listener: UnixListener, socket: &Path) -> Result<(), String> {
        self.listen_on(listener, socket, "meta", Connection::serve_meta)
    }
}

/// systemd's readiness protocol: a unit of Type=notify has started once its
/// main process sends `READY=1` to the datagram socket `NOTIFY_SOCKET`
/// names, a path or, after `@`, an abstract name.
pub struct Readiness;

/// Announces that the Nexus serves.
pub trait AnnouncesReadiness {
    /// Sends `READY=1` to `$NOTIFY_SOCKET` when it is set; does nothing
    /// when it is not.
    fn announce_ready() -> Result<(), String>;
}

impl AnnouncesReadiness for Readiness {
    fn announce_ready() -> Result<(), String> {
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::net::{SocketAddr, UnixDatagram};
        let Some(target) = std::env::var_os("NOTIFY_SOCKET") else {
            return Ok(());
        };
        let address = match target.as_encoded_bytes().strip_prefix(b"@") {
            Some(name) => SocketAddr::from_abstract_name(name),
            None => SocketAddr::from_pathname(Path::new(&target)),
        }
        .map_err(|error| format!("NOTIFY_SOCKET {}: {error}", target.display()))?;
        UnixDatagram::unbound()
            .and_then(|socket| socket.send_to_addr(b"READY=1", &address))
            .map(|_| ())
            .map_err(|error| format!("NOTIFY_SOCKET {}: {error}", target.display()))
    }
}

/// How a greeting that did not settle the contract ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GreetingEnd {
    /// A typed `GreetingRefused` was written and the refusal recorded,
    /// metadata alone.
    Refused,
    /// The peer went or the frame could not be read; nothing was written.
    Transport(String),
}

/// One greeting serves both sockets: the peer's first frame is read as a
/// Signal `Handshake` and answered `C`'s receipt, before any Query is
/// decoded. A first frame that is no Handshake is answered the same typed
/// refusal a mismatched digest is, `GreetingRefused(ContractMismatch(own))`.
pub trait GreetsPeers {
    /// Ok only once `Greeted` is written.
    fn greet<C: signal::Contracted>(&mut self, nexus: &RunningNexus) -> Result<(), GreetingEnd>;
}

impl GreetsPeers for Connection {
    fn greet<C: signal::Contracted>(&mut self, nexus: &RunningNexus) -> Result<(), GreetingEnd> {
        use crate::undecodable::ReadsOrdinaryFrames;
        let frame = self.peer.read_frame().map_err(GreetingEnd::Transport)?;
        let receipt = match rkyv::from_bytes::<signal::Handshake, rkyv::rancor::Error>(&frame) {
            Ok(greeting) => C::receipt(&greeting),
            Err(_) => signal::HandshakeReceipt::GreetingRefused(
                signal::HandshakeRejection::ContractMismatch(C::contract_digest()),
            ),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&receipt)
            .map_err(|error| GreetingEnd::Transport(error.to_string()))?;
        self.peer
            .write_frame(&bytes)
            .map_err(GreetingEnd::Transport)?;
        match receipt {
            signal::HandshakeReceipt::Greeted(_) => Ok(()),
            signal::HandshakeReceipt::GreetingRefused(_) => {
                nexus.refused_greeting(self.process.as_ref(), self.peer.peer_process());
                let _ = self.peer.shutdown(Shutdown::Both);
                Err(GreetingEnd::Refused)
            }
        }
    }
}

/// A Unix stream carries Signal as length-prefixed rkyv frames of at most
/// 1 MiB.
pub trait CarriesSignalFrames {
    const FRAME_LIMIT: usize = 1024 * 1024;

    fn write_frame(&mut self, bytes: &[u8]) -> Result<(), String>;
    fn read_frame(&mut self) -> Result<Vec<u8>, String>;
    fn write_query(&mut self, value: &Query) -> Result<(), String>;
    fn read_query(&mut self) -> Result<Query, String>;
    fn write_response(&mut self, value: &Response) -> Result<(), String>;
    fn read_response(&mut self) -> Result<Response, String>;
    fn read_meta_query(&mut self) -> Result<meta_signal_flow::Query, String>;
    fn write_meta_response(&mut self, value: &meta_signal_flow::Response) -> Result<(), String>;
}

impl CarriesSignalFrames for UnixStream {
    fn write_frame(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        self.write_all(bytes).map_err(|e| e.to_string())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, String> {
        let mut length = [0; 4];
        self.read_exact(&mut length).map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > Self::FRAME_LIMIT {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(bytes)
    }

    fn write_query(&mut self, value: &Query) -> Result<(), String> {
        self.write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?)
    }

    fn read_query(&mut self) -> Result<Query, String> {
        rkyv::from_bytes::<Query, rkyv::rancor::Error>(&self.read_frame()?)
            .map_err(|e| e.to_string())
    }

    fn write_response(&mut self, value: &Response) -> Result<(), String> {
        self.write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?)
    }

    fn read_response(&mut self) -> Result<Response, String> {
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&self.read_frame()?)
            .map_err(|e| e.to_string())
    }

    fn read_meta_query(&mut self) -> Result<meta_signal_flow::Query, String> {
        rkyv::from_bytes::<meta_signal_flow::Query, rkyv::rancor::Error>(&self.read_frame()?)
            .map_err(|e| e.to_string())
    }

    fn write_meta_response(&mut self, value: &meta_signal_flow::Response) -> Result<(), String> {
        self.write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?)
    }
}

#[cfg(test)]
mod tests {
    use crate::herdr::ConfiguresHerdrCli;
    use crate::herdr::ReadsHerdrPanes;
    use crate::live::{HoldsLiveValue, Live};
    mod admission;
    mod configuring;
    mod counters;
    mod curriculum;
    mod delivery;
    mod ending;
    mod handover;
    mod handover_archive;
    mod handover_configuration;
    mod handover_memory;
    mod handover_reminding;
    mod heartbeat_production;
    mod launching_nexus;
    mod model;
    mod observe_metaflows;
    mod open_requests;
    mod queue_drain;
    mod queue_operations;
    mod reap_takeover;
    pub(crate) mod reaping;
    mod refresh_and_compaction;
    mod refreshing;
    mod rehearsing;
    mod reminding;
    mod repairing;
    mod reservation;
    mod restarting;
    mod seat_binding;
    mod send_admission;
    mod send_tool;
    mod start_admission;
    mod submission;
    mod turn_ends;
    mod waking;

    pub(crate) fn sendable_message(message: signal_flow::Message) -> signal_flow::SendableMessage {
        let request = |request| match request {
            signal_flow::Request::Order(value) => signal_flow::SendableRequest::Order(value),
            signal_flow::Request::Question(value) => signal_flow::SendableRequest::Question(value),
            signal_flow::Request::Living(value) => {
                signal_flow::SendableRequest::Living(signal_flow::SendableRequest_Living_Data {
                    context: value.context,
                    verbatim: value.verbatim,
                })
            }
            signal_flow::Request::Notice(value) => signal_flow::SendableRequest::Notice(value),
            signal_flow::Request::Task(value) => signal_flow::SendableRequest::Task(value),
            signal_flow::Request::Result(flow_ethos::ResultPayload::Answered(value)) => {
                signal_flow::SendableRequest::Result(signal_flow::SendableRequest_Result_Data {
                    request_id: value.request_id,
                    string: value.string,
                })
            }
            signal_flow::Request::Done(flow_ethos::DonePayload::Answered(value)) => {
                signal_flow::SendableRequest::Done(signal_flow::SendableRequest_Done_Data {
                    request_id: value.request_id,
                    string: value.string,
                })
            }
            _ => panic!("the test request is not externally sendable"),
        };
        match message {
            signal_flow::Message::HardAbrupt(value) => {
                signal_flow::SendableMessage::HardAbrupt(request(value))
            }
            signal_flow::Message::MiddleAbrupt(value) => {
                signal_flow::SendableMessage::MiddleAbrupt(request(value))
            }
            signal_flow::Message::Soft(value) => signal_flow::SendableMessage::Soft(request(value)),
        }
    }

    pub(crate) fn deliver_key(counter: i64) -> signal_flow::DeliverKey {
        signal_flow::DeliverKey {
            namespace: signal_flow::Namespace("testMessageMemory".into()),
            counter: signal_flow::Counter(counter),
        }
    }

    /// The answer is `Unidentified` for exactly this process: the refusal
    /// carries the process beside the evidence read, never an identity.
    pub(crate) fn assert_unidentified(
        response: signal_flow::Response,
        process: &signal_flow::Process,
    ) {
        let signal_flow::Response::Refused(signal_flow::Refusal::Unidentified(evidence)) = response
        else {
            panic!("expected Refused.Unidentified, found {response:?}");
        };
        assert_eq!(&evidence.process, process);
    }

    use super::{
        Arc, CarriesSignalFrames, Connection, Dispatches, ReconcilesListedFlows, RunningNexus,
        ServesConnection, UnixStream,
    };
    use crate::caller::{
        CallerPane, CallerProcess, IdentifiesPeer, LocatesCallerPane, ReadsProcess, ResolvesCaller,
    };
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::{
        codex::{CodexEndpoint, CodexEndpoints},
        composition::{ComposesLaunch, LaunchComposer, OpensLaunchComposer},
        context::FixtureComposer,
        herdr::HerdrCli,
        store::{
            ConfiguresFlowStore, FlowStore, LaunchOutcome, OpensFlowStore, RecordsFlowLifecycle,
            RecordsLaunchOutcome, RecordsNativeLaunchBinding, RecordsNativeLaunchIntent,
            RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
            RecordsRegistrationAcknowledgement, RegistersFlowIdentity, ReservesLaunchAttempt,
        },
    };
    use signal_flow::{
        Aspect, CallerIdentity, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin,
        HarnessKind, HerdrPaneBinding, HerdrRoute, HerdrRouteSelection, LaunchAttemptPhase,
        LaunchProfile, LaunchStatusOutcome, Model, NativeLaunchBinding, NativeLaunchIntent,
        NativeTranscriptAbsence, NativeTranscriptBoundary, Origin, PromptDeliveryIntent,
        PromptDeliveryResult, Query, RegistrationAcknowledgement, Response, StartRejection,
    };
    use std::{
        collections::BTreeSet,
        fs,
        os::unix::fs::MetadataExt,
        os::unix::net::UnixListener,
        path::{Path, PathBuf},
        time::Duration,
    };

    struct NexusFixture {
        directory: tempfile::TempDir,
        nexus: RunningNexus,
        snapshot_program: PathBuf,
        /// The fixture Herdr server's socket for the session
        /// `messaging-build`: the test process listens on it, standing as
        /// that server, and every marked fixture process descends from it.
        _herdr_server: UnixListener,
    }

    #[test]
    fn metaflows_do_not_advertise_undeclared_sleeping_history() {
        use crate::store::ManagesMetaflowState;
        use crate::store::metaflow::SeedsMetaflow;
        let fixture = NexusFixture::new();
        let address = signal_flow::Address {
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("retiredTest".into()),
            model: signal_flow::Model::Haiku,
        };
        let history = signal_flow::Permaflow {
            address: address.clone(),
            permaflow_state: flow_ethos::PermaflowState::Asleep,
            past: flow_ethos::Past(vec![flow_ethos::FlowId("abcdef".into())]),
            queue: flow_ethos::Queue(vec![]),
        };
        fixture.nexus.store.seed_metaflow(history.clone());
        let Response::Listed(listed) = fixture.nexus.dispatch(Query::Metaflows) else {
            panic!("metaflows must be listed");
        };
        assert!(!listed.iter().any(|row| row.address == address));
        assert_eq!(
            fixture.nexus.store.list_metaflows().unwrap(),
            vec![history.clone()]
        );
        fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
            permaflow_state: flow_ethos::PermaflowState::Awake(flow_ethos::FlowId("123abc".into())),
            ..history
        });
        let Response::Listed(listed) = fixture.nexus.dispatch(Query::Metaflows) else {
            panic!("metaflows must be listed");
        };
        assert!(listed.iter().any(|row| row.address == address));
    }

    #[test]
    fn every_ordinary_send_answer_is_observable_as_one_durable_outcome() {
        use crate::store::send_outcome::RecordsSendOutcomes;
        use meta_signal_flow::OrdinarySendOutcome;
        let fixture = NexusFixture::new();
        let send = signal_flow::SendRequest {
            address: signal_flow::Address {
                aspect: Aspect::Psyche,
                topic: flow_ethos::Topic("core".into()),
                model: signal_flow::Model::Opus,
            },
            sendable_message: signal_flow::SendableMessage::Soft(
                signal_flow::SendableRequest::Notice("ledger witness".into()),
            ),
        };
        // A Send no process stands behind sends as Unbound and is served.
        {
            use crate::store::metaflow::SeedsMetaflow;
            fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
                address: send.address.clone(),
                permaflow_state: flow_ethos::PermaflowState::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
        }
        let response = fixture.nexus.dispatch(Query::Send(send));
        assert!(
            matches!(response, Response::SendAccepted(_)),
            "{response:?}"
        );
        for response in [
            signal_flow::PrivateSendResponse::SendAccepted(signal_flow::SendAcceptance {
                logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-delivery-1".into()),
                request_id: flow_ethos::RequestId("req1".into()),
                delivery_cursor: signal_flow::DeliveryCursor(1),
                delivery_state: signal_flow::DeliveryState::Queued,
            }),
            signal_flow::PrivateSendResponse::SendAccepted(signal_flow::SendAcceptance {
                logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-delivery-2".into()),
                request_id: flow_ethos::RequestId("req2".into()),
                delivery_cursor: signal_flow::DeliveryCursor(2),
                delivery_state: signal_flow::DeliveryState::Attempting,
            }),
        ] {
            fixture
                .nexus
                .store
                .record_send_outcome(
                    None,
                    signal_flow::SentBy::Unknown,
                    None,
                    "digest".into(),
                    response,
                )
                .unwrap();
        }
        let records = fixture.nexus.store.read_send_outcomes().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.ordinary_send_outcome,
                    OrdinarySendOutcome::Refused(_)
                ))
                .count(),
            0
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    record.ordinary_send_outcome,
                    OrdinarySendOutcome::Accepted(_)
                ))
                .count(),
            3
        );
        assert_eq!(
            records
                .iter()
                .filter(|record| matches!(
                    &record.ordinary_send_outcome,
                    OrdinarySendOutcome::Accepted(acceptance)
                        if matches!(acceptance.delivery_state, signal_flow::DeliveryState::Queued)
                ))
                .count(),
            2
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::ReadSendOutcomes),
            meta_signal_flow::Response::SendOutcomesRead(
                records
                    .iter()
                    .map(crate::message_projection::ProjectsSendOutcomeRecord::visible_send_outcome_record)
                    .collect()
            )
        );
    }

    fn inspected(response: Response, launch_request_id: &str) -> signal_flow::LaunchInspection {
        let Response::LaunchInspected(inspection) = response else {
            panic!("expected a launch inspection, got {response:?}")
        };
        assert_eq!(
            inspection.launch_attempt.launch_profile.launch_request_id.0,
            launch_request_id
        );
        inspection
    }

    fn assert_inspection_outcome(
        response: Response,
        launch_request_id: &str,
        expected_outcome: LaunchStatusOutcome,
        expected_flow_id: Option<&str>,
    ) {
        let inspection = inspected(response, launch_request_id);
        assert_eq!(
            inspection.launch_status_outcome_option,
            Some(expected_outcome)
        );
        assert_eq!(
            inspection
                .flow_node_option
                .as_ref()
                .map(|node| node.flow_id.0.as_str()),
            expected_flow_id
        );
    }

    /// How the fixture Herdr answers `agent prompt`, shaped as Herdr 0.8.2
    /// answers: a success reply on stdout, an error reply on stderr.
    #[derive(Clone, Copy)]
    enum PromptFixture<'a> {
        /// The text is typed and the reply is `agent_prompted` for this pane.
        Prompted(&'a str),
        /// As Prompted, but a Claude seat's transcript records nothing yet:
        /// its receipt comes later, from its hook or its transcript.
        PromptedUnrecorded(&'a str),
        /// This error code is answered before any text is typed.
        RefusedBeforeInput(&'a str),
        /// The text is typed, then this error code is answered.
        FailedAfterInput(&'a str),
    }

    impl PromptFixture<'_> {
        fn shell(
            &self,
            typed: &std::path::Path,
            agents: &[serde_json::Value],
            claude_receipt: Option<(&std::path::Path, &str)>,
        ) -> String {
            let mut typing = format!("printf '%s' \"$6\" >> '{}'", typed.display());
            if let Some((transcript, session)) = claude_receipt
                && !matches!(self, Self::PromptedUnrecorded(_))
            {
                typing.push_str(&format!(
                    "; escaped=$(printf '%s' \"$6\" | sed ':a;N;$!ba;s/\\\\/\\\\\\\\/g;s/\"/\\\\\"/g;s/\\n/\\\\n/g'); printf '{{\"type\":\"user\",\"sessionId\":\"{session}\",\"uuid\":\"fixture-receipt\",\"message\":{{\"content\":\"%s\"}}}}\\n' \"$escaped\" >> '{}'",
                    transcript.display()
                ));
            }
            let error = |code: &str| {
                format!(
                    "printf '%s\\n' '{{\"id\":\"cli:agent:prompt\",\"error\":{{\"code\":\"{code}\",\"message\":\"fixture\"}}}}' >&2; exit 1"
                )
            };
            match self {
                Self::Prompted(pane) | Self::PromptedUnrecorded(pane) => {
                    // Herdr answers with the prompted pane's own AgentInfo,
                    // which the roster already holds; a pane the roster does
                    // not know is answered with its id alone, as a reply
                    // naming another pane is.
                    let agent = agents
                        .iter()
                        .find(|agent| {
                            agent.get("pane_id").and_then(serde_json::Value::as_str) == Some(pane)
                        })
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({ "pane_id": pane }));
                    let reply = serde_json::json!({
                        "id":"cli:agent:prompt",
                        "result":{ "type":"agent_prompted", "agent":agent }
                    });
                    format!("{typing}; submitted \"$5\" \"$6\"; printf '%s\\n' '{reply}'")
                }
                Self::RefusedBeforeInput(code) => error(code),
                // The text was typed and the fixture's composer shows it
                // gone: the harness took it, whatever Herdr answered.
                Self::FailedAfterInput(code) => {
                    format!("{typing}; submitted \"$5\" \"$6\"; {}", error(code))
                }
            }
        }
    }

    trait ControlsHerdrSnapshot {
        fn set_agents(&self, agents: Vec<serde_json::Value>);
        fn accept_pane_operations(
            &self,
            agents: Vec<serde_json::Value>,
            prompt: PromptFixture,
        ) -> PathBuf;
        /// The bytes the fixture Herdr typed into the pane, in order.
        fn typed(&self) -> Option<String>;
    }

    /// The answer to a launch whose pane Herdr did not open.
    fn pane_unopened() -> StartRejection {
        StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Open,
            native_launch_failure: signal_flow::NativeLaunchFailure::AdapterRefused,
        })
    }

    /// The answer to a launch whose claimed flow was not ready for its title.
    fn title_not_ready() -> StartRejection {
        StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Naming,
            native_launch_failure: signal_flow::NativeLaunchFailure::NotReady,
        })
    }

    fn current_process_identity() -> meta_signal_flow::ProcessIdentity {
        let process_id = i64::from(std::process::id());
        let metadata = fs::metadata(format!("/proc/{process_id}")).expect("current process");
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).expect("process stat");
        let (_, fields) = stat.rsplit_once(") ").expect("process comm boundary");
        meta_signal_flow::ProcessIdentity {
            process_id: meta_signal_flow::ProcessId(process_id),
            process_user_id: meta_signal_flow::ProcessUserId(i64::from(metadata.uid())),
            process_start_token: meta_signal_flow::ProcessStartToken(
                fields
                    .split_whitespace()
                    .nth(19)
                    .expect("process start token")
                    .into(),
            ),
        }
    }

    fn existing_binding(flow_id: &str, pane: &str) -> meta_signal_flow::FlowBinding {
        meta_signal_flow::FlowBinding {
            flow_id: flow_ethos::FlowId(flow_id.into()),
            aspect: Aspect::Mind,
            model_name: flow_ethos::ModelName("gpt-sol".into()),
            harness_kind: HarnessKind::Codex,
            native_session_id: signal_flow::NativeSessionId(
                "01a0b22ce24f7452994064490878680f".into(),
            ),
            herdr_workspace_id: signal_flow::HerdrWorkspaceId("workspace".into()),
            herdr_pane_id: signal_flow::HerdrPaneId(pane.into()),
            herdr_tab_id: meta_signal_flow::HerdrTabId("tab".into()),
            herdr_terminal_id: signal_flow::HerdrTerminalId(format!("terminal-{pane}")),
            herdr_agent_name: signal_flow::HerdrAgentName(format!("agent-{pane}")),
            process_identity: current_process_identity(),
            working_directory: meta_signal_flow::WorkingDirectory(
                fs::read_link("/proc/self/cwd")
                    .expect("current cwd")
                    .to_string_lossy()
                    .into_owned(),
            ),
        }
    }

    fn configure_existing_binding_model(fixture: &NexusFixture) {
        use crate::store::ManagesMetaflowState;
        fixture
            .nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    model: Model::Sol,
                    native: meta_signal_flow::Native("gpt-sol".into()),
                },
            ]))
            .expect("existing binding model declared");
    }

    fn write_native_identity_claim(
        fixture: &NexusFixture,
        binding: &meta_signal_flow::FlowBinding,
    ) {
        use crate::herdr::launch::{ClaimsNativeIdentity, NamesHerdrHarness};
        let harness = binding.harness_kind.expected_harness();
        let identity = <crate::herdr::HerdrCli as ClaimsNativeIdentity>::canonical_native_identity(
            &binding.native_session_id.0,
        );
        let version = if binding.harness_kind == HarnessKind::Claude {
            "uuid-version=uuid-v4\n"
        } else {
            ""
        };
        fs::write(
            fixture
                .directory
                .path()
                .join("flows")
                .join(format!(".{}.flow-id", binding.flow_id.0)),
            format!(
                "version=1\nharness={}\nidentity={}\nalias={}\n{}",
                harness, identity, binding.flow_id.0, version
            ),
        )
        .expect("native identity claim written");
    }

    /// Herdr and the Codex rollouts as they show `bindings` live, for the
    /// one MetaBindExisting `run` performs: the pane in the snapshot with
    /// its harness, this test process in its foreground, and a Codex session
    /// named by a rollout's session_meta. The fixture's own Herdr returns
    /// after it.
    fn with_bound_world<T>(
        fixture: &NexusFixture,
        bindings: &[&meta_signal_flow::FlowBinding],
        run: impl FnOnce() -> T,
    ) -> T {
        use crate::herdr::launch::NamesHerdrHarness;
        configure_existing_binding_model(fixture);
        let agents = bindings
            .iter()
            .map(|binding| {
                let harness = binding.harness_kind.expected_harness();
                write_native_identity_claim(fixture, binding);
                let mut agent = serde_json::json!({
                    "agent": harness,
                    "agent_status": "idle",
                    "interactive_ready": true,
                    "name": binding.herdr_agent_name.0,
                    "pane_id": binding.herdr_pane_id.0,
                    "terminal_id": binding.herdr_terminal_id.0,
                    "workspace_id": binding.herdr_workspace_id.0,
                });
                // A Claude pane reports its session; a Codex one reports
                // none, and its rollout below proves it.
                if binding.harness_kind == HarnessKind::Claude {
                    agent["agent_session"] = serde_json::json!({
                        "source": format!("herdr:{harness}"),
                        "agent": harness,
                        "kind": "id",
                        "value": binding.native_session_id.0,
                    });
                }
                agent
            })
            .collect::<Vec<_>>();
        let rollouts = fixture.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&rollouts).expect("rollout root");
        for binding in bindings
            .iter()
            .filter(|binding| binding.harness_kind == HarnessKind::Codex)
        {
            fs::write(
                rollouts.join(format!("rollout-{}.jsonl", binding.native_session_id.0)),
                format!(
                    "{}\n",
                    serde_json::json!({"type":"session_meta","payload":{"id":binding.native_session_id.0}})
                ),
            )
            .expect("rollout written");
        }
        let previous = fs::read_to_string(&fixture.snapshot_program).ok();
        herdr_world(fixture, agents, std::process::id());
        let result = run();
        // Restored as every fixture script is installed, never written from
        // this process (fixture_executable).
        match previous {
            Some(body) => FixtureExecutable {
                path: fixture.snapshot_program.clone(),
            }
            .install(&body),
            None => {
                let _ = fs::remove_file(&fixture.snapshot_program);
            }
        }
        result
    }

    /// A Herdr whose session snapshot shows `agents` and whose every pane
    /// runs `pid` in its foreground.
    fn herdr_world(fixture: &NexusFixture, agents: Vec<serde_json::Value>, pid: u32) {
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":agents,"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"pane process-info\") printf '{{\"result\":{{\"process_info\":{{\"pane_id\":\"%s\",\"foreground_processes\":[{{\"pid\":{}}}]}}}}}}\\n' \"$6\" ;;\n  *) exit 64 ;;\nesac\n",
            fixture.session_listing(),
            snapshot,
            pid,
        ));
    }

    /// One binding's MetaBindExisting result against the fixture's Herdr.
    fn bound_once(
        fixture: &NexusFixture,
        binding: meta_signal_flow::FlowBinding,
    ) -> meta_signal_flow::FlowBindingResult {
        configure_existing_binding_model(fixture);
        write_native_identity_claim(fixture, &binding);
        let socket_path = fixture.directory.path().join("herdr-verify.sock");
        let _ = fs::remove_file(&socket_path);
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let address = signal_flow::Address {
            aspect: binding.aspect.clone(),
            topic: flow_ethos::Topic("core".into()),
            model: signal_flow::Model::Sol,
        };
        let meta_signal_flow::Response::BoundExisting(bound) = fixture.nexus.dispatch_meta_from(
            meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                flow_container: flow_container(&socket_path),
                address,
                flow_binding: binding,
            }),
            Some(adoption_resolver()),
        ) else {
            panic!("a live container answers per binding")
        };
        bound.flow_binding_result
    }

    fn refused_for(
        result: &meta_signal_flow::FlowBindingResult,
    ) -> Option<meta_signal_flow::FlowBindingRefusalReason> {
        match result {
            meta_signal_flow::FlowBindingResult::Refused(refused) => {
                Some(refused.flow_binding_refusal_reason.clone())
            }
            meta_signal_flow::FlowBindingResult::Bound(_) => None,
        }
    }

    /// MetaBindExisting takes no pane or session as given: the pane must be
    /// in the container's Herdr session with the binding's harness, run the
    /// binding's process, and run its session, as Herdr reports it for
    /// Claude and as a rollout's session_meta proves it for a Codex pane
    /// that reports none. Each failure is refused with its reason and
    /// nothing is stored.
    #[test]
    fn meta_bind_existing_verifies_the_pane_process_and_session_with_herdr() {
        use meta_signal_flow::FlowBindingRefusalReason as Reason;
        let fixture = NexusFixture::new();
        let shown = |binding: &meta_signal_flow::FlowBinding| {
            serde_json::json!({
                "agent": if binding.harness_kind == HarnessKind::Claude { "claude" } else { "codex" },
                "agent_status": "idle",
                "pane_id": binding.herdr_pane_id.0,
                "terminal_id": binding.herdr_terminal_id.0,
                "workspace_id": binding.herdr_workspace_id.0,
            })
        };
        let me = std::process::id();

        // Herdr unreadable.
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install("#!/bin/sh\nexit 64\n");
        let codex = existing_binding("b001", "pane-1");
        assert_eq!(
            refused_for(&bound_once(&fixture, codex)),
            Some(Reason::HerdrUnreadable)
        );

        // The pane is not in the session, or another harness runs in it.
        let absent = existing_binding("b002", "pane-2");
        let mut elsewhere = shown(&absent);
        elsewhere["pane_id"] = serde_json::Value::String("pane-9".into());
        herdr_world(&fixture, vec![elsewhere], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, absent.clone())),
            Some(Reason::PaneAbsent)
        );
        let mut claude_there = shown(&absent);
        claude_there["agent"] = serde_json::Value::String("claude".into());
        herdr_world(&fixture, vec![claude_there], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, absent)),
            Some(Reason::PaneAbsent)
        );

        // The pane runs another process.
        let other_process = existing_binding("b003", "pane-3");
        herdr_world(&fixture, vec![shown(&other_process)], 1);
        assert_eq!(
            refused_for(&bound_once(&fixture, other_process)),
            Some(Reason::ProcessMismatch)
        );

        // Herdr reports another session.
        let mismatched = existing_binding("b004", "pane-4");
        let mut reporting = shown(&mismatched);
        reporting["agent_session"] = serde_json::json!({
            "source":"herdr:codex","agent":"codex","kind":"id","value":"native-someone-else"
        });
        herdr_world(&fixture, vec![reporting], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, mismatched)),
            Some(Reason::SessionMismatch)
        );

        // A Codex pane reporting no session, and no rollout names it.
        let unproven = existing_binding("b005", "pane-5");
        herdr_world(&fixture, vec![shown(&unproven)], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, unproven.clone())),
            Some(Reason::SessionUnproven)
        );
        // A rollout under that name whose session_meta names another thread
        // proves nothing either.
        let rollouts = fixture.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&rollouts).unwrap();
        fs::write(
            rollouts.join(format!("rollout-{}.jsonl", unproven.native_session_id.0)),
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"another-thread\"}}\n",
        )
        .unwrap();
        assert_eq!(
            refused_for(&bound_once(&fixture, unproven.clone())),
            Some(Reason::SessionUnproven)
        );
        // The rollout naming it proves it.
        fs::write(
            rollouts.join(format!("rollout-{}.jsonl", unproven.native_session_id.0)),
            format!(
                "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{}\"}}}}\n",
                unproven.native_session_id.0
            ),
        )
        .unwrap();
        assert_eq!(refused_for(&bound_once(&fixture, unproven)), None);

        // A Claude pane with no session file for the process must report
        // its session, and that session.
        // The successful b005 adoption owns its cell; use a separate fixture
        // to verify Claude session evidence without colliding with that owner.
        let claude_fixture = NexusFixture::new();
        let mut claude = existing_binding("b006", "pane-6");
        claude.harness_kind = HarnessKind::Claude;
        claude.native_session_id =
            signal_flow::NativeSessionId("01a0b22ce24f4452994064490878680f".into());
        herdr_world(&claude_fixture, vec![shown(&claude)], me);
        assert_eq!(
            refused_for(&bound_once(&claude_fixture, claude.clone())),
            Some(Reason::SessionUnreported)
        );
        let mut reporting = shown(&claude);
        reporting["agent_session"] = serde_json::json!({
            "source":"herdr:claude","agent":"claude","kind":"id","value":claude.native_session_id.0
        });
        herdr_world(&claude_fixture, vec![reporting], me);
        assert_eq!(refused_for(&bound_once(&claude_fixture, claude)), None);

        // Nothing refused was stored.
        for refused in ["b001", "b002", "b003", "b004"] {
            assert!(matches!(
                fixture.nexus.dispatch(Query::ResolveRecipient(
                    signal_flow::RecipientResolutionRequest(flow_ethos::FlowId(refused.into()))
                )),
                Response::RecipientResolutionRejected(
                    signal_flow::RecipientResolutionRejection::UnknownFlow
                )
            ));
        }
    }

    fn flow_container(socket: &Path) -> meta_signal_flow::FlowContainer {
        meta_signal_flow::FlowContainer {
            herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
            herdr_server_socket_path: meta_signal_flow::HerdrServerSocketPath(
                socket.to_string_lossy().into_owned(),
            ),
            process_identity: current_process_identity(),
            meta_flow_owner_id: meta_signal_flow::MetaFlowOwnerId(flow_ethos::FlowId(
                "field-owner".into(),
            )),
        }
    }

    fn adoption_resolver() -> meta_signal_flow::ResolverIdentity {
        meta_signal_flow::ResolverIdentity {
            process_identity: current_process_identity(),
            flow_id_option: Some(flow_ethos::FlowId("field-owner".into())),
        }
    }

    /// Stands for each seat's own word of a submission: every file the
    /// fixture Herdr's `submitted` renames into `directory` names a pane
    /// and the text its harness took, and is reported as that pane's live
    /// flow's harness would report it (Claude Code's UserPromptSubmit
    /// digest, a Codex turn started), until the directory is gone. The
    /// store is held only while a file is read, so the fixture's store is
    /// never held open past the fixture.
    fn report_fixture_submissions(
        directory: PathBuf,
        store: std::sync::Weak<FlowStore>,
        submissions: std::sync::Arc<crate::submission::SeatSubmissions>,
    ) {
        std::thread::spawn(move || {
            use crate::event_wait::AwaitsDirectory;
            use crate::store::{NamesLiveFlow, ReadsFlowRows};
            use crate::submission::{SeatSubmission, WitnessesSubmissions};
            use flow_native::DigestsPrompt;
            let watched = directory.clone();
            watched.holds_by(None, &mut || {
                let Ok(entries) = fs::read_dir(&directory) else {
                    return true;
                };
                for path in entries.flatten().map(|entry| entry.path()) {
                    let named = path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("sub-"));
                    let Some(body) = named.then(|| fs::read_to_string(&path).ok()).flatten() else {
                        continue;
                    };
                    let _ = fs::remove_file(&path);
                    let (pane, text) = body.split_once('\n').unwrap_or((body.as_str(), ""));
                    let Some(nodes) = store.upgrade().and_then(|store| store.flow_nodes().ok())
                    else {
                        continue;
                    };
                    for node in nodes {
                        let in_pane = matches!(
                            &node.herdr_route_selection,
                            HerdrRouteSelection::Available(route) if route.herdr_pane_id.0 == pane
                        );
                        if !in_pane || !node.flow_lifecycle.is_live() {
                            continue;
                        }
                        let submission = match node.harness_kind {
                            HarnessKind::Claude => SeatSubmission::Prompt(text.prompt_sha256()),
                            HarnessKind::Codex => SeatSubmission::TurnStarted,
                        };
                        submissions.witness(&node.session_id.0, submission);
                    }
                }
                false
            });
        });
    }

    impl NexusFixture {
        fn new() -> Self {
            Self::with_store(|_| {})
        }

        /// A fixture whose store at `flow.sema` is first prepared by `seed`.
        fn with_store(seed: impl FnOnce(&Path)) -> Self {
            use crate::store::SetsNativeOwnerEnvironmentForTests;
            let directory = tempfile::tempdir().expect("temporary nexus fixture");
            seed(&directory.path().join("flow.sema"));
            let flows_root = directory.path().join("flows");
            fs::create_dir(&flows_root).expect("fixture flows root");
            fs::write(
                flows_root.join(".908786.flow-id"),
                "version=1\nharness=codex\nidentity=01a0b22ce24f7452994064490878680f\nalias=908786\n",
            )
            .expect("fixture flow claim");
            let snapshot_program = directory.path().join("herdr-fixture");
            let codex_endpoints = CodexEndpoints {
                stable: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex"),
                    home: directory.path().join("codex-home"),
                    socket: "unused".into(),
                    transcript_root: directory.path().join("native-transcripts/codex"),
                    model_names: BTreeSet::from([
                        "unused".into(),
                        "fixture-model".into(),
                        "gpt-sol".into(),
                    ]),
                },
                next: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex-next"),
                    home: directory.path().join("codex-next-home"),
                    socket: "unused-next".into(),
                    transcript_root: directory.path().join("native-transcripts/codex-next"),
                    model_names: BTreeSet::from(["gpt-6-sol".into(), "gpt-6-luna".into()]),
                },
                timeout: Duration::from_secs(1),
                workspace_root: directory.path().to_path_buf(),
                nonces: std::sync::Arc::new(crate::codex::SampleNonce),
            };
            let mut store =
                FlowStore::open(&directory.path().join("flow.sema")).expect("fixture store");
            let mut runtime = store
                .runtime_configuration()
                .expect("fixture runtime configuration");
            runtime.source_root = directory.path().to_string_lossy().into_owned();
            store
                .configure_runtime(runtime)
                .expect("fixture source root configuration");
            store.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
                host_id: "isolated-test-host".into(),
                boot_id: "isolated-test-boot".into(),
            });
            let nexus = RunningNexus {
                store: store.into(),
                codex_endpoints: Live::holding(codex_endpoints.clone()),
                herdr: HerdrCli::at(snapshot_program.clone(), flows_root)
                    .with_codex_endpoints(codex_endpoints),
                composer: Live::holding(LaunchComposer::with_context(
                    crate::context::ContextComposer(std::sync::Arc::new(FixtureComposer::from(
                        &[
                            ("spirit-core", "Spirit body."),
                            ("intent-flow", "Intent body."),
                            ("operation-main-flow", "Main flow body."),
                            ("knowledge-flow", "Knowledge body."),
                        ][..],
                    ))),
                )),
                dispatch_gate: std::sync::Mutex::new(()),
                pane_leases: crate::delivery::lease::PaneLeases::default(),
                waking: crate::waking::WakingMetaflows::default(),
                stopped_at_confirmation: crate::waking::StoppedAtConfirmation::default(),
                refresh_refusals: crate::waking::RefreshRefusals::default(),
                pane_watch_rearms: Default::default(),
                pane_watch_hooks: Default::default(),
                stopping: crate::Stopping::default(),
                launch_calls: crate::send_tool::LaunchCalls::default(),
                codex_turns: crate::codex_turns::CodexTurns::default(),
                turn_ends: crate::turn_ends::TurnEndSubscribers::default(),
                compactions: crate::compacting::CompactionWatch::default(),
                seat_submissions: std::sync::Arc::default(),
                configuration_changes: Default::default(),
            };
            let herdr_config = directory.path().join("herdr-config");
            let session_socket = herdr_config.join("sessions/messaging-build/herdr.sock");
            fs::create_dir_all(session_socket.parent().expect("session directory"))
                .expect("fixture Herdr session directory");
            let herdr_server =
                UnixListener::bind(&session_socket).expect("fixture Herdr server socket");
            let submissions = directory.path().join("seat-submissions");
            fs::create_dir(&submissions).expect("fixture seat submissions");
            report_fixture_submissions(
                submissions,
                std::sync::Arc::downgrade(&nexus.store),
                nexus.seat_submissions.clone(),
            );
            let fixture = Self {
                directory,
                nexus,
                snapshot_program,
                _herdr_server: herdr_server,
            };
            // The default session is listed, stopped, as Herdr lists it.
            fixture.list_herdr_sessions(&[
                ("default", herdr_config.join("herdr.sock")),
                ("messaging-build", session_socket),
            ]);
            fixture
        }

        /// The crash and restart of safeguards-0547.md S1: every in-memory
        /// state is lost (the gate, the reap workers and their rearm set,
        /// each Codex thread's turn, any answer not yet written) and only
        /// the store is durable. The store is closed, its logical state read
        /// while no Nexus holds it, and a new Nexus opened over the same
        /// path and the same Herdr fixture. The caller then runs its start:
        /// `recover_wakes`, and the reap supervisor where it runs one.
        fn restarted(&mut self) -> crate::tests::reaping::LogicalState {
            use crate::store::SetsNativeOwnerEnvironmentForTests;
            let path = self.directory.path().join("flow.sema");
            let scratch = self.directory.path().join("restart-scratch");
            let _ = fs::remove_dir_all(&scratch);
            fs::create_dir(&scratch).expect("restart scratch directory");
            let placeholder =
                FlowStore::open(&scratch.join("placeholder.sema")).expect("placeholder store");
            let closed = std::mem::replace(&mut self.nexus.store, placeholder.into());
            assert_eq!(
                std::sync::Arc::strong_count(&closed),
                1,
                "nothing else holds the crashed Nexus's store"
            );
            drop(closed);
            let state = crate::tests::reaping::LogicalState::read(&path);
            let mut store = FlowStore::open(&path).expect("the store reopens after the crash");
            store.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
                host_id: "isolated-test-host".into(),
                boot_id: "isolated-test-boot".into(),
            });
            self.nexus.store = store.into();
            self.nexus.dispatch_gate = std::sync::Mutex::new(());
            self.nexus.waking = crate::waking::WakingMetaflows::default();
            self.nexus.stopped_at_confirmation = crate::waking::StoppedAtConfirmation::default();
            self.nexus.refresh_refusals = crate::waking::RefreshRefusals::default();
            self.nexus.pane_watch_rearms = Default::default();
            self.nexus.pane_watch_hooks = Default::default();
            self.nexus.stopping = crate::Stopping::default();
            self.nexus.codex_turns = crate::codex_turns::CodexTurns::default();
            state
        }

        /// The fixture Herdr's shell function `submitted PANE TEXT`: a
        /// harness's own word that it took TEXT in PANE, as its hook or its
        /// app-server would give it, written whole into `seat-submissions/`
        /// for `report_fixture_submissions` to report.
        fn submission_report(&self) -> String {
            format!(
                "submitted() {{ d='{}'; t=\"$d/.part-$$\"; printf '%s\\n%s' \"$1\" \"$2\" > \"$t\" && mv \"$t\" \"$d/sub-$$-$(date +%s%N)\"; }}",
                self.directory.path().join("seat-submissions").display()
            )
        }

        /// What the fixture Herdr's `session list --json` answers. The last
        /// session listed is the live one: every launch opens there.
        fn list_herdr_sessions(&self, sessions: &[(&str, PathBuf)]) {
            if let Some((live, _)) = sessions.last() {
                self.declare_launch_session(live);
            }
            let sessions: Vec<serde_json::Value> = sessions
                .iter()
                .map(|(name, socket)| serde_json::json!({"name": name, "socket_path": socket}))
                .collect();
            fs::write(
                self.directory.path().join("herdr-sessions.json"),
                serde_json::json!({"sessions": sessions}).to_string(),
            )
            .expect("fixture Herdr session list");
        }

        /// Declares `session` the one Herdr session every launch opens in:
        /// the declared Nexus with that session, or, where none is declared,
        /// one declared from the fixture store's own runtime and delivery
        /// configuration.
        fn declare_launch_session(&self, session: &str) {
            use crate::store::delivery::RecordsDeliveries;
            use crate::store::{ConfiguresFlowStore, ManagesMetaflowState};
            let herdr_session_name = signal_flow::HerdrSessionName(session.into());
            let nexus = match self
                .nexus
                .store
                .configuration_snapshot()
                .expect("fixture configuration")
            {
                Some(snapshot) => meta_signal_flow::ConfigureNexus {
                    herdr_session_name,
                    ..snapshot.configured_nexus.0
                },
                None => {
                    let runtime = self
                        .nexus
                        .store
                        .runtime_configuration()
                        .expect("fixture runtime configuration");
                    let delivery = self
                        .nexus
                        .store
                        .delivery_configuration()
                        .expect("fixture delivery configuration");
                    let endpoint = |endpoint: crate::store::CodexEndpointConfiguration| {
                        meta_signal_flow::CodexEndpoint {
                            client_path: flow_ethos::ClientPath(endpoint.client_path),
                            home: flow_ethos::Home(endpoint.home),
                            control_socket_path: flow_ethos::ControlSocketPath(endpoint.socket),
                            model_name_vector: endpoint
                                .model_names
                                .into_iter()
                                .map(flow_ethos::ModelName)
                                .collect(),
                        }
                    };
                    meta_signal_flow::ConfigureNexus {
                        source_root: flow_ethos::SourceRoot(runtime.source_root),
                        first_codex_endpoint: endpoint(runtime.stable_codex),
                        second_codex_endpoint: endpoint(runtime.next_codex),
                        harness_profiles: flow_ethos::HarnessProfiles(
                            delivery.harness_profile_vector,
                        ),
                        meta_aspects: delivery.meta_aspects,
                        message_nexus_path_option: delivery.message_nexus_path_option,
                        message_nexus_binary_option: None,
                        curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                            "/fixture/curriculum.sock".into(),
                        ),
                        lease: flow_ethos::Lease(60),
                        presentation_prompt_option: None,
                        herdr_session_name,
                    }
                }
            };
            self.nexus
                .store
                .configure_change(meta_signal_flow::ConfigurationChange::Nexus(nexus))
                .expect("fixture launch session declared");
        }

        /// The fixture Herdr's first line after its shebang: it answers
        /// `session list --json` from the listing the fixture keeps.
        fn session_listing(&self) -> String {
            format!(
                "[ \"$1 $2 $3\" = \"session list --json\" ] && {{ cat '{}'; exit 0; }}",
                self.directory.path().join("herdr-sessions.json").display()
            )
        }

        fn node(&self) -> FlowNode {
            FlowNode {
                flow_id: flow_ethos::FlowId("908786".into()),
                session_id: signal_flow::SessionId("01a0b22c-e24f-7452-9940-64490878680f".into()),
                harness_kind: HarnessKind::Codex,
                endpoint_selection: EndpointSelection::Available(signal_flow::Available_Data {
                    endpoint_path: signal_flow::EndpointPath("/tmp/native-fallback.sock".into()),
                    route_readiness: signal_flow::RouteReadiness::Ready,
                }),
                herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                    herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                    herdr_agent_name: signal_flow::HerdrAgentName("psyche-mind-astra".into()),
                    herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
                    herdr_terminal_id: signal_flow::HerdrTerminalId("term_65bb7f87270cb3".into()),
                }),
                origin: Origin::Flow(FlowOrigin {
                    flow_id: flow_ethos::FlowId("908786".into()),
                    session_id: signal_flow::SessionId(
                        "01a0b22c-e24f-7452-9940-64490878680f".into(),
                    ),
                }),
                flow_lifecycle: FlowLifecycle::Active,
            }
        }

        fn current_agent(&self) -> serde_json::Value {
            serde_json::json!({
                "agent":"codex",
                "agent_status":"working",
                "cwd":"/home/li/primary",
                "focused":false,
                "foreground_cwd":"/home/li/primary",
                "interactive_ready":true,
                "name":"psyche-mind-astra",
                "pane_id":"w1:p3",
                "revision":4,
                "state_change_seq":85,
                "tab_id":"w1:t1",
                "terminal_id":"term_65bb7f87270cb3",
                "terminal_title":"primary",
                "terminal_title_stripped":"primary",
                "workspace_id":"w1",
                "agent_session":{
                    "source":"herdr:codex","agent":"codex","kind":"id",
                    "value":"01a0b22c-e24f-7452-9940-64490878680f"
                }
            })
        }
    }

    /// The pane list a Herdr snapshot carries beside `agents`: every pane
    /// an agent is shown in, as Herdr lists each pane it holds.
    fn panes_of(agents: &[serde_json::Value]) -> Vec<serde_json::Value> {
        agents
            .iter()
            .filter_map(|agent| {
                Some(serde_json::json!({
                    "pane_id": agent.get("pane_id")?,
                    "terminal_id": agent.get("terminal_id")?,
                }))
            })
            .collect()
    }

    impl ControlsHerdrSnapshot for NexusFixture {
        fn set_agents(&self, agents: Vec<serde_json::Value>) {
            let panes = panes_of(&agents);
            let snapshot = serde_json::json!({
                "id":"cli:api:snapshot",
                "result":{
                    "snapshot":{
                        "agents":agents,
                        "panes":panes,
                        "protocol":20,
                        "version":"0.8.2"
                    },
                    "type":"session_snapshot"
                }
            });
            let body = format!(
                "#!/bin/sh\n{}\n[ \"$1\" = \"--session\" ] && [ \"$3\" = \"api\" ] && [ \"$4\" = \"snapshot\" ] || exit 64\nprintf '%s\\n' '{}'\n",
                self.session_listing(),
                snapshot
            );
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
        }

        fn accept_pane_operations(
            &self,
            agents: Vec<serde_json::Value>,
            prompt: PromptFixture,
        ) -> PathBuf {
            let snapshot = serde_json::json!({
                "id":"cli:api:snapshot",
                "result":{"snapshot":{"agents":agents.clone(),"protocol":20,"version":"0.8.2"},
                "type":"session_snapshot"}
            });
            let log = self.directory.path().join("herdr-operations.log");
            let typed = self.directory.path().join("pane-typed.txt");
            let claude_session = agents.iter().find_map(|agent| {
                let session = agent.pointer("/agent_session/value")?.as_str()?;
                (agent.pointer("/agent_session/agent")?.as_str()? == "claude").then_some(session)
            });
            let claude_transcript = claude_session.map(|session| {
                self.directory
                    .path()
                    .join("native-transcripts/claude/-home-li-primary")
                    .join(format!("{session}.jsonl"))
            });
            let body = format!(
                "#!/bin/sh\n{}\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"agent prompt\") printf '%s\\n' \"$*\" >> '{}'; {} ;;\n  \"pane close\"|\"pane send-keys\"|\"agent wait\") printf '%s\\n' \"$*\" >> '{}' ;;\n  \"agent read\") printf '%s\\n' '❯ ' '› ' ;;\n  *) exit 64 ;;\nesac\n",
                self.session_listing(),
                self.submission_report(),
                snapshot,
                log.display(),
                prompt.shell(
                    &typed,
                    &agents,
                    claude_transcript.as_deref().zip(claude_session),
                ),
                log.display(),
            );
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
            log
        }

        fn typed(&self) -> Option<String> {
            fs::read_to_string(self.directory.path().join("pane-typed.txt")).ok()
        }
    }

    /// A malformed frame is answered FrameRefused on its own connection;
    /// an idle peer and the next caller are untouched.
    #[test]
    fn a_malformed_frame_is_refused_on_its_own_connection_alone() {
        use super::{CarriesSignalFrames, ServesOrdinary};
        use std::{io::Write, os::unix::net::UnixStream};
        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        let socket = fixture.directory.path().join("ordinary.sock");
        let served = socket.clone();
        std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
        // The name is moved in only once the socket listens.
        assert!(
            crate::event_wait::AwaitsPath::appeared_by(
                socket.as_path(),
                std::time::Instant::now() + Duration::from_secs(2)
            ),
            "ordinary socket never listened"
        );
        let connect = || {
            let peer = UnixStream::connect(&socket).expect("the ordinary socket listens");
            peer.set_read_timeout(Some(Duration::from_secs(2)))
                .expect("test peer timeout");
            peer
        };

        let mut idle = connect();
        // Garbage as the first frame is no greeting: the typed refusal.
        let mut garbage = connect();
        garbage
            .write_all(&16u32.to_be_bytes())
            .and_then(|_| garbage.write_all(&[0xff; 16]))
            .expect("garbage frame written");
        assert_eq!(garbage.receipt(), refused_ordinary_greeting());
        assert!(
            garbage.read_frame().is_err(),
            "the refusal ends the connection"
        );
        // Garbage after a greeting is a Query refused in this contract's
        // words.
        let mut greeted_garbage = connect();
        greeted_garbage.greet_ordinary();
        greeted_garbage
            .write_all(&16u32.to_be_bytes())
            .and_then(|_| greeted_garbage.write_all(&[0xff; 16]))
            .expect("garbage frame written");
        assert!(matches!(
            greeted_garbage.read_response(),
            Ok(Response::FrameRefused(refused)) if refused.flow_id_option.is_none()
        ));

        let mut peer = connect();
        peer.greet_ordinary();
        peer.write_query(&Query::Metaflows)
            .expect("metaflows query written");
        assert!(matches!(
            peer.read_response().expect("list answered"),
            Response::Listed(_)
        ));
        idle.write_all(&[0]).expect("idle peer still open");
    }

    /// O1: an ordinary connection is greeted before its Query is decoded,
    /// and an Observe stream greets once and then streams.
    #[test]
    fn an_ordinary_connection_is_greeted_before_its_query_is_decoded() {
        use super::{CarriesSignalFrames, Connection, ServesConnection};
        use crate::store::AnnouncesLaunchChanges;
        use std::net::Shutdown;
        let fixture = NexusFixture::new();
        let (peer, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        std::thread::scope(|scope| {
            let server = scope.spawn(|| {
                Connection::accepted(peer)
                    .unwrap()
                    .serve_ordinary(&fixture.nexus)
            });
            client.greet_ordinary();
            client.write_query(&Query::Metaflows).unwrap();
            assert!(matches!(
                client.read_response().unwrap(),
                Response::Listed(_)
            ));
            assert!(server.join().unwrap().is_ok());
        });
        // An Observe stream greets once, then streams.
        let (peer, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        std::thread::scope(|scope| {
            let server = scope.spawn(|| {
                Connection::accepted(peer)
                    .unwrap()
                    .serve_ordinary(&fixture.nexus)
            });
            client.greet_ordinary();
            client
                .write_query(&Query::Observe(signal_flow::ObserveSelection::Undecodable))
                .unwrap();
            assert!(matches!(
                client.read_response().unwrap(),
                Response::UndecodableObserved(_)
            ));
            client.shutdown(Shutdown::Both).unwrap();
            fixture.nexus.store.launch_changes.announce();
            let _ = server.join();
        });
    }

    /// O2 and O3: a signal-flow 60 peer, its greeting, its raw Report and its
    /// raw Bind, and a first frame too short for a greeting, are each refused
    /// typed before any Query is decoded; nothing takes effect; each leaves
    /// one record, metadata alone.
    #[test]
    fn a_signal_flow_60_peer_is_refused_typed_before_any_query_is_decoded() {
        use super::{CarriesSignalFrames, Connection, ServesConnection};
        use crate::store::ManagesMetaflowState;
        use crate::store::undecodable_frame::RecordsUndecodableFrames;
        use signal::Contracted;
        use signal_flow::Process;
        let fixture = NexusFixture::new();
        let marker = "REFUSED-FRAME-BODY-MARKER";
        let report = signal_flow_60::Query::Report(signal_flow_60::Report_Data {
            flow_id: flow_ethos::FlowId("abcdef".into()),
            process_option: None,
            event: signal_flow_60::Event::ToolUsed(marker.into()),
        });
        let address = signal_flow::Address {
            aspect: Aspect::Field,
            topic: flow_ethos::Topic("bindRefused".into()),
            model: signal_flow::Model::Sol,
        };
        let bind = signal_flow_60::Query::Bind(signal_flow_60::Bind_Data {
            address: address.clone(),
            process: Process {
                pid: flow_ethos::generated::library::Pid(1),
                started: flow_ethos::generated::library::Started(1),
            },
        });
        let frames: Vec<Vec<u8>> = vec![
            rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow_60::Query::greeting())
                .unwrap()
                .to_vec(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&report)
                .unwrap()
                .to_vec(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&bind)
                .unwrap()
                .to_vec(),
            vec![1, 2, 3, 4],
        ];
        assert_ne!(
            signal_flow_60::Query::contract_digest(),
            signal_flow::Query::contract_digest()
        );
        let metaflows = fixture.nexus.store.list_metaflows().unwrap();
        for (index, frame) in frames.into_iter().enumerate() {
            let (peer, mut client) = UnixStream::pair().unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            std::thread::scope(|scope| {
                let server = scope.spawn(|| {
                    Connection::accepted(peer)
                        .unwrap()
                        .serve_ordinary(&fixture.nexus)
                });
                client.write_frame(&frame).unwrap();
                assert_eq!(
                    client.receipt(),
                    refused_ordinary_greeting(),
                    "frame {index}"
                );
                assert!(
                    client.read_frame().is_err(),
                    "frame {index}: the connection ends"
                );
                assert!(server.join().unwrap().is_err());
            });
            let records = fixture.nexus.store.undecodable_frames().unwrap();
            assert_eq!(records.len(), index + 1, "one record per refusal");
            let record = rkyv::to_bytes::<rkyv::rancor::Error>(records.last().unwrap()).unwrap();
            assert!(
                !record
                    .windows(marker.len())
                    .any(|part| part == marker.as_bytes()),
                "the record keeps nothing of the frame"
            );
        }
        assert_eq!(
            fixture.nexus.store.list_metaflows().unwrap(),
            metaflows,
            "the raw Bind took no effect"
        );
    }

    /// O6: the empty connection a stop wakes the ordinary accept loop with
    /// ends at the greeting read, and the loop returns.
    #[test]
    fn a_stop_wakes_the_ordinary_accept_loop_through_the_greeting_read() {
        use super::{ServesOrdinary, StopsNexus};
        let fixture = NexusFixture::new();
        let socket = fixture.directory.path().join("ordinary-stop.sock");
        std::thread::scope(|scope| {
            let serving = scope.spawn(|| fixture.nexus.serve_ordinary(&socket));
            assert!(crate::event_wait::AwaitsPath::appeared_by(
                socket.as_path(),
                std::time::Instant::now() + Duration::from_secs(5)
            ));
            fixture.nexus.stop(&[socket.as_path()]);
            let _ = serving.join().expect("the accept loop returns");
        });
    }

    /// The receipt a refused ordinary greeting is answered with: this
    /// contract's own digest.
    pub(crate) fn refused_ordinary_greeting() -> signal::HandshakeReceipt {
        use signal::Contracted;
        signal::HandshakeReceipt::GreetingRefused(signal::HandshakeRejection::ContractMismatch(
            signal_flow::Query::contract_digest(),
        ))
    }

    /// A test peer of the ordinary socket greets as every client does.
    pub(crate) trait GreetsOrdinarySocket {
        /// Writes signal-flow's greeting and asserts it is Greeted.
        fn greet_ordinary(&mut self);
        /// The receipt frame the Nexus wrote.
        fn receipt(&mut self) -> signal::HandshakeReceipt;
    }

    impl GreetsOrdinarySocket for std::os::unix::net::UnixStream {
        fn greet_ordinary(&mut self) {
            use super::CarriesSignalFrames;
            use signal::Contracted;
            self.write_frame(
                &rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Query::greeting())
                    .expect("greeting encodes"),
            )
            .expect("greeting written");
            assert_eq!(
                self.receipt(),
                signal::HandshakeReceipt::Greeted(signal_flow::Query::contract_digest())
            );
        }

        fn receipt(&mut self) -> signal::HandshakeReceipt {
            use super::CarriesSignalFrames;
            rkyv::from_bytes::<signal::HandshakeReceipt, rkyv::rancor::Error>(
                &self.read_frame().expect("receipt frame"),
            )
            .expect("receipt decodes")
        }
    }

    #[test]
    fn running_nexus_parses_actual_working_interactive_snapshot() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(node.clone())
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into()))
            )),
            Response::RecipientResolved(node)
        );
    }

    impl NexusFixture {
        fn settled_agent(&self) -> serde_json::Value {
            let mut agent = self.current_agent();
            agent["agent_status"] = serde_json::Value::String("idle".into());
            agent
        }

        fn register_with(&self, lifecycle: FlowLifecycle) {
            let mut node = self.node();
            node.flow_lifecycle = lifecycle;
            let response = self
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node));
            assert!(
                matches!(response, meta_signal_flow::Response::FlowRegistered(_)),
                "{response:?}"
            );
        }

        /// A MiddleAbrupt Deliver of `text` from the owner: the tier that
        /// neither interrupts nor waits for the recipient to rest.
        fn send(&self, text: &str) -> Sent {
            self.send_to("908786", text)
        }

        fn send_to(&self, flow_id: &str, text: &str) -> Sent {
            match self.nexus.dispatch_meta(meta_signal_flow::Query::Deliver(
                meta_signal_flow::DeliveryRequest {
                    delivery_id: meta_signal_flow::DeliveryId(format!("delivery-{flow_id}-{text}")),
                    flow_id: flow_ethos::FlowId(flow_id.into()),
                    message: middle_abrupt(text),
                },
            )) {
                meta_signal_flow::Response::Delivered(delivery) => {
                    assert_eq!(
                        delivery.interrupt_witness,
                        meta_signal_flow::InterruptWitness::NotRequested
                    );
                    Sent::Graded(delivery.delivery_grade)
                }
                meta_signal_flow::Response::DeliveryRejected(rejection) => Sent::Refused(rejection),
                other => panic!("Deliver answers Delivered or DeliveryRejected: {other:?}"),
            }
        }

        /// The lifecycle as the row holds it, with no reconciliation. This
        /// is what the Deliver path did or did not write; reconciliation is
        /// a separate claim from the stored row.
        fn stored_lifecycle(&self) -> FlowLifecycle {
            use crate::store::ReadsFlowRows as _;
            self.nexus
                .store
                .flow_node("908786")
                .expect("stored row")
                .expect("registered flow")
                .flow_lifecycle
        }

        fn only_lifecycle(&self) -> FlowLifecycle {
            use crate::store::ReadsFlowRows as _;
            let rows = self.nexus.store.flow_nodes().expect("Flow rows read");
            assert_eq!(rows.len(), 1);
            self.nexus.reconciled_node(rows[0].clone()).flow_lifecycle
        }
    }

    /// What a Deliver answered, without its identifiers.
    #[derive(Debug, PartialEq, Eq)]
    enum Sent {
        Graded(meta_signal_flow::DeliveryGrade),
        Refused(meta_signal_flow::DeliveryRejection),
    }

    fn middle_abrupt(text: &str) -> meta_signal_flow::Message {
        meta_signal_flow::Message::MiddleAbrupt(meta_signal_flow::Letter {
            visible_sender: signal_flow::VisibleSender::Notification,
            content: meta_signal_flow::Content::Text(text.into()),
        })
    }

    /// The pane text of a MiddleAbrupt from the Nexus, written out by hand.
    fn rendered(text: &str) -> String {
        format!("MiddleAbrupt.{{ Notification Text.«{text}» }}")
    }

    fn prompts(operation_log: &std::path::Path) -> Vec<String> {
        fs::read_to_string(operation_log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains(" agent prompt "))
            .map(str::to_owned)
            .collect()
    }

    const OBSERVED_PROMPT: &str = "--session messaging-build agent prompt w1:p3 MiddleAbrupt.{ Notification Text.«bare prompt» } --wait --until working --until idle --until done --until blocked --timeout 10000";

    #[test]
    fn observed_send_promotes_a_pending_flow_and_types_only_the_bare_input() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented),
            "a settled recipient seen reacting is Presented"
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(
            fixture.typed().as_deref(),
            Some(rendered("bare prompt").as_str())
        );
        assert_eq!(prompts(&operation_log), vec![OBSERVED_PROMPT.to_owned()]);

        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Stop(flow_ethos::FlowId("908786".into()))),
            Response::Stopped(flow_ethos::FlowId("908786".into()))
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Stopped);
        let operations = fs::read_to_string(operation_log).expect("Herdr operation log");
        assert!(operations.contains("pane close w1:p3"));
        assert!(!operations.contains("pane wait-output"));
        assert!(!operations.contains("pane read"));
    }

    /// The pane a staged launch registered, as Herdr shows a seat not yet
    /// able to take a prompt.
    fn unready_launch_agent() -> serde_json::Value {
        serde_json::json!({
            "agent":"codex","agent_status":"unknown","interactive_ready":false,
            "name":"fixture-agent","pane_id":"w1:p1","terminal_id":"fixture-terminal",
            "workspace_id":"fixture-workspace",
            "agent_session":{
                "source":"herdr:codex","agent":"codex","kind":"id",
                "value":STAGED_SESSION
            }
        })
    }

    /// A launch refused after its flow was registered, whose FlowId claim
    /// was given back, leaves a Pending flow in a live pane that is not
    /// ready for a prompt. Stop resolves the route from the recorded pane,
    /// closes it, and records the flow Stopped.
    #[test]
    fn stop_closes_the_live_pane_of_a_pending_flow_that_was_never_ready() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![unready_launch_agent()],
            PromptFixture::Prompted("w1:p1"),
        );
        let launch = fixture.staged_launch("orphaned-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        fs::remove_file(fixture.directory.path().join("flows/.908786.flow-id")).unwrap();
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Pending);

        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Stop(flow_ethos::FlowId("908786".into()))),
            Response::Stopped(flow_ethos::FlowId("908786".into()))
        );
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        let operations = fs::read_to_string(&operation_log).expect("Herdr operation log");
        assert!(operations.contains("pane close w1:p1"), "{operations}");

        // A pane Herdr no longer shows is not closed: the route is gone.
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(vec![], PromptFixture::Prompted("w1:p1"));
        let launch = fixture.staged_launch("vanished-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Stop(flow_ethos::FlowId("908786".into()))),
            Response::StopRejected(signal_flow::StopRejection::RouteUnavailable)
        );
    }

    /// A launch refused after its flow was registered in its pane closes
    /// that pane and records the flow Stopped, which gives back the Address
    /// its acknowledgement held.
    #[test]
    fn a_refused_launch_closes_its_pane_and_stops_its_flow() {
        use crate::launching::{AbandonsRefusedLaunch, LaunchesFlows};
        use crate::store::address_hold::HoldsLaunchAddress;
        use crate::store::metaflow::SleepsMetaflows;

        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![unready_launch_agent()],
            PromptFixture::Prompted("w1:p1"),
        );
        let launch = fixture.staged_launch("refused-title-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        assert!(
            fixture
                .nexus
                .store
                .metaflow_awake_as("908786")
                .unwrap()
                .is_some()
        );

        fixture
            .nexus
            .abandon_refused_launch("refused-title-request", &HarnessKind::Codex, None);
        let operations = fs::read_to_string(&operation_log).expect("Herdr operation log");
        assert!(operations.contains("pane close w1:p1"), "{operations}");
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        assert_eq!(
            fixture.nexus.store.metaflow_awake_as("908786").unwrap(),
            None
        );
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert_eq!(
            fixture.nexus.settle(
                "refused-title-request",
                Response::StartRejected(title_not_ready())
            ),
            Response::StartRejected(title_not_ready())
        );
    }

    /// A pane whose harness never appeared shows in Herdr's snapshot as a
    /// pane with no agent; the refused launch closes it once it holds the
    /// session the pane was created for. Without one, nothing is closed and
    /// the pane stays recorded for a later reconciliation.
    #[test]
    fn a_refused_launch_closes_a_pane_whose_harness_never_appeared() {
        use crate::herdr::PaneOwnership;
        use crate::launching::{AbandonsRefusedLaunch, LaunchTeardown};
        use crate::store::opened_thread::RecordsOpenedThreads;
        use crate::store::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
        let fixture = NexusFixture::new();
        let launch = fixture.staged_launch("bare-pane-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        assert!(
            fixture
                .nexus
                .store
                .record_created_launch_pane(launch.pane.clone())
                .unwrap()
        );
        fixture
            .nexus
            .store
            .request_harness_start("bare-pane-request")
            .unwrap();
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{
                "agents":[],
                "panes":[{"pane_id":launch.pane.herdr_pane_id.0,"terminal_id":launch.pane.herdr_terminal_id.0}],
                "protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        let log = fixture.directory.path().join("bare-pane.log");
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"pane close\") printf '%s\\n' \"$*\" >> '{}' ;;\n  *) exit 64 ;;\nesac\n",
            snapshot,
            log.display(),
        ));
        // No session recorded for the pane: missing identity closes nothing.
        assert_eq!(
            fixture
                .nexus
                .abandon_refused_launch("bare-pane-request", &HarnessKind::Codex, None),
            LaunchTeardown::Kept(PaneOwnership::Unidentified)
        );
        assert!(fs::read_to_string(&log).unwrap_or_default().is_empty());
        assert!(
            fixture
                .nexus
                .store
                .recoverable_launch_pane("bare-pane-request")
                .unwrap()
                .is_some()
        );
        // The thread the launch opened is the pane's session.
        assert!(
            fixture
                .nexus
                .store
                .record_opened_thread("bare-pane-request", "bare-pane-thread")
                .unwrap()
        );
        assert_eq!(
            fixture
                .nexus
                .abandon_refused_launch("bare-pane-request", &HarnessKind::Codex, None),
            LaunchTeardown::TakenDown
        );
        let operations = fs::read_to_string(&log).expect("Herdr operation log");
        assert!(
            operations.contains(&format!("pane close {}", launch.pane.herdr_pane_id.0)),
            "{operations}"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .recoverable_launch_pane("bare-pane-request")
                .unwrap(),
            None
        );
    }

    #[test]
    fn working_pending_flow_takes_the_send_as_accepted_and_stays_pending() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.current_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("queued prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Transported)
        );
        // The Deliver did not promote it: only a Presented Deliver does that.
        // List separately reports the seat live, because its pane is.
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending);
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(
            fixture.typed().as_deref(),
            Some(rendered("queued prompt").as_str())
        );
        assert_eq!(
            prompts(&operation_log),
            vec![format!(
                "--session messaging-build agent prompt w1:p3 {}",
                rendered("queued prompt")
            )]
        );
    }

    #[test]
    fn refusal_before_input_types_nothing_and_is_not_delivered() {
        for code in [
            "agent_blocked",
            "agent_not_found",
            "agent_not_ready",
            "agent_target_ambiguous",
            "empty_agent_prompt",
            "agent_prompt_failed",
        ] {
            let fixture = NexusFixture::new();
            let operation_log = fixture.accept_pane_operations(
                vec![fixture.settled_agent()],
                PromptFixture::RefusedBeforeInput(code),
            );
            fixture.register_with(FlowLifecycle::Pending);

            assert_eq!(
                fixture.send("bare prompt"),
                Sent::Refused(meta_signal_flow::DeliveryRejection::NotDelivered),
                "{code}"
            );
            assert_eq!(fixture.typed(), None, "{code}");
            assert_eq!(prompts(&operation_log).len(), 1, "{code}");
            assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending, "{code}");
        }
    }

    #[test]
    fn typed_prompt_whose_reaction_is_unobserved_is_uncertain_once() {
        for failure in [
            "agent_prompt_stalled",
            "timeout",
            "agent_not_running",
            "unlisted",
        ] {
            let fixture = NexusFixture::new();
            let operation_log = fixture.accept_pane_operations(
                vec![fixture.settled_agent()],
                PromptFixture::FailedAfterInput(failure),
            );
            fixture.register_with(FlowLifecycle::Pending);

            assert_eq!(
                fixture.send("bare prompt"),
                Sent::Graded(meta_signal_flow::DeliveryGrade::Uncertain),
                "{failure}"
            );
            assert_eq!(
                fixture.typed().as_deref(),
                Some(rendered("bare prompt").as_str()),
                "{failure}"
            );
            assert_eq!(
                prompts(&operation_log),
                vec![OBSERVED_PROMPT.to_owned()],
                "{failure}: exactly one prompt, no retry"
            );
            assert_eq!(
                fixture.stored_lifecycle(),
                FlowLifecycle::Pending,
                "{failure}"
            );
        }
    }

    #[test]
    fn reaction_reported_for_another_pane_is_uncertain_and_keeps_pending() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p9"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Uncertain)
        );
        assert_eq!(prompts(&operation_log).len(), 1);
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending);
    }

    #[test]
    fn active_send_to_a_working_flow_is_accepted_without_claiming_presentation() {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.current_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);

        assert_eq!(
            fixture.send("ordinary active prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Transported)
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
    }

    #[test]
    fn active_send_to_a_settled_flow_is_observed_presented() {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented)
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
    }

    #[test]
    fn stale_pending_send_is_refused_before_prompt_and_keeps_pending() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let mut node = fixture.node();
        node.flow_lifecycle = FlowLifecycle::Pending;
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node)),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        let mut stale = fixture.current_agent();
        stale["terminal_id"] = serde_json::Value::String("term-replaced".into());
        fixture.set_agents(vec![stale]);

        assert_eq!(
            fixture.send_to("908786", "stale route prompt"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        use crate::store::ReadsFlowRows as _;
        let rows = fixture.nexus.store.flow_nodes().expect("Flow rows read");
        assert_eq!(rows[0].flow_lifecycle, FlowLifecycle::Pending);
    }

    #[test]
    fn meta_bind_existing_atomically_adopts_one_verified_seat() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let container = flow_container(&socket_path);
        let mut accepted = existing_binding("a001", "pane-1");
        accepted.native_session_id =
            signal_flow::NativeSessionId("01a0b22c-e24f-7452-9940-64490878680f".into());
        let address = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("message".into()),
            model: Model::Sol,
        };

        let response = with_bound_world(&fixture, &[&accepted.clone()], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: container.clone(),
                    address: address.clone(),
                    flow_binding: accepted,
                }),
                Some(adoption_resolver()),
            )
        });
        let bound = match response {
            meta_signal_flow::Response::BoundExisting(bound) => bound,
            other => panic!("valid container returned {other:?}"),
        };
        assert_eq!(bound.flow_container, container);
        assert!(matches!(
            &bound.flow_binding_result,
            meta_signal_flow::FlowBindingResult::Bound(binding)
                if binding.flow_id.0 == "a001"
                    && binding.flow_lifecycle
                        == meta_signal_flow::FlowLifecycle::AdoptedActive
        ));

        let Response::RecipientResolved(node) = fixture.nexus.dispatch(Query::ResolveRecipient(
            signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("a001".into())),
        )) else {
            panic!("accepted existing flow must resolve")
        };
        assert_eq!(node.flow_lifecycle, FlowLifecycle::Active);
        assert_eq!(node.origin, Origin::Unbound);
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId(
                    "field-ambiguous".into()
                ))
            )),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::UnknownFlow
            )
        ));
    }

    #[test]
    fn meta_bind_existing_rejects_unverified_container_without_store_effects() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let mut container = flow_container(&socket_path);
        container.process_identity.process_start_token =
            meta_signal_flow::ProcessStartToken("stale".into());

        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                    meta_signal_flow::MetaBindExisting {
                        flow_container: container,
                        address: signal_flow::Address {
                            aspect: Aspect::Mind,
                            topic: flow_ethos::Topic("message".into()),
                            model: Model::Sol,
                        },
                        flow_binding: existing_binding("a002", "pane-1"),
                    }
                ),),
            meta_signal_flow::Response::BindExistingRejected(
                meta_signal_flow::BindExistingRejection::ContainerIdentityMismatch
            )
        );
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("a002".into()))
            )),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::UnknownFlow
            )
        ));
    }

    /// MetaBindExisting is taken from any peer: one naming another owner
    /// than the container's binds the verified seat all the same.
    #[test]
    fn meta_bind_existing_is_taken_from_any_peer() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr-owner.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let binding = existing_binding("a003", "pane-1");
        let address = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("message".into()),
            model: Model::Sol,
        };
        let response = with_bound_world(&fixture, &[&binding], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address,
                    flow_binding: binding.clone(),
                }),
                Some(meta_signal_flow::ResolverIdentity {
                    process_identity: current_process_identity(),
                    flow_id_option: Some(flow_ethos::FlowId("different-owner".into())),
                }),
            )
        });
        assert!(
            matches!(
                &response,
                meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                    flow_binding_result: meta_signal_flow::FlowBindingResult::Bound(_),
                    ..
                })
            ),
            "{response:?}"
        );
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("a003".into()))
            )),
            Response::RecipientResolved(_)
        ));
    }

    #[test]
    fn native_claim_compares_the_canonical_identity_of_a_uuid_session() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr-canonical-id.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let binding = existing_binding("a008", "pane-1");
        let response = with_bound_world(&fixture, &[&binding], || {
            let claim = fixture.directory.path().join("flows/.a008.flow-id");
            fs::write(
                claim,
                "version=1\nharness=codex\nidentity=ffffffffffffffffffffffffffffffff\nalias=a008\n",
            )
            .expect("mismatched canonical claim written");
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address: signal_flow::Address {
                        aspect: Aspect::Mind,
                        topic: flow_ethos::Topic("core".into()),
                        model: Model::Sol,
                    },
                    flow_binding: binding.clone(),
                }),
                Some(adoption_resolver()),
            )
        });
        assert!(matches!(
            response,
            meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                flow_binding_result: meta_signal_flow::FlowBindingResult::Refused(
                    meta_signal_flow::RefusedFlowBinding {
                        flow_binding_refusal_reason:
                            meta_signal_flow::FlowBindingRefusalReason::NativeIdentityMismatch,
                        ..
                    }
                ),
                ..
            })
        ));
        let accepted = existing_binding("a009", "pane-2");
        let accepted = meta_signal_flow::FlowBinding {
            native_session_id: signal_flow::NativeSessionId(
                "01a0b22c-e24f-7452-9940-64490878680f".into(),
            ),
            ..accepted
        };
        let response = with_bound_world(&fixture, &[&accepted], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address: signal_flow::Address {
                        aspect: Aspect::Mind,
                        topic: flow_ethos::Topic("core".into()),
                        model: Model::Sol,
                    },
                    flow_binding: accepted.clone(),
                }),
                Some(adoption_resolver()),
            )
        });
        assert!(matches!(
            response,
            meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                flow_binding_result: meta_signal_flow::FlowBindingResult::Bound(_),
                ..
            })
        ));
    }

    #[test]
    fn adoption_of_a_historical_owner_without_process_proof_refuses_without_touching_queue() {
        use crate::store::ManagesMetaflowState;
        use crate::store::metaflow::SeedsMetaflow;
        let fixture = NexusFixture::new();
        let address = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("message".into()),
            model: Model::Sol,
        };
        let mut previous = fixture.node();
        previous.flow_id = flow_ethos::FlowId("historical-owner".into());
        previous.session_id = signal_flow::SessionId("historical-session".into());
        previous.flow_lifecycle = FlowLifecycle::Active;
        let previous_role = CallerIdentity {
            flow_id: previous.flow_id.clone(),
            aspect: Aspect::Mind,
            model_name: flow_ethos::ModelName("gpt-sol".into()),
        };
        fixture
            .nexus
            .store
            .register_flow_in_role(previous, previous_role)
            .expect("legacy owner row persists without native proof");
        let original = signal_flow::Permaflow {
            address: address.clone(),
            permaflow_state: flow_ethos::PermaflowState::Awake(flow_ethos::FlowId(
                "historical-owner".into(),
            )),
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![signal_flow::QueueEntry {
                logical_delivery_id: flow_ethos::LogicalDeliveryId("retained-logical-id".into()),
                request_id: flow_ethos::RequestId("test".into()),
                sent_by: signal_flow::SentBy::Sender(signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: flow_ethos::Topic("core".into()),
                    model: Model::Luna,
                }),
                delivery: flow_ethos::Delivery::Queue,
                message: signal_flow::Message::Soft(signal_flow::Request::Order(
                    "leave queued".into(),
                )),
            }]),
        };
        fixture.nexus.store.seed_metaflow(original.clone());

        let socket_path = fixture.directory.path().join("herdr-unknown-owner.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let binding = existing_binding("a004", "pane-1");
        let response = with_bound_world(&fixture, &[&binding], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address: address.clone(),
                    flow_binding: binding.clone(),
                }),
                Some(adoption_resolver()),
            )
        });
        assert!(matches!(
            response,
            meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                flow_binding_result: meta_signal_flow::FlowBindingResult::Refused(
                    meta_signal_flow::RefusedFlowBinding {
                        flow_binding_refusal_reason:
                            meta_signal_flow::FlowBindingRefusalReason::AddressOwnerUnknown,
                        ..
                    }
                ),
                ..
            })
        ));
        assert_eq!(
            fixture.nexus.store.list_metaflows().unwrap(),
            vec![original]
        );
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("a004".into()))
            )),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::UnknownFlow
            )
        ));
    }

    #[test]
    fn same_verified_owner_can_refresh_without_changing_queued_logical_work() {
        use crate::store::ManagesMetaflowState;
        use crate::store::metaflow::SeedsMetaflow;
        let fixture = NexusFixture::new();
        let binding = existing_binding("a001", "pane-1");
        let address = signal_flow::Address {
            aspect: binding.aspect.clone(),
            topic: flow_ethos::Topic("core".into()),
            model: Model::Sol,
        };
        let original_queue = vec![signal_flow::QueueEntry {
            logical_delivery_id: flow_ethos::LogicalDeliveryId("retained-logical-id".into()),
            request_id: flow_ethos::RequestId("test".into()),
            sent_by: signal_flow::SentBy::Sender(signal_flow::Address {
                aspect: Aspect::Field,
                topic: flow_ethos::Topic("core".into()),
                model: Model::Luna,
            }),
            delivery: flow_ethos::Delivery::Queue,
            message: signal_flow::Message::Soft(signal_flow::Request::Order("leave queued".into())),
        }];
        fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
            address,
            permaflow_state: flow_ethos::PermaflowState::Asleep,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(original_queue.clone()),
        });
        let first = rebind(&fixture, binding.clone());
        let second = rebind(&fixture, binding);
        assert!(
            matches!(&first, meta_signal_flow::FlowBindingResult::Bound(_)),
            "{first:?}"
        );
        assert!(
            matches!(&second, meta_signal_flow::FlowBindingResult::Bound(_)),
            "{second:?}"
        );
        assert_eq!(
            fixture.nexus.store.list_metaflows().unwrap()[0].queue,
            flow_ethos::Queue(original_queue)
        );
    }

    #[test]
    fn adoption_refuses_a_different_live_address_owner_without_mutation() {
        use crate::store::ManagesMetaflowState;
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr-live-owner.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let address = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Sol,
        };
        let incumbent = existing_binding("a005", "pane-incumbent");
        let incumbent_result = with_bound_world(&fixture, &[&incumbent], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address: address.clone(),
                    flow_binding: incumbent.clone(),
                }),
                Some(adoption_resolver()),
            )
        });
        assert!(matches!(
            incumbent_result,
            meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                flow_binding_result: meta_signal_flow::FlowBindingResult::Bound(_),
                ..
            })
        ));
        let original = fixture.nexus.store.list_metaflows().unwrap();

        let challenger = existing_binding("a006", "pane-challenger");
        let response = with_bound_world(&fixture, &[&challenger], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address: address.clone(),
                    flow_binding: challenger.clone(),
                }),
                Some(adoption_resolver()),
            )
        });
        assert!(matches!(
            response,
            meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                flow_binding_result: meta_signal_flow::FlowBindingResult::Refused(
                    meta_signal_flow::RefusedFlowBinding {
                        flow_binding_refusal_reason:
                            meta_signal_flow::FlowBindingRefusalReason::AddressOwnerLive,
                        ..
                    }
                ),
                ..
            })
        ));
        assert_eq!(fixture.nexus.store.list_metaflows().unwrap(), original);
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("a006".into()))
            )),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::UnknownFlow
            )
        ));
    }

    /// A binding is refused RoleMismatch, writing nothing, when its seat's
    /// identity does not match its Address: its model is not the native
    /// model configured for the Address's Model, or its aspect is not the
    /// Address's. No power or rank is read.
    #[test]
    fn adoption_refuses_an_address_whose_role_does_not_match_without_partial_rows() {
        use crate::store::ManagesMetaflowState;
        for (case, address) in [
            (
                "the binding's model is not the one configured for Astra",
                signal_flow::Address {
                    aspect: Aspect::Mind,
                    topic: flow_ethos::Topic("message".into()),
                    model: Model::Astra,
                },
            ),
            (
                "the binding's aspect is not the Address's",
                signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: flow_ethos::Topic("message".into()),
                    model: Model::Sol,
                },
            ),
        ] {
            let fixture = NexusFixture::new();
            configure_existing_binding_model(&fixture);
            let socket_path = fixture.directory.path().join("herdr-role-mismatch.sock");
            let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
            let binding = existing_binding("a007", "pane-1");
            let response = with_bound_world(&fixture, &[&binding], || {
                fixture.nexus.dispatch_meta_from(
                    meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                        flow_container: flow_container(&socket_path),
                        address,
                        flow_binding: binding.clone(),
                    }),
                    Some(adoption_resolver()),
                )
            });
            assert!(
                matches!(
                    response,
                    meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                        flow_binding_result: meta_signal_flow::FlowBindingResult::Refused(
                            meta_signal_flow::RefusedFlowBinding {
                                flow_binding_refusal_reason:
                                    meta_signal_flow::FlowBindingRefusalReason::RoleMismatch,
                                ..
                            }
                        ),
                        ..
                    })
                ),
                "{case}: {response:?}"
            );
            assert!(matches!(
                fixture.nexus.dispatch(Query::ResolveRecipient(
                    signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("a007".into()))
                )),
                Response::RecipientResolutionRejected(
                    signal_flow::RecipientResolutionRejection::UnknownFlow
                )
            ));
            assert!(
                fixture.nexus.store.list_metaflows().unwrap().is_empty(),
                "{case}"
            );
        }
    }

    /// A Codex pane as Herdr 0.8.2 reports a rested imported Codex flow: done,
    /// with no `interactive_ready` key (the live 26c50c, 2026-09-25).
    fn rested_imported_codex_agent() -> serde_json::Value {
        serde_json::json!({
            "agent":"codex", "agent_status":"done", "name":"agent-w1:p3",
            "pane_id":"w1:p3", "terminal_id":"terminal-w1:p3"
        })
    }

    /// Herdr omits `name` for a pane it was never given one for. The label
    /// is not part of the binding and is no part of the presentation either:
    /// a nameless recipient seen reacting is Presented, as any other is.
    /// Grading through `current_route`, which re-reads the name, settled
    /// every such delivery Uncertain.
    fn nameless_rested_codex_agent() -> serde_json::Value {
        let mut agent = rested_imported_codex_agent();
        agent.as_object_mut().expect("agent object").remove("name");
        agent
    }

    #[test]
    fn a_nameless_recipient_seen_reacting_is_presented() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![nameless_rested_codex_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        bind_existing_caller(&fixture, "908786", "w1:p3");

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented)
        );
        assert_eq!(prompts(&operation_log), vec![OBSERVED_PROMPT.to_owned()]);
    }

    fn resolved(fixture: &NexusFixture) -> FlowNode {
        let Response::RecipientResolved(node) = fixture.nexus.dispatch(Query::ResolveRecipient(
            signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into())),
        )) else {
            panic!("an imported flow must resolve")
        };
        node
    }

    fn listed(fixture: &NexusFixture) -> FlowNode {
        use crate::store::ReadsFlowRows as _;
        let rows = fixture.nexus.store.flow_nodes().expect("Flow rows read");
        assert_eq!(rows.len(), 1);
        fixture.nexus.reconciled_node(rows[0].clone())
    }

    // The fixture's Codex endpoints name sockets that do not exist: an
    // imported Codex flow is routed through its Herdr pane alone, and its
    // endpoint stays a separate fact that neither gates nor changes.
    #[test]
    fn an_imported_codex_flow_is_sent_through_its_pane_without_an_endpoint() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![rested_imported_codex_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        bind_existing_caller(&fixture, "908786", "w1:p3");

        let node = resolved(&fixture);
        assert_eq!(node.endpoint_selection, EndpointSelection::Unavailable);
        assert_eq!(
            node.herdr_route_selection,
            HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                herdr_agent_name: signal_flow::HerdrAgentName("agent-w1:p3".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("w1:p3".into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("terminal-w1:p3".into()),
            })
        );
        assert_eq!(node.flow_lifecycle, FlowLifecycle::Active);

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented)
        );
        assert_eq!(
            fixture.typed().as_deref(),
            Some(rendered("bare prompt").as_str())
        );
        assert_eq!(prompts(&operation_log), vec![OBSERVED_PROMPT.to_owned()]);
        let after = listed(&fixture);
        assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
        assert_eq!(after.endpoint_selection, EndpointSelection::Unavailable);
    }

    #[test]
    fn an_imported_codex_flow_whose_pane_is_gone_is_refused_before_typing() {
        let fixture = NexusFixture::new();
        let operation_log =
            fixture.accept_pane_operations(vec![], PromptFixture::Prompted("w1:p3"));
        bind_existing_caller(&fixture, "908786", "w1:p3");

        let node = resolved(&fixture);
        assert_eq!(node.herdr_route_selection, HerdrRouteSelection::Unavailable);
        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        assert_eq!(fixture.typed(), None);
        assert!(prompts(&operation_log).is_empty());
        // The pane is gone and Flow never closed it, so the flow Exited. The
        // Deliver above is the command that witnessed it, so the row itself now
        // says so; List would report the same either way, and Retired is not
        // what an observation ever produces.
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Exited);
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Exited);
    }

    fn rebind(
        fixture: &NexusFixture,
        binding: meta_signal_flow::FlowBinding,
    ) -> meta_signal_flow::FlowBindingResult {
        rebind_against(fixture, binding.clone(), binding)
    }

    fn rebind_against(
        fixture: &NexusFixture,
        binding: meta_signal_flow::FlowBinding,
        witnessed_binding: meta_signal_flow::FlowBinding,
    ) -> meta_signal_flow::FlowBindingResult {
        let socket_path = fixture.directory.path().join("herdr-rebind.sock");
        let _ = fs::remove_file(&socket_path);
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let meta_signal_flow::Response::BoundExisting(bound) =
            with_bound_world(fixture, &[&witnessed_binding], || {
                fixture.nexus.dispatch_meta_from(
                    meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                        flow_container: flow_container(&socket_path),
                        address: signal_flow::Address {
                            aspect: binding.aspect.clone(),
                            topic: flow_ethos::Topic("core".into()),
                            model: signal_flow::Model::Sol,
                        },
                        flow_binding: binding,
                    }),
                    Some(adoption_resolver()),
                )
            })
        else {
            panic!("a live container answers per binding")
        };
        bound.flow_binding_result
    }

    /// The exact binding of the registered 908786 row (a RegisterFlow row,
    /// Active and roleless, as 5f38bc is live).
    fn binding_of_registered_row(fixture: &NexusFixture) -> meta_signal_flow::FlowBinding {
        let mut binding = existing_binding("908786", "w1:p3");
        binding.native_session_id = signal_flow::NativeSessionId(fixture.node().session_id.0);
        binding.herdr_terminal_id = signal_flow::HerdrTerminalId("term_65bb7f87270cb3".into());
        binding.herdr_agent_name = signal_flow::HerdrAgentName("psyche-mind-astra".into());
        binding
    }

    fn duplicate(flow_binding_result: &meta_signal_flow::FlowBindingResult) -> bool {
        matches!(
            flow_binding_result,
            meta_signal_flow::FlowBindingResult::Refused(refused)
                if refused.flow_binding_refusal_reason
                    == meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId
        )
    }

    #[test]
    fn a_matching_binding_of_a_flow_already_held_asserts_only_its_role() {
        use crate::store::{ReadsFlowRoles, ReadsFlowRows};
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);
        let before = fixture.nexus.store.flow_nodes().expect("Flow rows read");
        assert_eq!(fixture.nexus.store.role("908786").expect("role read"), None);

        let role = CallerIdentity {
            flow_id: flow_ethos::FlowId("908786".into()),
            aspect: Aspect::Mind,
            model_name: flow_ethos::ModelName("gpt-sol".into()),
        };
        for _ in 0..2 {
            let result = rebind(&fixture, binding_of_registered_row(&fixture));
            assert!(
                matches!(
                    &result,
                    meta_signal_flow::FlowBindingResult::Bound(bound) if bound.flow_id.0 == "908786"
                ),
                "{result:?}"
            );
            assert_eq!(
                fixture.nexus.store.role("908786").expect("role read"),
                Some(role.clone())
            );
            assert_eq!(
                fixture.nexus.store.flow_nodes().expect("Flow rows read"),
                before
            );
        }
    }

    #[test]
    fn a_differing_binding_of_a_flow_already_held_is_refused_and_writes_nothing() {
        use crate::store::ReadsFlowRoles;
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);
        let before = listed(&fixture);

        let mut other_terminal = binding_of_registered_row(&fixture);
        other_terminal.herdr_terminal_id = signal_flow::HerdrTerminalId("term_reused".into());
        let mut other_pane = binding_of_registered_row(&fixture);
        other_pane.herdr_pane_id = signal_flow::HerdrPaneId("w1:p4".into());
        let mut other_thread = binding_of_registered_row(&fixture);
        other_thread.native_session_id = signal_flow::NativeSessionId("another-thread".into());
        let mut other_harness = binding_of_registered_row(&fixture);
        other_harness.harness_kind = HarnessKind::Claude;
        for binding in [other_terminal, other_pane, other_thread, other_harness] {
            let result = rebind_against(&fixture, binding, binding_of_registered_row(&fixture));
            assert!(
                matches!(result, meta_signal_flow::FlowBindingResult::Refused(_)),
                "{result:?}"
            );
            assert_eq!(fixture.nexus.store.role("908786").expect("role read"), None);
            assert_eq!(listed(&fixture), before);
        }

        // A role once recorded is not replaced by a different one.
        assert!(!duplicate(&rebind(
            &fixture,
            binding_of_registered_row(&fixture)
        )));
        let mut other_role = binding_of_registered_row(&fixture);
        other_role.model_name = flow_ethos::ModelName("gpt-astra".into());
        let refused_role = rebind(&fixture, other_role);
        assert!(
            matches!(
                &refused_role,
                meta_signal_flow::FlowBindingResult::Refused(
                    meta_signal_flow::RefusedFlowBinding {
                        flow_binding_refusal_reason:
                            meta_signal_flow::FlowBindingRefusalReason::RoleMismatch,
                        ..
                    }
                )
            ),
            "{refused_role:?}"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .role("908786")
                .expect("role read")
                .map(|role| role.model_name),
            Some(flow_ethos::ModelName("gpt-sol".to_owned()))
        );
        assert_eq!(listed(&fixture), before);
    }

    /// A seat Flow never stopped can still be gone: its pane leaves Herdr and
    /// nothing can reach it. List used to answer such a flow Pending on the
    /// route it was bound to, which is what made a gone seat look launchable.
    /// It is reported `Exited` — the seat's own going — with no route, its row
    /// and history kept. It is never `Retired`: an exit retains the record and
    /// retirement is an act of authority, not an inference from a pane.
    #[test]
    fn a_flow_whose_pane_left_herdr_is_listed_exited_and_keeps_its_row() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);

        // The pane exits. Nothing else changes.
        fixture.set_agents(vec![]);

        use crate::store::ReadsFlowRows as _;
        let rows = fixture.nexus.store.flow_nodes().expect("Flow rows read");
        let rows = fixture.nexus.reconciled(rows);
        assert_eq!(rows.len(), 1, "the row and its history stay");
        assert_eq!(rows[0].flow_id, flow_ethos::FlowId("908786".into()));
        assert_eq!(rows[0].flow_lifecycle, FlowLifecycle::Exited);
        assert_ne!(
            rows[0].flow_lifecycle,
            FlowLifecycle::Retired,
            "a pane going away never retires a flow"
        );
        assert_eq!(
            rows[0].herdr_route_selection,
            HerdrRouteSelection::Unavailable
        );
        assert_eq!(rows[0].endpoint_selection, EndpointSelection::Unavailable);
        assert_eq!(rows[0].origin, node.origin, "its provenance is kept");

        // List wrote nothing: the stored row still says what Flow recorded.
        assert_eq!(
            fixture.stored_lifecycle(),
            FlowLifecycle::Active,
            "List must not write"
        );

        // A Deliver is a command, and it does record the exit it witnesses.
        assert_eq!(
            fixture.send("to a seat that is gone"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Exited);
        assert_eq!(
            fixture.send("again"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::FlowExited)
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into()))
            )),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::FlowUnavailable
            )
        );
    }

    /// A Herdr that cannot be read never retires a flow: the pane's fate is
    /// unknown, so the row is answered as it stands.
    #[test]
    fn an_unreadable_herdr_leaves_a_listed_flow_as_it_stands() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(fixture.node())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install("#!/bin/sh\necho 'herdr: server unreachable' >&2\nexit 1\n");

        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
    }

    /// A seat bound through the privileged contract, or started before Flow
    /// Nexus, has no Presented Deliver to promote it, and so stayed Pending for
    /// as long as it lived. Herdr showing its bound pane present is a witness
    /// that it is live, and List reports so — without writing: List is a
    /// query, and a query does not change what it is asked about.
    #[test]
    fn a_pending_seat_whose_pane_herdr_shows_live_is_listed_active() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Pending);
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending);

        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        // Listing it again, and again, changes nothing in the store.
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(
            fixture.stored_lifecycle(),
            FlowLifecycle::Pending,
            "List must not write"
        );
    }

    /// Flow had no way to retire a flow at all, so a seat retired elsewhere
    /// stayed listed as if it were launchable. Retire is privileged, keeps the
    /// row and its history, and takes the flow out of receiving. It is the
    /// only thing that produces Retired: no observation ever does.
    #[test]
    fn an_operation_is_answered_by_its_own_outcome() {
        use crate::generated::operation::{Failed_Data, Operation, Outcome, Record_Data};
        use crate::performing::Performs;
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);

        // A record of a flow the store does not hold is refused, not made.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Stopped(flow_ethos::FlowId(
                    "unknown".into()
                )))),
            Outcome::Failed(Failed_Data::StoreRefused)
        );
        // Confirming a launch the store never bound does not start it.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Confirm(flow_ethos::FlowId("unknown".into()))),
            Outcome::Failed(Failed_Data::Unstarted)
        );
        // A witnessed exit of a live flow is recorded, and the row says so.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Exited(flow_ethos::FlowId(
                    "908786".into()
                )))),
            Outcome::Recorded
        );
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Exited);
        // An exit is recorded once: the flow is no longer live.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Exited(flow_ethos::FlowId(
                    "908786".into()
                )))),
            Outcome::Failed(Failed_Data::StoreRefused)
        );
    }

    /// Ruling 12 of flow f1c841: a harness event reaches Flow as
    /// `Report.{ FlowId Event }`, recorded in the flow's Memory and answered
    /// Reported; a FlowId Flow does not hold is refused and never adopted.
    /// The owner reads the events back with ReadEvents.
    #[test]
    fn a_quiet_sender_observer_exits_on_peer_close_and_resume_reads_new_state() {
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        use crate::store::metaflow::SeedsMetaflow;

        let fixture = NexusFixture::new();
        let sender = signal_flow::Address {
            aspect: Aspect::Field,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Luna,
        };
        let recipient = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("message".into()),
            model: Model::Sol,
        };
        fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
            address: recipient.clone(),
            permaflow_state: flow_ethos::PermaflowState::Asleep,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });

        let nexus = Arc::new(fixture.nexus);
        let (server, mut client) = UnixStream::pair().expect("observer socket pair");
        let connection = Connection {
            peer: server,
            process: None,
        };
        let thread_nexus = Arc::clone(&nexus);
        let thread_sender = sender.clone();
        let observer = std::thread::spawn(move || {
            let mut connection = connection;
            connection.serve_sends(
                &thread_nexus,
                &crate::observe_sends::SendSelection::Sent(signal_flow::SentBy::Sender(
                    thread_sender,
                )),
                None,
            )
        });

        let Response::SendsObserved(initial) = client
            .read_response()
            .expect("initial subscription snapshot")
        else {
            panic!("subscription begins with its snapshot")
        };
        assert!(initial.visible_delivery_snapshot_vector.is_empty());
        drop(client);
        let result = observer.join().expect("quiet observer exits after close");
        assert!(result.is_err(), "a closed subscription is not kept alive");

        let signal_flow::PrivateSendResponse::SendAccepted(accepted) = nexus
            .store
            .accept_logical_delivery(
                &recipient,
                signal_flow::SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Queue,
                signal_flow::Message::Soft(signal_flow::Request::Order(
                    "resume from durable state".into(),
                )),
                None,
            )
            .expect("logical send accepted after observer disconnect")
        else {
            panic!("queued admission has an acceptance cursor")
        };
        let resumed = nexus
            .store
            .read_logical_delivery_snapshots(&sender, Some(initial.delivery_cursor.0))
            .expect("resume reads changes after the last cursor");
        assert_eq!(resumed.delivery_cursor, accepted.delivery_cursor);
        assert_eq!(resumed.delivery_snapshot_vector.len(), 1);
        assert_eq!(
            resumed.delivery_snapshot_vector[0].logical_delivery_id,
            accepted.logical_delivery_id
        );
        assert_eq!(
            resumed.delivery_snapshot_vector[0].delivery_state,
            signal_flow::DeliveryState::Queued
        );
    }

    #[test]
    fn sender_observation_replays_archived_history_in_cursor_order_and_keeps_other_senders_private()
    {
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        use crate::store::metaflow::SeedsMetaflow;

        let fixture = NexusFixture::new();
        let sender = signal_flow::Address {
            aspect: Aspect::Field,
            topic: flow_ethos::Topic("observerA".into()),
            model: Model::Luna,
        };
        let other_sender = signal_flow::Address {
            aspect: Aspect::Field,
            topic: flow_ethos::Topic("observerB".into()),
            model: Model::Luna,
        };
        let recipient = signal_flow::Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("message".into()),
            model: Model::Sol,
        };
        fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
            address: recipient.clone(),
            permaflow_state: flow_ethos::PermaflowState::Asleep,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
        let admit = |sender: &signal_flow::Address, words: &str| {
            let signal_flow::PrivateSendResponse::SendAccepted(accepted) = fixture
                .nexus
                .store
                .accept_logical_delivery(
                    &recipient,
                    signal_flow::SentBy::Sender(sender.clone()),
                    flow_ethos::Delivery::Queue,
                    signal_flow::Message::Soft(signal_flow::Request::Notice(words.into())),
                    None,
                )
                .expect("logical send admits")
            else {
                panic!("queued admission has an acceptance cursor")
            };
            accepted
        };
        let archived = admit(&sender, "archive this sender's request");
        let other = admit(&other_sender, "another sender's request");
        fixture
            .nexus
            .store
            .transition_logical_delivery(
                &archived.logical_delivery_id,
                signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                    harness_kind: flow_ethos::HarnessKind::Codex,
                    native_session_id: signal_flow::NativeSessionId("recipient-session".into()),
                    native_prompt_id: signal_flow::NativePromptId("recipient-prompt".into()),
                }),
            )
            .expect("request ends");
        assert_eq!(
            fixture
                .nexus
                .store
                .archive_request(&archived.request_id.0)
                .expect("archive reads"),
            Ok(())
        );

        let current = fixture
            .nexus
            .store
            .read_logical_delivery_snapshots(&sender, None)
            .expect("cursorless opening reads");
        assert!(
            current.delivery_snapshot_vector.is_empty(),
            "an archived request is absent from the held-only opening"
        );
        let history = fixture
            .nexus
            .store
            .read_logical_delivery_snapshots(&sender, Some(0))
            .expect("sender history reads");
        assert_eq!(history.store_identity, current.store_identity);
        assert_eq!(history.delivery_cursor, current.delivery_cursor);
        assert_eq!(
            history
                .delivery_snapshot_vector
                .iter()
                .map(|snapshot| snapshot.delivery_state.clone())
                .collect::<Vec<_>>(),
            vec![
                signal_flow::DeliveryState::Queued,
                signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                    harness_kind: flow_ethos::HarnessKind::Codex,
                    native_session_id: signal_flow::NativeSessionId("recipient-session".into()),
                    native_prompt_id: signal_flow::NativePromptId("recipient-prompt".into()),
                }),
                signal_flow::DeliveryState::Archived,
            ]
        );
        assert!(
            history
                .delivery_snapshot_vector
                .windows(2)
                .all(|pair| pair[0].delivery_cursor < pair[1].delivery_cursor)
        );
        assert!(
            history
                .delivery_snapshot_vector
                .iter()
                .all(|snapshot| snapshot.logical_delivery_id == archived.logical_delivery_id)
        );

        let other_opening = fixture
            .nexus
            .store
            .read_logical_delivery_snapshots(&other_sender, None)
            .expect("other sender opening reads");
        assert_eq!(other_opening.store_identity, history.store_identity);
        assert_eq!(other_opening.delivery_cursor, history.delivery_cursor);
        assert_eq!(other_opening.delivery_snapshot_vector.len(), 1);
        assert_eq!(
            other_opening.delivery_snapshot_vector[0].logical_delivery_id,
            other.logical_delivery_id
        );
        assert!(
            history
                .delivery_snapshot_vector
                .iter()
                .all(|snapshot| snapshot.logical_delivery_id != other.logical_delivery_id)
        );
        assert!(
            fixture
                .nexus
                .store
                .read_logical_delivery_snapshots(&sender, Some(history.delivery_cursor.0))
                .expect("caught-up sender reads")
                .delivery_snapshot_vector
                .is_empty()
        );
    }

    /// An archive with no process behind it is answered by the request's
    /// state alone: an id no request holds is unknown.
    #[test]
    fn archive_from_an_unidentified_process_is_answered_by_the_requests_state() {
        let fixture = NexusFixture::new();
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::ArchiveRequest(flow_ethos::RequestId("none".into()))),
            Response::ArchiveRejected(signal_flow::ArchiveRejection::UnknownRequest)
        );
    }

    #[test]
    fn a_report_is_recorded_for_a_held_flow_and_refused_for_an_unknown_one() {
        use signal_flow::{Event, Report_Data};
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);
        let report = |flow_id: &str, event: Event| {
            fixture.nexus.dispatch(Query::Report(Report_Data {
                flow_id: flow_ethos::FlowId(flow_id.into()),
                process_option: None,
                event,
            }))
        };

        assert_eq!(
            report("0a0a0a", Event::Started(None)),
            Response::Refused(signal_flow::Refusal::Unknown(
                signal_flow::Unknown_Data::FlowId(flow_ethos::FlowId("0a0a0a".into()))
            ))
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::ReadEvents(flow_ethos::FlowId(
                    "0a0a0a".into()
                ))),
            meta_signal_flow::Response::ReadEventsRejected(
                meta_signal_flow::ReadEventsRejected_Data::UnknownFlow
            ),
            "the refused flow was not adopted"
        );
        assert_eq!(report("908786", Event::Started(None)), Response::Reported);
        assert_eq!(
            report("908786", Event::ToolUsed("Bash".into())),
            Response::Reported
        );
        let stop = Event::Stopped(signal_flow::NativeStop {
            native_session_id_option: Some(signal_flow::NativeSessionId("other-session".into())),
            transcript_path_option: None,
            last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
            background_work: signal_flow::BackgroundWork(vec![]),
        });
        assert_eq!(report("908786", stop.clone()), Response::Reported);
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::ReadEvents(flow_ethos::FlowId(
                    "908786".into()
                ))),
            meta_signal_flow::Response::EventsRead(meta_signal_flow::EventsRead_Data {
                flow_id: flow_ethos::FlowId("908786".into()),
                harness_event_metadata_vector: [
                    Event::Started(None),
                    Event::ToolUsed("Bash".into()),
                    stop
                ]
                .iter()
                .map(|event| {
                    use crate::store::events::{
                        ProjectsHarnessEventMetadata, ProjectsRecordedHarnessEvent,
                    };
                    event.recorded().metadata()
                })
                .collect(),
            })
        );
    }

    #[test]
    fn retire_keeps_the_row_and_takes_the_flow_out_of_receiving() {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.current_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        let node = fixture.node();
        fixture.register_with(FlowLifecycle::Active);

        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Retire(flow_ethos::FlowId(
                    "unknown".into()
                ))),
            meta_signal_flow::Response::RetireRejected(
                meta_signal_flow::RetireRejection::UnknownFlow
            )
        );

        let meta_signal_flow::Response::FlowRetired(retired) =
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Retire(flow_ethos::FlowId(
                    "908786".into(),
                )))
        else {
            panic!("a live flow is retired")
        };
        assert_eq!(retired.flow_lifecycle, FlowLifecycle::Retired);
        assert_eq!(retired.flow_id, flow_ethos::FlowId("908786".into()));
        assert_eq!(retired.session_id, node.session_id, "its history is kept");
        assert_eq!(retired.origin, node.origin);

        // Retiring it again changes nothing and says so.
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Retire(flow_ethos::FlowId(
                    "908786".into()
                ))),
            meta_signal_flow::Response::RetireRejected(
                meta_signal_flow::RetireRejection::AlreadyGone
            )
        );

        // Its pane is closed, and though Herdr still shows it live, it
        // stays Retired:
        // retirement is a decision about the flow, not a reading of Herdr,
        // and List does not talk it back into being live.
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Retired);
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Retired);
        assert_eq!(
            fixture.send("to a retired flow"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::FlowRetired)
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into()))
            )),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::FlowUnavailable
            )
        );
    }

    #[test]
    fn running_nexus_marks_stale_or_noninteractive_snapshots_unavailable() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));

        for changed_field in ["terminal", "pane", "harness", "interactive"] {
            let mut agent = fixture.current_agent();
            match changed_field {
                "terminal" => agent["terminal_id"] = "term_replaced".into(),
                "pane" => agent["pane_id"] = "w1:p9".into(),
                "harness" => agent["agent"] = "claude".into(),
                "interactive" => agent["interactive_ready"] = false.into(),
                _ => unreachable!("closed fixture variants"),
            }
            fixture.set_agents(vec![agent]);
            let Response::RecipientResolved(resolved) =
                fixture.nexus.dispatch(Query::ResolveRecipient(
                    signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into())),
                ))
            else {
                panic!("registered recipient resolves")
            };
            assert_eq!(
                resolved.herdr_route_selection,
                HerdrRouteSelection::Unavailable,
                "stale {changed_field} must not remain routable"
            );
            assert!(matches!(
                resolved.endpoint_selection,
                EndpointSelection::Available(signal_flow::Available_Data {
                    route_readiness: signal_flow::RouteReadiness::Parked,
                    ..
                })
            ));
        }
    }

    /// 88475f renamed its Herdr agent after launch (claude-86b6e54c… to
    /// psyche-opus-88475f). The route is the session, pane and terminal
    /// ids; the new name is read back, and the pane is still the one to
    /// close.
    #[test]
    fn a_renamed_agent_keeps_its_route_and_reports_its_current_name() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        let mut renamed = fixture.current_agent();
        renamed["name"] = "psyche-opus-88475f".into();
        fixture.set_agents(vec![renamed]);
        let Response::RecipientResolved(resolved) =
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into())),
            ))
        else {
            panic!("registered recipient resolves")
        };
        let HerdrRouteSelection::Available(route) = &resolved.herdr_route_selection else {
            panic!("a renamed agent keeps its route: {resolved:?}")
        };
        let HerdrRouteSelection::Available(stored) = &node.herdr_route_selection else {
            panic!("fixture node carries a route")
        };
        assert_eq!(
            route.herdr_agent_name,
            signal_flow::HerdrAgentName("psyche-opus-88475f".into())
        );
        assert_eq!(route.herdr_session_name, stored.herdr_session_name);
        assert_eq!(route.herdr_pane_id, stored.herdr_pane_id);
        assert_eq!(route.herdr_terminal_id, stored.herdr_terminal_id);
        assert!(crate::herdr::ReadsHerdrRoster::route_is_available(
            &fixture.nexus.herdr,
            &node
        ));
        assert_eq!(
            fixture.nexus.herdr.pane_presence(&node),
            crate::herdr::PanePresence::Present
        );
    }

    #[test]
    fn duplicate_registration_is_idempotent_and_conflict_has_no_partial_mutation() {
        let fixture = NexusFixture::new();
        let mut second_agent = fixture.current_agent();
        second_agent["terminal_id"] = "term_conflicting".into();
        fixture.set_agents(vec![fixture.current_agent(), second_agent]);
        let node = fixture.node();
        let registration = meta_signal_flow::Query::RegisterFlow(node.clone());
        assert!(matches!(
            fixture.nexus.dispatch_meta(registration.clone()),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        assert!(matches!(
            fixture.nexus.dispatch_meta(registration),
            meta_signal_flow::Response::FlowRegistered(_)
        ));

        let mut conflict = node.clone();
        let HerdrRouteSelection::Available(route) = &mut conflict.herdr_route_selection else {
            panic!("fixture route")
        };
        route.herdr_terminal_id = signal_flow::HerdrTerminalId("term_conflicting".into());
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(conflict)),
            meta_signal_flow::Response::FlowRegistrationRejected(
                meta_signal_flow::FlowRegistrationRejection::ConflictingBinding
            )
        );

        fixture.set_agents(vec![fixture.current_agent()]);
        assert_eq!(
            fixture.nexus.dispatch(Query::ResolveRecipient(
                signal_flow::RecipientResolutionRequest(flow_ethos::FlowId("908786".into()))
            )),
            Response::RecipientResolved(node)
        );
        assert!(fixture.directory.path().join("flow.sema").exists());
    }

    /// A launch driven phase by phase through the store, as the Start path
    /// drives it, so a fixture can stop at any phase.
    struct StagedLaunch {
        profile: LaunchProfile,
        origin: Origin,
        composed: signal_flow::ComposedLaunch,
        pane: HerdrPaneBinding,
        binding: NativeLaunchBinding,
        intent: Option<PromptDeliveryIntent>,
        transcript_root: PathBuf,
    }

    const STAGED_SESSION: &str = "01a0b22c-e24f-7452-9940-64490878680f";

    impl NexusFixture {
        /// Declares the model a profile names as the one native model
        /// configured for its Address's Model, as a running Nexus's
        /// configuration would before such a Start.
        fn configure_model_of(&self, profile: &LaunchProfile) {
            use crate::store::ManagesMetaflowState as _;
            self.nexus
                .store
                .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                    meta_signal_flow::ConfigureModel {
                        model: profile.address.model.clone(),
                        native: meta_signal_flow::Native(profile.model_name.0.clone()),
                    },
                ]))
                .expect("model configured");
        }

        fn staged_launch(
            &self,
            launch_request_id: &str,
            predecessor: Option<&str>,
        ) -> StagedLaunch {
            // A staged launch opens in the session the Nexus declares.
            self.declare_launch_session("fixture-session");
            let profile = LaunchProfile {
                launch_request_id: signal_flow::LaunchRequestId(launch_request_id.into()),
                system_modules: signal_flow::SystemModules(vec![
                    signal_flow::ModuleIdentity("spirit-core".into()),
                    signal_flow::ModuleIdentity("intent-flow".into()),
                ]),
                user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                    "operation-main-flow".into(),
                )]),
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                // Each staged launch is the seat of its own Address, its
                // topic the request id with its hyphens taken out.
                address: signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: flow_ethos::Topic(launch_request_id.replace('-', "")),
                    model: signal_flow::Model::Sol,
                },
                model_name: flow_ethos::ModelName("fixture-model".into()),
                effort: signal_flow::Effort("medium".into()),
                flow_id_option: predecessor.map(str::to_owned).map(flow_ethos::FlowId),
                remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(
                    0,
                )),
                herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                leave_outs_option: None,
                instruction_prompt: signal_flow::InstructionPrompt("fixture instruction".into()),
            };
            let composed = self
                .nexus
                .composer
                .current()
                .compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                        harness_kind: HarnessKind::Codex,
                        launch_profile: profile.clone(),
                    },
                    Vec::<signal_flow::LeaveOut>::new(),
                )
                .expect("launch composes");
            let pane = HerdrPaneBinding {
                launch_request_id: signal_flow::LaunchRequestId(launch_request_id.into()),
                herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                herdr_agent_name: signal_flow::HerdrAgentName("fixture-agent".into()),
                herdr_workspace_id: signal_flow::HerdrWorkspaceId("fixture-workspace".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("w1:p1".into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("fixture-terminal".into()),
            };
            let transcript_root = self.directory.path().join("native-transcripts/codex");
            fs::create_dir_all(&transcript_root).expect("transcript root");
            self.configure_model_of(&profile);
            StagedLaunch {
                binding: NativeLaunchBinding {
                    launch_request_id: signal_flow::LaunchRequestId(launch_request_id.into()),
                    flow_id: flow_ethos::FlowId("908786".into()),
                    native_session_id: signal_flow::NativeSessionId(STAGED_SESSION.into()),
                    harness_kind: HarnessKind::Codex,
                    herdr_pane_binding: pane.clone(),
                },
                profile,
                origin: Origin::Unbound,
                composed,
                pane,
                intent: None,
                transcript_root,
            }
        }

        /// A Herdr stand-in that knows the successor's agent, shows the
        /// predecessor's pane in every snapshot, and logs every call.
        fn herdr_for_replacement(&self, close_status: i32) -> PathBuf {
            self.herdr_for_replacement_showing(close_status, vec![self.current_agent()])
        }

        /// The successor's own pane as a snapshot shows it: the staged
        /// launch binds w1:p1 on the fixture terminal. Every replacement
        /// stand-in shows it, because the successor's seat is really there;
        /// only the predecessor's presence varies between these tests.
        fn successor_agent(&self) -> serde_json::Value {
            serde_json::json!({
                "agent":"codex",
                "agent_status":"working",
                "interactive_ready":true,
                "name":"fixture-agent",
                "pane_id":"w1:p1",
                "revision":1,
                "terminal_id":"fixture-terminal",
                "workspace_id":"fixture-workspace",
                "agent_session":{
                    "source":"herdr:codex","agent":"codex","kind":"id",
                    "value":STAGED_SESSION
                }
            })
        }

        fn herdr_for_replacement_showing(
            &self,
            close_status: i32,
            agents: Vec<serde_json::Value>,
        ) -> PathBuf {
            let mut agents = agents;
            agents.push(self.successor_agent());
            let log = self.directory.path().join("herdr-calls.log");
            let agent = serde_json::json!({"result":{"agent":{
                "name":"fixture-agent",
                "agent":"codex",
                "workspace_id":"fixture-workspace",
                "pane_id":"w1:p1",
                "terminal_id":"fixture-terminal",
                "agent_session":{
                    "source":"herdr:codex",
                    "agent":"codex",
                    "kind":"id",
                    "value":STAGED_SESSION
                }
            }}});
            let snapshot = serde_json::json!({
                "id":"cli:api:snapshot",
                "result":{"snapshot":{"agents":agents.clone(),"protocol":20,"version":"0.8.2"},
                "type":"session_snapshot"}
            });
            let body = format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n{}\ncase \"$*\" in\n  \"--session fixture-session agent get w1:p1\") printf '%s\\n' '{}' ;;\n  *\" api snapshot\") printf '%s\\n' '{}' ;;\n  *\" pane close \"*) exit {} ;;\n  *\" agent read \"*) printf '%s\\n' '› ' ;;\n  *) exit 64 ;;\nesac\n",
                log.display(),
                self.session_listing(),
                agent,
                snapshot,
                close_status
            );
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
            log
        }

        /// The predecessor: the fixture's Herdr pane w1:p3, Active.
        fn register_predecessor(&self, flow_id: &str) {
            fs::write(
                self.directory
                    .path()
                    .join(format!("flows/.{flow_id}.flow-id")),
                format!(
                    "version=1\nharness=codex\nidentity=02b0c33df35f8563aa5175501989791a\nalias={flow_id}\n"
                ),
            )
            .expect("predecessor flow claim");
            let mut node = self.node();
            node.flow_id = flow_ethos::FlowId(flow_id.into());
            assert!(matches!(
                self.nexus.store.register_flow(node),
                Ok(crate::store::FlowRegistration::Registered(_))
            ));
            assert!(self.nexus.store.record_active(flow_id).unwrap());
        }

        fn routable(&self, flow_id: &str) -> bool {
            matches!(
                self.nexus.dispatch(Query::ResolveRecipient(
                    signal_flow::RecipientResolutionRequest(flow_ethos::FlowId(flow_id.into()))
                )),
                Response::RecipientResolved(_)
            )
        }

        fn lifecycle(&self, flow_id: &str) -> FlowLifecycle {
            use crate::store::ReadsFlowRows as _;
            self.nexus
                .store
                .flow_node(flow_id)
                .expect("Flow row read")
                .expect("flow row")
                .flow_lifecycle
        }
    }

    #[test]
    fn metaflows_resolve_their_own_workspace_or_the_nexus_source_root() {
        use crate::launching::ResolvesLaunchRoot as _;
        use crate::store::{ConfiguresFlowStore as _, ManagesMetaflowState as _};
        let fixture = NexusFixture::new();
        let first = fixture.staged_launch("workspace-first", None).profile;
        let second = fixture.staged_launch("workspace-default", None).profile;
        let own_root = fixture.directory.path().join("first-workspace");
        fs::create_dir(&own_root).unwrap();
        let declaration =
            |profile: &LaunchProfile, root_option| meta_signal_flow::ConfigurePermaflow {
                address: profile.address.clone(),
                root_option,
                system_modules: profile.system_modules.clone(),
                user_modules: profile.user_modules.clone(),
                flow_kind: profile.flow_kind.clone(),
                tool_set: profile.tool_set.clone(),
                continuity: meta_signal_flow::Continuity::OnDemand,
                purpose: meta_signal_flow::Purpose(String::new()),
                records: meta_signal_flow::Records(vec![]),
            };
        fixture
            .nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
                declaration(
                    &first,
                    Some(meta_signal_flow::Root(
                        own_root.to_string_lossy().into_owned(),
                    )),
                ),
                declaration(&second, None),
            ]))
            .unwrap();
        assert_eq!(
            fixture.nexus.launch_root(&first.address).unwrap().0,
            own_root.to_string_lossy()
        );
        assert_eq!(
            fixture.nexus.launch_root(&second.address).unwrap(),
            meta_signal_flow::Root(
                fixture
                    .nexus
                    .store
                    .runtime_configuration()
                    .unwrap()
                    .source_root
            )
        );
    }

    #[test]
    fn invalid_metaflow_workspace_is_refused_before_launch_reservation_or_native_artifacts() {
        use crate::store::{ManagesMetaflowState as _, ReadsLaunchAttempt as _};
        let fixture = NexusFixture::new();
        let staged = fixture.staged_launch("invalid-workspace", None);
        let invalid = fixture.directory.path().join("absent-workspace");
        fixture
            .nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
                meta_signal_flow::ConfigurePermaflow {
                    address: staged.profile.address.clone(),
                    root_option: Some(meta_signal_flow::Root(
                        invalid.to_string_lossy().into_owned(),
                    )),
                    system_modules: staged.profile.system_modules.clone(),
                    user_modules: staged.profile.user_modules.clone(),
                    flow_kind: staged.profile.flow_kind.clone(),
                    tool_set: staged.profile.tool_set.clone(),
                    continuity: meta_signal_flow::Continuity::OnDemand,
                    purpose: meta_signal_flow::Purpose(String::new()),
                    records: meta_signal_flow::Records(vec![]),
                },
            ]))
            .expect("invalid workspace declaration is stored as configuration");

        assert_eq!(
            crate::launching::LaunchesFlows::start(
                &fixture.nexus,
                staged.profile.clone(),
                staged.origin,
            ),
            Response::StartRejected(StartRejection::CompositionRefused(
                signal_flow::CompositionRefusal::Unconfigured,
            ))
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_attempt(&staged.profile.launch_request_id.0)
                .unwrap(),
            None
        );
        assert!(!invalid.exists());
        assert!(
            !fs::read_to_string(fixture.directory.path().join("calls"))
                .unwrap_or_default()
                .contains("workspace create")
        );
    }

    impl StagedLaunch {
        /// The profile a Start or Replace carries; an ordinary dispatch
        /// with no calling flow derives the Unbound origin.
        fn request(&self) -> LaunchProfile {
            self.profile.clone()
        }

        fn reserve(&self, nexus: &RunningNexus) {
            nexus
                .store
                .reserve_launch_attempt(&self.composed, self.origin.clone())
                .expect("reservation persists");
        }

        fn record_native_intent(&self, nexus: &RunningNexus) {
            assert!(
                nexus
                    .store
                    .record_native_launch_intent(NativeLaunchIntent {
                        launch_request_id: self.profile.launch_request_id.clone(),
                        prompt_sha256: self.composed.first_prompt_payload.prompt_sha256.clone(),
                        system_prompt_sha256: self
                            .composed
                            .system_prompt
                            .system_prompt_sha256
                            .clone(),
                        harness_kind: HarnessKind::Codex,
                        model_name: self.profile.model_name.clone(),
                        effort: self.profile.effort.clone(),
                        prompt_sha256_vector: self
                            .composed
                            .first_prompt_payload
                            .prompt_part_vector
                            .iter()
                            .map(|part| part.prompt_sha256.clone())
                            .collect(),
                    })
                    .unwrap()
            );
        }

        fn bind(&self, nexus: &RunningNexus) {
            assert!(
                nexus
                    .store
                    .record_native_launch_binding(self.binding.clone())
                    .unwrap()
            );
        }

        /// Registers the bound flow in the role its profile names, as
        /// Start does, and journals the acknowledgement, which takes the
        /// launch's Address.
        fn register_and_acknowledge(&self, nexus: &RunningNexus) {
            nexus
                .store
                .register_flow_in_role(
                    FlowNode {
                        flow_id: self.binding.flow_id.clone(),
                        session_id: signal_flow::SessionId(
                            self.binding.native_session_id.0.clone(),
                        ),
                        harness_kind: HarnessKind::Codex,
                        endpoint_selection: EndpointSelection::Unavailable,
                        herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                            herdr_session_name: self.pane.herdr_session_name.clone(),
                            herdr_agent_name: self.pane.herdr_agent_name.clone(),
                            herdr_pane_id: self.pane.herdr_pane_id.clone(),
                            herdr_terminal_id: self.pane.herdr_terminal_id.clone(),
                        }),
                        origin: self.origin.clone(),
                        flow_lifecycle: FlowLifecycle::Pending,
                    },
                    signal_flow::CallerIdentity {
                        flow_id: self.binding.flow_id.clone(),
                        aspect: self.profile.address.aspect.clone(),
                        model_name: self.profile.model_name.clone(),
                    },
                )
                .unwrap();
            assert!(
                nexus
                    .store
                    .record_registration_acknowledgement(RegistrationAcknowledgement {
                        launch_request_id: self.profile.launch_request_id.clone(),
                        flow_id: self.binding.flow_id.clone(),
                        native_session_id: self.binding.native_session_id.clone(),
                        herdr_pane_binding: self.pane.clone(),
                    })
                    .unwrap()
            );
        }

        fn record_prompt_intent(&mut self, nexus: &RunningNexus) {
            let root_metadata = fs::metadata(&self.transcript_root).unwrap();
            let intent = PromptDeliveryIntent {
                launch_request_id: self.profile.launch_request_id.clone(),
                prompt_sha256: self.composed.first_prompt_payload.prompt_sha256.clone(),
                flow_id: self.binding.flow_id.clone(),
                native_session_id: self.binding.native_session_id.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: self.profile.model_name.clone(),
                effort: self.profile.effort.clone(),
                prompt_sha256_vector: self
                    .composed
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
                parts_submitted: signal_flow::PartsSubmitted(1),
                stops_reported: signal_flow::StopsReported(0),
                leave_outs: signal_flow::LeaveOuts(Vec::new()),
                herdr_pane_binding: self.pane.clone(),
                native_transcript_boundary: NativeTranscriptBoundary::Absent(
                    NativeTranscriptAbsence {
                        native_session_id: self.binding.native_session_id.clone(),
                        harness_kind: HarnessKind::Codex,
                        transcript_root_device: signal_flow::TranscriptRootDevice(
                            root_metadata.dev().to_string(),
                        ),
                        transcript_root_inode: signal_flow::TranscriptRootInode(
                            root_metadata.ino().to_string(),
                        ),
                    },
                ),
            };
            assert!(
                nexus
                    .store
                    .record_prompt_delivery_intent(intent.clone())
                    .unwrap()
            );
            self.intent = Some(intent);
        }

        fn record_ambiguity(&self, nexus: &RunningNexus) {
            assert!(
                nexus
                    .store
                    .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(
                        self.intent.clone().expect("prompt intent recorded"),
                    ))
                    .unwrap()
            );
        }

        fn stage_to_ambiguity(&mut self, nexus: &RunningNexus) {
            self.reserve(nexus);
            self.record_native_intent(nexus);
            self.bind(nexus);
            self.register_and_acknowledge(nexus);
            self.record_prompt_intent(nexus);
            self.record_ambiguity(nexus);
        }

        /// The harness writes the authenticated first turn: the receipt.
        fn write_receipt(&self) {
            let mut rows = String::new();
            for row in [
                serde_json::json!({"type":"session_meta","payload":{"id":STAGED_SESSION}}),
                serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-staged"}}),
                serde_json::json!({"type":"event_msg","payload":{"thread_id":STAGED_SESSION,"turn_id":"turn-staged","item":{"type":"UserMessage","content":[{"type":"text","text":self.composed.first_prompt_payload.first_prompt_body.0}]}}}),
                // The flow's first answer: the receipt.
                serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Beginning."}]}}),
            ] {
                rows.push_str(&format!("{row}\n"));
            }
            fs::write(
                self.transcript_root
                    .join(format!("rollout-{STAGED_SESSION}.jsonl")),
                rows,
            )
            .expect("transcript written");
        }
    }

    #[test]
    fn replace_refuses_an_absent_unknown_or_stopped_predecessor() {
        let fixture = NexusFixture::new();
        let absent = fixture.staged_launch("absent-request", None);
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(absent.request())),
            Response::ReplaceRejected(signal_flow::ReplaceRejection::PredecessorAbsent)
        );
        let unknown = fixture.staged_launch("unknown-request", Some("nobody"));
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(unknown.request())),
            Response::ReplaceRejected(signal_flow::ReplaceRejection::UnknownPredecessor)
        );
        fixture.register_predecessor("fac697");
        assert!(fixture.nexus.store.record_stopped("fac697").unwrap());
        let stopped = fixture.staged_launch("stopped-request", Some("fac697"));
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(stopped.request())),
            Response::ReplaceRejected(signal_flow::ReplaceRejection::PredecessorStopped)
        );
        for request in ["absent-request", "unknown-request", "stopped-request"] {
            assert_eq!(
                fixture
                    .nexus
                    .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                        request.into()
                    ))),
                Response::LaunchStatusRejected(
                    signal_flow::LaunchStatusRejection::UnknownLaunchRequest
                )
            );
        }
        fixture
            .nexus
            .store
            .record_launch_outcome(
                "outcome-without-attempt",
                LaunchOutcome::StartRejected(StartRejection::CompositionRefused(
                    signal_flow::CompositionRefusal::InvalidRemembering,
                )),
            )
            .unwrap();
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                    "outcome-without-attempt".into()
                ))),
            Response::LaunchStatusRejected(signal_flow::LaunchStatusRejection::InconsistentRecords)
        );
        assert!(
            !fixture.snapshot_program.exists(),
            "no launch was attempted"
        );
    }

    /// 42265e: a launch registered at its Address but never confirmed was
    /// Stopped; the Address came back, yet the unsettled attempt kept
    /// refusing every fresh Start there. A launch whose flow has ended is
    /// settled by that end, and the Address takes a fresh launch.
    #[test]
    fn an_unconfirmed_launch_stopped_leaves_its_address_to_a_fresh_start() {
        use crate::store::ReadsLaunchAttempt as _;
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        use crate::store::metaflow::TakesLaunchAddress as _;
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        let mut fresh = fixture.staged_launch("fresh-request", None);
        fresh.profile.address = address.clone();
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_address_refusal(&fresh.profile)
                .unwrap(),
            Some(StartRejection::AddressAwake),
            "the unconfirmed launch holds the Address while its flow lives"
        );

        assert!(matches!(
            fixture
                .nexus
                .dispatch(Query::Stop(flow_ethos::FlowId("908786".into()))),
            Response::Stopped(_)
        ));
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        assert_eq!(fixture.nexus.store.metaflow(&address).unwrap(), None);
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert_eq!(
            fixture.nexus.store.launch_outcome("seat-request").unwrap(),
            None,
            "the stopped launch has no outcome, as the live store holds it"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_address_refusal(&fresh.profile)
                .unwrap(),
            None,
            "a fresh Start at the Address is not refused"
        );
        assert!(
            fixture
                .nexus
                .store
                .ambiguous_launch_attempts()
                .unwrap()
                .is_empty(),
            "a stopped launch is no longer watched for its receipt"
        );
    }

    /// A store an older Nexus left with a hold and an Awake metaflow behind
    /// an ended flow: the next Start at that Address gives the hold back
    /// before it reads the Address, with no hand edit.
    #[test]
    fn a_stale_hold_behind_an_ended_flow_is_given_back_at_start() {
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::address_hold::LeavesStaleHold as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        assert!(fixture.nexus.store.record_stopped("908786").unwrap());
        fixture.nexus.store.leave_stale_hold("908786", &address);
        assert!(
            fixture
                .nexus
                .store
                .address_hold("908786")
                .unwrap()
                .is_some()
        );

        let mut fresh = fixture.staged_launch("fresh-request", None);
        fresh.profile.address = address.clone();
        // The Start gives the stale hold back before anything launches; the
        // fixture's Herdr refuses what follows.
        let response = fixture.nexus.dispatch(Query::Start(fresh.request()));
        assert!(
            !matches!(
                response,
                Response::StartRejected(StartRejection::AddressAwake)
            ),
            "the Address is free: {response:?}"
        );
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert!(
            fixture
                .nexus
                .store
                .metaflow(&address)
                .unwrap()
                .is_none_or(|metaflow| metaflow.permaflow_state
                    != flow_ethos::PermaflowState::Awake(flow_ethos::FlowId("908786".into()))),
            "the ended flow no longer names the Address"
        );
    }

    /// A launch at an Address that is in flight before its native binding,
    /// while no metaflow exists yet, still refuses another Start there.
    #[test]
    fn a_launch_in_flight_before_binding_refuses_another_start_at_its_address() {
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        let calls = fixture.herdr_for_replacement(0);
        let seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.reserve(&fixture.nexus);
        seat.record_native_intent(&fixture.nexus);
        assert_eq!(fixture.nexus.store.metaflow(&address).unwrap(), None);
        let before = fs::read_to_string(&calls).unwrap_or_default();
        let mut second = fixture.staged_launch("second-request", None);
        second.profile.address = address;
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(second.request())),
            Response::StartRejected(StartRejection::AddressAwake)
        );
        assert_eq!(
            fs::read_to_string(&calls).unwrap_or_default(),
            before,
            "the refused Start touches no pane"
        );
    }

    /// A launch names the Address of its seat: the write acknowledging the
    /// seat's witnessed binding records that metaflow Awake as the launched
    /// flow and holds it, confirmation keeps it in the write that makes the
    /// flow Active, and a Start at an Address a live seat holds is refused
    /// before anything launches.
    #[test]
    fn a_confirmed_launch_takes_its_address_and_a_held_address_refuses_the_next_start() {
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        let calls = fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        let held = fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .expect("the acknowledged binding took the Address");
        assert_eq!(
            held.state,
            flow_ethos::PermaflowState::Awake(flow_ethos::FlowId("908786".into()))
        );
        assert!(
            fixture
                .nexus
                .store
                .address_hold("908786")
                .unwrap()
                .is_some(),
            "until confirmation the launch only holds it"
        );
        let mut second = fixture.staged_launch("second-request", None);
        second.profile.address = address.clone();
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(second.request())),
            Response::StartRejected(StartRejection::AddressAwake),
            "another unsettled launch names the Address"
        );

        seat.write_receipt();
        let Response::Started(started) = fixture.nexus.dispatch(Query::Start(seat.request()))
        else {
            panic!("the observed launch starts")
        };
        let metaflow = fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .expect("the launch recorded its metaflow");
        assert_eq!(
            metaflow.permaflow_state,
            flow_ethos::PermaflowState::Awake(started.flow_id.clone())
        );
        assert!(metaflow.past.0.is_empty());
        assert_eq!(
            fixture
                .nexus
                .store
                .address_hold(&started.flow_id.0)
                .unwrap(),
            None,
            "confirmation keeps the Address and drops the hold"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .metaflow_awake_as(&started.flow_id.0)
                .unwrap(),
            Some(metaflow)
        );

        let launched = fs::read_to_string(&calls).unwrap_or_default();
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(second.request())),
            Response::StartRejected(StartRejection::AddressAwake),
            "the Address is Awake as a live flow"
        );
        assert_eq!(
            fs::read_to_string(&calls).unwrap_or_default(),
            launched,
            "a refused Start touches no pane"
        );
    }

    #[test]
    fn launch_status_answers_pending_and_every_outcome_once() {
        let fixture = NexusFixture::new();
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                    "never-sent".into()
                ))),
            Response::LaunchStatusRejected(
                signal_flow::LaunchStatusRejection::UnknownLaunchRequest
            )
        );

        // Started.
        fixture.herdr_for_replacement(0);
        let mut started = fixture.staged_launch("started-request", None);
        started.stage_to_ambiguity(&fixture.nexus);
        let pending = inspected(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                    "started-request".into(),
                ))),
            "started-request",
        );
        assert_eq!(
            pending.launch_attempt.launch_attempt_phase,
            LaunchAttemptPhase::PromptAmbiguous
        );
        assert!(pending.launch_status_outcome_option.is_none());
        started.write_receipt();
        let Response::Started(outcome) = fixture.nexus.dispatch(Query::Start(started.request()))
        else {
            panic!("the observed launch starts")
        };
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                    "started-request".into(),
                ))),
            "started-request",
            LaunchStatusOutcome::Started(outcome),
            Some("908786"),
        );

        // StartRejected after reservation: Herdr cannot open the pane. The
        // Claude launch's FlowId is claimed at Reserve, before the pane.
        fixture.claims_flow_ids();
        fs::remove_file(&fixture.snapshot_program).expect("Herdr withdrawn");
        let mut rejected = fixture.staged_launch("rejected-request", None);
        rejected.profile.model_name = flow_ethos::ModelName("claude-fixture".into());
        fixture.configure_model_of(&rejected.profile);
        let rejection = fixture.nexus.dispatch(Query::Start(rejected.request()));
        assert_eq!(rejection, Response::StartRejected(pane_unopened()));
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                    "rejected-request".into(),
                ))),
            "rejected-request",
            LaunchStatusOutcome::StartRejected(pane_unopened()),
            None,
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(rejected.request())),
            rejection,
            "a settled rejection is not launched again"
        );
        // Nothing composed was written to disk for it.
        assert!(!fixture.directory.path().join("launch-bundles").exists());

        // ReplaceRejected: the successor's launch is refused; the
        // predecessor keeps receiving.
        fixture.register_predecessor("fac697");
        let mut refused = fixture.staged_launch("refused-request", Some("fac697"));
        refused.profile.model_name = flow_ethos::ModelName("claude-fixture".into());
        fixture.configure_model_of(&refused.profile);
        let rejection = fixture.nexus.dispatch(Query::Replace(refused.request()));
        assert_eq!(
            rejection,
            Response::ReplaceRejected(
                signal_flow::ReplaceRejection::LaunchRefused(pane_unopened())
            )
        );
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId(
                    "refused-request".into(),
                ))),
            "refused-request",
            LaunchStatusOutcome::ReplaceRejected(signal_flow::ReplaceRejection::LaunchRefused(
                pane_unopened(),
            )),
            None,
        );
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Active);
        // Replaced is answered by the replacement fixture above.
    }

    fn bind_existing_caller(fixture: &NexusFixture, flow_id: &str, pane: &str) {
        let socket_path = fixture
            .directory
            .path()
            .join(format!("herdr-{flow_id}.sock"));
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let binding = existing_binding(flow_id, pane);
        let response = with_bound_world(fixture, &[&binding.clone()], || {
            fixture.nexus.dispatch_meta_from(
                meta_signal_flow::Query::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                    flow_container: flow_container(&socket_path),
                    address: signal_flow::Address {
                        aspect: binding.aspect.clone(),
                        topic: flow_ethos::Topic("core".into()),
                        model: signal_flow::Model::Sol,
                    },
                    flow_binding: binding,
                }),
                Some(adoption_resolver()),
            )
        });
        assert!(
            matches!(
                &response,
                meta_signal_flow::Response::BoundExisting(bound)
                    if matches!(bound.flow_binding_result, meta_signal_flow::FlowBindingResult::Bound(_))
            ),
            "{response:?}"
        );
    }

    fn codex_agent_in(pane: &str, terminal: &str) -> serde_json::Value {
        serde_json::json!({
            "agent":"codex", "agent_status":"working", "name":format!("agent-{pane}"),
            "pane_id":pane, "terminal_id":terminal
        })
    }

    fn pane(pane: &str) -> Option<CallerPane> {
        Some(CallerPane {
            herdr_session_name: "messaging-build".into(),
            herdr_pane_id: pane.into(),
        })
    }

    fn mind_live() -> CallerIdentity {
        CallerIdentity {
            flow_id: flow_ethos::FlowId("a001".into()),
            aspect: Aspect::Mind,
            model_name: flow_ethos::ModelName("gpt-sol".into()),
        }
    }

    /// A freshly spawned process is not yet marked. glibc's `posix_spawn`
    /// wakes the vfork parent from inside the child's `execve`, at
    /// `mm_release`, which the kernel reaches before it has laid the new
    /// environment into the new address space: for a few dozen microseconds
    /// `/proc/<pid>/environ` reads back empty (the `comm` is already the new
    /// program's). A reader cannot tell that from a scrubbed environment, so
    /// it would take the peer for the owner. Every fixture peer is therefore
    /// spawned so that the spawn returns once the kernel reports its
    /// `execve` complete (`exec_witness`), and its marks are read then,
    /// once. The Nexus needs no such wait: its peers are processes that have
    /// already connected to it.
    trait SpawnsMarkedPeer {
        /// Spawns the command marked with `pane`, returning once its new
        /// image runs and its marks are readable.
        fn spawn_marked(&mut self, pane: &str) -> std::process::Child;
        /// Spawns the command marked with `pane`, as `spawn_marked`, and
        /// returns once the first process it starts has exec'd into an
        /// image whose `comm` is `comm`, with that process's id.
        fn spawn_marked_with_child(&mut self, pane: &str, comm: &str)
        -> (std::process::Child, u32);
    }

    impl SpawnsMarkedPeer for std::process::Command {
        fn spawn_marked(&mut self, pane: &str) -> std::process::Child {
            use crate::exec_witness::SpawnsWitnessedExec;
            let child = self
                .env_remove("HERDR_SESSION")
                .env("HERDR_PANE_ID", pane)
                .spawn_after_exec()
                .expect("marked fixture process");
            assert_eq!(
                CallerProcess {
                    process_id: child.id(),
                }
                .marked_pane_id()
                .as_deref(),
                Some(pane),
                "the fixture peer's Herdr marks are readable once its exec completed"
            );
            child
        }

        fn spawn_marked_with_child(
            &mut self,
            pane: &str,
            comm: &str,
        ) -> (std::process::Child, u32) {
            use crate::exec_witness::SpawnsWitnessedExec;
            let (child, started) = self
                .env_remove("HERDR_SESSION")
                .env("HERDR_PANE_ID", pane)
                .spawn_until_child_execs(comm)
                .expect("marked fixture process and its child");
            assert_eq!(
                CallerProcess {
                    process_id: child.id(),
                }
                .marked_pane_id()
                .as_deref(),
                Some(pane),
                "the fixture peer's Herdr marks are readable once its exec completed"
            );
            (child, started)
        }
    }

    /// A process that sleeps in a pane of Herdr's default-session shape,
    /// killed on drop: it carries `HERDR_PANE_ID` alone, and its Herdr
    /// server ancestor is the test process, listening on the fixture
    /// session's socket.
    struct MarkedProcess(std::process::Child);

    impl MarkedProcess {
        /// The program is spawned directly, never through a shell that
        /// `exec`s it: the spawn is witnessed at the one `execve` it makes,
        /// and a second `execve` in that same process reopens the window
        /// — `/proc/<pid>/environ` reads back empty again, which a reader
        /// cannot tell from a scrubbed environment. A shell is spawned only
        /// where the shell itself is the marked process and stays (its
        /// command line ends in another word, so the shell does not turn
        /// its last command into an `exec` of its own accord).
        fn spawn(program: &str, arguments: &[&str], pane: &str) -> Self {
            Self(
                std::process::Command::new(program)
                    .args(arguments)
                    .spawn_marked(pane),
            )
        }
    }

    impl Drop for MarkedProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn a_caller_bound_in_the_registry_resolves_with_its_role() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);

        assert_eq!(
            fixture.nexus.resolve_caller(pane("pane-1"), None),
            Response::CallerResolved(mind_live())
        );
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(pane("pane-1"), Some(flow_ethos::FlowId("a001".into()))),
            Response::CallerResolved(mind_live())
        );
    }

    #[test]
    fn a_process_in_a_pane_is_found_by_its_own_marks_or_its_ancestors() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        // The first sleeper carries the pane mark itself; the second shell
        // stays the parent of a sleeper whose environment drops it. Neither
        // carries a session: the session is the one whose socket their
        // Herdr server ancestor listens on.
        let marked = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        // The shell's child is `env` until it execs `sleep` without the
        // marks; the spawn returns once it has.
        let (scrubbed, sleeper) = std::process::Command::new("sh")
            .args(["-c", "env -u HERDR_SESSION -u HERDR_PANE_ID sleep 30; true"])
            .spawn_marked_with_child("pane-2", "sleep");
        let scrubbed = MarkedProcess(scrubbed);
        let marked_process = CallerProcess {
            process_id: marked.0.id(),
        };
        assert_eq!(marked_process.marked_pane(), None);
        assert_eq!(
            marked_process.caller_pane(&fixture.nexus.herdr),
            pane("pane-1")
        );

        assert_eq!(
            fs::read_to_string(format!(
                "/proc/{}/task/{}/children",
                scrubbed.0.id(),
                scrubbed.0.id()
            ))
            .unwrap_or_default()
            .split_whitespace()
            .next(),
            Some(sleeper.to_string().as_str()),
            "the sleeper is the shell's child"
        );
        let sleeper = CallerProcess {
            process_id: sleeper,
        };
        assert_eq!(sleeper.marked_pane_id(), None);
        assert_eq!(sleeper.caller_pane(&fixture.nexus.herdr), pane("pane-2"));

        assert_eq!(
            fixture
                .nexus
                .resolve_caller(marked_process.caller_pane(&fixture.nexus.herdr), None),
            Response::CallerResolved(mind_live())
        );
    }

    /// The Report of a hook built on an earlier contract, a Stop without its
    /// background work, decodes as no Query: it is refused in this
    /// contract's words, naming the calling process and its seat (the pane
    /// it runs in and the flow bound there), and the refusal is recorded,
    /// so Observe.Undecodable shows it to an owner the earlier client
    /// could not answer.
    #[test]
    fn an_undecodable_report_frame_is_recorded_as_a_typed_refusal_with_process_and_seat_and_appears_in_observe()
     {
        use crate::store::ReadsFlowMemory;
        use crate::undecodable::{ObservesUndecodableFrames, ReadsOrdinaryFrames};
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let hook = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let caller = CallerProcess {
            process_id: hook.0.id(),
        };
        let process = FlowStore::process_identity_for_pid(hook.0.id()).expect("hook identity");
        let earlier = signal_flow_before_request_ids::Query::Report(
            signal_flow_before_request_ids::Report_Data {
                flow_id: "a001".into(),
                event: signal_flow_before_request_ids::Event::Stopped(
                    signal_flow_before_request_ids::NativeStop {
                        native_session_id_option: Some("session-a001".into()),
                        transcript_path_option: Some("/p/session-a001.jsonl".into()),
                        last_assistant_message_option: None,
                    },
                ),
            },
        );
        let frame = rkyv::to_bytes::<rkyv::rancor::Error>(&earlier).expect("earlier frame");

        let refused = fixture
            .nexus
            .ordinary_query(&frame, Some(&process), Some(caller))
            .map(|_| ())
            .map_err(|refused| *refused);
        let Err(Response::FrameRefused(refused)) = refused else {
            panic!("an earlier hook's Report decodes as no Query");
        };
        assert_eq!(
            refused
                .flow_id_option
                .as_ref()
                .map(|value| value.0.as_str()),
            Some("a001")
        );
        assert_eq!(refused.seat_evidence.process, process);
        assert_eq!(
            refused.seat_evidence.seat_pane_option,
            Some(signal_flow::SeatPane {
                herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("pane-1".into()),
            })
        );

        let mut observed = Vec::new();
        let ended = fixture.nexus.observe_undecodable(
            &std::sync::atomic::AtomicBool::new(false),
            &mut |response| {
                observed.push(response.clone());
                Err("the observer has what it came for".into())
            },
        );
        assert!(ended.is_err());
        assert_eq!(observed, vec![Response::UndecodableObserved(vec![refused])]);
    }

    /// A flow sends through Flow from the process it runs: the sender is
    /// the metaflow Awake as the flow bound in the caller's pane, the request
    /// is queued under it, a recipient is the Address the Send names, and a
    /// process in no bound pane sends nothing.
    #[test]
    fn a_send_is_authenticated_and_primary_coordination_is_refused() {
        use crate::sending::SendsRequests;
        use crate::store::ReadsFlowMemory;
        use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
        use signal_flow::{Address, Message, Model, Permaflow, Request, SendRequest, SentBy};
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let address = |aspect: Aspect, topic: &str, model: Model| Address {
            aspect,
            topic: flow_ethos::Topic(topic.into()),
            model,
        };
        let sender = address(Aspect::Mind, "core", Model::Sol);
        let sibling = address(Aspect::Mind, "flow", Model::Sol);
        let field = address(Aspect::Field, "core", Model::Sol);
        let elsewhere = address(Aspect::Psyche, "flow", Model::Sol);
        for (held, state) in [
            (
                sender.clone(),
                flow_ethos::PermaflowState::Awake(flow_ethos::FlowId("a001".into())),
            ),
            (sibling.clone(), flow_ethos::PermaflowState::Asleep),
            (field.clone(), flow_ethos::PermaflowState::Asleep),
            (elsewhere.clone(), flow_ethos::PermaflowState::Asleep),
            (
                address(Aspect::Mind, "core", Model::Astra),
                flow_ethos::PermaflowState::Asleep,
            ),
        ] {
            fixture.nexus.store.seed_metaflow(Permaflow {
                address: held,
                state,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
        }
        let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let peer = FlowStore::process_identity_for_pid(caller.0.id()).expect("caller identity");
        let send = |recipient: Address, request: Request, peer: &signal_flow::Process| {
            fixture
                .nexus
                .send_request(
                    SendRequest {
                        address: recipient,
                        sendable_message: sendable_message(Message::Soft(request)),
                    },
                    Some(peer),
                )
                .response
        };
        let order = Request::Order("Review the drain fix".into());
        assert!(matches!(
            send(sibling.clone(), order.clone(), &peer),
            Response::SendAccepted(_)
        ));
        assert!(matches!(
            send(field.clone(), Request::Notice("released".into()), &peer),
            Response::SendAccepted(_)
        ));
        assert!(matches!(
            send(elsewhere, order.clone(), &peer),
            Response::SendAccepted(_)
        ));
        assert!(matches!(
            send(
                address(Aspect::Mind, "core", Model::Astra),
                Request::Result(flow_ethos::ResultPayload::Answered(
                    flow_ethos::ResultPayload_Answered_Data {
                        logical_delivery_id: flow_ethos::LogicalDeliveryId(String::new()),
                        request_id: flow_ethos::RequestId("none".into()),
                        string: "done".into(),
                    },
                )),
                &peer
            ),
            Response::SendRefused(signal_flow::VisibleRefusal::Unresolved)
        ));
        let queue = fixture
            .nexus
            .store
            .metaflow(&sibling)
            .expect("metaflow reads")
            .expect("metaflow held")
            .queue;
        let [queued] = queue.0.as_slice() else {
            panic!("one request is queued: {queue:?}")
        };
        assert_eq!(
            queued.logical_delivery_id,
            flow_ethos::LogicalDeliveryId("logical-delivery-00000000000000000001".into())
        );
        assert_eq!(queued.request_id.0.len(), 4);
        assert!(
            queued
                .request_id
                .0
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
        );
        assert_eq!(queued.sent_by, SentBy::Sender(sender));
        assert_eq!(queued.delivery, flow_ethos::Delivery::Queue);
        assert_eq!(queued.message, Message::Soft(order.clone()));
        let stranger = MarkedProcess::spawn("sleep", &["30"], "pane-9");
        let stranger =
            FlowStore::process_identity_for_pid(stranger.0.id()).expect("stranger identity");
        // A caller in no flow sends as Unbound and is served.
        assert!(matches!(
            send(sibling.clone(), order, &stranger),
            Response::SendAccepted(_)
        ));
        let queue = fixture
            .nexus
            .store
            .metaflow(&sibling)
            .expect("metaflow reads")
            .expect("metaflow held")
            .queue;
        assert_eq!(
            queue.0.last().map(|entry| entry.sent_by.clone()),
            Some(SentBy::Unbound)
        );
    }

    /// Result correlation uses only the reverse route and the exact typed
    /// Order retained in the delivery ledger, whoever answers. Missing and
    /// misaddressed matches leave every retained request untouched.
    #[test]
    fn a_result_resolves_one_owned_request_id_and_refuses_missing_or_foreign_ids() {
        use crate::store::RecordsLogicalDeliveries;
        use crate::store::metaflow::SeedsMetaflow;
        use flow_ethos::{PermaflowState as State, ResultPayload};
        use signal_flow::{
            Address, Message, Model, PrivateSendResponse, Refusal, SendableMessage,
            SendableRequest, SentBy,
        };

        let fixture = NexusFixture::new();
        let at = |topic: &str| Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic(topic.into()),
            model: Model::Sol,
        };
        let sender = at("resultSender");
        let recipient = at("resultRecipient");
        let stranger = at("resultStranger");
        for address in [&sender, &recipient, &stranger] {
            fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
                address: address.clone(),
                permaflow_state: State::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
        }
        let PrivateSendResponse::SendAccepted(accepted) = fixture
            .nexus
            .store
            .accept_logical_delivery(
                &recipient,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Queue,
                Message::Soft(flow_ethos::Request::Order("Review".into())),
                None,
            )
            .expect("order admission")
        else {
            panic!("order accepted")
        };
        fixture
            .nexus
            .store
            .transition_logical_delivery(
                &accepted.logical_delivery_id,
                signal_flow::DeliveryState::Uncertain,
            )
            .expect("order reaches an answerable state");
        let result = |request_id: String| {
            SendableMessage::Soft(SendableRequest::Result(
                signal_flow::SendableRequest_Result_Data {
                    request_id: flow_ethos::RequestId(request_id),
                    string: "done".into(),
                },
            ))
        };
        assert!(matches!(
            fixture
                .nexus
                .store
                .resolve_sendable_message(result(accepted.request_id.clone().0)),
            Ok(Ok(Message::Soft(flow_ethos::Request::Result(
                ResultPayload::Answered(_)
            ))))
        ));
        assert_eq!(
            fixture
                .nexus
                .store
                .resolve_sendable_message(result("none".into()))
                .unwrap(),
            Err(Refusal::Unresolved)
        );
    }

    #[test]
    fn archive_waits_for_its_answer_then_each_sender_archives_its_own_request() {
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        use crate::store::metaflow::SeedsMetaflow;
        use flow_ethos::{PermaflowState as State, ResultPayload};
        use signal_flow::{
            Address, ArchiveRejection, Message, Model, PrivateSendResponse, SendableMessage,
            SendableRequest, SentBy,
        };

        let fixture = NexusFixture::new();
        let at = |topic: &str| Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic(topic.into()),
            model: Model::Sol,
        };
        let sender = at("archiveOrderSender");
        let recipient = at("archiveOrderRecipient");
        for address in [&sender, &recipient] {
            fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
                address: address.clone(),
                permaflow_state: State::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
        }
        let PrivateSendResponse::SendAccepted(order) = fixture
            .nexus
            .store
            .accept_logical_delivery(
                &recipient,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Queue,
                Message::Soft(flow_ethos::Request::Order("Review".into())),
                None,
            )
            .expect("order admits")
        else {
            panic!("order accepted")
        };
        fixture
            .nexus
            .store
            .transition_logical_delivery(
                &order.logical_delivery_id,
                signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                    harness_kind: signal_flow::HarnessKind::Codex,
                    native_session_id: signal_flow::NativeSessionId("archive-order-session".into()),
                    native_prompt_id: signal_flow::NativePromptId("archive-order-prompt".into()),
                }),
            )
            .expect("order delivered");
        let resolved = fixture
            .nexus
            .store
            .resolve_sendable_message(SendableMessage::Soft(SendableRequest::Result(
                signal_flow::SendableRequest_Result_Data {
                    request_id: order.request_id.clone(),
                    string: "done".into(),
                },
            )))
            .expect("answer resolves")
            .expect("answer is authorized");
        assert!(matches!(
            resolved,
            Message::Soft(flow_ethos::Request::Result(ResultPayload::Answered(_)))
        ));
        let PrivateSendResponse::SendAccepted(answer) = fixture
            .nexus
            .store
            .accept_logical_delivery(
                &sender,
                SentBy::Sender(recipient.clone()),
                flow_ethos::Delivery::Queue,
                resolved,
                None,
            )
            .expect("answer admits")
        else {
            panic!("answer accepted")
        };
        assert_eq!(
            fixture
                .nexus
                .store
                .archive_request(&order.request_id.0)
                .expect("archive checks answer"),
            Err(ArchiveRejection::NotYet)
        );
        fixture
            .nexus
            .store
            .transition_logical_delivery(
                &answer.logical_delivery_id,
                signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                    harness_kind: signal_flow::HarnessKind::Codex,
                    native_session_id: signal_flow::NativeSessionId(
                        "archive-answer-session".into(),
                    ),
                    native_prompt_id: signal_flow::NativePromptId("archive-answer-prompt".into()),
                }),
            )
            .expect("answer delivered");
        assert_eq!(
            fixture
                .nexus
                .store
                .archive_request(&answer.request_id.0)
                .expect("answer archives"),
            Ok(())
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .archive_request(&order.request_id.0)
                .expect("order archives"),
            Ok(())
        );
    }

    /// Done resolution is the same exact typed operation for Tasks. The
    /// resolved private task key and its new delivery enter one atomic
    /// admission; repeating the public Done cannot close another task.
    #[test]
    fn a_done_closes_one_owned_task_id_once() {
        use crate::store::RecordsLogicalDeliveries;
        use crate::store::metaflow::SeedsMetaflow;
        use flow_ethos::{AnswerableTask, DonePayload, PermaflowState as State};
        use signal_flow::{
            Address, Message, Model, PrivateSendResponse, Refusal, SendableMessage,
            SendableRequest, SentBy,
        };

        let fixture = NexusFixture::new();
        let at = |topic: &str| Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic(topic.into()),
            model: Model::Sol,
        };
        let sender = at("taskSender");
        let recipient = at("taskRecipient");
        for address in [&sender, &recipient] {
            fixture.nexus.store.seed_metaflow(signal_flow::Permaflow {
                address: address.clone(),
                permaflow_state: State::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
        }
        let task = AnswerableTask {
            minutes: flow_ethos::Minutes(5),
            string: "Do it".into(),
        };
        let PrivateSendResponse::SendAccepted(accepted) = fixture
            .nexus
            .store
            .accept_logical_delivery(
                &recipient,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Queue,
                Message::Soft(flow_ethos::Request::Task(task)),
                None,
            )
            .expect("task admission")
        else {
            panic!("task accepted")
        };
        fixture
            .nexus
            .store
            .transition_logical_delivery(
                &accepted.logical_delivery_id,
                signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                    harness_kind: signal_flow::HarnessKind::Codex,
                    native_session_id: signal_flow::NativeSessionId(
                        "answerable-task-session".into(),
                    ),
                    native_prompt_id: signal_flow::NativePromptId("answerable-task-turn".into()),
                }),
            )
            .expect("task reaches an answerable state");
        let done = || {
            SendableMessage::Soft(SendableRequest::Done(
                signal_flow::SendableRequest_Done_Data {
                    request_id: accepted.request_id.clone(),
                    string: "done".into(),
                },
            ))
        };
        let private_done = fixture
            .nexus
            .store
            .resolve_sendable_message(done())
            .expect("done resolution")
            .expect("task id resolves");
        assert!(matches!(
            &private_done,
            Message::Soft(flow_ethos::Request::Done(DonePayload::Answered(_)))
        ));
        assert!(matches!(
            fixture
                .nexus
                .store
                .accept_logical_delivery(
                    &sender,
                    SentBy::Sender(recipient.clone()),
                    flow_ethos::Delivery::Queue,
                    private_done,
                    None
                )
                .expect("done admission"),
            PrivateSendResponse::SendAccepted(_)
        ));
        assert_eq!(
            fixture
                .nexus
                .store
                .resolve_sendable_message(done())
                .unwrap(),
            Err(Refusal::Unresolved)
        );
    }

    /// A Launch starts from the metaflow's declaration in Flow's
    /// configuration, at its Model's configured native model and effort; an
    /// undeclared Address is refused before anything launches.
    #[test]
    fn a_launch_is_composed_from_the_declaration_or_refused_undeclared() {
        use crate::sending::LaunchesDeclared;
        use meta_signal_flow::{ConfigurationChange, ConfigureModel, ConfigurePermaflow};
        use signal_flow::{Address, LaunchRequest, Model};
        let fixture = NexusFixture::new();
        let declared = Address {
            aspect: Aspect::Field,
            topic: flow_ethos::Topic("publisher".into()),
            model: Model::Terra,
        };
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Configure(
                    ConfigurationChange::Metaflows(vec![ConfigurePermaflow {
                        root_option: None,
                        address: declared.clone(),
                        system_modules: signal_flow::SystemModules(vec![
                            signal_flow::ModuleIdentity("spirit-core".into())
                        ]),
                        user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                            "knowledge-flow".into()
                        )]),
                        flow_kind: signal_flow::FlowKind::Regular,
                        tool_set: signal_flow::ToolSet::Default,
                        continuity: meta_signal_flow::Continuity::OnDemand,
                        purpose: meta_signal_flow::Purpose("Publishes the books.".into()),
                        records: meta_signal_flow::Records(vec![meta_signal_flow::RecordPath(
                            "flows/a68420/vision/flow.md".into()
                        )]),
                    }])
                )),
            meta_signal_flow::Response::Configured(_)
        ));
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Configure(
                    ConfigurationChange::Models(vec![ConfigureModel {
                        model: Model::Terra,
                        native: meta_signal_flow::Native("gpt-6-luna".into()),
                    }])
                )),
            meta_signal_flow::Response::Configured(_)
        ));
        let launch = |address: Address| LaunchRequest {
            address,
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(1)),
            instruction_prompt: signal_flow::InstructionPrompt(
                "Publish the newest edition.".into(),
            ),
        };
        let Ok(Query::Start(profile)) = fixture.nexus.declared_launch(launch(declared.clone()))
        else {
            panic!("a declared Address launches as a Start");
        };
        assert_eq!(profile.address, declared);
        assert_eq!(
            profile.system_modules.0,
            vec![signal_flow::ModuleIdentity("spirit-core".to_owned())]
        );
        assert_eq!(
            profile.user_modules.0,
            vec![signal_flow::ModuleIdentity("knowledge-flow".to_owned())]
        );
        assert_eq!(
            profile.model_name,
            flow_ethos::ModelName("gpt-6-luna".into())
        );
        assert_eq!(profile.effort, signal_flow::Effort("low".into()));
        assert_eq!(profile.flow_id_option, None);
        // Every launch opens in the one Herdr session the Nexus declares,
        // whoever asks: the fixture's live session, then another declared.
        assert_eq!(
            profile.herdr_session_name,
            signal_flow::HerdrSessionName("messaging-build".into())
        );
        fixture.declare_launch_session("declaredSession");
        let Ok(Query::Start(redeclared)) = fixture.nexus.declared_launch(launch(declared.clone()))
        else {
            panic!("a declared Address launches as a Start");
        };
        assert_eq!(
            redeclared.herdr_session_name,
            signal_flow::HerdrSessionName("declaredSession".into())
        );
        fixture.declare_launch_session("messaging-build");
        assert_eq!(
            profile.instruction_prompt,
            signal_flow::InstructionPrompt(
                "Publishes the books.\n\nPublish the newest edition.".into()
            )
        );
        assert_eq!(
            profile.remembering,
            signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(1)),
            "the Launch's Remembering is the profile's"
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::Launch(launch(Address {
                aspect: Aspect::Mind,
                topic: flow_ethos::Topic("nowhere".into()),
                model: Model::Sol,
            }))),
            Response::StartRejected(StartRejection::Undeclared)
        );
    }

    /// A Start's origin is derived from its calling process as Identify
    /// resolves a caller: the one flow bound in the peer's pane, with its
    /// native session. A process in no bound pane, or a peer not read, is
    /// Unbound: recorded as such, never refused.
    #[test]
    fn a_start_origin_is_the_bound_flow_in_the_callers_pane_or_unbound() {
        use crate::generated::starter::DerivesOrigin;
        use crate::store::ReadsFlowRows;
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let bound = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let unbound = MarkedProcess::spawn("sleep", &["30"], "pane-9");
        let session_id = fixture
            .nexus
            .store
            .flow_node("a001")
            .unwrap()
            .expect("the bound flow's row")
            .session_id;
        assert_eq!(
            fixture.nexus.origin_of(Some(CallerProcess {
                process_id: bound.0.id(),
            })),
            Origin::Flow(FlowOrigin {
                flow_id: flow_ethos::FlowId("a001".into()),
                session_id,
            })
        );
        assert_eq!(
            fixture.nexus.origin_of(Some(CallerProcess {
                process_id: unbound.0.id(),
            })),
            Origin::Unbound
        );
        assert_eq!(
            fixture.nexus.origin_of(None::<CallerProcess>),
            Origin::Unbound
        );
    }

    /// A Herdr server that listens on the sockets of two listed sessions
    /// does not say which one its pane is in: the pane is not found, and no
    /// flow is named.
    #[test]
    fn a_pane_id_whose_herdr_server_serves_two_sessions_names_no_pane() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let marked = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let marked_process = CallerProcess {
            process_id: marked.0.id(),
        };
        assert_eq!(
            marked_process.caller_pane(&fixture.nexus.herdr),
            pane("pane-1")
        );

        let herdr_config = fixture.directory.path().join("herdr-config");
        let other_socket = herdr_config.join("sessions/other/herdr.sock");
        fs::create_dir_all(other_socket.parent().unwrap()).unwrap();
        let _other_server = UnixListener::bind(&other_socket).unwrap();
        fixture.list_herdr_sessions(&[
            ("default", herdr_config.join("herdr.sock")),
            (
                "messaging-build",
                herdr_config.join("sessions/messaging-build/herdr.sock"),
            ),
            ("other", other_socket),
        ]);
        assert_eq!(marked_process.caller_pane(&fixture.nexus.herdr), None);
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(marked_process.caller_pane(&fixture.nexus.herdr), None),
            Response::CallerResolutionRejected(
                signal_flow::CallerResolutionRejection::CallerUnknown
            )
        );
    }

    /// The Herdr server is known by the session socket it serves, never by
    /// its binary: the `herdr` this Nexus runs (here the fixture script, as
    /// on a host where a wrapper on `PATH` stands before Herdr's own binary)
    /// is not the binary the server runs, and a process between the peer and
    /// the server that serves no session is walked past.
    #[test]
    fn a_pane_id_names_the_session_its_server_serves_whatever_herdr_path_names() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let server = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let herdr_cli = fs::canonicalize(&fixture.snapshot_program).unwrap();
        assert_ne!(herdr_cli, server);

        // The spawn returns once the shell's child has exec'd `sleep`.
        let (shell, child) = std::process::Command::new("sh")
            .args(["-c", "sleep 30; true"])
            .spawn_marked_with_child("pane-1", "sleep");
        let _shell = MarkedProcess(shell);
        let peer = CallerProcess { process_id: child };
        assert_eq!(peer.marked_pane_id().as_deref(), Some("pane-1"));
        assert_eq!(peer.caller_pane(&fixture.nexus.herdr), pane("pane-1"));
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(peer.caller_pane(&fixture.nexus.herdr), None),
            Response::CallerResolved(mind_live())
        );
    }

    #[test]
    fn an_unbound_peer_is_caller_unknown() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![
            codex_agent_in("pane-1", "terminal-pane-1"),
            codex_agent_in("pane-9", "terminal-pane-9"),
        ]);
        let unknown = Response::CallerResolutionRejected(
            signal_flow::CallerResolutionRejection::CallerUnknown,
        );

        // A pane the registry binds no flow to.
        assert_eq!(fixture.nexus.resolve_caller(pane("pane-9"), None), unknown);
        // A peer outside any Herdr pane.
        assert_eq!(fixture.nexus.resolve_caller(None, None), unknown);
        // A dispatch has no connection, so no peer.
        assert_eq!(fixture.nexus.dispatch(Query::ResolveCaller(None)), unknown);
        // Herdr reused the pane ID for a new terminal: the binding is gone.
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-new")]);
        assert_eq!(fixture.nexus.resolve_caller(pane("pane-1"), None), unknown);
    }

    #[test]
    fn a_claimed_flow_id_that_differs_is_caller_mismatch() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        assert_eq!(
            fixture.nexus.resolve_caller(
                pane("pane-1"),
                Some(flow_ethos::FlowId("field-other".into()))
            ),
            Response::CallerResolutionRejected(
                signal_flow::CallerResolutionRejection::CallerMismatch(mind_live())
            )
        );
    }

    #[test]
    fn matching_meta_cli_reads_configuration_and_populated_metadata_over_the_server() {
        use crate::store::events::RecordsHarnessEvents;
        use signal_flow::Event;
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture
            .nexus
            .store
            .record_event(
                "a001",
                Event::Stopped(signal_flow::NativeStop {
                    native_session_id_option: Some(signal_flow::NativeSessionId(
                        "cli-session".into(),
                    )),
                    transcript_path_option: Some(signal_flow::TranscriptPath(
                        "/fixture/cli-transcript".into(),
                    )),
                    last_assistant_message_option: signal_flow::LastAssistantMessageOption(Some(
                        "CLI_BODY_MUST_BE_ABSENT".into(),
                    )),
                    background_work: signal_flow::BackgroundWork(vec![]),
                }),
            )
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let runtime = directory.path().join("runtime");
        let configuration = directory.path().join("configuration");
        let socket = runtime.join("nexus/main/flow-meta.sock");
        fs::create_dir_all(socket.parent().unwrap()).unwrap();
        fs::create_dir_all(configuration.join("nexus")).unwrap();
        fs::write(configuration.join("nexus/flow-nexus.datom"), "{ main }").unwrap();
        let listener = UnixListener::bind(&socket).unwrap();
        let binary = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join("flow-nexus-meta");
        std::thread::scope(|scope| {
            let server = scope.spawn(|| {
                for _ in 0..2 {
                    let (peer, _) = listener.accept().unwrap();
                    Connection {
                        peer,
                        process: None,
                    }
                    .serve_meta(&fixture.nexus)
                    .unwrap();
                }
            });
            for query in ["Configuration", "ReadEvents.a001"] {
                let output = std::process::Command::new(&binary)
                    .env_clear()
                    .env("XDG_RUNTIME_DIR", &runtime)
                    .env("XDG_CONFIG_HOME", &configuration)
                    .arg(query)
                    .output()
                    .expect("matching actual meta CLI runs");
                assert!(output.status.success(), "{output:?}");
                let text = String::from_utf8(output.stdout).unwrap();
                if query == "Configuration" {
                    assert!(text.starts_with("Configuration."), "{text}");
                } else {
                    assert!(text.starts_with("EventsRead."), "{text}");
                    assert!(
                        text.contains("cli-session") && text.contains("/fixture/cli-transcript"),
                        "{text}"
                    );
                    assert!(!text.contains("CLI_BODY_MUST_BE_ABSENT"), "{text}");
                    println!("MATCHED_META_CLI_POPULATED_READ_EVENTS={text}");
                }
            }
            server.join().unwrap();
        });
    }

    #[test]
    fn meta_connection_settles_contract_before_decoding_queries() {
        use signal::Contracted;
        for greeting in [
            meta_signal_flow::Query::greeting(),
            meta_signal_flow_76::Query::greeting(),
            // The ordinary 61 greeting is no meta greeting either.
            signal_flow::Query::greeting(),
        ] {
            let fixture = NexusFixture::new();
            let (peer, mut client) = UnixStream::pair().unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            std::thread::scope(|scope| {
                let server = scope.spawn(|| {
                    Connection {
                        peer,
                        process: None,
                    }
                    .serve_meta(&fixture.nexus)
                });
                client
                    .write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(&greeting).unwrap())
                    .unwrap();
                let receipt = rkyv::from_bytes::<signal::HandshakeReceipt, rkyv::rancor::Error>(
                    &client.read_frame().unwrap(),
                )
                .unwrap();
                if greeting == meta_signal_flow::Query::greeting() {
                    assert_eq!(
                        receipt,
                        signal::HandshakeReceipt::Greeted(
                            meta_signal_flow::Query::contract_digest()
                        )
                    );
                    client
                        .write_frame(
                            &rkyv::to_bytes::<rkyv::rancor::Error>(
                                &meta_signal_flow::Query::Configuration,
                            )
                            .unwrap(),
                        )
                        .unwrap();
                    let response = rkyv::from_bytes::<
                        meta_signal_flow::Response,
                        rkyv::rancor::Error,
                    >(&client.read_frame().unwrap())
                    .unwrap();
                    assert!(matches!(
                        response,
                        meta_signal_flow::Response::Configuration(_)
                    ));
                    assert!(server.join().unwrap().is_ok());
                } else {
                    assert_eq!(
                        receipt,
                        signal::HandshakeReceipt::GreetingRefused(
                            signal::HandshakeRejection::ContractMismatch(
                                meta_signal_flow::Query::contract_digest()
                            ),
                        )
                    );
                    assert!(
                        server
                            .join()
                            .unwrap()
                            .unwrap_err()
                            .contains("meta greeting refused")
                    );
                    assert!(
                        client.read_frame().is_err(),
                        "refusal closes before any Query is decoded"
                    );
                    // The refusal is recorded as the ordinary socket records
                    // its own: one record, metadata alone.
                    use crate::store::undecodable_frame::RecordsUndecodableFrames;
                    assert_eq!(fixture.nexus.store.undecodable_frames().unwrap().len(), 1);
                }
            });
        }

        use crate::store::ReadsFlowRows;
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        let before = fixture.nexus.store.flow_node("a001").unwrap();
        let (peer, mut client) = UnixStream::pair().unwrap();
        client
            .write_frame(
                &rkyv::to_bytes::<rkyv::rancor::Error>(&meta_signal_flow_76::Query::Retire(
                    flow_ethos::FlowId("a001".into()),
                ))
                .unwrap(),
            )
            .unwrap();
        let result = Connection {
            peer,
            process: None,
        }
        .serve_meta(&fixture.nexus);
        assert!(result.unwrap_err().contains("meta greeting refused"));
        let receipt = rkyv::from_bytes::<signal::HandshakeReceipt, rkyv::rancor::Error>(
            &client.read_frame().unwrap(),
        )
        .unwrap();
        assert_eq!(
            receipt,
            signal::HandshakeReceipt::GreetingRefused(
                signal::HandshakeRejection::ContractMismatch(
                    meta_signal_flow::Query::contract_digest()
                ),
            )
        );
        assert!(
            client.read_frame().is_err(),
            "raw legacy queries are closed without dispatch"
        );
        assert_eq!(
            fixture.nexus.store.flow_node("a001").unwrap(),
            before,
            "the raw Retire query never takes effect"
        );
    }

    #[test]
    fn the_kernel_names_the_peer_of_a_connection() {
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let own = Some(CallerProcess {
            process_id: std::process::id(),
        });
        assert_eq!(ours.peer_process(), own);
        assert_eq!(theirs.peer_process(), own);
    }
    #[test]
    fn agent_connection_cancellation_releases_quiet_and_unanswered_subscriptions_without_exit() {
        use crate::HoldsStopRequest;
        use crate::event_wait::AwaitsDescriptors;
        use crate::observation_lifetime::ServesCancellableObservation;
        use crate::observe_agent::ObservesAgent;
        use std::io::{BufRead, BufReader, Read, Write};
        use std::os::fd::AsFd;
        use std::sync::mpsc;
        for acknowledged in [false, true] {
            for stop in [false, true] {
                let fixture = NexusFixture::new();
                bind_existing_caller(&fixture, "a001", "pane-1");
                fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
                let (peer, client) = UnixStream::pair().unwrap();
                let (sent, received) = mpsc::channel();
                std::thread::scope(|scope| {
                    let nexus = &fixture.nexus;
                    let observer = scope.spawn(move || {
                        let mut connection = Connection::accepted(peer).unwrap();
                        connection.serve_cancellable_observation(
                            nexus,
                            &mut |cancellation, _peer| {
                                nexus.observe_agent("a001", cancellation, &mut |response| {
                                    sent.send(response.clone()).unwrap();
                                    Ok(())
                                })
                            },
                        )
                    });
                    let opening = received.recv_timeout(Duration::from_secs(3)).unwrap();
                    assert!(matches!(
                        opening,
                        Response::AgentObserved(signal_flow::AgentObservation {
                            agent_state: signal_flow::AgentState::Working,
                            ..
                        })
                    ));
                    assert!(
                        [fixture._herdr_server.as_fd()]
                            .ready(Some(std::time::Instant::now() + Duration::from_secs(3)))
                            .unwrap()[0]
                    );
                    let (mut upstream, _) = fixture._herdr_server.accept().unwrap();
                    upstream
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let mut request = String::new();
                    BufReader::new(&upstream).read_line(&mut request).unwrap();
                    assert!(request.contains("events.subscribe"));
                    if acknowledged {
                        upstream
                            .write_all(b"{\"result\":{\"type\":\"subscription_started\"}}\n")
                            .unwrap();
                    }
                    if stop {
                        nexus.stopping.request();
                    } else {
                        drop(client);
                    }
                    let mut byte = [0];
                    match upstream.read(&mut byte) {
                        Ok(0) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                        other => panic!(
                            "upstream closes within its read timeout without data: {other:?}"
                        ),
                    }
                    let result = observer.join().unwrap();
                    assert!(result.unwrap_err().contains(if stop {
                        "NexusStopped"
                    } else {
                        "PeerClosed"
                    }));
                    assert!(
                        received.try_recv().is_err(),
                        "cancellation never emits Gone"
                    );
                });
                assert_eq!(fixture.lifecycle("a001"), FlowLifecycle::Active);
            }
        }
    }

    #[test]
    fn pending_launch_connection_releases_on_peer_close_or_stop() {
        use crate::HoldsStopRequest;
        use crate::launching::ObservesLaunch;
        use crate::observation_lifetime::ServesCancellableObservation;
        use std::sync::mpsc;
        for stop in [false, true] {
            let fixture = NexusFixture::new();
            let staged = fixture.staged_launch("pending-observation", None);
            staged.reserve(&fixture.nexus);
            let (peer, client) = UnixStream::pair().unwrap();
            let (sent, received) = mpsc::channel();
            let (done, finished) = mpsc::channel();
            std::thread::scope(|scope| {
                let nexus = &fixture.nexus;
                let observer = scope.spawn(move || {
                    let mut connection = Connection::accepted(peer).unwrap();
                    let result = connection.serve_cancellable_observation(
                        nexus,
                        &mut |cancellation, _peer| {
                            nexus.observe_launch(
                                "pending-observation",
                                cancellation,
                                &mut |response| {
                                    sent.send(response.clone()).unwrap();
                                    Ok(())
                                },
                            )
                        },
                    );
                    done.send(result).unwrap();
                });
                assert!(matches!(
                    received.recv_timeout(Duration::from_secs(3)).unwrap(),
                    Response::LaunchInspected(_)
                ));
                if stop {
                    nexus.stopping.request();
                } else {
                    drop(client);
                }
                assert!(
                    finished
                        .recv_timeout(Duration::from_secs(3))
                        .unwrap()
                        .is_err()
                );
                observer.join().unwrap();
            });
        }
    }

    #[test]
    fn agent_subscription_delivers_state_and_genuine_gone_records_exit() {
        use crate::event_wait::AwaitsDescriptors;
        use crate::observation_lifetime::ServesCancellableObservation;
        use crate::observe_agent::ObservesAgent;
        use std::io::{BufRead, BufReader, Write};
        use std::os::fd::AsFd;
        use std::sync::mpsc;
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let (peer, _client) = UnixStream::pair().unwrap();
        let (sent, received) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        std::thread::scope(|scope| {
            let nexus = &fixture.nexus;
            let observer = scope.spawn(move || {
                let mut connection = Connection::accepted(peer).unwrap();
                let result =
                    connection.serve_cancellable_observation(nexus, &mut |cancellation, _peer| {
                        nexus.observe_agent("a001", cancellation, &mut |response| {
                            sent.send(response.clone()).unwrap();
                            Ok(())
                        })
                    });
                done.send(result).unwrap();
            });
            assert!(matches!(
                received.recv_timeout(Duration::from_secs(3)).unwrap(),
                Response::AgentObserved(signal_flow::AgentObservation {
                    agent_state: signal_flow::AgentState::Working,
                    ..
                })
            ));
            assert!(
                [fixture._herdr_server.as_fd()]
                    .ready(Some(std::time::Instant::now() + Duration::from_secs(3)))
                    .unwrap()[0]
            );
            let (mut upstream, _) = fixture._herdr_server.accept().unwrap();
            upstream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            BufReader::new(&upstream)
                .read_line(&mut String::new())
                .unwrap();
            upstream.write_all(concat!(
                "{\"result\":{\"type\":\"subscription_started\"}}\n",
                "{\"event\":\"pane.agent_status_changed\",\"data\":{\"pane_id\":\"pane-1\",\"agent_status\":\"idle\"}}\n",
                "{\"event\":\"pane.closed\",\"data\":{\"pane_id\":\"pane-1\"}}\n"
            ).as_bytes()).unwrap();
            assert!(matches!(
                received.recv_timeout(Duration::from_secs(3)).unwrap(),
                Response::AgentObserved(signal_flow::AgentObservation {
                    agent_state: signal_flow::AgentState::Idle,
                    ..
                })
            ));
            assert!(matches!(
                received.recv_timeout(Duration::from_secs(3)).unwrap(),
                Response::AgentObserved(signal_flow::AgentObservation {
                    agent_state: signal_flow::AgentState::Gone,
                    ..
                })
            ));
            assert!(
                finished
                    .recv_timeout(Duration::from_secs(3))
                    .unwrap()
                    .is_ok()
            );
            observer.join().unwrap();
        });
        assert_eq!(fixture.lifecycle("a001"), FlowLifecycle::Exited);
    }
    #[test]
    fn saturated_agent_admission_closes_each_attempt_and_preserves_the_live_flow() {
        use crate::event_wait::AwaitsDescriptors;
        use crate::observation_lifetime::ServesCancellableObservation;
        use crate::observe_agent::ObservesAgent;
        use rustix::net::{AddressFamily, SocketAddrUnix, SocketFlags, SocketType};
        use std::os::fd::AsFd;
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "a001", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let socket = fixture.directory.path().join("saturated-herdr.sock");
        let descriptor = rustix::net::socket_with(
            AddressFamily::UNIX,
            SocketType::STREAM,
            SocketFlags::CLOEXEC,
            None,
        )
        .unwrap();
        rustix::net::bind(&descriptor, &SocketAddrUnix::new(&socket).unwrap()).unwrap();
        rustix::net::listen(&descriptor, 0).unwrap();
        let listener = UnixListener::from(descriptor);
        let _filler = UnixStream::connect(&socket).unwrap();
        fixture.list_herdr_sessions(&[("messaging-build", socket)]);
        for _ in 0..10 {
            let (peer, _client) = UnixStream::pair().unwrap();
            let mut connection = Connection::accepted(peer).unwrap();
            let mut states = Vec::new();
            let result = connection.serve_cancellable_observation(
                &fixture.nexus,
                &mut |cancellation, _peer| {
                    fixture
                        .nexus
                        .observe_agent("a001", cancellation, &mut |response| {
                            states.push(response.clone());
                            Ok(())
                        })
                },
            );
            assert!(result.unwrap_err().contains("backlog saturated"));
            assert_eq!(states.len(), 1, "only the initial state, never Gone");
            assert_eq!(fixture.lifecycle("a001"), FlowLifecycle::Active);
        }
        let (_accepted_filler, _) = listener.accept().unwrap();
        assert!(
            ![listener.as_fd()]
                .ready(Some(std::time::Instant::now()))
                .unwrap()[0]
        );
    }
}
