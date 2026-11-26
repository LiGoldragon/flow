//! The override panel: the owner's hand on what the Flow Nexus holds and
//! does, its inputs at the root of the meta Signal and answered there
//! (`MetaCall`'s answers in the crate root). Every override is recorded as
//! a transition of the flow it acts on, with the cause Overridden
//! (`Record.Transitioned`).
//!
//! Hold is the shutdown switch, kept in Memory so a Nexus that starts again
//! starts held: while it stands the Nexus takes no act of its own. Its
//! clocks stand (reminders, handover reminders, the heartbeat), nothing
//! queued is handed to a seat or wakes one, no seat is put to sleep,
//! refreshed or given its successor at its rest, and a Send is queued, not
//! typed. Every query and every act a caller asks for is still answered.
//! Resume moves the switch back and takes up at once what is owed, as a
//! Nexus that starts does.
//!
//! An override acts on the owner's word. Close closes a flow's harness as
//! Retire did, whatever its metaflow's continuity; Renew starts its
//! permaflow's fresh flow and reaps the old one at once, whether or not the
//! old one rests, as a Refresh of a Maintained metaflow does at a rest.
//! The fresh flow's Started is the event a Renew answers: the old flow's
//! running turn is interrupted (`InterruptsRunningTurns`), a Claude seat by
//! its harness's interrupt keys and a Codex seat through its app-server,
//! and then its harness is closed.

use crate::RunningNexus;
use crate::codex::SelectsCodexEndpoint;
use crate::delivery::lease::LeasesPanes;
use crate::generated::memory::{Cause, Moment, Transition};
use crate::generated::operation::{Operation, Outcome, Record_Data};
use crate::generated::overriding::{
    CodexThread, EntersByOverride, HeedsOverrides, Holding, InterruptsCodexThreads,
    InterruptsRunningTurns, RemembersOverrides, Seat, Subject, Transitioned, TurnInterruption,
};
use crate::herdr::pane::{Interruption, NamesKeys, PaneAgent, WritesPane};
use crate::live::HoldsLiveValue;
use crate::performing::Performs;
use crate::reminding::CountsUnixSeconds;
use crate::store::delivery::{RecordsDeliveries, SelectsHarnessProfile};
use crate::store::{ReadsFlowRoles, ReadsFlowRows};
use meta_signal_flow::{Output, Refused_Data, State, StoreFailure};
use signal_flow::{AgentState, HarnessKind, HerdrRouteSelection};

impl HeedsOverrides for RunningNexus {
    /// A switch Memory cannot read is no Hold: the Nexus acts as it did.
    fn held(&self) -> bool {
        matches!(self.store.holding(), Ok(Holding::Held(_)))
    }

    fn permaflow_held<N: crate::generated::overriding::Addressed>(&self, input: N) -> bool {
        // An unread hold is kept: no machine effect is admitted on uncertainty.
        !matches!(self.store.permaflow_held(input), Ok(false))
    }

    fn state_now(&self, seat: Seat) -> Option<State> {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        self.store.state_of(AskedFlow(seat.flow_id)).ok().flatten()
    }

    fn seat_named(&self, subject: Subject) -> Result<Seat, Output> {
        match self.store.seat(subject.clone()) {
            Ok(Some(seat)) => Ok(seat),
            Ok(None) => Err(Output::Refused(match subject {
                Subject::Named(flow_id) => Refused_Data::UnknownFlow(flow_id),
                Subject::Permaflow(address) => Refused_Data::UnknownAddress(address),
            })),
            Err(error) => Err(Output::Refused(Refused_Data::StoreRefused(StoreFailure(
                error.to_string(),
            )))),
        }
    }

    fn record_override(&self, transitioned: Transitioned) -> Result<bool, Output> {
        match self.perform(Operation::Record(Record_Data::Transitioned(transitioned))) {
            Outcome::Recorded => Ok(true),
            refused => Err(Output::Refused(Refused_Data::StoreRefused(StoreFailure(
                format!("{refused:?}"),
            )))),
        }
    }

    // A subject that names no seat, an Address no flow ever ran at, has no
    // transition to record.
    fn record_named(&self, subject: Subject) -> Result<bool, Output> {
        match self.store.seat(subject) {
            Ok(Some(seat)) => match self.state_now(seat.clone()) {
                Some(state) => self.record_override(Transitioned {
                    transition: state.overridden(),
                    seat,
                }),
                None => Ok(false),
            },
            Ok(None) => Ok(false),
            Err(error) => Err(Output::Refused(Refused_Data::StoreRefused(StoreFailure(
                error.to_string(),
            )))),
        }
    }

    fn record_every_seat(&self) -> Result<bool, Output> {
        let seats = self.store.seats().map_err(|error| {
            Output::Refused(Refused_Data::StoreRefused(StoreFailure(error.to_string())))
        })?;
        for seat in seats {
            if let Some(state) = self.state_now(seat.clone()) {
                self.record_override(Transitioned {
                    transition: state.overridden(),
                    seat,
                })?;
            }
        }
        Ok(true)
    }
}

impl EntersByOverride for State {
    fn overridden(&self) -> Transition {
        Transition {
            state: self.clone(),
            cause: Cause::Overridden,
            moment: Moment(std::time::SystemTime::now().seconds_since_epoch()),
        }
    }
}

impl InterruptsRunningTurns for RunningNexus {
    // A seat with no row, or no pane, has no harness running a turn. The
    // keys go only into a Claude seat Herdr shows Working or Blocked, under
    // its pane lease: Escape into a resting Claude opens its rewind menu.
    fn interrupt_running_turn(&self, seat: Seat) -> TurnInterruption {
        let node = match self.store.flow_node(&seat.flow_id.0) {
            Ok(Some(node)) => node,
            Ok(None) => return TurnInterruption::Rested,
            Err(error) => return TurnInterruption::StoreRefused(error.to_string()),
        };
        match node.harness_kind {
            HarnessKind::Claude => {
                let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
                    return TurnInterruption::Rested;
                };
                match self.herdr.pane_agent(&node) {
                    PaneAgent::Present {
                        agent_state: AgentState::Working | AgentState::Blocked,
                        ..
                    } => {}
                    PaneAgent::Present { .. } | PaneAgent::Absent => {
                        return TurnInterruption::Rested;
                    }
                    PaneAgent::Unreadable => return TurnInterruption::PaneUnreadable,
                }
                let keys = match self.store.delivery_configuration() {
                    Ok(configuration) => configuration
                        .profile(&node.harness_kind)
                        .interrupt_keys
                        .texts(),
                    Err(error) => return TurnInterruption::StoreRefused(error.to_string()),
                };
                let _lease = self.pane_leases.hold(route);
                match self.herdr.interrupt(route, &keys) {
                    Interruption::Refused => TurnInterruption::KeysRefused,
                    Interruption::Observed | Interruption::Unobserved => {
                        TurnInterruption::Interrupted
                    }
                }
            }
            HarnessKind::Codex => {
                let model = match self.store.role(&seat.flow_id.0) {
                    Ok(Some(role)) => role.model_name,
                    Ok(None) => return TurnInterruption::Rested,
                    Err(error) => return TurnInterruption::StoreRefused(error.to_string()),
                };
                self.codex_endpoints
                    .current()
                    .adapter_for(&model.0)
                    .and_then(|adapter| {
                        adapter.interrupt_thread(CodexThread(node.session_id.0.clone()))
                    })
                    .unwrap_or_else(|error| TurnInterruption::AppServerRefused(error.to_string()))
            }
        }
    }
}

impl crate::generated::overriding::Addressed for signal_flow::Address {
    fn address(&self) -> signal_flow::Address {
        self.clone()
    }
}

impl crate::generated::overriding::AdmitsPermaflowControl for RunningNexus {
    fn ordinary_control(
        &self,
        subject: Subject,
    ) -> Result<Option<crate::waking::WakingLease>, signal_flow::Output> {
        self.machine_control(subject)
            .map_err(|answer| match answer {
                Output::Refused(Refused_Data::PermaflowHeld(address)) => {
                    signal_flow::Output::Refused(signal_flow::Refusal::PermaflowHeld(address))
                }
                Output::Refused(Refused_Data::StoreRefused(failure)) => {
                    signal_flow::Output::Refused(signal_flow::Refusal::Store(failure.0))
                }
                _ => signal_flow::Output::Refused(signal_flow::Refusal::NotConfigured),
            })
    }

    fn machine_control(
        &self,
        subject: Subject,
    ) -> Result<Option<crate::waking::WakingLease>, Output> {
        use crate::generated::overriding::SerializesPermaflowEffects;
        let address = match subject {
            Subject::Permaflow(address) => Some(address),
            subject => self
                .store
                .seat(subject)
                .map_err(|error| {
                    Output::Refused(Refused_Data::StoreRefused(StoreFailure(error.to_string())))
                })?
                .map(|seat| seat.address),
        };
        let Some(address) = address else {
            return Ok(None);
        };
        let effect = self.waking.effect(address.clone());
        if self.permaflow_held(address.clone()) {
            return Err(Output::Refused(Refused_Data::PermaflowHeld(address)));
        }
        Ok(Some(effect))
    }
}

impl crate::generated::overriding::ParkedLaunch for crate::generated::operation::LaunchReservation {
    fn launch(&self) -> signal_flow::ComposedLaunch {
        self.composed_launch.clone()
    }
    fn origin(&self) -> signal_flow::Origin {
        self.origin.clone()
    }
    fn attempt(&self) -> signal_flow::LaunchAttempt {
        signal_flow::LaunchAttempt {
            launch_request_id: self
                .composed_launch
                .launch_profile
                .launch_request_id
                .clone(),
            launch_profile: self.composed_launch.launch_profile.clone(),
            prompt_sha256: self
                .composed_launch
                .first_prompt_payload
                .prompt_sha256
                .clone(),
            origin: self.origin.clone(),
            launch_attempt_phase: signal_flow::LaunchAttemptPhase::Reserved,
            native_launch_intent_option: None,
            native_launch_binding_option: None,
            registration_acknowledgement_option: None,
            prompt_delivery_intent_option: None,
            prompt_delivery_result_option: None,
        }
    }
}
impl crate::generated::overriding::HeldLaunch for signal_flow::LaunchRequestId {
    fn request_id(&self) -> signal_flow::LaunchRequestId {
        self.clone()
    }
}
impl crate::generated::overriding::HeldLaunch for crate::generated::memory::StoredHeldLaunch {
    fn request_id(&self) -> signal_flow::LaunchRequestId {
        self.launch_request_id.clone()
    }
}
