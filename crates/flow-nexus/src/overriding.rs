//! The override panel: the owner's hand on what the Flow Nexus holds and
//! does, answered on the meta socket as `Override` (meta-signal-flow's
//! `panel`). Every override is recorded as a transition of the flow it acts
//! on, with the cause Overridden (`Record.Transitioned`).
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

use crate::delivery::lease::LeasesPanes;
use crate::generated::memory::{Cause, Moment, RefreshCaller, Transition};
use crate::generated::operation::{Operation, Outcome, Record_Data};
use crate::generated::overriding::{
    EntersByOverride, HeedsOverrides, Holding, RemembersOverrides, Seat, Subject, Transitioned,
};
use crate::herdr::{PanePresence, ReadsHerdrPanes};
use crate::performing::Performs;
use crate::reminding::CountsUnixSeconds;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{ManagesMetaflowState, NamesLiveFlow, ReadsFlowRows};
use crate::waking::{AtStop, HoldsSoftForIdle, KeptAtStop, SleepsAndWakesSeats};
use crate::{MetaCall, RunningNexus};
use meta_signal_flow::panel::{Output, Refused_Data, SetState_Data, State, StateSet_Data};
use signal_flow::{Address, FlowId, HerdrRouteSelection, RefreshRejection};

impl HeedsOverrides for RunningNexus {
    /// A switch Memory cannot read is no Hold: the Nexus acts as it did.
    fn held(&self) -> bool {
        matches!(self.store.holding(), Ok(Holding::Held(_)))
    }

    fn state_now(&self, seat: Seat) -> State {
        let live = matches!(
            self.store.flow_node(&seat.flow_id.0),
            Ok(Some(node)) if node.flow_lifecycle.is_live()
        );
        let awake = matches!(
            self.store.metaflow(&seat.address),
            Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Awake(seat.flow_id.clone())
        );
        if !(live && awake) {
            return State::Asleep;
        }
        let recorded = self.store.transitions().ok().and_then(|flows| {
            flows
                .into_iter()
                .find(|flow| flow.flow_id == seat.flow_id)
                .and_then(|flow| flow.transition_vector.last().map(|last| last.state.clone()))
        });
        match recorded {
            Some(state) => state,
            None if self.rests_now(&seat.flow_id.0) => State::Resting,
            None => State::Acting,
        }
    }

    fn seat_named(&self, subject: Subject) -> Result<Seat, Output> {
        match self.store.seat(subject.clone()) {
            Ok(Some(seat)) => Ok(seat),
            Ok(None) => Err(Output::Refused(match subject {
                Subject::Named(flow_id) => Refused_Data::UnknownFlow(flow_id),
                Subject::Permaflow(address) => Refused_Data::UnknownAddress(address),
            })),
            Err(_) => Err(Output::Refused(Refused_Data::StoreRefused)),
        }
    }

    fn record_override(&self, transitioned: Transitioned) -> Result<bool, Output> {
        match self.perform(Operation::Record(Record_Data::Transitioned(transitioned))) {
            Outcome::Recorded => Ok(true),
            _ => Err(Output::Refused(Refused_Data::StoreRefused)),
        }
    }

    // A subject that names no seat, an Address no flow ever ran at, has no
    // transition to record.
    fn record_named(&self, subject: Subject) -> Result<bool, Output> {
        match self.store.seat(subject) {
            Ok(Some(seat)) => self.record_override(Transitioned {
                transition: self.state_now(seat.clone()).overridden(),
                seat,
            }),
            Ok(None) => Ok(false),
            Err(_) => Err(Output::Refused(Refused_Data::StoreRefused)),
        }
    }

    fn record_every_seat(&self) -> Result<bool, Output> {
        let seats = self
            .store
            .seats()
            .map_err(|_| Output::Refused(Refused_Data::StoreRefused))?;
        for seat in seats {
            self.record_override(Transitioned {
                transition: self.state_now(seat.clone()).overridden(),
                seat,
            })?;
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

impl meta_signal_flow::panel::Answering for MetaCall<'_> {
    fn set_state(&self, set_state: &SetState_Data) -> Result<StateSet_Data, Output> {
        let nexus = self.nexus;
        let _turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let seat = nexus.seat_named(Subject::Named(set_state.flow_id.clone()))?;
        nexus.record_override(Transitioned {
            seat,
            transition: set_state.state.overridden(),
        })?;
        Ok(StateSet_Data {
            flow_id: set_state.flow_id.clone(),
            state: set_state.state.clone(),
        })
    }

    // The flow is sent its handover warning now, whatever its context: the
    // words its model's handover policy configures, then the path its
    // crystallization is written at, as a MiddleAbrupt Notice from Flow,
    // reaching it at its next tool boundary.
    fn crystallize_now(&self, flow_id: &FlowId) -> Result<FlowId, Output> {
        use crate::handover_reminding::{HandoverReminderNotice, RendersHandoverWarning};
        use crate::store::context::NamesFlowModel;
        use crate::store::handover_configuration::ReadsHandoverConfiguration;
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        let nexus = self.nexus;
        let address = {
            let _turn = nexus
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let seat = nexus.seat_named(Subject::Named(flow_id.clone()))?;
            // A flow with no harness, ended or no longer its metaflow's,
            // has nobody to ask.
            let live = matches!(
                nexus.store.flow_node(&flow_id.0),
                Ok(Some(node)) if node.flow_lifecycle.is_live()
            );
            let awake = matches!(
                nexus.store.metaflow_awake_as(&flow_id.0),
                Ok(Some(metaflow)) if metaflow.address == seat.address
            );
            if !(live && awake) {
                return Err(Output::Refused(Refused_Data::Unharnessed(flow_id.clone())));
            }
            let model = nexus.store.flow_model(&flow_id.0).ok().flatten();
            let words = match nexus.store.handover_policies() {
                Ok(meta_signal_flow::HandoverPoliciesConfiguration::Configured(policies)) => {
                    policies
                        .into_iter()
                        .find(|policy| Some(&policy.model_name.0) == model.as_ref())
                        .map(|policy| policy.reminder_text.0)
                        .unwrap_or_default()
                }
                Ok(meta_signal_flow::HandoverPoliciesConfiguration::Unconfigured) => String::new(),
                Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
            };
            let warning = HandoverReminderNotice {
                address: seat.address.clone(),
                text: words,
                generation: 0,
            }
            .notice_text(&flow_id.0);
            if !matches!(
                nexus.store.accept_logical_delivery(
                    &seat.address,
                    signal_flow::SentBy::Flow,
                    flow_ethos::Delivery::Wake,
                    signal_flow::Message::MiddleAbrupt(signal_flow::Request::Notice(
                        warning.trim_start().to_owned()
                    )),
                    None,
                ),
                Ok(signal_flow::PrivateSendResponse::SendAccepted(_))
            ) {
                return Err(Output::Refused(Refused_Data::StoreRefused));
            }
            nexus.record_override(Transitioned {
                seat: seat.clone(),
                transition: State::Crystallizing.overridden(),
            })?;
            seat.address
        };
        // Handed over off the gate, as any queued MiddleAbrupt request is; a
        // held Nexus keeps it queued.
        nexus.drain_awake(&address);
        Ok(flow_id.clone())
    }

    fn renew(&self, address: &Address) -> Result<FlowId, Output> {
        use crate::store::address_hold::HoldsLaunchAddress;
        use crate::store::restart::RecordsRestarts;
        use crate::store::successor::SuccessionCause;
        use crate::waking::{HoldsWakingMetaflows, RestsEndedAwake, WakesInParts};
        let nexus = self.nexus;
        let unrenewed = |rejection| Err(Output::Refused(Refused_Data::Unrenewed(rejection)));
        let turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(lock) = nexus.waking.hold(address) else {
            return unrenewed(RefreshRejection::Locked);
        };
        nexus.rest_ended_awake(address);
        let metaflow = match nexus.store.metaflow(address) {
            Ok(Some(metaflow)) => metaflow,
            Ok(None) => {
                return Err(Output::Refused(Refused_Data::UnknownAddress(
                    address.clone(),
                )));
            }
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        };
        let flow_ethos::State::Awake(flow_id) = metaflow.state.clone() else {
            return unrenewed(RefreshRejection::NotAwake);
        };
        if !matches!(nexus.store.restart_record(&flow_id.0), Ok(None)) {
            return Err(Output::Refused(Refused_Data::Restarting(flow_id)));
        }
        match nexus.store.address_hold(&flow_id.0) {
            Ok(None) => {}
            Ok(Some(_)) => return unrenewed(RefreshRejection::SuccessorUnconfirmed),
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        }
        let node = match nexus.store.flow_node(&flow_id.0) {
            Ok(Some(node)) => node,
            Ok(None) => return Err(Output::Refused(Refused_Data::UnknownFlow(flow_id))),
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        };
        match nexus.herdr.pane_presence(&node) {
            PanePresence::Present => {}
            PanePresence::Absent | PanePresence::Bare => {
                let _ = nexus.perform(Operation::Record(Record_Data::Exited(flow_id)));
                return unrenewed(RefreshRejection::NotAwake);
            }
            PanePresence::Unknown => return unrenewed(RefreshRejection::HerdrUnavailable),
        }
        // The successor is started and confirmed first, then the old flow
        // is reaped, whatever the metaflow's continuity and whether or not
        // the old flow rests; the lock keeps any wake, drain or delivery of
        // the metaflow out meanwhile.
        drop(turn);
        let renewed = nexus.refresh_successor_first(
            &flow_id.0,
            &metaflow,
            SuccessionCause::Ordered(RefreshCaller::Elsewhere(signal_flow::Origin::Unbound)),
        );
        drop(lock);
        crate::send_tool::ServesLaunchCalls::wake_deferred(nexus);
        let successor = match renewed {
            AtStop::ReapPending(pending) => pending.launched.flow_id,
            AtStop::Refreshed(_) => match nexus.store.metaflow(address) {
                Ok(Some(metaflow)) => match metaflow.state {
                    flow_ethos::State::Awake(successor) => successor,
                    flow_ethos::State::Asleep => {
                        return unrenewed(RefreshRejection::PersistenceRefused);
                    }
                },
                _ => return Err(Output::Refused(Refused_Data::StoreRefused)),
            },
            AtStop::Kept(KeptAtStop::RefreshLaunchRefused(response)) => {
                return unrenewed(match *response {
                    signal_flow::Output::ReplaceRejected(rejection) => {
                        RefreshRejection::SuccessorRefused(rejection)
                    }
                    signal_flow::Output::StartRejected(rejection) => {
                        RefreshRejection::SuccessorRefused(
                            signal_flow::ReplaceRejection::LaunchRefused(rejection),
                        )
                    }
                    signal_flow::Output::LaunchPending(_)
                    | signal_flow::Output::StartAmbiguous(_) => {
                        RefreshRejection::SuccessorUnconfirmed
                    }
                    _ => RefreshRejection::PersistenceRefused,
                });
            }
            AtStop::Kept(KeptAtStop::RefreshUnplaceable) => {
                return unrenewed(RefreshRejection::Undeclared);
            }
            _ => return unrenewed(RefreshRejection::PersistenceRefused),
        };
        {
            let _turn = nexus
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            nexus.record_override(Transitioned {
                seat: Seat {
                    flow_id,
                    address: address.clone(),
                },
                transition: State::Asleep.overridden(),
            })?;
        }
        // What queued while the lock stood is handed to the fresh flow,
        // oldest first; a held Nexus keeps it queued.
        nexus.wake_due(address.clone());
        Ok(successor)
    }

    // Retire's act, on the owner's word: the flow's pane, still there
    // running its own session or none, is closed under its lease and the
    // flow recorded Retired; a flow already ended is closed already. A
    // restart that may still start a harness holds the flow.
    fn close(&self, flow_id: &FlowId) -> Result<FlowId, Output> {
        use crate::store::restart::RecordsRestarts;
        let nexus = self.nexus;
        let _turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let seat = nexus.seat_named(Subject::Named(flow_id.clone()))?;
        let node = match nexus.store.flow_node(&flow_id.0) {
            Ok(Some(node)) => node,
            Ok(None) => return Err(Output::Refused(Refused_Data::UnknownFlow(flow_id.clone()))),
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        };
        if node.flow_lifecycle.is_live() {
            match nexus.store.restart_record(&flow_id.0) {
                Ok(None) => {}
                Ok(Some(record))
                    if matches!(
                        record.restart_phase,
                        meta_signal_flow::RestartPhase::Failed(_)
                    ) => {}
                Ok(Some(_)) => {
                    return Err(Output::Refused(Refused_Data::Restarting(flow_id.clone())));
                }
                Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
            }
            if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection
                && matches!(
                    nexus.herdr.pane_presence(&node),
                    PanePresence::Present | PanePresence::Bare
                )
            {
                let closed = {
                    let _lease = nexus.pane_leases.hold(route);
                    nexus.perform(Operation::Close(node.clone()))
                };
                if closed != Outcome::Closed {
                    return Err(Output::Refused(Refused_Data::HerdrRefused(flow_id.clone())));
                }
            }
            if nexus.perform(Operation::Record(Record_Data::Retired(flow_id.clone())))
                != Outcome::Recorded
            {
                return Err(Output::Refused(Refused_Data::StoreRefused));
            }
        }
        nexus.record_override(Transitioned {
            seat,
            transition: State::Asleep.overridden(),
        })?;
        Ok(flow_id.clone())
    }

    // The ordinary DeliverAll: an Awake seat is handed its whole queue now,
    // an Asleep metaflow holding work is woken with it. A held Nexus hands
    // an Awake seat its queue, the owner asking, and wakes nothing: a wake
    // is a launch.
    fn deliver(&self, address: &Address) -> Result<Address, Output> {
        use crate::sending::SendsRequests;
        let nexus = self.nexus;
        match nexus.deliver_all(address.clone()) {
            signal_flow::Output::Drained(_) => {}
            signal_flow::Output::Refused(signal_flow::Refusal::Unknown(_)) => {
                return Err(Output::Refused(Refused_Data::UnknownAddress(
                    address.clone(),
                )));
            }
            _ => return Err(Output::Refused(Refused_Data::StoreRefused)),
        }
        let _turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        nexus.record_named(Subject::Permaflow(address.clone()))?;
        Ok(address.clone())
    }

    // The sender lock a permaflow holds as a recipient is taken away,
    // whoever holds it and whether or not it has lapsed.
    fn unlock(&self, address: &Address) -> Result<Address, Output> {
        let nexus = self.nexus;
        let _turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match nexus.store.metaflow(address) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(Output::Refused(Refused_Data::UnknownAddress(
                    address.clone(),
                )));
            }
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        }
        match nexus.store.stored_metaflow_lock(address) {
            Ok(Some(stored)) => {
                let now = std::time::SystemTime::now().seconds_since_epoch();
                if nexus.store.release_metaflow_lock(stored.lock, now).is_err() {
                    return Err(Output::Refused(Refused_Data::StoreRefused));
                }
            }
            Ok(None) => {}
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        }
        nexus.record_named(Subject::Permaflow(address.clone()))?;
        Ok(address.clone())
    }

    // A flow Flow holds live, or one its metaflow is Awake as, or one a
    // launch or a restart holds, has a harness and is not forgotten: it is
    // closed first.
    fn forget(&self, flow_id: &FlowId) -> Result<FlowId, Output> {
        use crate::store::address_hold::HoldsLaunchAddress;
        use crate::store::restart::RecordsRestarts;
        let nexus = self.nexus;
        let _turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let seat = nexus.seat_named(Subject::Named(flow_id.clone()))?;
        let node = match nexus.store.flow_node(&flow_id.0) {
            Ok(Some(node)) => node,
            Ok(None) => return Err(Output::Refused(Refused_Data::UnknownFlow(flow_id.clone()))),
            Err(_) => return Err(Output::Refused(Refused_Data::StoreRefused)),
        };
        let awake = matches!(nexus.store.metaflow_awake_as(&flow_id.0), Ok(Some(_)));
        if node.flow_lifecycle.is_live()
            || awake
            || !matches!(nexus.store.address_hold(&flow_id.0), Ok(None))
        {
            return Err(Output::Refused(Refused_Data::Harnessed(flow_id.clone())));
        }
        if !matches!(nexus.store.restart_record(&flow_id.0), Ok(None)) {
            return Err(Output::Refused(Refused_Data::Restarting(flow_id.clone())));
        }
        match nexus.perform(Operation::Record(Record_Data::Forgotten(Transitioned {
            seat,
            transition: State::Asleep.overridden(),
        }))) {
            Outcome::Recorded => Ok(flow_id.clone()),
            Outcome::Failed(crate::generated::operation::Failed_Data::UnknownFlow) => {
                Err(Output::Refused(Refused_Data::UnknownFlow(flow_id.clone())))
            }
            _ => Err(Output::Refused(Refused_Data::StoreRefused)),
        }
    }

    // The switch is set and every live seat's state recorded as it stands,
    // the cause Overridden.
    fn hold(&self) -> Result<(), Output> {
        let nexus = self.nexus;
        let _turn = nexus
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let moment = Moment(std::time::SystemTime::now().seconds_since_epoch());
        if nexus.perform(Operation::Record(Record_Data::Switched(Holding::Held(
            moment,
        )))) != Outcome::Recorded
        {
            return Err(Output::Refused(Refused_Data::StoreRefused));
        }
        eprintln!("flow-nexus: held by the owner; the Nexus takes no act of its own");
        nexus.record_every_seat().map(|_| ())
    }

    // The switch is moved back, every live seat's state recorded, and what
    // the Nexus owes is taken up now, as at its start: a restart, a
    // successor, a queue, a wake.
    fn resume(&self) -> Result<(), Output> {
        let nexus = self.nexus;
        {
            let _turn = nexus
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if nexus.perform(Operation::Record(Record_Data::Switched(Holding::Acting)))
                != Outcome::Recorded
            {
                return Err(Output::Refused(Refused_Data::StoreRefused));
            }
            eprintln!("flow-nexus: resumed by the owner; the Nexus acts on its own again");
            nexus.record_every_seat()?;
        }
        nexus.recover_wakes();
        Ok(())
    }
}
