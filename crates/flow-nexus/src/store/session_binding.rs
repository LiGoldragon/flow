//! Meta `Bind`'s one write: a native Claude session the owner names by its
//! transcript becomes the flow its Address is Awake as.
use super::current_seat::{RecordsCurrentSeats, SeatEvent};
use super::{FlowStore, ReadsFlowStore, StoreError, StoredMetaflow};
use crate::generated::memory::{
    Cause, Flow, FlowHerdrRouteRecord, FlowRecord, Generation, Moment, SeatActivity, SeatStanding,
    Transition,
};
use crate::reminding::CountsUnixSeconds;
use crate::store::{AnnouncesLaunchChanges, ManagesMetaflowState, NamesLiveFlow};
use flow_ethos::State as MetaflowState;
use sema_engine::{QueryPlan, RecordKey};
use signal_flow::{
    Address, EndpointSelection, FlowId, FlowLifecycle, HarnessKind, HerdrRoute, Metaflow, Origin,
    SessionId,
};

/// What a Bind writes: the Address, the flow its session names, the
/// session, the Herdr route of the pane showing it if one does, and the
/// seat standing its Harness Nexus tells now if it tells one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SessionBinding {
    pub address: Address,
    pub flow_id: FlowId,
    pub session: String,
    pub route: Option<HerdrRoute>,
    pub standing: Option<SeatStanding>,
}

/// Why a Bind was not written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SessionBindingRefusal {
    /// A flow of that id is bound to another native session.
    OtherSession,
}

pub(crate) trait BindsNativeSessions {
    /// In one commit: the flow of the binding's session, harness Claude,
    /// created or reused and Active; its route when one is shown; the
    /// Address Awake as it, the flow it was Awake as before retired in the
    /// store (its pane untouched) and kept in Past; its seat row seeded
    /// from the standing told, with an override transition. The flow it
    /// replaced, None when the Address was Asleep or Awake as this flow.
    fn bind_native_session(
        &self,
        binding: SessionBinding,
    ) -> Result<Result<Option<FlowId>, SessionBindingRefusal>, StoreError>;
}

impl BindsNativeSessions for FlowStore {
    fn bind_native_session(
        &self,
        binding: SessionBinding,
    ) -> Result<Result<Option<FlowId>, SessionBindingRefusal>, StoreError> {
        let SessionBinding {
            address,
            flow_id,
            session,
            route,
            standing,
        } = binding;
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let existing_flow = self.flow(&flow_id.0)?;
        if let Some(existing) = &existing_flow
            && (existing.harness_kind != HarnessKind::Claude
                || existing
                    .session_id_option
                    .as_ref()
                    .map(|value| value.0.as_str())
                    != Some(session.as_str()))
        {
            return Ok(Err(SessionBindingRefusal::OtherSession));
        }
        let existing_metaflow = self.stored_metaflow(&address)?;
        let mut metaflow = existing_metaflow
            .as_ref()
            .map(|stored| stored.metaflow.clone())
            .unwrap_or(Metaflow {
                address: address.clone(),
                state: MetaflowState::Asleep,
                past: flow_ethos::Past(Vec::new()),
                queue: flow_ethos::Queue(Vec::new()),
            });
        let replaced = match &metaflow.state {
            MetaflowState::Awake(current) if *current != flow_id => Some(current.clone()),
            _ => None,
        };
        if let Some(previous) = &replaced {
            metaflow.past.0.push(previous.clone());
        }
        metaflow.past.0.retain(|past| past != &flow_id);
        metaflow.state = MetaflowState::Awake(flow_id.clone());
        let flow_record = FlowRecord {
            flow_id: flow_id.clone(),
            flow_type: existing_flow
                .as_ref()
                .map_or(signal_flow::FlowType("claude-bound".into()), |flow| {
                    flow.flow_type.clone()
                }),
            origin: existing_flow
                .as_ref()
                .map_or(Origin::Unbound, |flow| flow.origin.clone()),
            session_id_option: Some(SessionId(session.clone())),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: existing_flow
                .as_ref()
                .map_or(EndpointSelection::Unavailable, |flow| {
                    flow.endpoint_selection.clone()
                }),
            flow_lifecycle: FlowLifecycle::Active,
            generation: existing_flow
                .as_ref()
                .map_or(Generation(1), |flow| flow.generation),
        };
        let mut commit = self.engine.begin_atomic_commit();
        commit = match existing_flow {
            Some(_) => commit.mutate(self.flows, flow_record),
            None => commit.assert(self.flows, flow_record),
        };
        if let Some(route) = route {
            let record = FlowHerdrRouteRecord {
                flow_id: flow_id.clone(),
                herdr_route: route,
            };
            commit = match self.herdr_route(&flow_id.0)? {
                Some(_) => commit.mutate(self.herdr_routes, record),
                None => commit.assert(self.herdr_routes, record),
            };
        }
        if let Some(previous) = &replaced
            && let Some(mut old) = self.flow(&previous.0)?
            && old.flow_lifecycle.is_live()
        {
            old.flow_lifecycle = FlowLifecycle::Retired;
            commit = commit.mutate(self.flows, old);
        }
        commit = match existing_metaflow {
            Some(_) => commit.mutate(self.metaflows, StoredMetaflow { metaflow }),
            None => commit.assert(self.metaflows, StoredMetaflow { metaflow }),
        };
        if let Some(standing) = standing {
            commit = self.stage_current_event(
                commit,
                &flow_id.0,
                SeatEvent::NewIncarnation(standing.clone()),
            )?;
            let transition = Transition {
                state: match standing {
                    SeatStanding::Alive(SeatActivity::Working) => meta_signal_flow::State::Acting,
                    SeatStanding::Alive(SeatActivity::Resting) => meta_signal_flow::State::Resting,
                    SeatStanding::Ended => meta_signal_flow::State::Asleep,
                },
                cause: Cause::Overridden,
                moment: Moment(std::time::SystemTime::now().seconds_since_epoch()),
            };
            let held = self
                .engine
                .match_records(QueryPlan::key(
                    self.flow_transitions,
                    RecordKey::new(flow_id.0.clone()),
                ))?
                .records()
                .first()
                .cloned();
            commit = match held {
                Some(mut flow) => {
                    flow.address = address.clone();
                    flow.transition_vector.push(transition);
                    commit.mutate(self.flow_transitions, flow)
                }
                None => commit.assert(
                    self.flow_transitions,
                    Flow {
                        flow_id: flow_id.clone(),
                        address: address.clone(),
                        transition_vector: vec![transition],
                    },
                ),
            };
        }
        commit = super::pending_reap::ReassignsPendingReaps::reaps_reassigned(
            self,
            &address,
            Some(&flow_id),
            commit,
        )?
        .commit;
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(Ok(replaced))
    }
}
