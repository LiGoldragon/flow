//! Takeover observes the native source before the Nexus serves. A later
//! source-availability announcement retries only a seat without a record.
use crate::RunningNexus;
use crate::claude::{ReadsClaudeSessions, ReadsClaudeTurns};
use crate::codex::{ReadsThreadStanding, SelectsCodexEndpoint};
use crate::generated::memory::SeatStanding;
use crate::live::HoldsLiveValue;
use crate::store::current_seat::CurrentSeatRead;
use crate::store::current_seat::RecordsCurrentSeats;
use crate::store::{ManagesMetaflowState, ReadsFlowRoles, ReadsFlowRows};
use crate::waking::ReadsClaudeDelivery;
use signal_flow::{FlowNode, HarnessKind};

pub trait ObservesCurrentSeats {
    fn observe_takeover(&self) -> Result<(), crate::store::StoreError>;
    fn observe_returning_source(&self, node: &FlowNode);
}

impl ObservesCurrentSeats for RunningNexus {
    fn observe_takeover(&self) -> Result<(), crate::store::StoreError> {
        // Cut addresses cannot regain launch authority as a side effect
        // of takeover. Leave their queues and audit intact.
        use crate::generated::overriding::ParksHeldLaunches;
        use crate::store::fresh_launch::TracksFreshLaunches;
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        use crate::store::ordered_replacement::OrdersReplacements;
        use crate::store::successor::OwesSuccessors;
        use crate::store::{ReadsLaunchAttempt, RecordsLaunchOutcome};
        for held in self.store.held_launches()? {
            if let Some(attempt) = self.store.launch_attempt(&held.launch_request_id.0)?
                && self
                    .store
                    .metaflow_declaration(&attempt.launch_profile.address)?
                    .is_none()
                && attempt.launch_attempt_phase == signal_flow::LaunchAttemptPhase::Reserved
            {
                self.store.record_launch_outcome(
                    &held.launch_request_id.0,
                    crate::store::LaunchOutcome::StartRejected(
                        signal_flow::StartRejection::Undeclared,
                    ),
                )?;
                // Reopen could not release this prefix while its parked
                // attempt still owned it. The terminal pre-input refusal
                // now proves that these exact requests remain retryable.
                if let Some(claim) = self.store.fresh_cohort(&attempt.launch_profile.address)?
                    && claim.launch_request_id == held.launch_request_id
                {
                    self.store.reconcile_fresh(&claim)?;
                }
            }
        }
        for intent in self.store.successor_intents()? {
            if self.store.metaflow_declaration(&intent.address)?.is_none() {
                self.store.forget_successor(&intent.address)?;
            }
        }
        for order in self.store.ordered_replacements()? {
            if self.store.metaflow_declaration(&order.address)?.is_none() {
                self.store.forget_ordered_replacement(&order.address)?;
            }
        }
        for seat in self.store.list_metaflows()? {
            if let flow_ethos::State::Awake(id) = seat.state
                && let Some(node) = self.store.flow_node(&id.0)?
            {
                self.store
                    .record_observation(&id.0, self.read_native_standing(&node))?;
            }
        }
        Ok(())
    }

    fn observe_returning_source(&self, node: &FlowNode) {
        if matches!(
            self.store.current_seat(&node.flow_id.0),
            Ok(CurrentSeatRead::NoRecord)
        ) {
            if node.flow_lifecycle != signal_flow::FlowLifecycle::Active {
                return;
            }
            if let Some(observed) = self.read_native_standing(node) {
                match self.store.establish_observation(&node.flow_id.0, observed) {
                    // A committed Alive row already names exactly one
                    // retry through the recipient dispatcher.
                    Ok(_) => {}
                    Err(error) => {
                        eprintln!("flow-nexus: failed to record returning source: {error}")
                    }
                }
            }
        }
    }
}

pub(crate) trait ReadsNativeStanding {
    fn read_native_standing(&self, node: &FlowNode) -> Option<SeatStanding>;
}
impl ReadsNativeStanding for RunningNexus {
    fn read_native_standing(&self, node: &FlowNode) -> Option<SeatStanding> {
        use SeatStanding::{Alive, Ended};
        match node.harness_kind {
            HarnessKind::Codex => {
                let role = self.store.role(&node.flow_id.0).ok()??;
                self.codex_endpoints
                    .current()
                    .adapter_for(&role.model_name.0)
                    .ok()?
                    .thread_standing(&node.session_id.0)
                    .ok()
            }
            HarnessKind::Claude => match self
                .herdr
                .claude_sessions
                .session_is_live(&node.session_id.0)?
            {
                false => Some(Ended),
                true => self
                    .claude_transcript(node)?
                    .main_turn_activity(&node.session_id.0)
                    .map(Alive),
            },
        }
    }
}
