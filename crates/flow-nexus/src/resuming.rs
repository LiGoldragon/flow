//! A wake that resumes: an Asleep metaflow whose last flow's prompt cache
//! is still warm, and whose context is below the ceiling its model is
//! configured with (`Configure.Resumptions`), wakes as that same flow
//! rather than a fresh one.
//!
//! The judgment reads what Flow recorded of the last flow through its
//! hooks: the last turn end recorded for its Address, which must be that
//! flow's own, and the context size its session carried into its last
//! turn ([`JudgesResumption`]). A model with no resumption configured, a
//! flow with no turn end or no size recorded, a last turn ended
//! CacheWarmth minutes ago or longer, or a context at or above
//! ResumeCeiling wakes fresh. So does a flow an owner retired, a
//! Simplified run, and a flow Flow holds no native session for.
//!
//! A resume takes the steps a restart's resume takes, with no old seat to
//! close: the launch composed from the context a fresh wake would compose,
//! on the flow's own model, with no prompt; a new pane opened; the harness
//! started on the session the flow holds, `--resume <session>` for Claude
//! and `resume <thread>` for a Codex TUI; the pane verified as the flow's
//! own seat. One write then binds it (`Record.Resumed`): the flow Active
//! again under its own FlowId, its route and SeatBirth the new seat's, its
//! Address Awake as it. Its queue is then handed to it by the drain that
//! follows every wake. A resume that does not bind discards the pane it
//! opened and answers its failure, and the wake starts a fresh flow.

use crate::RunningNexus;
use crate::generated::operation::{
    Failed_Data, Open_Data, Operation, Outcome, PaneLaunch, Record_Data, Record_Data_Resumed_Data,
};
use crate::performing::Performs;
use crate::reminding::CountsUnixSeconds;
use crate::restarting::{ResumedFlow, VerifiesResumedSeat};
use crate::store::context::{JudgesResumption, NamesFlowModel, ResumptionStanding};
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::waking::{FreshContext, ResumeContext};
use meta_signal_flow::RestartFailure;
use signal_flow::{FlowId, FlowLifecycle, Metaflow};

/// Whether, and as what, an Asleep metaflow's last flow resumes.
pub(crate) trait JudgesWakeResumption {
    /// The resumption of `metaflow`'s last flow, when it resumes; None when
    /// the wake is fresh. Logs why a configured resumption was not taken.
    fn resumption(&self, metaflow: &Metaflow, fresh: &FreshContext) -> Option<ResumeContext>;
}

impl JudgesWakeResumption for RunningNexus {
    fn resumption(&self, metaflow: &Metaflow, fresh: &FreshContext) -> Option<ResumeContext> {
        let flow_id = metaflow.past.0.last()?;
        let now = std::time::SystemTime::now().seconds_since_epoch();
        let standing = match self
            .store
            .resumption_standing(&metaflow.address, &flow_id.0, now)
        {
            Ok(standing) => standing,
            Err(error) => {
                eprintln!(
                    "flow-nexus: flow {} is not judged for a resumption: {error}",
                    flow_id.0
                );
                return None;
            }
        };
        let address = &metaflow.address;
        let fresh_because = |reason: String| {
            eprintln!(
                "flow-nexus: wake of {:?} {} {:?} starts fresh, flow {} not resumed: {reason}",
                address.aspect, address.topic.0, address.model, flow_id.0
            );
        };
        match standing {
            ResumptionStanding::Resumable => {}
            ResumptionStanding::Unconfigured => return None,
            other => {
                fresh_because(format!("{other:?}"));
                return None;
            }
        }
        if fresh.flow_kind == signal_flow::FlowKind::Simplified {
            fresh_because("a Simplified run".into());
            return None;
        }
        let node = match self.store.flow_node(&flow_id.0) {
            Ok(Some(node)) => node,
            _ => {
                fresh_because("its node is unread".into());
                return None;
            }
        };
        // A flow Flow or its pane ended resumes; one an owner retired, or
        // one still live, does not.
        if node.flow_lifecycle.is_live() || node.flow_lifecycle == FlowLifecycle::Retired {
            fresh_because(format!("{:?}", node.flow_lifecycle));
            return None;
        }
        if node.session_id.0.is_empty() {
            fresh_because("no native session".into());
            return None;
        }
        let model_name = match self.store.flow_model(&flow_id.0) {
            Ok(Some(model_name)) => model_name,
            _ => {
                fresh_because("its model is unknown".into());
                return None;
            }
        };
        Some(ResumeContext {
            flow_id: flow_id.clone(),
            native_session_id: signal_flow::NativeSessionId(node.session_id.0),
            harness_kind: node.harness_kind,
            model_name,
            fresh: fresh.clone(),
        })
    }
}

/// The resume a wake takes for an Asleep metaflow's last flow.
pub(crate) trait ResumesLastFlow {
    /// Resumes the flow `resume` names at `metaflow`'s Address and binds it
    /// Awake: its FlowId; or, the pane it opened discarded, why it did not.
    /// Runs off the dispatch gate and takes it for each store step.
    fn resume_last_flow(
        &self,
        metaflow: &Metaflow,
        resume: &ResumeContext,
    ) -> Result<FlowId, RestartFailure>;
}

impl ResumesLastFlow for RunningNexus {
    fn resume_last_flow(
        &self,
        metaflow: &Metaflow,
        resume: &ResumeContext,
    ) -> Result<FlowId, RestartFailure> {
        use crate::herdr::launch::HoldsSystemPrompt;
        use crate::launching::{ReadsHarnessArguments, ResolvesLaunchRoot};
        let refused = |category: &str| RestartFailure::ResumeRefused(category.into());
        let address = &metaflow.address;
        let fresh = &resume.fresh;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        // The launch a fresh wake would compose, on the flow's own model,
        // with no prompt: the session already holds its context.
        let profile = signal_flow::LaunchProfile {
            launch_request_id: signal_flow::LaunchRequestId(format!(
                "resume-{}-{nanos}",
                resume.flow_id.0
            )),
            system_modules: fresh.system_modules.clone(),
            prompt_modules: fresh.prompt_modules.clone(),
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
            address: address.clone(),
            model_name: signal_flow::ModelName(resume.model_name.clone()),
            effort: signal_flow::Effort(
                <Self as crate::sending::ChoosesEffort>::effort_of(address).into(),
            ),
            flow_id_option: None,
            herdr_session_name: fresh.herdr_session_name.clone(),
            leave_outs_option: fresh.leave_outs_option.clone(),
            flow_kind: fresh.flow_kind.clone(),
            tool_set: fresh.tool_set.clone(),
            instruction_prompt: signal_flow::InstructionPrompt(String::new()),
        };
        let (launch, root, harness_arguments, pane) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let launch = match self.perform(Operation::Compose(profile)) {
                Outcome::Composed(launch) => launch,
                _ => return Err(refused("NotComposed")),
            };
            // The harness the flow ran is the one it resumes on.
            if launch.harness_kind != resume.harness_kind {
                return Err(refused("HarnessChanged"));
            }
            let root = self
                .launch_root(address)
                .map_err(|_| refused("RootRefused"))?;
            let harness_arguments = self
                .harness_arguments(address)
                .map_err(|_| refused("MalformedHarnessFlags"))?;
            let pane = match self.perform(Operation::Open(Open_Data {
                composed_launch: launch.clone(),
                root: root.clone(),
            })) {
                Outcome::Opened(pane) => pane,
                _ => return Err(refused("Open")),
            };
            (launch, root, harness_arguments, pane)
        };
        let model = launch.launch_profile.model_name.clone();
        let request = launch.launch_profile.launch_request_id.clone();
        let resumed = ResumedFlow {
            flow_id: &resume.flow_id,
            address,
            native_session_id: &resume.native_session_id,
        };
        let verified = match self.perform(Operation::Resume(PaneLaunch {
            composed_launch: launch,
            root,
            herdr_pane_binding: pane.clone(),
            flow_id_option: Some(resume.flow_id.clone()),
            native_session_id_option: Some(resume.native_session_id.clone()),
            harness_arguments: crate::generated::operation::HarnessArguments(harness_arguments),
        })) {
            Outcome::Spawned(_) => self.verified_resumed_seat(&resumed, &pane, &model.0),
            Outcome::Failed(Failed_Data::SpawnRefused(_)) => Err(refused("SpawnRefused")),
            _ => Err(refused("Spawn")),
        };
        self.herdr.release_system_prompt(&request.0);
        let recorded = verified.and_then(|birth| {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match self.perform(Operation::Record(Record_Data::Resumed(
                Record_Data_Resumed_Data {
                    address: address.clone(),
                    seat_birth: birth,
                },
            ))) {
                Outcome::Recorded => Ok(resume.flow_id.clone()),
                _ => Err(refused("NotRecorded")),
            }
        });
        if let Err(failure) = &recorded {
            // The pane is the resume's own; a flow that did not bind leaves
            // it to no one.
            if self.perform(Operation::Discard(pane.clone())) != Outcome::Closed {
                eprintln!(
                    "flow-nexus: the pane {} a resume of flow {} opened is not shown closed: {failure:?}",
                    pane.herdr_pane_id.0, resume.flow_id.0
                );
            }
        }
        recorded
    }
}
