//! The values the starter's capabilities read, each yielded through the
//! Ethos trait that names it: a capability takes the trait, and these
//! concrete types implement it. A shared reference yields what its referent
//! does.

use crate::caller::CallerProcess;
use crate::generated::curriculum::{
    ContextProvenance, Query as CurriculumQuery, StartupComposition,
};
use crate::generated::curriculum::{RoleComposition, RoleSelection};
use crate::generated::operation::Outcome;
use crate::generated::role::{
    Addressed, AgentCall, AgentCaller, AgentCalling, ResolvesAgentCaller, RoleComposing,
    RoleNaming, RoleSelecting,
};
use crate::generated::starter::{
    CallerProcessing, ContextRequest, ContextRequesting, CurriculumQuerying, DeliveryIntended,
    HarnessedProfile, JsonRow, LaunchedRecord, LaunchedRecording, LeaveOutListing, OutcomeCarrying,
    PaneBound, PartNumbering, Profiled, Progressing, ProvenanceListing, SegmentListing,
    StartupComposing, StartupProgress, SystemPrompted, Textual,
};
use signal_flow::{
    Address, HarnessKind, HerdrPaneBinding, LaunchProfile, LeaveOut, PromptDeliveryIntent,
    SystemPrompt,
};

impl Textual for str {
    fn text(&self) -> String {
        self.to_owned()
    }
}

impl Textual for String {
    fn text(&self) -> String {
        self.clone()
    }
}

impl<T: Textual + ?Sized> Textual for &T {
    fn text(&self) -> String {
        (**self).text()
    }
}

impl Profiled for HarnessedProfile {
    fn profile(&self) -> LaunchProfile {
        self.launch_profile.clone()
    }

    fn harness_kind(&self) -> HarnessKind {
        self.harness_kind.clone()
    }
}

impl<T: Profiled + ?Sized> Profiled for &T {
    fn profile(&self) -> LaunchProfile {
        (**self).profile()
    }

    fn harness_kind(&self) -> HarnessKind {
        (**self).harness_kind()
    }
}

impl LeaveOutListing for [LeaveOut] {
    fn leave_outs(&self) -> Vec<LeaveOut> {
        self.to_vec()
    }
}

impl LeaveOutListing for Vec<LeaveOut> {
    fn leave_outs(&self) -> Vec<LeaveOut> {
        self.clone()
    }
}

impl<T: LeaveOutListing + ?Sized> LeaveOutListing for &T {
    fn leave_outs(&self) -> Vec<LeaveOut> {
        (**self).leave_outs()
    }
}

impl ContextRequesting for ContextRequest {
    fn context_request(&self) -> ContextRequest {
        self.clone()
    }
}

impl<T: ContextRequesting + ?Sized> ContextRequesting for &T {
    fn context_request(&self) -> ContextRequest {
        (**self).context_request()
    }
}

impl StartupComposing for StartupComposition {
    fn composition(&self) -> StartupComposition {
        self.clone()
    }
}

impl<T: StartupComposing + ?Sized> StartupComposing for &T {
    fn composition(&self) -> StartupComposition {
        (**self).composition()
    }
}

impl CurriculumQuerying for CurriculumQuery {
    fn query(&self) -> CurriculumQuery {
        self.clone()
    }
}

impl<T: CurriculumQuerying + ?Sized> CurriculumQuerying for &T {
    fn query(&self) -> CurriculumQuery {
        (**self).query()
    }
}

impl ProvenanceListing for [ContextProvenance] {
    fn provenance(&self) -> Vec<ContextProvenance> {
        self.to_vec()
    }
}

impl ProvenanceListing for Vec<ContextProvenance> {
    fn provenance(&self) -> Vec<ContextProvenance> {
        self.clone()
    }
}

impl<T: ProvenanceListing + ?Sized> ProvenanceListing for &T {
    fn provenance(&self) -> Vec<ContextProvenance> {
        (**self).provenance()
    }
}

impl SegmentListing for [String] {
    fn segments(&self) -> Vec<String> {
        self.to_vec()
    }
}

impl SegmentListing for Vec<String> {
    fn segments(&self) -> Vec<String> {
        self.clone()
    }
}

impl<T: SegmentListing + ?Sized> SegmentListing for &T {
    fn segments(&self) -> Vec<String> {
        (**self).segments()
    }
}

/// A part's number from one and the count of parts.
impl PartNumbering for (usize, usize) {
    fn part_number(&self) -> i64 {
        self.0 as i64
    }

    fn part_count(&self) -> i64 {
        self.1 as i64
    }
}

impl SystemPrompted for SystemPrompt {
    fn system_prompt(&self) -> SystemPrompt {
        self.clone()
    }
}

impl<T: SystemPrompted + ?Sized> SystemPrompted for &T {
    fn system_prompt(&self) -> SystemPrompt {
        (**self).system_prompt()
    }
}

impl PaneBound for HerdrPaneBinding {
    fn pane(&self) -> HerdrPaneBinding {
        self.clone()
    }
}

impl<T: PaneBound + ?Sized> PaneBound for &T {
    fn pane(&self) -> HerdrPaneBinding {
        (**self).pane()
    }
}

impl JsonRow for serde_json::Value {
    fn row(&self) -> serde_json::Value {
        self.clone()
    }
}

impl<T: JsonRow + ?Sized> JsonRow for &T {
    fn row(&self) -> serde_json::Value {
        (**self).row()
    }
}

impl DeliveryIntended for PromptDeliveryIntent {
    fn intent(&self) -> PromptDeliveryIntent {
        self.clone()
    }
}

impl<T: DeliveryIntended + ?Sized> DeliveryIntended for &T {
    fn intent(&self) -> PromptDeliveryIntent {
        (**self).intent()
    }
}

impl Progressing for StartupProgress {
    fn progress(&self) -> StartupProgress {
        self.clone()
    }
}

impl<T: Progressing + ?Sized> Progressing for &T {
    fn progress(&self) -> StartupProgress {
        (**self).progress()
    }
}

impl OutcomeCarrying for Outcome {
    fn outcome(&self) -> Outcome {
        self.clone()
    }
}

impl<T: OutcomeCarrying + ?Sized> OutcomeCarrying for &T {
    fn outcome(&self) -> Outcome {
        (**self).outcome()
    }
}

impl LaunchedRecording for LaunchedRecord {
    fn launched_record(&self) -> LaunchedRecord {
        self.clone()
    }
}

impl<T: LaunchedRecording + ?Sized> LaunchedRecording for &T {
    fn launched_record(&self) -> LaunchedRecord {
        (**self).launched_record()
    }
}

impl CallerProcessing for Option<CallerProcess> {
    fn caller_process(&self) -> Option<CallerProcess> {
        *self
    }
}

impl<T: CallerProcessing + ?Sized> CallerProcessing for &T {
    fn caller_process(&self) -> Option<CallerProcess> {
        (**self).caller_process()
    }
}

impl<T: crate::generated::starter::ReadsVerifiedContext + ?Sized>
    crate::generated::starter::ReadsVerifiedContext for &T
{
    fn address(&self) -> signal_flow::Address {
        (**self).address()
    }

    fn system_text(&self) -> String {
        (**self).system_text()
    }

    fn user_segments(&self) -> Vec<String> {
        (**self).user_segments()
    }
}

impl RoleSelecting for RoleSelection {
    fn role_selection(&self) -> RoleSelection {
        self.clone()
    }
}

impl<T: RoleSelecting + ?Sized> RoleSelecting for &T {
    fn role_selection(&self) -> RoleSelection {
        (**self).role_selection()
    }
}

impl RoleComposing for RoleComposition {
    fn role_composition(&self) -> RoleComposition {
        self.clone()
    }
}

impl<T: RoleComposing + ?Sized> RoleComposing for &T {
    fn role_composition(&self) -> RoleComposition {
        (**self).role_composition()
    }
}

impl Addressed for Address {
    fn addressed(&self) -> Address {
        self.clone()
    }
}

impl<T: Addressed + ?Sized> Addressed for &T {
    fn addressed(&self) -> Address {
        (**self).addressed()
    }
}

impl AgentCalling for AgentCall {
    fn agent_call(&self) -> AgentCall {
        self.clone()
    }
}

impl<T: AgentCalling + ?Sized> AgentCalling for &T {
    fn agent_call(&self) -> AgentCall {
        (**self).agent_call()
    }
}

impl ResolvesAgentCaller for AgentCaller {
    fn agent_caller(&self) -> AgentCaller {
        self.clone()
    }
}

impl<T: ResolvesAgentCaller + ?Sized> ResolvesAgentCaller for &T {
    fn agent_caller(&self) -> AgentCaller {
        (**self).agent_caller()
    }
}

impl RoleNaming for [String] {
    fn role_names(&self) -> Vec<String> {
        self.to_vec()
    }
}

impl RoleNaming for Vec<String> {
    fn role_names(&self) -> Vec<String> {
        self.clone()
    }
}

impl<T: RoleNaming + ?Sized> RoleNaming for &T {
    fn role_names(&self) -> Vec<String> {
        (**self).role_names()
    }
}
