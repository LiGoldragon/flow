//! Launch settlement: the Start path, Replace, LaunchStatus and the
//! Observe.Launch subscription, which all answer from one durable account of
//! a launch request — its attempt, its outcome, and whether it replaces a
//! predecessor.

use crate::HoldsStopRequest;
use crate::delivery::lease::LeasesPanes;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, PaneLaunch, Record_Data, Record_Data_Settled_Data,
    Register_Data, Reserve_Data, Reserved_Data, Submit_Data, Title_Data,
};
use crate::generated::starter::{
    DeliveryIntended, HarnessedProfile, OutcomeCarrying, Profiled, Progressing,
    SettlesStartupProgress, Textual,
};
use crate::herdr::ReadsHerdrPanes;
use crate::herdr::launch::{
    ChecksComposedLaunch, ObservesNativeLaunchBinding, StartupProgress, TranscriptRefusal,
    WaitsForPaneRest,
};
use crate::performing::ClassifiesNativeLaunchFailure;
use crate::performing::Performs;
use crate::send_tool::ServesLaunchCalls;
use crate::store::NamesLiveFlow;
use crate::store::ReadsLaunchInspectionRecords;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::TakesLaunchAddress;
use crate::store::opened_thread::RecordsOpenedThreads;
use crate::store::{
    AnnouncesLaunchChanges, ReadsCreatedLaunchPane, RecordsCreatedLaunchPane,
    RecordsNativeLaunchBinding,
};
use crate::store::{AnswersLaunchOutcome, LaunchedRecord, ReplacedRecord};
use crate::waking::{RestsEndedAwake, SettlesStopsAtConfirmation, WakePolicy};
use crate::{
    RunningNexus,
    herdr::{
        LocatesNativeTranscripts, PaneOwnership, PanePresence,
        launch::{AcceptsLaunchRegistration, ObservesNativeTargetReceipt},
    },
    store::{
        LaunchChanges, LaunchInspectionRecords, LaunchOutcome, ReadsFlowRows, ReadsLaunchAttempt,
        RecordsLaunchOutcome, RecordsReplacement, Replacement, StoreError,
    },
};
use notify::Watcher;
use signal_flow::{
    BindingRefusal, ComposedLaunch, ContextRefusal, EndpointSelection, Event, FlowLifecycle,
    FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection, LaunchAttempt, LaunchAttemptPhase,
    LaunchAttemptReservation, LaunchInspection, LaunchProfile, LaunchStatusRejection, Launched,
    NativeLaunchFailure, NativeLaunchIntent, NativeLaunchRefusal, NativeLaunchStage, Origin,
    PromptDeliveryIntent, PromptDeliveryResult, RegistrationAcknowledgement, RegistrationRefusal,
    ReplaceRejection, Replaced, Response, StartRejection, StopRejection,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// The launch verbs of the running Nexus. `start` is the one Start path;
/// `settle` records what a launch came to and, for a replacement, stops and
/// reaps the predecessor before the successor becomes routable.
pub trait LaunchesFlows {
    /// Runs or resumes the launch a profile names, for its origin.
    fn start(&self, profile: LaunchProfile, origin: Origin) -> Response;
    /// Looks again at the native transcript of a launch whose first prompt
    /// was not yet seen, promoting it when the receipt is there.
    fn promote_ambiguous(&self, attempt: LaunchAttempt) -> Response;
    /// The recorded outcome of a request already settled, or None.
    fn settled(&self, profile: &LaunchProfile, origin: &Origin) -> Option<Response>;
    /// Records what a Start-path response settles, completing a replacement.
    fn settle(&self, launch_request_id: &str, response: Response) -> Response;
    /// Starts the successor through the Start path, then replaces.
    fn replace(&self, profile: LaunchProfile, origin: Origin) -> Response;
    /// Stops the predecessor, then closes its pane; only then is the
    /// successor routable.
    fn reap(&self, replacement: Replacement, launched: Launched) -> Response;
    /// Answers once: the outcome, else the pending attempt.
    fn launch_status(&self, launch_request_id: &str) -> Response;
    /// Everything a new launch does after Reserve, from the native intent
    /// to the confirmed receipt.
    fn launch_reserved(
        &self,
        launch: ComposedLaunch,
        origin: Origin,
        reserved: Option<String>,
    ) -> Response;
}

impl LaunchesFlows for RunningNexus {
    fn start(&self, profile: LaunchProfile, origin: Origin) -> Response {
        let launch_request_id = profile.launch_request_id.clone();
        let existing = match self.store.launch_attempt(&launch_request_id) {
            Ok(existing) => existing,
            Err(_) => {
                return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
            }
        };
        if let Some(attempt) = existing {
            if attempt.launch_profile != profile || attempt.origin != origin {
                return Response::StartRejected(StartRejection::LaunchRequestConflict);
            }
            if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptObserved {
                let Some(binding) = attempt.native_launch_binding_option else {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                };
                return self.confirmed(&binding.flow_id);
            }
            if attempt.launch_attempt_phase == LaunchAttemptPhase::NativeLaunchIntentRecorded
                && attempt
                    .native_launch_intent_option
                    .as_ref()
                    .is_some_and(|intent| intent.harness_kind == HarnessKind::Codex)
            {
                let launch = match self.perform(Operation::Compose(attempt.launch_profile.clone()))
                {
                    Outcome::Composed(launch) => launch,
                    outcome => return Self::composition_refused(outcome),
                };
                let pane = match self.store.recoverable_launch_pane(&launch_request_id) {
                    Ok(Some(pane)) => pane,
                    _ => return Response::LaunchPending(attempt),
                };
                // The thread Spawn opened and recorded is the one the pane's
                // TUI attached to; a remote-resumed TUI reports no session.
                let observed = match self.store.opened_thread(&launch_request_id) {
                    Ok(Some(thread)) => self
                        .herdr
                        .observe_attached_codex_binding(&launch, &pane, &thread),
                    Ok(None) => self.herdr.observe_native_binding(&launch, &pane, None),
                    Err(_) => {
                        return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                    }
                };
                let binding = match observed {
                    Ok(binding) => binding,
                    Err(error) => {
                        let _ = self.store.orphan_launch_pane(&launch_request_id);
                        return Response::StartRejected(StartRejection::NativeLaunchRefused(
                            NativeLaunchRefusal {
                                native_launch_stage: NativeLaunchStage::Bind,
                                native_launch_failure: NativeLaunchFailure::from_adapter_error(
                                    &error,
                                ),
                            },
                        ));
                    }
                };
                if !self
                    .store
                    .record_native_launch_binding(binding)
                    .unwrap_or(false)
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                return self
                    .store
                    .launch_attempt(&launch_request_id)
                    .ok()
                    .flatten()
                    .map(Response::LaunchPending)
                    .unwrap_or(Response::StartRejected(
                        StartRejection::LaunchPersistenceRefused,
                    ));
            }
            if attempt.launch_attempt_phase != LaunchAttemptPhase::PromptAmbiguous {
                return Response::LaunchPending(attempt);
            }
            return self.promote_ambiguous(attempt);
        }
        let persistence = || Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        // A hold an ended flow still keeps on this Address is given back
        // first, so the refusal reads what Metaflows reports.
        match self.store.give_back_ended_holds(&profile.address) {
            Ok(given) => {
                for flow_id in given {
                    eprintln!(
                        "flow-nexus: the Address hold of ended flow {flow_id} was given back"
                    );
                }
            }
            Err(_) => return persistence(),
        }
        // A metaflow an older Nexus left Awake as an ended flow is Asleep.
        self.rest_ended_awake(&profile.address);
        // A seat whose Address it could not take is never launched.
        match self.store.launch_address_refusal(&profile) {
            Ok(None) => {}
            Ok(Some(rejection)) => return Response::StartRejected(rejection),
            Err(_) => return persistence(),
        }
        let launch = match self.perform(Operation::Compose(profile)) {
            Outcome::Composed(launch) => launch,
            outcome => return Self::composition_refused(outcome),
        };
        // A Claude launch's FlowId is claimed here, before its harness
        // exists; the harness is spawned with it.
        let reserved = match self.perform(Operation::Reserve(Reserve_Data {
            composed_launch: launch.clone(),
            origin: origin.clone(),
        })) {
            Outcome::Reserved(Reserved_Data {
                launch_attempt_reservation: LaunchAttemptReservation::Reserved(_),
                flow_id_option,
            }) => flow_id_option,
            Outcome::Reserved(Reserved_Data {
                launch_attempt_reservation: LaunchAttemptReservation::Existing(attempt),
                ..
            }) => {
                return Response::LaunchPending(attempt);
            }
            Outcome::Reserved(Reserved_Data {
                launch_attempt_reservation: LaunchAttemptReservation::Conflict,
                ..
            }) => {
                return Response::StartRejected(StartRejection::LaunchRequestConflict);
            }
            Outcome::Failed(Failed_Data::ClaimRefused) => {
                return Response::StartRejected(StartRejection::BindingRefused(
                    BindingRefusal::ClaimUnavailable,
                ));
            }
            _ => return persistence(),
        };
        let harness_kind = launch.harness_kind.clone();
        let response = self.launch_reserved(launch, origin, reserved.clone());
        // The launch's own responder ends with it, whatever it came to.
        self.close_launch_responder();
        // A launch refused after its pane exists leaves nothing running: its
        // pane is closed and the flow it registered recorded Stopped. Then
        // it gives back the flow it held and the FlowId it claimed; the
        // refusal stands whatever either does.
        if let Response::StartRejected(_) = &response {
            self.abandon_refused_launch(&launch_request_id, &harness_kind, reserved.as_deref());
            if let Some(flow_id) = reserved {
                let _ = self.perform(Operation::Release(flow_id));
            }
        }
        response
    }

    fn launch_reserved(
        &self,
        launch: ComposedLaunch,
        origin: Origin,
        reserved: Option<String>,
    ) -> Response {
        let persistence = || Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        let native_intent = NativeLaunchIntent {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            harness_kind: launch.harness_kind.clone(),
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
            prompt_sha256_vector: launch.part_hashes(),
        };
        if self.perform(Operation::Record(Record_Data::Intent(native_intent))) != Outcome::Recorded
        {
            return persistence();
        }
        let pane = match self.perform(Operation::Open(launch.clone())) {
            Outcome::Opened(pane) => pane,
            outcome => return Self::native_refused(NativeLaunchStage::Open, outcome),
        };
        if !self.store.record_created_launch_pane(pane).unwrap_or(false) {
            return persistence();
        }
        let Some(pane) = self
            .store
            .request_harness_start(&launch.launch_profile.launch_request_id)
            .unwrap_or(None)
        else {
            return persistence();
        };
        let pane_launch = PaneLaunch {
            composed_launch: launch.clone(),
            herdr_pane_binding: pane,
            flow_id_option: reserved,
            native_session_id_option: None,
        };
        // A Codex Spawn names the thread it opened, its first turn already
        // in it, before the TUI attached; Bind requires that thread.
        let opened = match self.perform(Operation::Spawn(pane_launch.clone())) {
            Outcome::Spawned(opened) => opened,
            Outcome::Failed(Failed_Data::SpawnRefused(refusal)) => {
                return Response::StartRejected(StartRejection::HarnessSpawnRefused(refusal));
            }
            outcome => return Self::native_refused(NativeLaunchStage::Spawn, outcome),
        };
        let binding = match self.perform(Operation::Bind(PaneLaunch {
            native_session_id_option: opened,
            ..pane_launch
        })) {
            Outcome::Bound(binding) => binding,
            outcome => return Self::native_refused(NativeLaunchStage::Bind, outcome),
        };
        if self.perform(Operation::Record(Record_Data::Binding(binding.clone())))
            != Outcome::Recorded
        {
            return persistence();
        }
        let node = FlowNode {
            flow_id: binding.flow_id.clone(),
            session_id: binding.native_session_id.clone(),
            harness_kind: binding.harness_kind.clone(),
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: binding.herdr_pane_binding.herdr_session_name.clone(),
                herdr_agent_name: binding.herdr_pane_binding.herdr_agent_name.clone(),
                herdr_pane_id: binding.herdr_pane_binding.herdr_pane_id.clone(),
                herdr_terminal_id: binding.herdr_pane_binding.herdr_terminal_id.clone(),
            }),
            origin,
            flow_lifecycle: FlowLifecycle::Pending,
        };
        if !self.herdr.validate_registration(&node) {
            return Response::StartRejected(StartRejection::RegistrationRefused(
                RegistrationRefusal::RouteUnverified,
            ));
        }
        let caller = signal_flow::Caller {
            flow_id: binding.flow_id.clone(),
            aspect: launch.launch_profile.address.aspect.clone(),
            power_level: Self::power_of(&launch.launch_profile.address.layer),
            model_name: launch.launch_profile.model_name.clone(),
        };
        let registered = match self.perform(Operation::Register(Register_Data {
            flow_node: node,
            caller,
        })) {
            Outcome::Registered(node) => node,
            Outcome::Failed(Failed_Data::ConflictingBinding) => {
                return Response::StartRejected(StartRejection::RegistrationRefused(
                    RegistrationRefusal::RoleConflict,
                ));
            }
            _ => {
                return Response::StartRejected(StartRejection::RegistrationRefused(
                    RegistrationRefusal::RoleUnrecorded,
                ));
            }
        };
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: binding.launch_request_id.clone(),
            flow_id: registered.flow_id.clone(),
            native_session_id: registered.session_id.clone(),
            herdr_pane_binding: binding.herdr_pane_binding.clone(),
        };
        // The acknowledgement takes the seat's Address in the same write, so
        // the seat is identified before its first prompt; an Address taken
        // meanwhile refuses it.
        if self.perform(Operation::Record(Record_Data::Acknowledgement(
            acknowledgement.clone(),
        ))) != Outcome::Recorded
        {
            return match self.store.launch_address_refusal(&launch.launch_profile) {
                Ok(Some(rejection)) => Response::StartRejected(rejection),
                _ => persistence(),
            };
        }
        // The claimed Flow names its own pane before any prompt: a new pane
        // never keeps a title another Flow left behind.
        match self.perform(Operation::Title(Title_Data {
            composed_launch: launch.clone(),
            native_launch_binding: binding.clone(),
        })) {
            Outcome::Titled => {}
            outcome => return Self::native_refused(NativeLaunchStage::Naming, outcome),
        }
        let mut delivery_intent =
            match self
                .herdr
                .accept_registration(&launch, &binding, &acknowledgement)
            {
                Ok(intent) => intent,
                Err(refusal) => {
                    eprintln!(
                        "flow-nexus: registration of flow {} refused at {refusal:?}: {refusal}",
                        binding.flow_id
                    );
                    return Response::StartRejected(StartRejection::RegistrationRefused(
                        (&refusal).into(),
                    ));
                }
            };
        // A later part waits for a Stop reported after this one.
        delivery_intent.stops_reported = self.stops_reported(&binding.flow_id);
        if self.perform(Operation::Record(Record_Data::Delivery(
            delivery_intent.clone(),
        ))) != Outcome::Recorded
        {
            return Response::StartRejected(StartRejection::IntentPersistenceRefused);
        }
        let launch_profile = launch.launch_profile.clone();
        let initial = match self.perform(Operation::Submit(Submit_Data {
            composed_launch: launch,
            prompt_delivery_intent: delivery_intent.clone(),
        })) {
            Outcome::Submitted(result) => result,
            _ => PromptDeliveryResult::Ambiguous(delivery_intent.clone()),
        };
        if self.perform(Operation::Record(Record_Data::Delivered(initial.clone())))
            != Outcome::Recorded
        {
            return persistence();
        }
        // A transcript that contradicts the launch (another model, another
        // first input) refuses it; only an unread receipt stays ambiguous.
        let recorded = match initial {
            PromptDeliveryResult::Observed(receipt) => return self.confirmed(&receipt.flow_id),
            PromptDeliveryResult::Ambiguous(intent) => intent,
        };
        match self.herdr.observe_native_target_receipt(&recorded) {
            Ok(progress) => self.settle_progress(
                HarnessedProfile {
                    launch_profile,
                    harness_kind: recorded.harness_kind.clone(),
                },
                &recorded,
                progress,
            ),
            Err(refusal) => {
                eprintln!(
                    "flow-nexus: launch {} refused by its receipt: {refusal}",
                    recorded.launch_request_id
                );
                Self::receipt_refused(&refusal)
            }
        }
    }

    fn promote_ambiguous(&self, attempt: LaunchAttempt) -> Response {
        let Some(intent) = attempt.prompt_delivery_intent_option else {
            return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        };
        let response = match self.herdr.observe_native_target_receipt(&intent) {
            Ok(progress) => self.settle_progress(
                HarnessedProfile {
                    launch_profile: attempt.launch_profile.clone(),
                    harness_kind: intent.harness_kind.clone(),
                },
                &intent,
                progress,
            ),
            Err(refusal) => {
                eprintln!(
                    "flow-nexus: launch {} refused by its receipt: {refusal}",
                    attempt.launch_request_id
                );
                Self::receipt_refused(&refusal)
            }
        };
        // The refused launch leaves nothing running, as one refused before
        // its first part does.
        if let Response::StartRejected(_) = &response {
            self.abandon_refused_launch(&attempt.launch_request_id, &intent.harness_kind, None);
        }
        response
    }

    fn settled(&self, profile: &LaunchProfile, origin: &Origin) -> Option<Response> {
        let launch_request_id = &profile.launch_request_id;
        let outcome = match self.store.launch_outcome(launch_request_id) {
            Ok(outcome) => outcome?,
            Err(_) => {
                return Some(Response::StartRejected(
                    StartRejection::LaunchPersistenceRefused,
                ));
            }
        };
        match self.store.launch_attempt(launch_request_id) {
            Ok(Some(attempt))
                if &attempt.launch_profile != profile || &attempt.origin != origin =>
            {
                return Some(Response::StartRejected(
                    StartRejection::LaunchRequestConflict,
                ));
            }
            Ok(Some(attempt)) if outcome.awaits_reaping() => {
                return Some(self.resume_reaping(launch_request_id, attempt));
            }
            Ok(_) => {}
            Err(_) => {
                return Some(Response::StartRejected(
                    StartRejection::LaunchPersistenceRefused,
                ));
            }
        }
        Some(outcome.response(&self.store))
    }

    fn settle(&self, launch_request_id: &str, response: Response) -> Response {
        let replacement = match self.store.replacement(launch_request_id) {
            Ok(replacement) => replacement,
            Err(_) => return response,
        };
        match response {
            Response::Started(started) => match replacement {
                Some(replacement) => self.reap(replacement, started),
                None => {
                    // A Started flow stays Started; a failed outcome write
                    // only leaves LaunchStatus to answer from the attempt.
                    let _ = self.perform(Self::settled_as(
                        launch_request_id,
                        LaunchOutcome::Started(LaunchedRecord::from(&started)),
                    ));
                    Response::Started(started)
                }
            },
            Response::StartRejected(rejection)
                if rejection != StartRejection::LaunchRequestConflict =>
            {
                let reserved = matches!(self.store.launch_attempt(launch_request_id), Ok(Some(_)));
                let outcome = match replacement {
                    Some(_) => {
                        LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(rejection))
                    }
                    None => LaunchOutcome::StartRejected(rejection),
                };
                if reserved {
                    let _ = self.perform(Self::settled_as(launch_request_id, outcome.clone()));
                } else {
                    // Nothing was reserved: the request never became a
                    // launch, and it may be sent again.
                    let _ = self.perform(Operation::Record(Record_Data::Withdrawn(
                        launch_request_id.into(),
                    )));
                }
                outcome.response(&self.store)
            }
            response => response,
        }
    }

    fn replace(&self, profile: LaunchProfile, origin: Origin) -> Response {
        let refused = |rejection| Response::ReplaceRejected(rejection);
        let persistence = || {
            Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            ))
        };
        let launch_request_id = profile.launch_request_id.clone();
        let Some(predecessor) = profile.flow_id_option.clone() else {
            return refused(ReplaceRejection::PredecessorAbsent);
        };
        match self.store.replacement(&launch_request_id) {
            Ok(Some(_)) => {
                // A replacement already under way or settled: resume it.
                if let Some(settled) = self.settled(&profile, &origin) {
                    return Self::as_replacement(settled);
                }
                let response = self.start(profile, origin);
                return Self::as_replacement(self.settle(&launch_request_id, response));
            }
            Ok(None) => {}
            Err(_) => return persistence(),
        }
        match self.store.launch_outcome(&launch_request_id) {
            Ok(None) => {}
            Ok(Some(_)) => {
                return refused(ReplaceRejection::LaunchRefused(
                    StartRejection::LaunchRequestConflict,
                ));
            }
            Err(_) => return persistence(),
        }
        match self.store.launch_attempt(&launch_request_id) {
            Ok(Some(attempt)) if attempt.launch_profile != profile || attempt.origin != origin => {
                return refused(ReplaceRejection::LaunchRefused(
                    StartRejection::LaunchRequestConflict,
                ));
            }
            Ok(_) => {}
            Err(_) => return persistence(),
        }
        match self.store.flow_node(&predecessor) {
            Ok(None) => return refused(ReplaceRejection::UnknownPredecessor),
            Ok(Some(node)) if node.flow_lifecycle == FlowLifecycle::Stopped => {
                return refused(ReplaceRejection::PredecessorStopped);
            }
            Ok(Some(_)) => {}
            Err(_) => return persistence(),
        }
        if self.perform(Operation::Record(Record_Data::Replacing(Replacement {
            launch_request_id: launch_request_id.clone(),
            predecessor,
        }))) != Outcome::Recorded
        {
            return persistence();
        }
        let response = self.start(profile, origin);
        Self::as_replacement(self.settle(&launch_request_id, response))
    }

    fn reap(&self, replacement: Replacement, launched: Launched) -> Response {
        let refuse = |rejection: ReplaceRejection| {
            let _ = self.perform(Self::settled_as(
                &replacement.launch_request_id,
                LaunchOutcome::ReplaceRejected(rejection.clone()),
            ));
            Response::ReplaceRejected(rejection)
        };
        let node = match self.store.flow_node(&replacement.predecessor) {
            Ok(Some(node)) => node,
            Ok(None) => return refuse(ReplaceRejection::UnknownPredecessor),
            Err(_) => {
                return refuse(ReplaceRejection::ReapRefused(
                    StopRejection::PersistenceRefused,
                ));
            }
        };
        // The predecessor stops receiving first: Stopped is what
        // ResolveRecipient and Deliver refuse.
        if node.flow_lifecycle != FlowLifecycle::Stopped
            && self.perform(Operation::Record(Record_Data::Stopped(
                replacement.predecessor.clone(),
            ))) != Outcome::Recorded
        {
            return refuse(ReplaceRejection::ReapRefused(
                StopRejection::PersistenceRefused,
            ));
        }
        // A predecessor whose pane Herdr confirms gone is already reaped;
        // a pane that exists is closed, and its failed close refuses. When
        // Herdr cannot be read the pane may still be open: the reap is
        // refused as retryable and the successor stays unroutable.
        match self.herdr.pane_presence(&node) {
            PanePresence::Absent => {}
            PanePresence::Unknown => {
                return refuse(ReplaceRejection::ReapRefused(
                    StopRejection::RouteUnavailable,
                ));
            }
            PanePresence::Present | PanePresence::Bare => {
                if self.perform(Operation::Close(node)) != Outcome::Closed {
                    return refuse(ReplaceRejection::ReapRefused(StopRejection::CloseRefused));
                }
            }
        }
        let replaced = Replaced {
            flow_id: replacement.predecessor.clone(),
            launched,
        };
        // The Replaced outcome is what releases the successor to routing.
        if self.perform(Self::settled_as(
            &replacement.launch_request_id,
            LaunchOutcome::Replaced(ReplacedRecord::from(&replaced)),
        )) != Outcome::Recorded
        {
            return Response::ReplaceRejected(ReplaceRejection::ReapRefused(
                StopRejection::PersistenceRefused,
            ));
        }
        Response::Replaced(replaced)
    }

    fn launch_status(&self, launch_request_id: &str) -> Response {
        match self.store.launch_inspection_records(launch_request_id) {
            Ok(Some(records)) => Response::LaunchInspected(records.launch_inspection(&self.store)),
            Ok(None) => Response::LaunchStatusRejected(LaunchStatusRejection::UnknownLaunchRequest),
            Err(StoreError::LaunchStatusReadViewChanged) => {
                Response::LaunchStatusRejected(LaunchStatusRejection::ReadViewChanged)
            }
            Err(StoreError::LaunchStatusInconsistent) => {
                Response::LaunchStatusRejected(LaunchStatusRejection::InconsistentRecords)
            }
            Err(_) => Response::LaunchStatusRejected(LaunchStatusRejection::PersistenceRefused),
        }
    }
}

/// A Codex launch an earlier Nexus left between opening its thread and
/// acknowledging its seat is never finished: nothing replays it, and its
/// registration waits on a seat at rest that a first turn pending on a send
/// call never reaches. At the Nexus's start, before any socket or responder
/// is served, each such launch (unsettled, Codex, its thread recorded, its
/// phase NativeLaunchIntentRecorded or NativeBound) is reconciled.
///
/// Its outcome and its cleanup are apart. First its cleanup is begun, a
/// durable record with every step Pending; then its outcome is settled
/// refused, `NativeLaunchRefused.{ Spawn Unclassified }`, so it is never a
/// sender again and a replayed Start answers the settlement. Then each step
/// still Pending is taken: the thread step answers the calls the
/// app-server replays on the thread (a call whose commit the earlier Nexus
/// recorded is answered from its record and queued nothing more; any other
/// is refused, `ThreadUnowned`, and queues nothing), then accounts for the
/// thread and archives it, and is Done only once the archive succeeded; the
/// pane step takes down its pane and any flow it registered, closing only a
/// pane proven its own, and is Done only once nothing of it is left
/// running. A step not Done is taken again at the next start, the launch
/// already settled. A launch past its acknowledgement holds its Address and
/// is served by the Nexus's responders; a settled one with no unfinished
/// cleanup is left alone. No launch is granted send authority here, and the
/// queue of a target a recorded call reached is left as it is: its wake is
/// the Nexus's start's `recover_wakes`, or the next request queued for it.
pub trait ReconcilesInterruptedLaunches {
    fn reconcile_interrupted_launches(&self);
}

impl ReconcilesInterruptedLaunches for RunningNexus {
    fn reconcile_interrupted_launches(&self) {
        use crate::codex::{AccountsOpenedThread, SelectsCodexEndpoint};
        use crate::send_tool::RefusesInterruptedCalls;
        use crate::store::launch_cleanup::{
            CleanupPart, CleanupStep, CompletesCleanup, KeepsLaunchCleanups,
        };
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let opened = match self.store.opened_threads() {
            Ok(opened) => opened,
            Err(error) => {
                eprintln!("flow-nexus: opened launch threads unreadable: {error}");
                return;
            }
        };
        for opened in opened {
            let launch_request_id = opened.launch_request_id;
            let thread = opened.native_session_id;
            let Ok(Some(attempt)) = self.store.launch_attempt(&launch_request_id) else {
                continue;
            };
            let unsettled = self
                .store
                .launch_attempt_unsettled(&attempt)
                .unwrap_or(false);
            let interrupted = unsettled
                && attempt
                    .native_launch_intent_option
                    .as_ref()
                    .is_some_and(|intent| intent.harness_kind == HarnessKind::Codex)
                && matches!(
                    attempt.launch_attempt_phase,
                    LaunchAttemptPhase::NativeLaunchIntentRecorded
                        | LaunchAttemptPhase::NativeBound
                );
            let cleanup = if interrupted {
                // The cleanup is durable before the launch is settled, so a
                // settled launch never loses what is left to clean.
                match self.store.begin_launch_cleanup(&launch_request_id, &thread) {
                    Ok(cleanup) => cleanup,
                    Err(error) => {
                        eprintln!(
                            "flow-nexus: launch {launch_request_id} interrupted after opening thread {thread} left as it is: its cleanup is not recorded: {error}"
                        );
                        continue;
                    }
                }
            } else {
                match self.store.launch_cleanup(&launch_request_id) {
                    Ok(Some(cleanup)) if !cleanup.is_complete() => cleanup,
                    _ => continue,
                }
            };
            if unsettled {
                self.settle(
                    &launch_request_id,
                    Response::StartRejected(StartRejection::NativeLaunchRefused(
                        NativeLaunchRefusal {
                            native_launch_stage: NativeLaunchStage::Spawn,
                            native_launch_failure: NativeLaunchFailure::Unclassified,
                        },
                    )),
                );
            }
            if cleanup.step(CleanupPart::Thread) == CleanupStep::Pending {
                match self
                    .codex_endpoints
                    .adapter_for(&attempt.launch_profile.model_name)
                {
                    Ok(adapter) => {
                        let answered = self.refuse_interrupted_calls(&thread, &adapter);
                        let account = adapter.account_opened_thread(&thread);
                        eprintln!(
                            "flow-nexus: launch {launch_request_id} interrupted after opening thread {thread}: {answered:?}, {account:?}"
                        );
                        if matches!(account.thread_archive, signal_flow::ThreadArchive::Archived)
                            && let Err(error) = self
                                .store
                                .finish_cleanup_step(&launch_request_id, CleanupPart::Thread)
                        {
                            eprintln!(
                                "flow-nexus: launch {launch_request_id}: archived thread {thread} not recorded: {error}"
                            );
                        }
                    }
                    Err(error) => eprintln!(
                        "flow-nexus: launch {launch_request_id} thread {thread} left for the next start: no Codex endpoint: {error}"
                    ),
                }
            }
            if cleanup.step(CleanupPart::Pane) == CleanupStep::Pending {
                match self.abandon_refused_launch(&launch_request_id, &HarnessKind::Codex, None) {
                    LaunchTeardown::TakenDown => {
                        if let Err(error) = self
                            .store
                            .finish_cleanup_step(&launch_request_id, CleanupPart::Pane)
                        {
                            eprintln!(
                                "flow-nexus: launch {launch_request_id}: teardown not recorded: {error}"
                            );
                        }
                    }
                    kept => eprintln!(
                        "flow-nexus: launch {launch_request_id} pane left for the next start: {kept:?}"
                    ),
                }
            }
        }
    }
}

/// What taking down a refused launch came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchTeardown {
    /// Nothing the launch recorded is left running: the flow it registered
    /// is recorded Stopped, its pane closed or gone; or it registered no
    /// flow and recorded no pane to recover.
    TakenDown,
    /// The pane is left as it is: Herdr did not prove it the launch's own
    /// (`PaneOwnership`), and any flow it registered stays as it was.
    Kept(PaneOwnership),
    /// The pane was the launch's own and closing it failed.
    CloseFailed,
    /// The pane is gone and the flow it registered was not recorded
    /// Stopped.
    StopUnrecorded,
}

/// What a launch refused after Reserve leaves in Herdr is taken down.
pub(crate) trait AbandonsRefusedLaunch {
    /// Closes the pane a refused launch created, only when Herdr shows it
    /// running that launch's own native session, or running none: the
    /// session of the flow it registered, else the thread it opened (Codex)
    /// or the session its reserved FlowId claims (Claude). A launch with no
    /// recorded session closes nothing, and its pane stays recorded for a
    /// later reconciliation. A flow it registered is recorded Stopped once
    /// its pane is closed or Herdr shows it gone; otherwise it stays as it
    /// was, for Stop to finish.
    fn abandon_refused_launch(
        &self,
        launch_request_id: &str,
        harness_kind: &HarnessKind,
        reserved: Option<&str>,
    ) -> LaunchTeardown;
}

impl AbandonsRefusedLaunch for RunningNexus {
    fn abandon_refused_launch(
        &self,
        launch_request_id: &str,
        harness_kind: &HarnessKind,
        reserved: Option<&str>,
    ) -> LaunchTeardown {
        use crate::herdr::launch::ClaimsNativeIdentity;
        let bound = self
            .store
            .launch_attempt(launch_request_id)
            .ok()
            .flatten()
            .and_then(|attempt| attempt.native_launch_binding_option)
            .map(|binding| binding.flow_id);
        let registered = bound
            .as_deref()
            .or(reserved)
            .and_then(|flow_id| self.store.flow_node(flow_id).ok().flatten())
            .filter(|node| node.flow_lifecycle.is_live());
        if let Some(node) = registered {
            let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
                return LaunchTeardown::Kept(PaneOwnership::Unknown);
            };
            match self.herdr.pane_ownership(&node) {
                PaneOwnership::Absent => {}
                PaneOwnership::Owned | PaneOwnership::Bare => {
                    let _lease = self.pane_leases.hold(route);
                    if self.perform(Operation::Close(node.clone())) != Outcome::Closed {
                        eprintln!(
                            "flow-nexus: refused launch {launch_request_id} left flow {} Pending: its pane was not closed",
                            node.flow_id
                        );
                        return LaunchTeardown::CloseFailed;
                    }
                }
                kept => {
                    eprintln!(
                        "flow-nexus: refused launch {launch_request_id} left flow {} and its pane as they are: {kept:?}",
                        node.flow_id
                    );
                    return LaunchTeardown::Kept(kept);
                }
            }
            if self.perform(Operation::Record(Record_Data::Stopped(
                node.flow_id.clone(),
            ))) != Outcome::Recorded
            {
                eprintln!(
                    "flow-nexus: refused launch {launch_request_id}: flow {} not recorded Stopped",
                    node.flow_id
                );
                return LaunchTeardown::StopUnrecorded;
            }
            return LaunchTeardown::TakenDown;
        }
        let Ok(Some(pane)) = self.store.recoverable_launch_pane(launch_request_id) else {
            return LaunchTeardown::TakenDown;
        };
        // The session the pane was created for: the thread a Codex launch
        // opened, or the session a Claude launch's reserved FlowId claims.
        let session_id = match harness_kind {
            HarnessKind::Codex => self.store.opened_thread(launch_request_id).ok().flatten(),
            HarnessKind::Claude => reserved.and_then(|flow_id| {
                self.herdr
                    .claimed_native_identity(flow_id, &HarnessKind::Claude)
                    .ok()
            }),
        }
        .unwrap_or_default();
        let node = FlowNode {
            flow_id: reserved.unwrap_or_default().into(),
            session_id,
            harness_kind: harness_kind.clone(),
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: pane.herdr_session_name.clone(),
                herdr_agent_name: pane.herdr_agent_name.clone(),
                herdr_pane_id: pane.herdr_pane_id.clone(),
                herdr_terminal_id: pane.herdr_terminal_id.clone(),
            }),
            origin: Origin::Unbound,
            flow_lifecycle: FlowLifecycle::Pending,
        };
        let teardown = match self.herdr.pane_ownership(&node) {
            PaneOwnership::Absent => LaunchTeardown::TakenDown,
            PaneOwnership::Owned | PaneOwnership::Bare => {
                if self.perform(Operation::Close(node)) == Outcome::Closed {
                    LaunchTeardown::TakenDown
                } else {
                    LaunchTeardown::CloseFailed
                }
            }
            kept => LaunchTeardown::Kept(kept),
        };
        // Only a pane taken down stops being recoverable; any other stays
        // recorded, for the next reconciliation to look at again.
        if teardown == LaunchTeardown::TakenDown {
            let _ = self.store.orphan_launch_pane(launch_request_id);
        } else {
            eprintln!(
                "flow-nexus: refused launch {launch_request_id} left its pane {} as it is: {teardown:?}",
                pane.herdr_pane_id
            );
        }
        teardown
    }
}

trait DescribesLaunchInspection {
    fn launch_inspection(self, store: &crate::store::FlowStore) -> LaunchInspection;
}

impl DescribesLaunchInspection for LaunchInspectionRecords {
    fn launch_inspection(self, store: &crate::store::FlowStore) -> LaunchInspection {
        let launch_status_outcome_option = self
            .launch_outcome_option
            .map(|outcome| outcome.status_outcome(store));
        LaunchInspection {
            launch_attempt: self.launch_attempt,
            launch_status_outcome_option,
            flow_node_option: self.flow_node_option,
        }
    }
}

/// How a launch's first USER message reaches its flow, part by part.
/// What one reading of the transcript comes to: the receipt confirms the
/// launch; a waiting launch stays ambiguous, recording an adopted cursor; an
/// answered part sends the next. The next part is sent once the flow is done
/// with the one before: for Claude, a Stop reported after it and Herdr
/// showing the pane at rest; for Codex, the turn the transcript showed
/// completed. The part is recorded as submitted before it is typed, and
/// never typed twice.
impl SettlesStartupProgress for RunningNexus {
    fn settle_progress<N: Profiled, O: DeliveryIntended, P: Progressing>(
        &self,
        input_0: N,
        input_1: O,
        input_2: P,
    ) -> Response {
        let recorded = input_1.intent();
        match input_2.progress() {
            StartupProgress::Received(receipt) => {
                let flow_id = receipt.flow_id.clone();
                if self.perform(Operation::Record(Record_Data::Delivered(
                    PromptDeliveryResult::Observed(receipt),
                ))) != Outcome::Recorded
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                self.confirmed(&flow_id)
            }
            StartupProgress::Waiting(updated) => {
                if updated != recorded
                    && self.perform(Operation::Record(Record_Data::Delivered(
                        PromptDeliveryResult::Ambiguous(updated.clone()),
                    ))) != Outcome::Recorded
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                Response::StartAmbiguous(updated)
            }
            StartupProgress::PartAnswered(intent) => self.advance_startup(&input_0, &intent),
        }
    }

    fn advance_startup<N: Profiled, O: DeliveryIntended>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Response {
        let intent = input_1.intent();
        let mut next = intent.clone();
        if intent.harness_kind == HarnessKind::Claude {
            let stops = self.stops_reported(&intent.flow_id);
            if stops <= intent.stops_reported
                || self
                    .herdr
                    .await_pane_rest(&intent.herdr_pane_binding)
                    .is_err()
            {
                return Response::StartAmbiguous(intent);
            }
            next.stops_reported = stops;
        }
        let launch = match self.perform(Operation::Compose(input_0.profile())) {
            Outcome::Composed(launch) => launch,
            outcome => return Self::composition_refused(outcome),
        };
        if launch.part_hashes() != intent.prompt_sha256_vector {
            eprintln!(
                "flow-nexus: launch {} composed other parts than it recorded",
                intent.launch_request_id
            );
            return Response::StartRejected(StartRejection::ContextRefused(
                ContextRefusal::PayloadMismatch,
            ));
        }
        next.parts_submitted += 1;
        if self.perform(Operation::Record(Record_Data::Delivered(
            PromptDeliveryResult::Ambiguous(next.clone()),
        ))) != Outcome::Recorded
        {
            return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        }
        if let Outcome::Failed(failure) = self.perform(Operation::Submit(Submit_Data {
            composed_launch: launch,
            prompt_delivery_intent: next.clone(),
        })) {
            // It may have reached the flow: it is never typed again.
            eprintln!(
                "flow-nexus: launch {} part {} not confirmed typed: {failure:?}",
                next.launch_request_id, next.parts_submitted
            );
        }
        Response::StartAmbiguous(next)
    }

    fn stops_reported<N: Textual>(&self, input: N) -> i64 {
        let flow_id = input.text();
        // A Stop that names another native session than the flow's own (a
        // harness the seat started, under the same FLOW_ID) is not its Stop.
        let session = self
            .store
            .flow_node(&flow_id)
            .ok()
            .flatten()
            .map(|node| node.session_id)
            .unwrap_or_default();
        self.store
            .events(&flow_id)
            .ok()
            .flatten()
            .map(|events| {
                events
                    .iter()
                    .filter(|event| {
                        matches!(event, Event::Stopped(stop)
                            if session.is_empty()
                                || stop
                                    .native_session_id_option
                                    .as_deref()
                                    .is_none_or(|stopped| stopped == session))
                    })
                    .count()
            })
            .unwrap_or_default() as i64
    }

    fn composition_refused<N: OutcomeCarrying>(input: N) -> Response {
        match input.outcome() {
            Outcome::Failed(Failed_Data::ContextRefused(refusal)) => {
                Response::StartRejected(StartRejection::ContextRefused(refusal))
            }
            Outcome::Failed(Failed_Data::LeaveOutRefused(refusal)) => {
                Response::StartRejected(StartRejection::LeaveOutRefused(refusal))
            }
            _ => Response::StartRejected(StartRejection::CompositionRefused),
        }
    }
}

/// Helpers of the launch verbs that are not themselves wire operations.
trait ResumesReplacement {
    fn resume_reaping(&self, launch_request_id: &str, attempt: LaunchAttempt) -> Response;
    fn as_replacement(response: Response) -> Response;
    /// The Record operation that stores what a launch request came to.
    fn settled_as(launch_request_id: &str, launch_outcome: LaunchOutcome) -> Operation;
    /// Confirms a launch whose receipt was observed: it is Started.
    fn confirmed(&self, flow_id: &str) -> Response;
    /// The answer to a native launch step that did not succeed: the stage
    /// and category its operation failed with, else the step's stage with
    /// the catch-all category.
    fn native_refused(stage: NativeLaunchStage, outcome: Outcome) -> Response;
    /// The answer to a launch its receipt refused: the Receipt stage and the
    /// refusal's category.
    fn receipt_refused(refusal: &TranscriptRefusal) -> Response;
}

impl ResumesReplacement for RunningNexus {
    fn native_refused(stage: NativeLaunchStage, outcome: Outcome) -> Response {
        Response::StartRejected(StartRejection::NativeLaunchRefused(match outcome {
            Outcome::Failed(Failed_Data::LaunchRefused(refusal)) => refusal,
            _ => NativeLaunchRefusal {
                native_launch_stage: stage,
                native_launch_failure: NativeLaunchFailure::AdapterRefused,
            },
        }))
    }

    fn receipt_refused(refusal: &TranscriptRefusal) -> Response {
        Response::StartRejected(StartRejection::NativeLaunchRefused(NativeLaunchRefusal {
            native_launch_stage: NativeLaunchStage::Receipt,
            native_launch_failure: refusal.into(),
        }))
    }

    fn resume_reaping(&self, launch_request_id: &str, attempt: LaunchAttempt) -> Response {
        let replacement = match self.store.replacement(launch_request_id) {
            Ok(Some(replacement)) => replacement,
            _ => {
                return Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                    StartRejection::LaunchPersistenceRefused,
                ));
            }
        };
        let Some(binding) = attempt.native_launch_binding_option else {
            return Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            ));
        };
        match self.perform(Operation::Confirm(binding.flow_id)) {
            Outcome::Started(started) => self.reap(replacement, started),
            _ => Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            )),
        }
    }

    fn settled_as(launch_request_id: &str, launch_outcome: LaunchOutcome) -> Operation {
        Operation::Record(Record_Data::Settled(Record_Data_Settled_Data {
            launch_request_id: launch_request_id.into(),
            launch_outcome,
        }))
    }

    fn confirmed(&self, flow_id: &str) -> Response {
        match self.perform(Operation::Confirm(flow_id.into())) {
            Outcome::Started(started) => {
                // A Stop that landed before this commit found the seat
                // unconfirmed and left it; it is settled once the gate is
                // released.
                self.note_stop_at_confirmation(flow_id);
                Response::Started(started)
            }
            Outcome::Failed(Failed_Data::Unstarted) => {
                Response::StartRejected(StartRejection::NativeLaunchRefused(NativeLaunchRefusal {
                    native_launch_stage: NativeLaunchStage::Confirm,
                    native_launch_failure: NativeLaunchFailure::SessionUnrecorded,
                }))
            }
            Outcome::Failed(Failed_Data::AddressAwake) => {
                Response::StartRejected(StartRejection::AddressAwake)
            }
            Outcome::Failed(Failed_Data::AddressEnded) => {
                Response::StartRejected(StartRejection::AddressEnded)
            }
            _ => Response::StartRejected(StartRejection::LaunchPersistenceRefused),
        }
    }

    fn as_replacement(response: Response) -> Response {
        match response {
            Response::StartRejected(rejection) => {
                Response::ReplaceRejected(ReplaceRejection::LaunchRefused(rejection))
            }
            response => response,
        }
    }
}

/// A file-change subscription on the native transcript root of one
/// ambiguous launch. The watcher announces a change of the session's
/// transcript through the store's launch changes and marks itself moved.
pub struct TranscriptWatch {
    _watcher: notify::RecommendedWatcher,
    moved: Arc<AtomicBool>,
}

pub trait WatchesTranscript: Sized {
    fn open(
        root: &std::path::Path,
        native_session_id: &str,
        changes: LaunchChanges,
    ) -> Result<Self, String>;
    /// Whether the transcript moved since this was last asked.
    fn moved(&self) -> bool;
}

impl WatchesTranscript for TranscriptWatch {
    fn open(
        root: &std::path::Path,
        native_session_id: &str,
        changes: LaunchChanges,
    ) -> Result<Self, String> {
        let moved = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&moved);
        let session = native_session_id.to_owned();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                let Ok(event) = event else {
                    return;
                };
                let concerns_session = event.paths.iter().any(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.ends_with(".jsonl") && name.contains(&session))
                });
                if concerns_session {
                    flag.store(true, Ordering::SeqCst);
                    changes.announce();
                }
            })
            .map_err(|error| error.to_string())?;
        watcher
            .watch(root, notify::RecursiveMode::Recursive)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            _watcher: watcher,
            moved,
        })
    }

    fn moved(&self) -> bool {
        self.moved.swap(false, Ordering::SeqCst)
    }
}

/// The Observe.Launch subscription. The connection is the subscription: the
/// current inspection on open, one LaunchInspected frame per phase change,
/// and the inspection with outcome last. It waits on announced changes,
/// never on a timer, and holds the dispatch gate only while it promotes an
/// ambiguous launch.
pub trait ObservesLaunch {
    fn observe_launch(
        &self,
        launch_request_id: &str,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesLaunch for RunningNexus {
    fn observe_launch(
        &self,
        launch_request_id: &str,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        let changes = self.store.launch_changes.clone();
        let mut sent_phase: Option<LaunchAttemptPhase> = None;
        let mut watch: Option<TranscriptWatch> = None;
        loop {
            let seen = changes.current();
            if self.stopping.requested() {
                return Err("the Nexus is stopping".into());
            }
            let answer = self.launch_status(launch_request_id);
            let Response::LaunchInspected(inspection) = &answer else {
                return send(&answer);
            };
            if inspection.launch_status_outcome_option.is_some() {
                return send(&answer);
            }
            let attempt = &inspection.launch_attempt;
            if sent_phase.as_ref() != Some(&attempt.launch_attempt_phase) {
                send(&answer)?;
                sent_phase = Some(attempt.launch_attempt_phase.clone());
            }
            if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptAmbiguous {
                let promote = match (&watch, &attempt.prompt_delivery_intent_option) {
                    (None, Some(intent)) => match self.watch_transcript(intent) {
                        // Whatever landed before the watch began is looked at once.
                        Ok(opened) => {
                            watch = Some(opened);
                            true
                        }
                        Err(error) => {
                            eprintln!(
                                "flow-nexus: launch {launch_request_id} transcript watch: {error}"
                            );
                            false
                        }
                    },
                    (Some(watch), _) => watch.moved(),
                    (None, None) => false,
                };
                if promote {
                    self.promote_serially(launch_request_id);
                    continue;
                }
            }
            changes.after(seen);
        }
    }
}

/// The Nexus's own watch over ambiguous launches. A receipt that lands
/// after Start answered StartAmbiguous promotes the launch whether or not
/// anyone subscribes or sends Start again: each ambiguous launch's
/// transcript is watched, and every change of it re-observes the receipt.
/// It waits on announced changes, never on a timer.
pub trait PromotesAmbiguousLaunches {
    /// Runs until the Nexus is told to stop.
    fn promote_ambiguous_launches(&self);
}

impl PromotesAmbiguousLaunches for RunningNexus {
    fn promote_ambiguous_launches(&self) {
        let changes = self.store.launch_changes.clone();
        let mut watches = AmbiguousLaunchWatches::default();
        while !self.stopping.requested() {
            let seen = changes.current();
            watches.pass(self);
            changes.after(seen);
        }
    }
}

/// One transcript watch per ambiguous launch; None when the watch could not
/// be opened (logged once).
#[derive(Default)]
pub struct AmbiguousLaunchWatches {
    watches: std::collections::BTreeMap<String, Option<TranscriptWatch>>,
}

pub trait WatchesAmbiguousLaunches {
    /// Opens a watch for each newly ambiguous launch and looks at it once;
    /// re-observes each launch whose transcript moved; drops settled ones.
    fn pass(&mut self, nexus: &RunningNexus);
}

impl WatchesAmbiguousLaunches for AmbiguousLaunchWatches {
    fn pass(&mut self, nexus: &RunningNexus) {
        let attempts = match nexus.store.ambiguous_launch_attempts() {
            Ok(attempts) => attempts,
            Err(error) => {
                eprintln!("flow-nexus: ambiguous launches unreadable: {error}");
                return;
            }
        };
        self.watches.retain(|launch_request_id, _| {
            attempts
                .iter()
                .any(|attempt| &attempt.launch_request_id == launch_request_id)
        });
        for attempt in attempts {
            let launch_request_id = attempt.launch_request_id.clone();
            let promote = match self.watches.get(&launch_request_id) {
                None => {
                    let watch = attempt
                        .prompt_delivery_intent_option
                        .as_ref()
                        .map(|intent| nexus.watch_transcript(intent))
                        .transpose()
                        .unwrap_or_else(|error| {
                            eprintln!(
                                "flow-nexus: launch {launch_request_id} transcript watch: {error}"
                            );
                            None
                        });
                    self.watches.insert(launch_request_id.clone(), watch);
                    // Whatever landed before the watch began is looked at once.
                    true
                }
                Some(Some(watch)) => watch.moved(),
                Some(None) => false,
            };
            // A launch whose first message has parts still to send is looked
            // at on every announced change: its next part waits on a Stop
            // report as well as on the transcript.
            let parts_pending =
                attempt
                    .prompt_delivery_intent_option
                    .as_ref()
                    .is_some_and(|intent| {
                        usize::try_from(intent.parts_submitted)
                            .is_ok_and(|parts| parts < intent.prompt_sha256_vector.len())
                    });
            if promote || parts_pending {
                nexus.promote_serially(&launch_request_id);
            }
        }
    }
}

trait PromotesObservedLaunch {
    fn watch_transcript(&self, intent: &PromptDeliveryIntent) -> Result<TranscriptWatch, String>;
    /// Promotes under the dispatch gate, then settles any Stop the
    /// confirmation found waiting.
    fn promote_serially(&self, launch_request_id: &str);
    fn promote_under_gate(&self, launch_request_id: &str);
}

impl PromotesObservedLaunch for RunningNexus {
    fn watch_transcript(&self, intent: &PromptDeliveryIntent) -> Result<TranscriptWatch, String> {
        let root = self
            .herdr
            .native_transcript_root(&intent.harness_kind, &intent.model_name)?;
        TranscriptWatch::open(
            &root,
            &intent.native_session_id,
            self.store.launch_changes.clone(),
        )
    }

    fn promote_serially(&self, launch_request_id: &str) {
        self.promote_under_gate(launch_request_id);
        self.settle_stops_at_confirmation();
    }

    fn promote_under_gate(&self, launch_request_id: &str) {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(self.store.launch_outcome(launch_request_id), Ok(None)) {
            return;
        }
        let Ok(Some(attempt)) = self.store.launch_attempt(launch_request_id) else {
            return;
        };
        if attempt.launch_attempt_phase != LaunchAttemptPhase::PromptAmbiguous {
            return;
        }
        let response = self.promote_ambiguous(attempt);
        self.settle(launch_request_id, response);
    }
}
