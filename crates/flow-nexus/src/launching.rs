//! Launch settlement: the Start path, Replace, LaunchStatus and the
//! Observe.Launch subscription, which all answer from one durable account of
//! a launch request — its attempt, its outcome, and whether it replaces a
//! predecessor.

use crate::codex::SelectsCodexEndpoint;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, PaneLaunch, Record_Data, Record_Data_Settled_Data,
    Register_Data, Reserve_Data, Submit_Data, Title_Data,
};
use crate::herdr::ReadsHerdrPanes;
use crate::performing::Performs;
use crate::store::AnnouncesLaunchChanges;
use crate::store::AnswersLaunchOutcome;
use crate::{
    RunningNexus,
    codex::ResolvesBoundCodexSkills,
    herdr::{
        LocatesNativeTranscripts, PanePresence,
        launch::{
            AcceptsLaunchRegistration, ObservesNativeTargetReceipt, ResolvesClaudeNativeSkills,
        },
    },
    store::{
        LaunchChanges, LaunchOutcome, ReadsFlowRows, ReadsLaunchAttempt, RecordsLaunchOutcome,
        RecordsReplacement, Replacement,
    },
};
use notify::Watcher;
use signal_flow::{
    EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
    LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation, LaunchStatusRejection, Launched,
    NativeLaunchIntent, PromptDeliveryIntent, PromptDeliveryResult, RegistrationAcknowledgement,
    ReplaceRejection, Replaced, Response, StartRejection, StartRequest, StopRejection,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// The launch verbs of the running Nexus. `start` is the one Start path;
/// `settle` records what a launch came to and, for a replacement, stops and
/// reaps the predecessor before the successor becomes routable.
pub trait LaunchesFlows {
    /// Runs or resumes the launch a StartRequest names.
    fn start(&self, request: StartRequest) -> Response;
    /// Looks again at the native transcript of a launch whose first prompt
    /// was not yet seen, promoting it when the receipt is there.
    fn promote_ambiguous(&self, attempt: LaunchAttempt) -> Response;
    /// The recorded outcome of a request already settled, or None.
    fn settled(&self, request: &StartRequest) -> Option<Response>;
    /// Records what a Start-path response settles, completing a replacement.
    fn settle(&self, launch_request_id: &str, response: Response) -> Response;
    /// Starts the successor through the Start path, then replaces.
    fn replace(&self, request: StartRequest) -> Response;
    /// Stops the predecessor, then closes its pane; only then is the
    /// successor routable.
    fn reap(&self, replacement: Replacement, launched: Launched) -> Response;
    /// Answers once: the outcome, else the pending attempt.
    fn launch_status(&self, launch_request_id: &str) -> Response;
}

impl LaunchesFlows for RunningNexus {
    fn start(&self, request: StartRequest) -> Response {
        let origin = request.origin_clue.clone();
        let launch_request_id = request.launch_profile.launch_request_id.clone();
        let existing = match self.store.launch_attempt(&launch_request_id) {
            Ok(existing) => existing,
            Err(_) => {
                return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
            }
        };
        if let Some(attempt) = existing {
            if attempt.launch_profile != request.launch_profile || attempt.origin_clue != origin {
                return Response::StartRejected(StartRejection::LaunchRequestConflict);
            }
            if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptObserved {
                let Some(binding) = attempt.native_launch_binding_option else {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                };
                return self.confirmed(&binding.flow_id);
            }
            if attempt.launch_attempt_phase != LaunchAttemptPhase::PromptAmbiguous {
                return Response::LaunchPending(attempt);
            }
            return self.promote_ambiguous(attempt);
        }
        let persistence = || Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        let launch = match self.perform(Operation::Compose(request.launch_profile)) {
            Outcome::Composed(launch) => launch,
            _ => return Response::StartRejected(StartRejection::CompositionRefused),
        };
        // A Codex launch whose model no endpoint serves is refused before
        // anything is reserved.
        if launch.launch_profile.harness_kind == HarnessKind::Codex
            && self
                .codex_endpoints
                .adapter_for(&launch.launch_profile.model_name)
                .is_err()
        {
            return Response::StartRejected(StartRejection::NativeLaunchRefused);
        }
        match self.perform(Operation::Reserve(Reserve_Data {
            composed_launch: launch.clone(),
            origin_clue: origin.clone(),
        })) {
            Outcome::Reserved(LaunchAttemptReservation::Reserved(_)) => {}
            Outcome::Reserved(LaunchAttemptReservation::Existing(attempt)) => {
                return Response::LaunchPending(attempt);
            }
            Outcome::Reserved(LaunchAttemptReservation::Conflict) => {
                return Response::StartRejected(StartRejection::LaunchRequestConflict);
            }
            _ => return persistence(),
        }
        let native_intent = NativeLaunchIntent {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            harness_kind: launch.launch_profile.harness_kind.clone(),
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            skill_name_vector: launch.launch_profile.skill_name_vector.clone(),
        };
        if self.perform(Operation::Record(Record_Data::Intent(native_intent))) != Outcome::Recorded
        {
            return persistence();
        }
        let pane = match self.perform(Operation::Open(launch.clone())) {
            Outcome::Opened(pane) => pane,
            _ => return Response::StartRejected(StartRejection::NativeLaunchRefused),
        };
        let pane_launch = PaneLaunch {
            composed_launch: launch.clone(),
            herdr_pane_binding: pane,
        };
        if self.perform(Operation::Spawn(pane_launch.clone())) != Outcome::Spawned {
            return Response::StartRejected(StartRejection::NativeLaunchRefused);
        }
        let binding = match self.perform(Operation::Bind(pane_launch)) {
            Outcome::Bound(binding) => binding,
            _ => return Response::StartRejected(StartRejection::BindingRefused),
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
            origin_clue: origin,
            flow_lifecycle: FlowLifecycle::Pending,
        };
        if !self.herdr.validate_registration(&node) {
            return Response::StartRejected(StartRejection::RegistrationRefused);
        }
        let caller = signal_flow::Caller {
            flow_id: binding.flow_id.clone(),
            flow_aspect: launch.launch_profile.flow_aspect.clone(),
            power_level: launch.launch_profile.power_level.clone(),
            model_name: launch.launch_profile.model_name.clone(),
        };
        let registered = match self.perform(Operation::Register(Register_Data {
            flow_node: node,
            caller,
        })) {
            Outcome::Registered(node) => node,
            _ => return Response::StartRejected(StartRejection::RegistrationRefused),
        };
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: binding.launch_request_id.clone(),
            flow_id: registered.flow_id.clone(),
            native_session_id: registered.session_id.clone(),
            herdr_pane_binding: binding.herdr_pane_binding.clone(),
        };
        if self.perform(Operation::Record(Record_Data::Acknowledgement(
            acknowledgement.clone(),
        ))) != Outcome::Recorded
        {
            return persistence();
        }
        // The claimed Flow names its own pane before any prompt: a new pane
        // never keeps a title another Flow left behind.
        if self.perform(Operation::Title(Title_Data {
            composed_launch: launch.clone(),
            native_launch_binding: binding.clone(),
        })) != Outcome::Titled
        {
            return Response::StartRejected(StartRejection::BindingRefused);
        }
        let native_skill_selection_vector = match launch.launch_profile.harness_kind {
            HarnessKind::Codex => self
                .codex_endpoints
                .adapter_for(&launch.launch_profile.model_name)
                .and_then(|adapter| adapter.resolve_bound_codex_skills(&launch, &binding))
                .map_err(|_| ()),
            HarnessKind::Claude => self
                .herdr
                .resolve_claude_native_skills(&launch, &binding)
                .map_err(|_| ()),
        };
        let Ok(native_skill_selection_vector) = native_skill_selection_vector else {
            return Response::StartRejected(StartRejection::RegistrationRefused);
        };
        let delivery_intent = match self.herdr.accept_registration(
            &launch,
            &binding,
            &acknowledgement,
            native_skill_selection_vector,
        ) {
            Ok(intent) => intent,
            Err(_) => {
                return Response::StartRejected(StartRejection::RegistrationRefused);
            }
        };
        if self.perform(Operation::Record(Record_Data::Delivery(
            delivery_intent.clone(),
        ))) != Outcome::Recorded
        {
            return Response::StartRejected(StartRejection::IntentPersistenceRefused);
        }
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
        let result = match initial {
            PromptDeliveryResult::Observed(receipt) => PromptDeliveryResult::Observed(receipt),
            PromptDeliveryResult::Ambiguous(_) => self
                .herdr
                .observe_native_target_receipt(&delivery_intent)
                .unwrap_or_else(|_| PromptDeliveryResult::Ambiguous(delivery_intent.clone())),
        };
        match result {
            PromptDeliveryResult::Ambiguous(intent) => {
                if intent != delivery_intent
                    && self.perform(Operation::Record(Record_Data::Delivered(
                        PromptDeliveryResult::Ambiguous(intent.clone()),
                    ))) != Outcome::Recorded
                {
                    return persistence();
                }
                Response::StartAmbiguous(intent)
            }
            PromptDeliveryResult::Observed(receipt) => {
                if self.perform(Operation::Record(Record_Data::Delivered(
                    PromptDeliveryResult::Observed(receipt),
                ))) != Outcome::Recorded
                {
                    return persistence();
                }
                self.confirmed(&binding.flow_id)
            }
        }
    }

    fn promote_ambiguous(&self, attempt: LaunchAttempt) -> Response {
        let (Some(intent), Some(binding)) = (
            attempt.prompt_delivery_intent_option,
            attempt.native_launch_binding_option,
        ) else {
            return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        };
        let observed = self
            .herdr
            .observe_native_target_receipt(&intent)
            .unwrap_or_else(|_| PromptDeliveryResult::Ambiguous(intent.clone()));
        let receipt = match observed {
            PromptDeliveryResult::Observed(receipt) => receipt,
            PromptDeliveryResult::Ambiguous(updated) => {
                if updated != intent
                    && self.perform(Operation::Record(Record_Data::Delivered(
                        PromptDeliveryResult::Ambiguous(updated.clone()),
                    ))) != Outcome::Recorded
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                return Response::StartAmbiguous(updated);
            }
        };
        if self.perform(Operation::Record(Record_Data::Delivered(
            PromptDeliveryResult::Observed(receipt),
        ))) != Outcome::Recorded
        {
            return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
        }
        self.confirmed(&binding.flow_id)
    }

    fn settled(&self, request: &StartRequest) -> Option<Response> {
        let launch_request_id = &request.launch_profile.launch_request_id;
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
                if attempt.launch_profile != request.launch_profile
                    || attempt.origin_clue != request.origin_clue =>
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
        Some(outcome.response())
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
                        LaunchOutcome::Started(started.clone()),
                    ));
                    let _ = self.perform(Operation::Continue(started.flow_id.clone()));
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
                    if self.perform(Self::settled_as(launch_request_id, outcome.clone()))
                        == Outcome::Recorded
                    {
                        let _ = self.perform(Operation::Prune(launch_request_id.into()));
                    }
                } else {
                    // Nothing was reserved: the request never became a
                    // launch, and it may be sent again.
                    let _ = self.perform(Operation::Record(Record_Data::Withdrawn(
                        launch_request_id.into(),
                    )));
                }
                outcome.response()
            }
            response => response,
        }
    }

    fn replace(&self, request: StartRequest) -> Response {
        let refused = |rejection| Response::ReplaceRejected(rejection);
        let persistence = || {
            Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            ))
        };
        let launch_request_id = request.launch_profile.launch_request_id.clone();
        let Some(predecessor) = request.launch_profile.flow_id_option.clone() else {
            return refused(ReplaceRejection::PredecessorAbsent);
        };
        match self.store.replacement(&launch_request_id) {
            Ok(Some(_)) => {
                // A replacement already under way or settled: resume it.
                if let Some(settled) = self.settled(&request) {
                    return Self::as_replacement(settled);
                }
                let response = self.start(request);
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
            Ok(Some(attempt))
                if attempt.launch_profile != request.launch_profile
                    || attempt.origin_clue != request.origin_clue =>
            {
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
        let response = self.start(request);
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
            PanePresence::Present => {
                if self.perform(Operation::Close(node)) != Outcome::Closed {
                    return refuse(ReplaceRejection::ReapRefused(StopRejection::CloseRefused));
                }
            }
        }
        // The predecessor is stopped and its pane is gone: no launch of it
        // needs its bundle copy any more.
        self.prune_launch_bundles_of(&replacement.predecessor);
        let replaced = Replaced {
            flow_id: replacement.predecessor.clone(),
            launched,
        };
        // The Replaced outcome is what releases the successor to routing.
        if self.perform(Self::settled_as(
            &replacement.launch_request_id,
            LaunchOutcome::Replaced(replaced.clone()),
        )) != Outcome::Recorded
        {
            return Response::ReplaceRejected(ReplaceRejection::ReapRefused(
                StopRejection::PersistenceRefused,
            ));
        }
        let _ = self.perform(Operation::Continue(replaced.launched.flow_id.clone()));
        Response::Replaced(replaced)
    }

    fn launch_status(&self, launch_request_id: &str) -> Response {
        let persistence =
            || Response::LaunchStatusRejected(LaunchStatusRejection::PersistenceRefused);
        match self.store.launch_outcome(launch_request_id) {
            Ok(Some(outcome)) => return outcome.response(),
            Ok(None) => {}
            Err(_) => return persistence(),
        }
        match self.store.launch_attempt(launch_request_id) {
            Ok(Some(attempt)) => Response::LaunchPending(attempt),
            Ok(None) => Response::LaunchStatusRejected(LaunchStatusRejection::UnknownLaunchRequest),
            Err(_) => persistence(),
        }
    }
}

/// Removes the per-launch bundle copies of a stopped flow: no launch that
/// bound it can still need them.
pub trait PrunesLaunchBundles {
    fn prune_launch_bundles_of(&self, flow_id: &str);
}

impl PrunesLaunchBundles for RunningNexus {
    fn prune_launch_bundles_of(&self, flow_id: &str) {
        match self.store.launch_requests_bound_to(flow_id) {
            Ok(launch_request_ids) => {
                for launch_request_id in launch_request_ids {
                    let _ = self.perform(Operation::Prune(launch_request_id));
                }
            }
            Err(error) => {
                eprintln!("flow-nexus: flow {flow_id} bundle copies not found: {error}");
            }
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
}

impl ResumesReplacement for RunningNexus {
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
            Outcome::Started(started) => Response::Started(started),
            Outcome::Failed(Failed_Data::Unstarted) => {
                Response::StartRejected(StartRejection::NativeLaunchRefused)
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
/// current answer on open, one LaunchPending frame per phase change, and the
/// outcome frame last. It waits on announced changes, never on a timer, and
/// holds the dispatch gate only while it promotes an ambiguous launch.
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
            let answer = self.launch_status(launch_request_id);
            let Response::LaunchPending(attempt) = &answer else {
                return send(&answer);
            };
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
    /// Runs for the life of the Nexus.
    fn promote_ambiguous_launches(&self) -> !;
}

impl PromotesAmbiguousLaunches for RunningNexus {
    fn promote_ambiguous_launches(&self) -> ! {
        let changes = self.store.launch_changes.clone();
        let mut watches = AmbiguousLaunchWatches::default();
        loop {
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
            if promote {
                nexus.promote_serially(&launch_request_id);
            }
        }
    }
}

trait PromotesObservedLaunch {
    fn watch_transcript(&self, intent: &PromptDeliveryIntent) -> Result<TranscriptWatch, String>;
    fn promote_serially(&self, launch_request_id: &str);
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
