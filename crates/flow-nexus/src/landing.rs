//! The approval pipeline.
//!
//! When the living approves a vision, its landing is a commit on the
//! context repository's `main`. Flow watches that repository's refs; every
//! commit on `main` that the context composer does not serve yet becomes a
//! landing row, and Flow carries each landing through its steps:
//!
//! 1. Deploy: the deploy executable is run for the newest landing, and the
//!    deployment is witnessed only by the context composer serving a
//!    revision that includes the landing's commit.
//! 2. Retrain: every live flow whose launch modules, with their dependency
//!    closure, include a path the landing changed is marked due a refresh;
//!    at its next rest Flow closes it, its metaflow sleeps, and work queued
//!    for the metaflow wakes a fresh flow composed from the deployed
//!    modules (`waking`).
//! 3. Implement: each changed module is queued as an Order to Mind
//!    `<subject>` Secondary, or to Mind core Secondary when no such
//!    metaflow is recorded.
//! 4. Report: a Result to Psyche core Secondary once every step is
//!    witnessed; an Order to it whenever a step fails.
//!
//! Retrain and Implement wait for the deploy, so a refreshed flow and the
//! implementer both wake into the landed modules. A failed step is tried
//! again at the next change of `main` and at the Nexus's next start; the
//! watch is a file watch on the ref files, never a timer.

use crate::codex_turns::SettlesRestingCodexSeats;
use crate::generated::landing::{
    ArgumentListing, ChangeCarrying, ChangedModule, Commit, CommitNaming, DeploysContext,
    FlowNaming, KeepsLandings, LandedChange, Landing, LandingCarrying, LandingOrder,
    LandingOrdering, LandingOrders, LandingWatching, Landings, ModuleIdentities, ModuleListing,
    ModuleSource, ModuleSourcing, NamesModule, ReadsContextRepository, RefreshCause, RefreshDue,
    RefreshDueCarrying, ReportsLanding, ResolvesModuleSources, Retrained, Retraining, RunsLandings,
    Step,
};
use crate::herdr::ReadsHerdrPanes;
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::store::landing::LandingSender;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{
    AnnouncesLaunchChanges, FlowStore, ManagesMetaflowState, NamesLiveFlow, ReadsFlowRows,
    ReadsLaunchAttempt,
};
use crate::waking::{SleepsAndWakesSeats, WakePolicy};
use crate::{HoldsStopRequest, RunningNexus};
use flow_ethos::{LandingWatch, State};
use notify::Watcher;
use signal_flow::{Address, Aspect, HarnessKind, Layer, Request};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How long one deploy may run before Flow ends it and records it failed.
const DEPLOY_BOUND: Duration = Duration::from_secs(30 * 60);

impl CommitNaming for str {
    fn commit(&self) -> Commit {
        self.to_owned()
    }
}

impl CommitNaming for String {
    fn commit(&self) -> Commit {
        self.clone()
    }
}

impl<T: CommitNaming + ?Sized> CommitNaming for &T {
    fn commit(&self) -> Commit {
        (**self).commit()
    }
}

impl FlowNaming for str {
    fn flow_id(&self) -> signal_flow::FlowId {
        self.to_owned()
    }
}

impl FlowNaming for String {
    fn flow_id(&self) -> signal_flow::FlowId {
        self.clone()
    }
}

impl<T: FlowNaming + ?Sized> FlowNaming for &T {
    fn flow_id(&self) -> signal_flow::FlowId {
        (**self).flow_id()
    }
}

impl ModuleListing for Vec<String> {
    fn module_identities(&self) -> ModuleIdentities {
        self.clone()
    }
}

impl<T: ModuleListing + ?Sized> ModuleListing for &T {
    fn module_identities(&self) -> ModuleIdentities {
        (**self).module_identities()
    }
}

impl ModuleSourcing for ModuleSource {
    fn module_source(&self) -> ModuleSource {
        self.clone()
    }
}

impl<T: ModuleSourcing + ?Sized> ModuleSourcing for &T {
    fn module_source(&self) -> ModuleSource {
        (**self).module_source()
    }
}

impl LandingCarrying for Landing {
    fn carried_landing(&self) -> Landing {
        self.clone()
    }
}

impl<T: LandingCarrying + ?Sized> LandingCarrying for &T {
    fn carried_landing(&self) -> Landing {
        (**self).carried_landing()
    }
}

impl RefreshDueCarrying for RefreshDue {
    fn carried_refresh_due(&self) -> RefreshDue {
        self.clone()
    }
}

impl LandingOrdering for Vec<LandingOrder> {
    fn landing_orders(&self) -> LandingOrders {
        self.clone()
    }
}

impl<T: LandingOrdering + ?Sized> LandingOrdering for &T {
    fn landing_orders(&self) -> LandingOrders {
        (**self).landing_orders()
    }
}

impl LandingWatching for LandingWatch {
    fn landing_watch(&self) -> LandingWatch {
        self.clone()
    }
}

impl<T: LandingWatching + ?Sized> LandingWatching for &T {
    fn landing_watch(&self) -> LandingWatch {
        (**self).landing_watch()
    }
}

impl ChangeCarrying for LandedChange {
    fn carried_change(&self) -> LandedChange {
        self.clone()
    }
}

impl<T: ChangeCarrying + ?Sized> ChangeCarrying for &T {
    fn carried_change(&self) -> LandedChange {
        (**self).carried_change()
    }
}

impl<const COUNT: usize> ArgumentListing for [&str; COUNT] {
    fn arguments(&self) -> Vec<String> {
        self.iter().map(|argument| (*argument).to_owned()).collect()
    }
}

/// A module file is `<type>/<subject>.md`, one level down.
impl NamesModule for str {
    fn module_identity(&self) -> Option<String> {
        let (kind, file) = self.split_once('/')?;
        let subject = file.strip_suffix(".md")?;
        (!kind.is_empty() && !subject.is_empty() && !subject.contains('/'))
            .then(|| format!("{kind}-{subject}"))
    }

    fn module_topic(&self) -> Option<String> {
        self.module_identity()?;
        let (_, file) = self.split_once('/')?;
        file.strip_suffix(".md").map(str::to_owned)
    }
}

impl ReadsContextRepository for LandingWatch {
    fn git<N: ArgumentListing>(&self, input: N) -> Result<String, String> {
        let arguments = input.arguments();
        let output = Command::new("git")
            .arg("-C")
            .arg(&self.context_repository)
            .args(&arguments)
            .stdin(Stdio::null())
            .output()
            .map_err(|error| format!("git {} did not run: {error}", arguments.join(" ")))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(format!(
                "git {} failed: {}",
                arguments.join(" "),
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }

    fn main_commit(&self) -> Result<Commit, String> {
        Ok(self
            .git(["rev-parse", "--verify", "refs/heads/main^{commit}"])?
            .trim()
            .to_owned())
    }

    fn git_directory(&self) -> Result<String, String> {
        Ok(self
            .git(["rev-parse", "--path-format=absolute", "--git-common-dir"])?
            .trim()
            .to_owned())
    }

    fn module_paths(&self) -> Result<Vec<String>, String> {
        Ok(self
            .git(["ls-tree", "-r", "--name-only", "refs/heads/main"])?
            .lines()
            .filter(|path| path.module_identity().is_some())
            .map(str::to_owned)
            .collect())
    }

    fn commits_after<N: CommitNaming>(&self, input: N) -> Result<Vec<Commit>, String> {
        let base = input.commit();
        if !self.includes(base.as_str(), "refs/heads/main") {
            return Err(format!("{base} is not on main"));
        }
        let range = format!("{base}..refs/heads/main");
        Ok(self
            .git(["rev-list", "--reverse", "--first-parent", range.as_str()])?
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect())
    }

    fn landed_change<N: CommitNaming>(&self, input: N) -> Result<LandedChange, String> {
        let commit = input.commit();
        let subject = self
            .git(["log", "-1", "--format=%s", commit.as_str()])?
            .trim()
            .to_owned();
        let parent = format!("{commit}~1");
        let paths = self.git(["diff", "--name-only", parent.as_str(), commit.as_str()])?;
        let mut changed_modules = Vec::new();
        for path in paths
            .lines()
            .filter(|path| path.module_identity().is_some())
        {
            let diff = self.git([
                "diff",
                "--unified=0",
                "--no-color",
                parent.as_str(),
                commit.as_str(),
                "--",
                path,
            ])?;
            let changed_lines = diff
                .lines()
                .filter(|line| {
                    (line.starts_with('+') && !line.starts_with("+++"))
                        || (line.starts_with('-') && !line.starts_with("---"))
                })
                .map(str::to_owned)
                .collect();
            changed_modules.push(ChangedModule {
                changed_path: path.to_owned(),
                changed_lines,
            });
        }
        Ok(LandedChange {
            commit,
            subject,
            changed_modules,
        })
    }

    fn includes<N: CommitNaming, O: CommitNaming>(&self, input_0: N, input_1: O) -> bool {
        let (ancestor, descendant) = (input_0.commit(), input_1.commit());
        self.git([
            "merge-base",
            "--is-ancestor",
            ancestor.as_str(),
            descendant.as_str(),
        ])
        .is_ok()
    }

    fn depth<N: CommitNaming>(&self, input: N) -> i64 {
        let commit = input.commit();
        self.git(["rev-list", "--count", commit.as_str()])
            .ok()
            .and_then(|count| count.trim().parse().ok())
            .unwrap_or(i64::MAX)
    }
}

/// The deploy executable takes one inline datom, `Deploy.{ Repository
/// Commit }`, and answers on standard output; a non-zero exit is a refusal.
/// What it prints is its claim: the deployment is witnessed only through the
/// context composer.
impl DeploysContext for LandingWatch {
    fn deploy<N: CommitNaming>(&self, input: N) -> Result<String, String> {
        if self.deploy_binary.is_empty() {
            return Err("no deploy executable is configured".into());
        }
        let request = format!(
            "Deploy.{{ {} {} }}",
            self.context_repository,
            input.commit()
        );
        let child = Command::new(&self.deploy_binary)
            .arg(&request)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("{} did not start: {error}", self.deploy_binary))?;
        let process_id = child.id();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(child.wait_with_output());
        });
        match receiver.recv_timeout(DEPLOY_BOUND) {
            Ok(Ok(output)) => {
                let printed = String::from_utf8_lossy(&output.stdout).trim().to_owned();
                if output.status.success() {
                    Ok(printed)
                } else {
                    Err(format!(
                        "{request} exited {}: {printed} {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    ))
                }
            }
            Ok(Err(error)) => Err(format!("{request} could not be waited on: {error}")),
            Err(_) => {
                if let Ok(process_id) = i32::try_from(process_id) {
                    // SAFETY: kill(2) on the child this thread spawned.
                    unsafe {
                        libc::kill(process_id, libc::SIGKILL);
                    }
                }
                Err(format!(
                    "{request} did not finish within {} minutes and was ended",
                    DEPLOY_BOUND.as_secs() / 60
                ))
            }
        }
    }
}

impl ReportsLanding for Landing {
    fn finished_report(&self) -> Option<Request> {
        let witnessed = |step: &Step| match step {
            Step::Witnessed(witness) => Some(witness.clone()),
            _ => None,
        };
        let deploy = witnessed(&self.deploy)?;
        let retrain = witnessed(&self.retrain)?;
        let implement = witnessed(&self.implement)?;
        Some(Request::Result(format!(
            "Approval pipeline: landing {} ({}) is deployed, its flows are marked for retraining, and its implementation is queued.\nDeploy: {deploy}\nRetrain: {retrain}\nImplement: {implement}",
            self.commit, self.subject
        )))
    }

    fn failure_report(&self) -> Option<Request> {
        let failed = [
            ("Deploy", &self.deploy),
            ("Retrain", &self.retrain),
            ("Implement", &self.implement),
        ]
        .into_iter()
        .filter_map(|(name, step)| match step {
            Step::Failed(reason) => Some(format!("{name} failed: {reason}")),
            _ => None,
        })
        .collect::<Vec<_>>();
        (!failed.is_empty()).then(|| {
            Request::Order(format!(
                "Approval pipeline: landing {} ({}) is blocked.\n{}\nFlow tries it again at the next change of the context repository's main, or at its next start. Unblock it, then rewrite refs/heads/main to retry now.",
                self.commit,
                self.subject,
                failed.join("\n")
            ))
        })
    }
}

impl RunsLandings for RunningNexus {
    fn watch_landings<N: LandingWatching>(&self, input: N) -> bool {
        let watch = input.landing_watch();
        let git_directory = match watch.git_directory() {
            Ok(directory) => std::path::PathBuf::from(directory),
            Err(error) => {
                eprintln!("flow-nexus: landings are not watched: {error}");
                return false;
            }
        };
        let moved = Arc::new(AtomicBool::new(true));
        let flag = Arc::clone(&moved);
        let changes = self.store.launch_changes.clone();
        let watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let Ok(event) = event else {
                return;
            };
            let concerns_main = event.paths.iter().any(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == "main" || name == "packed-refs")
            });
            if concerns_main {
                flag.store(true, Ordering::SeqCst);
                changes.announce();
            }
        });
        let mut watcher = match watcher {
            Ok(watcher) => watcher,
            Err(error) => {
                eprintln!("flow-nexus: landings are not watched: {error}");
                return false;
            }
        };
        for (path, mode) in [
            (
                git_directory.join("refs/heads"),
                notify::RecursiveMode::Recursive,
            ),
            // A push moves origin's main: a deploy refused for a landing not
            // yet pushed is tried again.
            (
                git_directory.join("refs/remotes"),
                notify::RecursiveMode::Recursive,
            ),
            (git_directory.clone(), notify::RecursiveMode::NonRecursive),
        ] {
            if let Err(error) = watcher.watch(&path, mode) {
                eprintln!(
                    "flow-nexus: landings are not watched at {}: {error}",
                    path.display()
                );
                return false;
            }
        }
        // A refresh the last Nexus marked and did not finish is taken up.
        self.settle_refreshes();
        let changes = self.store.launch_changes.clone();
        while !self.stopping.requested() {
            let seen = changes.current();
            if moved.swap(false, Ordering::SeqCst) {
                if let Err(error) = self.record_landings(&watch) {
                    eprintln!("flow-nexus: landings not recorded: {error}");
                }
                if let Err(error) = self.advance_landings(&watch) {
                    eprintln!("flow-nexus: landings not advanced: {error}");
                }
            }
            changes.after(seen);
        }
        true
    }

    fn served_module<N: LandingWatching>(&self, input: N) -> Result<ModuleSource, String> {
        let watch = input.landing_watch();
        let mut refusal = String::from("the repository holds no module file");
        for path in watch.module_paths()? {
            let Some(identity) = path.module_identity() else {
                continue;
            };
            match self.composer.module_sources(vec![identity.clone()]) {
                Ok(rows) => {
                    if let Some(row) = rows.into_iter().find(|row| {
                        row.module_identity == identity && row.source_relative_path == path
                    }) {
                        return Ok(row);
                    }
                    refusal = format!("the context composer serves {identity} from elsewhere");
                }
                Err(error) => refusal = error,
            }
        }
        Err(format!(
            "the context composer serves no module of this repository: {refusal}"
        ))
    }

    fn record_landings<N: LandingWatching>(&self, input: N) -> Result<Landings, String> {
        let watch = input.landing_watch();
        let served = self.served_module(&watch)?;
        let main = watch.main_commit()?;
        if watch.includes(main.as_str(), served.source_revision.as_str()) {
            return Ok(vec![]);
        }
        let mut recorded = Vec::new();
        for commit in watch.commits_after(served.source_revision.as_str())? {
            if self
                .store
                .landing_row(commit.as_str())
                .map_err(|error| error.to_string())?
                .is_some()
            {
                continue;
            }
            let change = watch.landed_change(commit.as_str())?;
            let landing = Landing {
                commit,
                base: served.source_revision.clone(),
                subject: change.subject,
                changed_paths: change
                    .changed_modules
                    .into_iter()
                    .map(|module| module.changed_path)
                    .collect(),
                deploy: Step::Pending,
                retrain: Step::Pending,
                retrainings: vec![],
                implement: Step::Pending,
                owners: vec![],
                reported: false,
            };
            self.store
                .write_landing(&landing)
                .map_err(|error| error.to_string())?;
            eprintln!(
                "flow-nexus: landing {} recorded ({} module files changed)",
                landing.commit,
                landing.changed_paths.len()
            );
            recorded.push(landing);
        }
        Ok(recorded)
    }

    fn advance_landings<N: LandingWatching>(&self, input: N) -> Result<Landings, String> {
        let watch = input.landing_watch();
        let mut open = self
            .store
            .landing_rows()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|landing| !landing.reported)
            .collect::<Vec<_>>();
        open.sort_by_key(|landing| watch.depth(landing.commit.as_str()));
        let report_to = FlowStore::landing_sender();

        // Deploy the newest landing not yet served, once.
        let mut served = self.served_module(&watch);
        let mut attempted: Option<Result<String, String>> = None;
        if let Some(newest) = open
            .iter()
            .rev()
            .find(|landing| !matches!(landing.deploy, Step::Witnessed(_)))
        {
            let covered = served.as_ref().is_ok_and(|module| {
                watch.includes(newest.commit.as_str(), module.source_revision.as_str())
            });
            if !covered {
                eprintln!("flow-nexus: landing {} deploying", newest.commit);
                attempted = Some(watch.deploy(newest.commit.as_str()));
                served = self.served_module(&watch);
            }
        }
        let answered = match &attempted {
            None => "no deploy was run".to_owned(),
            Some(Ok(printed)) => format!("the deploy answered {printed}"),
            Some(Err(refusal)) => format!("the deploy was refused: {refusal}"),
        };

        let mut advanced = Vec::new();
        for mut landing in open {
            if !matches!(landing.deploy, Step::Witnessed(_)) {
                landing.deploy = match &served {
                    Ok(module)
                        if watch
                            .includes(landing.commit.as_str(), module.source_revision.as_str()) =>
                    {
                        Step::Witnessed(format!(
                            "the context composer serves {} at {}, read from {}; {answered}",
                            module.repository_name, module.source_revision, module.module_identity
                        ))
                    }
                    Ok(module) => Step::Failed(format!(
                        "the context composer serves {} at {}, which does not include it; {answered}",
                        module.repository_name, module.source_revision
                    )),
                    Err(error) => Step::Failed(format!("{error}; {answered}")),
                };
                self.store
                    .write_landing(&landing)
                    .map_err(|error| error.to_string())?;
            }
            let deployed = matches!(landing.deploy, Step::Witnessed(_));
            if deployed
                && !matches!(landing.retrain, Step::Witnessed(_))
                && let Ok(module) = &served
            {
                landing = match self.retrain_flows(module, &landing) {
                    Ok(retrained) => retrained,
                    Err(error) => {
                        landing.retrain = Step::Failed(error);
                        self.store
                            .write_landing(&landing)
                            .map_err(|error| error.to_string())?;
                        landing
                    }
                };
            }
            if matches!(landing.deploy, Step::Witnessed(_))
                && !matches!(landing.implement, Step::Witnessed(_))
            {
                match watch.landed_change(landing.commit.as_str()) {
                    Ok(change) => {
                        let orders = self.implementation_orders(&change);
                        let unowned = orders
                            .iter()
                            .filter(|order| {
                                !matches!(self.store.metaflow(&order.address), Ok(Some(_)))
                            })
                            .map(|order| {
                                format!(
                                    "{:?} {} {:?}",
                                    order.address.aspect, order.address.topic, order.address.layer
                                )
                            })
                            .collect::<Vec<_>>();
                        if unowned.is_empty() {
                            let mut owners = Vec::new();
                            for order in &orders {
                                if !owners.contains(&order.address) {
                                    owners.push(order.address.clone());
                                }
                            }
                            landing.owners = owners.clone();
                            landing.implement = Step::Witnessed(if orders.is_empty() {
                                "no module file changed; nothing to implement".to_owned()
                            } else {
                                format!(
                                    "{} Orders queued, to {}",
                                    orders.len(),
                                    owners
                                        .iter()
                                        .map(|address| format!(
                                            "{:?} {} {:?}",
                                            address.aspect, address.topic, address.layer
                                        ))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            });
                            let queued = {
                                let _turn = self
                                    .dispatch_gate
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                self.store.queue_landing_orders(&landing, &orders)
                            };
                            match queued {
                                Ok(queued) => {
                                    for address in queued {
                                        self.wake_due(address);
                                    }
                                }
                                Err(error) => {
                                    landing.implement = Step::Failed(error.to_string());
                                    landing.owners = vec![];
                                }
                            }
                        } else {
                            // Nothing is queued until every change has its
                            // implementer, so a retry queues each change once.
                            landing.implement = Step::Failed(format!(
                                "no metaflow is recorded to implement it: {}",
                                unowned.join(", ")
                            ));
                        }
                    }
                    Err(error) => landing.implement = Step::Failed(error),
                }
                if matches!(landing.implement, Step::Failed(_)) {
                    self.store
                        .write_landing(&landing)
                        .map_err(|error| error.to_string())?;
                }
            }
            let report = match landing.finished_report() {
                Some(report) => {
                    landing.reported = true;
                    Some(report)
                }
                None => landing.failure_report(),
            };
            if let Some(report) = report {
                let queued = {
                    let _turn = self
                        .dispatch_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    self.store.queue_landing_orders(
                        &landing,
                        vec![LandingOrder {
                            address: report_to.clone(),
                            request: report,
                        }],
                    )
                };
                match queued {
                    Ok(queued) if queued.is_empty() => eprintln!(
                        "flow-nexus: landing {} report not queued: no metaflow is recorded at Psyche core Secondary",
                        landing.commit
                    ),
                    Ok(_) => self.wake_due(report_to.clone()),
                    Err(error) => eprintln!(
                        "flow-nexus: landing {} report not queued: {error}",
                        landing.commit
                    ),
                }
            }
            advanced.push(landing);
        }
        // A flow marked just now that already rests is ended now; the rows
        // answered are the ones that records.
        self.settle_refreshes();
        let mut settled = Vec::new();
        for landing in advanced {
            settled.push(
                self.store
                    .landing_row(landing.commit.as_str())
                    .map_err(|error| error.to_string())?
                    .unwrap_or(landing),
            );
        }
        Ok(settled)
    }

    fn retrain_flows<N: ModuleSourcing, O: LandingCarrying>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<Landing, String> {
        let module = input_0.module_source();
        let mut landing = input_1.carried_landing();
        let metaflows = self
            .store
            .list_metaflows()
            .map_err(|error| error.to_string())?;
        let mut marked = Vec::new();
        let mut unretrainable = Vec::new();
        let mut unchanged = 0_usize;
        for metaflow in metaflows {
            let State::Awake(flow_id) = &metaflow.state else {
                continue;
            };
            if !matches!(
                self.store.flow_node(flow_id),
                Ok(Some(node)) if node.flow_lifecycle.is_live()
            ) {
                continue;
            }
            let named = |address: &Address| {
                format!(
                    "{flow_id} ({:?} {} {:?})",
                    address.aspect, address.topic, address.layer
                )
            };
            if landing
                .retrainings
                .iter()
                .any(|retraining| &retraining.flow_id == flow_id)
            {
                continue;
            }
            let profile = self
                .store
                .launch_requests_bound_to(flow_id)
                .ok()
                .and_then(|mut requests| requests.pop())
                .and_then(|request| self.store.composable_launch_profile(&request).ok())
                .flatten();
            let Some(profile) = profile else {
                unretrainable.push(format!(
                    "{} has no launch profile",
                    named(&metaflow.address)
                ));
                continue;
            };
            let identities = profile
                .system_modules
                .iter()
                .chain(profile.user_modules.iter())
                .cloned()
                .collect::<Vec<_>>();
            match self.composer.module_sources(&identities) {
                Ok(rows) => {
                    if rows.iter().any(|row| {
                        row.repository_name == module.repository_name
                            && landing.changed_paths.contains(&row.source_relative_path)
                    }) {
                        marked.push(Retraining {
                            flow_id: flow_id.clone(),
                            address: metaflow.address.clone(),
                            retrained: Retrained::Marked,
                        });
                    } else {
                        unchanged += 1;
                    }
                }
                Err(error) => unretrainable.push(format!(
                    "{}: its modules do not resolve: {error}",
                    named(&metaflow.address)
                )),
            }
        }
        let names = marked
            .iter()
            .map(|retraining| {
                format!(
                    "{} ({:?} {} {:?})",
                    retraining.flow_id,
                    retraining.address.aspect,
                    retraining.address.topic,
                    retraining.address.layer
                )
            })
            .collect::<Vec<_>>();
        let witness = format!(
            "{} flows marked due a refresh at rest{}{}; {unchanged} live flows load no changed module{}",
            marked.len(),
            if names.is_empty() { "" } else { ": " },
            names.join(", "),
            if unretrainable.is_empty() {
                String::new()
            } else {
                format!("; not retrainable: {}", unretrainable.join("; "))
            }
        );
        {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for retraining in marked {
                landing.retrainings.push(retraining.clone());
                self.store
                    .mark_refresh_due(
                        &landing,
                        RefreshDue {
                            flow_id: retraining.flow_id,
                            refresh_cause: RefreshCause::Landed(landing.commit.clone()),
                        },
                    )
                    .map_err(|error| error.to_string())?;
            }
            landing.retrain = Step::Witnessed(witness);
            self.store
                .write_landing(&landing)
                .map_err(|error| error.to_string())?;
        }
        Ok(landing)
    }

    fn implementation_orders<N: ChangeCarrying>(&self, input: N) -> LandingOrders {
        let change = input.carried_change();
        let recorded = |address: &Address| matches!(self.store.metaflow(address), Ok(Some(_)));
        let core = Address {
            aspect: Aspect::Mind,
            topic: "core".into(),
            layer: Layer::Secondary,
        };
        change
            .changed_modules
            .iter()
            .filter_map(|module| {
                let topical = Address {
                    aspect: Aspect::Mind,
                    topic: module.changed_path.module_topic()?,
                    layer: Layer::Secondary,
                };
                // With neither recorded the Order is still made, to Mind core
                // Secondary; the pass refuses to queue an unowned landing.
                let address = if recorded(&topical) {
                    topical
                } else {
                    core.clone()
                };
                Some(LandingOrder {
                    address,
                    request: Request::Order(format!(
                        "Implement landed vision: context commit {} ({}) changed {}. Carry it into the code that implements it, or hand it to the flow that owns that code, and report to Psyche core Secondary once it is implemented.\nChanged lines:\n{}",
                        change.commit,
                        change.subject,
                        module.changed_path,
                        module.changed_lines.join("\n")
                    )),
                })
            })
            .collect()
    }

    fn settle_refreshes(&self) -> bool {
        let Ok(due) = self.store.refreshes_due() else {
            eprintln!("flow-nexus: refreshes due unreadable");
            return false;
        };
        for row in due {
            match self.store.flow_node(row.flow_id.as_str()) {
                Ok(Some(node)) if node.flow_lifecycle.is_live() => {
                    let node = self.herdr.refresh_route(node);
                    let resting = match node.harness_kind {
                        HarnessKind::Codex => self.codex_resting(row.flow_id.as_str()),
                        HarnessKind::Claude => matches!(
                            self.herdr.pane_agent(&node),
                            PaneAgent::Present { agent_state, .. } if Self::rests_at(&agent_state)
                        ),
                    };
                    if resting {
                        self.settle_stopped(row.flow_id.as_str());
                    }
                }
                Ok(_) => {
                    self.end_refresh(row.flow_id.as_str());
                }
                Err(error) => eprintln!(
                    "flow-nexus: flow {} due a refresh unreadable: {error}",
                    row.flow_id
                ),
            }
        }
        true
    }

    fn end_refresh<N: FlowNaming>(&self, input: N) -> bool {
        let flow_id = input.flow_id();
        let Ok(Some(due)) = self.store.refresh_due(flow_id.as_str()) else {
            return false;
        };
        let RefreshCause::Landed(commit) = due.refresh_cause;
        if let Ok(Some(mut landing)) = self.store.landing_row(commit.as_str()) {
            for retraining in landing.retrainings.iter_mut() {
                if retraining.flow_id == flow_id {
                    retraining.retrained = Retrained::Ended;
                }
            }
            if let Err(error) = self.store.write_landing(&landing) {
                eprintln!(
                    "flow-nexus: landing {commit} refresh of {flow_id} not recorded: {error}"
                );
            }
        }
        match self.store.clear_refresh_due(flow_id.as_str()) {
            Ok(cleared) => cleared,
            Err(error) => {
                eprintln!("flow-nexus: refresh of {flow_id} not cleared: {error}");
                false
            }
        }
    }
}
