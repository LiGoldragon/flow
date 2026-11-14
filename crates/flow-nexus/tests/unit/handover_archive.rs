use super::NexusFixture;
use crate::handover_archive::{
    AccessesExistingGitHandoverArchive, ExistingGitHandoverArchive, HandoverArchivePublication,
    HandoverArchiveRefusal, HandoverArchiveRequest, HandoverRetirementRefusal,
    PublishesHandoverArchive, RealGitCommand, RetiresHandoverGenerations, RunsGitCommand,
    SubstitutesGitRunner,
};
use crate::handover_observation::{ObservesHandoverRetirement, ReadsRetirementSubscription};
use crate::reminding::ArchivesHandoverOnTick;
use crate::store::ManagesMetaflowState;
use crate::store::handover::HoldsHandovers;
use crate::{Connection, ServesConnection};
use meta_signal_flow::ConfigurationChange;
use signal_flow::{Address, Aspect, Model, Output, RetirementObservationFailure};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::{
    fs,
    net::Shutdown,
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DelayedGitPhase {
    Inspection,
    Add,
}

struct DelayingRealGit {
    phase: DelayedGitPhase,
    entered: AtomicBool,
    deadlines: Mutex<Vec<Instant>>,
}

impl DelayingRealGit {
    fn new(phase: DelayedGitPhase) -> Self {
        Self {
            phase,
            entered: AtomicBool::new(false),
            deadlines: Mutex::new(Vec::new()),
        }
    }

    fn is_selected(&self, args: &[&str]) -> bool {
        match self.phase {
            DelayedGitPhase::Inspection => args == ["rev-parse", "--is-inside-work-tree"],
            DelayedGitPhase::Add => args.first() == Some(&"add"),
        }
    }
}

impl RunsGitCommand for DelayingRealGit {
    fn run(
        &self,
        repository: &Path,
        args: &[&str],
        deadline: Instant,
    ) -> Result<std::process::Output, HandoverArchiveRefusal> {
        self.deadlines.lock().unwrap().push(deadline);
        let output = RealGitCommand.run(repository, args, deadline)?;
        if self.is_selected(args) {
            self.entered.store(true, Ordering::SeqCst);
            if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                std::thread::sleep(remaining + Duration::from_millis(5));
            }
            return Err(HandoverArchiveRefusal::PublicationTimedOut);
        }
        Ok(output)
    }
}

struct RecordingPublisher {
    calls: AtomicUsize,
    fail: bool,
}

struct BlockingPublisher {
    entered: Arc<Barrier>,
    proceed: Arc<Barrier>,
}

impl PublishesHandoverArchive for BlockingPublisher {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal> {
        self.entered.wait();
        self.proceed.wait();
        Ok(HandoverArchivePublication {
            path: request.address.topic.0.into(),
            commit: "commit".into(),
            blob: "blob".into(),
        })
    }
}
impl PublishesHandoverArchive for RecordingPublisher {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(HandoverArchiveRefusal::Git("test failure".into()));
        }
        Ok(HandoverArchivePublication {
            path: request.address.topic.0.into(),
            commit: "test-commit".into(),
            blob: "test-blob".into(),
        })
    }
}
fn repo() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    run(d.path(), &["init", "-q"]);
    run(d.path(), &["config", "user.email", "test@example.com"]);
    run(d.path(), &["config", "user.name", "Test"]);
    d
}
fn run(path: &Path, args: &[&str]) {
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .status()
            .unwrap()
            .success()
    );
}
fn request(_path: &Path) -> HandoverArchiveRequest {
    HandoverArchiveRequest {
        address: Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Sol,
        },
        flow_id: "abc123".into(),
        document: "handover\n".into(),
    }
}

fn timed_retirement_at_phase(
    phase: DelayedGitPhase,
) -> (
    NexusFixture,
    tempfile::TempDir,
    Address,
    Arc<DelayingRealGit>,
) {
    let fixture = NexusFixture::new();
    let archive = repo();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic(format!("{:?}Deadline", phase)),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "before", "after")
        .unwrap();
    let runner = Arc::new(DelayingRealGit::new(phase));
    let publisher = ExistingGitHandoverArchive::new(archive.path())
        .with_deadline(Duration::from_millis(250))
        .with_git_runner(runner.clone());

    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 0, &publisher),
        Err(HandoverRetirementRefusal::Archive(
            HandoverArchiveRefusal::PublicationTimedOut
        ))
    );
    assert!(runner.entered.load(Ordering::SeqCst));
    let deadlines = runner.deadlines.lock().unwrap();
    assert!(deadlines.windows(2).all(|pair| pair[0] == pair[1]));
    drop(deadlines);
    let retained = fixture.nexus.store.closed_handovers(&address, 0).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].predecessor.0, "before");
    (fixture, archive, address, runner)
}

#[test]
fn inspection_phase_uses_whole_publication_budget_and_retains_generation() {
    let (_fixture, archive, address, runner) =
        timed_retirement_at_phase(DelayedGitPhase::Inspection);
    assert_eq!(runner.deadlines.lock().unwrap().len(), 1);
    assert!(
        !archive
            .path()
            .join(format!("Mind/{}/Sol/before.md", address.topic.0))
            .exists()
    );
}

#[test]
fn add_phase_uses_whole_publication_budget_and_retains_generation() {
    let (_fixture, archive, address, runner) = timed_retirement_at_phase(DelayedGitPhase::Add);
    assert!(runner.deadlines.lock().unwrap().len() >= 2);
    let relative = format!("Mind/{}/Sol/before.md", address.topic.0);
    assert!(archive.path().join(&relative).exists());
    assert_eq!(
        runout(archive.path(), &["diff", "--cached", "--name-only"]),
        relative
    );
    assert!(
        !Command::new("git")
            .arg("-C")
            .arg(archive.path())
            .args(["rev-parse", "--verify", "HEAD"])
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn post_commit_timeout_reverifies_same_generation_before_removal() {
    let fixture = NexusFixture::new();
    let archive = repo();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("RealPostCommitDeadline".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "before", "after")
        .unwrap();
    let hook_entered = archive.path().join("post-commit-entered");
    let hook = archive.path().join(".git/hooks/post-commit");
    fs::write(
        &hook,
        format!(
            "#!/bin/sh\necho entered > '{}'\nsleep 30\n",
            hook_entered.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    let timed =
        ExistingGitHandoverArchive::new(archive.path()).with_deadline(Duration::from_millis(500));
    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 0, &timed),
        Err(HandoverRetirementRefusal::Archive(
            HandoverArchiveRefusal::PublicationTimedOut
        ))
    );
    assert!(hook_entered.exists(), "the real post-commit hook entered");
    let retained = fixture.nexus.store.closed_handovers(&address, 0).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].predecessor.0, "before");
    let relative = format!("Mind/{}/Sol/before.md", address.topic.0);
    let commit = runout(archive.path(), &["rev-parse", "HEAD"]);
    let blob = runout(
        archive.path(),
        &["rev-parse", &format!("{commit}:{relative}")],
    );
    let committed = Command::new("git")
        .arg("-C")
        .arg(archive.path())
        .args(["cat-file", "blob", &blob])
        .output()
        .unwrap();
    assert!(committed.status.success());
    assert_eq!(
        committed.stdout,
        fs::read(archive.path().join(&relative)).unwrap()
    );
    assert_eq!(
        runout(archive.path(), &["rev-list", "--count", "HEAD"]),
        "1"
    );

    fs::remove_file(hook).unwrap();
    let subscription = fixture
        .nexus
        .store
        .observe_handover_retirement(&address)
        .unwrap();
    let publisher =
        ExistingGitHandoverArchive::new(archive.path()).with_deadline(Duration::from_secs(5));
    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 0, &publisher),
        Ok(1)
    );
    assert_eq!(
        runout(archive.path(), &["rev-list", "--count", "HEAD"]),
        "1"
    );
    assert!(matches!(
        subscription.next_response().unwrap(),
        Output::RetirementChanged(signal_flow::RetirementChange::ArchiveCommitted(_))
    ));
    assert!(matches!(
        subscription.next_response().unwrap(),
        Output::RetirementChanged(signal_flow::RetirementChange::GenerationRemoved(_))
    ));
    assert!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 0)
            .unwrap()
            .is_empty()
    );
}

#[cfg(unix)]
#[test]
fn separate_session_pipe_holder_cannot_extend_the_budget_or_remove_the_generation() {
    let fixture = NexusFixture::new();
    let archive = repo();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("SeparateSessionPipes".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "before", "after")
        .unwrap();
    let pid_path = archive.path().join("separate-session-pid");
    // The hook waits on the descendant's word through a FIFO: its read
    // returns once the descendant, in its own session, has written it.
    let ready_path = archive.path().join(".git/separate-session-ready");
    let hook = archive.path().join(".git/hooks/pre-commit");
    fs::write(
        &hook,
        format!(
            "#!/bin/sh\nexec 3>&1 4>&2\nmkfifo '{ready}'\nsetsid sh -c 'echo $$ > \"{pid}\"; echo ready > \"{ready}\"; sleep 30' >&3 2>&4 &\nread word < '{ready}'\nexit 0\n",
            pid = pid_path.display(),
            ready = ready_path.display(),
        ),
    )
    .unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    let publisher =
        ExistingGitHandoverArchive::new(archive.path()).with_deadline(Duration::from_millis(300));
    let started = Instant::now();
    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 0, &publisher),
        Err(HandoverRetirementRefusal::Archive(
            HandoverArchiveRefusal::PublicationTimedOut
        ))
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    let retained = fixture.nexus.store.closed_handovers(&address, 0).unwrap();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].predecessor.0, "before");
    let pid = fs::read_to_string(pid_path)
        .unwrap()
        .trim()
        .parse::<libc::pid_t>()
        .unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, 0);
    // The regression owns its deliberately detached fixture process.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
}

#[test]
fn archives_and_is_idempotent() {
    let d = repo();
    let p = ExistingGitHandoverArchive::new(d.path());
    let first = p.publish(request(d.path())).unwrap();
    let second = p.publish(request(d.path())).unwrap();
    assert_eq!(runout(d.path(), &["rev-list", "--count", "HEAD"]), "1");
    assert_eq!(first, second);
    assert_eq!(first.commit, runout(d.path(), &["rev-parse", "HEAD"]));
    assert_eq!(
        first.blob,
        runout(
            d.path(),
            &[
                "rev-parse",
                &format!("{}:{}", first.commit, first.path.display())
            ]
        )
    );
    assert_eq!(
        Command::new("git")
            .arg("-C")
            .arg(d.path())
            .args(["cat-file", "blob", &first.blob])
            .output()
            .unwrap()
            .stdout,
        b"handover\n"
    );
}
#[test]
fn preserves_foreign_index_and_worktree() {
    let d = repo();
    fs::write(d.path().join("foreign"), "keep").unwrap();
    run(d.path(), &["add", "--", "foreign"]);
    fs::write(d.path().join("untracked"), "keep").unwrap();
    ExistingGitHandoverArchive::new(d.path())
        .publish(request(d.path()))
        .unwrap();
    assert_eq!(
        runout(d.path(), &["status", "--porcelain", "--", "foreign"]),
        "A  foreign"
    );
    assert!(d.path().join("untracked").exists());
}
#[test]
fn failed_commit_keeps_pending_and_retry_commits() {
    let d = repo();
    let hook = d.path().join(".git/hooks/pre-commit");
    fs::write(&hook, "#!/bin/sh\nexit 1\n").unwrap();
    fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
    let p = ExistingGitHandoverArchive::new(d.path());
    assert!(matches!(
        p.publish(request(d.path())),
        Err(HandoverArchiveRefusal::Git(_))
    ));
    fs::remove_file(hook).unwrap();
    p.publish(request(d.path())).unwrap();
}
#[test]
fn conflict_and_traversal_are_refused() {
    let d = repo();
    let p = ExistingGitHandoverArchive::new(d.path());
    p.publish(request(d.path())).unwrap();
    let mut r = request(d.path());
    r.document = "other".into();
    assert_eq!(p.publish(r), Err(HandoverArchiveRefusal::Conflict));
    let mut r = request(d.path());
    r.flow_id = "../escape".into();
    assert_eq!(p.publish(r), Err(HandoverArchiveRefusal::InvalidPath));
}
#[test]
fn existing_read_error_is_not_absence() {
    let d = repo();
    let p = ExistingGitHandoverArchive::new(d.path());
    let target = d.path().join("Mind/core/Sol/abc123.md");
    fs::create_dir_all(&target).unwrap();
    assert!(matches!(
        p.publish(request(d.path())),
        Err(HandoverArchiveRefusal::Write(_))
    ));
}

#[test]
fn symlinked_archive_parent_is_refused() {
    let d = repo();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), d.path().join("Mind")).unwrap();
    assert_eq!(
        ExistingGitHandoverArchive::new(d.path()).publish(request(d.path())),
        Err(HandoverArchiveRefusal::InvalidPath)
    );
}

#[test]
fn retirement_publishes_before_removal_and_retries_failures() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("retire".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "one", "two")
        .unwrap();
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "two", "three")
        .unwrap();
    let failed = RecordingPublisher {
        calls: AtomicUsize::new(0),
        fail: true,
    };
    assert!(
        fixture
            .nexus
            .retire_handover_generations(&address, 1, &failed)
            .is_err()
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 1)
            .unwrap()
            .len(),
        1
    );
    let success = RecordingPublisher {
        calls: AtomicUsize::new(0),
        fail: false,
    };
    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 1, &success)
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 1)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(fixture.nexus.store.handover(&address).unwrap(), Vec::new());
}

#[test]
fn retirement_scan_read_failure_is_explicitly_incomplete() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("retirementScanReadFailure".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "one", "two")
        .unwrap();
    let subscription = fixture
        .nexus
        .store
        .observe_handover_retirement(&address)
        .unwrap();
    fixture.nexus.store.fail_next_closed_handover_read();
    assert!(
        fixture
            .nexus
            .retire_handover_generations(
                &address,
                0,
                &RecordingPublisher {
                    calls: AtomicUsize::new(0),
                    fail: false,
                },
            )
            .is_err()
    );
    assert!(matches!(
        subscription.next_response().unwrap(),
        Output::RetirementObservationFailed(RetirementObservationFailure::MissingEvidence(_))
    ));
    assert_eq!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 0)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn attaching_during_publication_sees_closed_then_archive_then_removed() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("attachDuringPublication".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "before", "after")
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let proceed = Arc::new(Barrier::new(2));
    let publisher = BlockingPublisher {
        entered: Arc::clone(&entered),
        proceed: Arc::clone(&proceed),
    };
    std::thread::scope(|scope| {
        let nexus = &fixture.nexus;
        let address_for_retirement = address.clone();
        let retirement = scope.spawn(move || {
            nexus.retire_handover_generations(&address_for_retirement, 0, &publisher)
        });
        entered.wait();
        let subscription = fixture
            .nexus
            .store
            .observe_handover_retirement(&address)
            .unwrap();
        assert_eq!(
            subscription.snapshot.handover_generation_key_vector.len(),
            1
        );
        proceed.wait();
        assert_eq!(retirement.join().unwrap(), Ok(1));
        assert!(matches!(
            subscription.next_response().unwrap(),
            signal_flow::Output::RetirementChanged(
                signal_flow::RetirementChange::ArchiveCommitted(_)
            )
        ));
        assert!(matches!(
            subscription.next_response().unwrap(),
            signal_flow::Output::RetirementChanged(
                signal_flow::RetirementChange::GenerationRemoved(_)
            )
        ));
    });
}

#[test]
fn initial_snapshot_write_failure_detaches_the_ephemeral_subscription() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("initialWriteFailure".into()),
        model: Model::Sol,
    };
    let subscription = fixture
        .nexus
        .store
        .observe_handover_retirement(&address)
        .unwrap();
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_observations
            .subscription_count(),
        1
    );
    let (server, client) = UnixStream::pair().unwrap();
    client.shutdown(Shutdown::Both).unwrap();
    drop(client);
    let mut connection = Connection {
        peer: server,
        process: None,
    };
    assert!(
        connection
            .serve_retirement(&fixture.nexus, subscription)
            .is_err()
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_observations
            .subscription_count(),
        0
    );
}

#[test]
fn retention_boundary_and_zero_count_are_explicit() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("retention".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "one", "two")
        .unwrap();
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "two", "three")
        .unwrap();
    let keep_all = RecordingPublisher {
        calls: AtomicUsize::new(0),
        fail: false,
    };
    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 2, &keep_all)
            .unwrap(),
        0
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 0)
            .unwrap()
            .len(),
        2
    );
    let archive_all = RecordingPublisher {
        calls: AtomicUsize::new(0),
        fail: false,
    };
    assert_eq!(
        fixture
            .nexus
            .retire_handover_generations(&address, 0, &archive_all)
            .unwrap(),
        2
    );
    assert!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.nexus.store.handover(&address).unwrap(), Vec::new());
}

#[test]
fn archive_tick_finds_closed_handover_without_a_metaflow_row() {
    let fixture = NexusFixture::new();
    let archive = repo();
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("orphaned".into()),
        model: Model::Sol,
    };
    fixture
        .nexus
        .store
        .close_handover_for_successor(&address, "predecessor", "successor")
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .list_metaflows()
            .unwrap()
            .iter()
            .all(|row| row.address != address)
    );
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::HandoverArchive(
            meta_signal_flow::generated::signal::Path(
                archive.path().to_string_lossy().into_owned(),
            ),
        ))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::HandoverCount(0))
        .unwrap();

    fixture.nexus.retire_handover_archives();

    assert!(
        archive
            .path()
            .join("Mind/orphaned/Sol/predecessor.md")
            .exists()
    );
    assert!(
        fixture
            .nexus
            .store
            .closed_handovers(&address, 0)
            .unwrap()
            .is_empty()
    );
    assert!(fixture.nexus.store.handover(&address).unwrap().is_empty());
}
fn runout(path: &Path, args: &[&str]) -> String {
    String::from_utf8(
        Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .into()
}
