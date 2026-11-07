use super::NexusFixture;
use crate::handover_archive::{
    AccessesExistingGitHandoverArchive, ExistingGitHandoverArchive, HandoverArchivePublication,
    HandoverArchiveRefusal, HandoverArchiveRequest, PublishesHandoverArchive,
    RetiresHandoverGenerations,
};
use crate::reminding::ArchivesHandoverOnTick;
use crate::store::ManagesMetaflowState;
use crate::store::handover::HoldsHandovers;
use meta_signal_flow::ConfigurationChange;
use signal_flow::{Address, Aspect, Model};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

struct RecordingPublisher {
    calls: AtomicUsize,
    fail: bool,
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
            path: request.address.topic.into(),
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
            topic: "core".into(),
            model: Model::Sol,
        },
        flow_id: "abc123".into(),
        document: "handover\n".into(),
    }
}
#[test]
fn archives_and_is_idempotent() {
    let d = repo();
    let p = ExistingGitHandoverArchive::new(d.path());
    p.publish(request(d.path())).unwrap();
    p.publish(request(d.path())).unwrap();
    assert_eq!(runout(d.path(), &["rev-list", "--count", "HEAD"]), "1");
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
        topic: "retire".into(),
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
fn retention_boundary_and_zero_count_are_explicit() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Mind,
        topic: "retention".into(),
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
        topic: "orphaned".into(),
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
            archive.path().to_string_lossy().into_owned(),
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
