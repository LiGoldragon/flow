use super::*;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};
fn repo() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    run(d.path(), &["init", "-q"]);
    run(d.path(), &["config", "user.email", "test@example.com"]);
    run(d.path(), &["config", "user.name", "Test"]);
    d
}
fn run(path: &Path, args: &[&str]) {
    assert!(Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .status()
        .unwrap()
        .success());
}
fn request(path: &Path) -> HandoverArchiveRequest {
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
