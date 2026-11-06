//! Publication of closed handovers into the configured Git archive.

use signal_flow::{Address, Aspect};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use thiserror::Error;

/// The immutable document and its archive coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverArchiveRequest {
    pub repository: PathBuf,
    pub address: Address,
    pub model: String,
    pub flow_id: String,
    pub document: String,
}

/// The archive write completed and the document is committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverArchivePublication {
    pub path: PathBuf,
}

/// Why an archive publication did not complete.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HandoverArchiveRefusal {
    #[error("archive repository is not an existing Git work tree")]
    Unconfigured,
    #[error("archive path component is invalid")]
    InvalidPath,
    #[error("archive document conflicts with an existing file")]
    Conflict,
    #[error("archive Git command failed: {0}")]
    Git(String),
    #[error("archive write failed: {0}")]
    Write(String),
}

/// A configured publisher of closed handovers.
pub trait PublishesHandoverArchive {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal>;
}

/// The process-backed publisher. It never creates or initializes a repository.
#[derive(Debug, Default, Clone, Copy)]
pub struct GitHandoverArchive;

impl PublishesHandoverArchive for GitHandoverArchive {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal> {
        validate_component(&request.model)?;
        validate_component(&request.flow_id)?;
        let aspect = aspect_name(&request.address.aspect);
        validate_component(aspect)?;
        validate_component(&request.address.topic)?;
        ensure_repository(&request.repository)?;
        let relative = PathBuf::from(aspect)
            .join(&request.address.topic)
            .join(&request.model)
            .join(format!("{}.md", request.flow_id));
        let target = request.repository.join(relative);
        if let Ok(existing) = fs::read_to_string(&target) {
            if existing != request.document {
                return Err(HandoverArchiveRefusal::Conflict);
            }
            if is_tracked_and_clean(&request.repository, &target)? {
                return Ok(HandoverArchivePublication { path: target });
            }
            commit_target(&request.repository, &target)?;
            return Ok(HandoverArchivePublication { path: target });
        }
        if target.exists() {
            return Err(HandoverArchiveRefusal::Conflict);
        }
        fs::create_dir_all(target.parent().expect("archive path has parent"))
            .and_then(|_| fs::write(&target, request.document.as_bytes()))
            .map_err(|error| HandoverArchiveRefusal::Write(error.to_string()))?;
        // Stage and commit only this path.  The archive publisher shares its
        // repository with the rest of the psyche and must preserve unrelated
        // staged work.
        commit_target(&request.repository, &target)?;
        Ok(HandoverArchivePublication { path: target })
    }
}

fn validate_component(value: &str) -> Result<(), HandoverArchiveRefusal> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        Err(HandoverArchiveRefusal::InvalidPath)
    } else {
        Ok(())
    }
}

fn aspect_name(aspect: &Aspect) -> &'static str {
    match aspect {
        Aspect::Psyche => "Psyche",
        Aspect::Mind => "Mind",
        Aspect::Field => "Field",
    }
}

fn ensure_repository(repository: &Path) -> Result<(), HandoverArchiveRefusal> {
    if !repository.is_dir() {
        return Err(HandoverArchiveRefusal::Unconfigured);
    }
    let output = Command::new("git")
        .args([
            "-C",
            &repository.to_string_lossy(),
            "rev-parse",
            "--is-inside-work-tree",
        ])
        .output()
        .map_err(|error| HandoverArchiveRefusal::Git(error.to_string()))?;
    if !output.status.success() || String::from_utf8_lossy(&output.stdout).trim() != "true" {
        Err(HandoverArchiveRefusal::Unconfigured)
    } else {
        Ok(())
    }
}

fn path_string(target: &Path, repository: &Path) -> String {
    target
        .strip_prefix(repository)
        .unwrap_or(target)
        .to_string_lossy()
        .into_owned()
}

fn is_tracked_and_clean(repository: &Path, target: &Path) -> Result<bool, HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    if run_git_output(
        repository,
        ["ls-files", "--error-unmatch", "--", relative.as_str()],
    )
    .is_err()
    {
        return Ok(false);
    }
    let status = run_git_output(
        repository,
        ["status", "--porcelain", "--", relative.as_str()],
    )?;
    Ok(status.trim().is_empty())
}

fn commit_target(repository: &Path, target: &Path) -> Result<(), HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    run_git(repository, ["add", "--", relative.as_str()])?;
    // --only is essential: the repository may contain unrelated staged work.
    run_git(
        repository,
        [
            "commit",
            "--only",
            "-m",
            "Archive handover",
            "--",
            relative.as_str(),
        ],
    )
}

fn run_git<const N: usize>(
    repository: &Path,
    args: [&str; N],
) -> Result<(), HandoverArchiveRefusal> {
    run_git_output(repository, args).map(|_| ())
}

fn run_git_output<const N: usize>(
    repository: &Path,
    args: [&str; N],
) -> Result<String, HandoverArchiveRefusal> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(args)
        .output()
        .map_err(|error| HandoverArchiveRefusal::Git(error.to_string()))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(HandoverArchiveRefusal::Git(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn repository() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        run(&directory, ["init", "-q"]);
        run(&directory, ["config", "user.email", "test@example.com"]);
        run(&directory, ["config", "user.name", "Test"]);
        directory
    }

    fn run<const N: usize>(directory: &tempfile::TempDir, args: [&str; N]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(directory.path())
            .args(args)
            .status()
            .unwrap();
        assert!(status.success());
    }

    fn request(repository: &Path) -> HandoverArchiveRequest {
        HandoverArchiveRequest {
            repository: repository.to_owned(),
            address: Address {
                model: signal_flow::Model::Sol,
            },
            model: "Model".into(),
            flow_id: "abc123".into(),
            document: "handover\n".into(),
        }
    }

    #[test]
    fn publication_preserves_foreign_staged_work() {
        let directory = repository();
        fs::write(directory.path().join("foreign"), "keep").unwrap();
        run(&directory, ["add", "--", "foreign"]);
        GitHandoverArchive
            .publish(request(directory.path()))
            .unwrap();
        let status = Command::new("git")
            .arg("-C")
            .arg(directory.path())
            .args(["status", "--porcelain", "--", "foreign"])
            .output()
            .unwrap();
        assert_eq!(String::from_utf8_lossy(&status.stdout).trim(), "A  foreign");
    }

    #[test]
    fn identical_pending_document_is_retryable() {
        let directory = repository();
        let target = directory.path().join("Mind/core/Model/abc123.md");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "handover\n").unwrap();
        let first = GitHandoverArchive.publish(request(directory.path()));
        assert!(first.is_ok());
        let second = GitHandoverArchive.publish(request(directory.path()));
        assert!(second.is_ok());
    }
}
