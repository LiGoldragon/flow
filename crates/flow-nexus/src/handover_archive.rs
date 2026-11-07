//! Publication of closed handovers into a configured Git archive.

use crate::{
    RunningNexus,
    store::handover::{ClosedHandover, HandoverRemovalOutcome, HoldsHandovers},
};
use signal_flow::{Address, Aspect, HandoverArchiveIdentity, Model};
use std::{
    fs, io,
    path::{Path, PathBuf},
    process::Command,
};
use thiserror::Error;

/// The immutable handover document and its archive coordinates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverArchiveRequest {
    pub address: Address,
    pub flow_id: String,
    pub document: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverArchivePublication {
    pub path: PathBuf,
    pub commit: String,
    pub blob: String,
}

pub trait IdentifiesHandoverArchivePublication {
    fn identity(&self) -> HandoverArchiveIdentity;
}

impl IdentifiesHandoverArchivePublication for HandoverArchivePublication {
    fn identity(&self) -> HandoverArchiveIdentity {
        HandoverArchiveIdentity {
            git_commit: self.commit.clone(),
            archive_document_path: self.path.to_string_lossy().into_owned(),
            git_blob: self.blob.clone(),
        }
    }
}

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

pub trait PublishesHandoverArchive {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal>;
}

/// Runs archive publication before retiring closed Memory rows. The caller supplies
/// the resolved retention policy and configured publisher from its configuration view.
pub trait RetiresHandoverGenerations {
    fn retire_handover_generations(
        &self,
        address: &Address,
        retain: usize,
        publisher: &dyn PublishesHandoverArchive,
    ) -> Result<usize, HandoverRetirementRefusal>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HandoverRetirementRefusal {
    #[error("handover Memory failed: {0}")]
    Memory(String),
    #[error("handover archive refused publication: {0}")]
    Archive(#[from] HandoverArchiveRefusal),
    #[error("handover removal did not remove the exact generation: {0:?}")]
    Removal(HandoverRemovalOutcome),
}

impl RetiresHandoverGenerations for RunningNexus {
    fn retire_handover_generations(
        &self,
        address: &Address,
        retain: usize,
        publisher: &dyn PublishesHandoverArchive,
    ) -> Result<usize, HandoverRetirementRefusal> {
        let rows = self
            .store
            .closed_handovers(address, retain)
            .map_err(|e| HandoverRetirementRefusal::Memory(e.to_string()))?;
        let mut retired = 0;
        for closed in rows {
            let publication = match publisher.publish(HandoverArchiveRequest {
                address: address.clone(),
                flow_id: closed.predecessor.clone(),
                document: render_closed(&closed),
            }) {
                Ok(publication) => publication,
                Err(error) => {
                    self.store.fail_handover_retirement(
                        address,
                        format!("archive publication failed: {error}"),
                    );
                    return Err(error.into());
                }
            };
            match self
                .store
                .complete_closed_handover_retirement(address, &closed, publication.identity())
                .map_err(|e| HandoverRetirementRefusal::Memory(e.to_string()))?
            {
                HandoverRemovalOutcome::Removed => retired += 1,
                outcome => return Err(HandoverRetirementRefusal::Removal(outcome)),
            }
        }
        Ok(retired)
    }
}

fn render_closed(closed: &ClosedHandover) -> String {
    format!(
        "generation={} predecessor={} successor={} sections={:?} touch={:?}\n",
        closed.generation, closed.predecessor, closed.successor, closed.sections, closed.touch
    )
}

/// A publisher bound to one pre-existing Git work tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingGitHandoverArchive {
    repository: PathBuf,
}

pub trait AccessesExistingGitHandoverArchive: Sized {
    fn new(repository: impl Into<PathBuf>) -> Self;
    fn repository(&self) -> &Path;
}

impl AccessesExistingGitHandoverArchive for ExistingGitHandoverArchive {
    fn new(repository: impl Into<PathBuf>) -> Self {
        Self {
            repository: repository.into(),
        }
    }
    fn repository(&self) -> &Path {
        &self.repository
    }
}

impl PublishesHandoverArchive for ExistingGitHandoverArchive {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal> {
        ensure_repository(&self.repository)?;
        validate_component(&request.flow_id)?;
        validate_component(&request.address.topic)?;
        let aspect = aspect_name(&request.address.aspect);
        let model = model_name(&request.address.model);
        let relative = PathBuf::from(aspect)
            .join(&request.address.topic)
            .join(model)
            .join(format!("{}.md", request.flow_id));
        let target = self.repository.join(&relative);
        reject_symlink_components(&self.repository, &target)?;
        match fs::read_to_string(&target) {
            Ok(existing) => {
                if existing != request.document {
                    return Err(HandoverArchiveRefusal::Conflict);
                }
                if !is_tracked_and_clean(&self.repository, &target)? {
                    commit_target(&self.repository, &target)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(target.parent().expect("archive path has parent"))
                    .and_then(|_| fs::write(&target, request.document.as_bytes()))
                    .map_err(|error| HandoverArchiveRefusal::Write(error.to_string()))?;
                commit_target(&self.repository, &target)?;
            }
            Err(error) => return Err(HandoverArchiveRefusal::Write(error.to_string())),
        }
        let commit = run_git_output(&self.repository, ["rev-parse", "HEAD"])?
            .trim()
            .to_owned();
        let object = format!("{commit}:{}", path_string(&target, &self.repository));
        let blob = run_git_output(&self.repository, ["rev-parse", object.as_str()])?
            .trim()
            .to_owned();
        let archived = run_git_output(&self.repository, ["cat-file", "blob", blob.as_str()])?;
        if archived != request.document {
            return Err(HandoverArchiveRefusal::Conflict);
        }
        Ok(HandoverArchivePublication {
            path: relative,
            commit,
            blob,
        })
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

fn reject_symlink_components(
    repository: &Path,
    target: &Path,
) -> Result<(), HandoverArchiveRefusal> {
    let relative = target
        .strip_prefix(repository)
        .map_err(|_| HandoverArchiveRefusal::InvalidPath)?;
    let mut cursor = repository.to_path_buf();
    for component in relative.components() {
        cursor.push(component);
        match fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(HandoverArchiveRefusal::InvalidPath);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(HandoverArchiveRefusal::Write(error.to_string())),
        }
    }
    Ok(())
}
fn aspect_name(aspect: &Aspect) -> &'static str {
    match aspect {
        Aspect::Psyche => "Psyche",
        Aspect::Mind => "Mind",
        Aspect::Field => "Field",
    }
}
fn model_name(model: &Model) -> &'static str {
    match model {
        Model::Fable => "Fable",
        Model::Opus => "Opus",
        Model::Sonnet => "Sonnet",
        Model::Haiku => "Haiku",
        Model::Astra => "Astra",
        Model::Sol => "Sol",
        Model::Luna => "Luna",
        Model::Terra => "Terra",
    }
}
fn ensure_repository(repository: &Path) -> Result<(), HandoverArchiveRefusal> {
    if !repository.is_dir() {
        return Err(HandoverArchiveRefusal::Unconfigured);
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .map_err(|e| HandoverArchiveRefusal::Git(e.to_string()))?;
    if output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true" {
        Ok(())
    } else {
        Err(HandoverArchiveRefusal::Unconfigured)
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
    Ok(run_git_output(
        repository,
        ["status", "--porcelain", "--", relative.as_str()],
    )?
    .trim()
    .is_empty())
}
fn commit_target(repository: &Path, target: &Path) -> Result<(), HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    run_git(repository, ["add", "--", relative.as_str()])?;
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
        .map_err(|e| HandoverArchiveRefusal::Git(e.to_string()))?;
    if output.status.success() {
        String::from_utf8(output.stdout).map_err(|error| {
            HandoverArchiveRefusal::Git(format!("git returned non-UTF-8 evidence: {error}"))
        })
    } else {
        Err(HandoverArchiveRefusal::Git(
            String::from_utf8_lossy(&output.stderr).trim().into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn invalid_utf8_git_evidence_is_rejected_instead_of_aliasing_replacement_text() {
        let repository = tempfile::tempdir().unwrap();
        run_git(repository.path(), ["init", "-q"]).unwrap();
        let mut hash = Command::new("git")
            .arg("-C")
            .arg(repository.path())
            .args(["hash-object", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        hash.stdin.as_mut().unwrap().write_all(&[0xff]).unwrap();
        let output = hash.wait_with_output().unwrap();
        assert!(output.status.success());
        let blob = String::from_utf8(output.stdout).unwrap();
        assert!(matches!(
            run_git_output(repository.path(), ["cat-file", "blob", blob.trim()]),
            Err(HandoverArchiveRefusal::Git(_))
        ));
    }
}
