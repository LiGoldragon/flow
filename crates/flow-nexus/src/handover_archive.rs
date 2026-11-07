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
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;

const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

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
    #[error("archive publication timed out")]
    PublicationTimedOut,
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
        let rows = match self.store.closed_handovers(address, retain) {
            Ok(rows) => rows,
            Err(error) => {
                self.store.fail_handover_retirement(
                    address,
                    format!("closed generations could not be read from Memory: {error}"),
                );
                return Err(HandoverRetirementRefusal::Memory(error.to_string()));
            }
        };
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
    deadline: Duration,
}

pub trait AccessesExistingGitHandoverArchive: Sized {
    fn new(repository: impl Into<PathBuf>) -> Self;
    fn repository(&self) -> &Path;
}

impl AccessesExistingGitHandoverArchive for ExistingGitHandoverArchive {
    fn new(repository: impl Into<PathBuf>) -> Self {
        Self {
            repository: repository.into(),
            deadline: Duration::from_millis(60_000),
        }
    }
    fn repository(&self) -> &Path {
        &self.repository
    }
}

impl ExistingGitHandoverArchive {
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }
}

impl PublishesHandoverArchive for ExistingGitHandoverArchive {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal> {
        let deadline = Instant::now() + self.deadline;
        ensure_repository(&self.repository, deadline)?;
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
                if !is_tracked_and_clean(&self.repository, &target, deadline)? {
                    commit_target(&self.repository, &target, deadline)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(target.parent().expect("archive path has parent"))
                    .and_then(|_| fs::write(&target, request.document.as_bytes()))
                    .map_err(|error| HandoverArchiveRefusal::Write(error.to_string()))?;
                commit_target(&self.repository, &target, deadline)?;
            }
            Err(error) => return Err(HandoverArchiveRefusal::Write(error.to_string())),
        }
        let commit = run_git_output(&self.repository, ["rev-parse", "HEAD"], deadline)?
            .trim()
            .to_owned();
        let object = format!("{commit}:{}", path_string(&target, &self.repository));
        let blob = run_git_output(&self.repository, ["rev-parse", object.as_str()], deadline)?
            .trim()
            .to_owned();
        let archived = run_git_output(
            &self.repository,
            ["cat-file", "blob", blob.as_str()],
            deadline,
        )?;
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
fn ensure_repository(repository: &Path, deadline: Instant) -> Result<(), HandoverArchiveRefusal> {
    if !repository.is_dir() {
        return Err(HandoverArchiveRefusal::Unconfigured);
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output_with_deadline(deadline)?;
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
fn is_tracked_and_clean(
    repository: &Path,
    target: &Path,
    deadline: Instant,
) -> Result<bool, HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    if run_git_output(
        repository,
        ["ls-files", "--error-unmatch", "--", relative.as_str()],
        deadline,
    )
    .is_err()
    {
        return Ok(false);
    }
    Ok(run_git_output(
        repository,
        ["status", "--porcelain", "--", relative.as_str()],
        deadline,
    )?
    .trim()
    .is_empty())
}
fn commit_target(
    repository: &Path,
    target: &Path,
    deadline: Instant,
) -> Result<(), HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    run_git(repository, ["add", "--", relative.as_str()], deadline)?;
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
        deadline,
    )
}
fn run_git<const N: usize>(
    repository: &Path,
    args: [&str; N],
    deadline: Instant,
) -> Result<(), HandoverArchiveRefusal> {
    run_git_output(repository, args, deadline).map(|_| ())
}
fn run_git_output<const N: usize>(
    repository: &Path,
    args: [&str; N],
    deadline: Instant,
) -> Result<String, HandoverArchiveRefusal> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(args)
        .output_with_deadline(deadline)?;
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

trait RunsWithDeadline {
    fn output_with_deadline(
        &mut self,
        deadline: Instant,
    ) -> Result<std::process::Output, HandoverArchiveRefusal>;
}
impl RunsWithDeadline for Command {
    fn output_with_deadline(
        &mut self,
        deadline: Instant,
    ) -> Result<std::process::Output, HandoverArchiveRefusal> {
        // Git hooks are allowed to create descendants.  Put the command and
        // all of its descendants in one owned process group so a publication
        // timeout has one cleanup boundary rather than killing only git.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: the closure performs only async-signal-safe work and
            // is installed before spawning the child.
            unsafe {
                self.pre_exec(|| {
                    // SAFETY: setpgid is called in the child between fork
                    // and exec, and uses only the child process' own ids.
                    let result = libc::setpgid(0, 0);
                    if result == 0 {
                        Ok(())
                    } else {
                        Err(io::Error::last_os_error())
                    }
                });
            }
        }
        let mut child = self
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| HandoverArchiveRefusal::Git(e.to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HandoverArchiveRefusal::Git("git stdout was not piped".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| HandoverArchiveRefusal::Git("git stderr was not piped".into()))?;
        let (output_sender, output_receiver) = std::sync::mpsc::channel();
        let stdout_thread = thread::spawn({
            let sender = output_sender.clone();
            move || {
                let mut bytes = Vec::new();
                let result = io::Read::read_to_end(
                    &mut io::BufReader::new(stdout).take((MAX_CAPTURE_BYTES + 1) as u64),
                    &mut bytes,
                )
                .and_then(|_| {
                    if bytes.len() > MAX_CAPTURE_BYTES {
                        Err(io::Error::other("git output exceeded capture bound"))
                    } else {
                        Ok(())
                    }
                });
                let _ = sender.send((true, result.map(|_| bytes)));
            }
        });
        let stderr_thread = thread::spawn({
            let sender = output_sender;
            move || {
                let mut bytes = Vec::new();
                let result = io::Read::read_to_end(
                    &mut io::BufReader::new(stderr).take((MAX_CAPTURE_BYTES + 1) as u64),
                    &mut bytes,
                )
                .and_then(|_| {
                    if bytes.len() > MAX_CAPTURE_BYTES {
                        Err(io::Error::other("git output exceeded capture bound"))
                    } else {
                        Ok(())
                    }
                });
                let _ = sender.send((false, result.map(|_| bytes)));
            }
        });
        let process_id = child.id();
        let status = loop {
            match child
                .try_wait()
                .map_err(|e| HandoverArchiveRefusal::Git(e.to_string()))?
            {
                Some(exit) => {
                    break exit;
                }
                None if Instant::now() >= deadline => {
                    kill_process_group(process_id);
                    let _ = child.wait();
                    return Err(HandoverArchiveRefusal::PublicationTimedOut);
                }
                None => thread::sleep(Duration::from_millis(2)),
            }
        };
        let mut stdout = None;
        let mut stderr = None;
        while stdout.is_none() || stderr.is_none() {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(HandoverArchiveRefusal::PublicationTimedOut)?;
            let (is_stdout, bytes) = output_receiver.recv_timeout(remaining).map_err(|_| {
                kill_process_group(process_id);
                HandoverArchiveRefusal::PublicationTimedOut
            })?;
            let bytes = match bytes {
                Ok(bytes) => bytes,
                Err(error) => {
                    kill_process_group(process_id);
                    let _ = child.wait();
                    return Err(HandoverArchiveRefusal::Git(error.to_string()));
                }
            };
            if is_stdout {
                stdout = Some(bytes);
            } else {
                stderr = Some(bytes);
            }
        }
        let _ = stdout_thread.join();
        let _ = stderr_thread.join();
        Ok(std::process::Output {
            status,
            stdout: stdout.expect("stdout capture recorded"),
            stderr: stderr.expect("stderr capture recorded"),
        })
    }
}

fn kill_process_group(process_id: u32) {
    #[cfg(unix)]
    {
        // A negative pid addresses the process group whose id is the child
        // pid.  Ignore ESRCH: the group may have exited between observation
        // and cleanup.
        let _ = unsafe { libc::kill(-(process_id as libc::pid_t), libc::SIGKILL) };
    }
    #[cfg(not(unix))]
    {
        let _ = process_id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn invalid_utf8_git_evidence_is_rejected_instead_of_aliasing_replacement_text() {
        let repository = tempfile::tempdir().unwrap();
        run_git(
            repository.path(),
            ["init", "-q"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
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
            run_git_output(
                repository.path(),
                ["cat-file", "blob", blob.trim()],
                Instant::now() + Duration::from_secs(60)
            ),
            Err(HandoverArchiveRefusal::Git(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn publication_timeout_owns_and_kills_a_hanging_hook_process_group() {
        let repository = tempfile::tempdir().unwrap();
        run_git(
            repository.path(),
            ["init", "-q"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        run_git(
            repository.path(),
            ["config", "user.email", "flow@example.invalid"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        run_git(
            repository.path(),
            ["config", "user.name", "Flow Test"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        fs::create_dir_all(repository.path().join(".git/hooks")).unwrap();
        let hook = repository.path().join(".git/hooks/pre-commit");
        fs::write(&hook, "#!/bin/sh\nsleep 30\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        let publisher = ExistingGitHandoverArchive::new(repository.path())
            .with_deadline(Duration::from_millis(100));
        let result = publisher.publish(HandoverArchiveRequest {
            address: Address {
                aspect: Aspect::Mind,
                topic: "testTopic".into(),
                model: Model::Sol,
            },
            flow_id: "flow-test".into(),
            document: "generation=1\n".into(),
        });
        assert_eq!(result, Err(HandoverArchiveRefusal::PublicationTimedOut));
    }
}
