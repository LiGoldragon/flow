//! Publication of closed handovers into a configured Git archive.

use crate::{
    RunningNexus,
    store::handover::{ClosedHandover, HandoverRemovalOutcome, HoldsHandovers},
};
use signal_flow::{Address, Aspect, HandoverArchiveIdentity, Model};
use std::{
    fs, io,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;

const MAX_CAPTURE_BYTES: usize = 1024 * 1024;

/// The immutable handover document and its archive coordinates.
#[derive(Clone)]
pub struct HandoverArchiveRequest {
    pub address: Address,
    pub flow_id: String,
    pub document: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone)]
pub struct ExistingGitHandoverArchive {
    repository: PathBuf,
    deadline: Duration,
    runner: Arc<dyn RunsGitCommand>,
}

pub(crate) trait RunsGitCommand: Send + Sync {
    fn run(
        &self,
        repository: &Path,
        args: &[&str],
        deadline: Instant,
    ) -> Result<std::process::Output, HandoverArchiveRefusal>;
}

pub(crate) struct RealGitCommand;
impl RunsGitCommand for RealGitCommand {
    fn run(
        &self,
        repository: &Path,
        args: &[&str],
        deadline: Instant,
    ) -> Result<std::process::Output, HandoverArchiveRefusal> {
        let mut command = Command::new("git");
        command.arg("-C").arg(repository).args(args);
        command.output_with_deadline(deadline)
    }
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
            runner: Arc::new(RealGitCommand),
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

    #[cfg(test)]
    pub(crate) fn with_git_runner(mut self, runner: Arc<dyn RunsGitCommand>) -> Self {
        self.runner = runner;
        self
    }
}

impl PublishesHandoverArchive for ExistingGitHandoverArchive {
    fn publish(
        &self,
        request: HandoverArchiveRequest,
    ) -> Result<HandoverArchivePublication, HandoverArchiveRefusal> {
        let deadline = Instant::now() + self.deadline;
        ensure_repository(self.runner.as_ref(), &self.repository, deadline)?;
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
                if !is_tracked_and_clean(self.runner.as_ref(), &self.repository, &target, deadline)?
                {
                    commit_target(self.runner.as_ref(), &self.repository, &target, deadline)?;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(target.parent().expect("archive path has parent"))
                    .and_then(|_| fs::write(&target, request.document.as_bytes()))
                    .map_err(|error| HandoverArchiveRefusal::Write(error.to_string()))?;
                commit_target(self.runner.as_ref(), &self.repository, &target, deadline)?;
            }
            Err(error) => return Err(HandoverArchiveRefusal::Write(error.to_string())),
        }
        let commit = run_git_output(
            self.runner.as_ref(),
            &self.repository,
            ["rev-parse", "HEAD"],
            deadline,
        )?
        .trim()
        .to_owned();
        let object = format!("{commit}:{}", path_string(&target, &self.repository));
        let blob = run_git_output(
            self.runner.as_ref(),
            &self.repository,
            ["rev-parse", object.as_str()],
            deadline,
        )?
        .trim()
        .to_owned();
        let archived = run_git_output(
            self.runner.as_ref(),
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
fn ensure_repository(
    runner: &dyn RunsGitCommand,
    repository: &Path,
    deadline: Instant,
) -> Result<(), HandoverArchiveRefusal> {
    if !repository.is_dir() {
        return Err(HandoverArchiveRefusal::Unconfigured);
    }
    let output = runner.run(
        repository,
        &["rev-parse", "--is-inside-work-tree"],
        deadline,
    )?;
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
    runner: &dyn RunsGitCommand,
    repository: &Path,
    target: &Path,
    deadline: Instant,
) -> Result<bool, HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    match run_git_output(
        runner,
        repository,
        ["ls-files", "--error-unmatch", "--", relative.as_str()],
        deadline,
    ) {
        Ok(_) => {}
        Err(HandoverArchiveRefusal::PublicationTimedOut) => {
            return Err(HandoverArchiveRefusal::PublicationTimedOut);
        }
        Err(_) => return Ok(false),
    }
    Ok(run_git_output(
        runner,
        repository,
        ["status", "--porcelain", "--", relative.as_str()],
        deadline,
    )?
    .trim()
    .is_empty())
}
fn commit_target(
    runner: &dyn RunsGitCommand,
    repository: &Path,
    target: &Path,
    deadline: Instant,
) -> Result<(), HandoverArchiveRefusal> {
    let relative = path_string(target, repository);
    run_git(
        runner,
        repository,
        ["add", "--", relative.as_str()],
        deadline,
    )?;
    run_git(
        runner,
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
    runner: &dyn RunsGitCommand,
    repository: &Path,
    args: [&str; N],
    deadline: Instant,
) -> Result<(), HandoverArchiveRefusal> {
    run_git_output(runner, repository, args, deadline).map(|_| ())
}
fn run_git_output<const N: usize>(
    runner: &dyn RunsGitCommand,
    repository: &Path,
    args: [&str; N],
    deadline: Instant,
) -> Result<String, HandoverArchiveRefusal> {
    let output = runner.run(repository, &args, deadline)?;
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
        // File preparation may have consumed the entire publication budget.
        // An expired budget never starts a Git process or any of its hooks.
        if Instant::now() >= deadline {
            return Err(HandoverArchiveRefusal::PublicationTimedOut);
        }
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
        let mut stdout = stdout;
        let mut stderr = stderr;
        let process_id = child.id();
        if let Err(error) = make_nonblocking(&stdout).and_then(|_| make_nonblocking(&stderr)) {
            terminate_process_group(&mut child, process_id);
            return Err(error);
        }
        let mut stdout_bytes = Vec::new();
        let mut stderr_bytes = Vec::new();
        let mut stdout_eof = false;
        let mut stderr_eof = false;
        let mut status = None;
        while status.is_none() || !stdout_eof || !stderr_eof {
            if !stdout_eof {
                stdout_eof = match drain_capture(&mut stdout, &mut stdout_bytes) {
                    Ok(eof) => eof,
                    Err(error) => {
                        terminate_process_group(&mut child, process_id);
                        return Err(error);
                    }
                };
            }
            if !stderr_eof {
                stderr_eof = match drain_capture(&mut stderr, &mut stderr_bytes) {
                    Ok(eof) => eof,
                    Err(error) => {
                        terminate_process_group(&mut child, process_id);
                        return Err(error);
                    }
                };
            }
            if status.is_none() {
                status = child
                    .try_wait()
                    .map_err(|error| HandoverArchiveRefusal::Git(error.to_string()))?;
            }
            if status.is_some() && stdout_eof && stderr_eof {
                break;
            }
            if Instant::now() >= deadline {
                terminate_process_group(&mut child, process_id);
                // Dropping our nonblocking read ends is sufficient even when a
                // hook deliberately moved a pipe-holding descendant into a
                // different session. There is no reader thread left to join.
                return Err(HandoverArchiveRefusal::PublicationTimedOut);
            }
            thread::sleep(Duration::from_millis(2));
        }
        Ok(std::process::Output {
            status: status.expect("Git exited before both pipes reached EOF"),
            stdout: stdout_bytes,
            stderr: stderr_bytes,
        })
    }
}

#[cfg(unix)]
fn make_nonblocking(reader: &impl std::os::fd::AsRawFd) -> Result<(), HandoverArchiveRefusal> {
    let descriptor = std::os::fd::AsRawFd::as_raw_fd(reader);
    // SAFETY: fcntl reads and updates flags on this live, owned descriptor.
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
    {
        return Err(HandoverArchiveRefusal::Git(
            io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn make_nonblocking(_reader: &impl Sized) -> Result<(), HandoverArchiveRefusal> {
    Ok(())
}

fn drain_capture(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
) -> Result<bool, HandoverArchiveRefusal> {
    let mut chunk = [0_u8; 8192];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(true),
            Ok(count) => {
                if bytes.len().saturating_add(count) > MAX_CAPTURE_BYTES {
                    return Err(HandoverArchiveRefusal::Git(
                        "git output exceeded capture bound".into(),
                    ));
                }
                bytes.extend_from_slice(&chunk[..count]);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(HandoverArchiveRefusal::Git(error.to_string())),
        }
    }
}

fn terminate_process_group(child: &mut std::process::Child, process_id: u32) {
    kill_process_group(process_id);
    let _ = child.wait();
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
    fn expired_deadline_never_spawns_the_command() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("spawned");
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(format!("printf spawned > '{}'", marker.display()));
        assert!(matches!(
            command.output_with_deadline(Instant::now()),
            Err(HandoverArchiveRefusal::PublicationTimedOut)
        ));
        assert!(!marker.exists(), "an expired command had no side effect");
    }

    #[test]
    fn invalid_utf8_git_evidence_is_rejected_instead_of_aliasing_replacement_text() {
        let repository = tempfile::tempdir().unwrap();
        run_git(
            &RealGitCommand,
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
                &RealGitCommand,
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
            &RealGitCommand,
            repository.path(),
            ["init", "-q"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        run_git(
            &RealGitCommand,
            repository.path(),
            ["config", "user.email", "flow@example.invalid"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        run_git(
            &RealGitCommand,
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

    #[cfg(unix)]
    #[test]
    fn publication_timeout_kills_a_descendant_holding_capture_pipes_after_git_exits() {
        let repository = tempfile::tempdir().unwrap();
        for args in [
            &["init", "-q"][..],
            &["config", "user.email", "flow@example.invalid"][..],
            &["config", "user.name", "Flow Test"][..],
        ] {
            RealGitCommand
                .run(
                    repository.path(),
                    args,
                    Instant::now() + Duration::from_secs(60),
                )
                .unwrap();
        }
        let child_pid = repository.path().join("descendant-pid");
        let hook = repository.path().join(".git/hooks/pre-commit");
        fs::write(
            &hook,
            format!(
                "#!/bin/sh\nsleep 30 &\necho $! > '{}'\nexit 0\n",
                child_pid.display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        let result = ExistingGitHandoverArchive::new(repository.path())
            .with_deadline(Duration::from_millis(200))
            .publish(HandoverArchiveRequest {
                address: Address {
                    aspect: Aspect::Mind,
                    topic: "descendantPipes".into(),
                    model: Model::Sol,
                },
                flow_id: "flow-descendant".into(),
                document: "generation=1\n".into(),
            });
        assert_eq!(result, Err(HandoverArchiveRefusal::PublicationTimedOut));
        let pid = fs::read_to_string(child_pid)
            .unwrap()
            .trim()
            .parse::<libc::pid_t>()
            .unwrap();
        for _ in 0..100 {
            if unsafe { libc::kill(pid, 0) } == -1 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }

    #[cfg(unix)]
    #[test]
    fn publication_rejects_unbounded_hook_output() {
        let repository = tempfile::tempdir().unwrap();
        run_git(
            &RealGitCommand,
            repository.path(),
            ["init", "-q"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        run_git(
            &RealGitCommand,
            repository.path(),
            ["config", "user.email", "flow@example.invalid"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        run_git(
            &RealGitCommand,
            repository.path(),
            ["config", "user.name", "Flow Test"],
            Instant::now() + Duration::from_secs(60),
        )
        .unwrap();
        let hook = repository.path().join(".git/hooks/pre-commit");
        fs::write(&hook, "#!/bin/sh\nyes output\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&hook, fs::Permissions::from_mode(0o755)).unwrap();
        let result = ExistingGitHandoverArchive::new(repository.path())
            .with_deadline(Duration::from_secs(2))
            .publish(HandoverArchiveRequest {
                address: Address {
                    aspect: Aspect::Mind,
                    topic: "testTopic".into(),
                    model: Model::Sol,
                },
                flow_id: "flow-output-bound".into(),
                document: "generation=1\n".into(),
            });
        assert!(
            matches!(result, Err(HandoverArchiveRefusal::Git(message)) if message.contains("capture bound"))
        );
    }
}
