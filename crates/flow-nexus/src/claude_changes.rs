//! Native Claude changes. Directory watches survive an atomic replacement
//! of a transcript. Install the parent watch before discovering its children
//! and the project watches before reading any session's current activity.
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

pub(crate) struct ClaudeDirectories {
    pub descriptor: OwnedFd,
    projects: PathBuf,
    registry: PathBuf,
    watches: BTreeMap<i32, PathBuf>,
}

#[derive(Default)]
pub(crate) struct ClaudeChanges {
    pub topology: bool,
    pub registry: bool,
    pub sessions: BTreeSet<String>,
}

pub(crate) trait ReadsClaudeChanges: Sized {
    fn watch(projects: PathBuf, registry: PathBuf) -> io::Result<Self>;
    fn install(&mut self) -> io::Result<()>;
    fn changes(&mut self) -> io::Result<ClaudeChanges>;
}
trait TracksDirectory {
    fn track(&mut self, path: &Path) -> io::Result<()>;
}
impl TracksDirectory for ClaudeDirectories {
    fn track(&mut self, path: &Path) -> io::Result<()> {
        let name = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        let mask = libc::IN_ONLYDIR
            | libc::IN_CREATE
            | libc::IN_MOVED_TO
            | libc::IN_DELETE
            | libc::IN_MOVED_FROM
            | libc::IN_CLOSE_WRITE
            | libc::IN_MODIFY
            | libc::IN_MOVE_SELF
            | libc::IN_DELETE_SELF
            | libc::IN_ATTRIB;
        // SAFETY: name is NUL terminated; descriptor is ours.
        let wd =
            unsafe { libc::inotify_add_watch(self.descriptor.as_raw_fd(), name.as_ptr(), mask) };
        if wd < 0 {
            return Err(io::Error::last_os_error());
        }
        self.watches.insert(wd, path.to_owned());
        Ok(())
    }
}
impl ReadsClaudeChanges for ClaudeDirectories {
    fn watch(projects: PathBuf, registry: PathBuf) -> io::Result<Self> {
        // SAFETY: no pointer arguments; a successful call grants ownership.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut this = Self {
            descriptor,
            projects,
            registry,
            watches: BTreeMap::new(),
        };
        this.install()?;
        Ok(this)
    }
    fn install(&mut self) -> io::Result<()> {
        for target in [self.projects.clone(), self.registry.clone()] {
            // Watch the nearest *parent* first, even for an existing root:
            // it can itself be removed and replaced at the same pathname.
            for directory in target.ancestors().skip(1) {
                if directory.is_dir() {
                    self.track(directory)?;
                    break;
                }
            }
            if target.is_dir() {
                self.track(&target)?;
            }
        }
        if let Ok(entries) = self.projects.read_dir() {
            for project in entries.flatten() {
                if project.file_type().is_ok_and(|kind| kind.is_dir()) {
                    let _ = self.track(&project.path());
                }
            }
        }
        Ok(())
    }
    fn changes(&mut self) -> io::Result<ClaudeChanges> {
        let mut result = ClaudeChanges::default();
        let mut buf = [0u8; 16384];
        let header = std::mem::size_of::<libc::inotify_event>();
        loop {
            // SAFETY: buffer spans the full writable length passed to read.
            let n = unsafe {
                libc::read(
                    self.descriptor.as_raw_fd(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                )
            };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            if n == 0 {
                break;
            }
            let mut offset = 0;
            while offset + header <= n as usize {
                // SAFETY: a complete header is within bounds; the byte
                // buffer has no alignment requirement.
                let event = unsafe {
                    std::ptr::read_unaligned(buf[offset..].as_ptr().cast::<libc::inotify_event>())
                };
                let end = (offset + header + event.len as usize).min(n as usize);
                let bytes = &buf[offset + header..end];
                let name = OsStr::from_bytes(bytes.split(|b| *b == 0).next().unwrap_or_default());
                offset = end;
                if event.mask & libc::IN_Q_OVERFLOW != 0 {
                    result.topology = true;
                    continue;
                }
                let Some(base) = self.watches.get(&event.wd) else {
                    continue;
                };
                if event.mask & (libc::IN_IGNORED | libc::IN_MOVE_SELF | libc::IN_DELETE_SELF) != 0
                {
                    self.watches.remove(&event.wd);
                    result.topology = true;
                    continue;
                }
                let path = base.join(name);
                if self.projects.starts_with(&path)
                    || self.registry.starts_with(&path)
                    || (base == &self.projects && event.mask & libc::IN_ISDIR != 0)
                {
                    result.topology = true;
                } else if base == &self.registry && path.extension() == Some(OsStr::new("json")) {
                    result.registry = true;
                } else if base.parent() == Some(self.projects.as_path())
                    && path.extension() == Some(OsStr::new("jsonl"))
                    && let Some(session) = path.file_stem().and_then(OsStr::to_str)
                {
                    result.sessions.insert(session.into());
                }
            }
        }
        if result.topology {
            self.install()?;
        }
        Ok(result)
    }
}
