//! Waiting on events: data on a descriptor, a process's exit, a stop
//! request. A wait here blocks in poll(2) until one of its descriptors is
//! ready; a deadline, where one is given, only bounds how long an awaited
//! event may take, and nothing wakes on a timer to look again.

use std::{
    io,
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    path::Path,
    time::Instant,
};

/// Descriptors waited on together.
pub trait AwaitsDescriptors {
    /// Blocks until at least one descriptor is readable, or hung up, or
    /// until `deadline` passes. Answers, for each descriptor in order,
    /// whether it is ready; all false once the deadline has passed.
    fn ready(&self, deadline: Option<Instant>) -> io::Result<Vec<bool>>;
}

impl AwaitsDescriptors for [BorrowedFd<'_>] {
    fn ready(&self, deadline: Option<Instant>) -> io::Result<Vec<bool>> {
        let mut entries = self
            .iter()
            .map(|descriptor| libc::pollfd {
                fd: descriptor.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            })
            .collect::<Vec<_>>();
        loop {
            let timeout = match deadline {
                None => -1,
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    // Rounded up, so a wait never ends before its deadline.
                    i32::try_from(remaining.as_micros().div_ceil(1_000)).unwrap_or(i32::MAX)
                }
            };
            // SAFETY: `entries` is a valid array of `entries.len()` pollfd
            // structures for the duration of the call, and every descriptor
            // in it is borrowed for that duration.
            let answered =
                unsafe { libc::poll(entries.as_mut_ptr(), entries.len() as libc::nfds_t, timeout) };
            if answered < 0 {
                let error = io::Error::last_os_error();
                // A signal interrupted the wait: it is resumed with the time
                // the deadline still leaves.
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            return Ok(entries
                .iter()
                .map(|entry| entry.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0)
                .collect());
        }
    }
}

/// A process's exit as a descriptor: a pidfd, readable once the process
/// has ended, whether or not this process is its parent.
pub trait OpensExitDescriptor {
    /// The pidfd of the process with this id; Err when it is already gone
    /// or the kernel refuses one.
    fn exit_descriptor(&self) -> io::Result<OwnedFd>;
}

impl OpensExitDescriptor for u32 {
    fn exit_descriptor(&self) -> io::Result<OwnedFd> {
        let pid = libc::pid_t::try_from(*self)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: pidfd_open takes a pid and flags and returns a new
        // descriptor or -1; no memory is passed.
        let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptor was just opened and is owned by nothing else.
        Ok(unsafe { OwnedFd::from_raw_fd(descriptor as i32) })
    }
}

/// An event that, once announced, stays announced: an eventfd whose count
/// is written once and never read, so every wait on it, before or after the
/// announcement, sees it readable.
pub struct Announcement {
    descriptor: OwnedFd,
}

pub trait AnnouncesOnce: Sized {
    fn open() -> io::Result<Self>;
    fn announce(&self);
    fn descriptor(&self) -> BorrowedFd<'_>;
}

impl AnnouncesOnce for Announcement {
    fn open() -> io::Result<Self> {
        // SAFETY: eventfd takes an initial count and flags and returns a new
        // descriptor or -1; no memory is passed.
        let descriptor = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptor was just opened and is owned by nothing else.
        Ok(Self {
            descriptor: unsafe { OwnedFd::from_raw_fd(descriptor) },
        })
    }

    fn announce(&self) {
        let one = 1u64.to_ne_bytes();
        // SAFETY: the buffer holds the eight bytes an eventfd write takes.
        // A full count (EAGAIN) still leaves the descriptor readable.
        let _ = unsafe {
            libc::write(
                self.descriptor.as_raw_fd(),
                one.as_ptr().cast::<libc::c_void>(),
                one.len(),
            )
        };
    }

    fn descriptor(&self) -> BorrowedFd<'_> {
        self.descriptor.as_fd()
    }
}

/// A directory whose changes are awaited as inotify events.
pub trait AwaitsDirectory {
    /// Blocks until `holds` answers true, asked once the watch is set and
    /// again at each change inotify reports in this directory (an entry
    /// made, moved in or out, written, changed or removed), or until
    /// `deadline`, when one is given, passes; whether it held.
    fn holds_by(&self, deadline: Option<Instant>, holds: &mut dyn FnMut() -> bool) -> bool;

    /// [`Self::holds_by`] bounded by `deadline`.
    fn until_holds(&self, deadline: Instant, holds: &mut dyn FnMut() -> bool) -> bool {
        self.holds_by(Some(deadline), holds)
    }
}

impl AwaitsDirectory for Path {
    fn holds_by(&self, deadline: Option<Instant>, holds: &mut dyn FnMut() -> bool) -> bool {
        use std::os::unix::ffi::OsStrExt;
        let Ok(path) = std::ffi::CString::new(self.as_os_str().as_bytes()) else {
            return holds();
        };
        // SAFETY: inotify_init1 takes flags and returns a new descriptor or
        // -1; no memory is passed.
        let descriptor = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
        if descriptor < 0 {
            return holds();
        }
        // SAFETY: the descriptor was just opened and is owned by nothing else.
        let watch = unsafe { OwnedFd::from_raw_fd(descriptor) };
        let mask = libc::IN_CREATE
            | libc::IN_MOVED_TO
            | libc::IN_MOVED_FROM
            | libc::IN_CLOSE_WRITE
            | libc::IN_MODIFY
            | libc::IN_ATTRIB
            | libc::IN_DELETE;
        // SAFETY: the path is zero-terminated and outlives the call.
        if unsafe { libc::inotify_add_watch(watch.as_raw_fd(), path.as_ptr(), mask) } < 0 {
            return holds();
        }
        let mut events = [0_u8; 4096];
        loop {
            if holds() {
                return true;
            }
            match [watch.as_fd()].ready(deadline) {
                Ok(ready) if ready[0] => {}
                _ => return holds(),
            }
            // Every event read is only a cue to ask again.
            // SAFETY: read writes at most the buffer's length into it.
            while unsafe {
                libc::read(
                    watch.as_raw_fd(),
                    events.as_mut_ptr().cast::<libc::c_void>(),
                    events.len(),
                )
            } > 0
            {}
        }
    }
}

/// A path awaited until it exists.
pub trait AwaitsPath {
    /// Blocks until the path exists, read at each change in its directory,
    /// or until `deadline` passes; whether it exists.
    fn appeared_by(&self, deadline: Instant) -> bool;
}

impl AwaitsPath for Path {
    fn appeared_by(&self, deadline: Instant) -> bool {
        let Some(directory) = self.parent() else {
            return self.exists();
        };
        directory.until_holds(deadline, &mut || self.exists())
    }
}
