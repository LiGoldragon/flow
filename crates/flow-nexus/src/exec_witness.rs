//! The moment a fixture process's `execve` is complete, as the kernel
//! reports it to the process's tracer.
//!
//! A parent that spawns a program is woken before the program's new image
//! is laid out: glibc's `posix_spawn` wakes it from inside the child's
//! `execve` (at `mm_release`), and a close-on-exec pipe reaches EOF at
//! `do_close_on_exec`, both before `create_elf_tables` writes the new
//! environment's bounds. In that window `/proc/<pid>/environ` reads back
//! empty, which a reader cannot tell from a scrubbed environment. A tracer
//! is stopped with the tracee at `PTRACE_EVENT_EXEC`, which the kernel
//! raises once the binary loader has returned: the executable, the
//! arguments and the environment are then the new image's. Each witness
//! here waits for that stop, then detaches; nothing is re-read on a timer.
//!
//! The tracer is the thread that spawned or seized the process, and only
//! that thread may wait for it, so each witness runs in one call.

use std::io;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command};

/// One traced process, by its id.
pub(crate) struct Tracee(libc::pid_t);

/// What a traced process stopped for.
enum Stop {
    /// Its `execve` completed.
    Exec,
    /// It forked, vforked or cloned the process named.
    Forked(libc::pid_t),
    /// A plain SIGTRAP: for a process that asked to be traced and set no
    /// options, its `execve` completed.
    Trap,
    /// A signal stops it; it is resumed with that signal delivered.
    Signal(libc::c_int),
    /// A stop that delivers nothing on resume.
    Other,
}

trait TracesProcess {
    fn request(&self, request: libc::c_uint, data: usize) -> io::Result<()>;
    /// The next stop of this tracee; an exit is an error.
    fn stopped(&self) -> io::Result<Stop>;
    /// Resumes the tracee until its next stop, delivering `signal`.
    fn resumed(&self, signal: libc::c_int) -> io::Result<()>;
    fn detached(&self, signal: libc::c_int) -> io::Result<()>;
    /// Resumes the tracee through every stop until its `execve` completes
    /// into an image whose `comm` is `comm`, any image when none is given,
    /// then detaches it.
    fn released_at_exec(&self, comm: Option<&str>) -> io::Result<()>;
}

impl TracesProcess for Tracee {
    fn request(&self, request: libc::c_uint, data: usize) -> io::Result<()> {
        // SAFETY: a ptrace request on a tracee of this thread; the address
        // is unused and the data is an integer, never dereferenced here.
        let answer = unsafe {
            libc::ptrace(
                request,
                self.0,
                std::ptr::null_mut::<libc::c_void>(),
                data as *mut libc::c_void,
            )
        };
        if answer == -1 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn stopped(&self) -> io::Result<Stop> {
        let mut status = 0;
        // SAFETY: waitpid writes the status of this one process into the
        // integer it is given.
        if unsafe { libc::waitpid(self.0, &mut status, libc::__WALL) } == -1 {
            return Err(io::Error::last_os_error());
        }
        if !libc::WIFSTOPPED(status) {
            return Err(io::Error::other(format!(
                "traced process {} ended before its exec: status {status}",
                self.0
            )));
        }
        let signal = libc::WSTOPSIG(status);
        let event = status >> 16;
        Ok(match event {
            libc::PTRACE_EVENT_EXEC => Stop::Exec,
            libc::PTRACE_EVENT_FORK | libc::PTRACE_EVENT_VFORK | libc::PTRACE_EVENT_CLONE => {
                let mut child: libc::c_ulong = 0;
                // SAFETY: GETEVENTMSG writes one unsigned long, the new
                // process's id, into the integer it is given.
                let answer = unsafe {
                    libc::ptrace(
                        libc::PTRACE_GETEVENTMSG,
                        self.0,
                        std::ptr::null_mut::<libc::c_void>(),
                        &mut child as *mut libc::c_ulong,
                    )
                };
                if answer == -1 {
                    return Err(io::Error::last_os_error());
                }
                Stop::Forked(child as libc::pid_t)
            }
            0 if signal == libc::SIGTRAP => Stop::Trap,
            // A process traced from its fork starts stopped by SIGSTOP.
            0 if signal == libc::SIGSTOP => Stop::Other,
            0 => Stop::Signal(signal),
            _ => Stop::Other,
        })
    }

    fn resumed(&self, signal: libc::c_int) -> io::Result<()> {
        self.request(libc::PTRACE_CONT, signal as usize)
    }

    fn detached(&self, signal: libc::c_int) -> io::Result<()> {
        self.request(libc::PTRACE_DETACH, signal as usize)
    }

    fn released_at_exec(&self, comm: Option<&str>) -> io::Result<()> {
        loop {
            match self.stopped()? {
                Stop::Exec => {
                    let now = std::fs::read_to_string(format!("/proc/{}/comm", self.0))?;
                    if comm.is_none_or(|comm| now.trim_end() == comm) {
                        return self.detached(0);
                    }
                    self.resumed(0)?;
                }
                Stop::Signal(signal) => self.resumed(signal)?,
                Stop::Forked(_) | Stop::Trap | Stop::Other => self.resumed(0)?,
            }
        }
    }
}

trait ReleasesFirstTrap {
    /// Resumes a process that asked to be traced through any signal until
    /// the SIGTRAP its completed `execve` raises.
    fn trapped_at_exec(&self) -> io::Result<()>;
}

impl ReleasesFirstTrap for Tracee {
    fn trapped_at_exec(&self) -> io::Result<()> {
        loop {
            match self.stopped()? {
                Stop::Trap => return Ok(()),
                Stop::Signal(signal) => self.resumed(signal)?,
                Stop::Exec | Stop::Forked(_) | Stop::Other => self.resumed(0)?,
            }
        }
    }
}

/// A command spawned so that it is seen once its new image runs.
pub(crate) trait SpawnsWitnessedExec {
    /// Spawns the command, which asks to be traced before its `execve`,
    /// and returns once that `execve` completed: its executable,
    /// arguments and environment are the program's.
    fn spawn_after_exec(&mut self) -> io::Result<Child>;
    /// Spawns the command, follows the first process it starts, and
    /// returns once that process's `execve` completed into an image whose
    /// `comm` is `comm`, with that process's id.
    fn spawn_until_child_execs(&mut self, comm: &str) -> io::Result<(Child, u32)>;
}

trait AsksToBeTraced {
    fn traced_from_exec(&mut self) -> &mut Self;
}

impl AsksToBeTraced for Command {
    fn traced_from_exec(&mut self) -> &mut Self {
        // SAFETY: the hook makes one system call, which is
        // async-signal-safe, and touches no memory of the parent.
        unsafe {
            self.pre_exec(|| {
                let answer = libc::ptrace(
                    libc::PTRACE_TRACEME,
                    0,
                    std::ptr::null_mut::<libc::c_void>(),
                    std::ptr::null_mut::<libc::c_void>(),
                );
                if answer == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            })
        }
    }
}

impl SpawnsWitnessedExec for Command {
    fn spawn_after_exec(&mut self) -> io::Result<Child> {
        let child = self.traced_from_exec().spawn()?;
        // A process that asked to be traced is stopped with SIGTRAP once
        // its execve completed; detaching suppresses the signal.
        let tracee = Tracee(child.id() as libc::pid_t);
        tracee.trapped_at_exec()?;
        tracee.detached(0)?;
        Ok(child)
    }

    fn spawn_until_child_execs(&mut self, comm: &str) -> io::Result<(Child, u32)> {
        let child = self.traced_from_exec().spawn()?;
        let parent = Tracee(child.id() as libc::pid_t);
        parent.trapped_at_exec()?;
        parent.request(
            libc::PTRACE_SETOPTIONS,
            (libc::PTRACE_O_TRACEFORK
                | libc::PTRACE_O_TRACEVFORK
                | libc::PTRACE_O_TRACECLONE
                | libc::PTRACE_O_TRACEEXEC) as usize,
        )?;
        parent.resumed(0)?;
        let started = loop {
            match parent.stopped()? {
                // The process it starts is traced from its first
                // instruction, with these options; the parent is let go.
                Stop::Forked(started) => {
                    parent.detached(0)?;
                    break Tracee(started);
                }
                Stop::Signal(signal) => parent.resumed(signal)?,
                Stop::Exec | Stop::Trap | Stop::Other => parent.resumed(0)?,
            }
        };
        started.released_at_exec(Some(comm))?;
        Ok((child, started.0 as u32))
    }
}

/// A running descendant traced from now until its next `execve`.
pub(crate) trait SeizesUntilExec {
    /// Traces the process, which keeps running, so that it stops once its
    /// next `execve` completes.
    fn seized(process_id: u32) -> io::Result<Self>
    where
        Self: Sized;
    /// Waits for that stop, then detaches.
    fn exec_completed(self) -> io::Result<()>;
}

impl SeizesUntilExec for Tracee {
    fn seized(process_id: u32) -> io::Result<Self> {
        let tracee = Tracee(process_id as libc::pid_t);
        tracee.request(libc::PTRACE_SEIZE, libc::PTRACE_O_TRACEEXEC as usize)?;
        Ok(tracee)
    }

    fn exec_completed(self) -> io::Result<()> {
        self.released_at_exec(None)
    }
}
