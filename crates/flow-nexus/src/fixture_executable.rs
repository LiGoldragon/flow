//! Test-only shell-script executables that never raise ETXTBSY.
//!
//! A script written with `fs::write` from the test process holds a writable
//! descriptor for a moment. A test on another thread that forks inside that
//! moment inherits the descriptor, and it stays open until the fork execs
//! (`O_CLOEXEC` closes it only at exec). An exec of the script in that
//! window fails with "Text file busy", and a close or sync in the writing
//! thread cannot prevent it. So the script is written by a child `sh`: the
//! writable descriptor lives only in that single-threaded child, and the
//! finished file is renamed into place, so no process ever holds the
//! executable open for writing.

use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

/// One fixture script at its absolute path.
pub struct FixtureExecutable {
    pub path: PathBuf,
}

/// Installs or replaces the script body at the fixture's path.
pub trait InstallsScript {
    fn install(&self, body: &str);
}

impl InstallsScript for FixtureExecutable {
    fn install(&self, body: &str) {
        let mut writer = Command::new("sh")
            .args([
                "-c",
                "cat > \"$1.part\" && chmod 700 \"$1.part\" && mv -f \"$1.part\" \"$1\"",
                "fixture-executable",
            ])
            .arg(&self.path)
            .stdin(Stdio::piped())
            .spawn()
            .expect("fixture script writer");
        writer
            .stdin
            .take()
            .expect("fixture script writer input")
            .write_all(body.as_bytes())
            .expect("fixture script body");
        assert!(
            writer.wait().expect("fixture script writer exit").success(),
            "fixture script installed at {}",
            self.path.display()
        );
    }
}

/// A latch a fixture script waits behind: a FIFO the script opens for
/// reading (`: < '<path>'`), an open that blocks until a writer holds the
/// FIFO. Released, the latch holds the FIFO open for writing, so every
/// waiting open passes and every later one at once; dropped, it leaves a
/// plain file at the path, which never blocks an open.
pub struct Latch {
    pub path: PathBuf,
    held: std::sync::OnceLock<std::fs::File>,
}

pub trait HoldsLatch {
    /// Makes the FIFO at the latch's path.
    fn closed(path: PathBuf) -> Self;
    /// Opens the latch; releasing it again changes nothing.
    fn release(&self);
}

impl HoldsLatch for Latch {
    fn closed(path: PathBuf) -> Self {
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("latch path");
        // SAFETY: mkfifo reads a zero-terminated path that outlives the call.
        assert_eq!(
            unsafe { libc::mkfifo(name.as_ptr(), 0o600) },
            0,
            "latch FIFO at {}",
            path.display()
        );
        Self {
            path,
            held: std::sync::OnceLock::new(),
        }
    }

    fn release(&self) {
        self.held.get_or_init(|| {
            // Read and write: a FIFO opened so never blocks, and it is a
            // writer for as long as it stays open.
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.path)
                .expect("latch released")
        });
    }
}

impl Drop for Latch {
    fn drop(&mut self) {
        self.release();
        let plain = self.path.with_extension("released");
        if std::fs::write(&plain, b"").is_ok() {
            let _ = std::fs::rename(&plain, &self.path);
        }
    }
}

/// A directory tree whose changes are awaited, every directory in it
/// watched as it is made (notify's recursive inotify watch).
pub trait AwaitsTree {
    /// Blocks until `holds` answers true, asked once the watch is set and
    /// again at each change anywhere below this directory, or until
    /// `deadline` passes; whether it held.
    fn until_holds_below(
        &self,
        deadline: std::time::Instant,
        holds: &mut dyn FnMut() -> bool,
    ) -> bool;
}

impl AwaitsTree for std::path::Path {
    fn until_holds_below(
        &self,
        deadline: std::time::Instant,
        holds: &mut dyn FnMut() -> bool,
    ) -> bool {
        use notify::Watcher;
        let (changed, changes) = std::sync::mpsc::channel();
        let mut watcher = notify::recommended_watcher(move |_: notify::Result<notify::Event>| {
            let _ = changed.send(());
        })
        .expect("a watcher opens");
        std::fs::create_dir_all(self).expect("the watched tree exists");
        watcher
            .watch(self, notify::RecursiveMode::Recursive)
            .expect("the tree is watched");
        loop {
            if holds() {
                return true;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if changes.recv_timeout(remaining).is_err() {
                return holds();
            }
            // The changes already queued are one cue to ask again.
            while changes.try_recv().is_ok() {}
        }
    }
}
