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
