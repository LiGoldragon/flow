//! A socket placed at its name in a place's Runtime only once it listens.
//!
//! A place's Runtime is shared with every other Nexus at it, and a peer
//! watching for a socket waits on one event: the socket's name moved in
//! (inotify's `IN_MOVED_TO`). The socket is bound under a temporary name
//! beside its name, `.<name>.<pid>`, its mode set to `0600`, made to listen,
//! then renamed to its name; rename(2) is atomic, so the name never holds a
//! socket that does not listen. A watcher that sees the name moved in
//! connects at once.

use std::{
    ffi::CString,
    fs,
    io::ErrorKind,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, PermissionsExt},
        io::{FromRawFd, OwnedFd},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
};

/// A socket's name in its place and the temporary name it is made under:
/// `.<name>.<pid>` in the same directory, so the rename never leaves the
/// directory and a leftover of a crash is never taken for the socket.
pub struct PlacedSocket {
    name: PathBuf,
    temporary: PathBuf,
}

/// What stands at a socket's name before the socket is placed there.
pub enum AtSocketName {
    /// Nothing: the socket is renamed in without replacing anything, so a
    /// socket another process placed there since is never replaced.
    Absent,
    /// A socket nobody accepts on, left by a process that died: the rename
    /// replaces it, with no moment at which the name is absent.
    Stale,
}

/// Places a socket at its name, listening.
pub trait PlacesSocket: Sized {
    /// The socket to be placed at `name`.
    fn at(name: &Path) -> Result<Self, String>;

    /// The temporary name the socket is made under.
    fn temporary(&self) -> &Path;

    /// What stands at the name. A socket that accepts a connection belongs
    /// to a live process and is refused, as is a file that is no socket.
    fn found(&self) -> Result<AtSocketName, String>;

    /// A socket bound at the temporary name, its mode `0600`, listening. A
    /// leftover at this process's own temporary name is a dead process's
    /// that had the same id, and is removed first.
    fn listening(&self) -> Result<UnixListener, String>;

    /// Renames the temporary name to the name, over a stale socket or into
    /// an absent name.
    fn place(&self, found: AtSocketName) -> Result<(), String>;

    /// The socket listening at its name; on any refusal nothing of it is
    /// left at either name.
    fn claim(&self) -> Result<UnixListener, String> {
        let found = self.found()?;
        let listener = self.listening()?;
        if let Err(error) = self.place(found) {
            drop(listener);
            let _ = fs::remove_file(self.temporary());
            return Err(error);
        }
        Ok(listener)
    }
}

impl PlacesSocket for PlacedSocket {
    fn at(name: &Path) -> Result<Self, String> {
        let unplaced = || {
            format!(
                "socket path {} names no file in a directory",
                name.display()
            )
        };
        let directory = name.parent().ok_or_else(unplaced)?;
        let file_name = name.file_name().ok_or_else(unplaced)?;
        let mut temporary = std::ffi::OsString::from(".");
        temporary.push(file_name);
        temporary.push(format!(".{}", std::process::id()));
        Ok(Self {
            name: name.to_owned(),
            temporary: directory.join(temporary),
        })
    }

    fn temporary(&self) -> &Path {
        &self.temporary
    }

    fn found(&self) -> Result<AtSocketName, String> {
        let name = &self.name;
        match fs::symlink_metadata(name) {
            Ok(metadata) if metadata.file_type().is_socket() => match UnixStream::connect(name) {
                Ok(_peer) => Err(format!(
                    "socket {} is already accepting connections",
                    name.display()
                )),
                Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                    Ok(AtSocketName::Stale)
                }
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(AtSocketName::Absent),
                Err(error) => Err(format!(
                    "cannot establish whether socket {} is stale: {error}",
                    name.display()
                )),
            },
            Ok(_) => Err(format!(
                "socket path {} exists and is not a socket",
                name.display()
            )),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(AtSocketName::Absent),
            Err(error) => Err(format!("cannot inspect socket {}: {error}", name.display())),
        }
    }

    fn listening(&self) -> Result<UnixListener, String> {
        let temporary = &self.temporary;
        match fs::remove_file(temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "cannot remove leftover {}: {error}",
                    temporary.display()
                ));
            }
        }
        let path = temporary.as_os_str().as_bytes();
        // SAFETY: an all-zero sockaddr_un is valid; its path is filled
        // below within its bounds, leaving the terminating zero.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        if path.len() >= address.sun_path.len() {
            return Err(format!("socket path {} is too long", temporary.display()));
        }
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (slot, byte) in address.sun_path.iter_mut().zip(path) {
            *slot = *byte as libc::c_char;
        }
        // SAFETY: socket takes no pointer; a non-negative result is a
        // descriptor this process owns, handed to OwnedFd at once.
        let descriptor =
            unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if descriptor < 0 {
            return Err(format!(
                "cannot open a socket for {}: {}",
                temporary.display(),
                std::io::Error::last_os_error()
            ));
        }
        // SAFETY: the descriptor was just opened and is owned by nothing else.
        let socket = unsafe { OwnedFd::from_raw_fd(descriptor) };
        let failed = |step: &str, error: std::io::Error| {
            let _ = fs::remove_file(temporary);
            format!("cannot {step} socket {}: {error}", temporary.display())
        };
        // SAFETY: the address is a valid sockaddr_un of the length given.
        let bound = unsafe {
            libc::bind(
                descriptor,
                (&raw const address).cast::<libc::sockaddr>(),
                std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
            )
        };
        if bound != 0 {
            return Err(format!(
                "cannot bind socket {}: {}",
                temporary.display(),
                std::io::Error::last_os_error()
            ));
        }
        fs::set_permissions(temporary, fs::Permissions::from_mode(0o600))
            .map_err(|error| failed("protect", error))?;
        // SAFETY: listen takes the descriptor this process owns.
        if unsafe { libc::listen(descriptor, libc::SOMAXCONN) } != 0 {
            return Err(failed("listen on", std::io::Error::last_os_error()));
        }
        Ok(UnixListener::from(socket))
    }

    fn place(&self, found: AtSocketName) -> Result<(), String> {
        let path = |path: &Path| {
            CString::new(path.as_os_str().as_bytes())
                .map_err(|_| format!("socket path {} holds a zero byte", path.display()))
        };
        let from = path(&self.temporary)?;
        let to = path(&self.name)?;
        let flags = match found {
            AtSocketName::Absent => libc::RENAME_NOREPLACE,
            AtSocketName::Stale => 0,
        };
        // SAFETY: both paths are zero-terminated and outlive the call.
        let renamed = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                flags,
            )
        };
        if renamed == 0 {
            Ok(())
        } else {
            Err(format!(
                "cannot place socket {} at {}: {}",
                self.temporary.display(),
                self.name.display(),
                std::io::Error::last_os_error()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    //! Each claim is made by this test process at a scratch Runtime, so its
    //! own temporary name is `.<name>.<this pid>`.
    use super::{PlacedSocket, PlacesSocket};
    use std::{
        fs,
        os::unix::{
            fs::{FileTypeExt, MetadataExt, PermissionsExt},
            net::UnixStream,
        },
        path::Path,
    };

    /// A socket bound at `path` and never listening, then closed: a
    /// connection to it is refused, whoever else holds the descriptor.
    fn stale(path: &Path) {
        // SAFETY: the address is a valid sockaddr_un; the descriptor is
        // closed before return.
        unsafe {
            let descriptor = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            assert!(descriptor >= 0);
            let mut address: libc::sockaddr_un = std::mem::zeroed();
            address.sun_family = libc::AF_UNIX as libc::sa_family_t;
            for (slot, byte) in address
                .sun_path
                .iter_mut()
                .zip(std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str()))
            {
                *slot = *byte as libc::c_char;
            }
            let bound = libc::bind(
                descriptor,
                (&raw const address).cast::<libc::sockaddr>(),
                std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
            );
            assert_eq!(bound, 0);
            libc::close(descriptor);
        }
    }

    fn names(directory: &Path) -> Vec<String> {
        let mut names = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn a_socket_appears_at_its_name_listening_and_owner_only() {
        let runtime = tempfile::tempdir().unwrap();
        let name = runtime.path().join("flow.sock");
        let placed = PlacedSocket::at(&name).unwrap();
        assert_eq!(
            placed.temporary(),
            runtime
                .path()
                .join(format!(".flow.sock.{}", std::process::id()))
        );
        let _listener = placed.claim().unwrap();
        let metadata = fs::symlink_metadata(&name).unwrap();
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        UnixStream::connect(&name).expect("the name listens");
        assert_eq!(names(runtime.path()), ["flow.sock"]);
    }

    #[test]
    fn a_stale_socket_is_replaced_by_the_rename_and_a_live_one_refused() {
        let runtime = tempfile::tempdir().unwrap();
        let name = runtime.path().join("flow.sock");
        stale(&name);
        let before = fs::symlink_metadata(&name).unwrap().ino();
        let listener = PlacedSocket::at(&name).unwrap().claim().unwrap();
        assert_ne!(fs::symlink_metadata(&name).unwrap().ino(), before);
        UnixStream::connect(&name).expect("the replacement listens");

        let refused = PlacedSocket::at(&name).unwrap().claim();
        assert!(refused.is_err(), "a live socket is refused");
        UnixStream::connect(&name).expect("the live socket still listens");
        drop(listener);
        assert_eq!(names(runtime.path()), ["flow.sock"]);
    }

    #[test]
    fn a_file_that_is_no_socket_is_refused_and_left() {
        let runtime = tempfile::tempdir().unwrap();
        let name = runtime.path().join("flow.sock");
        fs::write(&name, "not a socket").unwrap();
        assert!(PlacedSocket::at(&name).unwrap().claim().is_err());
        assert_eq!(fs::read(&name).unwrap(), b"not a socket");
        assert_eq!(names(runtime.path()), ["flow.sock"]);
    }

    /// A crash between bind and rename leaves a temporary name. One of this
    /// process's own id is a dead process's and is replaced; another's is
    /// left as it is, and neither is the socket.
    #[test]
    fn a_leftover_temporary_name_is_never_the_socket() {
        let runtime = tempfile::tempdir().unwrap();
        let name = runtime.path().join("flow.sock");
        let placed = PlacedSocket::at(&name).unwrap();
        stale(placed.temporary());
        let foreign = runtime.path().join(".flow.sock.1");
        stale(&foreign);
        let _listener = placed.claim().unwrap();
        UnixStream::connect(&name).expect("the name listens");
        assert!(
            UnixStream::connect(&foreign).is_err(),
            "the leftover is not served"
        );
        assert_eq!(names(runtime.path()), [".flow.sock.1", "flow.sock"]);
    }
}
