//! Authenticated process evidence for ordinary-socket refresh requests.

use std::{
    fs,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::net::UnixStream,
};

use nix::sys::socket::{getsockopt, sockopt::PeerCredentials};
use signal_flow::{CallerProof, CallerRelationship, ProcessIdentity, RefreshRejection};

const MAXIMUM_PARENT_DEPTH: usize = 64;
const SO_PEERPIDFD: nix::libc::c_int = 77;

pub trait ReadsProcessIdentity {
    fn peer_process_identity(&self, peer: &UnixStream)
    -> Result<ProcessIdentity, RefreshRejection>;
    fn process_identity(&self, process_id: i64) -> Result<ProcessIdentity, RefreshRejection>;
}

pub trait ProvesRefreshCaller {
    fn prove_refresh_caller(
        &self,
        peer: &ProcessIdentity,
        flow_id: &str,
        harness: &ProcessIdentity,
    ) -> Result<CallerProof, RefreshRejection>;
}

/// Authenticates a refresh caller while the kernel-owned peer pidfd remains
/// live for the complete ancestry proof.
pub trait ProvesSocketRefreshCaller {
    fn prove_socket_refresh_caller(
        &self,
        peer: &UnixStream,
        flow_id: &str,
        harness: &ProcessIdentity,
    ) -> Result<CallerProof, RefreshRejection>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct LinuxProcessEvidence;

impl LinuxProcessEvidence {
    fn peer_pidfd(&self, peer: &UnixStream) -> Result<OwnedFd, RefreshRejection> {
        let mut pidfd: nix::libc::c_int = -1;
        let mut length = std::mem::size_of_val(&pidfd) as nix::libc::socklen_t;
        // A pidfd opened later from SO_PEERCRED's numeric PID would retain a
        // PID-reuse race. SO_PEERPIDFD returns the connected task itself.
        let result = unsafe {
            nix::libc::getsockopt(
                peer.as_raw_fd(),
                nix::libc::SOL_SOCKET,
                SO_PEERPIDFD,
                std::ptr::addr_of_mut!(pidfd).cast(),
                std::ptr::addr_of_mut!(length),
            )
        };
        if result != 0 || pidfd < 0 || length as usize != std::mem::size_of_val(&pidfd) {
            return Err(RefreshRejection::CallerProofUnavailable);
        }
        // SAFETY: successful SO_PEERPIDFD initializes a new owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(pidfd) })
    }

    fn pidfd_process_id(&self, pidfd: &OwnedFd) -> Result<i64, RefreshRejection> {
        let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", pidfd.as_raw_fd()))
            .map_err(|_| RefreshRejection::CallerProofUnavailable)?;
        fdinfo
            .lines()
            .find_map(|line| line.strip_prefix("Pid:"))
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|process_id| *process_id > 0)
            .ok_or(RefreshRejection::CallerProofUnavailable)
    }

    fn status_values(&self, process_id: i64) -> Result<(i64, i64), RefreshRejection> {
        let status = fs::read_to_string(format!("/proc/{process_id}/status"))
            .map_err(|_| RefreshRejection::CallerProofUnavailable)?;
        let mut parent = None;
        let mut user = None;
        for line in status.lines() {
            if let Some(value) = line.strip_prefix("PPid:") {
                parent = value.trim().parse::<i64>().ok();
            } else if let Some(value) = line.strip_prefix("Uid:") {
                user = value
                    .split_whitespace()
                    .next()
                    .and_then(|value| value.parse::<i64>().ok());
            }
        }
        match (parent, user) {
            (Some(parent), Some(user)) => Ok((parent, user)),
            _ => Err(RefreshRejection::CallerProofUnavailable),
        }
    }

    fn start_token(&self, process_id: i64) -> Result<String, RefreshRejection> {
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat"))
            .map_err(|_| RefreshRejection::CallerProofUnavailable)?;
        let suffix = stat
            .rsplit_once(')')
            .map(|(_, suffix)| suffix.trim())
            .ok_or(RefreshRejection::CallerProofUnavailable)?;
        suffix
            .split_whitespace()
            .nth(19)
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .map(str::to_owned)
            .ok_or(RefreshRejection::CallerProofUnavailable)
    }

    fn parent_process_id(&self, process_id: i64) -> Result<i64, RefreshRejection> {
        self.status_values(process_id).map(|(parent, _)| parent)
    }
}

impl ReadsProcessIdentity for LinuxProcessEvidence {
    fn peer_process_identity(
        &self,
        peer: &UnixStream,
    ) -> Result<ProcessIdentity, RefreshRejection> {
        let credentials = getsockopt(peer, PeerCredentials)
            .map_err(|_| RefreshRejection::CallerProofUnavailable)?;
        let pidfd = self.peer_pidfd(peer)?;
        let pinned_process_id = self.pidfd_process_id(&pidfd)?;
        if pinned_process_id != i64::from(credentials.pid()) {
            return Err(RefreshRejection::CallerProofMismatch);
        }
        let identity = self.process_identity(i64::from(credentials.pid()))?;
        if self.pidfd_process_id(&pidfd)? != identity.process_id {
            return Err(RefreshRejection::CallerProofUnavailable);
        }
        if identity.process_user_id != i64::from(credentials.uid()) {
            return Err(RefreshRejection::CallerProofMismatch);
        }
        Ok(identity)
    }

    fn process_identity(&self, process_id: i64) -> Result<ProcessIdentity, RefreshRejection> {
        if process_id <= 0 {
            return Err(RefreshRejection::CallerProofUnavailable);
        }
        let first_start = self.start_token(process_id)?;
        let (_, process_user_id) = self.status_values(process_id)?;
        let second_start = self.start_token(process_id)?;
        if first_start != second_start {
            return Err(RefreshRejection::CallerProofUnavailable);
        }
        Ok(ProcessIdentity {
            process_id,
            process_user_id,
            process_start_token: first_start,
        })
    }
}

impl ProvesRefreshCaller for LinuxProcessEvidence {
    fn prove_refresh_caller(
        &self,
        peer: &ProcessIdentity,
        flow_id: &str,
        harness: &ProcessIdentity,
    ) -> Result<CallerProof, RefreshRejection> {
        if peer.process_user_id != harness.process_user_id {
            return Err(RefreshRejection::CallerProofMismatch);
        }
        let mut process_id = peer.process_id;
        for depth in 0..=MAXIMUM_PARENT_DEPTH {
            let observed = self.process_identity(process_id)?;
            if depth == 0 && observed != *peer {
                // The pidfd-backed socket observation is the requester's
                // identity. Never continue an ancestry walk from a reused
                // numeric PID after that pidfd has been released.
                return Err(RefreshRejection::CallerProofMismatch);
            }
            if observed == *harness {
                return Ok(CallerProof {
                    flow_id: flow_id.to_owned(),
                    process_identity: peer.clone(),
                    caller_relationship: if depth == 0 {
                        CallerRelationship::Harness
                    } else {
                        CallerRelationship::Descendant
                    },
                });
            }
            let parent = self.parent_process_id(process_id)?;
            if parent <= 0 || parent == process_id {
                break;
            }
            process_id = parent;
        }
        Err(RefreshRejection::CallerProofMismatch)
    }
}

impl ProvesSocketRefreshCaller for LinuxProcessEvidence {
    fn prove_socket_refresh_caller(
        &self,
        peer: &UnixStream,
        flow_id: &str,
        harness: &ProcessIdentity,
    ) -> Result<CallerProof, RefreshRejection> {
        let credentials = getsockopt(peer, PeerCredentials)
            .map_err(|_| RefreshRejection::CallerProofUnavailable)?;
        let pidfd = self.peer_pidfd(peer)?;
        let process_id = self.pidfd_process_id(&pidfd)?;
        if process_id != i64::from(credentials.pid()) {
            return Err(RefreshRejection::CallerProofMismatch);
        }
        let peer_identity = self.process_identity(process_id)?;
        if peer_identity.process_user_id != i64::from(credentials.uid()) {
            return Err(RefreshRejection::CallerProofMismatch);
        }
        let proof = self.prove_refresh_caller(&peer_identity, flow_id, harness)?;
        if self.pidfd_process_id(&pidfd)? != process_id
            || self.process_identity(process_id)? != peer_identity
        {
            return Err(RefreshRejection::CallerProofUnavailable);
        }
        Ok(proof)
    }
}

#[cfg(test)]
mod tests {
    use super::{LinuxProcessEvidence, ProvesRefreshCaller, ReadsProcessIdentity};
    use signal_flow::{CallerRelationship, RefreshRejection};

    #[test]
    fn exact_process_is_authenticated_as_the_harness() {
        let evidence = LinuxProcessEvidence;
        let current = evidence
            .process_identity(i64::from(std::process::id()))
            .expect("current process identity");
        assert_eq!(
            evidence
                .prove_refresh_caller(&current, "flow-a", &current)
                .expect("exact process proves itself")
                .caller_relationship,
            CallerRelationship::Harness
        );
    }

    #[test]
    fn changed_start_token_is_rejected() {
        let evidence = LinuxProcessEvidence;
        let current = evidence
            .process_identity(i64::from(std::process::id()))
            .expect("current process identity");
        let mut replaced = current.clone();
        replaced.process_start_token.push('0');
        assert_eq!(
            evidence.prove_refresh_caller(&current, "flow-a", &replaced),
            Err(RefreshRejection::CallerProofMismatch)
        );
    }

    #[test]
    fn unix_peer_identity_is_bound_through_a_kernel_pidfd() {
        let evidence = LinuxProcessEvidence;
        let (peer, _other) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let identity = evidence
            .peer_process_identity(&peer)
            .expect("kernel provides peer pidfd");
        assert_eq!(identity.process_id, i64::from(std::process::id()));
    }
}
