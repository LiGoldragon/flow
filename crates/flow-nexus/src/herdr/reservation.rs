//! A Claude launch's FlowId is reserved before its harness starts.
//!
//! Flow chooses the Claude session id itself, derived from the launching
//! Nexus's own ordinary socket and the launch request id, and claims the FlowId for that session through `flow-id` at
//! Reserve. Spawn then starts Claude with `--session-id` set to it and with
//! the FlowId exported as FLOW_ID in the pane, so the harness hook reports
//! under the FlowId from the harness's first event; Bind verifies the
//! harness came up as that session. A Codex launch reserves nothing: its
//! session is named by its app server, and Bind claims from that.

use super::HerdrCli;
use super::launch::ClaimsNativeIdentity;
use sha2::{Digest, Sha256};
use signal_flow::{ComposedLaunch, HarnessKind, LaunchProfile};

pub trait ChoosesNativeSession {
    /// The Claude session id this Nexus chooses for a launch: UUIDv5-shaped
    /// (version nibble 5, RFC 4122 variant), from the SHA-256 of the
    /// Nexus's own ordinary socket path and the launch request id. One
    /// request to one Nexus always names one session, so its claim is the
    /// same alias each time; two Nexuses sharing one Claude home (a
    /// sandbox beside the production Nexus) serve different sockets, so
    /// the same request id never names one session twice. `None` for a
    /// Codex launch.
    fn reserved_native_session_id(&self, profile: &LaunchProfile) -> Option<String>;
}

impl ChoosesNativeSession for HerdrCli {
    fn reserved_native_session_id(&self, profile: &LaunchProfile) -> Option<String> {
        if profile.harness_kind != HarnessKind::Claude {
            return None;
        }
        let digest = Sha256::digest(
            format!(
                "flow-claude-session-v2\0{}\0{}",
                self.ordinary_socket.to_string_lossy(),
                profile.launch_request_id
            )
            .as_bytes(),
        );
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Some(format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        ))
    }
}

pub trait ReservesFlowIdentity {
    /// Claims the FlowId of a Claude launch for the session Flow chose for
    /// it; `None` for a launch that reserves none.
    fn reserve_flow_identity(&self, launch: &ComposedLaunch) -> Result<Option<String>, String>;
}

impl ReservesFlowIdentity for HerdrCli {
    fn reserve_flow_identity(&self, launch: &ComposedLaunch) -> Result<Option<String>, String> {
        let Some(session) = self.reserved_native_session_id(&launch.launch_profile) else {
            return Ok(None);
        };
        self.claim_flow_identity(&HarnessKind::Claude, &session)
            .map(Some)
    }
}

pub trait ReleasesFlowIdentity {
    /// Gives back the FlowId Reserve claimed for a launch that was then
    /// refused: under the claim's own lock, the empty lane `flows/<FlowId>/`
    /// and the claim marker are removed, so the alias is free again. A lane
    /// something already wrote into is kept; a marker that is absent or
    /// names another harness is left as it is.
    fn release_flow_identity(&self, flow_id: &str) -> Result<(), String>;
}

impl ReleasesFlowIdentity for HerdrCli {
    fn release_flow_identity(&self, flow_id: &str) -> Result<(), String> {
        use super::{DecodesFlowClaim, FlowClaim};
        use std::fs;
        use std::io::ErrorKind;
        if flow_id.is_empty() || !flow_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("released FlowId is not a claim alias".into());
        }
        let lock_path = self.flows_root.join(format!(".{flow_id}.flow-id.lock"));
        let lock = match fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(lock) => lock,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(format!("claim lock is unreadable: {error}")),
        };
        lock.lock()
            .map_err(|error| format!("claim lock was not taken: {error}"))?;
        let marker_path = self.flows_root.join(format!(".{flow_id}.flow-id"));
        let marker = match fs::read_to_string(&marker_path) {
            Ok(marker) => marker,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(format!("claim marker is unreadable: {error}")),
        };
        match FlowClaim::decode(&marker) {
            Some(claim) if claim.alias == flow_id && claim.harness_kind == HarnessKind::Claude => {}
            _ => return Ok(()),
        }
        match fs::remove_dir(self.flows_root.join(flow_id)) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            // The lane holds something: the claim stays with it.
            Err(_) => return Ok(()),
        }
        fs::remove_file(&marker_path)
            .map_err(|error| format!("claim marker was not removed: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use super::{ChoosesNativeSession, ReleasesFlowIdentity};
    use crate::herdr::{ConfiguresHerdrCli, HerdrCli};
    use signal_flow::{HarnessKind, LaunchProfile};
    use std::fs;
    use std::path::Path;

    fn profile(harness_kind: HarnessKind) -> LaunchProfile {
        let mut profile = crate::herdr::launch::tests::launch(harness_kind).launch_profile;
        profile.launch_request_id = "request-1".into();
        profile
    }

    /// One request to one Nexus names one session every time; the same
    /// request to a Nexus serving another socket (a sandbox sharing the
    /// Claude home) names another, so their claims never collide.
    #[test]
    fn two_nexuses_never_choose_one_session_for_one_request() {
        let production =
            HerdrCli::default().with_ordinary_socket(Path::new("/run/user/1001/flow/flow.sock"));
        let sandbox =
            HerdrCli::default().with_ordinary_socket(Path::new("/tmp/tmp.x1/run/flow/flow.sock"));
        let chosen = production
            .reserved_native_session_id(&profile(HarnessKind::Claude))
            .unwrap();
        assert_eq!(
            production.reserved_native_session_id(&profile(HarnessKind::Claude)),
            Some(chosen.clone())
        );
        assert_ne!(
            sandbox.reserved_native_session_id(&profile(HarnessKind::Claude)),
            Some(chosen.clone())
        );
        assert_eq!(chosen.len(), 36);
        assert_eq!(chosen.as_bytes()[14], b'5');
        assert!(matches!(chosen.as_bytes()[19], b'8' | b'9' | b'a' | b'b'));
        assert_eq!(
            production.reserved_native_session_id(&profile(HarnessKind::Codex)),
            None
        );
    }

    fn claimed(root: &Path, alias: &str, harness: &str) {
        let identity = format!("{alias}000000500080{}", "0".repeat(14));
        let version = if harness == "claude" {
            "uuid-version=uuid-v5\n"
        } else {
            ""
        };
        fs::write(root.join(format!(".{alias}.flow-id.lock")), "").unwrap();
        fs::write(
            root.join(format!(".{alias}.flow-id")),
            format!("version=1\nharness={harness}\nidentity={identity}\nalias={alias}\n{version}"),
        )
        .unwrap();
        fs::create_dir(root.join(alias)).unwrap();
    }

    /// A refused launch's claim is given back: its empty lane and marker
    /// go. A lane something wrote into keeps its claim, a Codex claim is
    /// not Reserve's to release, and an unclaimed alias is a no-op.
    #[test]
    fn a_refused_launch_gives_back_its_empty_claim() {
        let directory = tempfile::tempdir().unwrap();
        let flows = directory.path().join("flows");
        fs::create_dir(&flows).unwrap();
        let herdr = HerdrCli::at(directory.path().join("herdr"), flows.clone());
        claimed(&flows, "a1b2c3", "claude");
        herdr.release_flow_identity("a1b2c3").unwrap();
        assert!(!flows.join(".a1b2c3.flow-id").exists());
        assert!(!flows.join("a1b2c3").exists());
        claimed(&flows, "d4e5f6", "claude");
        fs::write(flows.join("d4e5f6").join("log.md"), "written").unwrap();
        herdr.release_flow_identity("d4e5f6").unwrap();
        assert!(flows.join(".d4e5f6.flow-id").exists());
        claimed(&flows, "0c0d0e", "codex");
        herdr.release_flow_identity("0c0d0e").unwrap();
        assert!(flows.join(".0c0d0e.flow-id").exists());
        herdr.release_flow_identity("777777").unwrap();
        assert!(herdr.release_flow_identity("../x").is_err());
    }
}
