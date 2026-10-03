//! A Claude launch's FlowId is reserved before its harness starts.
//!
//! Flow chooses the Claude session id itself, derived from the launch
//! request id, and claims the FlowId for that session through `flow-id` at
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
    /// The Claude session id Flow chooses for this launch: UUIDv5-shaped
    /// (version nibble 5, RFC 4122 variant), from the SHA-256 of the launch
    /// request id, so one request always names one session and its claim
    /// is the same alias each time. `None` for a Codex launch.
    fn reserved_native_session_id(&self) -> Option<String>;
}

impl ChoosesNativeSession for LaunchProfile {
    fn reserved_native_session_id(&self) -> Option<String> {
        if self.harness_kind != HarnessKind::Claude {
            return None;
        }
        let digest = Sha256::digest(
            format!("flow-claude-session-v1\0{}", self.launch_request_id).as_bytes(),
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
        let Some(session) = launch.launch_profile.reserved_native_session_id() else {
            return Ok(None);
        };
        self.claim_flow_identity(&HarnessKind::Claude, &session)
            .map(Some)
    }
}
