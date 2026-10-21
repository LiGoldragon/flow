//! What MetaBindExisting checks of a container and each flow it binds
//! before any binding reaches the store.

use crate::{RunningNexus, herdr::launch::ClaimsNativeIdentity, store::NamesConfiguredModel};
use meta_signal_flow::{
    FlowBinding, FlowBindingRefusalReason, FlowBindingResult, FlowContainer, ProcessIdentity,
    RefusedFlowBinding,
};
use signal_flow::{
    Address, EndpointSelection, FlowLifecycle, FlowNode, HerdrRoute, HerdrRouteSelection, Origin,
    PowerLevel,
};
use std::{
    fs,
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};

/// A process named by its identity, read back from `/proc`.
pub trait ChecksProcessIdentity {
    /// Whether the process still runs as the same user and start.
    fn is_live_process(&self) -> bool;
    /// Whether the process works in the expected absolute directory.
    fn works_in(&self, expected: &str) -> bool;
}

impl ChecksProcessIdentity for ProcessIdentity {
    fn is_live_process(&self) -> bool {
        let Ok(process_id) = u32::try_from(self.process_id) else {
            return false;
        };
        let process_root = PathBuf::from(format!("/proc/{process_id}"));
        let Ok(metadata) = fs::metadata(&process_root) else {
            return false;
        };
        if i64::from(metadata.uid()) != self.process_user_id {
            return false;
        }
        let Ok(stat) = fs::read_to_string(process_root.join("stat")) else {
            return false;
        };
        let Some((_, fields)) = stat.rsplit_once(") ") else {
            return false;
        };
        fields.split_whitespace().nth(19) == Some(self.process_start_token.as_str())
    }

    fn works_in(&self, expected: &str) -> bool {
        let Ok(process_id) = u32::try_from(self.process_id) else {
            return false;
        };
        let expected = Path::new(expected);
        expected.is_absolute()
            && fs::canonicalize(format!("/proc/{process_id}/cwd")).ok()
                == fs::canonicalize(expected).ok()
    }
}

/// A process named by its identity, read from `/proc`.
pub trait ReadsProcessIdentity: Sized {
    /// The identity of the running process, None when it cannot be read.
    fn of(process_id: u32) -> Option<Self>;
}

impl ReadsProcessIdentity for ProcessIdentity {
    fn of(process_id: u32) -> Option<Self> {
        let process_root = PathBuf::from(format!("/proc/{process_id}"));
        let metadata = fs::metadata(&process_root).ok()?;
        let stat = fs::read_to_string(process_root.join("stat")).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        Some(Self {
            process_id: i64::from(process_id),
            process_user_id: i64::from(metadata.uid()),
            process_start_token: fields.split_whitespace().nth(19)?.into(),
        })
    }
}

/// The Herdr container a MetaBindExisting names.
pub trait ChecksFlowContainer {
    fn is_well_formed(&self) -> bool;
    /// Whether the container's Herdr server socket is present as a socket.
    fn socket_is_live(&self) -> bool;
}

impl ChecksFlowContainer for FlowContainer {
    fn is_well_formed(&self) -> bool {
        !self.herdr_session_name.is_empty()
            && !self.meta_flow_owner_id.is_empty()
            && Path::new(&self.herdr_server_socket_path).is_absolute()
    }

    fn socket_is_live(&self) -> bool {
        fs::metadata(&self.herdr_server_socket_path)
            .map(|metadata| metadata.file_type().is_socket())
            .unwrap_or(false)
    }
}

/// One flow a MetaBindExisting binds.
pub trait ChecksFlowBinding {
    fn is_well_formed(&self) -> bool;
}

impl ChecksFlowBinding for FlowBinding {
    fn is_well_formed(&self) -> bool {
        !self.flow_id.is_empty()
            && !self.model_name.is_empty()
            && !self.native_session_id.is_empty()
            && !self.herdr_workspace_id.is_empty()
            && !self.herdr_pane_id.is_empty()
            && !self.herdr_tab_id.is_empty()
            && !self.herdr_terminal_id.is_empty()
            && !self.herdr_agent_name.is_empty()
            && Path::new(&self.working_directory).is_absolute()
    }
}

/// The node admitted only after native identity, process and Herdr agree.
pub(crate) trait VerifiesAdoptedSeat {
    fn verified_seat_node(
        &self,
        herdr_session_name: &str,
        address: &Address,
        binding: &FlowBinding,
    ) -> Result<FlowNode, FlowBindingRefusalReason>;
}

impl VerifiesAdoptedSeat for RunningNexus {
    fn verified_seat_node(
        &self,
        herdr_session_name: &str,
        address: &Address,
        binding: &FlowBinding,
    ) -> Result<FlowNode, FlowBindingRefusalReason> {
        if !binding.is_well_formed()
            || binding.aspect != address.aspect
            || binding.power_level
                != match address.layer {
                    signal_flow::Layer::Primary => PowerLevel::High,
                    signal_flow::Layer::Secondary => PowerLevel::Medium,
                    signal_flow::Layer::Tertiary => PowerLevel::Low,
                    signal_flow::Layer::Quaternary => PowerLevel::UltraLow,
                }
        {
            return Err(FlowBindingRefusalReason::RoleMismatch);
        }
        if !binding.process_identity.is_live_process() {
            return Err(FlowBindingRefusalReason::DeadProcess);
        }
        if !binding
            .process_identity
            .works_in(&binding.working_directory)
        {
            return Err(FlowBindingRefusalReason::AnatomyMismatch);
        }
        let configured_model = self
            .store
            .configured_model(address)
            .map_err(|_| FlowBindingRefusalReason::HerdrUnreadable)?;
        if configured_model.as_deref() != Some(binding.model_name.as_str()) {
            return Err(FlowBindingRefusalReason::RoleMismatch);
        }
        self.herdr
            .verify_existing_binding(herdr_session_name, binding)?;
        // Configuration names an endpoint that may serve this model; it does
        // not witness that the existing native flow is connected to it.
        // Keep endpoint readiness unavailable until a live connection is
        // independently observed.
        let endpoint_selection = EndpointSelection::Unavailable;
        Ok(FlowNode {
            flow_id: binding.flow_id.clone(),
            session_id: binding.native_session_id.clone(),
            harness_kind: binding.harness_kind.clone(),
            endpoint_selection,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: herdr_session_name.to_owned(),
                herdr_agent_name: binding.herdr_agent_name.clone(),
                herdr_pane_id: binding.herdr_pane_id.clone(),
                herdr_terminal_id: binding.herdr_terminal_id.clone(),
            }),
            origin: Origin::Unbound,
            flow_lifecycle: FlowLifecycle::Active,
        })
    }
}

/// A refusal reason refuses the binding of one flow.
pub trait RefusesBinding {
    fn refusal_of(self, flow_id: String) -> FlowBindingResult;
}

impl RefusesBinding for FlowBindingRefusalReason {
    fn refusal_of(self, flow_id: String) -> FlowBindingResult {
        FlowBindingResult::Refused(RefusedFlowBinding {
            flow_id,
            flow_binding_refusal_reason: self,
        })
    }
}

/// What Herdr, and for a Codex pane its rollout, shows of one binding:
/// the pane is in the container's Herdr session with an agent of the
/// binding's harness at its workspace, pane and terminal; the pane's
/// foreground processes include the binding's process; and its native
/// session is the binding's. A Claude pane must report its session; a Codex
/// pane that reports none is proven by a rollout whose `session_meta` names
/// the session.
pub trait VerifiesExistingBinding {
    fn verify_existing_binding(
        &self,
        herdr_session_name: &str,
        binding: &FlowBinding,
    ) -> Result<(), FlowBindingRefusalReason>;
}

impl VerifiesExistingBinding for crate::herdr::HerdrCli {
    fn verify_existing_binding(
        &self,
        herdr_session_name: &str,
        binding: &FlowBinding,
    ) -> Result<(), FlowBindingRefusalReason> {
        use crate::herdr::launch::{
            ClaimsNativeIdentity, NamesCodexThread, NamesHerdrHarness, ReadsLaunchFiles, RunsHerdr,
        };
        use crate::herdr::{LocatesNativeTranscripts, ReadsHerdrPanes};
        use signal_flow::HarnessKind;
        use std::io::BufRead;
        let expected = binding.harness_kind.expected_harness();
        let snapshot = self
            .session_snapshot(herdr_session_name)
            .ok_or(FlowBindingRefusalReason::HerdrUnreadable)?;
        let agents = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
            .ok_or(FlowBindingRefusalReason::HerdrUnreadable)?;
        let field = |agent: &serde_json::Value, key: &str| {
            agent
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        let mut matching_agents = agents.iter().filter(|agent| {
            field(agent, "workspace_id").as_deref() == Some(binding.herdr_workspace_id.as_str())
                && field(agent, "pane_id").as_deref() == Some(binding.herdr_pane_id.as_str())
                && field(agent, "terminal_id").as_deref()
                    == Some(binding.herdr_terminal_id.as_str())
                && field(agent, "agent").as_deref() == Some(expected)
        });
        let agent = matching_agents
            .next()
            .ok_or(FlowBindingRefusalReason::PaneAbsent)?;
        if matching_agents.next().is_some() {
            return Err(FlowBindingRefusalReason::AmbiguousPane);
        }
        let claimed_native_identity = self
            .claimed_native_identity(&binding.flow_id, &binding.harness_kind)
            .map_err(|_| FlowBindingRefusalReason::NativeIdentityMismatch)?;
        if claimed_native_identity
            != ClaimsNativeIdentity::canonical_native_identity(&binding.native_session_id)
        {
            return Err(FlowBindingRefusalReason::NativeIdentityMismatch);
        }
        let processes = self
            .run_json(&[
                "--session".into(),
                herdr_session_name.into(),
                "pane".into(),
                "process-info".into(),
                "--pane".into(),
                binding.herdr_pane_id.clone(),
            ])
            .map_err(|_| FlowBindingRefusalReason::HerdrUnreadable)?;
        let information = processes
            .pointer("/result/process_info")
            .ok_or(FlowBindingRefusalReason::HerdrUnreadable)?;
        let runs_process = field(information, "pane_id").as_deref()
            == Some(binding.herdr_pane_id.as_str())
            && information
                .get("foreground_processes")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|foreground| {
                    foreground.iter().any(|process| {
                        process.get("pid").and_then(serde_json::Value::as_i64)
                            == Some(binding.process_identity.process_id)
                    })
                });
        if !runs_process {
            return Err(FlowBindingRefusalReason::ProcessMismatch);
        }
        match (
            agent
                .pointer("/agent_session/value")
                .and_then(serde_json::Value::as_str),
            &binding.harness_kind,
        ) {
            (Some(session), _) if session == binding.native_session_id => Ok(()),
            (Some(_), _) => Err(FlowBindingRefusalReason::SessionMismatch),
            (None, HarnessKind::Claude) => Err(FlowBindingRefusalReason::SessionUnreported),
            (None, HarnessKind::Codex) => {
                let proven = self
                    .native_transcript_root(&HarnessKind::Codex, &binding.model_name)
                    .ok()
                    .and_then(|root| root.canonicalize().ok())
                    .is_some_and(|root| {
                        let mut found = Vec::new();
                        root.collect_native_transcripts(
                            &binding.native_session_id,
                            &HarnessKind::Codex,
                            0,
                            &mut found,
                        )
                        .is_ok()
                            && found.iter().any(|rollout| {
                                let mut first = String::new();
                                fs::File::open(rollout)
                                    .map(std::io::BufReader::new)
                                    .and_then(|mut reader| reader.read_line(&mut first))
                                    .is_ok()
                                    && serde_json::from_str::<serde_json::Value>(&first).is_ok_and(
                                        |row| row.names_thread(&binding.native_session_id),
                                    )
                            })
                    });
                if proven {
                    Ok(())
                } else {
                    Err(FlowBindingRefusalReason::SessionUnproven)
                }
            }
        }
    }
}
