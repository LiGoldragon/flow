//! What MetaBindExisting checks of a container and each flow it binds
//! before any binding reaches the store.

use meta_signal_flow::{
    FlowBinding, FlowBindingRefusalReason, FlowBindingResult, FlowContainer, ProcessIdentity,
    RefusedFlowBinding,
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
