//! Fable protection: the flow of an Address the meta ProtectedAddresses
//! names is kept. It is never refreshed for its model's threshold, and it
//! is restarted, refreshed, replaced, stopped or retired only by whom the
//! act allows: its own seat, read from the calling process as a restart's
//! origin is; for a Restart or Refresh, the meta surface too; for a Stop or
//! a Retire, the owner, a process in no flow's pane. Every other caller is
//! refused Protected, before anything is written.

use crate::RunningNexus;
use crate::caller::{AuthenticatesProcessAuthority, ProcessAuthority};
use crate::store::{FlowStore, ManagesMetaflowState, StoreError};
use signal_flow::{Address, Metaflow, Origin, Process};

/// Who asks for an act on a flow, as the Nexus read the asking process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Asker {
    /// A caller on the ordinary socket, by the origin the Nexus derived
    /// from its process.
    Ordinary(Origin),
    /// A caller on the ordinary socket whose process runs in no flow's
    /// pane: the owner, at a terminal or in a service.
    Owner,
    /// A peer the meta socket admitted, and the flow it runs in, when it
    /// runs in one.
    Meta(Option<String>),
}

impl From<Origin> for Asker {
    fn from(origin: Origin) -> Self {
        Asker::Ordinary(origin)
    }
}

/// An act a protected Address takes only from whom it allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardedAct {
    /// A Restart of the flow.
    Restart,
    /// A Refresh of its Address, a Replace of the flow, or a Launch that is
    /// one.
    Refresh,
    /// A Stop of the flow.
    Stop,
    /// A Retire of the flow.
    Retire,
}

/// What an asker may do to a protected Address's flow.
pub trait ChecksAsker {
    /// Whether this asker may do `act` to `flow_id`, the flow a protected
    /// Address is Awake as.
    fn may(&self, act: GuardedAct, flow_id: &str) -> bool;
    /// The origin a launch it asks for records.
    fn origin(&self) -> Origin;
}

impl ChecksAsker for Asker {
    fn may(&self, act: GuardedAct, flow_id: &str) -> bool {
        match self {
            Asker::Ordinary(Origin::Flow(asking)) => asking.flow_id.0 == flow_id,
            Asker::Ordinary(Origin::Unbound) => false,
            Asker::Owner => matches!(act, GuardedAct::Stop | GuardedAct::Retire),
            Asker::Meta(None) => true,
            Asker::Meta(Some(asking)) => {
                matches!(act, GuardedAct::Restart | GuardedAct::Refresh) || asking == flow_id
            }
        }
    }

    fn origin(&self) -> Origin {
        match self {
            Asker::Ordinary(origin) => origin.clone(),
            Asker::Owner | Asker::Meta(_) => Origin::Unbound,
        }
    }
}

/// Which flows a store protects.
pub trait ProtectsFlows {
    /// The protected Address `flow_id` is Awake as, or its restart holds;
    /// None when its Address is not protected or it has none.
    fn protected_address(&self, flow_id: &str) -> Result<Option<Address>, StoreError>;
}

impl ProtectsFlows for FlowStore {
    fn protected_address(&self, flow_id: &str) -> Result<Option<Address>, StoreError> {
        use crate::store::metaflow::SleepsMetaflows;
        use crate::store::restart::RecordsRestarts;
        let address = match self.metaflow_awake_as(flow_id)? {
            Some(metaflow) => Some(metaflow.address),
            None => self.restart_record(flow_id)?.map(|record| record.address),
        };
        Ok(match address {
            Some(address) if self.protects(&address)? => Some(address),
            _ => None,
        })
    }
}

/// The guard every guarded act passes before it writes anything.
pub trait GuardsProtectedAddresses {
    /// Whether `asker` may do `act` to `flow_id`: a flow whose Address is
    /// not protected takes it from anybody.
    fn admits(&self, flow_id: &str, asker: &Asker, act: GuardedAct) -> Result<bool, StoreError>;
    /// Whether `asker` may refresh `address`, or replace its flow: an
    /// Address not Awake has no flow to keep.
    fn admits_refresh(&self, address: &Address, asker: &Asker) -> Result<bool, StoreError>;
    /// Who the process on an ordinary connection is: the flow it runs in,
    /// the owner, or neither.
    fn asker_of(&self, peer: Option<&Process>) -> Asker;
}

impl GuardsProtectedAddresses for RunningNexus {
    fn admits(&self, flow_id: &str, asker: &Asker, act: GuardedAct) -> Result<bool, StoreError> {
        Ok(self.store.protected_address(flow_id)?.is_none() || asker.may(act, flow_id))
    }

    fn admits_refresh(&self, address: &Address, asker: &Asker) -> Result<bool, StoreError> {
        use crate::store::metaflow::SleepsMetaflows;
        match self.store.metaflow(address)? {
            Some(Metaflow {
                state: flow_ethos::State::Awake(flow_id),
                ..
            }) => self.admits(&flow_id.0, asker, GuardedAct::Refresh),
            _ => Ok(true),
        }
    }

    fn asker_of(&self, peer: Option<&Process>) -> Asker {
        let Some(peer) = peer else {
            return Asker::Ordinary(Origin::Unbound);
        };
        match self.process_authority(peer) {
            Ok(ProcessAuthority::Bound(origin)) => Asker::Ordinary(Origin::Flow(origin)),
            Ok(ProcessAuthority::Owner) => Asker::Owner,
            Ok(ProcessAuthority::Sending(_) | ProcessAuthority::Rejected) | Err(_) => {
                Asker::Ordinary(Origin::Unbound)
            }
        }
    }
}
