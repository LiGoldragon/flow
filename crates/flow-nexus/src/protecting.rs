//! Fable protection: the flow of an Address the meta ProtectedAddresses
//! names is kept. It is never refreshed for its model's threshold. Every
//! other act on it is answered as for any Address, to any caller.

use crate::store::{FlowStore, ManagesMetaflowState, StoreError};
use signal_flow::Address;

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
