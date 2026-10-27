//! The meta socket's repairs: every situation Flow is got out of is an
//! operation. `Inspect` reads a metaflow whole; `Rebind` records where its
//! flow lives now, a pane or another flow, with no harness action;
//! `Withdraw` takes one queued request out of its queue and closes the
//! attempt that holds it back.

use crate::herdr::{PaneRoute, ReadsHerdrPanes};
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::repair::RepairsMetaflows;
use crate::store::{ReadsFlowRows, RebindsNativeSession};
use crate::RunningNexus;
use meta_signal_flow::{
    InspectRejected_Data, RebindRejected_Data, Rebind_Data, Rebinding, Response, StoreRefusal,
    Withdraw_Data,
};
use signal_flow::{Address, HerdrRouteSelection};

pub trait RepairsFromMeta {
    /// Inspected, or InspectRejected.UnknownAddress.
    fn inspect(&self, address: Address) -> Response;
    /// Rebound once the new pane or flow is recorded; RebindRejected
    /// changes nothing.
    fn rebind(&self, rebind: Rebind_Data) -> Response;
    /// Withdrawn once the request has left the queue; WithdrawRejected
    /// changes nothing.
    fn withdraw(&self, withdraw: Withdraw_Data) -> Response;
}

trait RefusesStore {
    fn store_refused(error: impl ToString) -> Response;
}

impl RefusesStore for RunningNexus {
    fn store_refused(error: impl ToString) -> Response {
        Response::Refused(StoreRefusal::Store(error.to_string()))
    }
}

impl RepairsFromMeta for RunningNexus {
    fn inspect(&self, address: Address) -> Response {
        match self.store.inspection(&address) {
            Ok(Some(inspected)) => Response::Inspected(inspected),
            Ok(None) => Response::InspectRejected(InspectRejected_Data::UnknownAddress),
            Err(error) => Self::store_refused(error),
        }
    }

    fn rebind(&self, rebind: Rebind_Data) -> Response {
        let Rebind_Data { address, rebinding } = rebind;
        let rebound = match rebinding {
            Rebinding::Pane(herdr_pane_id) => {
                let flow_id = match self.store.current_flow(&address) {
                    Ok(Ok(flow_id)) => flow_id,
                    Ok(Err(rejection)) => return Response::RebindRejected(rejection),
                    Err(error) => return Self::store_refused(error),
                };
                let node = match self.store.flow_node(&flow_id) {
                    Ok(Some(node)) => node,
                    Ok(None) => return Response::RebindRejected(RebindRejected_Data::FlowUnknown),
                    Err(error) => return Self::store_refused(error),
                };
                // The pane is looked for in the Herdr session the flow's
                // route names.
                let HerdrRouteSelection::Available(recorded) = &node.herdr_route_selection else {
                    return Response::RebindRejected(RebindRejected_Data::PaneAbsent);
                };
                let route = match self.herdr.pane_route(
                    &recorded.herdr_session_name,
                    &herdr_pane_id,
                    &node.harness_kind,
                ) {
                    PaneRoute::Shown(route) => route,
                    PaneRoute::NotShown => {
                        return Response::RebindRejected(RebindRejected_Data::PaneAbsent);
                    }
                    PaneRoute::Unreadable => {
                        return Response::RebindRejected(RebindRejected_Data::HerdrUnavailable);
                    }
                };
                self.store.rebind_route(&flow_id, route).map(Ok)
            }
            Rebinding::Flow(flow_id) => self.store.rebind_current_flow(&address, &flow_id),
            Rebinding::Native(native_session_id) => {
                let flow_id = match self.store.current_flow(&address) {
                    Ok(Ok(flow_id)) => flow_id,
                    Ok(Err(rejection)) => return Response::RebindRejected(rejection),
                    Err(error) => return Self::store_refused(error),
                };
                let node = match self.store.flow_node(&flow_id) {
                    Ok(Some(node)) => node,
                    _ => return Response::RebindRejected(RebindRejected_Data::FlowUnknown),
                };
                self.store
                    .rebind_native_session(&flow_id, &node.session_id, &native_session_id)
                    .map(|changed| {
                        if changed {
                            Ok(())
                        } else {
                            Err(RebindRejected_Data::FlowNotLive)
                        }
                    })
            }
            Rebinding::Session(herdr_session_name) => {
                let flow_id = match self.store.current_flow(&address) {
                    Ok(Ok(flow_id)) => flow_id,
                    Ok(Err(rejection)) => return Response::RebindRejected(rejection),
                    Err(error) => return Self::store_refused(error),
                };
                let node = match self.store.flow_node(&flow_id) {
                    Ok(Some(node)) => node,
                    _ => return Response::RebindRejected(RebindRejected_Data::FlowUnknown),
                };
                let HerdrRouteSelection::Available(mut route) = node.herdr_route_selection else {
                    return Response::RebindRejected(RebindRejected_Data::PaneAbsent);
                };
                route.herdr_session_name = herdr_session_name;
                self.store.rebind_route(&flow_id, route).map(Ok)
            }
        };
        match rebound {
            Ok(Ok(())) => {
                eprintln!(
                    "flow-nexus: {:?} {} {:?} rebound by the owner",
                    address.aspect, address.topic, address.layer
                );
                Response::Rebound(address)
            }
            Ok(Err(rejection)) => Response::RebindRejected(rejection),
            Err(error) => Self::store_refused(error),
        }
    }

    fn withdraw(&self, withdraw: Withdraw_Data) -> Response {
        let Withdraw_Data {
            address,
            logical_delivery_id,
        } = withdraw;
        match self.store.withdraw_queued(&address, &logical_delivery_id) {
            Ok(Ok(())) => {
                eprintln!(
                    "flow-nexus: queued request {logical_delivery_id} withdrawn by the owner"
                );
                Response::Withdrawn(logical_delivery_id)
            }
            Ok(Err(rejection)) => Response::WithdrawRejected(rejection),
            Err(error) => Self::store_refused(error),
        }
    }
}
