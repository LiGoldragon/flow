//! Ordinary metaflow mutations. The connection supplies the witnessed peer;
//! the serial dispatcher holds exclusion across authorization and mutation.
use super::logical_delivery::{RecordsLogicalDeliveries, VerifiedDeliveryLock};
use super::pending_reap::ReassignsPendingReaps;
use super::*;
use crate::generated::memory::SeatBirth;
use crate::sending::AdmitsSendPayload;
use crate::sending::RefusesUnidentified;
use signal_flow::{
    Bind_Data, Deliver_Data, FlowId, LaunchProfile, Lock_Data, Refusal, StartRejection,
    Unknown_Data,
};

/// Materializes working state for a configured Address, never policy.
pub(crate) trait PreparesDeclaredMetaflow {
    fn prepare_declared_metaflow(&self, address: &Address) -> Result<bool, StoreError>;
}

impl PreparesDeclaredMetaflow for FlowStore {
    fn prepare_declared_metaflow(&self, address: &Address) -> Result<bool, StoreError> {
        use super::metaflow_declaration::ReadsMetaflowDeclarations;
        if self.stored_metaflow(address)?.is_some() {
            return Ok(true);
        }
        if self.metaflow_declaration(address)?.is_none()
            || self.configured_model(address)?.is_none()
        {
            return Ok(false);
        }
        let row = StoredMetaflow {
            metaflow: Metaflow {
                address: address.clone(),
                state: MetaflowState::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            },
        };
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.metaflows, row),
        )?;
        Ok(true)
    }
}

pub(crate) trait MutatesMetaflows {
    /// Binds the named live process to its Address, whoever asks.
    /// `launched` is the FlowId of the launched flow the process runs in,
    /// when Flow can name it; the metaflow is then Awake as that flow, so
    /// its Stop drains its queue.
    fn bind_metaflow(
        &self,
        binding: Bind_Data,
        launched: Option<FlowId>,
    ) -> Result<Response, StoreError>;
    fn lock_metaflow(&self, request: Lock_Data, now: i64) -> Result<Response, StoreError>;
    fn queue_metaflow(
        &self,
        request: Deliver_Data,
        now: i64,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError>;
}

/// The sleep and wake transitions of a metaflow. Each is one atomic write
/// and refuses (`Ok(false)`) a metaflow not in the state it expects.
pub(crate) trait SleepsMetaflows {
    /// Awake as `flow_id` becomes Asleep, and `flow_id` joins Past. The
    /// flow's own row, with its native session, is untouched.
    fn sleep_metaflow(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError>;
    /// The metaflow becomes Awake as the started flow, under the rules a
    /// launch takes its Address by (see [`TakesLaunchAddress`]); a fresh
    /// wake's launch already took it at its confirmation. Pending requests
    /// belong to the Address, not either seat. The delivery count must be
    /// zero: only an exactly correlated native receipt can consume work.
    fn awaken_metaflow(
        &self,
        address: &Address,
        flow_id: &str,
        delivered: usize,
    ) -> Result<bool, StoreError>;
    /// The metaflow becomes Awake again as its last flow, resumed on its own
    /// native session in the seat `birth` names, in one write: the flow,
    /// Stopped or Exited, Active again; its route and SeatBirth the resumed
    /// seat's; its Address taken as a wake takes it, the flow out of Past,
    /// the successor a Maintained metaflow was owed given and what another
    /// flow owed at the Address taken over. False, and nothing written,
    /// when the flow is not one that ended or the Address cannot be taken.
    fn resume_metaflow(&self, address: &Address, birth: SeatBirth) -> Result<bool, StoreError>;
    #[cfg(test)]
    fn remove_oldest_queued(
        &self,
        address: &Address,
        typed: &flow_ethos::QueueEntry,
    ) -> Result<bool, StoreError>;
    /// The metaflow currently Awake as this flow, if any.
    fn metaflow_awake_as(&self, flow_id: &str) -> Result<Option<Metaflow>, StoreError>;
    /// The metaflow at this Address, if any.
    fn metaflow(&self, address: &Address) -> Result<Option<Metaflow>, StoreError>;
}

impl MutatesMetaflows for FlowStore {
    fn bind_metaflow(
        &self,
        binding: Bind_Data,
        launched: Option<FlowId>,
    ) -> Result<Response, StoreError> {
        let peer = &binding.process;
        if !FlowStore::process_is_live(peer) {
            return Ok(Response::Refused(Refusal::unidentified(binding.process)));
        }
        let address = binding.address.clone();
        let binding_address = address.clone();
        let bindings = self
            .engine
            .match_records(QueryPlan::all(self.metaflow_processes))?;
        if bindings
            .records()
            .iter()
            .any(|binding| binding.process == *peer && binding.address != address)
        {
            return Ok(Response::Refused(Refusal::Taken(address)));
        }
        let existing = self.stored_metaflow(&address)?;
        if let Some(stored) = existing.as_ref()
            && let Some(previous) = self.bound_process(&address)?
        {
            if previous == *peer
                && let MetaflowState::Awake(id) = &stored.metaflow.state
            {
                return Ok(Response::Bound(id.clone()));
            }
            if FlowStore::process_is_live(&previous) {
                return Ok(Response::Refused(Refusal::Taken(address)));
            }
        }
        let state = self.state()?;
        let Some(next) = state.next_flow_number.checked_add(1) else {
            return Err(StoreError::StateInvariant);
        };
        let flow_id =
            launched.unwrap_or_else(|| FlowId(format!("flow-{:016x}", state.next_flow_number)));
        let new_metaflow = existing.is_none();
        let new_binding = self.bound_process(&address)?.is_none();
        let mut metaflow = existing.map(|row| row.metaflow).unwrap_or(Metaflow {
            address: address.clone(),
            state: MetaflowState::Asleep,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
        if let MetaflowState::Awake(previous) = metaflow.state
            && previous != flow_id
        {
            metaflow.past.0.push(previous);
        }
        metaflow.state = MetaflowState::Awake(flow_id.clone());
        let commit = self.engine.begin_atomic_commit().mutate(
            self.state,
            FlowStoreState {
                next_flow_number: next,
            },
        );
        let commit = if new_metaflow {
            commit.assert(self.metaflows, StoredMetaflow { metaflow })
        } else {
            commit.mutate(self.metaflows, StoredMetaflow { metaflow })
        };
        let binding = StoredMetaflowProcess {
            process: peer.clone(),
            address,
        };
        let commit = if new_binding {
            commit.assert(self.metaflow_processes, binding)
        } else {
            commit.mutate(self.metaflow_processes, binding)
        };
        // The bound flow is the Address's receiving flow from this commit,
        // a process binding's among them: what the flow it displaced owed
        // there is taken over in it.
        let reassigned = self.reaps_reassigned(&binding_address, Some(&flow_id), commit)?;
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("MetaflowBind");
            if self.faults.refused_now(RefusedWrite::MetaflowBind) {
                return Err(StoreError::StateInvariant);
            }
        }
        self.engine.commit_atomic(reassigned.commit)?;
        if reassigned.moved > 0 {
            self.launch_changes.announce();
        }
        Ok(Response::Bound(flow_id))
    }

    fn lock_metaflow(&self, request: Lock_Data, now: i64) -> Result<Response, StoreError> {
        // The sender is whom the request is queued under; it is checked
        // against nothing.
        let address = request.second_address;
        if !self.prepare_declared_metaflow(&address)? {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                address,
            ))));
        }
        if let Some(held) = self.stored_metaflow_lock(&address)?
            && held.lock.until > flow_ethos::generated::library::Until(now)
        {
            return Ok(Response::Refused(Refusal::Held(held.lock)));
        }
        let Some(configuration) = self.configuration_snapshot()? else {
            return Ok(Response::Refused(Refusal::NotConfigured));
        };
        let Some(until) = now
            .checked_add(configuration.configured_nexus.0.lease.0)
            .filter(|until| *until > now)
        else {
            return Ok(Response::Refused(Refusal::Store(
                "configured lease must be positive and fit the clock".into(),
            )));
        };
        let lock = Lock {
            first_address: request.first_address,
            second_address: address,
            until: flow_ethos::generated::library::Until(until),
        };
        let row = StoredMetaflowLock { lock: lock.clone() };
        let commit = self.engine.begin_atomic_commit();
        let commit = if self.stored_metaflow_lock(&lock.second_address)?.is_some() {
            commit.mutate(self.metaflow_locks, row)
        } else {
            commit.assert(self.metaflow_locks, row)
        };
        self.engine.commit_atomic(commit)?;
        Ok(Response::Locked(lock))
    }

    fn queue_metaflow(
        &self,
        request: Deliver_Data,
        now: i64,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};
        use sha2::Digest;
        let digest_text = request.datomize(Vec::new()).protosize().compact();
        let digest = format!("{:x}", sha2::Sha256::digest(digest_text.as_bytes()));
        let message = match self.resolve_sendable_message(request.sendable_message.clone())? {
            Ok(message) => message,
            Err(refusal) => return Ok(signal_flow::PrivateSendResponse::Refused(refusal)),
        };
        let Some(held) = self.stored_metaflow_lock(&request.lock.second_address)? else {
            return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(
                Unknown_Data::Lock(request.lock),
            )));
        };
        if held.lock != request.lock {
            return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(
                Unknown_Data::Lock(request.lock),
            )));
        }
        if held.lock.until <= flow_ethos::generated::library::Until(now) {
            return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Lapsed));
        }
        let Some(_) = self.stored_metaflow(&request.lock.second_address)? else {
            return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(
                Unknown_Data::Address(request.lock.second_address),
            )));
        };
        if let Some(refusal) = message.oversized_refusal(&request.lock.first_address) {
            let released = self.release_metaflow_lock(request.lock.clone(), now)?;
            if released != Response::Released {
                return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Store(
                    "a verified delivery lock could not be released".into(),
                )));
            }
            return Ok(signal_flow::PrivateSendResponse::Refused(refusal));
        }
        let delivery = if matches!(message, signal_flow::Message::Soft(_)) {
            flow_ethos::Delivery::Queue
        } else {
            flow_ethos::Delivery::Wake
        };
        self.accept_deliver_call(
            &request.lock.second_address,
            request.lock.first_address.clone(),
            delivery,
            message,
            VerifiedDeliveryLock {
                lock: request.lock.clone(),
                now,
            },
            super::logical_delivery::DeliverCallSpec {
                key: request.deliver_key,
                sender: request.lock.first_address,
                digest,
            },
        )
    }
}

impl SleepsMetaflows for FlowStore {
    fn sleep_metaflow(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(false);
        };
        if stored.metaflow.state != MetaflowState::Awake(flow_ethos::FlowId(flow_id.to_owned())) {
            return Ok(false);
        }
        stored.metaflow.state = MetaflowState::Asleep;
        stored
            .metaflow
            .past
            .0
            .push(flow_ethos::FlowId(flow_id.to_owned()));
        // The ended flow's locks as a sender go in the same write.
        use super::address_hold::HoldsLaunchAddress;
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        let commit = self.sender_locks_released(address, commit)?;
        // The Address receives as nobody from this commit: what the ended
        // flow owed there is held by the Address, in it.
        let reassigned = self.reaps_reassigned(address, None, commit)?;
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("MetaflowSleep");
            if self.faults.refused_now(RefusedWrite::MetaflowSleep) {
                return Err(StoreError::StateInvariant);
            }
        }
        self.engine.commit_atomic(reassigned.commit)?;
        if reassigned.moved > 0 {
            self.launch_changes.announce();
        }
        Ok(true)
    }

    fn awaken_metaflow(
        &self,
        address: &Address,
        flow_id: &str,
        delivered: usize,
    ) -> Result<bool, StoreError> {
        let AddressTaking::Takes(taken) = self.address_taking(address, flow_id, None, delivered)?
        else {
            return Ok(false);
        };
        use super::successor::OwesSuccessors;
        // A woken flow is the successor a Maintained metaflow was owed.
        let commit = self.successor_given(
            address,
            taken.written(self, self.engine.begin_atomic_commit()),
        )?;
        // The woken flow receives from this commit: what another flow owed
        // at the Address is taken over in it (its own launch's settlement
        // took it over already when it is that launch's flow).
        let flow = FlowId(flow_id.to_owned());
        let reassigned = self.reaps_reassigned(address, Some(&flow), commit)?;
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("MetaflowAwaken");
            if self.faults.refused_now(RefusedWrite::MetaflowAwaken) {
                return Err(StoreError::StateInvariant);
            }
        }
        self.engine.commit_atomic(reassigned.commit)?;
        if reassigned.moved > 0 {
            self.launch_changes.announce();
        }
        Ok(true)
    }

    fn resume_metaflow(&self, address: &Address, birth: SeatBirth) -> Result<bool, StoreError> {
        let flow_id = birth.flow_id.0.clone();
        let Some(mut flow) = self.flow(&flow_id)? else {
            return Ok(false);
        };
        if !matches!(
            flow.lifecycle,
            FlowLifecycle::Stopped | FlowLifecycle::Exited
        ) {
            return Ok(false);
        }
        let AddressTaking::Takes(taken) = self.address_taking(address, &flow_id, None, 0)? else {
            return Ok(false);
        };
        flow.lifecycle = FlowLifecycle::Active;
        let commit = taken.written(
            self,
            self.engine.begin_atomic_commit().mutate(self.flows, flow),
        );
        let route = FlowHerdrRouteRecord {
            flow_id: flow_id.clone(),
            route: birth.herdr_route.clone(),
        };
        let commit = match self.herdr_route(&flow_id)? {
            Some(_) => commit.mutate(self.herdr_routes, route),
            None => commit.assert(self.herdr_routes, route),
        };
        let commit = match super::restart::RecordsRestarts::seat_birth(self, &flow_id)? {
            Some(_) => commit.mutate(self.seat_births, birth),
            None => commit.assert(self.seat_births, birth),
        };
        use super::successor::OwesSuccessors;
        let commit = self.successor_given(address, commit)?;
        let flow = FlowId(flow_id);
        let reassigned = self.reaps_reassigned(address, Some(&flow), commit)?;
        self.engine.commit_atomic(reassigned.commit)?;
        self.launch_changes.announce();
        Ok(true)
    }

    #[cfg(test)]
    fn remove_oldest_queued(
        &self,
        address: &Address,
        typed: &flow_ethos::QueueEntry,
    ) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(false);
        };
        if stored.metaflow.queue.0.first() != Some(typed) {
            return Ok(false);
        }
        stored.metaflow.queue.0.remove(0);
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        self.engine.commit_atomic(commit)?;
        Ok(true)
    }

    fn metaflow_awake_as(&self, flow_id: &str) -> Result<Option<Metaflow>, StoreError> {
        let awake = MetaflowState::Awake(flow_ethos::FlowId(flow_id.to_owned()));
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.metaflows))?
            .records()
            .iter()
            .find(|stored| stored.metaflow.state == awake)
            .map(|stored| stored.metaflow.clone()))
    }

    fn metaflow(&self, address: &Address) -> Result<Option<Metaflow>, StoreError> {
        Ok(self.stored_metaflow(address)?.map(|stored| stored.metaflow))
    }
}

/// What a launch may do to the metaflow of the Address it names.
pub(crate) enum AddressTaking {
    Takes(TakenAddress),
    Refused(StartRejection),
}

/// The writes that take an Address for a launched flow.
pub(crate) struct TakenAddress {
    metaflow: StoredMetaflow,
    new: bool,
    /// A binding of a process no longer live, left by an earlier Bind.
    stale_binding: bool,
}

/// How a flow Flow launched takes the metaflow of its Address. The rules
/// are Bind's: a metaflow Awake as a live flow, or bound to a live process,
/// is refused (AddressAwake), and an absent, Asleep, or Awake-as-not-live
/// one is taken, a displaced flow
/// joining Past. A launch takes over the predecessor its profile names,
/// live or not, and a flow already Awake there keeps it. A taken flow is
/// never also Past.
pub(crate) trait TakesLaunchAddress {
    /// Taking `address` binds a seat and preserves every pending request.
    /// A nonzero delivery count refuses: a binding is not a native receipt.
    fn address_taking(
        &self,
        address: &Address,
        flow_id: &str,
        predecessor: Option<&str>,
        delivered: usize,
    ) -> Result<AddressTaking, StoreError>;
    /// Why a Start of this profile would be refused before anything
    /// launches: its Address could not be taken, or another launch not yet
    /// settled names it.
    fn launch_address_refusal(
        &self,
        profile: &LaunchProfile,
    ) -> Result<Option<StartRejection>, StoreError>;
    /// Whether a flow is live: its row is Pending or Active.
    fn flow_is_live(&self, flow_id: &str) -> Result<bool, StoreError>;
}

impl TakesLaunchAddress for FlowStore {
    fn address_taking(
        &self,
        address: &Address,
        flow_id: &str,
        predecessor: Option<&str>,
        delivered: usize,
    ) -> Result<AddressTaking, StoreError> {
        let existing = self.stored_metaflow(address)?;
        let binding = self.bound_process(address)?;
        let live_binding = binding.as_ref().is_some_and(FlowStore::process_is_live);
        let mut metaflow = match existing.as_ref() {
            Some(stored) => stored.metaflow.clone(),
            None => Metaflow {
                address: address.clone(),
                state: MetaflowState::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            },
        };
        match &metaflow.state {
            MetaflowState::Awake(current) if current.0 == flow_id => {}
            MetaflowState::Awake(current) => {
                if Some(current.0.as_str()) != predecessor
                    && (live_binding || self.flow_is_live(&current.0)?)
                {
                    return Ok(AddressTaking::Refused(StartRejection::AddressAwake));
                }
                metaflow.past.0.push(current.clone());
            }
            MetaflowState::Asleep if live_binding => {
                return Ok(AddressTaking::Refused(StartRejection::AddressAwake));
            }
            MetaflowState::Asleep => {}
        }
        if delivered != 0 {
            return Ok(AddressTaking::Refused(
                StartRejection::LaunchPersistenceRefused,
            ));
        }
        metaflow.past.0.retain(|past| past.0 != flow_id);
        metaflow.state = MetaflowState::Awake(flow_ethos::FlowId(flow_id.to_owned()));
        Ok(AddressTaking::Takes(TakenAddress {
            metaflow: StoredMetaflow { metaflow },
            new: existing.is_none(),
            stale_binding: binding.is_some() && !live_binding,
        }))
    }

    fn launch_address_refusal(
        &self,
        profile: &LaunchProfile,
    ) -> Result<Option<StartRejection>, StoreError> {
        let predecessor = profile
            .flow_id_option
            .as_ref()
            .map(|value| value.0.as_str());
        let refused = match self.stored_metaflow(&profile.address)? {
            Some(stored) => match &stored.metaflow.state {
                MetaflowState::Awake(current) => (Some(current.0.as_str()) != predecessor
                    && (self.flow_is_live(&current.0)?
                        || self
                            .bound_process(&profile.address)?
                            .as_ref()
                            .is_some_and(FlowStore::process_is_live)))
                .then_some(StartRejection::AddressAwake),
                MetaflowState::Asleep => self
                    .bound_process(&profile.address)?
                    .as_ref()
                    .is_some_and(FlowStore::process_is_live)
                    .then_some(StartRejection::AddressAwake),
            },
            None => None,
        };
        if refused.is_some() {
            return Ok(refused);
        }
        for attempt in self
            .engine
            .match_records(QueryPlan::all(self.launch_attempts))?
            .records()
        {
            let attempt = &attempt.attempt;
            if attempt.launch_request_id != profile.launch_request_id
                && attempt.launch_profile.address == profile.address
                && self.launch_attempt_unsettled(attempt)?
            {
                return Ok(Some(StartRejection::AddressAwake));
            }
        }
        Ok(None)
    }

    fn flow_is_live(&self, flow_id: &str) -> Result<bool, StoreError> {
        Ok(self.flow(flow_id)?.is_some_and(|flow| {
            matches!(
                flow.lifecycle,
                FlowLifecycle::Pending | FlowLifecycle::Active
            )
        }))
    }
}

/// Adds the writes that take an Address to an atomic commit.
pub(crate) trait WritesTakenAddress {
    /// The writes that take the Address: the metaflow Awake as the flow and
    /// a stale binding retracted.
    fn written(
        self,
        store: &FlowStore,
        commit: sema_engine::AtomicCommit,
    ) -> sema_engine::AtomicCommit;
}

impl WritesTakenAddress for TakenAddress {
    fn written(
        self,
        store: &FlowStore,
        commit: sema_engine::AtomicCommit,
    ) -> sema_engine::AtomicCommit {
        let key = FlowStore::address_record_key(&self.metaflow.metaflow.address);
        let commit = if self.new {
            commit.assert(store.metaflows, self.metaflow)
        } else {
            commit.mutate(store.metaflows, self.metaflow)
        };
        if self.stale_binding {
            commit.retract(store.metaflow_processes, RecordKey::new(key))
        } else {
            commit
        }
    }
}

/// An Address Flow holds only through process bindings: Awake as an id
/// that names no flow row, every past id naming none either. Bind writes
/// one for a peer that runs in no flow Flow launched; Flow can neither
/// deliver to it nor wake it.
pub(crate) trait ForgetsEndedBindings {
    /// Retracts the metaflow at `address`, its process binding and any lock
    /// on it, in one write, when it is Awake as `flow_id`, a binding is
    /// recorded for it and its process no longer runs, nothing is queued, and neither `flow_id` nor
    /// any past id names a flow row. True when it was forgotten; false
    /// leaves it as it was.
    fn forget_ended_binding(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError>;
}

impl ForgetsEndedBindings for FlowStore {
    fn forget_ended_binding(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError> {
        let Some(stored) = self.stored_metaflow(address)? else {
            return Ok(false);
        };
        if stored.metaflow.state != MetaflowState::Awake(flow_ethos::FlowId(flow_id.to_owned()))
            || !stored.metaflow.queue.0.is_empty()
        {
            return Ok(false);
        }
        match self.bound_process(address)? {
            Some(process) if !FlowStore::process_is_live(&process) => {}
            _ => return Ok(false),
        }
        for id in stored
            .metaflow
            .past
            .0
            .iter()
            .chain(std::iter::once(&signal_flow::FlowId(flow_id.to_owned())))
        {
            if self.flow(&id.0)?.is_some() {
                return Ok(false);
            }
        }
        let key = FlowStore::address_record_key(address);
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .retract(self.metaflows, RecordKey::new(key.clone()))
            .retract(self.metaflow_processes, RecordKey::new(key.clone()));
        if self.stored_metaflow_lock(address)?.is_some() {
            commit = commit.retract(self.metaflow_locks, RecordKey::new(key));
        }
        // The Address receives as nobody once its metaflow is forgotten:
        // what the binding owed there is held by the Address, in this commit.
        let reassigned = self.reaps_reassigned(address, None, commit)?;
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("BindingForget");
            if self.faults.refused_now(RefusedWrite::BindingForget) {
                return Err(StoreError::StateInvariant);
            }
        }
        self.engine.commit_atomic(reassigned.commit)?;
        if reassigned.moved > 0 {
            self.launch_changes.announce();
        }
        Ok(true)
    }
}

/// Seeds a metaflow row directly, for tests of what reads and moves it.
#[cfg(test)]
pub(crate) trait SeedsMetaflow {
    fn seed_metaflow(&self, metaflow: Metaflow);
}

#[cfg(test)]
impl SeedsMetaflow for FlowStore {
    fn seed_metaflow(&self, mut metaflow: Metaflow) {
        // Hand-authored fixtures historically used `test` for every queued
        // request.  Request ids are store-wide now, so retain the first value
        // and give later colliding fixture rows stable four-character ids.
        let bindings = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.request_ids))
            .expect("request ids readable");
        let existing = bindings
            .records()
            .iter()
            .map(|binding| {
                (
                    binding.request_id.clone(),
                    binding.logical_delivery_id.clone(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        let mut seen = existing
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        for entry in &mut metaflow.queue.0 {
            if existing
                .get(&entry.request_id)
                .is_some_and(|logical_id| logical_id == &entry.logical_delivery_id)
                || seen.insert(entry.request_id.clone())
            {
                continue;
            }
            use sha2::Digest;
            let digest = sha2::Sha256::digest(entry.logical_delivery_id.0.as_bytes());
            let mut candidate = String::with_capacity(4);
            for byte in digest {
                candidate
                    .push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(byte as usize) % 36] as char);
                if candidate.len() == 4 {
                    break;
                }
            }
            while !seen.insert(flow_ethos::RequestId(candidate.clone())) {
                let last = candidate.pop().expect("four-character fixture id");
                let at = b"0123456789abcdefghijklmnopqrstuvwxyz"
                    .iter()
                    .position(|byte| *byte as char == last)
                    .expect("base36 fixture id");
                candidate.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(at + 1) % 36] as char);
            }
            entry.request_id = flow_ethos::RequestId(candidate);
        }
        let exists = self
            .stored_metaflow(&metaflow.address)
            .expect("metaflow readable")
            .is_some();
        let commit = self.engine.begin_atomic_commit();
        let commit = if exists {
            commit.mutate(self.metaflows, StoredMetaflow { metaflow })
        } else {
            commit.assert(self.metaflows, StoredMetaflow { metaflow })
        };
        self.engine.commit_atomic(commit).expect("metaflow seeded");
        self.seed_missing_queued_deliveries()
            .expect("seed queued lifecycle snapshots");
    }
}

/// Where a flow stands in its Address's succession, read from the metaflow
/// row and the AddressHold rows alone: the record its launch writes at
/// acknowledgement and at confirmation, before any reap or handover. No
/// obligation, outcome or lifecycle decides it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Succession {
    /// The Address receives as this flow.
    Receiving,
    /// A later flow of its Address's lineage is confirmed: the Address
    /// receives as `by`, or, Asleep, last received as it.
    Superseded { address: Address, by: FlowId },
    /// It holds its Address; its launch is not confirmed.
    Unconfirmed,
    /// No metaflow or AddressHold names it, or it was the last flow of an
    /// Asleep metaflow: it is routed by its lifecycle.
    Unseated,
}

/// One reading of every flow's succession, for a scan that asks of many.
#[derive(Debug, Clone, Default)]
pub(crate) struct SuccessionIndex {
    successions: std::collections::BTreeMap<String, Succession>,
}

/// The succession a flow has in an index.
pub(crate) trait ReadsSuccessionIndex {
    fn of(&self, flow_id: &str) -> Succession;
}

impl ReadsSuccessionIndex for SuccessionIndex {
    fn of(&self, flow_id: &str) -> Succession {
        self.successions
            .get(flow_id)
            .cloned()
            .unwrap_or(Succession::Unseated)
    }
}

/// Which flow an Address receives as, and where any flow stands.
pub(crate) trait ReadsSuccession {
    fn succession(&self, flow_id: &str) -> Result<Succession, StoreError>;
    /// The flow `address` receives as: the flow its metaflow is Awake as,
    /// or, while that flow's launch is unconfirmed, the flow its hold found
    /// the metaflow Awake as.
    fn receiving_flow(&self, address: &Address) -> Result<Option<FlowId>, StoreError>;
    fn successions(&self) -> Result<SuccessionIndex, StoreError>;
}

/// The succession every metaflow and hold implies.
trait ComputesSuccession {
    fn computed(
        metaflows: &[Metaflow],
        holds: &[super::address_hold::AddressHold],
    ) -> SuccessionIndex;
    fn receiving_of(
        metaflow: &Metaflow,
        holds: &[super::address_hold::AddressHold],
    ) -> Option<FlowId>;
}

impl ComputesSuccession for FlowStore {
    fn receiving_of(
        metaflow: &Metaflow,
        holds: &[super::address_hold::AddressHold],
    ) -> Option<FlowId> {
        let MetaflowState::Awake(awake) = &metaflow.state else {
            return None;
        };
        let found = holds
            .iter()
            .find(|hold| hold.flow_id == awake.0)
            .map(|hold| hold.found.as_ref().map(|found| found.state.clone()));
        match found {
            // Between acknowledgement and confirmation the Address receives
            // as the flow the hold found it Awake as, if any.
            Some(Some(MetaflowState::Awake(found))) if found != *awake => Some(found),
            Some(_) => None,
            None => Some(awake.clone()),
        }
    }

    fn computed(
        metaflows: &[Metaflow],
        holds: &[super::address_hold::AddressHold],
    ) -> SuccessionIndex {
        let mut successions = std::collections::BTreeMap::new();
        for metaflow in metaflows {
            let awake = match &metaflow.state {
                MetaflowState::Awake(flow_id) => Some(flow_id.clone()),
                MetaflowState::Asleep => None,
            };
            let mut lineage: Vec<FlowId> = Vec::new();
            for flow_id in metaflow.past.0.iter().cloned().chain(awake.clone()) {
                lineage.retain(|known| known != &flow_id);
                lineage.push(flow_id);
            }
            let receiving = Self::receiving_of(metaflow, holds);
            let last = lineage.last().cloned();
            for (at, flow_id) in lineage.iter().enumerate() {
                let succession = if Some(flow_id) == receiving.as_ref() {
                    Succession::Receiving
                } else if at + 1 < lineage.len() {
                    // Superseded by the flow the Address receives as now,
                    // or, Asleep, the last it received as.
                    match receiving.clone().or_else(|| match &awake {
                        None => last.clone(),
                        Some(_) => None,
                    }) {
                        Some(by) if by != *flow_id => Succession::Superseded {
                            address: metaflow.address.clone(),
                            by,
                        },
                        _ => Succession::Unseated,
                    }
                } else {
                    Succession::Unseated
                };
                successions.insert(flow_id.0.clone(), succession);
            }
        }
        // A hold is read first: its flow's launch is not confirmed.
        for hold in holds {
            successions.insert(hold.flow_id.clone(), Succession::Unconfirmed);
        }
        SuccessionIndex { successions }
    }
}

impl ReadsSuccession for FlowStore {
    fn succession(&self, flow_id: &str) -> Result<Succession, StoreError> {
        Ok(self.successions()?.of(flow_id))
    }

    fn receiving_flow(&self, address: &Address) -> Result<Option<FlowId>, StoreError> {
        let Some(metaflow) = self.stored_metaflow(address)?.map(|stored| stored.metaflow) else {
            return Ok(None);
        };
        let holds = self
            .engine
            .match_records(QueryPlan::all(self.address_holds))?
            .records()
            .to_vec();
        Ok(Self::receiving_of(&metaflow, &holds))
    }

    fn successions(&self) -> Result<SuccessionIndex, StoreError> {
        let metaflows = self
            .engine
            .match_records(QueryPlan::all(self.metaflows))?
            .records()
            .iter()
            .map(|stored| stored.metaflow.clone())
            .collect::<Vec<_>>();
        let holds = self
            .engine
            .match_records(QueryPlan::all(self.address_holds))?
            .records()
            .to_vec();
        Ok(Self::computed(&metaflows, &holds))
    }
}

/// The flows a launch remembers, as its Remembering names them: the flows
/// named one by one, or the metaflow's last flows to a depth, oldest first.
/// The metaflow's lineage is its past then the flow it is Awake as; the flow
/// the launch itself bound is never its own predecessor, so a launch read
/// again after its flow took the Address resolves as it first did.
pub(crate) trait ResolvesRemembering {
    fn remembered_flows(
        &self,
        profile: &LaunchProfile,
    ) -> Result<Vec<signal_flow::FlowId>, StoreError>;
}

impl ResolvesRemembering for FlowStore {
    fn remembered_flows(
        &self,
        profile: &LaunchProfile,
    ) -> Result<Vec<signal_flow::FlowId>, StoreError> {
        let depth = match &profile.remembering {
            signal_flow::Remembering::Custom(flows) => return Ok(flows.clone()),
            signal_flow::Remembering::Predecessors(depth) => {
                usize::try_from(depth.0).unwrap_or_default()
            }
        };
        if depth == 0 {
            return Ok(Vec::new());
        }
        let Some(stored) = self.stored_metaflow(&profile.address)? else {
            return Ok(Vec::new());
        };
        let own = self
            .launch_attempt(&profile.launch_request_id.0)?
            .and_then(|attempt| attempt.native_launch_binding_option)
            .map(|binding| binding.flow_id);
        let mut lineage: Vec<signal_flow::FlowId> = Vec::new();
        let awake = match &stored.metaflow.state {
            MetaflowState::Awake(flow_id) => Some(flow_id.clone()),
            MetaflowState::Asleep => None,
        };
        for flow_id in stored.metaflow.past.0.iter().cloned().chain(awake) {
            if Some(&flow_id) == own.as_ref() {
                continue;
            }
            lineage.retain(|known| known != &flow_id);
            lineage.push(flow_id);
        }
        let start = lineage.len().saturating_sub(depth);
        Ok(lineage.split_off(start))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deliver_key(counter: i64) -> signal_flow::DeliverKey {
        signal_flow::DeliverKey {
            namespace: signal_flow::Namespace("testMessageMemory".into()),
            counter: signal_flow::Counter(counter),
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        store: FlowStore,
        peer: Process,
        message: Address,
    }
    impl Fixture {
        fn new(binary: String) -> Self {
            let directory = tempfile::tempdir().expect("private fixture");
            let path = directory.path().join("flow.sema");
            let defaults = DefaultConfiguration {
                home: directory.path().join("home"),
                runtime_directory: directory.path().join("runtime"),
            };
            let settings = defaults.configuration();
            let configuration =
                ConfigurationOnlyStore::open(&path).expect("new configuration store");
            configuration
                .configure_change(ConfigurationChange::Nexus(ConfigureNexus {
                    source_root: flow_ethos::SourceRoot(settings.source_root),
                    first_codex_endpoint: settings.stable_codex,
                    second_codex_endpoint: settings.next_codex,
                    harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                    meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                    message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                        settings.message_nexus_path.clone(),
                    )),
                    message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(binary)),
                    curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                        "/fixture/curriculum.sock".into(),
                    ),
                    lease: flow_ethos::Lease(30),
                    presentation_prompt_option: None,
                    herdr_session_name: signal_flow::HerdrSessionName("default".into()),
                }))
                .expect("Nexus configuration");
            drop(configuration);
            let store = FlowStore::open_configured(&path)
                .expect("schema2 reopens through byte-valued Engine counters");
            Self {
                _directory: directory,
                store,
                peer: FlowStore::process_identity_for_pid(std::process::id())
                    .expect("current process"),
                message: Address {
                    aspect: Aspect::Field,
                    topic: flow_ethos::Topic("message".into()),
                    model: Model::Astra,
                },
            }
        }
    }

    /// Bind is taken from any caller and for any executable: the Message
    /// Address binds a process whatever binary it runs. Only a process that
    /// is not live is refused.
    #[test]
    fn bind_takes_any_live_process_whatever_its_executable() {
        let fixture = Fixture::new("/not/the/actual/executable".into());
        let binding = Bind_Data {
            address: fixture.message.clone(),
            process: fixture.peer.clone(),
        };
        assert!(matches!(
            fixture.store.bind_metaflow(binding.clone(), None).unwrap(),
            Response::Bound(_)
        ));
        let binding = Bind_Data {
            address: Address {
                model: Model::Luna,
                ..fixture.message.clone()
            },
            process: fixture.peer.clone(),
        };
        let mut forged = binding;
        forged.process.started.0 += 1;
        assert!(matches!(
            fixture.store.bind_metaflow(forged, None).unwrap(),
            Response::Refused(Refusal::Unidentified(_))
        ));
        assert_eq!(fixture.store.list_metaflows().unwrap().len(), 1);
    }

    #[test]
    fn lock_queue_is_atomic_exact_and_never_claims_native_delivery() {
        let fixture = Fixture::new(
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        );
        let binding = Bind_Data {
            address: fixture.message.clone(),
            process: fixture.peer.clone(),
        };
        let first = fixture.store.bind_metaflow(binding.clone(), None).unwrap();
        assert!(matches!(&first, Response::Bound(_)));
        assert_eq!(fixture.store.bind_metaflow(binding, None).unwrap(), first);
        let target = Address {
            model: Model::Sol,
            ..fixture.message.clone()
        };
        fixture.store.seed_metaflow(Metaflow {
            address: target.clone(),
            state: MetaflowState::Asleep,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
        let request = Lock_Data {
            first_address: fixture.message.clone(),
            second_address: target.clone(),
        };
        let Response::Locked(lock) = fixture.store.lock_metaflow(request.clone(), 100).unwrap()
        else {
            panic!("lease expected")
        };
        assert_eq!(lock.until, flow_ethos::Until(130));
        assert_eq!(
            fixture.store.lock_metaflow(request, 100).unwrap(),
            Response::Refused(Refusal::Held(lock.clone()))
        );
        let body = Request::Living(flow_ethos::Living_Data {
            context: flow_ethos::Context("fixture context".into()),
            verbatim: flow_ethos::Verbatim("Exact words.\nSecond line.".into()),
        });
        let mut forged = lock.clone();
        forged.until.0 += 1;
        assert!(matches!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock: forged,
                        deliver_key: deliver_key(0),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Living(match body.clone() {
                                Request::Living(value) =>
                                    signal_flow::SendableRequest_Living_Data {
                                        context: value.context,
                                        verbatim: value.verbatim,
                                    },
                                _ => unreachable!(),
                            })
                        )
                    },
                    101
                )
                .unwrap(),
            signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(_))
        ));
        assert!(
            fixture
                .store
                .metaflow(&target)
                .unwrap()
                .unwrap()
                .queue
                .0
                .is_empty()
        );
        assert!(matches!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock: lock.clone(),
                        deliver_key: deliver_key(1),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Living(match body.clone() {
                                Request::Living(value) =>
                                    signal_flow::SendableRequest_Living_Data {
                                        context: value.context,
                                        verbatim: value.verbatim,
                                    },
                                _ => unreachable!(),
                            })
                        )
                    },
                    101
                )
                .unwrap(),
            signal_flow::PrivateSendResponse::SendAccepted(_)
        ));
        // Queued with the Sender its Lock named.
        assert_eq!(
            fixture.store.metaflow(&target).unwrap().unwrap().queue.0[0].sent_by,
            signal_flow::SentBy::Sender(lock.first_address.clone())
        );
        assert_eq!(
            fixture.store.metaflow(&target).unwrap().unwrap().queue.0[0].message,
            flow_ethos::Message::Soft(body)
        );
        assert!(
            fixture
                .store
                .stored_metaflow_lock(&lock.second_address)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock,
                        deliver_key: deliver_key(2),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Notice("duplicate".into())
                        )
                    },
                    102
                )
                .unwrap(),
            signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(_))
        ));
    }

    #[test]
    fn unknown_sender_and_expired_lease_do_not_append() {
        let fixture = Fixture::new(
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
        );
        let request = Lock_Data {
            first_address: fixture.message.clone(),
            second_address: fixture.message.clone(),
        };
        assert!(matches!(
            fixture.store.lock_metaflow(request.clone(), 10).unwrap(),
            Response::Refused(Refusal::Unknown(_))
        ));
        fixture
            .store
            .bind_metaflow(
                Bind_Data {
                    address: fixture.message.clone(),
                    process: fixture.peer.clone(),
                },
                None,
            )
            .unwrap();
        let Response::Locked(lock) = fixture.store.lock_metaflow(request, 10).unwrap() else {
            panic!("lease expected")
        };
        assert_eq!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock,
                        deliver_key: deliver_key(3),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Notice("expired".into())
                        )
                    },
                    40
                )
                .unwrap(),
            signal_flow::PrivateSendResponse::Refused(Refusal::Lapsed)
        );
        assert!(
            fixture.store.list_metaflows().unwrap()[0]
                .queue
                .0
                .is_empty()
        );
    }
}
