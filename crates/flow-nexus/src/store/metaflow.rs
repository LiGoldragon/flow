//! Ordinary metaflow mutations. The connection supplies the witnessed peer;
//! the serial dispatcher holds exclusion across authorization and mutation.
use super::logical_delivery::{RecordsLogicalDeliveries, VerifiedDeliveryLock};
use super::*;
use crate::sending::AdmitsSendPayload;
use signal_flow::{
    Bind_Data, Deliver_Data, FlowId, LaunchProfile, Lock_Data, Recipient, Refusal, SentBy,
    StartRejection, Unknown_Data,
};

pub(crate) trait MutatesMetaflows {
    fn message_peer(&self, peer: &Process) -> Result<bool, StoreError>;
    /// Binds the peer to its Address. `launched` is the FlowId of the
    /// launched flow the peer runs in, when Flow can name it; the metaflow
    /// is then Awake as that flow, so its Stop drains its queue.
    fn bind_metaflow(
        &self,
        binding: Bind_Data,
        peer: &Process,
        launched: Option<FlowId>,
    ) -> Result<Response, StoreError>;
    fn lock_metaflow(&self, request: Lock_Data, now: i64) -> Result<Response, StoreError>;
    fn queue_metaflow(&self, request: Deliver_Data, now: i64) -> Result<Response, StoreError>;
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
    fn message_peer(&self, peer: &Process) -> Result<bool, StoreError> {
        let address = Address {
            aspect: Aspect::Field,
            topic: "message".into(),
            layer: Layer::Primary,
        };
        Ok(
            FlowStore::process_is_live(peer)
                && self.bound_process(&address)?.as_ref() == Some(peer),
        )
    }

    fn bind_metaflow(
        &self,
        binding: Bind_Data,
        peer: &Process,
        launched: Option<FlowId>,
    ) -> Result<Response, StoreError> {
        if binding.process != *peer || !FlowStore::process_is_live(peer) {
            return Ok(Response::Refused(Refusal::Unidentified(binding.process)));
        }
        let address = binding.address;
        if address
            == (Address {
                aspect: Aspect::Field,
                topic: "message".into(),
                layer: Layer::Primary,
            })
        {
            let Some(configuration) = self.configuration_snapshot()? else {
                return Ok(Response::Refused(Refusal::NotConfigured));
            };
            let expected = fs::canonicalize(&configuration.nexus.message_nexus_binary).ok();
            if expected.is_none() || FlowStore::process_executable(peer) != expected {
                return Ok(Response::Refused(Refusal::NotMessage));
            }
        }
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
        let flow_id = launched.unwrap_or_else(|| format!("flow-{:016x}", state.next_flow_number));
        let new_metaflow = existing.is_none();
        let new_binding = self.bound_process(&address)?.is_none();
        let mut metaflow = existing.map(|row| row.metaflow).unwrap_or(Metaflow {
            address: address.clone(),
            state: MetaflowState::Asleep,
            past: vec![],
            queue: vec![],
        });
        if let MetaflowState::Awake(previous) = metaflow.state
            && previous != flow_id
        {
            metaflow.past.push(previous);
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
        self.engine.commit_atomic(commit)?;
        Ok(Response::Bound(flow_id))
    }

    fn lock_metaflow(&self, request: Lock_Data, now: i64) -> Result<Response, StoreError> {
        let Some(sender) = self.stored_metaflow(&request.sender)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                request.sender,
            ))));
        };
        match sender.metaflow.state {
            MetaflowState::Asleep => return Ok(Response::Refused(Refusal::Asleep)),
            MetaflowState::Awake(_) => {}
        }
        let address = match request.recipient {
            Recipient::Address(address) => address,
            Recipient::Up => {
                let layer = match request.sender.layer {
                    Layer::Primary => return Ok(Response::Refused(Refusal::NoneAbove)),
                    Layer::Secondary => Layer::Primary,
                    Layer::Tertiary => Layer::Secondary,
                    Layer::Quaternary => Layer::Tertiary,
                };
                Address {
                    aspect: request.sender.aspect.clone(),
                    topic: request.sender.topic.clone(),
                    layer,
                }
            }
        };
        if self.stored_metaflow(&address)?.is_none() {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                address,
            ))));
        }
        if let Some(held) = self.stored_metaflow_lock(&address)?
            && held.lock.until > now
        {
            return Ok(Response::Refused(Refusal::Held(held.lock)));
        }
        let Some(configuration) = self.configuration_snapshot()? else {
            return Ok(Response::Refused(Refusal::NotConfigured));
        };
        let Some(until) = now
            .checked_add(configuration.nexus.lease)
            .filter(|until| *until > now)
        else {
            return Ok(Response::Refused(Refusal::Store(
                "configured lease must be positive and fit the clock".into(),
            )));
        };
        let lock = Lock {
            sender: request.sender,
            address,
            until,
        };
        let row = StoredMetaflowLock { lock: lock.clone() };
        let commit = self.engine.begin_atomic_commit();
        let commit = if self.stored_metaflow_lock(&lock.address)?.is_some() {
            commit.mutate(self.metaflow_locks, row)
        } else {
            commit.assert(self.metaflow_locks, row)
        };
        self.engine.commit_atomic(commit)?;
        Ok(Response::Locked(lock))
    }

    fn queue_metaflow(&self, request: Deliver_Data, now: i64) -> Result<Response, StoreError> {
        let Some(held) = self.stored_metaflow_lock(&request.lock.address)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Lock(
                request.lock,
            ))));
        };
        if held.lock != request.lock {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Lock(
                request.lock,
            ))));
        }
        if held.lock.until <= now {
            return Ok(Response::Refused(Refusal::Lapsed));
        }
        let Some(_) = self.stored_metaflow(&request.lock.address)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                request.lock.address,
            ))));
        };
        if let Some(refusal) = request.message.oversized_refusal(&request.lock.sender) {
            let released = self.release_metaflow_lock(request.lock.clone(), now)?;
            if released != Response::Released {
                return Ok(released);
            }
            return Ok(Response::Refused(refusal));
        }
        self.accept_logical_delivery(
            &request.lock.address,
            SentBy::Sender(request.lock.sender.clone()),
            request.delivery,
            request.message,
            Some(VerifiedDeliveryLock {
                lock: request.lock.clone(),
                now,
            }),
        )
    }
}

impl SleepsMetaflows for FlowStore {
    fn sleep_metaflow(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(false);
        };
        if stored.metaflow.state != MetaflowState::Awake(flow_id.to_owned()) {
            return Ok(false);
        }
        stored.metaflow.state = MetaflowState::Asleep;
        stored.metaflow.past.push(flow_id.to_owned());
        self.write_metaflow(stored)?;
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
        let commit = taken.written(self, self.engine.begin_atomic_commit());
        self.engine.commit_atomic(commit)?;
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
        if stored.metaflow.queue.first() != Some(typed) {
            return Ok(false);
        }
        stored.metaflow.queue.remove(0);
        self.write_metaflow(stored)?;
        Ok(true)
    }

    fn metaflow_awake_as(&self, flow_id: &str) -> Result<Option<Metaflow>, StoreError> {
        let awake = MetaflowState::Awake(flow_id.to_owned());
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
                past: vec![],
                queue: vec![],
            },
        };
        match &metaflow.state {
            MetaflowState::Awake(current) if current == flow_id => {}
            MetaflowState::Awake(current) => {
                if Some(current.as_str()) != predecessor
                    && (live_binding || self.flow_is_live(current)?)
                {
                    return Ok(AddressTaking::Refused(StartRejection::AddressAwake));
                }
                metaflow.past.push(current.clone());
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
        metaflow.past.retain(|past| past != flow_id);
        metaflow.state = MetaflowState::Awake(flow_id.to_owned());
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
        let predecessor = profile.flow_id_option.as_deref();
        let refused = match self.stored_metaflow(&profile.address)? {
            Some(stored) => match &stored.metaflow.state {
                MetaflowState::Awake(current) => (Some(current.as_str()) != predecessor
                    && (self.flow_is_live(current)?
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
    fn written(
        self,
        store: &FlowStore,
        commit: memory_engine::AtomicCommit,
    ) -> memory_engine::AtomicCommit;
}

impl WritesTakenAddress for TakenAddress {
    fn written(
        self,
        store: &FlowStore,
        commit: memory_engine::AtomicCommit,
    ) -> memory_engine::AtomicCommit {
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
        if stored.metaflow.state != MetaflowState::Awake(flow_id.to_owned())
            || !stored.metaflow.queue.is_empty()
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
            .iter()
            .chain(std::iter::once(&flow_id.to_owned()))
        {
            if self.flow(id)?.is_some() {
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
        self.engine.commit_atomic(commit)?;
        Ok(true)
    }
}

/// One metaflow row written back in its own atomic commit.
/// Queues a request Flow itself raises for a metaflow it holds.
#[cfg(test)]
pub(crate) trait QueuesFlowRequest {
    /// Appends the request to the metaflow's queue; false when Flow holds
    /// no such metaflow.
    fn queue_flow_request(
        &self,
        address: &Address,
        request: flow_ethos::QueueEntry,
    ) -> Result<bool, StoreError>;
}

#[cfg(test)]
impl QueuesFlowRequest for FlowStore {
    fn queue_flow_request(
        &self,
        address: &Address,
        request: flow_ethos::QueueEntry,
    ) -> Result<bool, StoreError> {
        if self.stored_metaflow(address)?.is_none() {
            return Ok(false);
        }
        let response = self.accept_logical_delivery(
            address,
            request.sent_by,
            request.delivery,
            request.message,
            None,
        )?;
        Ok(matches!(response, Response::SendAccepted(_)))
    }
}

trait WritesMetaflow {
    fn write_metaflow(&self, stored: StoredMetaflow) -> Result<(), StoreError>;
}

impl WritesMetaflow for FlowStore {
    fn write_metaflow(&self, stored: StoredMetaflow) -> Result<(), StoreError> {
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        self.engine.commit_atomic(commit)?;
        Ok(())
    }
}

/// Seeds a metaflow row directly, for tests of what reads and moves it.
#[cfg(test)]
pub(crate) trait SeedsMetaflow {
    fn seed_metaflow(&self, metaflow: Metaflow);
}

#[cfg(test)]
impl SeedsMetaflow for FlowStore {
    fn seed_metaflow(&self, metaflow: Metaflow) {
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

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        _directory: tempfile::TempDir,
        store: FlowStore,
        peer: Process,
        message: Address,
    }
    impl Fixture {
        fn new(binary: String) -> Self {
            let directory = tempfile::tempdir().expect("private fixture");
            let path = directory.path().join("flow.memory");
            let defaults = DefaultConfiguration {
                home: directory.path().join("home"),
                runtime_directory: directory.path().join("runtime"),
            };
            let settings = defaults.configuration();
            let configuration =
                ConfigurationOnlyStore::open(&path).expect("new configuration store");
            configuration
                .configure_change(ConfigurationChange::Nexus(ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: binary,
                    lease: 30,
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
                    topic: "message".into(),
                    layer: Layer::Primary,
                },
            }
        }
    }

    #[test]
    fn bind_requires_actual_process_and_configured_message_executable() {
        let fixture = Fixture::new("/not/the/actual/executable".into());
        let binding = Bind_Data {
            address: fixture.message.clone(),
            process: fixture.peer.clone(),
        };
        assert_eq!(
            fixture
                .store
                .bind_metaflow(binding.clone(), &fixture.peer, None)
                .unwrap(),
            Response::Refused(Refusal::NotMessage)
        );
        let mut forged = binding;
        forged.process.started += 1;
        assert!(matches!(
            fixture
                .store
                .bind_metaflow(forged, &fixture.peer, None)
                .unwrap(),
            Response::Refused(Refusal::Unidentified(_))
        ));
        assert!(fixture.store.list_metaflows().unwrap().is_empty());
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
        let first = fixture
            .store
            .bind_metaflow(binding.clone(), &fixture.peer, None)
            .unwrap();
        assert!(matches!(&first, Response::Bound(_)));
        assert_eq!(
            fixture
                .store
                .bind_metaflow(binding, &fixture.peer, None)
                .unwrap(),
            first
        );
        assert!(fixture.store.message_peer(&fixture.peer).unwrap());
        let request = Lock_Data {
            sender: fixture.message.clone(),
            recipient: Recipient::Address(fixture.message.clone()),
        };
        let Response::Locked(lock) = fixture.store.lock_metaflow(request.clone(), 100).unwrap()
        else {
            panic!("lease expected")
        };
        assert_eq!(lock.until, 130);
        assert_eq!(
            fixture.store.lock_metaflow(request, 100).unwrap(),
            Response::Refused(Refusal::Held(lock.clone()))
        );
        let body = Request::Psyche(flow_ethos::Said {
            context: "fixture context".into(),
            verbatim: "Exact words.\nSecond line.".into(),
        });
        let mut forged = lock.clone();
        forged.until += 1;
        assert!(matches!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock: forged,
                        delivery: flow_ethos::Delivery::Queue,
                        message: flow_ethos::Message::Soft(body.clone())
                    },
                    101
                )
                .unwrap(),
            Response::Refused(Refusal::Unknown(_))
        ));
        assert!(fixture.store.list_metaflows().unwrap()[0].queue.is_empty());
        assert!(matches!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock: lock.clone(),
                        delivery: flow_ethos::Delivery::Queue,
                        message: flow_ethos::Message::Soft(body.clone())
                    },
                    101
                )
                .unwrap(),
            Response::SendAccepted(_)
        ));
        // Queued with the Sender its Lock named.
        assert_eq!(
            fixture.store.list_metaflows().unwrap()[0].queue[0].sent_by,
            SentBy::Sender(lock.sender.clone())
        );
        assert_eq!(
            fixture.store.list_metaflows().unwrap()[0].queue[0].message,
            flow_ethos::Message::Soft(body)
        );
        assert!(
            fixture
                .store
                .stored_metaflow_lock(&lock.address)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock,
                        delivery: flow_ethos::Delivery::Queue,
                        message: flow_ethos::Message::Soft(Request::Notice("duplicate".into()))
                    },
                    102
                )
                .unwrap(),
            Response::Refused(Refusal::Unknown(_))
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
            sender: fixture.message.clone(),
            recipient: Recipient::Address(fixture.message.clone()),
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
                &fixture.peer,
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
                        delivery: flow_ethos::Delivery::Queue,
                        message: flow_ethos::Message::Soft(Request::Notice("expired".into()))
                    },
                    40
                )
                .unwrap(),
            Response::Refused(Refusal::Lapsed)
        );
        assert!(fixture.store.list_metaflows().unwrap()[0].queue.is_empty());
    }
}
