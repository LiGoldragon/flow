//! Ordinary metaflow mutations. The connection supplies the witnessed peer;
//! the serial dispatcher holds exclusion across authorization and mutation.
use super::*;
use signal_flow::{Bind_Data, Deliver_Data, Lock_Data, Recipient, Refusal, Unknown_Data};

pub(crate) trait MutatesMetaflows {
    fn message_peer(&self, peer: &Process) -> Result<bool, StoreError>;
    fn bind_metaflow(&self, binding: Bind_Data, peer: &Process) -> Result<Response, StoreError>;
    fn lock_metaflow(&self, request: Lock_Data, now: i64) -> Result<Response, StoreError>;
    fn queue_metaflow(&self, request: Deliver_Data, now: i64) -> Result<Response, StoreError>;
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

    fn bind_metaflow(&self, binding: Bind_Data, peer: &Process) -> Result<Response, StoreError> {
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
        if let Some(stored) = existing.as_ref() {
            if stored.metaflow.state == MetaflowState::Ended {
                return Ok(Response::Refused(Refusal::Ended(address)));
            }
            if let Some(previous) = self.bound_process(&address)? {
                if previous == *peer {
                    if let MetaflowState::Awake(id) = &stored.metaflow.state {
                        return Ok(Response::Bound(id.clone()));
                    }
                }
                if FlowStore::process_is_live(&previous) {
                    return Ok(Response::Refused(Refusal::Taken(address)));
                }
            }
        }
        let state = self.state()?;
        let Some(next) = state.next_flow_number.checked_add(1) else {
            return Err(StoreError::StateInvariant);
        };
        let flow_id = format!("flow-{:016x}", state.next_flow_number);
        let new_metaflow = existing.is_none();
        let new_binding = self.bound_process(&address)?.is_none();
        let mut metaflow = existing.map(|row| row.metaflow).unwrap_or(Metaflow {
            address: address.clone(),
            state: MetaflowState::Asleep,
            past: vec![],
            queue: vec![],
        });
        if let MetaflowState::Awake(previous) = metaflow.state {
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
            MetaflowState::Ended => return Ok(Response::Refused(Refusal::Ended(request.sender))),
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
        let Some(recipient) = self.stored_metaflow(&address)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                address,
            ))));
        };
        if recipient.metaflow.state == MetaflowState::Ended {
            return Ok(Response::Refused(Refusal::Ended(address)));
        }
        if let Some(held) = self.stored_metaflow_lock(&address)? {
            if held.lock.until > now {
                return Ok(Response::Refused(Refusal::Held(held.lock)));
            }
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
        let Some(mut stored) = self.stored_metaflow(&request.lock.address)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                request.lock.address,
            ))));
        };
        if stored.metaflow.state == MetaflowState::Ended {
            return Ok(Response::Refused(Refusal::Ended(request.lock.address)));
        }
        stored.metaflow.queue.push(request.request);
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored)
            .retract(
                self.metaflow_locks,
                RecordKey::new(FlowStore::address_record_key(&request.lock.address)),
            );
        self.engine.commit_atomic(commit)?;
        // This is durable enqueue, never a claim of native delivery or wake.
        Ok(Response::Queued)
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
                .bind_metaflow(binding.clone(), &fixture.peer)
                .unwrap(),
            Response::Refused(Refusal::NotMessage)
        );
        let mut forged = binding;
        forged.process.started += 1;
        assert!(matches!(
            fixture.store.bind_metaflow(forged, &fixture.peer).unwrap(),
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
            .bind_metaflow(binding.clone(), &fixture.peer)
            .unwrap();
        assert!(matches!(&first, Response::Bound(_)));
        assert_eq!(
            fixture.store.bind_metaflow(binding, &fixture.peer).unwrap(),
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
                        request: body.clone()
                    },
                    101
                )
                .unwrap(),
            Response::Refused(Refusal::Unknown(_))
        ));
        assert!(fixture.store.list_metaflows().unwrap()[0].queue.is_empty());
        assert_eq!(
            fixture
                .store
                .queue_metaflow(
                    Deliver_Data {
                        lock: lock.clone(),
                        request: body.clone()
                    },
                    101
                )
                .unwrap(),
            Response::Queued
        );
        assert_eq!(fixture.store.list_metaflows().unwrap()[0].queue, vec![body]);
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
                        request: Request::Notice("duplicate".into())
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
                        request: Request::Notice("expired".into())
                    },
                    40
                )
                .unwrap(),
            Response::Refused(Refusal::Lapsed)
        );
        assert!(fixture.store.list_metaflows().unwrap()[0].queue.is_empty());
    }
}
