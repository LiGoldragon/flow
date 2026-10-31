//! Configured sampling heartbeat.  Its evidence deliberately excludes the
//! deliveries it generates, so an otherwise idle system becomes quiet.

use crate::store::logical_delivery::RecordsLogicalDeliveries;
use signal_flow::Address;
use std::collections::{BTreeMap, BTreeSet};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryPurpose {
    Ordinary,
    Heartbeat,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observed<T> {
    Known(T),
    Partial(T),
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedEvidence {
    pub awake_metaflows: usize,
    pub working_panes: Observed<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipientEvidence {
    pub ordinary_messages_sent: u64,
    pub ordinary_queue_length: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Evidence {
    pub shared: SharedEvidence,
    pub recipients: Vec<(Address, RecipientEvidence)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatDelivery {
    pub recipient: Address,
    pub body: String,
}

pub trait RendersHeartbeat {
    fn reminder(
        recipient: &Address,
        baseline: &RecipientEvidence,
        latest: &RecipientEvidence,
        shared_baseline: &SharedEvidence,
        shared_latest: &SharedEvidence,
    ) -> String;
}

impl RendersHeartbeat for HeartbeatDelivery {
    fn reminder(
        _recipient: &Address,
        baseline: &RecipientEvidence,
        latest: &RecipientEvidence,
        shared_baseline: &SharedEvidence,
        shared_latest: &SharedEvidence,
    ) -> String {
        let queue_growth =
            latest.ordinary_queue_length as i128 - baseline.ordinary_queue_length as i128;
        format!(
            "Heartbeat: since the prior admitted sample, awake metaflows {}→{}, working panes {:?}→{:?}, your messages sent (accepted, since purpose-family deployment/start) {}→{}, and your ordinary queue length {}→{} (growth {:+}). Judge your own sanity: look for an echo chamber, increasing jargon or nonsense, and ACK answering ACK.",
            shared_baseline.awake_metaflows,
            shared_latest.awake_metaflows,
            shared_baseline.working_panes,
            shared_latest.working_panes,
            baseline.ordinary_messages_sent,
            latest.ordinary_messages_sent,
            baseline.ordinary_queue_length,
            latest.ordinary_queue_length,
            queue_growth,
        )
    }
}

pub trait RunsHeartbeat {
    fn heartbeat_evidence(&self, recipients: &[Address]) -> Result<Evidence, crate::StoreError>;
    fn sync_heartbeat(&self, recipients: &[Address]);
    fn heartbeat_tick(&self, recipients: &[Address]);
    fn keep_heartbeating(&self);
}

impl RunsHeartbeat for crate::RunningNexus {
    fn heartbeat_evidence(&self, recipients: &[Address]) -> Result<Evidence, crate::StoreError> {
        use crate::herdr::ReadsHerdrPanes;
        use crate::store::{ManagesMetaflowState, ReadsFlowRows};
        let metaflows = self.store.list_metaflows()?;
        let awake_metaflows = metaflows
            .iter()
            .filter(|metaflow| matches!(metaflow.state, flow_ethos::State::Awake(_)))
            .count();
        let purposes = self.store.delivery_purposes()?;
        let mut panes = BTreeSet::new();
        let mut coverage_complete = true;
        for node in self.store.flow_nodes()? {
            let signal_flow::EndpointSelection::Available(endpoint) = &node.endpoint_selection
            else {
                continue;
            };
            if endpoint.endpoint_path != self.codex_endpoints.next.socket {
                continue;
            }
            match &node.herdr_route_selection {
                signal_flow::HerdrRouteSelection::Available(route) => {
                    panes.insert((
                        route.herdr_session_name.clone(),
                        route.herdr_pane_id.clone(),
                    ));
                }
                signal_flow::HerdrRouteSelection::Unavailable => coverage_complete = false,
            }
        }
        let mut snapshots = BTreeMap::new();
        let mut working = 0usize;
        for (session, pane) in panes {
            let snapshot = snapshots
                .entry(session.clone())
                .or_insert_with(|| self.herdr.session_snapshot(&session));
            let Some(agents) = snapshot
                .as_ref()
                .and_then(|value| value.pointer("/result/snapshot/agents"))
                .and_then(serde_json::Value::as_array)
            else {
                coverage_complete = false;
                continue;
            };
            let Some(agent) = agents.iter().find(|agent| {
                agent.get("pane_id").and_then(serde_json::Value::as_str) == Some(pane.as_str())
            }) else {
                coverage_complete = false;
                continue;
            };
            match agent
                .get("agent_status")
                .and_then(serde_json::Value::as_str)
            {
                Some("working") => working += 1,
                Some("idle" | "done") => {}
                _ => coverage_complete = false,
            }
        }
        let recipients = recipients
            .iter()
            .filter_map(|address| {
                let metaflow = metaflows
                    .iter()
                    .find(|metaflow| metaflow.address == *address)?;
                if !matches!(metaflow.state, flow_ethos::State::Awake(_)) {
                    return None;
                }
                let ordinary_queue_length = metaflow
                    .queue
                    .iter()
                    .filter(|entry| {
                        !purposes.iter().any(|row| {
                            row.logical_delivery_id == entry.logical_delivery_id
                                && row.purpose == DeliveryPurpose::Heartbeat
                        })
                    })
                    .count();
                Some((
                    address.clone(),
                    RecipientEvidence {
                        ordinary_messages_sent: purposes
                            .iter()
                            .filter(|row| {
                                row.address == *address && row.purpose == DeliveryPurpose::Ordinary
                            })
                            .count() as u64,
                        ordinary_queue_length,
                    },
                ))
            })
            .collect();
        Ok(Evidence {
            shared: SharedEvidence {
                awake_metaflows,
                working_panes: if coverage_complete {
                    Observed::Known(working)
                } else {
                    Observed::Partial(working)
                },
            },
            recipients,
        })
    }

    fn sync_heartbeat(&self, recipients: &[Address]) {
        use crate::store::{StoredHeartbeatStanding, TracksHeartbeat};
        let Ok(evidence) = self.heartbeat_evidence(recipients) else {
            return;
        };
        let Ok(held) = self.store.heartbeat_standings() else {
            return;
        };
        let standings = evidence
            .recipients
            .iter()
            .map(|(address, own)| {
                held.iter()
                    .find(|row| row.address == *address)
                    .cloned()
                    .unwrap_or(StoredHeartbeatStanding {
                        address: address.clone(),
                        baseline_awake: evidence.shared.awake_metaflows as i64,
                        baseline_working: match evidence.shared.working_panes {
                            Observed::Known(count) | Observed::Partial(count) => Some(count as i64),
                            Observed::Unknown => None,
                        },
                        baseline_working_complete: matches!(
                            evidence.shared.working_panes,
                            Observed::Known(_)
                        ),
                        baseline_sent: own.ordinary_messages_sent as i64,
                        baseline_queue: own.ordinary_queue_length as i64,
                        latest_awake: evidence.shared.awake_metaflows as i64,
                        latest_working: match evidence.shared.working_panes {
                            Observed::Known(count) | Observed::Partial(count) => Some(count as i64),
                            Observed::Unknown => None,
                        },
                        latest_working_complete: matches!(
                            evidence.shared.working_panes,
                            Observed::Known(_)
                        ),
                        latest_sent: own.ordinary_messages_sent as i64,
                        latest_queue: own.ordinary_queue_length as i64,
                        outstanding: None,
                    })
            })
            .collect();
        let _ = self.store.replace_heartbeat_standings(standings);
    }

    fn heartbeat_tick(&self, recipients: &[Address]) {
        use crate::store::{JudgesHeartbeatStanding, TracksHeartbeat};
        use crate::waking::SleepsAndWakesSeats;
        let Ok(evidence) = self.heartbeat_evidence(recipients) else {
            return;
        };
        let Ok(mut standings) = self.store.heartbeat_standings() else {
            return;
        };
        let mut deliveries = Vec::new();
        for (address, own) in &evidence.recipients {
            let Some(standing) = standings.iter_mut().find(|row| row.address == *address) else {
                continue;
            };
            standing.latest_awake = evidence.shared.awake_metaflows as i64;
            standing.latest_working = match evidence.shared.working_panes {
                Observed::Known(count) | Observed::Partial(count) => Some(count as i64),
                Observed::Unknown => None,
            };
            standing.latest_working_complete =
                matches!(evidence.shared.working_panes, Observed::Known(_));
            standing.latest_sent = own.ordinary_messages_sent as i64;
            standing.latest_queue = own.ordinary_queue_length as i64;
            if let Some(outstanding) = standing.outstanding.as_ref()
                && self
                    .store
                    .logical_delivery_snapshot(outstanding)
                    .ok()
                    .flatten()
                    .is_some_and(|snapshot| {
                        matches!(
                            snapshot.delivery_state,
                            signal_flow::DeliveryState::Delivered(_)
                                | signal_flow::DeliveryState::Failed(_)
                                | signal_flow::DeliveryState::Withdrawn
                        )
                    })
            {
                standing.outstanding = None;
            }
            if standing.changed() && standing.outstanding.is_none() {
                deliveries.push(HeartbeatDelivery {
                    recipient: address.clone(),
                    body: HeartbeatDelivery::reminder(
                        address,
                        &RecipientEvidence {
                            ordinary_messages_sent: standing.baseline_sent as u64,
                            ordinary_queue_length: standing.baseline_queue as usize,
                        },
                        own,
                        &SharedEvidence {
                            awake_metaflows: standing.baseline_awake as usize,
                            working_panes: standing
                                .baseline_working
                                .map(|value| {
                                    if standing.baseline_working_complete {
                                        Observed::Known(value as usize)
                                    } else {
                                        Observed::Partial(value as usize)
                                    }
                                })
                                .unwrap_or(Observed::Unknown),
                        },
                        &evidence.shared,
                    ),
                });
            }
        }
        if self.store.replace_heartbeat_standings(standings).is_err() {
            return;
        }
        for delivery in deliveries {
            let accepted = self.store.accept_logical_delivery_for(
                &delivery.recipient,
                signal_flow::SentBy::Flow,
                flow_ethos::Delivery::Wake,
                flow_ethos::Message::Soft(signal_flow::Request::Notice(delivery.body)),
                None,
                DeliveryPurpose::Heartbeat,
            );
            if let Ok(signal_flow::PrivateSendResponse::SendAccepted(_)) = accepted {
                self.wake_due(delivery.recipient);
            }
        }
    }

    fn keep_heartbeating(&self) {
        use crate::store::ManagesMetaflowState;
        let mut active: Option<meta_signal_flow::HeartbeatConfig> = None;
        let mut waited = std::time::Duration::ZERO;
        while !crate::HoldsStopRequest::requested(&self.stopping) {
            let configured = self
                .store
                .configuration_snapshot()
                .ok()
                .flatten()
                .and_then(|snapshot| snapshot.heartbeat);
            if configured != active {
                active = configured;
                waited = std::time::Duration::ZERO;
                if let Some(config) = active.as_ref() {
                    self.sync_heartbeat(&config.heartbeat_recipients);
                }
            }
            let step = std::time::Duration::from_millis(250);
            std::thread::sleep(step);
            let Some(config) = active.as_ref() else {
                continue;
            };
            waited += step;
            let period = std::time::Duration::from_secs(config.heartbeat_period_seconds as u64);
            if waited >= period {
                waited = std::time::Duration::ZERO;
                self.heartbeat_tick(&config.heartbeat_recipients);
            }
        }
    }
}
