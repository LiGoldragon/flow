//! Configured sampling heartbeat.  Its evidence deliberately excludes the
//! deliveries it generates, so an otherwise idle system becomes quiet.

use crate::live::HoldsLiveValue;
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
            if endpoint.endpoint_path.0 != self.codex_endpoints.current().next.socket {
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
                .or_insert_with(|| self.herdr.session_snapshot(&session.0));
            let Some(agents) = snapshot
                .as_ref()
                .and_then(|value| value.pointer("/result/snapshot/agents"))
                .and_then(serde_json::Value::as_array)
            else {
                coverage_complete = false;
                continue;
            };
            let Some(agent) = agents.iter().find(|agent| {
                agent.get("pane_id").and_then(serde_json::Value::as_str) == Some(pane.0.as_str())
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
                    .0
                    .iter()
                    .filter(|entry| {
                        !purposes.iter().any(|row| {
                            row.logical_delivery_id == entry.logical_delivery_id.0
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
        use crate::store::{
            BaselineAwake, BaselineQueue, BaselineSent, BaselineWorking, BaselineWorkingComplete,
            LatestAwake, LatestQueue, LatestSent, LatestWorking, LatestWorkingComplete,
            StoredHeartbeatStanding, TracksHeartbeat,
        };
        let Ok(evidence) = self.heartbeat_evidence(recipients) else {
            return;
        };
        let Ok(held) = self.store.heartbeat_standings() else {
            return;
        };
        let mut standings = held;
        for (address, own) in &evidence.recipients {
            if !standings.iter().any(|row| row.address == *address) {
                let working = match evidence.shared.working_panes {
                    Observed::Known(count) | Observed::Partial(count) => Some(count as i64),
                    Observed::Unknown => None,
                };
                let complete = matches!(evidence.shared.working_panes, Observed::Known(_));
                standings.push(StoredHeartbeatStanding {
                    address: address.clone(),
                    baseline_awake: BaselineAwake(evidence.shared.awake_metaflows as i64),
                    baseline_working: BaselineWorking(working),
                    baseline_working_complete: BaselineWorkingComplete(complete),
                    baseline_sent: BaselineSent(own.ordinary_messages_sent as i64),
                    baseline_queue: BaselineQueue(own.ordinary_queue_length as i64),
                    latest_awake: LatestAwake(evidence.shared.awake_metaflows as i64),
                    latest_working: LatestWorking(working),
                    latest_working_complete: LatestWorkingComplete(complete),
                    latest_sent: LatestSent(own.ordinary_messages_sent as i64),
                    latest_queue: LatestQueue(own.ordinary_queue_length as i64),
                    logical_delivery_id_option: None,
                });
            }
        }
        let _ = self.store.replace_heartbeat_standings(standings);
    }

    fn heartbeat_tick(&self, recipients: &[Address]) {
        use crate::store::{
            JudgesHeartbeatStanding, LatestAwake, LatestQueue, LatestSent, LatestWorking,
            LatestWorkingComplete, TracksHeartbeat,
        };
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
            standing.latest_awake = LatestAwake(evidence.shared.awake_metaflows as i64);
            standing.latest_working = LatestWorking(match evidence.shared.working_panes {
                Observed::Known(count) | Observed::Partial(count) => Some(count as i64),
                Observed::Unknown => None,
            });
            standing.latest_working_complete =
                LatestWorkingComplete(matches!(evidence.shared.working_panes, Observed::Known(_)));
            standing.latest_sent = LatestSent(own.ordinary_messages_sent as i64);
            standing.latest_queue = LatestQueue(own.ordinary_queue_length as i64);
            if let Some(outstanding) = standing.logical_delivery_id_option.as_ref()
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
                standing.logical_delivery_id_option = None;
            }
            if standing.changed() && standing.logical_delivery_id_option.is_none() {
                deliveries.push(HeartbeatDelivery {
                    recipient: address.clone(),
                    body: HeartbeatDelivery::reminder(
                        address,
                        &RecipientEvidence {
                            ordinary_messages_sent: standing.baseline_sent.0 as u64,
                            ordinary_queue_length: standing.baseline_queue.0 as usize,
                        },
                        own,
                        &SharedEvidence {
                            awake_metaflows: standing.baseline_awake.0 as usize,
                            working_panes: standing
                                .baseline_working
                                .0
                                .map(|value| {
                                    if standing.baseline_working_complete.0 {
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
        use crate::store::{AnnouncesLaunchChanges, ManagesMetaflowState};
        // Two events move the heartbeat: a Configure (or the stop, which
        // announces the same change) and the configured period ending. The
        // thread blocks on the configuration change, bounded by the next
        // beat; nothing wakes to look again.
        let mut active: Option<meta_signal_flow::HeartbeatConfig> = None;
        let mut next_beat: Option<std::time::Instant> = None;
        while !crate::HoldsStopRequest::requested(&self.stopping) {
            let seen = self.configuration_changes.current();
            let configured = self
                .store
                .configuration_snapshot()
                .ok()
                .flatten()
                .and_then(|snapshot| snapshot.configured_heartbeat.0);
            if configured != active {
                active = configured;
                next_beat = active
                    .as_ref()
                    .map(|config| std::time::Instant::now() + config.period());
                if let Some(config) = active.as_ref() {
                    self.sync_heartbeat(&config.heartbeat_recipients.0);
                }
            }
            match (active.as_ref(), next_beat) {
                (Some(config), Some(beat)) => {
                    if !self.configuration_changes.until(seen, beat) {
                        next_beat = Some(beat + config.period());
                        self.heartbeat_tick(&config.heartbeat_recipients.0);
                    }
                }
                _ => {
                    self.configuration_changes.after(seen);
                }
            }
        }
    }
}

/// The configured period between two beats; Configure refuses one that is
/// not positive.
trait HasHeartbeatPeriod {
    fn period(&self) -> std::time::Duration;
}

impl HasHeartbeatPeriod for meta_signal_flow::HeartbeatConfig {
    fn period(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.heartbeat_period_seconds.0.max(1) as u64)
    }
}
