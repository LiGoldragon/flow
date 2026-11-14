//! The step this release takes on open, after the recovery step: the
//! ordered-replacement family from v1 to v2 and the successor-intent family
//! from v4 to v5. Each family's step reads the format of the one before it
//! and only that one, so a store of any earlier release reaches the current
//! formats through the ordered chain of steps.
//!
//! A Refresh's caller is a fact of Memory: v2 holds it on the queued
//! Refresh (`ReplacementOrder::Refresh` carries it) and v5 on the succession
//! the Refresh causes (`SuccessionCause::Ordered` carries it), recorded bare
//! and never defaulted. No caller can be invented for a Refresh an earlier
//! release recorded without one, so the step refuses while any such row is
//! open: a queued Refresh in v1, or a succession ordered by a Refresh in
//! v4. The refusal names every such row it meets, before anything is
//! written: its Address, its predecessor, and the launch request its
//! succession owes. With none open, every other row carries over unchanged
//! (a Launch or Restart order, a threshold, exit, replace or emergency
//! cause).
//!
//! Each family's evolution is its own write and a no-op once current, so an
//! open interrupted between them resumes where it stopped. An older daemon
//! declares v1 or v4 alone and is refused at open by the new identity;
//! rollback is the restore of the store copy taken before the deploy.

use super::ordered_replacement::{
    DescribesOrderedReplacementTable, OrderedLaunch, OrderedReplacement, ReplacementOrder,
};
use super::successor::{DescribesSuccessorIntentTable, SuccessionCause, SuccessorIntent};
use super::successor_upgrade::{SuccessionCauseV4, SuccessorIntentV4};
use super::{FlowStore, ReadsFlowMemory as _, StoreError};
use crate::generated::memory::Predecessor;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, MemoryDefinition, MemoryUpgrades, MemoryVersion, RecordKey,
    SchemaHash, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{Address, LaunchRequestId};

/// The ordered-replacement family's schema label until this step.
pub(crate) const ORDERED_REPLACEMENT_SCHEMA_V1: &str = "flow-nexus-ordered-replacement-v1";

/// `ReplacementOrder` as v1 stored it: a Refresh names no caller.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplacementOrderV1 {
    Refresh,
    Launch(Box<OrderedLaunch>),
    Restart,
}

/// One replacement ordered at an Address, as v1 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderedReplacementV1 {
    pub(crate) address: Address,
    pub(crate) predecessor: Predecessor,
    pub(crate) replacement_order: ReplacementOrderV1,
}

impl EngineRecord for OrderedReplacementV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for OrderedReplacementV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(ORDERED_REPLACEMENT_SCHEMA_V1)
    }
}

/// A Refresh an earlier release recorded without its caller, still open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenRefreshWithoutCaller {
    /// A Refresh of a working flow, queued for its rest. A queued Refresh
    /// has no request id of its own: it is keyed by its Address and names
    /// the flow it waits for.
    QueuedRefresh {
        address: Address,
        predecessor: Predecessor,
    },
    /// The succession a Refresh ordered, owed until its successor takes the
    /// Address: its request is the launch request it owes.
    OrderedSuccession {
        address: Address,
        predecessor: Predecessor,
        launch_request_id: LaunchRequestId,
    },
}

impl std::fmt::Display for OpenRefreshWithoutCaller {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueuedRefresh {
                address,
                predecessor,
            } => write!(
                formatter,
                "queued Refresh at {} of flow {}",
                FlowStore::address_record_key(address),
                predecessor.0
            ),
            Self::OrderedSuccession {
                address,
                predecessor,
                launch_request_id,
            } => write!(
                formatter,
                "ordered succession at {} of flow {}, launch request {}",
                FlowStore::address_record_key(address),
                predecessor.0,
                launch_request_id.0
            ),
        }
    }
}

/// A v1 order is its own order in the current layout, unless it is a
/// Refresh, whose caller nothing recorded.
impl UpgradeFrom<OrderedReplacementV1> for OrderedReplacement {
    type Refusal = OpenRefreshWithoutCaller;

    fn upgrade_from(before: OrderedReplacementV1) -> Decision<Self, Self::Refusal> {
        let replacement_order = match before.replacement_order {
            ReplacementOrderV1::Refresh => {
                return Decision::Refused(OpenRefreshWithoutCaller::QueuedRefresh {
                    address: before.address,
                    predecessor: before.predecessor,
                });
            }
            ReplacementOrderV1::Launch(launch) => ReplacementOrder::Launch(launch),
            ReplacementOrderV1::Restart => ReplacementOrder::Restart,
        };
        Decision::Ready(Self {
            address: before.address,
            predecessor: before.predecessor,
            replacement_order,
        })
    }
}

/// A v4 intent is its own intent in the current layout, unless its cause is
/// a Refresh ordered from outside, whose caller nothing recorded.
impl UpgradeFrom<SuccessorIntentV4> for SuccessorIntent {
    type Refusal = OpenRefreshWithoutCaller;

    fn upgrade_from(before: SuccessorIntentV4) -> Decision<Self, Self::Refusal> {
        let succession_cause = match before.succession_cause {
            SuccessionCauseV4::Ordered => {
                return Decision::Refused(OpenRefreshWithoutCaller::OrderedSuccession {
                    address: before.address,
                    predecessor: before.predecessor,
                    launch_request_id: before.launch_request_id,
                });
            }
            SuccessionCauseV4::Refresh => SuccessionCause::Refresh,
            SuccessionCauseV4::Exit => SuccessionCause::Exit,
            SuccessionCauseV4::Replace => SuccessionCause::Replace,
            SuccessionCauseV4::EmergencyRefreshWithoutHandover(provenance) => {
                SuccessionCause::EmergencyRefreshWithoutHandover(provenance)
            }
        };
        Decision::Ready(Self {
            address: before.address,
            predecessor: before.predecessor,
            succession_cause,
            launch_request_id: before.launch_request_id,
        })
    }
}

/// The startup step, run after the recovery step and before any family is
/// registered.
pub(crate) trait UpgradesRefreshCallers {
    /// Upgrades a v1 ordered-replacement family and a v4 successor-intent
    /// family, or refuses naming every open Refresh either holds, writing
    /// nothing; a family already current, or absent, is left as it is.
    fn upgrade_refresh_callers(engine: &mut Engine) -> Result<(), StoreError>;
}

impl UpgradesRefreshCallers for FlowStore {
    fn upgrade_refresh_callers(engine: &mut Engine) -> Result<(), StoreError> {
        let stored = |engine: &Engine, table: &str, schema_hash: SchemaHash| {
            engine.list_tables().into_iter().any(|registration| {
                registration.identity().table_name() == table
                    && registration.identity().schema_hash() == schema_hash
            })
        };
        let orders_at_v1 = stored(
            engine,
            super::ordered_replacement::ORDERED_REPLACEMENT_TABLE_NAME.as_str(),
            OrderedReplacementV1::schema_hash(),
        );
        let intents_at_v4 = stored(
            engine,
            super::successor::SUCCESSOR_INTENT_TABLE_NAME.as_str(),
            SuccessorIntentV4::schema_hash(),
        );
        // Every open Refresh is read before anything is written, so a
        // refusal names them all and leaves both families as they stand.
        let mut open = Vec::new();
        if orders_at_v1 {
            let inventory = engine
                .inspect_predecessor::<OrderedReplacementV1, OrderedReplacement>(
                    &OrderedReplacement::descriptor(),
                )?;
            for (_, row) in inventory.rows() {
                if let Decision::Refused(refusal) = OrderedReplacement::upgrade_from(row.clone()) {
                    open.push(refusal);
                }
            }
        }
        if intents_at_v4 {
            let inventory = engine.inspect_predecessor::<SuccessorIntentV4, SuccessorIntent>(
                &SuccessorIntent::descriptor(),
            )?;
            for (_, row) in inventory.rows() {
                if let Decision::Refused(refusal) = SuccessorIntent::upgrade_from(row.clone()) {
                    open.push(refusal);
                }
            }
        }
        if !open.is_empty() {
            return Err(StoreError::RefreshCallerUpgrade(open));
        }
        if orders_at_v1 {
            match engine.upgrade_table::<OrderedReplacementV1, OrderedReplacement>(
                OrderedReplacement::descriptor(),
            ) {
                UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => {}
                UpgradeOutcome::Refused(refusal) => {
                    return Err(StoreError::RefreshCallerUpgrade(vec![
                        refusal.reason().clone(),
                    ]));
                }
                UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::RefreshCallerUpgradeFailed(format!(
                        "{failure:?}"
                    )));
                }
            }
        }
        if intents_at_v4 {
            match engine
                .upgrade_table::<SuccessorIntentV4, SuccessorIntent>(SuccessorIntent::descriptor())
            {
                UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => {}
                UpgradeOutcome::Refused(refusal) => {
                    return Err(StoreError::RefreshCallerUpgrade(vec![
                        refusal.reason().clone(),
                    ]));
                }
                UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::RefreshCallerUpgradeFailed(format!(
                        "{failure:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::memory::{
        EmergencyRefreshWithoutHandover_Data, OrderedLaunch, RefreshCaller,
    };
    use crate::store::ordered_replacement::NamesOrderedLaunch as _;
    use crate::store::successor_upgrade::{
        DescribesSuccessorIntentV4Table as _, SuccessorIntentV3,
    };
    use sema_engine::{
        Assertion, EngineOpen, FamilyName, QueryPlan, SchemaVersion, TableDescriptor,
    };

    fn address(topic: &str) -> Address {
        Address {
            aspect: signal_flow::Aspect::Mind,
            topic: flow_ethos::Topic(topic.into()),
            model: signal_flow::Model::Sol,
        }
    }

    fn order_v1(topic: &str, flow: &str, order: ReplacementOrderV1) -> OrderedReplacementV1 {
        OrderedReplacementV1 {
            address: address(topic),
            predecessor: Predecessor(flow.into()),
            replacement_order: order,
        }
    }

    fn intent_v4(
        topic: &str,
        flow: &str,
        request: &str,
        cause: SuccessionCauseV4,
    ) -> SuccessorIntentV4 {
        SuccessorIntentV4 {
            address: address(topic),
            predecessor: Predecessor(flow.into()),
            succession_cause: cause,
            launch_request_id: LaunchRequestId(request.into()),
        }
    }

    fn orders_v1_descriptor() -> TableDescriptor<OrderedReplacementV1> {
        TableDescriptor::new(
            super::super::ordered_replacement::ORDERED_REPLACEMENT_TABLE_NAME,
            FamilyName::new(super::super::ordered_replacement::ORDERED_REPLACEMENT_FAMILY),
            OrderedReplacementV1::schema_hash(),
        )
    }

    /// A store that holds both families as the release before this step
    /// wrote them.
    fn store_before(
        path: &std::path::Path,
        orders: &[OrderedReplacementV1],
        intents: &[SuccessorIntentV4],
    ) {
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1))).unwrap();
        let order_table = engine.register_table(orders_v1_descriptor()).unwrap();
        for order in orders {
            engine
                .assert(Assertion::new(order_table, order.clone()))
                .unwrap();
        }
        // v3 first: the family is born v3 and upgraded to v4 as the
        // recovery step writes it.
        let v3 = TableDescriptor::<SuccessorIntentV3>::new(
            super::super::successor::SUCCESSOR_INTENT_TABLE_NAME,
            FamilyName::new(super::super::successor::SUCCESSOR_INTENT_FAMILY),
            SuccessorIntentV3::schema_hash(),
        );
        engine.register_table(v3).unwrap();
        <SuccessorIntentV4 as crate::store::successor_upgrade::UpgradesSuccessorIntent>::upgrade_or_register(&mut engine)
            .unwrap();
        let intent_table = engine
            .register_table(SuccessorIntentV4::descriptor_v4())
            .unwrap();
        for intent in intents {
            engine
                .assert(Assertion::new(intent_table, intent.clone()))
                .unwrap();
        }
    }

    fn open_before_step(path: &std::path::Path) -> Engine {
        Engine::open(
            EngineOpen::new(path, SchemaVersion::new(1))
                .with_family(&SuccessorIntent::descriptor())
                .with_family(&OrderedReplacement::descriptor()),
        )
        .unwrap()
    }

    /// The step refuses while a Refresh is open, naming each one it meets:
    /// the queued Refreshes by Address and the flow they wait for, the
    /// successions an ordered Refresh owes by Address, flow and launch
    /// request. Every other row is no refusal. Nothing is written: both
    /// families still stand in their old format, row for row, and a second
    /// open refuses the same.
    #[test]
    fn an_open_refresh_without_a_caller_refuses_the_upgrade_naming_each_one() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("refresh-callers-before.sema");
        let launch = ReplacementOrderV1::Launch(Box::new(OrderedLaunch {
            launch_profile: fixture_profile(),
            origin: signal_flow::Origin::Unbound,
            prompt_sha256: crate::generated::memory::PromptSha256("a".repeat(64)),
        }));
        store_before(
            &path,
            &[
                order_v1("queuedFirst", "flow-q1", ReplacementOrderV1::Refresh),
                order_v1("queuedLaunch", "flow-q2", launch),
                order_v1("queuedRestart", "flow-q3", ReplacementOrderV1::Restart),
                order_v1("queuedSecond", "flow-q4", ReplacementOrderV1::Refresh),
            ],
            &[
                intent_v4(
                    "owedOrdered",
                    "flow-s1",
                    "launch-s1",
                    SuccessionCauseV4::Ordered,
                ),
                intent_v4(
                    "owedThreshold",
                    "flow-s2",
                    "launch-s2",
                    SuccessionCauseV4::Refresh,
                ),
                intent_v4("owedExit", "flow-s3", "launch-s3", SuccessionCauseV4::Exit),
                intent_v4(
                    "owedReplace",
                    "flow-s4",
                    "launch-s4",
                    SuccessionCauseV4::Replace,
                ),
            ],
        );
        for _ in 0..2 {
            let mut engine = open_before_step(&path);
            let Err(StoreError::RefreshCallerUpgrade(open)) =
                FlowStore::upgrade_refresh_callers(&mut engine)
            else {
                panic!("the step refuses while a Refresh is open");
            };
            assert_eq!(
                open,
                vec![
                    OpenRefreshWithoutCaller::QueuedRefresh {
                        address: address("queuedFirst"),
                        predecessor: Predecessor("flow-q1".into()),
                    },
                    OpenRefreshWithoutCaller::QueuedRefresh {
                        address: address("queuedSecond"),
                        predecessor: Predecessor("flow-q4".into()),
                    },
                    OpenRefreshWithoutCaller::OrderedSuccession {
                        address: address("owedOrdered"),
                        predecessor: Predecessor("flow-s1".into()),
                        launch_request_id: LaunchRequestId("launch-s1".into()),
                    },
                ],
                "each open Refresh, by its exact identity"
            );
            let refusal = StoreError::RefreshCallerUpgrade(open).to_string();
            assert!(
                refusal.contains("queued Refresh at ")
                    && refusal.contains("flow-q1")
                    && refusal.contains("flow-q4")
                    && refusal.contains("launch request launch-s1"),
                "{refusal}"
            );
            // Nothing was written: both families stand as they were.
            let stored = |table: &str| {
                engine
                    .list_tables()
                    .into_iter()
                    .find(|registration| registration.identity().table_name() == table)
                    .map(|registration| registration.identity().schema_hash())
            };
            assert_eq!(
                stored(super::super::ordered_replacement::ORDERED_REPLACEMENT_TABLE_NAME.as_str()),
                Some(OrderedReplacementV1::schema_hash())
            );
            assert_eq!(
                stored(super::super::successor::SUCCESSOR_INTENT_TABLE_NAME.as_str()),
                Some(SuccessorIntentV4::schema_hash())
            );
        }
    }

    /// With no Refresh open the step upgrades both families: every row is
    /// carried over unchanged, a Launch order whole with its profile, and the
    /// formats are current; a second open changes nothing.
    #[test]
    fn with_no_open_refresh_both_families_upgrade_and_every_row_carries_over() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("refresh-callers-none-open.sema");
        let launch = OrderedLaunch {
            launch_profile: fixture_profile(),
            origin: signal_flow::Origin::Unbound,
            prompt_sha256: crate::generated::memory::PromptSha256("b".repeat(64)),
        };
        let emergency = EmergencyRefreshWithoutHandover_Data {
            flow_id: signal_flow::FlowId("flow-e".into()),
            transcript_path_option: Some(signal_flow::TranscriptPath("/t/e.jsonl".into())),
        };
        store_before(
            &path,
            &[
                order_v1(
                    "carriedLaunch",
                    "flow-a",
                    ReplacementOrderV1::Launch(Box::new(launch.clone())),
                ),
                order_v1("carriedRestart", "flow-b", ReplacementOrderV1::Restart),
            ],
            &[
                intent_v4(
                    "carriedThreshold",
                    "flow-c",
                    "launch-c",
                    SuccessionCauseV4::Refresh,
                ),
                intent_v4("carriedExit", "flow-d", "launch-d", SuccessionCauseV4::Exit),
                intent_v4(
                    "carriedReplace",
                    "flow-f",
                    "launch-f",
                    SuccessionCauseV4::Replace,
                ),
                intent_v4(
                    "carriedEmergency",
                    "flow-g",
                    "launch-g",
                    SuccessionCauseV4::EmergencyRefreshWithoutHandover(emergency.clone()),
                ),
            ],
        );
        for _ in 0..2 {
            let mut engine = open_before_step(&path);
            FlowStore::upgrade_refresh_callers(&mut engine).expect("no Refresh is open");
            let orders = engine
                .register_table(OrderedReplacement::descriptor())
                .unwrap();
            let mut orders = engine
                .match_records(QueryPlan::all(orders))
                .unwrap()
                .records()
                .to_vec();
            orders.sort_by(|left, right| left.address.topic.0.cmp(&right.address.topic.0));
            assert_eq!(orders.len(), 2);
            assert_eq!(orders[0].address, address("carriedLaunch"));
            assert_eq!(orders[0].predecessor.0, "flow-a");
            assert_eq!(orders[0].ordered_launch(), Some(&launch));
            assert_eq!(orders[1].address, address("carriedRestart"));
            assert_eq!(orders[1].replacement_order, ReplacementOrder::Restart);
            let intents = engine
                .register_table(SuccessorIntent::descriptor())
                .unwrap();
            let mut intents = engine
                .match_records(QueryPlan::all(intents))
                .unwrap()
                .records()
                .to_vec();
            intents.sort_by(|left, right| left.address.topic.0.cmp(&right.address.topic.0));
            let causes = intents
                .iter()
                .map(|intent| {
                    (
                        intent.address.topic.0.clone(),
                        intent.succession_cause.clone(),
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                causes,
                vec![
                    (
                        "carriedEmergency".to_owned(),
                        SuccessionCause::EmergencyRefreshWithoutHandover(emergency.clone())
                    ),
                    ("carriedExit".to_owned(), SuccessionCause::Exit),
                    ("carriedReplace".to_owned(), SuccessionCause::Replace),
                    ("carriedThreshold".to_owned(), SuccessionCause::Refresh),
                ]
            );
            assert_eq!(intents[0].launch_request_id.0, "launch-g");
            assert_eq!(intents[0].predecessor.0, "flow-g");
            assert_eq!(
                engine
                    .list_tables()
                    .into_iter()
                    .filter(|registration| {
                        registration.identity().schema_hash()
                            == SchemaHash::for_label("flow-nexus-successor-intent-v5")
                            || registration.identity().schema_hash()
                                == SchemaHash::for_label("flow-nexus-ordered-replacement-v2")
                    })
                    .count(),
                2,
                "both families are current"
            );
        }
    }

    /// A record of the current format keeps its caller in every layout the
    /// family can hold: queued Refresh and the succession it causes, each
    /// caller kind, written and read back whole.
    #[test]
    fn a_current_refresh_and_its_succession_read_back_with_each_caller_kind() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("refresh-callers-current.sema");
        let callers = [
            RefreshCaller::AddressFlow(signal_flow::FlowId("flow-own".into())),
            RefreshCaller::Elsewhere(signal_flow::Origin::Unbound),
            RefreshCaller::Elsewhere(signal_flow::Origin::Flow(signal_flow::FlowOrigin {
                flow_id: signal_flow::FlowId("flow-other".into()),
                session_id: signal_flow::SessionId("session-other".into()),
            })),
        ];
        let orders: Vec<OrderedReplacement> = callers
            .iter()
            .enumerate()
            .map(|(index, caller)| OrderedReplacement {
                address: address(&format!("order{index}")),
                predecessor: Predecessor(format!("flow-{index}")),
                replacement_order: ReplacementOrder::Refresh(caller.clone()),
            })
            .collect();
        let intents: Vec<SuccessorIntent> = callers
            .iter()
            .enumerate()
            .map(|(index, caller)| SuccessorIntent {
                address: address(&format!("intent{index}")),
                predecessor: Predecessor(format!("flow-{index}")),
                succession_cause: SuccessionCause::Ordered(caller.clone()),
                launch_request_id: LaunchRequestId(format!("launch-{index}")),
            })
            .collect();
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let order_table = engine
                .register_table(OrderedReplacement::descriptor())
                .unwrap();
            let intent_table = engine
                .register_table(SuccessorIntent::descriptor())
                .unwrap();
            for order in &orders {
                engine
                    .assert(Assertion::new(order_table, order.clone()))
                    .unwrap();
            }
            for intent in &intents {
                engine
                    .assert(Assertion::new(intent_table, intent.clone()))
                    .unwrap();
            }
        }
        let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
        FlowStore::upgrade_refresh_callers(&mut engine).expect("a current store is left as it is");
        let order_table = engine
            .register_table(OrderedReplacement::descriptor())
            .unwrap();
        let intent_table = engine
            .register_table(SuccessorIntent::descriptor())
            .unwrap();
        let mut read_orders = engine
            .match_records(QueryPlan::all(order_table))
            .unwrap()
            .records()
            .to_vec();
        read_orders.sort_by(|left, right| left.address.topic.0.cmp(&right.address.topic.0));
        let mut read_intents = engine
            .match_records(QueryPlan::all(intent_table))
            .unwrap()
            .records()
            .to_vec();
        read_intents.sort_by(|left, right| left.address.topic.0.cmp(&right.address.topic.0));
        assert_eq!(read_orders, orders);
        assert_eq!(read_intents, intents);
    }

    fn fixture_profile() -> signal_flow::LaunchProfile {
        signal_flow::LaunchProfile {
            launch_request_id: signal_flow::LaunchRequestId("launch-ordered".into()),
            system_modules: signal_flow::SystemModules(vec![signal_flow::ModuleIdentity(
                "spirit:core:0".into(),
            )]),
            prompt_modules: signal_flow::PromptModules(vec![signal_flow::ModuleIdentity(
                "operation:main-flow:0".into(),
            )]),
            address: address("orderedLaunch"),
            model_name: flow_ethos::ModelName("gpt-6-astra".into()),
            effort: signal_flow::Effort("medium".into()),
            flow_id_option: Some(flow_ethos::FlowId("1b8ac0".into())),
            remembering: signal_flow::Remembering::Custom(vec![flow_ethos::FlowId(
                "836818".into(),
            )]),
            herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
            leave_outs_option: None,
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            instruction_prompt: signal_flow::InstructionPrompt("Carry the bounded task.".into()),
        }
    }
}
