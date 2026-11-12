//! Brief visibility over one coherent availability and cohesion snapshot.

use signal_flow::{Address, Listed, Metaflow, PermaflowListingRejection, Scope};

/// Available rows already contain only addresses and their awake standing.
/// Membership informs their visibility; it never creates an available row.
pub struct PermaflowInventory {
    pub available: Vec<Listed>,
    pub metaflows: Vec<Metaflow>,
}

pub trait SelectsPermaflows {
    /// All needs no identity. Mine requires an unambiguous caller and shows
    /// the union of its topic and every cohesion to which it belongs.
    fn select(
        &self,
        scope: &Scope,
        caller: Result<&Address, PermaflowListingRejection>,
    ) -> Result<Vec<Listed>, PermaflowListingRejection>;
}

impl SelectsPermaflows for PermaflowInventory {
    fn select(
        &self,
        scope: &Scope,
        caller: Result<&Address, PermaflowListingRejection>,
    ) -> Result<Vec<Listed>, PermaflowListingRejection> {
        let caller = match scope {
            Scope::All => return Ok(self.available.clone()),
            Scope::Mine => caller?,
        };
        Ok(self
            .available
            .iter()
            .filter(|listed| {
                listed.address.topic == caller.topic
                    || self.metaflows.iter().any(|metaflow| {
                        metaflow.address_vector.contains(caller)
                            && metaflow.address_vector.contains(&listed.address)
                    })
            })
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_flow::{Aspect, MetaflowName, Model, State, Topic};

    fn address(topic: &str, model: Model) -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: Topic(topic.into()),
            model,
        }
    }

    fn listed(address: Address, state: State) -> Listed {
        Listed { address, state }
    }

    #[test]
    fn mine_unites_topics_and_every_own_group_without_making_members_available() {
        let caller = address("flowNexus", Model::Sol);
        let topic_peer = address("flowNexus", Model::Astra);
        let first_peer = address("core", Model::Astra);
        let second_peer = address("curriculum", Model::Astra);
        let unavailable = address("undeclared", Model::Haiku);
        let unrelated = address("energy", Model::Sol);
        let rows = vec![
            listed(caller.clone(), State::Awake),
            listed(topic_peer, State::Asleep),
            listed(first_peer.clone(), State::Awake),
            listed(second_peer.clone(), State::Asleep),
            listed(unrelated, State::Awake),
        ];
        let inventory = PermaflowInventory {
            available: rows.clone(),
            metaflows: vec![
                Metaflow {
                    metaflow_name: MetaflowName("firstTeam".into()),
                    address_vector: vec![caller.clone(), first_peer.clone(), unavailable],
                },
                Metaflow {
                    metaflow_name: MetaflowName("secondTeam".into()),
                    address_vector: vec![caller.clone(), first_peer, second_peer],
                },
            ],
        };
        assert_eq!(
            inventory.select(&Scope::Mine, Ok(&caller)).unwrap(),
            rows[..4]
        );
        assert_eq!(
            inventory
                .select(&Scope::All, Err(PermaflowListingRejection::CallerUnknown))
                .unwrap(),
            rows
        );
    }

    #[test]
    fn mine_preserves_unknown_and_contradictory_refusals() {
        let inventory = PermaflowInventory {
            available: vec![listed(address("core", Model::Sol), State::Awake)],
            metaflows: vec![],
        };
        for rejection in [
            PermaflowListingRejection::CallerUnknown,
            PermaflowListingRejection::CallerContradictory,
            PermaflowListingRejection::StoreRefused,
        ] {
            assert_eq!(
                inventory.select(&Scope::Mine, Err(rejection.clone())),
                Err(rejection)
            );
        }
    }

    #[test]
    fn removing_membership_removes_visibility_while_topic_visibility_remains() {
        let caller = address("flowNexus", Model::Sol);
        let peer = address("core", Model::Astra);
        let mut inventory = PermaflowInventory {
            available: vec![
                listed(caller.clone(), State::Awake),
                listed(peer.clone(), State::Awake),
            ],
            metaflows: vec![Metaflow {
                metaflow_name: MetaflowName("flowTeam".into()),
                address_vector: vec![caller.clone(), peer],
            }],
        };
        assert_eq!(
            inventory.select(&Scope::Mine, Ok(&caller)).unwrap().len(),
            2
        );
        inventory.metaflows.clear();
        assert_eq!(
            inventory.select(&Scope::Mine, Ok(&caller)).unwrap(),
            inventory.available[..1]
        );
    }
}
