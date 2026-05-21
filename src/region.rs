// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use derive_new::new;

use crate::proto::metapb;
use crate::Error;
use crate::Key;
use crate::Result;

/// The ID of a region
pub type RegionId = u64;
/// The ID of a store
pub type StoreId = u64;

/// The ID and version information of a region.
#[derive(Eq, PartialEq, Hash, Clone, Default, Debug)]
pub struct RegionVerId {
    /// The ID of the region
    pub id: RegionId,
    /// Conf change version, auto increment when add or remove peer
    pub conf_ver: u64,
    /// Region version, auto increment when split or merge
    pub ver: u64,
}

/// Information about a TiKV region and its leader.
///
/// In TiKV all data is partitioned by range. Each partition is called a region.
#[derive(new, Clone, Default, Debug, PartialEq)]
pub struct RegionWithLeader {
    pub region: metapb::Region,
    pub leader: Option<metapb::Peer>,
}

impl Eq for RegionWithLeader {}

impl RegionWithLeader {
    pub fn contains(&self, key: &Key) -> bool {
        let key: &[u8] = key.into();
        let start_key = &self.region.start_key;
        let end_key = &self.region.end_key;
        key >= start_key.as_slice() && (key < end_key.as_slice() || end_key.is_empty())
    }

    pub fn start_key(&self) -> Key {
        self.region.start_key.to_vec().into()
    }

    pub fn end_key(&self) -> Key {
        self.region.end_key.to_vec().into()
    }

    pub fn range(&self) -> (Key, Key) {
        (self.start_key(), self.end_key())
    }

    pub fn ver_id(&self) -> RegionVerId {
        let region = &self.region;
        let epoch = region.region_epoch.as_ref().unwrap();
        RegionVerId {
            id: region.id,
            conf_ver: epoch.conf_ver,
            ver: epoch.version,
        }
    }

    pub fn id(&self) -> RegionId {
        self.region.id
    }

    pub fn get_store_id(&self) -> Result<StoreId> {
        self.leader
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::LeaderNotFound {
                region: self.ver_id(),
            })
            .map(|s| s.store_id)
    }

    /// Pick a peer round-robin across all voter peers (leader + followers).
    /// Learner peers are excluded because they cannot serve raw reads.
    /// Falls back to the leader if no voter peers are found.
    pub fn pick_any_peer(&self) -> Option<&metapb::Peer> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        // role == 0 (Voter) or role == 2 (IncomingVoter) can serve reads;
        // role == 1 (Learner) and role == 3 (DemotingVoter) cannot.
        let voters: Vec<&metapb::Peer> = self
            .region
            .peers
            .iter()
            .filter(|p| p.role == 0 || p.role == 2)
            .collect();
        if voters.is_empty() {
            self.leader.as_ref()
        } else {
            let idx = COUNTER.fetch_add(1, Ordering::Relaxed) % voters.len();
            Some(voters[idx])
        }
    }
}
