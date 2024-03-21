//  Copyright 2024 Foyer Project Authors.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use foyer_common::async_queue::AsyncQueue;
use foyer_intrusive::{
    core::adapter::Link,
    eviction::{EvictionPolicy, EvictionPolicyExt},
    intrusive_adapter, key_adapter,
};
use parking_lot::RwLock;

use crate::{
    device::Device,
    region::{Region, RegionId},
};

#[derive(Debug)]
pub struct RegionEpItem<L>
where
    L: Link,
{
    link: L,
    id: RegionId,
}

intrusive_adapter! { pub RegionEpItemAdapter<L> = Arc<RegionEpItem<L>>: RegionEpItem<L> { link: L } where L: Link }
key_adapter! { RegionEpItemAdapter<L> = RegionEpItem<L> { id: RegionId } where L: Link }

#[derive(Debug)]
pub struct RegionManager<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    /// Empty regions.
    clean_regions: AsyncQueue<RegionId>,

    regions: Vec<Region<D>>,
    items: Vec<Arc<RegionEpItem<EL>>>,

    /// Eviction policy.
    eviction: RwLock<EP>,
}

impl<D, EP, EL> RegionManager<D, EP, EL>
where
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub fn new(region_count: usize, eviction_config: EP::Config, device: D) -> Self {
        let eviction = EP::new(eviction_config);
        let clean_regions = AsyncQueue::new();

        let mut regions = Vec::with_capacity(region_count);
        let mut items = Vec::with_capacity(region_count);

        for id in 0..region_count as RegionId {
            let region = Region::new(id, device.clone());
            let item = Arc::new(RegionEpItem {
                link: EL::default(),
                id,
            });

            regions.push(region);
            items.push(item);
        }

        Self {
            clean_regions,
            regions,
            items,
            eviction: RwLock::new(eviction),
        }
    }

    pub fn region(&self, id: &RegionId) -> &Region<D> {
        &self.regions[*id as usize]
    }

    #[tracing::instrument(skip(self))]
    pub fn record_access(&self, id: &RegionId) {
        let mut eviction = self.eviction.write();
        let item = &self.items[*id as usize];
        if item.link.is_linked() {
            eviction.access(&self.items[*id as usize]);
        }
    }

    pub fn clean_regions(&self) -> &AsyncQueue<RegionId> {
        &self.clean_regions
    }

    pub fn eviction_push(&self, region_id: RegionId) {
        self.eviction.write().push(self.items[region_id as usize].clone());
    }

    pub fn eviction_pop(&self) -> Option<RegionId> {
        self.eviction.write().pop().map(|item| item.id)
    }
}
