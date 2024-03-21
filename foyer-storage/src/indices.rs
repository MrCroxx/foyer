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

use std::collections::BTreeMap;

use foyer_common::code::Key;
use itertools::Itertools;
use parking_lot::{RwLock, RwLockWriteGuard};

use crate::region::{RegionId, Version};

#[derive(Debug, Clone)]
pub struct Index<K>
where
    K: Key,
{
    pub key: K,

    pub region: RegionId,
    pub version: Version,
    pub offset: u32,
    pub len: u32,
    pub key_len: u32,
    pub value_len: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct Slot {
    pub region: RegionId,
    pub sequence: u32,
}

#[derive(Debug)]
struct IndicesInner<K>
where
    K: Key,
{
    slots: BTreeMap<K, Slot>,
    regions: Vec<BTreeMap<u32, Index<K>>>,
    sequences: Vec<usize>,
}

#[derive(Debug)]
pub struct Indices<K>
where
    K: Key,
{
    inner: RwLock<IndicesInner<K>>,
}

impl<K> Indices<K>
where
    K: Key,
{
    pub fn new(regions: usize) -> Self {
        let inner = IndicesInner {
            slots: BTreeMap::new(),
            regions: vec![BTreeMap::new(); regions],
            sequences: vec![0; regions],
        };
        Self {
            inner: RwLock::new(inner),
        }
    }

    pub fn insert(&self, index: Index<K>) {
        let mut inner = self.inner.write();
        self.insert_inner(&mut inner, index)
    }

    pub fn lookup(&self, key: &K) -> Option<Index<K>> {
        let inner = self.inner.read();
        let slot = inner.slots.get(key)?;
        inner.regions[slot.region as usize]
            .get(&slot.sequence)
            .cloned()
    }

    pub fn remap(&self, old_key: &K, new_key: K) -> bool {
        let mut inner = self.inner.write();
        match self.remove_inner(&mut inner, old_key) {
            Some(mut index) => {
                index.key = new_key;
                self.insert_inner(&mut inner, index);
                true
            }
            None => false,
        }
    }

    pub fn remove(&self, key: &K) -> Option<Index<K>> {
        let mut inner = self.inner.write();
        self.remove_inner(&mut inner, key)
    }

    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.slots.clear();
        inner.regions.iter_mut().for_each(|region| region.clear());
    }

    pub fn take_region(&self, region: &RegionId) -> Vec<Index<K>> {
        let mut inner = self.inner.write();
        let mut indices = BTreeMap::new();
        std::mem::swap(&mut indices, &mut inner.regions[*region as usize]);

        for index in indices.values() {
            inner.slots.remove(&index.key);
        }

        indices.into_values().collect_vec()
    }

    fn insert_inner(&self, inner: &mut RwLockWriteGuard<'_, IndicesInner<K>>, index: Index<K>) {
        let region = index.region;
        let key = index.key.clone();

        let sequence = inner.sequences[region as usize] as u32;
        inner.sequences[region as usize] += 1;

        inner.regions[region as usize].insert(sequence, index);
        inner.slots.insert(key, Slot { region, sequence });
    }

    fn remove_inner(
        &self,
        inner: &mut RwLockWriteGuard<'_, IndicesInner<K>>,
        key: &K,
    ) -> Option<Index<K>> {
        let slot = inner.slots.remove(key)?;
        inner.regions[slot.region as usize].remove(&slot.sequence)
    }
}
