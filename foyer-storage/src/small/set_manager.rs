//  Copyright 2024 Foyer Project Authors
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

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    sync::Arc,
};

use foyer_common::strict_assert;
use itertools::Itertools;
use ordered_hash_map::OrderedHashMap;

use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    device::{MonitoredDevice, RegionId},
    Dev,
};

use super::set::{Set, SetId, SetMut, SetStorage};
use crate::error::Result;

struct SetManagerInner {
    sets: Vec<RwLock<()>>,
    cache: Mutex<OrderedHashMap<SetId, Arc<SetStorage>>>,
    set_cache_capacity: usize,

    set_size: usize,
    device: MonitoredDevice,
    regions: Range<RegionId>,
    flush: bool,
}

#[derive(Clone)]
pub struct SetManager {
    inner: Arc<SetManagerInner>,
}

impl Debug for SetManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetManager")
            .field("sets", &self.inner.sets.len())
            .field("cache_capacity", &self.inner.set_cache_capacity)
            .field("size", &self.inner.set_size)
            .field("device", &self.inner.device)
            .field("regions", &self.inner.regions)
            .field("flush", &self.inner.flush)
            .finish()
    }
}

impl SetManager {
    pub fn new(
        set_size: usize,
        set_cache_capacity: usize,
        device: MonitoredDevice,
        regions: Range<RegionId>,
        flush: bool,
    ) -> Self {
        let sets = (device.region_size() / set_size) * (regions.end - regions.start) as usize;
        assert!(sets > 0);

        let sets = (0..sets).map(|_| RwLock::default()).collect_vec();
        let cache = Mutex::new(OrderedHashMap::with_capacity(set_cache_capacity));

        let inner = SetManagerInner {
            sets,
            cache,
            set_cache_capacity,
            set_size,
            device,
            regions,
            flush,
        };
        let inner = Arc::new(inner);
        Self { inner }
    }

    pub async fn write(&self, id: SetId) -> Result<SetWriteGuard<'_>> {
        let guard = self.inner.sets[id as usize].write().await;

        let invalid = self.inner.cache.lock().await.remove(&id);
        let storage = match invalid {
            // `guard` already guarantees that there is only one storage reference left.
            Some(storage) => Arc::into_inner(storage).unwrap(),
            None => self.storage(id).await?,
        };

        Ok(SetWriteGuard {
            guard,
            id,
            set: SetMut::new(storage),
            drop: DropPanicGuard::default(),
        })
    }

    pub async fn read(&self, id: SetId) -> Result<SetReadGuard<'_>> {
        let guard = self.inner.sets[id as usize].read().await;

        let mut cache = self.inner.cache.lock().await;
        let cached = cache.get(&id).cloned();
        let storage = match cached {
            Some(storage) => storage,
            None => {
                let storage = self.storage(id).await?;
                let storage = Arc::new(storage);
                cache.insert(id, storage.clone());
                if cache.len() > self.inner.set_cache_capacity {
                    cache.pop_front();
                    strict_assert!(cache.len() <= self.inner.set_cache_capacity);
                }
                storage
            }
        };
        drop(cache);

        Ok(SetReadGuard {
            _guard: guard,
            _id: id,
            set: Set::new(storage),
        })
    }

    pub async fn apply(&self, mut guard: SetWriteGuard<'_>) -> Result<()> {
        let storage = guard.set.into_storage();
        self.flush(guard.id, storage).await?;
        guard.drop.disable();
        drop(guard.guard);
        Ok(())
    }

    pub fn sets(&self) -> usize {
        self.inner.sets.len()
    }

    pub fn set_size(&self) -> usize {
        self.inner.set_size
    }

    pub fn set_data_size(&self) -> usize {
        self.inner.set_size - SetStorage::SET_HEADER_SIZE
    }

    async fn storage(&self, id: SetId) -> Result<SetStorage> {
        let (region, offset) = self.locate(id);
        let buffer = self.inner.device.read(region, offset, self.inner.set_size).await?;
        let storage = SetStorage::load(buffer);
        Ok(storage)
    }

    async fn flush(&self, id: SetId, storage: SetStorage) -> Result<()> {
        let buffer = storage.freeze();
        let (region, offset) = self.locate(id);
        self.inner.device.write(buffer, region, offset).await?;
        if self.inner.flush {
            self.inner.device.flush(Some(region)).await?;
        }
        Ok(())
    }

    #[inline]
    fn region_sets(&self) -> usize {
        self.inner.device.region_size() / self.inner.set_size
    }

    #[inline]
    fn locate(&self, id: SetId) -> (RegionId, u64) {
        let region_sets = self.region_sets();
        let region = id as RegionId / region_sets as RegionId;
        let offset = ((id as usize % region_sets) * self.inner.set_size) as u64;
        (region, offset)
    }
}

#[derive(Debug, Default)]
struct DropPanicGuard {
    disabled: bool,
}

impl Drop for DropPanicGuard {
    fn drop(&mut self) {
        if !self.disabled {
            panic!("unexpected drop panic guard drop");
        }
    }
}

impl DropPanicGuard {
    fn disable(&mut self) {
        self.disabled = true;
    }
}

#[derive(Debug)]
pub struct SetWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, ()>,
    id: SetId,
    set: SetMut,
    drop: DropPanicGuard,
}

impl<'a> Deref for SetWriteGuard<'a> {
    type Target = SetMut;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}

impl<'a> DerefMut for SetWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.set
    }
}

#[derive(Debug)]
pub struct SetReadGuard<'a> {
    _guard: RwLockReadGuard<'a, ()>,
    _id: SetId,
    set: Set,
}

impl<'a> Deref for SetReadGuard<'a> {
    type Target = Set;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}
