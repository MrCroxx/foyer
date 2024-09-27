//  Copyright 2024 foyer Project Authors
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

use bytes::{Buf, BufMut};
use foyer_common::strict_assert;
use itertools::Itertools;
use ordered_hash_map::OrderedHashMap;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{
    bloom_filter::BloomFilterU64,
    set::{Set, SetId, SetMut, SetStorage, SetTimestamp},
};
use crate::{
    device::{Dev, MonitoredDevice, RegionId},
    error::Result,
    IoBytesMut,
};

struct SetManagerInner {
    /// A phantom rwlock to prevent set storage operations on disk.
    ///
    /// All set disk operations must be prevented by the lock.
    ///
    /// In addition, the rwlock also serves as the lock of the in-memory bloom filter.
    sets: Vec<RwLock<BloomFilterU64<4>>>,
    cache: Mutex<OrderedHashMap<SetId, Arc<SetStorage>>>,
    set_cache_capacity: usize,
    set_picker: SetPicker,

    metadata: RwLock<Metadata>,

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
            .field("sets", &self.inner.sets)
            .field("cache", &self.inner.cache)
            .field("set_cache_capacity", &self.inner.set_cache_capacity)
            .field("set_picker", &self.inner.set_picker)
            .field("metadata", &self.inner.metadata)
            .field("set_size", &self.inner.set_size)
            .field("device", &self.inner.device)
            .field("regions", &self.inner.regions)
            .field("flush", &self.inner.flush)
            .finish()
    }
}

impl SetManager {
    pub async fn open(
        set_size: usize,
        set_cache_capacity: usize,
        device: MonitoredDevice,
        regions: Range<RegionId>,
        flush: bool,
    ) -> Result<Self> {
        let sets = (device.region_size() / set_size) * (regions.end - regions.start) as usize;
        assert!(sets > 0);

        let set_picker = SetPicker::new(sets);

        // load & flush metadata
        let metadata = Metadata::load(&device).await?;
        metadata.flush(&device).await?;
        let metadata = RwLock::new(metadata);

        let sets = (0..sets).map(|_| RwLock::default()).collect_vec();
        let cache = Mutex::new(OrderedHashMap::with_capacity(set_cache_capacity));

        let inner = SetManagerInner {
            sets,
            cache,
            set_cache_capacity,
            set_picker,
            metadata,
            set_size,
            device,
            regions,
            flush,
        };
        let inner = Arc::new(inner);
        Ok(Self { inner })
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
            bloom_filter: guard,
            id,
            set: SetMut::new(storage),
            drop: DropPanicGuard::default(),
        })
    }

    pub async fn read(&self, id: SetId, hash: u64) -> Result<Option<SetReadGuard<'_>>> {
        let guard = self.inner.sets[id as usize].read().await;
        if !guard.lookup(hash) {
            return Ok(None);
        }

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

        Ok(Some(SetReadGuard {
            _bloom_filter: guard,
            _id: id,
            set: Set::new(storage),
        }))
    }

    pub async fn apply(&self, mut guard: SetWriteGuard<'_>) -> Result<()> {
        let mut storage = guard.set.into_storage();

        // Update in-memory bloom filter.
        storage.update();
        *guard.bloom_filter = storage.bloom_filter().clone();

        let buffer = storage.freeze();

        let (region, offset) = self.locate(guard.id);
        self.inner.device.write(buffer, region, offset).await?;
        if self.inner.flush {
            self.inner.device.flush(Some(region)).await?;
        }
        guard.drop.disable();
        drop(guard.bloom_filter);
        Ok(())
    }

    pub async fn contains(&self, id: SetId, hash: u64) -> bool {
        let guard = self.inner.sets[id as usize].read().await;
        guard.lookup(hash)
    }

    pub fn sets(&self) -> usize {
        self.inner.sets.len()
    }

    pub fn set_size(&self) -> usize {
        self.inner.set_size
    }

    pub fn set_picker(&self) -> &SetPicker {
        &self.inner.set_picker
    }

    pub async fn watermark(&self) -> u128 {
        self.inner.metadata.read().await.watermark
    }

    pub async fn destroy(&self) -> Result<()> {
        self.update_watermark().await?;
        self.inner.cache.lock().await.clear();
        Ok(())
    }

    async fn update_watermark(&self) -> Result<()> {
        let mut metadata = self.inner.metadata.write().await;

        let watermark = SetTimestamp::current();
        metadata.watermark = watermark;
        metadata.flush(&self.inner.device).await
    }

    async fn storage(&self, id: SetId) -> Result<SetStorage> {
        let (region, offset) = self.locate(id);
        let buffer = self.inner.device.read(region, offset, self.inner.set_size).await?;
        let storage = SetStorage::load(buffer, self.watermark().await);
        Ok(storage)
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
    bloom_filter: RwLockWriteGuard<'a, BloomFilterU64<4>>,
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
    _bloom_filter: RwLockReadGuard<'a, BloomFilterU64<4>>,
    _id: SetId,
    set: Set,
}

impl<'a> Deref for SetReadGuard<'a> {
    type Target = Set;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}

#[derive(Debug, Clone)]
pub struct SetPicker {
    sets: usize,
}

impl SetPicker {
    /// Create a [`SetPicker`] with a total size count.
    ///
    /// The `sets` should be the count of all sets.
    ///
    /// Note:
    ///
    /// The 0th set will be used as the meta set.
    pub fn new(sets: usize) -> Self {
        Self { sets }
    }

    pub fn sid(&self, hash: u64) -> SetId {
        // skip the meta set
        hash % (self.sets as SetId - 1) + 1
    }
}

#[derive(Debug)]
struct Metadata {
    /// watermark timestamp
    watermark: u128,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            watermark: SetTimestamp::current(),
        }
    }
}

impl Metadata {
    const MAGIC: u64 = 0x20230512deadbeef;
    const SIZE: usize = 8 + 16;

    fn write(&self, mut buf: impl BufMut) {
        buf.put_u64(Self::MAGIC);
        buf.put_u128(self.watermark);
    }

    fn read(mut buf: impl Buf) -> Self {
        let magic = buf.get_u64();
        let watermark = buf.get_u128();

        if magic != Self::MAGIC || watermark > SetTimestamp::current() {
            return Self::default();
        }

        Self { watermark }
    }

    async fn flush(&self, device: &MonitoredDevice) -> Result<()> {
        let mut buf = IoBytesMut::with_capacity(Self::SIZE);
        self.write(&mut buf);
        let buf = buf.freeze();
        device.write(buf, 0, 0).await?;
        Ok(())
    }

    async fn load(device: &MonitoredDevice) -> Result<Self> {
        let buf = device.read(0, 0, Metadata::SIZE).await?;
        let metadata = Metadata::read(&buf[..Metadata::SIZE]);
        Ok(metadata)
    }
}