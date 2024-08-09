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

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::{Cache, CacheEntry};
use futures::{future::try_join_all, Future, FutureExt};
use itertools::Itertools;

use tokio::{runtime::Handle, sync::oneshot};

use std::{borrow::Borrow, fmt::Debug, hash::Hash, ops::Range, sync::Arc};

use crate::{
    device::{MonitoredDevice, RegionId},
    error::Result,
    serde::KvInfo,
    storage::{Storage, WaitHandle},
    DeviceStats, IoBytes,
};

use super::{flusher::Flusher, set::SetId, set_manager::SetManager};

pub struct GenericSmallStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub memory: Cache<K, V, S>,
    /// Set size in bytes.
    pub set_size: usize,
    /// Set cache capacity by set count.
    pub set_cache_capacity: usize,
    /// Device for small object disk cache.
    pub device: MonitoredDevice,
    /// Regions of the device to use.
    pub regions: Range<RegionId>,
    /// Whether to flush after writes.
    pub flush: bool,
    /// Flusher count.
    pub flushers: usize,
}

impl<K, V, S> Debug for GenericSmallStorageConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorageConfig").finish()
    }
}

struct GenericSmallStorageInner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    memory: Cache<K, V, S>,

    flushers: Vec<Flusher<K, V, S>>,

    device: MonitoredDevice,
    set_manager: SetManager,

    runtime: Handle,
}

pub struct GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    inner: Arc<GenericSmallStorageInner<K, V, S>>,
}

impl<K, V, S> Debug for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericSmallStorage").finish()
    }
}

impl<K, V, S> Clone for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V, S> GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    async fn open(config: GenericSmallStorageConfig<K, V, S>) -> Result<Self> {
        let runtime = Handle::current();

        let set_manager = SetManager::new(
            config.set_size,
            config.set_cache_capacity,
            config.device.clone(),
            config.regions,
            config.flush,
        );

        let flushers = (0..config.flushers)
            .map(|_| Flusher::new(set_manager.clone(), &runtime))
            .collect_vec();

        let inner = GenericSmallStorageInner {
            memory: config.memory,
            flushers,
            device: config.device,
            set_manager,
            runtime,
        };
        let inner = Arc::new(inner);

        Ok(Self { inner })
    }

    async fn close(&self) -> Result<()> {
        try_join_all(self.inner.flushers.iter().map(|flusher| flusher.close())).await?;
        Ok(())
    }

    fn enqueue(&self, entry: CacheEntry<K, V, S>, buffer: IoBytes, info: KvInfo, tx: oneshot::Sender<Result<bool>>) {
        // Entries with the same hash must be grouped in the batch.
        let id = entry.hash() as usize % self.inner.flushers.len();
        self.inner.flushers[id].entry(entry, buffer, info, tx);
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(K, V)>>> + Send + 'static
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        let hash = self.inner.memory.hash_builder().hash_one(key);
        let set_manager = self.inner.set_manager.clone();
        let sid = hash % set_manager.sets() as SetId;
        async move {
            let set = set_manager.read(sid).await?;
            let kv = set.get(hash)?;
            Ok(kv)
        }
    }

    fn delete<Q>(&self, key: &Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'static>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // Entries with the same hash must be grouped in the batch.
        let hash = self.inner.memory.hash_builder().hash_one(key);
        let id = hash as usize % self.inner.flushers.len();

        let (tx, rx) = oneshot::channel();
        let handle = WaitHandle::new(rx.map(|recv| recv.unwrap()));

        self.inner.flushers[id].deletion(hash, tx);

        handle
    }

    fn stats(&self) -> Arc<DeviceStats> {
        self.inner.device.stat().clone()
    }

    fn runtime(&self) -> &Handle {
        &self.inner.runtime
    }
}

impl<K, V, S> Storage for GenericSmallStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = GenericSmallStorageConfig<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        Self::open(config).await
    }

    async fn close(&self) -> Result<()> {
        self.close().await?;
        Ok(())
    }

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        buffer: IoBytes,
        info: KvInfo,
        tx: oneshot::Sender<Result<bool>>,
    ) {
        self.enqueue(entry, buffer, info, tx)
    }

    // FIXME: REMOVE THE CLIPPY IGNORE.
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::manual_async_fn)]
    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.load(key)
    }

    fn delete<Q>(&self, key: &Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'static>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.delete(key)
    }

    fn may_contains<Q>(&self, _key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        todo!()
    }

    async fn destroy(&self) -> Result<()> {
        todo!()
    }

    fn stats(&self) -> Arc<DeviceStats> {
        self.stats()
    }

    fn runtime(&self) -> &Handle {
        self.runtime()
    }
}
