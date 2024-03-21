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

use std::{
    future::Future,
    hash::BuildHasher,
    ops::Deref,
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use ahash::RandomState;
use crossbeam::queue::ArrayQueue;
use futures::FutureExt;
use hashbrown::hash_map::{Entry as HashMapEntry, HashMap};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    eviction::Eviction, handle::Handle, indexer::Indexer, listener::CacheEventListener, metrics::Metrics, CacheContext,
    Key, Value,
};

struct CacheSharedState<T, L> {
    metrics: Metrics,
    /// The object pool to avoid frequent handle allocating, shared by all shards.
    object_pool: ArrayQueue<Box<T>>,
    listener: L,
}

#[expect(clippy::type_complexity)]
struct CacheShard<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    indexer: I,
    eviction: E,

    capacity: usize,
    usage: Arc<AtomicUsize>,

    waiters: HashMap<K, Vec<oneshot::Sender<GenericCacheEntry<K, V, H, E, I, L, S>>>>,

    state: Arc<CacheSharedState<H, L>>,
}

impl<K, V, H, E, I, L, S> CacheShard<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn new(
        capacity: usize,
        eviction_config: &E::Config,
        usage: Arc<AtomicUsize>,
        context: Arc<CacheSharedState<H, L>>,
    ) -> Self {
        let indexer = I::new();
        let eviction = unsafe { E::new(capacity, eviction_config) };
        let waiters = HashMap::default();
        Self {
            indexer,
            eviction,
            capacity,
            usage,
            waiters,
            state: context,
        }
    }

    /// Insert a new entry into the cache. The handle for the new entry is returned.
    unsafe fn insert(
        &mut self,
        hash: u64,
        key: K,
        value: V,
        charge: usize,
        context: H::Context,
        last_reference_entries: &mut Vec<(K, V, H::Context, usize)>,
    ) -> NonNull<H> {
        let mut handle = self.state.object_pool.pop().unwrap_or_else(|| Box::new(H::new()));
        handle.init(hash, key, value, charge, context);
        let mut ptr = unsafe { NonNull::new_unchecked(Box::into_raw(handle)) };

        self.evict(charge, last_reference_entries);

        debug_assert!(!ptr.as_ref().base().is_in_indexer());
        if let Some(old) = self.indexer.insert(ptr) {
            self.state.metrics.replace.fetch_add(1, Ordering::Relaxed);

            debug_assert!(!old.as_ref().base().is_in_indexer());
            if old.as_ref().base().is_in_eviction() {
                self.eviction.remove(old);
            }
            debug_assert!(!old.as_ref().base().is_in_eviction());
            // Because the `old` handle is removed from the indexer, it will not be reinserted again.
            if let Some(entry) = self.try_release_handle(old, false) {
                last_reference_entries.push(entry);
            }
        } else {
            self.state.metrics.insert.fetch_add(1, Ordering::Relaxed);
        }
        self.eviction.push(ptr);

        debug_assert!(ptr.as_ref().base().is_in_indexer());
        debug_assert!(ptr.as_ref().base().is_in_indexer());

        self.usage.fetch_add(charge, Ordering::Relaxed);
        ptr.as_mut().base_mut().inc_refs();

        ptr
    }

    unsafe fn get(&mut self, hash: u64, key: &K) -> Option<NonNull<H>> {
        let mut ptr = match self.indexer.get(hash, key) {
            Some(ptr) => {
                self.state.metrics.hit.fetch_add(1, Ordering::Relaxed);
                ptr
            }
            None => {
                self.state.metrics.miss.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let base = ptr.as_mut().base_mut();
        debug_assert!(base.is_in_indexer());

        base.inc_refs();
        self.eviction.access(ptr);

        Some(ptr)
    }

    /// Remove a key from the cache.
    ///
    /// Return `Some(..)` if the handle is released, or `None` if the handle is still in use.
    unsafe fn remove(&mut self, hash: u64, key: &K) -> Option<(K, V, H::Context, usize)> {
        let ptr = self.indexer.remove(hash, key)?;
        self.state.metrics.remove.fetch_add(1, Ordering::Relaxed);
        if ptr.as_ref().base().is_in_eviction() {
            self.eviction.remove(ptr);
        }
        debug_assert!(!ptr.as_ref().base().is_in_indexer());
        debug_assert!(!ptr.as_ref().base().is_in_eviction());
        self.try_release_handle(ptr, false)
    }

    /// Clear all cache entries.
    unsafe fn clear(&mut self, last_reference_entries: &mut Vec<(K, V, H::Context, usize)>) {
        // TODO(MrCroxx): Avoid collecting here?
        let ptrs = self.indexer.drain().collect_vec();
        let eptrs = self.eviction.clear();

        // Assert that the handles in the indexer covers the handles in the eviction container.
        if cfg!(debug_assertions) {
            use std::{collections::HashSet as StdHashSet, hash::RandomState as StdRandomState};
            let ptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(ptrs.iter().copied());
            let eptrs: StdHashSet<_, StdRandomState> = StdHashSet::from_iter(eptrs.iter().copied());
            debug_assert!((&eptrs - &ptrs).is_empty());
        }

        self.state.metrics.remove.fetch_add(ptrs.len(), Ordering::Relaxed);

        // The handles in the indexer covers the handles in the eviction container.
        // So only the handles drained from the indexer need to be released.
        for ptr in ptrs {
            debug_assert!(!ptr.as_ref().base().is_in_indexer());
            if let Some(entry) = self.try_release_handle(ptr, false) {
                last_reference_entries.push(entry);
            }
        }
    }

    unsafe fn evict(&mut self, charge: usize, last_reference_entries: &mut Vec<(K, V, H::Context, usize)>) {
        while self.usage.load(Ordering::Relaxed) + charge > self.capacity
            && let Some(evicted) = self.eviction.pop()
        {
            self.state.metrics.evict.fetch_add(1, Ordering::Relaxed);
            let base = evicted.as_ref().base();
            debug_assert!(base.is_in_indexer());
            debug_assert!(!base.is_in_eviction());
            if let Some(entry) = self.try_release_handle(evicted, false) {
                last_reference_entries.push(entry);
            }
        }
    }

    /// Release a handle used by an external user.
    ///
    /// Return `Some(..)` if the handle is released, or `None` if the handle is still in use.
    unsafe fn try_release_external_handle(&mut self, mut ptr: NonNull<H>) -> Option<(K, V, H::Context, usize)> {
        ptr.as_mut().base_mut().dec_refs();
        self.try_release_handle(ptr, true)
    }

    /// Try release handle if there is no external reference and no reinsertion is needed.
    ///
    /// Return the entry if the handle is released.
    ///
    /// Recycle it if possible.
    unsafe fn try_release_handle(&mut self, mut ptr: NonNull<H>, reinsert: bool) -> Option<(K, V, H::Context, usize)> {
        let base = ptr.as_mut().base_mut();

        if base.has_refs() {
            return None;
        }

        debug_assert!(base.is_inited());
        debug_assert!(!base.has_refs());

        // If the entry is not updated or removed from the cache, try to reinsert it or remove it from the indexer and
        // the eviction container.
        if base.is_in_indexer() {
            // The usage is higher than the capacity means most handles are held externally,
            // the cache shard cannot release enough charges for the new inserted entries.
            // In this case, the reinsertion should be given up.
            if reinsert && self.usage.load(Ordering::Relaxed) <= self.capacity {
                let was_in_eviction = base.is_in_eviction();
                self.eviction.reinsert(ptr);
                if ptr.as_ref().base().is_in_eviction() {
                    if was_in_eviction {
                        self.state.metrics.reinsert.fetch_add(1, Ordering::Relaxed);
                    }
                    return None;
                }
            }

            // If the entry has not been reinserted, remove it from the indexer and the eviction container (if needed).
            self.indexer.remove(base.hash(), base.key());
            if ptr.as_ref().base().is_in_eviction() {
                self.eviction.remove(ptr);
            }
        }

        // Here the handle is neither in the indexer nor in the eviction container.
        debug_assert!(!base.is_in_indexer());
        debug_assert!(!base.is_in_eviction());
        debug_assert!(!base.has_refs());

        self.state.metrics.release.fetch_add(1, Ordering::Relaxed);

        self.usage.fetch_sub(base.charge(), Ordering::Relaxed);
        let entry = base.take();

        let handle = Box::from_raw(ptr.as_ptr());
        let _ = self.state.object_pool.push(handle);

        Some(entry)
    }
}

impl<K, V, H, E, I, L, S> Drop for CacheShard<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn drop(&mut self) {
        unsafe { self.clear(&mut vec![]) }
    }
}

pub struct CacheConfig<E, L, S = RandomState>
where
    E: Eviction,
    L: CacheEventListener<<E::Handle as Handle>::Key, <E::Handle as Handle>::Value>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub capacity: usize,
    pub shards: usize,
    pub eviction_config: E::Config,
    pub object_pool_capacity: usize,
    pub hash_builder: S,
    pub event_listener: L,
}

#[expect(clippy::type_complexity)]
pub enum GenericEntry<K, V, H, E, I, L, S, ER>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
    ER: std::error::Error,
{
    Invalid,
    Hit(GenericCacheEntry<K, V, H, E, I, L, S>),
    Wait(oneshot::Receiver<GenericCacheEntry<K, V, H, E, I, L, S>>),
    Miss(JoinHandle<std::result::Result<GenericCacheEntry<K, V, H, E, I, L, S>, ER>>),
}

impl<K, V, H, E, I, L, S, ER> Default for GenericEntry<K, V, H, E, I, L, S, ER>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
    ER: std::error::Error,
{
    fn default() -> Self {
        Self::Invalid
    }
}

impl<K, V, H, E, I, L, S, ER> Future for GenericEntry<K, V, H, E, I, L, S, ER>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
    ER: std::error::Error + From<oneshot::error::RecvError>,
{
    type Output = std::result::Result<GenericCacheEntry<K, V, H, E, I, L, S>, ER>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match &mut *self {
            Self::Invalid => unreachable!(),
            Self::Hit(_) => std::task::Poll::Ready(Ok(match std::mem::take(&mut *self) {
                GenericEntry::Hit(entry) => entry,
                _ => unreachable!(),
            })),
            Self::Wait(waiter) => waiter.poll_unpin(cx).map_err(|err| err.into()),
            Self::Miss(join_handle) => join_handle.poll_unpin(cx).map(|join_result| join_result.unwrap()),
        }
    }
}

#[expect(clippy::type_complexity)]
pub struct GenericCache<K, V, H, E, I, L, S = RandomState>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    shards: Vec<Mutex<CacheShard<K, V, H, E, I, L, S>>>,

    capacity: usize,
    usages: Vec<Arc<AtomicUsize>>,

    context: Arc<CacheSharedState<H, L>>,

    hash_builder: S,
}

impl<K, V, H, E, I, L, S> GenericCache<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn new(config: CacheConfig<E, L, S>) -> Self {
        let usages = (0..config.shards).map(|_| Arc::new(AtomicUsize::new(0))).collect_vec();
        let context = Arc::new(CacheSharedState {
            metrics: Metrics::default(),
            object_pool: ArrayQueue::new(config.object_pool_capacity),
            listener: config.event_listener,
        });

        let shard_capacity = config.capacity / config.shards;

        let shards = usages
            .iter()
            .map(|usage| CacheShard::new(shard_capacity, &config.eviction_config, usage.clone(), context.clone()))
            .map(Mutex::new)
            .collect_vec();

        Self {
            shards,
            capacity: config.capacity,
            usages,
            context,
            hash_builder: config.hash_builder,
        }
    }

    pub fn insert(self: &Arc<Self>, key: K, value: V, charge: usize) -> GenericCacheEntry<K, V, H, E, I, L, S> {
        self.insert_with_context(key, value, charge, CacheContext::default())
    }

    pub fn insert_with_context(
        self: &Arc<Self>,
        key: K,
        value: V,
        charge: usize,
        context: CacheContext,
    ) -> GenericCacheEntry<K, V, H, E, I, L, S> {
        let hash = self.hash_builder.hash_one(&key);

        let mut to_deallocate = vec![];

        let (entry, waiters) = unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            let waiters = shard.waiters.remove(&key);
            let mut ptr = shard.insert(hash, key, value, charge, context.into(), &mut to_deallocate);
            if let Some(waiters) = waiters.as_ref() {
                ptr.as_mut().base_mut().inc_refs_by(waiters.len());
            }
            let entry = GenericCacheEntry {
                cache: self.clone(),
                ptr,
            };
            (entry, waiters)
        };

        if let Some(waiters) = waiters {
            for waiter in waiters {
                let _ = waiter.send(GenericCacheEntry {
                    cache: self.clone(),
                    ptr: entry.ptr,
                });
            }
        }

        // Do not deallocate data within the lock section.
        for (key, value, context, charges) in to_deallocate {
            self.context.listener.on_release(key, value, context.into(), charges)
        }

        entry
    }

    pub fn remove(&self, key: &K) {
        let hash = self.hash_builder.hash_one(key);

        let entry = unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.remove(hash, key)
        };

        // Do not deallocate data within the lock section.
        if let Some((key, value, context, charges)) = entry {
            self.context.listener.on_release(key, value, context.into(), charges);
        }
    }

    pub fn get(self: &Arc<Self>, key: &K) -> Option<GenericCacheEntry<K, V, H, E, I, L, S>> {
        let hash = self.hash_builder.hash_one(key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.get(hash, key).map(|ptr| GenericCacheEntry {
                cache: self.clone(),
                ptr,
            })
        }
    }

    pub fn clear(&self) {
        let mut to_deallocate = vec![];
        for shard in self.shards.iter() {
            let mut shard = shard.lock();
            unsafe { shard.clear(&mut to_deallocate) };
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn usage(&self) -> usize {
        self.usages.iter().map(|usage| usage.load(Ordering::Relaxed)).sum()
    }

    pub fn metrics(&self) -> &Metrics {
        &self.context.metrics
    }

    unsafe fn try_release_external_handle(&self, ptr: NonNull<H>) {
        let entry = {
            let base = ptr.as_ref().base();
            let mut shard = self.shards[base.hash() as usize % self.shards.len()].lock();
            shard.try_release_external_handle(ptr)
        };

        // Do not deallocate data within the lock section.
        if let Some((key, value, context, charges)) = entry {
            self.context.listener.on_release(key, value, context.into(), charges);
        }
    }
}

// TODO(MrCroxx): use `hashbrown::HashTable` with `Handle` may relax the `Clone` bound?
impl<K, V, H, E, I, L, S> GenericCache<K, V, H, E, I, L, S>
where
    K: Key + Clone,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn entry<F, FU, ER>(self: &Arc<Self>, key: K, f: F) -> GenericEntry<K, V, H, E, I, L, S, ER>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<(V, usize, CacheContext), ER>> + Send + 'static,
        ER: std::error::Error + Send + 'static,
    {
        let hash = self.hash_builder.hash_one(&key);

        unsafe {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            if let Some(ptr) = shard.get(hash, &key) {
                return GenericEntry::Hit(GenericCacheEntry {
                    cache: self.clone(),
                    ptr,
                });
            }
            let entry = match shard.waiters.entry(key.clone()) {
                HashMapEntry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().push(tx);
                    GenericEntry::Wait(rx)
                }
                HashMapEntry::Vacant(v) => {
                    v.insert(vec![]);
                    let cache = self.clone();
                    let future = f();
                    let join = tokio::spawn(async move {
                        let (value, charge, context) = match future.await {
                            Ok((value, charge, context)) => (value, charge, context),
                            Err(e) => {
                                let mut shard = cache.shards[hash as usize % cache.shards.len()].lock();
                                shard.waiters.remove(&key);
                                return Err(e);
                            }
                        };
                        let entry = cache.insert_with_context(key, value, charge, context);
                        Ok(entry)
                    });
                    GenericEntry::Miss(join)
                }
            };
            match entry {
                GenericEntry::Wait(_) => shard.state.metrics.queue.fetch_add(1, Ordering::Relaxed),
                GenericEntry::Miss(_) => shard.state.metrics.fetch.fetch_add(1, Ordering::Relaxed),
                _ => unreachable!(),
            };
            entry
        }
    }
}

pub struct GenericCacheEntry<K, V, H, E, I, L, S = RandomState>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    cache: Arc<GenericCache<K, V, H, E, I, L, S>>,
    ptr: NonNull<H>,
}

impl<K, V, H, E, I, L, S> GenericCacheEntry<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    pub fn key(&self) -> &H::Key {
        unsafe { self.ptr.as_ref().base().key() }
    }

    pub fn value(&self) -> &H::Value {
        unsafe { self.ptr.as_ref().base().value() }
    }

    pub fn context(&self) -> &H::Context {
        unsafe { self.ptr.as_ref().base().context() }
    }

    pub fn charge(&self) -> usize {
        unsafe { self.ptr.as_ref().base().charge() }
    }

    pub fn refs(&self) -> usize {
        unsafe { self.ptr.as_ref().base().refs() }
    }
}

impl<K, V, H, E, I, L, S> Clone for GenericCacheEntry<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        let mut ptr = self.ptr;

        unsafe {
            let base = ptr.as_mut().base_mut();
            debug_assert!(base.has_refs());
            base.inc_refs();
        }

        Self {
            cache: self.cache.clone(),
            ptr,
        }
    }
}

impl<K, V, H, E, I, L, S> Drop for GenericCacheEntry<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    fn drop(&mut self) {
        unsafe { self.cache.try_release_external_handle(self.ptr) }
    }
}

impl<K, V, H, E, I, L, S> Deref for GenericCacheEntry<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<K, V, H, E, I, L, S> Send for GenericCacheEntry<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
}
unsafe impl<K, V, H, E, I, L, S> Sync for GenericCacheEntry<K, V, H, E, I, L, S>
where
    K: Key,
    V: Value,
    H: Handle<Key = K, Value = V>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = K, Handle = H>,
    L: CacheEventListener<K, V>,
    S: BuildHasher + Send + Sync + 'static,
{
}

#[cfg(test)]
mod tests {
    use rand::{rngs::SmallRng, RngCore, SeedableRng};

    use super::*;
    use crate::{
        cache::{FifoCache, FifoCacheConfig, FifoCacheEntry, LruCache, LruCacheConfig, LruCacheEntry},
        eviction::{
            fifo::{FifoConfig, FifoHandle},
            lru::LruConfig,
            test_utils::TestEviction,
        },
        listener::DefaultCacheEventListener,
    };

    fn is_send_sync_static<T: Send + Sync + 'static>() {}

    #[test]
    fn test_send_sync_static() {
        is_send_sync_static::<FifoCache<(), ()>>();
        is_send_sync_static::<FifoCacheConfig<(), ()>>();
        is_send_sync_static::<LruCache<(), ()>>();
        is_send_sync_static::<LruCacheConfig<(), ()>>();
    }

    #[test]
    fn test_cache_fuzzy() {
        const CAPACITY: usize = 256;

        let config = FifoCacheConfig {
            capacity: CAPACITY,
            shards: 4,
            eviction_config: FifoConfig {},
            object_pool_capacity: 16,
            hash_builder: RandomState::default(),
            event_listener: DefaultCacheEventListener::default(),
        };
        let cache = Arc::new(FifoCache::<u64, u64>::new(config));

        let mut rng = SmallRng::seed_from_u64(114514);
        for _ in 0..100000 {
            let key = rng.next_u64();
            if let Some(entry) = cache.get(&key) {
                assert_eq!(key, *entry);
                drop(entry);
                continue;
            }
            cache.insert(key, key, 1);
        }
        assert_eq!(cache.usage(), CAPACITY);
    }

    fn fifo(capacity: usize) -> Arc<FifoCache<u64, String>> {
        let config = FifoCacheConfig {
            capacity,
            shards: 1,
            eviction_config: FifoConfig {},
            object_pool_capacity: 1,
            hash_builder: RandomState::default(),
            event_listener: DefaultCacheEventListener::default(),
        };
        Arc::new(FifoCache::<u64, String>::new(config))
    }

    fn lru(capacity: usize) -> Arc<LruCache<u64, String>> {
        let config = LruCacheConfig {
            capacity,
            shards: 1,
            eviction_config: LruConfig {
                high_priority_pool_ratio: 0.0,
            },
            object_pool_capacity: 1,
            hash_builder: RandomState::default(),
            event_listener: DefaultCacheEventListener::default(),
        };
        Arc::new(LruCache::<u64, String>::new(config))
    }

    fn insert_fifo(cache: &Arc<FifoCache<u64, String>>, key: u64, value: &str) -> FifoCacheEntry<u64, String> {
        cache.insert(key, value.to_string(), value.len())
    }

    fn insert_lru(cache: &Arc<LruCache<u64, String>>, key: u64, value: &str) -> LruCacheEntry<u64, String> {
        cache.insert(key, value.to_string(), value.len())
    }

    #[test]
    fn test_reference_count() {
        let cache = fifo(100);

        let refs = |ptr: NonNull<FifoHandle<u64, String>>| unsafe { ptr.as_ref().base().refs() };

        let e1 = insert_fifo(&cache, 42, "the answer to life, the universe, and everything");
        let ptr = e1.ptr;
        assert_eq!(refs(ptr), 1);

        let e2 = cache.get(&42).unwrap();
        assert_eq!(refs(ptr), 2);

        let e3 = e2.clone();
        assert_eq!(refs(ptr), 3);

        drop(e2);
        assert_eq!(refs(ptr), 2);

        drop(e3);
        assert_eq!(refs(ptr), 1);

        drop(e1);
        assert_eq!(refs(ptr), 0);
    }

    #[test]
    fn test_replace() {
        let cache = fifo(10);

        insert_fifo(&cache, 114, "xx");
        assert_eq!(cache.usage(), 2);

        insert_fifo(&cache, 514, "QwQ");
        assert_eq!(cache.usage(), 5);

        insert_fifo(&cache, 114, "(0.0)");
        assert_eq!(cache.usage(), 8);

        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(514, "QwQ".to_string()), (114, "(0.0)".to_string())],
        );
    }

    #[test]
    fn test_replace_with_external_refs() {
        let cache = fifo(10);

        insert_fifo(&cache, 514, "QwQ");
        insert_fifo(&cache, 114, "(0.0)");

        let e4 = cache.get(&514).unwrap();
        let e5 = insert_fifo(&cache, 514, "bili");

        assert_eq!(e4.refs(), 1);
        assert_eq!(e5.refs(), 1);

        // remains: 514 => QwQ (3), 514 => bili (4)
        // evicted: 114 => (0.0) (5)
        assert_eq!(cache.usage(), 7);

        assert!(cache.get(&114).is_none());
        assert_eq!(cache.get(&514).unwrap().value(), "bili");
        assert_eq!(e4.value(), "QwQ");

        cache.remove(&514);
        assert_eq!(e5.value(), "bili");

        drop(e5);
        assert!(cache.get(&514).is_none());
        assert_eq!(e4.value(), "QwQ");

        assert_eq!(cache.usage(), 3);
        drop(e4);
        assert_eq!(cache.usage(), 0);
    }

    #[test]
    fn test_reinsert_while_all_referenced_lru() {
        let cache = lru(10);

        let e1 = insert_lru(&cache, 1, "111");
        let e2 = insert_lru(&cache, 2, "222");
        let e3 = insert_lru(&cache, 3, "333");
        assert_eq!(cache.usage(), 9);

        // No entry will be released because all of them are referenced externally.
        let e4 = insert_lru(&cache, 4, "444");
        assert_eq!(cache.usage(), 12);

        // `111`, `222` and `333` are evicted from the eviction container to make space for `444`.
        assert_eq!(cache.shards[0].lock().eviction.dump(), vec![(4, "444".to_string()),]);

        // `e1` cannot be reinserted for the usage has already exceeds the capacity.
        drop(e1);
        assert_eq!(cache.usage(), 9);

        // `222` and `333` will be reinserted
        drop(e2);
        drop(e3);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(4, "444".to_string()), (2, "222".to_string()), (3, "333".to_string()),]
        );
        assert_eq!(cache.usage(), 9);

        // `444` will be reinserted
        drop(e4);
        assert_eq!(
            cache.shards[0].lock().eviction.dump(),
            vec![(2, "222".to_string()), (3, "333".to_string()), (4, "444".to_string()),]
        );
        assert_eq!(cache.usage(), 9);
    }

    #[test]
    fn test_reinsert_while_all_referenced_fifo() {
        let cache = fifo(10);

        let e1 = insert_fifo(&cache, 1, "111");
        let e2 = insert_fifo(&cache, 2, "222");
        let e3 = insert_fifo(&cache, 3, "333");
        assert_eq!(cache.usage(), 9);

        // No entry will be released because all of them are referenced externally.
        let e4 = insert_fifo(&cache, 4, "444");
        assert_eq!(cache.usage(), 12);

        // `111`, `222` and `333` are evicted from the eviction container to make space for `444`.
        assert_eq!(cache.shards[0].lock().eviction.dump(), vec![(4, "444".to_string()),]);

        // `e1` cannot be reinserted for the usage has already exceeds the capacity.
        drop(e1);
        assert_eq!(cache.usage(), 9);

        // `222` and `333` will be not reinserted because fifo will ignore reinsert operations.
        drop([e2, e3, e4]);
        assert_eq!(cache.shards[0].lock().eviction.dump(), vec![(4, "444".to_string()),]);
        assert_eq!(cache.usage(), 3);

        // Note:
        //
        // For cache policy like FIFO, the entries will not be reinserted while all handles are referenced.
        // It's okay for this is not a common situation and is not supposed to happen in real workload.
    }
}
