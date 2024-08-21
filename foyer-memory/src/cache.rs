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

use std::{borrow::Borrow, fmt::Debug, hash::Hash, ops::Deref, sync::Arc};

use ahash::RandomState;
use futures::Future;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use foyer_common::{
    code::{HashBuilder, Key, Value},
    event::EventListener,
    future::Diversion,
};

use crate::{
    cleaner::CleanerEventListener,
    context::CacheContext,
    eviction::{
        fifo::{Fifo, FifoHandle},
        lfu::{Lfu, LfuHandle},
        lru::{Lru, LruHandle},
        s3fifo::{S3Fifo, S3FifoHandle},
        sanity::SanityEviction,
    },
    generic::{FetchMark, FetchState, GenericCache, GenericCacheConfig, GenericCacheEntry, GenericFetch, Weighter},
    indexer::{hash_table::HashTableIndexer, sanity::SanityIndexer},
    FifoConfig, LfuConfig, LruConfig, S3FifoConfig,
};

pub type FifoCache<K, V, S = RandomState> =
    GenericCache<K, V, SanityEviction<Fifo<(K, V)>>, SanityIndexer<HashTableIndexer<K, FifoHandle<(K, V)>>>, S>;
pub type FifoCacheEntry<K, V, S = RandomState> =
    GenericCacheEntry<K, V, SanityEviction<Fifo<(K, V)>>, SanityIndexer<HashTableIndexer<K, FifoHandle<(K, V)>>>, S>;
pub type FifoFetch<K, V, ER, S = RandomState> =
    GenericFetch<K, V, SanityEviction<Fifo<(K, V)>>, SanityIndexer<HashTableIndexer<K, FifoHandle<(K, V)>>>, S, ER>;

pub type LruCache<K, V, S = RandomState> =
    GenericCache<K, V, SanityEviction<Lru<(K, V)>>, SanityIndexer<HashTableIndexer<K, LruHandle<(K, V)>>>, S>;
pub type LruCacheEntry<K, V, S = RandomState> =
    GenericCacheEntry<K, V, SanityEviction<Lru<(K, V)>>, SanityIndexer<HashTableIndexer<K, LruHandle<(K, V)>>>, S>;
pub type LruFetch<K, V, ER, S = RandomState> =
    GenericFetch<K, V, SanityEviction<Lru<(K, V)>>, SanityIndexer<HashTableIndexer<K, LruHandle<(K, V)>>>, S, ER>;

pub type LfuCache<K, V, S = RandomState> =
    GenericCache<K, V, SanityEviction<Lfu<(K, V)>>, SanityIndexer<HashTableIndexer<K, LfuHandle<(K, V)>>>, S>;
pub type LfuCacheEntry<K, V, S = RandomState> =
    GenericCacheEntry<K, V, SanityEviction<Lfu<(K, V)>>, SanityIndexer<HashTableIndexer<K, LfuHandle<(K, V)>>>, S>;
pub type LfuFetch<K, V, ER, S = RandomState> =
    GenericFetch<K, V, SanityEviction<Lfu<(K, V)>>, SanityIndexer<HashTableIndexer<K, LfuHandle<(K, V)>>>, S, ER>;

pub type S3FifoCache<K, V, S = RandomState> =
    GenericCache<K, V, SanityEviction<S3Fifo<(K, V)>>, SanityIndexer<HashTableIndexer<K, S3FifoHandle<(K, V)>>>, S>;
pub type S3FifoCacheEntry<K, V, S = RandomState> = GenericCacheEntry<
    K,
    V,
    SanityEviction<S3Fifo<(K, V)>>,
    SanityIndexer<HashTableIndexer<K, S3FifoHandle<(K, V)>>>,
    S,
>;
pub type S3FifoFetch<K, V, ER, S = RandomState> =
    GenericFetch<K, V, SanityEviction<S3Fifo<(K, V)>>, SanityIndexer<HashTableIndexer<K, S3FifoHandle<(K, V)>>>, S, ER>;

/// A cached entry holder of the in-memory cache.
#[derive(Debug)]
pub enum CacheEntry<K, V, S = RandomState>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// A cached entry holder of the in-memory FIFO cache.
    Fifo(FifoCacheEntry<K, V, S>),
    /// A cached entry holder of the in-memory LRU cache.
    Lru(LruCacheEntry<K, V, S>),
    /// A cached entry holder of the in-memory LFU cache.
    Lfu(LfuCacheEntry<K, V, S>),
    /// A cached entry holder of the in-memory S3FIFO cache.
    S3Fifo(S3FifoCacheEntry<K, V, S>),
}

impl<K, V, S> Clone for CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(entry) => Self::Fifo(entry.clone()),
            Self::Lru(entry) => Self::Lru(entry.clone()),
            Self::Lfu(entry) => Self::Lfu(entry.clone()),
            Self::S3Fifo(entry) => Self::S3Fifo(entry.clone()),
        }
    }
}

impl<K, V, S> Deref for CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match self {
            CacheEntry::Fifo(entry) => entry.deref(),
            CacheEntry::Lru(entry) => entry.deref(),
            CacheEntry::Lfu(entry) => entry.deref(),
            CacheEntry::S3Fifo(entry) => entry.deref(),
        }
    }
}

impl<K, V, S> From<FifoCacheEntry<K, V, S>> for CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: FifoCacheEntry<K, V, S>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, S> From<LruCacheEntry<K, V, S>> for CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: LruCacheEntry<K, V, S>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, S> From<LfuCacheEntry<K, V, S>> for CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: LfuCacheEntry<K, V, S>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, S> From<S3FifoCacheEntry<K, V, S>> for CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: S3FifoCacheEntry<K, V, S>) -> Self {
        Self::S3Fifo(entry)
    }
}

impl<K, V, S> CacheEntry<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// Key hash of the cached entry.
    pub fn hash(&self) -> u64 {
        match self {
            CacheEntry::Fifo(entry) => entry.hash(),
            CacheEntry::Lru(entry) => entry.hash(),
            CacheEntry::Lfu(entry) => entry.hash(),
            CacheEntry::S3Fifo(entry) => entry.hash(),
        }
    }

    /// Key of the cached entry.
    pub fn key(&self) -> &K {
        match self {
            CacheEntry::Fifo(entry) => entry.key(),
            CacheEntry::Lru(entry) => entry.key(),
            CacheEntry::Lfu(entry) => entry.key(),
            CacheEntry::S3Fifo(entry) => entry.key(),
        }
    }

    /// Value of the cached entry.
    pub fn value(&self) -> &V {
        match self {
            CacheEntry::Fifo(entry) => entry.value(),
            CacheEntry::Lru(entry) => entry.value(),
            CacheEntry::Lfu(entry) => entry.value(),
            CacheEntry::S3Fifo(entry) => entry.value(),
        }
    }

    /// Context of the cached entry.
    pub fn context(&self) -> CacheContext {
        match self {
            CacheEntry::Fifo(entry) => entry.context().clone().into(),
            CacheEntry::Lru(entry) => entry.context().clone().into(),
            CacheEntry::Lfu(entry) => entry.context().clone().into(),
            CacheEntry::S3Fifo(entry) => entry.context().clone().into(),
        }
    }

    /// Weight of the cached entry.
    pub fn weight(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.weight(),
            CacheEntry::Lru(entry) => entry.weight(),
            CacheEntry::Lfu(entry) => entry.weight(),
            CacheEntry::S3Fifo(entry) => entry.weight(),
        }
    }

    /// External reference count of the cached entry.
    pub fn refs(&self) -> usize {
        match self {
            CacheEntry::Fifo(entry) => entry.refs(),
            CacheEntry::Lru(entry) => entry.refs(),
            CacheEntry::Lfu(entry) => entry.refs(),
            CacheEntry::S3Fifo(entry) => entry.refs(),
        }
    }

    /// If the cached entry is updated and outdated.
    pub fn is_outdated(&self) -> bool {
        match self {
            CacheEntry::Fifo(entry) => entry.is_outdated(),
            CacheEntry::Lru(entry) => entry.is_outdated(),
            CacheEntry::Lfu(entry) => entry.is_outdated(),
            CacheEntry::S3Fifo(entry) => entry.is_outdated(),
        }
    }
}

/// Eviction algorithm config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionConfig {
    /// FIFO eviction algorithm config.
    Fifo(FifoConfig),
    /// LRU eviction algorithm config.
    Lru(LruConfig),
    /// LFU eviction algorithm config.
    Lfu(LfuConfig),
    /// S3FIFO eviction algorithm config.
    S3Fifo(S3FifoConfig),
}

impl From<FifoConfig> for EvictionConfig {
    fn from(value: FifoConfig) -> EvictionConfig {
        EvictionConfig::Fifo(value)
    }
}

impl From<LruConfig> for EvictionConfig {
    fn from(value: LruConfig) -> EvictionConfig {
        EvictionConfig::Lru(value)
    }
}

impl From<LfuConfig> for EvictionConfig {
    fn from(value: LfuConfig) -> EvictionConfig {
        EvictionConfig::Lfu(value)
    }
}

impl From<S3FifoConfig> for EvictionConfig {
    fn from(value: S3FifoConfig) -> EvictionConfig {
        EvictionConfig::S3Fifo(value)
    }
}

/// In-memory cache builder.
pub struct CacheBuilder<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    name: String,

    capacity: usize,
    shards: usize,
    eviction_config: EvictionConfig,
    object_pool_capacity: usize,

    hash_builder: S,
    weighter: Arc<dyn Weighter<K, V>>,

    event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
}

impl<K, V> CacheBuilder<K, V, RandomState>
where
    K: Key,
    V: Value,
{
    /// Create a new in-memory cache builder.
    pub fn new(capacity: usize) -> Self {
        Self {
            name: "foyer".to_string(),

            capacity,
            shards: 8,
            eviction_config: LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            }
            .into(),
            object_pool_capacity: 1024,
            hash_builder: RandomState::default(),
            weighter: Arc::new(|_, _| 1),
            event_listener: None,
        }
    }
}

impl<K, V, S> CacheBuilder<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// Set the name of the foyer in-memory cache instance.
    ///
    /// Foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set in-memory cache sharding count. Entries will be distributed to different shards based on their hash.
    /// Operations on different shard can be parallelized.
    pub fn with_shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(mut self, eviction_config: impl Into<EvictionConfig>) -> Self {
        self.eviction_config = eviction_config.into();
        self
    }

    /// Set object pool for handles. The object pool is used to reduce handle allocation.
    ///
    /// The optimized value is supposed to be equal to the max cache entry count.
    ///
    /// The default value is 1024.
    pub fn with_object_pool_capacity(mut self, object_pool_capacity: usize) -> Self {
        self.object_pool_capacity = object_pool_capacity;
        self
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> CacheBuilder<K, V, OS>
    where
        OS: HashBuilder,
    {
        CacheBuilder {
            name: self.name,
            capacity: self.capacity,
            shards: self.shards,
            eviction_config: self.eviction_config,
            object_pool_capacity: self.object_pool_capacity,
            hash_builder,
            weighter: self.weighter,
            event_listener: self.event_listener,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(mut self, weighter: impl Weighter<K, V>) -> Self {
        self.weighter = Arc::new(weighter);
        self
    }

    /// Set event listener.
    pub fn with_event_listener(mut self, event_listener: Arc<dyn EventListener<Key = K, Value = V>>) -> Self {
        if self.event_listener.is_some() {
            tracing::warn!(
                "[cache]: The event listener can only be set once. The original event listener is overwritten."
            );
        }
        self.event_listener = Some(event_listener);
        self
    }

    /// Enable cleaner. Cleaner is a dedicated thread for clean the memory-released entries.
    ///
    /// Note: This must be set after the event listener is set.
    pub fn with_cleaner(mut self, threads: usize) -> Self {
        assert!(threads > 0, "cleaner threads must be larger than 0, given: {threads}");
        self.event_listener = Some(Arc::new(CleanerEventListener::new(threads, self.event_listener.take())));
        self
    }

    /// Build in-memory cache with the given configuration.
    pub fn build(self) -> Cache<K, V, S> {
        match self.eviction_config {
            EvictionConfig::Fifo(eviction_config) => Cache::Fifo(Arc::new(GenericCache::new(GenericCacheConfig {
                name: self.name,
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
            }))),
            EvictionConfig::Lru(eviction_config) => Cache::Lru(Arc::new(GenericCache::new(GenericCacheConfig {
                name: self.name,
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
            }))),
            EvictionConfig::Lfu(eviction_config) => Cache::Lfu(Arc::new(GenericCache::new(GenericCacheConfig {
                name: self.name,
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
            }))),
            EvictionConfig::S3Fifo(eviction_config) => Cache::S3Fifo(Arc::new(GenericCache::new(GenericCacheConfig {
                name: self.name,
                capacity: self.capacity,
                shards: self.shards,
                eviction_config,
                object_pool_capacity: self.object_pool_capacity,
                hash_builder: self.hash_builder,
                weighter: self.weighter,
                event_listener: self.event_listener,
            }))),
        }
    }
}

/// In-memory cache with plug-and-play algorithms.
pub enum Cache<K, V, S = RandomState>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// In-memory FIFO cache.
    Fifo(Arc<FifoCache<K, V, S>>),
    /// In-memory LRU cache.
    Lru(Arc<LruCache<K, V, S>>),
    /// In-memory LFU cache.
    Lfu(Arc<LfuCache<K, V, S>>),
    /// In-memory S3FIFO cache.
    S3Fifo(Arc<S3FifoCache<K, V, S>>),
}

impl<K, V, S> Debug for Cache<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fifo(_) => f.debug_tuple("Cache::FifoCache").finish(),
            Self::Lru(_) => f.debug_tuple("Cache::LruCache").finish(),
            Self::Lfu(_) => f.debug_tuple("Cache::LfuCache").finish(),
            Self::S3Fifo(_) => f.debug_tuple("Cache::S3FifoCache").finish(),
        }
    }
}

impl<K, V, S> Clone for Cache<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn clone(&self) -> Self {
        match self {
            Self::Fifo(cache) => Self::Fifo(cache.clone()),
            Self::Lru(cache) => Self::Lru(cache.clone()),
            Self::Lfu(cache) => Self::Lfu(cache.clone()),
            Self::S3Fifo(cache) => Self::S3Fifo(cache.clone()),
        }
    }
}

impl<K, V, S> Cache<K, V, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// Insert cache entry to the in-memory cache.
    #[fastrace::trace(name = "foyer::memory::cache::insert")]
    pub fn insert(&self, key: K, value: V) -> CacheEntry<K, V, S> {
        match self {
            Cache::Fifo(cache) => cache.insert(key, value).into(),
            Cache::Lru(cache) => cache.insert(key, value).into(),
            Cache::Lfu(cache) => cache.insert(key, value).into(),
            Cache::S3Fifo(cache) => cache.insert(key, value).into(),
        }
    }

    /// Insert cache entry with cache context to the in-memory cache.
    #[fastrace::trace(name = "foyer::memory::cache::insert_with_context")]
    pub fn insert_with_context(&self, key: K, value: V, context: CacheContext) -> CacheEntry<K, V, S> {
        match self {
            Cache::Fifo(cache) => cache.insert_with_context(key, value, context).into(),
            Cache::Lru(cache) => cache.insert_with_context(key, value, context).into(),
            Cache::Lfu(cache) => cache.insert_with_context(key, value, context).into(),
            Cache::S3Fifo(cache) => cache.insert_with_context(key, value, context).into(),
        }
    }

    /// Temporarily insert cache entry to the in-memory cache.
    ///
    /// The entry will be removed as soon as the returned entry is dropped.
    ///
    /// The entry will become a normal entry after it is accessed.
    #[fastrace::trace(name = "foyer::memory::cache::deposit")]
    pub fn deposit(&self, key: K, value: V) -> CacheEntry<K, V, S> {
        match self {
            Cache::Fifo(cache) => cache.deposit(key, value).into(),
            Cache::Lru(cache) => cache.deposit(key, value).into(),
            Cache::Lfu(cache) => cache.deposit(key, value).into(),
            Cache::S3Fifo(cache) => cache.deposit(key, value).into(),
        }
    }

    /// Temporarily insert cache entry with cache context to the in-memory cache.
    ///
    /// The entry will be removed as soon as the returned entry is dropped.
    ///
    /// The entry will become a normal entry after it is accessed.
    #[fastrace::trace(name = "foyer::memory::cache::deposit_with_context")]
    pub fn deposit_with_context(&self, key: K, value: V, context: CacheContext) -> CacheEntry<K, V, S> {
        match self {
            Cache::Fifo(cache) => cache.deposit_with_context(key, value, context).into(),
            Cache::Lru(cache) => cache.deposit_with_context(key, value, context).into(),
            Cache::Lfu(cache) => cache.deposit_with_context(key, value, context).into(),
            Cache::S3Fifo(cache) => cache.deposit_with_context(key, value, context).into(),
        }
    }

    /// Remove a cached entry with the given key from the in-memory cache.
    #[fastrace::trace(name = "foyer::memory::cache::remove")]
    pub fn remove<Q>(&self, key: &Q) -> Option<CacheEntry<K, V, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.remove(key).map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.remove(key).map(CacheEntry::from),
        }
    }

    /// Get cached entry with the given key from the in-memory cache.
    #[fastrace::trace(name = "foyer::memory::cache::get")]
    pub fn get<Q>(&self, key: &Q) -> Option<CacheEntry<K, V, S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lru(cache) => cache.get(key).map(CacheEntry::from),
            Cache::Lfu(cache) => cache.get(key).map(CacheEntry::from),
            Cache::S3Fifo(cache) => cache.get(key).map(CacheEntry::from),
        }
    }

    /// Check if the in-memory cache contains a cached entry with the given key.
    #[fastrace::trace(name = "foyer::memory::cache::contains")]
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.contains(key),
            Cache::Lru(cache) => cache.contains(key),
            Cache::Lfu(cache) => cache.contains(key),
            Cache::S3Fifo(cache) => cache.contains(key),
        }
    }

    /// Access the cached entry with the given key but don't return.
    ///
    /// Note: This method can be used to update the cache eviction information and order based on the algorithm.
    #[fastrace::trace(name = "foyer::memory::cache::touch")]
    pub fn touch<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Cache::Fifo(cache) => cache.touch(key),
            Cache::Lru(cache) => cache.touch(key),
            Cache::Lfu(cache) => cache.touch(key),
            Cache::S3Fifo(cache) => cache.touch(key),
        }
    }

    /// Clear the in-memory cache.
    #[fastrace::trace(name = "foyer::memory::cache::clear")]
    pub fn clear(&self) {
        match self {
            Cache::Fifo(cache) => cache.clear(),
            Cache::Lru(cache) => cache.clear(),
            Cache::Lfu(cache) => cache.clear(),
            Cache::S3Fifo(cache) => cache.clear(),
        }
    }

    /// Get the capacity of the in-memory cache.
    pub fn capacity(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.capacity(),
            Cache::Lru(cache) => cache.capacity(),
            Cache::Lfu(cache) => cache.capacity(),
            Cache::S3Fifo(cache) => cache.capacity(),
        }
    }

    /// Get the usage of the in-memory cache.
    pub fn usage(&self) -> usize {
        match self {
            Cache::Fifo(cache) => cache.usage(),
            Cache::Lru(cache) => cache.usage(),
            Cache::Lfu(cache) => cache.usage(),
            Cache::S3Fifo(cache) => cache.usage(),
        }
    }

    /// Hash the given key with the hash builder of the cache.
    pub fn hash<Q>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        self.hash_builder().hash_one(key)
    }

    /// Get the hash builder of the in-memory cache.
    fn hash_builder(&self) -> &S {
        match self {
            Cache::Fifo(cache) => cache.hash_builder(),
            Cache::Lru(cache) => cache.hash_builder(),
            Cache::Lfu(cache) => cache.hash_builder(),
            Cache::S3Fifo(cache) => cache.hash_builder(),
        }
    }
}

/// A future that is used to get entry value from the remote storage for the in-memory cache.
#[pin_project(project = FetchProj)]
pub enum Fetch<K, V, ER, S = RandomState>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// A future that is used to get entry value from the remote storage for the in-memory FIFO cache.
    Fifo(#[pin] FifoFetch<K, V, ER, S>),
    /// A future that is used to get entry value from the remote storage for the in-memory LRU cache.
    Lru(#[pin] LruFetch<K, V, ER, S>),
    /// A future that is used to get entry value from the remote storage for the in-memory LFU cache.
    Lfu(#[pin] LfuFetch<K, V, ER, S>),
    /// A future that is used to get entry value from the remote storage for the in-memory S3FIFO cache.
    S3Fifo(#[pin] S3FifoFetch<K, V, ER, S>),
}

impl<K, V, ER, S> From<FifoFetch<K, V, ER, S>> for Fetch<K, V, ER, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: FifoFetch<K, V, ER, S>) -> Self {
        Self::Fifo(entry)
    }
}

impl<K, V, ER, S> From<LruFetch<K, V, ER, S>> for Fetch<K, V, ER, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: LruFetch<K, V, ER, S>) -> Self {
        Self::Lru(entry)
    }
}

impl<K, V, ER, S> From<LfuFetch<K, V, ER, S>> for Fetch<K, V, ER, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: LfuFetch<K, V, ER, S>) -> Self {
        Self::Lfu(entry)
    }
}

impl<K, V, ER, S> From<S3FifoFetch<K, V, ER, S>> for Fetch<K, V, ER, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    fn from(entry: S3FifoFetch<K, V, ER, S>) -> Self {
        Self::S3Fifo(entry)
    }
}

impl<K, V, ER, S> Future for Fetch<K, V, ER, S>
where
    K: Key,
    V: Value,
    ER: From<oneshot::error::RecvError>,
    S: HashBuilder,
{
    type Output = std::result::Result<CacheEntry<K, V, S>, ER>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match self.project() {
            FetchProj::Fifo(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
            FetchProj::Lru(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
            FetchProj::Lfu(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
            FetchProj::S3Fifo(entry) => entry.poll(cx).map(|res| res.map(CacheEntry::from)),
        }
    }
}

impl<K, V, ER, S> Fetch<K, V, ER, S>
where
    K: Key,
    V: Value,
    S: HashBuilder,
{
    /// Get the fetch state.
    pub fn state(&self) -> FetchState {
        match self {
            Fetch::Fifo(fetch) => fetch.state(),
            Fetch::Lru(fetch) => fetch.state(),
            Fetch::Lfu(fetch) => fetch.state(),
            Fetch::S3Fifo(fetch) => fetch.state(),
        }
    }

    /// Get the ext of the fetch.
    #[doc(hidden)]
    pub fn store(&self) -> &Option<FetchMark> {
        match self {
            Fetch::Fifo(fetch) => fetch.store(),
            Fetch::Lru(fetch) => fetch.store(),
            Fetch::Lfu(fetch) => fetch.store(),
            Fetch::S3Fifo(fetch) => fetch.store(),
        }
    }
}

impl<K, V, S> Cache<K, V, S>
where
    K: Key + Clone,
    V: Value,
    S: HashBuilder,
{
    /// Get the cached entry with the given key from the in-memory cache.
    ///
    /// Use `fetch` to fetch the cache value from the remote storage on cache miss.
    ///
    /// The concurrent fetch requests will be deduplicated.
    #[fastrace::trace(name = "foyer::memory::cache::fetch")]
    pub fn fetch<F, FU, ER>(&self, key: K, fetch: F) -> Fetch<K, V, ER, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<V, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        match self {
            Cache::Fifo(cache) => Fetch::from(cache.fetch(key, fetch)),
            Cache::Lru(cache) => Fetch::from(cache.fetch(key, fetch)),
            Cache::Lfu(cache) => Fetch::from(cache.fetch(key, fetch)),
            Cache::S3Fifo(cache) => Fetch::from(cache.fetch(key, fetch)),
        }
    }

    /// Get the cached entry with the given key and context from the in-memory cache.
    ///
    /// Use `fetch` to fetch the cache value from the remote storage on cache miss.
    ///
    /// The concurrent fetch requests will be deduplicated.
    #[fastrace::trace(name = "foyer::memory::cache::fetch_with_context")]
    pub fn fetch_with_context<F, FU, ER>(&self, key: K, context: CacheContext, fetch: F) -> Fetch<K, V, ER, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = std::result::Result<V, ER>> + Send + 'static,
        ER: Send + 'static + Debug,
    {
        match self {
            Cache::Fifo(cache) => Fetch::from(cache.fetch_with_context(key, context, fetch)),
            Cache::Lru(cache) => Fetch::from(cache.fetch_with_context(key, context, fetch)),
            Cache::Lfu(cache) => Fetch::from(cache.fetch_with_context(key, context, fetch)),
            Cache::S3Fifo(cache) => Fetch::from(cache.fetch_with_context(key, context, fetch)),
        }
    }

    /// Get the cached entry with the given key from the in-memory cache.
    ///
    /// The fetch task will be spawned in the give `runtime`.
    ///
    /// Use `fetch` to fetch the cache value from the remote storage on cache miss.
    ///
    /// The concurrent fetch requests will be deduplicated.
    #[doc(hidden)]
    pub fn fetch_inner<F, FU, ER, ID>(
        &self,
        key: K,
        context: CacheContext,
        fetch: F,
        runtime: &tokio::runtime::Handle,
    ) -> Fetch<K, V, ER, S>
    where
        F: FnOnce() -> FU,
        FU: Future<Output = ID> + Send + 'static,
        ER: Send + 'static + Debug,
        ID: Into<Diversion<std::result::Result<V, ER>, FetchMark>>,
    {
        match self {
            Cache::Fifo(cache) => Fetch::from(cache.fetch_inner(key, context, fetch, runtime)),
            Cache::Lru(cache) => Fetch::from(cache.fetch_inner(key, context, fetch, runtime)),
            Cache::Lfu(cache) => Fetch::from(cache.fetch_inner(key, context, fetch, runtime)),
            Cache::S3Fifo(cache) => Fetch::from(cache.fetch_inner(key, context, fetch, runtime)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Range, time::Duration};

    use futures::future::join_all;
    use itertools::Itertools;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    use super::*;
    use crate::{eviction::s3fifo::S3FifoConfig, FifoConfig, LfuConfig, LruConfig};

    const CAPACITY: usize = 100;
    const SHARDS: usize = 4;
    const OBJECT_POOL_CAPACITY: usize = 64;
    const RANGE: Range<u64> = 0..1000;
    const OPS: usize = 10000;
    const CONCURRENCY: usize = 8;

    fn fifo() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(FifoConfig {})
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn lru() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(LruConfig {
                high_priority_pool_ratio: 0.1,
            })
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn lfu() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(LfuConfig {
                window_capacity_ratio: 0.1,
                protected_capacity_ratio: 0.8,
                cmsketch_eps: 0.001,
                cmsketch_confidence: 0.9,
            })
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn s3fifo() -> Cache<u64, u64> {
        CacheBuilder::new(CAPACITY)
            .with_shards(SHARDS)
            .with_eviction_config(S3FifoConfig {
                small_queue_capacity_ratio: 0.1,
                ghost_queue_capacity_ratio: 10.0,
                small_to_main_freq_threshold: 2,
            })
            .with_object_pool_capacity(OBJECT_POOL_CAPACITY)
            .build()
    }

    fn init_cache(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let mut v = RANGE.collect_vec();
        v.shuffle(rng);
        for i in v {
            cache.insert(i, i);
        }
    }

    async fn operate(cache: &Cache<u64, u64>, rng: &mut StdRng) {
        let i = rng.gen_range(RANGE);
        match rng.gen_range(0..=3) {
            0 => {
                let entry = cache.insert(i, i);
                assert_eq!(*entry.key(), i);
                assert_eq!(entry.key(), entry.value());
            }
            1 => {
                if let Some(entry) = cache.get(&i) {
                    assert_eq!(*entry.key(), i);
                    assert_eq!(entry.key(), entry.value());
                }
            }
            2 => {
                cache.remove(&i);
            }
            3 => {
                let entry = cache
                    .fetch(i, || async move {
                        tokio::time::sleep(Duration::from_micros(10)).await;
                        Ok::<_, tokio::sync::oneshot::error::RecvError>(i)
                    })
                    .await
                    .unwrap();
                assert_eq!(*entry.key(), i);
                assert_eq!(entry.key(), entry.value());
            }
            _ => unreachable!(),
        }
    }

    async fn case(cache: Cache<u64, u64>) {
        let mut rng = StdRng::seed_from_u64(42);

        init_cache(&cache, &mut rng);

        let handles = (0..CONCURRENCY)
            .map(|_| {
                let cache = cache.clone();
                let mut rng = rng.clone();
                tokio::spawn(async move {
                    for _ in 0..OPS {
                        operate(&cache, &mut rng).await;
                    }
                })
            })
            .collect_vec();

        join_all(handles).await;
    }

    #[tokio::test]
    async fn test_fifo_cache() {
        case(fifo()).await
    }

    #[tokio::test]
    async fn test_lru_cache() {
        case(lru()).await
    }

    #[tokio::test]
    async fn test_lfu_cache() {
        case(lfu()).await
    }

    #[tokio::test]
    async fn test_s3fifo_cache() {
        case(s3fifo()).await
    }

    #[tokio::test]
    async fn test_cache_with_zero_object_pool() {
        case(CacheBuilder::new(8).with_object_pool_capacity(0).build()).await
    }
}
