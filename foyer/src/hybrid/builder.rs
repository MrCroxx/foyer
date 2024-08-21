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

use std::{fmt::Debug, sync::Arc};

use ahash::RandomState;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    event::EventListener,
    tracing::TracingConfig,
};
use foyer_memory::{Cache, CacheBuilder, EvictionConfig, Weighter};
use foyer_storage::{
    AdmissionPicker, Compression, DeviceConfig, EvictionPicker, RecoverMode, ReinsertionPicker, RuntimeConfig,
    StoreBuilder, TombstoneLogConfig,
};

use crate::HybridCache;

/// Hybrid cache builder.
pub struct HybridCacheBuilder<K, V> {
    name: String,
    event_listener: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
    tracing_config: TracingConfig,
}

impl<K, V> Default for HybridCacheBuilder<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> HybridCacheBuilder<K, V> {
    /// Create a new hybrid cache builder.
    pub fn new() -> Self {
        Self {
            name: "foyer".to_string(),
            event_listener: None,
            tracing_config: TracingConfig::default(),
        }
    }

    /// Set the name of the foyer hybrid cache instance.
    ///
    /// Foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set event listener.
    ///
    /// Default: No event listener installed.
    pub fn with_event_listener(mut self, event_listener: Arc<dyn EventListener<Key = K, Value = V>>) -> Self {
        self.event_listener = Some(event_listener);
        self
    }

    /// Set tracing config.
    ///
    /// Default: Only operations over 1s will be recorded.
    pub fn with_tracing_config(mut self, tracing_config: TracingConfig) -> Self {
        self.tracing_config = tracing_config;
        self
    }

    /// Continue to modify the in-memory cache configurations.
    pub fn memory(self, capacity: usize) -> HybridCacheBuilderPhaseMemory<K, V, RandomState>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let mut builder = CacheBuilder::new(capacity).with_name(&self.name);
        if let Some(event_listener) = self.event_listener {
            builder = builder.with_event_listener(event_listener);
        }
        HybridCacheBuilderPhaseMemory {
            builder,
            name: self.name,
            tracing_config: self.tracing_config,
        }
    }
}

/// Hybrid cache builder to modify the in-memory cache configurations.
pub struct HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    name: String,
    tracing_config: TracingConfig,
    builder: CacheBuilder<K, V, S>,
}

impl<K, V, S> HybridCacheBuilderPhaseMemory<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Set in-memory cache sharding count. Entries will be distributed to different shards based on their hash.
    /// Operations on different shard can be parallelized.
    pub fn with_shards(self, shards: usize) -> Self {
        let builder = self.builder.with_shards(shards);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_config: self.tracing_config,
            builder,
        }
    }

    /// Set in-memory cache eviction algorithm.
    ///
    /// The default value is a general-used w-TinyLFU algorithm.
    pub fn with_eviction_config(self, eviction_config: impl Into<EvictionConfig>) -> Self {
        let builder = self.builder.with_eviction_config(eviction_config.into());
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_config: self.tracing_config,
            builder,
        }
    }

    /// Set object pool for handles. The object pool is used to reduce handle allocation.
    ///
    /// The optimized value is supposed to be equal to the max cache entry count.
    ///
    /// The default value is 1024.
    pub fn with_object_pool_capacity(self, object_pool_capacity: usize) -> Self {
        let builder = self.builder.with_object_pool_capacity(object_pool_capacity);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_config: self.tracing_config,
            builder,
        }
    }

    /// Set in-memory cache hash builder.
    pub fn with_hash_builder<OS>(self, hash_builder: OS) -> HybridCacheBuilderPhaseMemory<K, V, OS>
    where
        OS: HashBuilder + Debug,
    {
        let builder = self.builder.with_hash_builder(hash_builder);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_config: self.tracing_config,
            builder,
        }
    }

    /// Set in-memory cache weighter.
    pub fn with_weighter(self, weighter: impl Weighter<K, V>) -> Self {
        let builder = self.builder.with_weighter(weighter);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_config: self.tracing_config,
            builder,
        }
    }

    /// Enable cleaner. Cleaner is a dedicated thread for clean the memory-released entries.
    ///
    /// Note: This must be set after the event listener is set.
    pub fn with_cleaner(self, threads: usize) -> Self {
        let builder = self.builder.with_cleaner(threads);
        HybridCacheBuilderPhaseMemory {
            name: self.name,
            tracing_config: self.tracing_config,
            builder,
        }
    }

    /// Continue to modify the in-memory cache configurations.
    pub fn storage(self) -> HybridCacheBuilderPhaseStorage<K, V, S> {
        let memory = self.builder.build();
        HybridCacheBuilderPhaseStorage {
            builder: StoreBuilder::new(memory.clone()).with_name(&self.name),
            name: self.name,
            tracing_config: self.tracing_config,
            memory,
        }
    }
}

/// Hybird cache builder modify the disk cache configurations.
pub struct HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    name: String,
    tracing_config: TracingConfig,
    memory: Cache<K, V, S>,
    builder: StoreBuilder<K, V, S>,
}

impl<K, V, S> HybridCacheBuilderPhaseStorage<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Set device config for the disk cache store.
    pub fn with_device_config(self, device_config: impl Into<DeviceConfig>) -> Self {
        let builder = self.builder.with_device_config(device_config);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Enable/disable `sync` after writes.
    ///
    /// Default: `false`.
    pub fn with_flush(self, flush: bool) -> Self {
        let builder = self.builder.with_flush(flush);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the shard num of the indexer. Each shard has its own lock.
    ///
    /// Default: `64`.
    pub fn with_indexer_shards(self, indexer_shards: usize) -> Self {
        let builder = self.builder.with_indexer_shards(indexer_shards);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the recover mode for the disk cache store.
    ///
    /// See more in [`RecoverMode`].
    ///
    /// Default: [`RecoverMode::Quiet`].
    pub fn with_recover_mode(self, recover_mode: RecoverMode) -> Self {
        let builder = self.builder.with_recover_mode(recover_mode);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the recover concurrency for the disk cache store.
    ///
    /// Default: `8`.
    pub fn with_recover_concurrency(self, recover_concurrency: usize) -> Self {
        let builder = self.builder.with_recover_concurrency(recover_concurrency);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the flusher count for the disk cache store.
    ///
    /// The flusher count limits how many regions can be concurrently written.
    ///
    /// Default: `1`.
    pub fn with_flushers(self, flushers: usize) -> Self {
        let builder = self.builder.with_flushers(flushers);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the reclaimer count for the disk cache store.
    ///
    /// The reclaimer count limits how many regions can be concurrently reclaimed.
    ///
    /// Default: `1`.
    pub fn with_reclaimers(self, reclaimers: usize) -> Self {
        let builder = self.builder.with_reclaimers(reclaimers);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the total flush buffer threshold.
    ///
    /// Each flusher shares a volume at `threshold / flushers`.
    ///
    /// If the buffer of the flush queue exceeds the threshold, the further entries will be ignored.
    ///
    /// Default: 16 MiB.
    pub fn with_buffer_threshold(self, threshold: usize) -> Self {
        let builder = self.builder.with_buffer_threshold(threshold);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the clean region threshold for the disk cache store.
    ///
    /// The reclaimers only work when the clean region count is equal to or lower than the clean region threshold.
    ///
    /// Default: the same value as the `reclaimers`.
    pub fn with_clean_region_threshold(self, clean_region_threshold: usize) -> Self {
        let builder = self.builder.with_clean_region_threshold(clean_region_threshold);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the eviction pickers for th disk cache store.
    ///
    /// The eviction picker is used to pick the region to reclaim.
    ///
    /// The eviction pickers are applied in order. If the previous eviction picker doesn't pick any region, the next one
    /// will be applied.
    ///
    /// If no eviction picker pickes a region, a region will be picked randomly.
    ///
    /// Default: [ invalid ratio picker { threshold = 0.8 }, fifo picker ]
    pub fn with_eviction_pickers(self, eviction_pickers: Vec<Box<dyn EvictionPicker>>) -> Self {
        let builder = self.builder.with_eviction_pickers(eviction_pickers);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the admission pickers for th disk cache store.
    ///
    /// The admission picker is used to pick the entries that can be inserted into the disk cache store.
    ///
    /// Default: [`AdmitAllPicker`].
    pub fn with_admission_picker(self, admission_picker: Arc<dyn AdmissionPicker<Key = K>>) -> Self {
        let builder = self.builder.with_admission_picker(admission_picker);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the reinsertion pickers for th disk cache store.
    ///
    /// The reinsertion picker is used to pick the entries that can be reinsertion into the disk cache store while
    /// reclaiming.
    ///
    /// Note: Only extremely important entries should be picked. If too many entries are picked, both insertion and
    /// reinsertion will be stuck.
    ///
    /// Default: [`RejectAllPicker`].
    pub fn with_reinsertion_picker(self, reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>) -> Self {
        let builder = self.builder.with_reinsertion_picker(reinsertion_picker);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Set the compression algorithm of the disk cache store.
    ///
    /// Default: [`Compression::None`].
    pub fn with_compression(self, compression: Compression) -> Self {
        let builder = self.builder.with_compression(compression);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Enable the tombstone log with the given config.
    ///
    /// For updatable cache, either the tombstone log or [`RecoverMode::None`] must be enabled to prevent from the
    /// phantom entries after reopen.
    pub fn with_tombstone_log_config(self, tombstone_log_config: TombstoneLogConfig) -> Self {
        let builder = self.builder.with_tombstone_log_config(tombstone_log_config);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Configure the dedicated runtime for the disk cache store.
    pub fn with_runtime_config(self, runtime_config: RuntimeConfig) -> Self {
        let builder = self.builder.with_runtime_config(runtime_config);
        Self {
            name: self.name,
            tracing_config: self.tracing_config,
            memory: self.memory,
            builder,
        }
    }

    /// Build and open the hybrid cache with the given configurations.
    pub async fn build(self) -> anyhow::Result<HybridCache<K, V, S>> {
        let storage = self.builder.build().await?;
        Ok(HybridCache::new(self.name, self.memory, storage, self.tracing_config))
    }
}
