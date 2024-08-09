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

use crate::{
    compress::Compression,
    device::{
        direct_fs::DirectFsDeviceOptions,
        monitor::{DeviceStats, Monitored, MonitoredOptions},
        DeviceOptions, RegionId,
    },
    engine::{Engine, EngineConfig, SizeSelector},
    error::Result,
    large::{generic::GenericLargeStorageConfig, recover::RecoverMode, tombstone::TombstoneLogConfig},
    picker::{
        utils::{AdmitAllPicker, FifoPicker, InvalidRatioPicker, RejectAllPicker},
        AdmissionPicker, EvictionPicker, ReinsertionPicker,
    },
    serde::EntrySerializer,
    small::generic::GenericSmallStorageConfig,
    statistics::Statistics,
    storage::{
        either::{EitherConfig, Order},
        runtime::{RuntimeConfig, RuntimeStoreConfig},
        Storage,
    },
    Dev, DevExt, DirectFileDeviceOptions, IoBytesMut, WaitHandle,
};
use ahash::RandomState;
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
};
use foyer_memory::{Cache, CacheEntry};
use futures::{Future, FutureExt};
use std::{borrow::Borrow, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc, time::Instant};
use tokio::{runtime::Handle, sync::oneshot};

/// The disk cache engine that serves as the storage backend of `foyer`.
pub struct Store<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    engine: Engine<K, V, S>,
    admission_picker: Arc<dyn AdmissionPicker<Key = K>>,
    compression: Compression,
    statistics: Arc<Statistics>,
    metrics: Arc<Metrics>,
}

impl<K, V, S> Debug for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("engine", &self.engine)
            .field("admission_picker", &self.admission_picker)
            .field("compression", &self.compression)
            .field("statistics", &self.statistics)
            .finish()
    }
}

impl<K, V, S> Clone for Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            admission_picker: self.admission_picker.clone(),
            compression: self.compression,
            statistics: self.statistics.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<K, V, S> Store<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Close the disk cache gracefully.
    ///
    /// `close` will wait for all ongoing flush and reclaim tasks to finish.
    pub async fn close(&self) -> Result<()> {
        self.engine.close().await
    }

    /// Return if the given key can be picked by the admission picker.
    pub fn pick(&self, key: &K) -> bool {
        self.admission_picker.pick(&self.statistics, key)
    }

    /// Push a in-memory cache entry to the disk cache write queue.
    pub fn enqueue(
        &self,
        entry: CacheEntry<K, V, S>,
        force: bool,
    ) -> WaitHandle<impl Future<Output = Result<bool>> + Send> {
        let now = Instant::now();

        let (tx, rx) = oneshot::channel();
        let handle = WaitHandle::new(rx.map(|recv| recv.unwrap()));
        let compression = self.compression;
        let this = self.clone();

        self.runtime().spawn(async move {
            if force || this.pick(entry.key()) {
                let mut buffer = IoBytesMut::new();
                match EntrySerializer::serialize(entry.key(), entry.value(), &compression, &mut buffer) {
                    Ok(info) => {
                        let buffer = buffer.freeze();
                        this.engine.enqueue(entry, buffer, info, tx);
                    }
                    Err(e) => {
                        tracing::warn!("[store]: serialize kv error: {e}");
                        let _ = tx.send(Ok(false));
                    }
                }
            } else {
                let _ = tx.send(Ok(false));
            }
        });

        self.metrics.storage_enqueue.increment(1);
        self.metrics.storage_enqueue_duration.record(now.elapsed());

        handle
    }

    /// Load a cache entry from the disk cache.
    ///
    /// `load` may return a false-positive result on entry key hash collision. It's the caller's responsibility to
    /// check if the returned key matches the given key.
    pub fn load<'a, Q>(&'a self, key: &'a Q) -> impl Future<Output = Result<Option<(K, V)>>> + Send + 'a
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        self.engine.load(key)
    }

    /// Delete the cache entry with the given key from the disk cache.
    pub fn delete<'a, Q>(&'a self, key: &'a Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'a>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.engine.delete(key)
    }

    /// Check if the disk cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    pub fn may_contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.engine.may_contains(key)
    }

    /// Delete all cached entries of the disk cache.
    pub async fn destroy(&self) -> Result<()> {
        self.engine.destroy().await
    }

    /// Get the statistics information of the disk cache.
    pub fn stats(&self) -> Arc<DeviceStats> {
        self.engine.stats()
    }

    /// Get disk cache runtime handle.
    ///
    /// The runtime is determined during the opening phase.
    pub fn runtime(&self) -> &Handle {
        self.engine.runtime()
    }
}

/// The configurations for the device.
#[derive(Debug, Clone)]
pub enum DeviceConfig {
    /// No device.
    None,
    /// With device options.
    DeviceOptions(DeviceOptions),
}

impl From<DirectFileDeviceOptions> for DeviceConfig {
    fn from(value: DirectFileDeviceOptions) -> Self {
        Self::DeviceOptions(value.into())
    }
}

impl From<DirectFsDeviceOptions> for DeviceConfig {
    fn from(value: DirectFsDeviceOptions) -> Self {
        Self::DeviceOptions(value.into())
    }
}

/// [`CombinedConfig`] controls the ratio of the large object disk cache and the small object disk cache.
///
/// If [`CombinedConfig::Combined`] is used, it will use the `Either` engine
/// with the small object disk cache as the left engine,
/// and the large object disk cache as the right engine.
#[derive(Debug, Clone)]
pub enum CombinedConfig {
    /// All space are used as the large object disk cache.
    Large,
    /// All space are used as the small object disk cache.
    Small,
    /// Combined the large object disk cache and the small object disk cache.
    Combined {
        /// The ratio of the large object disk cache.
        large_object_cache_ratio: f64,
        /// The serialized entry size threshold to use the large object disk cache.
        large_object_threshold: usize,
        /// Load order.
        load_order: Order,
    },
}

impl Default for CombinedConfig {
    fn default() -> Self {
        // TODO(MrCroxx): Use combined cache after small object disk cache is ready.
        Self::Large
    }
}

impl CombinedConfig {
    /// Default large object disk cache only config.
    pub fn large() -> Self {
        Self::Large
    }

    /// Default small object disk cache only config.
    pub fn small() -> Self {
        Self::Small
    }

    /// Default combined large object disk cache and small object disk cache only config.
    pub fn combined() -> Self {
        Self::Combined {
            large_object_cache_ratio: 0.5,
            large_object_threshold: 4096,
            load_order: Order::RightFirst,
        }
    }
}

/// The builder of the disk cache.
pub struct StoreBuilder<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    memory: Cache<K, V, S>,

    name: String,
    device_config: DeviceConfig,
    flush: bool,
    indexer_shards: usize,
    recover_mode: RecoverMode,
    recover_concurrency: usize,
    flushers: usize,
    reclaimers: usize,
    // FIXME(MrCroxx): rename the field and builder fn.
    buffer_threshold: usize,
    clean_region_threshold: Option<usize>,
    eviction_pickers: Vec<Box<dyn EvictionPicker>>,
    admission_picker: Arc<dyn AdmissionPicker<Key = K>>,
    reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>,
    compression: Compression,
    tombstone_log_config: Option<TombstoneLogConfig>,
    combined_config: CombinedConfig,

    runtime_config: Option<RuntimeConfig>,
}

impl<K, V, S> StoreBuilder<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// Setup disk cache store for the given in-memory cache.
    pub fn new(memory: Cache<K, V, S>) -> Self {
        Self {
            memory,
            name: "foyer".to_string(),
            device_config: DeviceConfig::None,
            flush: false,
            indexer_shards: 64,
            recover_mode: RecoverMode::Quiet,
            recover_concurrency: 8,
            flushers: 1,
            reclaimers: 1,
            buffer_threshold: 16 * 1024 * 1024, // 16 MiB
            clean_region_threshold: None,
            eviction_pickers: vec![Box::new(InvalidRatioPicker::new(0.8)), Box::<FifoPicker>::default()],
            admission_picker: Arc::<AdmitAllPicker<K>>::default(),
            reinsertion_picker: Arc::<RejectAllPicker<K>>::default(),
            compression: Compression::None,
            tombstone_log_config: None,
            combined_config: CombinedConfig::default(),
            runtime_config: None,
        }
    }

    /// Set the name of the foyer disk cache instance.
    ///
    /// Foyer will use the name as the prefix of the metric names.
    ///
    /// Default: `foyer`.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    /// Set device config for the disk cache store.
    pub fn with_device_config(mut self, device_config: impl Into<DeviceConfig>) -> Self {
        self.device_config = device_config.into();
        self
    }

    /// Enable/disable `sync` after writes.
    ///
    /// Default: `false`.
    pub fn with_flush(mut self, flush: bool) -> Self {
        self.flush = flush;
        self
    }

    /// Set the shard num of the indexer. Each shard has its own lock.
    ///
    /// Default: `64`.
    pub fn with_indexer_shards(mut self, indexer_shards: usize) -> Self {
        self.indexer_shards = indexer_shards;
        self
    }

    /// Set the recover mode for the disk cache store.
    ///
    /// See more in [`RecoverMode`].
    ///
    /// Default: [`RecoverMode::Quiet`].
    pub fn with_recover_mode(mut self, recover_mode: RecoverMode) -> Self {
        self.recover_mode = recover_mode;
        self
    }

    /// Set the recover concurrency for the disk cache store.
    ///
    /// Default: `8`.
    pub fn with_recover_concurrency(mut self, recover_concurrency: usize) -> Self {
        self.recover_concurrency = recover_concurrency;
        self
    }

    /// Set the flusher count for the disk cache store.
    ///
    /// The flusher count limits how many regions can be concurrently written.
    ///
    /// Default: `1`.
    pub fn with_flushers(mut self, flushers: usize) -> Self {
        self.flushers = flushers;
        self
    }

    /// Set the reclaimer count for the disk cache store.
    ///
    /// The reclaimer count limits how many regions can be concurrently reclaimed.
    ///
    /// Default: `1`.
    pub fn with_reclaimers(mut self, reclaimers: usize) -> Self {
        self.reclaimers = reclaimers;
        self
    }

    /// Set the total flush buffer threshold.
    ///
    /// Each flusher shares a volume at `threshold / flushers`.
    ///
    /// If the buffer of the flush queue exceeds the threshold, the further entries will be ignored.
    ///
    /// Default: 16 MiB.
    pub fn with_buffer_threshold(mut self, threshold: usize) -> Self {
        self.buffer_threshold = threshold;
        self
    }

    /// Set the clean region threshold for the disk cache store.
    ///
    /// The reclaimers only work when the clean region count is equal to or lower than the clean region threshold.
    ///
    /// Default: the same value as the `reclaimers`.
    pub fn with_clean_region_threshold(mut self, clean_region_threshold: usize) -> Self {
        self.clean_region_threshold = Some(clean_region_threshold);
        self
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
    pub fn with_eviction_pickers(mut self, eviction_pickers: Vec<Box<dyn EvictionPicker>>) -> Self {
        self.eviction_pickers = eviction_pickers;
        self
    }

    /// Set the admission pickers for th disk cache store.
    ///
    /// The admission picker is used to pick the entries that can be inserted into the disk cache store.
    ///
    /// Default: [`AdmitAllPicker`].
    pub fn with_admission_picker(mut self, admission_picker: Arc<dyn AdmissionPicker<Key = K>>) -> Self {
        self.admission_picker = admission_picker;
        self
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
    pub fn with_reinsertion_picker(mut self, reinsertion_picker: Arc<dyn ReinsertionPicker<Key = K>>) -> Self {
        self.reinsertion_picker = reinsertion_picker;
        self
    }

    /// Set the compression algorithm of the disk cache store.
    ///
    /// Default: [`Compression::None`].
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Enable the tombstone log with the given config.
    ///
    /// For updatable cache, either the tombstone log or [`RecoverMode::None`] must be enabled to prevent from the
    /// phantom entries after reopen.
    pub fn with_tombstone_log_config(mut self, tombstone_log_config: TombstoneLogConfig) -> Self {
        self.tombstone_log_config = Some(tombstone_log_config);
        self
    }

    /// Set the ratio of the large object disk cache and the small object disk cache.
    pub fn with_combined_config(mut self, combined_config: CombinedConfig) -> Self {
        self.combined_config = combined_config;
        self
    }

    /// Enable the dedicated runtime for the disk cache store.
    pub fn with_runtime_config(mut self, runtime_config: RuntimeConfig) -> Self {
        self.runtime_config = Some(runtime_config);
        self
    }

    /// Build the disk cache store with the given configuration.
    pub async fn build(self) -> Result<Store<K, V, S>> {
        let clean_region_threshold = self.clean_region_threshold.unwrap_or(self.reclaimers);

        let admission_picker = self.admission_picker.clone();
        let metrics = Arc::new(Metrics::new(&self.name));
        let statistics = Arc::<Statistics>::default();

        let engine = match self.device_config {
            DeviceConfig::None => {
                tracing::warn!(
                    "[store builder]: No device config set. Use `NoneStore` which always returns `None` for queries."
                );
                Engine::open(EngineConfig::Noop).await?
            }
            DeviceConfig::DeviceOptions(options) => {
                let device = Monitored::open(MonitoredOptions {
                    options,
                    metrics: metrics.clone(),
                })
                .await?;
                match (self.combined_config, self.runtime_config) {
                    (CombinedConfig::Large, None) => {
                        let regions = 0..device.regions() as RegionId;
                        Engine::open(EngineConfig::Large(GenericLargeStorageConfig {
                            memory: self.memory,
                            name: self.name,
                            device,
                            regions,
                            compression: self.compression,
                            flush: self.flush,
                            indexer_shards: self.indexer_shards,
                            recover_mode: self.recover_mode,
                            recover_concurrency: self.recover_concurrency,
                            flushers: self.flushers,
                            reclaimers: self.reclaimers,
                            clean_region_threshold,
                            eviction_pickers: self.eviction_pickers,
                            reinsertion_picker: self.reinsertion_picker,
                            tombstone_log_config: self.tombstone_log_config,
                            buffer_threshold: self.buffer_threshold,
                            statistics: statistics.clone(),
                        }))
                        .await?
                    }
                    (CombinedConfig::Large, Some(runtime_config)) => {
                        let regions = 0..device.regions() as RegionId;
                        Engine::open(EngineConfig::LargeRuntime(RuntimeStoreConfig {
                            store_config: GenericLargeStorageConfig {
                                memory: self.memory,
                                name: self.name,
                                device,
                                regions,
                                compression: self.compression,
                                flush: self.flush,
                                indexer_shards: self.indexer_shards,
                                recover_mode: self.recover_mode,
                                recover_concurrency: self.recover_concurrency,
                                flushers: self.flushers,
                                reclaimers: self.reclaimers,
                                clean_region_threshold,
                                eviction_pickers: self.eviction_pickers,
                                reinsertion_picker: self.reinsertion_picker,
                                tombstone_log_config: self.tombstone_log_config,
                                buffer_threshold: self.buffer_threshold,
                                statistics: statistics.clone(),
                            },
                            runtime_config,
                        }))
                        .await?
                    }
                    (CombinedConfig::Small, None) => {
                        Engine::open(EngineConfig::Small(GenericSmallStorageConfig {
                            memory: todo!(),
                            set_size: todo!(),
                            set_cache_capacity: todo!(),
                            device,
                            regions: todo!(),
                            flush: todo!(),
                            flushers: todo!(),
                        }))
                        .await?
                    }
                    (CombinedConfig::Small, Some(runtime_config)) => {
                        Engine::open(EngineConfig::SmallRuntime(RuntimeStoreConfig {
                            store_config: GenericSmallStorageConfig {
                                memory: todo!(),
                                set_size: todo!(),
                                set_cache_capacity: todo!(),
                                device,
                                regions: todo!(),
                                flush: todo!(),
                                flushers: todo!(),
                            },
                            runtime_config,
                        }))
                        .await?
                    }
                    (
                        CombinedConfig::Combined {
                            large_object_cache_ratio,
                            large_object_threshold,
                            load_order,
                        },
                        None,
                    ) => {
                        let large_region_count = (device.regions() as f64 * large_object_cache_ratio) as usize;
                        let large_regions =
                            (device.regions() - large_region_count) as RegionId..device.regions() as RegionId;

                        Engine::open(EngineConfig::Combined(EitherConfig {
                            selector: SizeSelector::new(large_object_threshold),
                            left: GenericSmallStorageConfig {
                                memory: todo!(),
                                set_size: todo!(),
                                set_cache_capacity: todo!(),
                                device,
                                regions: todo!(),
                                flush: todo!(),
                                flushers: todo!(),
                            },
                            right: GenericLargeStorageConfig {
                                memory: self.memory,
                                name: self.name,
                                device,
                                regions: large_regions,
                                compression: self.compression,
                                flush: self.flush,
                                indexer_shards: self.indexer_shards,
                                recover_mode: self.recover_mode,
                                recover_concurrency: self.recover_concurrency,
                                flushers: self.flushers,
                                reclaimers: self.reclaimers,
                                clean_region_threshold,
                                eviction_pickers: self.eviction_pickers,
                                reinsertion_picker: self.reinsertion_picker,
                                tombstone_log_config: self.tombstone_log_config,
                                buffer_threshold: self.buffer_threshold,
                                statistics: statistics.clone(),
                            },
                            load_order,
                        }))
                        .await?
                    }
                    (
                        CombinedConfig::Combined {
                            large_object_cache_ratio,
                            large_object_threshold,
                            load_order,
                        },
                        Some(runtime_config),
                    ) => {
                        let large_region_count = (device.regions() as f64 * large_object_cache_ratio) as usize;
                        let large_regions =
                            (device.regions() - large_region_count) as RegionId..device.regions() as RegionId;

                        Engine::open(EngineConfig::CombinedRuntime(RuntimeStoreConfig {
                            store_config: EitherConfig {
                                selector: SizeSelector::new(large_object_threshold),
                                left: GenericSmallStorageConfig {
                                    memory: todo!(),
                                    set_size: todo!(),
                                    set_cache_capacity: todo!(),
                                    device,
                                    regions: todo!(),
                                    flush: todo!(),
                                    flushers: todo!(),
                                },
                                right: GenericLargeStorageConfig {
                                    memory: self.memory,
                                    name: self.name,
                                    device,
                                    regions: large_regions,
                                    compression: self.compression,
                                    flush: self.flush,
                                    indexer_shards: self.indexer_shards,
                                    recover_mode: self.recover_mode,
                                    recover_concurrency: self.recover_concurrency,
                                    flushers: self.flushers,
                                    reclaimers: self.reclaimers,
                                    clean_region_threshold,
                                    eviction_pickers: self.eviction_pickers,
                                    reinsertion_picker: self.reinsertion_picker,
                                    tombstone_log_config: self.tombstone_log_config,
                                    buffer_threshold: self.buffer_threshold,
                                    statistics: statistics.clone(),
                                },
                                load_order,
                            },
                            runtime_config,
                        }))
                        .await?
                    }
                }
            }
        };

        Ok(Store {
            engine,
            admission_picker,
            compression: self.compression,
            statistics,
            metrics,
        })
    }
}
