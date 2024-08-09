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
    device::MonitoredDevice,
    error::{Error, Result},
    large::serde::EntryHeader,
    region::RegionManager,
    serde::{Checksummer, KvInfo},
    Compression, IoBytes, Statistics,
};
use foyer_common::{
    code::{HashBuilder, StorageKey, StorageValue},
    metrics::Metrics,
    strict_assert,
};
use foyer_memory::CacheEntry;
use futures::future::{try_join, try_join_all};
use parking_lot::Mutex;
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    runtime::Handle,
    sync::{oneshot, Notify},
    task::JoinHandle,
};

use super::{
    batch::{Batch, BatchMut, InvalidStats, TombstoneInfo},
    generic::GenericLargeStorageConfig,
    indexer::Indexer,
    reclaimer::Reinsertion,
    serde::Sequence,
    tombstone::{Tombstone, TombstoneLog},
};

#[derive(Debug)]
pub enum Submission<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    CacheEntry {
        entry: CacheEntry<K, V, S>,
        buffer: IoBytes,
        info: KvInfo,
        sequence: Sequence,
        tx: oneshot::Sender<Result<bool>>,
    },
    Tombstone {
        tombstone: Tombstone,
        stats: Option<InvalidStats>,
        tx: oneshot::Sender<Result<bool>>,
    },
    Reinsertion {
        reinsertion: Reinsertion,
        tx: oneshot::Sender<Result<bool>>,
    },
}

#[derive(Debug)]
pub struct Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    batch: Arc<Mutex<BatchMut<K, V, S>>>,

    notify: Arc<Notify>,

    compression: Compression,
    metrics: Arc<Metrics>,

    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    stop: Arc<AtomicBool>,
}

impl<K, V, S> Clone for Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            batch: self.batch.clone(),
            notify: self.notify.clone(),
            compression: self.compression,
            metrics: self.metrics.clone(),
            handle: self.handle.clone(),
            stop: self.stop.clone(),
        }
    }
}

impl<K, V, S> Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &GenericLargeStorageConfig<K, V, S>,
        indexer: Indexer,
        region_manager: RegionManager,
        device: MonitoredDevice,
        tombstone_log: Option<TombstoneLog>,
        stats: Arc<Statistics>,
        metrics: Arc<Metrics>,
        runtime: Handle,
    ) -> Result<Self> {
        let notify = Arc::new(Notify::new());

        let buffer_size = config.buffer_threshold / config.flushers;
        let batch = Arc::new(Mutex::new(BatchMut::new(
            buffer_size,
            region_manager.clone(),
            device.clone(),
            indexer.clone(),
        )));

        let stop = Arc::<AtomicBool>::default();

        let runner = Runner {
            batch: batch.clone(),
            notify: notify.clone(),
            region_manager,
            indexer,
            tombstone_log,
            flush: config.flush,
            stats,
            metrics: metrics.clone(),
            stop: stop.clone(),
        };

        let handle = runtime.spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!("[lodc flusher]: flusher exit with error: {e}");
            }
        });
        let handle = Arc::new(Mutex::new(Some(handle)));

        Ok(Self {
            batch,
            notify,
            compression: config.compression,
            metrics,
            handle,
            stop,
        })
    }

    pub fn submit(&self, submission: Submission<K, V, S>) {
        match submission {
            Submission::CacheEntry {
                entry,
                buffer,
                info,
                sequence,
                tx,
            } => self.entry(entry, buffer, info, sequence, tx),
            Submission::Tombstone { tombstone, stats, tx } => self.tombstone(tombstone, stats, tx),
            Submission::Reinsertion { reinsertion, tx } => self.reinsertion(reinsertion, tx),
        }
        self.notify.notify_one();
    }

    pub async fn close(&self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        self.notify.notify_one();
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            handle.await.unwrap();
        }
        Ok(())
    }

    fn entry(
        &self,
        entry: CacheEntry<K, V, S>,
        buffer: IoBytes,
        info: KvInfo,
        sequence: u64,
        tx: oneshot::Sender<Result<bool>>,
    ) {
        let header = EntryHeader {
            key_len: info.key_len as _,
            value_len: info.value_len as _,
            hash: entry.hash(),
            sequence,
            checksum: Checksummer::checksum64(&buffer),
            compression: self.compression,
        };

        let mut allocation = match self.batch.lock().entry(header.entry_len(), entry, tx, sequence) {
            Some(allocation) => allocation,
            None => {
                self.metrics.storage_queue_drop.increment(1);
                return;
            }
        };
        strict_assert!(allocation.len() > header.entry_len());

        header.write(&mut allocation[0..EntryHeader::serialized_len()]);
        allocation[EntryHeader::serialized_len()..header.entry_len()].copy_from_slice(&buffer);
    }

    fn tombstone(&self, tombstone: Tombstone, stats: Option<InvalidStats>, tx: oneshot::Sender<Result<bool>>) {
        self.batch.lock().tombstone(tombstone, stats, tx);
    }

    fn reinsertion(&self, reinsertion: Reinsertion, tx: oneshot::Sender<Result<bool>>) {
        let mut allocation = match self.batch.lock().reinsertion(&reinsertion, tx) {
            Some(allocation) => allocation,
            None => {
                self.metrics.storage_queue_drop.increment(1);
                return;
            }
        };
        strict_assert!(allocation.len() > reinsertion.buffer.len());
        allocation[0..reinsertion.buffer.len()].copy_from_slice(&reinsertion.buffer);
    }
}

struct Runner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    batch: Arc<Mutex<BatchMut<K, V, S>>>,

    notify: Arc<Notify>,

    region_manager: RegionManager,
    indexer: Indexer,
    tombstone_log: Option<TombstoneLog>,

    flush: bool,

    stats: Arc<Statistics>,
    metrics: Arc<Metrics>,

    stop: Arc<AtomicBool>,
}

impl<K, V, S> Runner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub async fn run(self) -> Result<()> {
        loop {
            let rotation = self.batch.lock().rotate();
            let (batch, wait) = match rotation {
                Some(rotation) => rotation,
                None => {
                    if self.stop.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    self.notify.notified().await;
                    continue;
                }
            };

            wait.await;

            self.commit(batch).await
        }
    }

    async fn commit(&self, batch: Batch<K, V, S>) {
        tracing::trace!("[flusher] commit batch: {batch:?}");

        // Write regions concurrently.
        let futures = batch.groups.into_iter().map(|group| {
            let indexer = self.indexer.clone();
            let region_manager = self.region_manager.clone();
            let stats = self.stats.clone();
            let flush = self.flush;
            async move {
                // Wait for region is clean.
                let region = group.region.handle.await;
                tracing::trace!(
                    "[flusher]: write region: {id}, at offset: {offset}, buffer len: {buf_len}",
                    id = region.id(),
                    offset = group.region.offset,
                    buf_len = group.bytes.len(),
                );

                // Write buffer to device.
                let size: usize = group.bytes.len();
                if size > 0 {
                    region.write(group.bytes, group.region.offset).await?;
                    if flush {
                        region.flush().await?;
                    }
                    stats.cache_write_bytes.fetch_add(size, Ordering::Relaxed);
                }
                let mut indices = group.indices;
                for haddr in indices.iter_mut() {
                    haddr.address.region = region.id();
                }
                indexer.insert_batch(indices);
                for tx in group.txs {
                    let _ = tx.send(Ok(true));
                }

                if group.region.is_full {
                    region_manager.mark_evictable(region.id());
                }
                // Make sure entries are dropped after written.
                drop(group.entries);
                tracing::trace!("[flusher]: write region {id} finish.", id = region.id());
                Ok::<_, Error>(())
            }
        });
        let future = {
            let tombstones = batch.tombstones;
            let has_tombstone_log = self.tombstone_log.is_some();
            let region_manager = self.region_manager.clone();
            let tombstone_log = self.tombstone_log.clone();
            async move {
                if let Some(log) = tombstone_log {
                    log.append(tombstones.iter().map(|info| &info.tombstone)).await?;
                }
                for TombstoneInfo {
                    tombstone: _,
                    stats,
                    tx,
                } in tombstones
                {
                    if let Some(stats) = stats {
                        region_manager
                            .region(stats.region)
                            .stats()
                            .invalid
                            .fetch_add(stats.size, Ordering::Relaxed);
                    }
                    let _ = tx.send(Ok(has_tombstone_log));
                }
                Ok::<_, Error>(())
            }
        };
        if let Err(e) = try_join(try_join_all(futures), future).await {
            tracing::error!("[flusher]: error raised when committing batch, error: {e}");
        }
        if let Some(init) = batch.init.as_ref() {
            self.metrics.storage_queue_rotate.increment(1);
            self.metrics.storage_queue_rotate_duration.record(init.elapsed());
        }
    }
}
