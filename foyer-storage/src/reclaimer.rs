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
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use bytes::BufMut;
use foyer_common::code::{Key, Value};
use foyer_intrusive::{core::adapter::Link, eviction::EvictionPolicy};
use tokio::sync::broadcast;

use crate::{
    device::Device,
    error::Result,
    generic::{GenericStore, RegionEntryIter},
    judge::Judges,
    metrics::Metrics,
    region_manager::{RegionEpItemAdapter, RegionManager},
    storage::Storage,
};

#[derive(Debug)]
pub struct Reclaimer<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    threshold: usize,

    store: GenericStore<K, V, D, EP, EL>,

    region_manager: Arc<RegionManager<D, EP, EL>>,

    metrics: Arc<Metrics>,

    stop_rx: broadcast::Receiver<()>,
}

impl<K, V, D, EP, EL> Reclaimer<K, V, D, EP, EL>
where
    K: Key,
    V: Value,
    D: Device,
    EP: EvictionPolicy<Adapter = RegionEpItemAdapter<EL>>,
    EL: Link,
{
    pub fn new(
        threshold: usize,
        store: GenericStore<K, V, D, EP, EL>,
        region_manager: Arc<RegionManager<D, EP, EL>>,
        metrics: Arc<Metrics>,
        stop_rx: broadcast::Receiver<()>,
    ) -> Self {
        Self {
            threshold,
            store,
            region_manager,
            metrics,
            stop_rx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut watch = self.region_manager.clean_regions().watch();
        loop {
            tokio::select! {
                biased;
                Ok(()) = watch.changed() => {
                    self.handle().await?;
                }
                _ = self.stop_rx.recv() => {
                    tracing::info!("[reclaimer] exit");
                    return Ok(())
                }
            }
        }
    }

    async fn handle(&self) -> Result<()> {
        if self.region_manager.clean_regions().len() >= self.threshold {
            return Ok(());
        }

        // TODO(MrCroxx): subscribe evictable region changes.
        let region_id = loop {
            match self.region_manager.eviction_pop() {
                Some(id) => break id,
                None => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        };

        let _timer = self.metrics.slow_op_duration_reclaim.start_timer();

        let region = self.region_manager.region(&region_id);

        // step 1: drop indices
        let indices = self.store.catalog().take_region(&region_id);

        // Must guarantee there is no following reads on the region to be reclaim.
        // Which means there is no unfinished reader or reader who holds index and prepare to read.

        // wait unfinished readers
        {
            // only each `indices` holds one ref
            while region.refs().load(Ordering::SeqCst) > indices.len() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        // step 2: do reinsertion
        let reinsert = || {
            let region = region.clone();
            let metrics = self.metrics.clone();
            let reinsertions = self.store.reinsertions().clone();

            tracing::info!("[reclaimer] begin reinsertion, region: {}", region_id);

            async move {
                let mut iter = match RegionEntryIter::<K, V, D>::open(region).await {
                    Ok(Some(iter)) => iter,
                    Ok(None) => return Ok(true),
                    Err(e) => return Err(e),
                };

                while let Some((key, value)) = iter.next_kv().await? {
                    let weight = key.serialized_len() + value.serialized_len();

                    let mut judges = Judges::new(reinsertions.len());
                    for (index, reinsertion) in reinsertions.iter().enumerate() {
                        let judge = reinsertion.judge(&key, weight);
                        judges.set(index, judge);
                    }
                    if !judges.judge() {
                        for (index, reinsertion) in reinsertions.iter().enumerate() {
                            let judge = judges.get(index);
                            reinsertion.on_drop(&key, weight, judge);
                        }
                        continue;
                    }

                    let mut writer = self.store.writer(key.clone(), weight);
                    writer.set_skippable();

                    if !writer.judge() {
                        continue;
                    }

                    if writer.finish(value).await? {
                        for (index, reinsertion) in reinsertions.iter().enumerate() {
                            let judge = judges.get(index);
                            reinsertion.on_insert(&key, weight, judge);
                        }
                    } else {
                        for (index, reinsertion) in reinsertions.iter().enumerate() {
                            let judge = judges.get(index);
                            reinsertion.on_drop(&key, weight, judge);
                        }
                        // The writer is already been judged and admitted, but not inserted successfully and skipped.
                        // That means allocating timeouts and there is no clean region available.
                        // Reinsertion should be interrupted to make sure foreground insertion.
                        return Ok(false);
                    }

                    metrics.op_bytes_reinsert.inc_by(weight as u64);
                }

                tracing::info!("[reclaimer] finish reinsertion, region: {}", region_id);

                Ok(true)
            }
        };

        if !self.store.reinsertions().is_empty() {
            match reinsert().await {
                Ok(true) => {
                    tracing::info!("[reclaimer] reinsertion finish, region: {}", region_id)
                }
                Ok(false) => {
                    tracing::info!("[reclaimer] reinsertion skipped, region: {}", region_id)
                }
                Err(e) => tracing::warn!("reinsert region {:?} error: {:?}", region, e),
            }
        }

        // step 3: wipe region header
        let align = region.device().align();
        let mut buf = region.device().io_buffer(align, align);
        (&mut buf[..]).put_slice(&vec![0; align]);
        let (res, _buf) = region.device().write(buf, .., region_id, 0).await;
        res?;

        // step 4: send clean region
        self.region_manager.clean_regions().release(region_id);

        tracing::info!("[reclaimer] finish reclaim task, region: {}", region_id);

        self.metrics
            .op_bytes_reclaim
            .inc_by(region.device().region_size() as u64);
        self.metrics.total_bytes.sub(region.device().region_size() as u64);

        Ok(())
    }
}
