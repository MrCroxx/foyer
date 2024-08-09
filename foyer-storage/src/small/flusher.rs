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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::future::try_join_all;
use parking_lot::Mutex;
use tokio::{
    runtime::Handle,
    sync::{oneshot, Notify},
    task::JoinHandle,
};

use crate::{
    error::{Error, Result},
    serde::KvInfo,
    IoBytes,
};

use super::{
    batch::{Batch, BatchMut, SetBatch},
    set_manager::SetManager,
};

pub struct Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    batch: Arc<Mutex<BatchMut<K, V, S>>>,

    notify: Arc<Notify>,

    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    stop: Arc<AtomicBool>,
}

impl<K, V, S> Flusher<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(set_manager: SetManager, runtime: &Handle) -> Self {
        let batch = Arc::new(Mutex::new(BatchMut::new(
            set_manager.sets() as _,
            set_manager.set_data_size(),
        )));
        let notify = Arc::<Notify>::default();

        let stop = Arc::<AtomicBool>::default();

        let runner = Runner {
            batch: batch.clone(),
            notify: notify.clone(),
            set_manager,
            stop: stop.clone(),
        };

        let handle = runtime.spawn(async move {
            if let Err(e) = runner.run().await {
                tracing::error!("[sodc flusher]: flusher exit with error: {e}");
            }
        });
        let handle = Arc::new(Mutex::new(Some(handle)));

        Self {
            batch,
            notify,
            handle,
            stop,
        }
    }

    pub fn entry(&self, entry: CacheEntry<K, V, S>, buffer: IoBytes, info: KvInfo, tx: oneshot::Sender<Result<bool>>) {
        self.batch.lock().append(entry, buffer, info, tx);
        self.notify.notify_one();
    }

    pub fn deletion(&self, hash: u64, tx: oneshot::Sender<Result<bool>>) {
        self.batch.lock().delete(hash, tx);
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
}

struct Runner<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    batch: Arc<Mutex<BatchMut<K, V, S>>>,

    notify: Arc<Notify>,

    set_manager: SetManager,

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
            let batch = self.batch.lock().rotate();
            if batch.sets.is_empty() {
                if self.stop.load(Ordering::SeqCst) {
                    return Ok(());
                }
                self.notify.notified().await;
                continue;
            }
            self.commit(batch).await
        }
    }

    pub async fn commit(&self, batch: Batch<K, V, S>) {
        let futures = batch.sets.into_iter().map(
            |(
                sid,
                SetBatch {
                    deletions,
                    bytes,
                    entries,
                    txs,
                },
            )| {
                let set_manager = self.set_manager.clone();
                async move {
                    let mut set = set_manager.write(sid).await?;
                    set.apply(&deletions, &bytes[..]);
                    set_manager.apply(set).await?;

                    // TODO(MrCroxx): Move notification to another task.
                    for tx in txs {
                        let _ = tx.send(Ok(true));
                    }

                    drop(entries);

                    Ok::<_, Error>(())
                }
            },
        );
        if let Err(e) = try_join_all(futures).await {
            tracing::error!("[sodc flusher]: error raised when committing batch, error: {e}");
        }
    }
}
