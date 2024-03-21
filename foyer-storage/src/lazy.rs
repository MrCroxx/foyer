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

use std::sync::{Arc, OnceLock};

use foyer_common::code::{Key, Value};
use tokio::task::JoinHandle;

use crate::{
    compress::Compression,
    error::Result,
    storage::{Storage, StorageWriter},
    store::{NoneStore, NoneStoreWriter, Store},
};

#[derive(Debug)]
pub enum LazyStorageWriter<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    Store { writer: S::Writer },
    None { writer: NoneStoreWriter<K, V> },
}

impl<K, V, S> StorageWriter for LazyStorageWriter<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        match self {
            LazyStorageWriter::Store { writer } => writer.key(),
            LazyStorageWriter::None { writer } => writer.key(),
        }
    }

    fn weight(&self) -> usize {
        match self {
            LazyStorageWriter::Store { writer } => writer.weight(),
            LazyStorageWriter::None { writer } => writer.weight(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            LazyStorageWriter::Store { writer } => writer.judge(),
            LazyStorageWriter::None { writer } => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            LazyStorageWriter::Store { writer } => writer.force(),
            LazyStorageWriter::None { writer } => writer.force(),
        }
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        match self {
            LazyStorageWriter::Store { writer } => writer.finish(value).await,
            LazyStorageWriter::None { writer } => writer.finish(value).await,
        }
    }

    fn compression(&self) -> Compression {
        match self {
            LazyStorageWriter::Store { writer } => writer.compression(),
            LazyStorageWriter::None { writer } => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            LazyStorageWriter::Store { writer } => writer.set_compression(compression),
            LazyStorageWriter::None { writer } => writer.set_compression(compression),
        }
    }
}

#[derive(Debug)]
pub struct LazyStorage<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    once: Arc<OnceLock<S>>,
    none: NoneStore<K, V>,
}

impl<K, V, S> Clone for LazyStorage<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    fn clone(&self) -> Self {
        Self {
            once: Arc::clone(&self.once),
            none: NoneStore::default(),
        }
    }
}

impl<K, V, S> LazyStorage<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    fn with_handle(config: S::Config) -> (Self, JoinHandle<Result<S>>) {
        let once = Arc::new(OnceLock::new());

        let handle = tokio::spawn({
            let once = once.clone();
            async move {
                let store = match S::open(config).await {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::error!("Lazy open store fail: {}", e);
                        return Err(e);
                    }
                };
                once.set(store.clone()).unwrap();
                Ok(store)
            }
        });

        let res = Self {
            once,
            none: NoneStore::default(),
        };

        (res, handle)
    }
}

impl<K, V, S> Storage for LazyStorage<K, V, S>
where
    K: Key,
    V: Value,
    S: Storage<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;
    type Config = S::Config;
    type Writer = LazyStorageWriter<K, V, S>;

    async fn open(config: S::Config) -> Result<Self> {
        let (store, task) = Self::with_handle(config);
        tokio::spawn(task);
        Ok(store)
    }

    fn is_ready(&self) -> bool {
        self.once.get().is_some()
    }

    async fn close(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.close().await,
            None => self.none.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        match self.once.get() {
            Some(store) => LazyStorageWriter::Store {
                writer: store.writer(key, weight),
            },
            None => LazyStorageWriter::None {
                writer: NoneStoreWriter::new(key, weight),
            },
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self.once.get() {
            Some(store) => store.exists(key),
            None => self.none.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self.once.get() {
            Some(store) => store.lookup(key).await,
            None => self.none.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self.once.get() {
            Some(store) => store.remove(key),
            None => self.none.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self.once.get() {
            Some(store) => store.clear(),
            None => self.none.clear(),
        }
    }
}

pub type LazyStore<K, V> = LazyStorage<K, V, Store<K, V>>;
pub type LazyStoreWriter<K, V> = LazyStorageWriter<K, V, Store<K, V>>;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use foyer_intrusive::eviction::fifo::FifoConfig;

    use super::*;
    use crate::{
        device::fs::FsDeviceConfig,
        storage::StorageExt,
        store::{FifoFsStoreConfig, Store},
    };

    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;

    #[tokio::test]
    async fn test_lazy_store() {
        let tempdir = tempfile::tempdir().unwrap();

        let config = FifoFsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            catalog_bits: 1,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 1,
            reclaimers: 1,
            recover_concurrency: 2,
            clean_region_threshold: 1,
            compression: crate::compress::Compression::None,
        };

        let (store, handle) = LazyStorage::<_, _, Store<_, _>>::with_handle(config.into());

        assert!(!store.insert(100, 100).await.unwrap());

        handle.await.unwrap().unwrap();

        assert!(store.insert(100, 100).await.unwrap());
        assert_eq!(store.lookup(&100).await.unwrap(), Some(100));

        store.close().await.unwrap();
        drop(store);

        let config = FifoFsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 16 * MB,
                file_capacity: 4 * MB,
                align: 4096,
                io_size: 4096 * KB,
            },
            catalog_bits: 1,
            admissions: vec![],
            reinsertions: vec![],
            flushers: 1,
            reclaimers: 1,
            recover_concurrency: 2,
            clean_region_threshold: 1,
            compression: crate::compress::Compression::None,
        };

        let (store, handle) = LazyStorage::<_, _, Store<_, _>>::with_handle(config.into());

        assert!(store.lookup(&100).await.unwrap().is_none());

        handle.await.unwrap().unwrap();

        assert_eq!(store.lookup(&100).await.unwrap(), Some(100));
        store.close().await.unwrap();
    }
}
