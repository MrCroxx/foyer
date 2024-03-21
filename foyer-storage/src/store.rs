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

use std::marker::PhantomData;

use foyer_common::code::{Key, Value};
use foyer_intrusive::eviction::{
    fifo::{Fifo, FifoLink},
    lfu::{Lfu, LfuLink},
    lru::{Lru, LruLink},
};

use crate::{
    compress::Compression,
    device::fs::FsDevice,
    error::Result,
    generic::{GenericStore, GenericStoreConfig, GenericStoreWriter},
    region_manager::RegionEpItemAdapter,
    storage::{Storage, StorageWriter},
};

pub type LruFsStore<K, V> = GenericStore<K, V, FsDevice, Lru<RegionEpItemAdapter<LruLink>>, LruLink>;

pub type LruFsStoreConfig<K, V> = GenericStoreConfig<K, V, FsDevice, Lru<RegionEpItemAdapter<LruLink>>>;

pub type LruFsStoreWriter<K, V> = GenericStoreWriter<K, V, FsDevice, Lru<RegionEpItemAdapter<LruLink>>, LruLink>;

pub type LfuFsStore<K, V> = GenericStore<K, V, FsDevice, Lfu<RegionEpItemAdapter<LfuLink>>, LfuLink>;

pub type LfuFsStoreConfig<K, V> = GenericStoreConfig<K, V, FsDevice, Lfu<RegionEpItemAdapter<LfuLink>>>;

pub type LfuFsStoreWriter<K, V> = GenericStoreWriter<K, V, FsDevice, Lfu<RegionEpItemAdapter<LfuLink>>, LfuLink>;

pub type FifoFsStore<K, V> = GenericStore<K, V, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>, FifoLink>;

pub type FifoFsStoreConfig<K, V> = GenericStoreConfig<K, V, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>>;

pub type FifoFsStoreWriter<K, V> = GenericStoreWriter<K, V, FsDevice, Fifo<RegionEpItemAdapter<FifoLink>>, FifoLink>;

#[derive(Debug)]
pub struct NoneStoreWriter<K: Key, V: Value> {
    key: K,
    weight: usize,
    _marker: PhantomData<V>,
}

impl<K: Key, V: Value> NoneStoreWriter<K, V> {
    pub fn new(key: K, weight: usize) -> Self {
        Self {
            key,
            weight,
            _marker: PhantomData,
        }
    }
}

impl<K: Key, V: Value> StorageWriter for NoneStoreWriter<K, V> {
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    fn weight(&self) -> usize {
        self.weight
    }

    fn judge(&mut self) -> bool {
        false
    }

    fn force(&mut self) {}

    async fn finish(self, _: Self::Value) -> Result<bool> {
        Ok(false)
    }

    fn compression(&self) -> Compression {
        Compression::None
    }

    fn set_compression(&mut self, _: Compression) {}
}

#[derive(Debug)]
pub struct NoneStore<K: Key, V: Value>(PhantomData<(K, V)>);

impl<K: Key, V: Value> Default for NoneStore<K, V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<K: Key, V: Value> Clone for NoneStore<K, V> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K: Key, V: Value> Storage for NoneStore<K, V> {
    type Key = K;
    type Value = V;
    type Config = ();
    type Writer = NoneStoreWriter<K, V>;

    async fn open(_: Self::Config) -> Result<Self> {
        Ok(NoneStore(PhantomData))
    }

    fn is_ready(&self) -> bool {
        true
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        NoneStoreWriter::new(key, weight)
    }

    fn exists(&self, _: &Self::Key) -> Result<bool> {
        Ok(false)
    }

    async fn lookup(&self, _: &Self::Key) -> Result<Option<Self::Value>> {
        Ok(None)
    }

    fn remove(&self, _: &Self::Key) -> Result<bool> {
        Ok(false)
    }

    fn clear(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    LruFsStoreConfig { config: LruFsStoreConfig<K, V> },
    LfuFsStoreConfig { config: LfuFsStoreConfig<K, V> },
    FifoFsStoreConfig { config: FifoFsStoreConfig<K, V> },
    NoneStoreConfig,
}

impl<K, V> Clone for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        match self {
            Self::LruFsStoreConfig { config } => Self::LruFsStoreConfig { config: config.clone() },
            Self::LfuFsStoreConfig { config } => Self::LfuFsStoreConfig { config: config.clone() },
            Self::FifoFsStoreConfig { config } => Self::FifoFsStoreConfig { config: config.clone() },
            Self::NoneStoreConfig => Self::NoneStoreConfig,
        }
    }
}

impl<K, V> From<LruFsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn from(config: LruFsStoreConfig<K, V>) -> Self {
        StoreConfig::LruFsStoreConfig { config }
    }
}

impl<K, V> From<LfuFsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn from(config: LfuFsStoreConfig<K, V>) -> Self {
        StoreConfig::LfuFsStoreConfig { config }
    }
}

impl<K, V> From<FifoFsStoreConfig<K, V>> for StoreConfig<K, V>
where
    K: Key,
    V: Value,
{
    fn from(config: FifoFsStoreConfig<K, V>) -> Self {
        StoreConfig::FifoFsStoreConfig { config }
    }
}

#[derive(Debug)]
pub enum StoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    LruFsStorWriter { writer: LruFsStoreWriter<K, V> },
    LfuFsStorWriter { writer: LfuFsStoreWriter<K, V> },
    FifoFsStoreWriter { writer: FifoFsStoreWriter<K, V> },
    NoneStoreWriter { writer: NoneStoreWriter<K, V> },
}

impl<K, V> From<LruFsStoreWriter<K, V>> for StoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: LruFsStoreWriter<K, V>) -> Self {
        StoreWriter::LruFsStorWriter { writer }
    }
}

impl<K, V> From<LfuFsStoreWriter<K, V>> for StoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: LfuFsStoreWriter<K, V>) -> Self {
        StoreWriter::LfuFsStorWriter { writer }
    }
}

impl<K, V> From<FifoFsStoreWriter<K, V>> for StoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: FifoFsStoreWriter<K, V>) -> Self {
        StoreWriter::FifoFsStoreWriter { writer }
    }
}

impl<K, V> From<NoneStoreWriter<K, V>> for StoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    fn from(writer: NoneStoreWriter<K, V>) -> Self {
        StoreWriter::NoneStoreWriter { writer }
    }
}

#[derive(Debug)]
pub enum Store<K, V>
where
    K: Key,
    V: Value,
{
    LruFsStore { store: LruFsStore<K, V> },
    LfuFsStore { store: LfuFsStore<K, V> },
    FifoFsStore { store: FifoFsStore<K, V> },
    NoneStore { store: NoneStore<K, V> },
}

impl<K, V> Clone for Store<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        match self {
            Self::LruFsStore { store } => Self::LruFsStore { store: store.clone() },
            Self::LfuFsStore { store } => Self::LfuFsStore { store: store.clone() },
            Self::FifoFsStore { store } => Self::FifoFsStore { store: store.clone() },
            Self::NoneStore { store } => Self::NoneStore { store: store.clone() },
        }
    }
}

impl<K, V> StorageWriter for StoreWriter<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn key(&self) -> &Self::Key {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.key(),
            StoreWriter::LfuFsStorWriter { writer } => writer.key(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.key(),
            StoreWriter::NoneStoreWriter { writer } => writer.key(),
        }
    }

    fn weight(&self) -> usize {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.weight(),
            StoreWriter::LfuFsStorWriter { writer } => writer.weight(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.weight(),
            StoreWriter::NoneStoreWriter { writer } => writer.weight(),
        }
    }

    fn judge(&mut self) -> bool {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.judge(),
            StoreWriter::LfuFsStorWriter { writer } => writer.judge(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.judge(),
            StoreWriter::NoneStoreWriter { writer } => writer.judge(),
        }
    }

    fn force(&mut self) {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.force(),
            StoreWriter::LfuFsStorWriter { writer } => writer.force(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.force(),
            StoreWriter::NoneStoreWriter { writer } => writer.force(),
        }
    }

    async fn finish(self, value: Self::Value) -> Result<bool> {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.finish(value).await,
            StoreWriter::LfuFsStorWriter { writer } => writer.finish(value).await,
            StoreWriter::FifoFsStoreWriter { writer } => writer.finish(value).await,
            StoreWriter::NoneStoreWriter { writer } => writer.finish(value).await,
        }
    }

    fn compression(&self) -> Compression {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.compression(),
            StoreWriter::LfuFsStorWriter { writer } => writer.compression(),
            StoreWriter::FifoFsStoreWriter { writer } => writer.compression(),
            StoreWriter::NoneStoreWriter { writer } => writer.compression(),
        }
    }

    fn set_compression(&mut self, compression: Compression) {
        match self {
            StoreWriter::LruFsStorWriter { writer } => writer.set_compression(compression),
            StoreWriter::LfuFsStorWriter { writer } => writer.set_compression(compression),
            StoreWriter::FifoFsStoreWriter { writer } => writer.set_compression(compression),
            StoreWriter::NoneStoreWriter { writer } => writer.set_compression(compression),
        }
    }
}

impl<K, V> Storage for Store<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Config = StoreConfig<K, V>;
    type Writer = StoreWriter<K, V>;

    async fn open(config: Self::Config) -> Result<Self> {
        match config {
            StoreConfig::LruFsStoreConfig { config } => {
                let store = LruFsStore::open(config).await?;
                Ok(Self::LruFsStore { store })
            }
            StoreConfig::LfuFsStoreConfig { config } => {
                let store = LfuFsStore::open(config).await?;
                Ok(Self::LfuFsStore { store })
            }
            StoreConfig::FifoFsStoreConfig { config } => {
                let store = FifoFsStore::open(config).await?;
                Ok(Self::FifoFsStore { store })
            }
            StoreConfig::NoneStoreConfig => {
                let store = NoneStore::open(()).await?;
                Ok(Self::NoneStore { store })
            }
        }
    }

    fn is_ready(&self) -> bool {
        match self {
            Store::LruFsStore { store } => store.is_ready(),
            Store::LfuFsStore { store } => store.is_ready(),
            Store::FifoFsStore { store } => store.is_ready(),
            Store::NoneStore { store } => store.is_ready(),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Store::LruFsStore { store } => store.close().await,
            Store::LfuFsStore { store } => store.close().await,
            Store::FifoFsStore { store } => store.close().await,
            Store::NoneStore { store } => store.close().await,
        }
    }

    fn writer(&self, key: Self::Key, weight: usize) -> Self::Writer {
        match self {
            Store::LruFsStore { store } => store.writer(key, weight).into(),
            Store::LfuFsStore { store } => store.writer(key, weight).into(),
            Store::FifoFsStore { store } => store.writer(key, weight).into(),
            Store::NoneStore { store } => store.writer(key, weight).into(),
        }
    }

    fn exists(&self, key: &Self::Key) -> Result<bool> {
        match self {
            Store::LruFsStore { store } => store.exists(key),
            Store::LfuFsStore { store } => store.exists(key),
            Store::FifoFsStore { store } => store.exists(key),
            Store::NoneStore { store } => store.exists(key),
        }
    }

    async fn lookup(&self, key: &Self::Key) -> Result<Option<Self::Value>> {
        match self {
            Store::LruFsStore { store } => store.lookup(key).await,
            Store::LfuFsStore { store } => store.lookup(key).await,
            Store::FifoFsStore { store } => store.lookup(key).await,
            Store::NoneStore { store } => store.lookup(key).await,
        }
    }

    fn remove(&self, key: &Self::Key) -> Result<bool> {
        match self {
            Store::LruFsStore { store } => store.remove(key),
            Store::LfuFsStore { store } => store.remove(key),
            Store::FifoFsStore { store } => store.remove(key),
            Store::NoneStore { store } => store.remove(key),
        }
    }

    fn clear(&self) -> Result<()> {
        match self {
            Store::LruFsStore { store } => store.clear(),
            Store::LfuFsStore { store } => store.clear(),
            Store::FifoFsStore { store } => store.clear(),
            Store::NoneStore { store } => store.clear(),
        }
    }
}
