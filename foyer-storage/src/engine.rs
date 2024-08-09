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

use ahash::RandomState;
use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use futures::Future;
use std::{
    borrow::Borrow,
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::oneshot;

use crate::{
    error::Result,
    large::generic::{GenericLargeStorage, GenericLargeStorageConfig},
    serde::KvInfo,
    small::generic::{GenericSmallStorage, GenericSmallStorageConfig},
    storage::{
        either::{Either, EitherConfig, Selection, Selector},
        noop::Noop,
        runtime::{Runtime, RuntimeStoreConfig},
        WaitHandle,
    },
    DeviceStats, IoBytes, Storage,
};

pub struct SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    threshold: usize,
    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Debug for SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SizeSelector")
            .field("threshold", &self.threshold)
            .finish()
    }
}

impl<K, V, S> SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            _marker: PhantomData,
        }
    }
}

impl<K, V, S> Selector for SizeSelector<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;

    fn select(&self, _entry: &CacheEntry<Self::Key, Self::Value, Self::BuildHasher>, buffer: &IoBytes) -> Selection {
        if buffer.len() < self.threshold {
            Selection::Left
        } else {
            Selection::Right
        }
    }
}

enum StoreFuture<F1, F2, F3, F4, F5, F6, F7> {
    Noop(F1),
    Large(F2),
    LargeRuntime(F3),
    Small(F4),
    SmallRuntime(F5),
    Combined(F6),
    CombinedRuntime(F7),
}

impl<F1, F2, F3, F4, F5, F6, F7> StoreFuture<F1, F2, F3, F4, F5, F6, F7> {
    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(clippy::type_complexity)]
    pub fn as_pin_mut(
        self: Pin<&mut Self>,
    ) -> StoreFuture<Pin<&mut F1>, Pin<&mut F2>, Pin<&mut F3>, Pin<&mut F4>, Pin<&mut F5>, Pin<&mut F6>, Pin<&mut F7>>
    {
        unsafe {
            match *Pin::get_unchecked_mut(self) {
                StoreFuture::Noop(ref mut inner) => StoreFuture::Noop(Pin::new_unchecked(inner)),
                StoreFuture::Large(ref mut inner) => StoreFuture::Large(Pin::new_unchecked(inner)),
                StoreFuture::LargeRuntime(ref mut inner) => StoreFuture::LargeRuntime(Pin::new_unchecked(inner)),
                StoreFuture::Small(ref mut inner) => StoreFuture::Small(Pin::new_unchecked(inner)),
                StoreFuture::SmallRuntime(ref mut inner) => StoreFuture::SmallRuntime(Pin::new_unchecked(inner)),
                StoreFuture::Combined(ref mut inner) => StoreFuture::Combined(Pin::new_unchecked(inner)),
                StoreFuture::CombinedRuntime(ref mut inner) => StoreFuture::CombinedRuntime(Pin::new_unchecked(inner)),
            }
        }
    }
}

impl<F1, F2, F3, F4, F5, F6, F7> Future for StoreFuture<F1, F2, F3, F4, F5, F6, F7>
where
    F1: Future,
    F2: Future<Output = F1::Output>,
    F3: Future<Output = F1::Output>,
    F4: Future<Output = F1::Output>,
    F5: Future<Output = F1::Output>,
    F6: Future<Output = F1::Output>,
    F7: Future<Output = F1::Output>,
{
    type Output = F1::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_pin_mut() {
            StoreFuture::Noop(future) => future.poll(cx),
            StoreFuture::Large(future) => future.poll(cx),
            StoreFuture::LargeRuntime(future) => future.poll(cx),
            StoreFuture::Small(future) => future.poll(cx),
            StoreFuture::SmallRuntime(future) => future.poll(cx),
            StoreFuture::Combined(future) => future.poll(cx),
            StoreFuture::CombinedRuntime(future) => future.poll(cx),
        }
    }
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub enum EngineConfig<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    Noop,
    Large(GenericLargeStorageConfig<K, V, S>),
    LargeRuntime(RuntimeStoreConfig<GenericLargeStorage<K, V, S>>),
    Small(GenericSmallStorageConfig<K, V, S>),
    SmallRuntime(RuntimeStoreConfig<GenericSmallStorage<K, V, S>>),
    Combined(EitherConfig<K, V, S, GenericSmallStorage<K, V, S>, GenericLargeStorage<K, V, S>, SizeSelector<K, V, S>>),
    CombinedRuntime(
        RuntimeStoreConfig<
            Either<K, V, S, GenericSmallStorage<K, V, S>, GenericLargeStorage<K, V, S>, SizeSelector<K, V, S>>,
        >,
    ),
}

impl<K, V, S> Debug for EngineConfig<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "Noop"),
            Self::Large(config) => f.debug_tuple("Large").field(config).finish(),
            Self::LargeRuntime(config) => f.debug_tuple("LargeRuntime").field(config).finish(),
            Self::Small(config) => f.debug_tuple("Small").field(config).finish(),
            Self::SmallRuntime(config) => f.debug_tuple("SmallRuntime").field(config).finish(),
            Self::Combined(config) => f.debug_tuple("Combined").field(config).finish(),
            Self::CombinedRuntime(config) => f.debug_tuple("CombinedRuntime").field(config).finish(),
        }
    }
}

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#[allow(clippy::type_complexity)]
pub enum Engine<K, V, S = RandomState>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    /// No-op disk cache.
    Noop(Noop<K, V, S>),
    /// Large object disk cache.
    Large(GenericLargeStorage<K, V, S>),
    /// Large object disk cache with a dedicated runtime.
    LargeRuntime(Runtime<GenericLargeStorage<K, V, S>>),
    /// Small object disk cache.
    Small(GenericSmallStorage<K, V, S>),
    /// Small object disk cache with a dedicated runtime.
    SmallRuntime(Runtime<GenericSmallStorage<K, V, S>>),
    /// Combined large and small object disk cache.
    Combined(Either<K, V, S, GenericSmallStorage<K, V, S>, GenericLargeStorage<K, V, S>, SizeSelector<K, V, S>>),
    /// Combined large and small object disk cache with a dedicated runtime.
    CombinedRuntime(
        Runtime<Either<K, V, S, GenericSmallStorage<K, V, S>, GenericLargeStorage<K, V, S>, SizeSelector<K, V, S>>>,
    ),
}

impl<K, V, S> Debug for Engine<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop(storage) => f.debug_tuple("Noop").field(storage).finish(),
            Self::Large(storage) => f.debug_tuple("Large").field(storage).finish(),
            Self::LargeRuntime(storage) => f.debug_tuple("LargeRuntime").field(storage).finish(),
            Self::Small(storage) => f.debug_tuple("Small").field(storage).finish(),
            Self::SmallRuntime(storage) => f.debug_tuple("SmallRuntime").field(storage).finish(),
            Self::Combined(storage) => f.debug_tuple("Combined").field(storage).finish(),
            Self::CombinedRuntime(storage) => f.debug_tuple("CombinedRuntime").field(storage).finish(),
        }
    }
}

impl<K, V, S> Clone for Engine<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        match self {
            Self::Noop(storage) => Self::Noop(storage.clone()),
            Self::Large(storage) => Self::Large(storage.clone()),
            Self::LargeRuntime(storage) => Self::LargeRuntime(storage.clone()),
            Self::Small(storage) => Self::Small(storage.clone()),
            Self::SmallRuntime(storage) => Self::SmallRuntime(storage.clone()),
            Self::Combined(storage) => Self::Combined(storage.clone()),
            Self::CombinedRuntime(storage) => Self::CombinedRuntime(storage.clone()),
        }
    }
}

impl<K, V, S> Storage for Engine<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = EngineConfig<K, V, S>;

    async fn open(config: Self::Config) -> Result<Self> {
        match config {
            EngineConfig::Noop => Ok(Self::Noop(Noop::open(()).await?)),
            EngineConfig::Large(config) => Ok(Self::Large(GenericLargeStorage::open(config).await?)),
            EngineConfig::LargeRuntime(config) => Ok(Self::LargeRuntime(Runtime::open(config).await?)),
            EngineConfig::Small(config) => Ok(Self::Small(GenericSmallStorage::open(config).await?)),
            EngineConfig::SmallRuntime(config) => Ok(Self::SmallRuntime(Runtime::open(config).await?)),
            EngineConfig::Combined(config) => Ok(Self::Combined(Either::open(config).await?)),
            EngineConfig::CombinedRuntime(config) => Ok(Self::CombinedRuntime(Runtime::open(config).await?)),
        }
    }

    async fn close(&self) -> Result<()> {
        match self {
            Engine::Noop(storage) => storage.close().await,
            Engine::Large(storage) => storage.close().await,
            Engine::LargeRuntime(storage) => storage.close().await,
            Engine::Small(storage) => storage.close().await,
            Engine::SmallRuntime(storage) => storage.close().await,
            Engine::Combined(storage) => storage.close().await,
            Engine::CombinedRuntime(storage) => storage.close().await,
        }
    }

    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        buffer: IoBytes,
        info: KvInfo,
        tx: oneshot::Sender<Result<bool>>,
    ) {
        match self {
            Engine::Noop(storage) => storage.enqueue(entry, buffer, info, tx),
            Engine::Large(storage) => storage.enqueue(entry, buffer, info, tx),
            Engine::LargeRuntime(storage) => storage.enqueue(entry, buffer, info, tx),
            Engine::Small(storage) => storage.enqueue(entry, buffer, info, tx),
            Engine::SmallRuntime(storage) => storage.enqueue(entry, buffer, info, tx),
            Engine::Combined(storage) => storage.enqueue(entry, buffer, info, tx),
            Engine::CombinedRuntime(storage) => storage.enqueue(entry, buffer, info, tx),
        }
    }

    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        match self {
            Engine::Noop(storage) => StoreFuture::Noop(storage.load(key)),
            Engine::Large(storage) => StoreFuture::Large(storage.load(key)),
            Engine::LargeRuntime(storage) => StoreFuture::LargeRuntime(storage.load(key)),
            Engine::Small(storage) => StoreFuture::Small(storage.load(key)),
            Engine::SmallRuntime(storage) => StoreFuture::SmallRuntime(storage.load(key)),
            Engine::Combined(storage) => StoreFuture::Combined(storage.load(key)),
            Engine::CombinedRuntime(storage) => StoreFuture::CombinedRuntime(storage.load(key)),
        }
    }

    fn delete<Q>(&self, key: &Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'static>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Engine::Noop(storage) => WaitHandle::new(StoreFuture::Noop(storage.delete(key))),
            Engine::Large(storage) => WaitHandle::new(StoreFuture::Large(storage.delete(key))),
            Engine::LargeRuntime(storage) => WaitHandle::new(StoreFuture::LargeRuntime(storage.delete(key))),
            Engine::Small(storage) => WaitHandle::new(StoreFuture::Small(storage.delete(key))),
            Engine::SmallRuntime(storage) => WaitHandle::new(StoreFuture::SmallRuntime(storage.delete(key))),
            Engine::Combined(storage) => WaitHandle::new(StoreFuture::Combined(storage.delete(key))),
            Engine::CombinedRuntime(storage) => WaitHandle::new(StoreFuture::CombinedRuntime(storage.delete(key))),
        }
    }

    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self {
            Engine::Noop(storage) => storage.may_contains(key),
            Engine::Large(storage) => storage.may_contains(key),
            Engine::LargeRuntime(storage) => storage.may_contains(key),
            Engine::Small(storage) => storage.may_contains(key),
            Engine::SmallRuntime(storage) => storage.may_contains(key),
            Engine::Combined(storage) => storage.may_contains(key),
            Engine::CombinedRuntime(storage) => storage.may_contains(key),
        }
    }

    async fn destroy(&self) -> Result<()> {
        match self {
            Engine::Noop(storage) => storage.destroy().await,
            Engine::Large(storage) => storage.destroy().await,
            Engine::LargeRuntime(storage) => storage.destroy().await,
            Engine::Small(storage) => storage.destroy().await,
            Engine::SmallRuntime(storage) => storage.destroy().await,
            Engine::Combined(storage) => storage.destroy().await,
            Engine::CombinedRuntime(storage) => storage.destroy().await,
        }
    }

    fn stats(&self) -> Arc<DeviceStats> {
        match self {
            Engine::Noop(storage) => storage.stats(),
            Engine::Large(storage) => storage.stats(),
            Engine::LargeRuntime(storage) => storage.stats(),
            Engine::Small(storage) => storage.stats(),
            Engine::SmallRuntime(storage) => storage.stats(),
            Engine::Combined(storage) => storage.stats(),
            Engine::CombinedRuntime(storage) => storage.stats(),
        }
    }

    fn runtime(&self) -> &tokio::runtime::Handle {
        match self {
            Engine::Noop(storage) => storage.runtime(),
            Engine::Large(storage) => storage.runtime(),
            Engine::LargeRuntime(storage) => storage.runtime(),
            Engine::Small(storage) => storage.runtime(),
            Engine::SmallRuntime(storage) => storage.runtime(),
            Engine::Combined(storage) => storage.runtime(),
            Engine::CombinedRuntime(storage) => storage.runtime(),
        }
    }
}
