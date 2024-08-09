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

pub mod either;
pub mod noop;
pub mod runtime;

use std::{
    borrow::Borrow,
    fmt::Debug,
    future::Future,
    hash::Hash,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;
use pin_project::pin_project;
use tokio::{runtime::Handle, sync::oneshot};

use crate::{device::monitor::DeviceStats, error::Result, serde::KvInfo, IoBytes};

/// [`WaitHandle`] is returned by some ops of the disk cache.
///
/// [`WaitHandle`] implies that the operation is already started asynchronously without the needs to poll the handle.
/// (That's why it is named with `Handle` instead of `Future`).
///
/// If there is needs to wait the asynchronous op to finish, the caller can poll the handle like any other futures.
#[pin_project]
pub struct WaitHandle<F> {
    #[pin]
    future: F,
}

impl<F> WaitHandle<F> {
    /// Create a new [`WaitHandle`] with the given future.
    ///
    /// The future MUST be cancel-safe and can run asynchronously.
    pub fn new(future: F) -> Self {
        Self { future }
    }
}

impl<F> Future for WaitHandle<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll(cx)
    }
}

/// The storage trait for the disk cache storage engine.
// TODO(MrCroxx): Remove this after in-memory cache event listener is removed.
#[allow(clippy::type_complexity)]
pub trait Storage: Send + Sync + 'static + Clone + Debug {
    /// Disk cache key type.
    type Key: StorageKey;
    /// Disk cache value type.
    type Value: StorageValue;
    /// Disk cache hash builder type.
    type BuildHasher: HashBuilder;
    /// Disk cache config type.
    type Config: Send + Debug + 'static;

    /// Open the disk cache with the given configurations.
    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = Result<Self>> + Send + 'static;

    /// Close the disk cache gracefully.
    ///
    /// `close` will wait for all ongoing flush and reclaim tasks to finish.
    #[must_use]
    fn close(&self) -> impl Future<Output = Result<()>> + Send;

    /// Push a in-memory cache entry to the disk cache write queue.
    fn enqueue(
        &self,
        entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        buffer: IoBytes,
        info: KvInfo,
        tx: oneshot::Sender<Result<bool>>,
    );

    /// Load a cache entry from the disk cache.
    ///
    /// `load` may return a false-positive result on entry key hash collision. It's the caller's responsibility to
    /// check if the returned key matches the given key.
    #[must_use]
    fn load<Q>(&self, key: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static;

    /// Delete the cache entry with the given key from the disk cache.
    fn delete<Q>(&self, key: &Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'static>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Check if the disk cache contains a cached entry with the given key.
    ///
    /// `contains` may return a false-positive result if there is a hash collision with the given key.
    fn may_contains<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Delete all cached entries of the disk cache.
    #[must_use]
    fn destroy(&self) -> impl Future<Output = Result<()>> + Send;

    /// Get the statistics information of the disk cache.
    fn stats(&self) -> Arc<DeviceStats>;

    /// Get disk cache runtime handle.
    ///
    /// The runtime is determined during the opening phase.
    fn runtime(&self) -> &Handle;
}
