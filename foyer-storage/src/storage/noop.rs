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

use std::sync::Arc;
use std::{borrow::Borrow, fmt::Debug, future::Future, hash::Hash, marker::PhantomData};

use foyer_common::code::{HashBuilder, StorageKey, StorageValue};
use foyer_memory::CacheEntry;

use futures::future::ready;
use futures::FutureExt;
use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::device::monitor::DeviceStats;
use crate::serde::KvInfo;
use crate::storage::Storage;

use crate::error::Result;
use crate::IoBytes;

use super::WaitHandle;

pub struct Noop<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    runtime: Handle,
    _marker: PhantomData<(K, V, S)>,
}

impl<K, V, S> Debug for Noop<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NoneStore").finish()
    }
}

impl<K, V, S> Clone for Noop<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            _marker: PhantomData,
        }
    }
}

impl<K, V, S> Storage for Noop<K, V, S>
where
    K: StorageKey,
    V: StorageValue,
    S: HashBuilder + Debug,
{
    type Key = K;
    type Value = V;
    type BuildHasher = S;
    type Config = ();

    async fn open(_: Self::Config) -> Result<Self> {
        Ok(Self {
            runtime: Handle::current(),
            _marker: PhantomData,
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn enqueue(
        &self,
        _entry: CacheEntry<Self::Key, Self::Value, Self::BuildHasher>,
        _buffer: IoBytes,
        _info: KvInfo,
        tx: oneshot::Sender<Result<bool>>,
    ) {
        let _ = tx.send(Ok(false));
    }

    fn load<Q>(&self, _: &Q) -> impl Future<Output = Result<Option<(Self::Key, Self::Value)>>> + Send + 'static
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized + Send + Sync + 'static,
    {
        ready(Ok(None))
    }

    fn delete<Q>(&self, _: &Q) -> WaitHandle<impl Future<Output = Result<bool>> + Send + 'static>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(false));
        WaitHandle::new(rx.map(|recv| recv.unwrap()))
    }

    fn may_contains<Q>(&self, _: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        false
    }

    async fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> Arc<DeviceStats> {
        Arc::default()
    }

    fn runtime(&self) -> &Handle {
        &self.runtime
    }
}

#[cfg(test)]
mod tests {
    use foyer_memory::{Cache, CacheBuilder, FifoConfig};

    use super::*;

    fn cache_for_test() -> Cache<u64, Vec<u8>> {
        CacheBuilder::new(10)
            .with_eviction_config(FifoConfig::default())
            .build()
    }

    #[tokio::test]
    async fn test_none_store() {
        let memory = cache_for_test();
        let store = Noop::open(()).await.unwrap();
        let (tx, rx) = oneshot::channel();
        store.enqueue(
            memory.insert(0, vec![b'x'; 16384]),
            IoBytes::new(),
            KvInfo {
                key_len: 0,
                value_len: 0,
            },
            tx,
        );
        assert!(!rx.await.unwrap().unwrap());
        assert!(store.load(&0).await.unwrap().is_none());
        store.delete(&0).await.unwrap();
        store.destroy().await.unwrap();
        store.close().await.unwrap();
    }
}
