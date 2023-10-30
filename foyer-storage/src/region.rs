//  Copyright 2023 MrCroxx
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

use bytes::{Buf, BufMut};
use foyer_common::erwlock::{ErwLock, ErwLockInner};
use parking_lot::{lock_api::ArcRwLockWriteGuard, RawRwLock, RwLockWriteGuard};
use std::{
    collections::btree_map::{BTreeMap, Entry},
    fmt::Debug,
    ops::RangeBounds,
    sync::Arc,
};
use tokio::sync::oneshot;
use tracing::instrument;

use crate::{
    device::{BufferAllocator, Device},
    error::Result,
    slice::SliceMut,
};

pub type RegionId = u32;

pub const REGION_MAGIC: u64 = 0x19970327;

#[derive(Debug)]
pub struct RegionHeader {
    /// magic number to decide a valid region
    pub magic: u64,
}

impl RegionHeader {
    pub fn write(&self, buf: &mut [u8]) {
        (&mut buf[..]).put_u64(self.magic);
    }

    pub fn read(buf: &[u8]) -> Self {
        let magic = (&buf[..]).get_u64();
        Self { magic }
    }
}

#[derive(Debug)]
pub struct RegionInner<A>
where
    A: BufferAllocator,
{
    readers: usize,

    #[expect(clippy::type_complexity)]
    waits: BTreeMap<(usize, usize), Vec<oneshot::Sender<Result<ReadSlice<A>>>>>,
}

#[derive(Debug, Clone)]
pub struct RegionInnerExclusiveRequire {
    can_read: bool,
}

impl<A: BufferAllocator> ErwLockInner for RegionInner<A> {
    type R = RegionInnerExclusiveRequire;

    fn is_exclusive(&self, require: &Self::R) -> bool {
        require.can_read || self.readers == 0
    }
}

#[derive(Debug, Clone)]
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,

    inner: ErwLock<RegionInner<D::IoBufferAllocator>>,

    device: D,
}

/// [`Region`] represents a contiguous aligned range on device and its optional dirty buffer.
///
/// [`Region`] may be in one of the following states:
///
/// - physical write : written by flushers with append pattern
/// - physical read  : read if entry is not in ring buffer
/// - reclaim        : happens after the region is evicted, must guarantee there is no writers or readers,
///                    *or in-flight writers or readers*
impl<D> Region<D>
where
    D: Device,
{
    pub fn new(id: RegionId, device: D) -> Self {
        let inner = RegionInner {
            readers: 0,

            waits: BTreeMap::new(),
        };
        Self {
            id,
            inner: ErwLock::new(inner),
            device,
        }
    }

    /// Load region data into a [`ReadSlice`].
    ///
    /// Data may be loaded ether from physical device or from dirty buffer.
    ///
    /// Use version `0` to skip version check.
    ///
    /// Returns `None` if verion mismatch or given range cannot be fully filled.
    #[tracing::instrument(skip(self, range), fields(start, end))]
    pub async fn load(
        &self,
        range: impl RangeBounds<usize>,
    ) -> Result<Option<ReadSlice<D::IoBufferAllocator>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => *i + 1,
            std::ops::Bound::Excluded(i) => *i,
            std::ops::Bound::Unbounded => self.device.region_size(),
        };

        let rx = {
            let mut inner = self.inner.write();

            // join wait map if exists
            let rx = match inner.waits.entry((start, end)) {
                Entry::Vacant(v) => {
                    v.insert(vec![]);
                    None
                }
                Entry::Occupied(mut o) => {
                    let (tx, rx) = oneshot::channel();
                    o.get_mut().push(tx);
                    Some(rx)
                }
            };

            inner.readers += 1;
            drop(inner);

            rx
        };

        // wait for result if joined into wait map
        if let Some(rx) = rx {
            return rx.await.map_err(anyhow::Error::from)?.map(Some);
        }

        // otherwise, read from device
        let region = self.id;
        let mut buf = self.device.io_buffer(end - start, end - start);

        let mut offset = 0;
        while start + offset < end {
            let len = std::cmp::min(self.device.io_size(), end - start - offset);
            tracing::trace!(
                "read region {} [{}..{}]",
                region,
                start + offset,
                start + offset + len
            );
            let s = unsafe { SliceMut::new(&mut buf[offset..offset + len]) };
            let (res, _s) = self
                .device
                .read(s, .., region, (start + offset) as u64)
                .await;
            let read = match res {
                Ok(bytes) => bytes,
                Err(e) => {
                    let mut inner = self.inner.write();
                    self.cleanup(&mut inner, start, end)?;
                    inner.readers -= 1;
                    return Err(e.into());
                }
            };
            if read != len {
                let mut inner = self.inner.write();
                self.cleanup(&mut inner, start, end)?;
                inner.readers -= 1;
                return Ok(None);
            }
            offset += len;
        }
        let buf = Arc::new(buf);

        let cleanup = {
            let inner = self.inner.clone();
            let f = move || {
                let mut guard = inner.write();
                guard.readers -= 1;
            };
            Box::new(f)
        };

        if let Some(txs) = self.inner.write().waits.remove(&(start, end)) {
            // TODO: handle error !!!!!!!!!!!
            for tx in txs {
                tx.send(Ok(ReadSlice {
                    buf: buf.clone(),
                    cleanup: Some(cleanup.clone()),
                }))
                .map_err(|_| anyhow::anyhow!("fail to send load result"))?;
            }
        }

        Ok(Some(ReadSlice {
            buf,
            cleanup: Some(cleanup),
        }))
    }

    #[instrument(skip(self))]
    pub async fn exclusive(
        &self,
        can_write: bool,
        can_read: bool,
    ) -> ArcRwLockWriteGuard<RawRwLock, RegionInner<D::IoBufferAllocator>> {
        self.inner
            .exclusive(&RegionInnerExclusiveRequire { can_read })
            .await
    }

    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    /// Cleanup waits.
    fn cleanup(
        &self,
        guard: &mut RwLockWriteGuard<'_, RegionInner<D::IoBufferAllocator>>,
        start: usize,
        end: usize,
    ) -> Result<()> {
        if let Some(txs) = guard.waits.remove(&(start, end)) {
            guard.readers -= txs.len();
            for tx in txs {
                tx.send(Err(anyhow::anyhow!("cancelled by previous error").into()))
                    .map_err(|_| anyhow::anyhow!("fail to cleanup waits"))?;
            }
        }
        Ok(())
    }
}

impl<A> RegionInner<A>
where
    A: BufferAllocator,
{
    pub fn readers(&self) -> usize {
        self.readers
    }
}

// read & write slice

pub trait CleanupFn = FnOnce() + Send + Sync + 'static;

pub struct ReadSlice<A>
where
    A: BufferAllocator,
{
    buf: Arc<Vec<u8, A>>,
    cleanup: Option<Box<dyn CleanupFn>>,
}

impl<A> Debug for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSlice")
            .field("len", &self.buf.len())
            .finish()
    }
}

impl<A> ReadSlice<A>
where
    A: BufferAllocator,
{
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<A> AsRef<[u8]> for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

impl<A> Drop for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn drop(&mut self) {
        if let Some(f) = self.cleanup.take() {
            f();
        }
    }
}
