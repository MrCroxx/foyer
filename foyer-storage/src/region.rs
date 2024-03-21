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
    collections::btree_map::{BTreeMap, Entry},
    fmt::Debug,
    ops::RangeBounds,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bytes::{Buf, BufMut};
use foyer_common::range::RangeBoundsExt;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::{
    device::{BufferAllocator, Device, DeviceExt},
    error::Result,
};

pub type RegionId = u32;

pub const REGION_MAGIC: u64 = 0x19970327;

#[derive(Debug)]
pub enum Version {
    V1,
}

impl Version {
    pub fn latest() -> Self {
        Self::V1
    }

    pub fn to_u64(&self) -> u64 {
        match self {
            Version::V1 => 1,
        }
    }
}

impl From<Version> for u64 {
    fn from(value: Version) -> Self {
        match value {
            Version::V1 => 1,
        }
    }
}

impl TryFrom<u64> for Version {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            v => Err(anyhow::anyhow!("invalid region format version: {}", v)),
        }
    }
}

#[derive(Debug)]
pub struct RegionHeader {
    /// magic number to decide a valid region
    pub magic: u64,
    /// format version
    pub version: Version,
}

impl RegionHeader {
    pub fn write(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.magic);
        buf.put_u64(self.version.to_u64());
    }

    pub fn read(mut buf: &[u8]) -> std::result::Result<Self, anyhow::Error> {
        let magic = buf.get_u64();
        if magic != REGION_MAGIC {
            return Err(anyhow::anyhow!(
                "region magic mismatch, magic: {}, expected: {}",
                magic,
                REGION_MAGIC
            ));
        }
        let version = buf.get_u64().try_into()?;
        Ok(Self { magic, version })
    }
}

#[derive(Debug)]
pub struct RegionInner<A>
where
    A: BufferAllocator,
{
    #[expect(clippy::type_complexity)]
    waits: BTreeMap<(usize, usize), Vec<oneshot::Sender<Result<Arc<Vec<u8, A>>>>>>,
}

#[derive(Debug, Clone)]
pub struct Region<D>
where
    D: Device,
{
    id: RegionId,

    inner: Arc<Mutex<RegionInner<D::IoBufferAllocator>>>,

    device: D,

    refs: Arc<AtomicUsize>,
}

impl<D> Region<D>
where
    D: Device,
{
    pub fn new(id: RegionId, device: D) -> Self {
        let inner = RegionInner { waits: BTreeMap::new() };
        Self {
            id,
            inner: Arc::new(Mutex::new(inner)),
            device,
            refs: Arc::new(AtomicUsize::default()),
        }
    }

    pub fn view(&self, offset: u32, len: u32) -> RegionView {
        self.refs.fetch_add(1, Ordering::SeqCst);
        RegionView {
            id: self.id,
            offset,
            len,
            refs: Arc::clone(&self.refs),
        }
    }

    pub fn refs(&self) -> &Arc<AtomicUsize> {
        &self.refs
    }

    /// Load region data by view from device.
    #[expect(clippy::type_complexity)]
    #[tracing::instrument(skip(self, view))]
    pub async fn load(&self, view: RegionView) -> Result<Option<Arc<Vec<u8, D::IoBufferAllocator>>>> {
        let res = self
            .load_range(view.offset as usize..view.offset as usize + view.len as usize)
            .await;
        // drop view after load finish
        drop(view);
        res
    }

    /// Load region data with given `range` from device.
    #[expect(clippy::type_complexity)]
    #[tracing::instrument(skip(self, range), fields(start, end))]
    pub async fn load_range(
        &self,
        range: impl RangeBounds<usize>,
    ) -> Result<Option<Arc<Vec<u8, D::IoBufferAllocator>>>> {
        let range = range.bounds(0..self.device.region_size());

        let rx = {
            let mut inner = self.inner.lock();

            // join wait map if exists
            let rx = match inner.waits.entry((range.start, range.end)) {
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

            drop(inner);

            rx
        };

        // wait for result if joined into wait map
        if let Some(rx) = rx {
            return rx.await.map_err(anyhow::Error::from)?.map(Some);
        }

        // otherwise, read from device
        let region = self.id;

        let buf = match self.device.load(region, range.start..range.end).await {
            Err(e) => {
                self.cleanup(range.start, range.end)?;
                return Err(e.into());
            }
            Ok(buf) if buf.len() != range.size().unwrap() => {
                self.cleanup(range.start, range.end)?;
                return Ok(None);
            }
            Ok(buf) => buf,
        };
        let buf = Arc::new(buf);

        if let Some(txs) = self.inner.lock().waits.remove(&(range.start, range.end)) {
            for tx in txs {
                tx.send(Ok(buf.clone())).unwrap()
            }
        }

        Ok(Some(buf))
    }

    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    /// Cleanup waits.
    fn cleanup(&self, start: usize, end: usize) -> Result<()> {
        if let Some(txs) = self.inner.lock().waits.remove(&(start, end)) {
            for tx in txs {
                tx.send(Err(anyhow::anyhow!("cancelled by previous error").into()))
                    .unwrap()
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct RegionView {
    id: RegionId,
    offset: u32,
    len: u32,
    refs: Arc<AtomicUsize>,
}

impl Clone for RegionView {
    fn clone(&self) -> Self {
        self.refs.fetch_add(1, Ordering::SeqCst);
        Self {
            id: self.id,
            offset: self.offset,
            len: self.len,
            refs: Arc::clone(&self.refs),
        }
    }
}

impl Drop for RegionView {
    fn drop(&mut self) {
        self.refs.fetch_sub(1, Ordering::SeqCst);
    }
}

impl RegionView {
    pub fn id(&self) -> &RegionId {
        &self.id
    }

    pub fn offset(&self) -> &u32 {
        &self.offset
    }

    pub fn len(&self) -> &u32 {
        &self.len
    }

    pub fn refs(&self) -> &Arc<AtomicUsize> {
        &self.refs
    }
}
