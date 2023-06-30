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

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut, RangeBounds},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures::Future;
use parking_lot::{lock_api::ArcRwLockWriteGuard, RawRwLock, RwLock};

use crate::{
    device::{BufferAllocator, Device},
    error::Result,
    slice::{Slice, SliceMut},
};

pub type RegionId = u32;
/// 0 matches any version
pub type Version = u32;

#[derive(Debug)]
pub struct RegionInner<A>
where
    A: BufferAllocator,
{
    version: Version,

    buffer: Option<Vec<u8, A>>,
    len: usize,
    capacity: usize,

    writers: usize,
    buffered_readers: usize,
    physical_readers: usize,

    wakers: Vec<Waker>,
}

#[derive(Debug)]
pub struct Region<A, D>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
{
    id: RegionId,

    inner: Arc<RwLock<RegionInner<A>>>,

    device: D,
}

/// [`Region`] represents a contiguous aligned range on device and its optional dirty buffer.
///
/// [`Region`] may be in one of the following states:
///
/// - buffered write : append-only buffer write, written parts can be read concurrently.
/// - buffered read  : happenes if the region is dirty with an attached dirty buffer
/// - physical read  : happenes if the region is clean, read directly from the devie
/// - flush          : happenes after the region dirty buffer is full, there are 2 steps when flushing
///                    step 1 writes dirty buffer to device, must guarantee there is no writers or physical readers
///                    step 2 detaches dirty buffer, must guarantee there is no buffer readers
/// - reclaim        : happens after the region is evicted, must guarantee there is no writers, buffer readers or physical readers,
///                    *or in-flight writers or readers* (verify by version)
impl<A, D> Region<A, D>
where
    A: BufferAllocator,
    D: Device<IoBufferAllocator = A>,
{
    pub fn new(id: RegionId, device: D) -> Self {
        let inner = RegionInner {
            version: 1,

            buffer: None,
            len: 0,
            capacity: device.region_size(),

            writers: 0,
            buffered_readers: 0,
            physical_readers: 0,

            wakers: vec![],
        };
        Self {
            id,
            inner: Arc::new(RwLock::new(inner)),
            device,
        }
    }

    pub fn allocate(&self, size: usize) -> Option<WriteSlice> {
        let callback = {
            let inner = self.inner.clone();
            move || {
                let mut guard = inner.write();
                guard.writers -= 1;
                guard.wake_all();
            }
        };

        let mut inner = self.inner.write();

        if inner.len + size > inner.capacity {
            return None;
        }

        inner.writers += 1;
        let version = inner.version;
        let offset = inner.len;
        inner.len += size;
        let buffer = inner.buffer.as_mut().unwrap();

        let slice = unsafe { SliceMut::new(&mut buffer[offset..offset + size]) };
        let region_id = self.id;

        let slice = WriteSlice {
            slice,
            region_id,
            version,
            offset,
            callback: Box::new(callback),
        };

        Some(slice)
    }

    pub async fn load(
        &self,
        range: impl RangeBounds<usize>,
        version: Version,
    ) -> Result<Option<ReadSlice<A>>> {
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

        // restrict guard lifetime
        {
            let mut inner = self.inner.write();
            if version != 0 && version != inner.version {
                return Ok(None);
            }

            // if buffer attached, buffered read

            if inner.buffer.is_some() {
                inner.buffered_readers += 1;
                let allocator = inner.buffer.as_ref().unwrap().allocator().clone();
                let slice = unsafe { Slice::new(&inner.buffer.as_ref().unwrap()[start..end]) };
                let callback = {
                    let inner = self.inner.clone();
                    move || {
                        let mut guard = inner.write();
                        guard.buffered_readers -= 1;
                        guard.wake_all();
                    }
                };
                return Ok(Some(ReadSlice::Slice {
                    slice,
                    allocator: Some(allocator),
                    callback: Box::new(callback),
                }));
            }

            // if buffer detached, physical read
            inner.physical_readers += 1;
            drop(inner);
        }

        let region = self.id;
        let mut buf = self.device.io_buffer(end - start, end - start);

        let mut offset = 0;
        while start + offset < end {
            let len = std::cmp::min(self.device.io_size(), end - start - offset);
            tracing::trace!(
                "physical read region {} [{}..{}]",
                region,
                start + offset,
                start + offset + len
            );
            let s = unsafe { SliceMut::new(&mut buf[offset..offset + len]) };
            self.device
                .read(s, region, (start + offset) as u64, len)
                .await?;
            offset += len;
        }

        let callback = {
            let inner = self.inner.clone();
            move || {
                let mut guard = inner.write();
                guard.physical_readers -= 1;
                guard.wake_all();
            }
        };
        Ok(Some(ReadSlice::Owned {
            buf: Some(buf),
            callback: Box::new(callback),
        }))
    }

    pub fn attach_buffer(&self, buf: Vec<u8, A>) {
        let mut inner = self.inner.write();

        assert_eq!(inner.writers, 0);
        assert_eq!(inner.buffered_readers, 0);

        inner.attach_buffer(buf);
    }

    pub fn detach_buffer(&self) -> Vec<u8, A> {
        let mut inner = self.inner.write();

        inner.detach_buffer()
    }

    pub fn has_buffer(&self) -> bool {
        let inner = self.inner.read();
        inner.has_buffer()
    }

    pub async fn exclusive(
        &self,
        can_write: bool,
        can_buffered_read: bool,
        can_physical_read: bool,
    ) -> ExclusiveGuard<A> {
        ExclusiveFuture {
            inner: self.inner.clone(),
            can_write,
            can_buffered_read,
            can_physical_read,
            is_waker_set: false,
        }
        .await
    }

    pub fn id(&self) -> RegionId {
        self.id
    }

    pub fn device(&self) -> &D {
        &self.device
    }

    pub fn version(&self) -> Version {
        self.inner.read().version
    }

    pub fn advance(&self) -> Version {
        let mut inner = self.inner.write();
        let res = inner.version;
        inner.version += 1;
        res
    }
}

impl<A> RegionInner<A>
where
    A: BufferAllocator,
{
    pub fn attach_buffer(&mut self, buf: Vec<u8, A>) {
        assert!(self.buffer.is_none());
        assert_eq!(buf.len(), buf.capacity());
        assert_eq!(buf.capacity(), self.capacity);
        self.buffer = Some(buf);
        self.len = 0;
    }

    pub fn detach_buffer(&mut self) -> Vec<u8, A> {
        self.buffer.take().unwrap()
    }

    pub fn has_buffer(&self) -> bool {
        self.buffer.is_some()
    }

    pub fn writers(&self) -> usize {
        self.writers
    }

    pub fn buffered_readers(&self) -> usize {
        self.buffered_readers
    }

    pub fn physical_readers(&self) -> usize {
        self.physical_readers
    }

    fn wake_all(&self) {
        for waker in self.wakers.iter() {
            waker.wake_by_ref();
        }
    }
}

// future

pub struct ExclusiveGuard<A: BufferAllocator> {
    inner: ArcRwLockWriteGuard<RawRwLock, RegionInner<A>>,
}

unsafe impl<A: BufferAllocator> Send for ExclusiveGuard<A> {}

impl<A: BufferAllocator> Deref for ExclusiveGuard<A> {
    type Target = RegionInner<A>;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<A: BufferAllocator> DerefMut for ExclusiveGuard<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.deref_mut()
    }
}

struct ExclusiveFuture<A>
where
    A: BufferAllocator,
{
    inner: Arc<RwLock<RegionInner<A>>>,

    can_write: bool,
    can_buffered_read: bool,
    can_physical_read: bool,

    is_waker_set: bool,
}

impl<A: BufferAllocator> ExclusiveFuture<A> {
    fn is_ready(&self, guard: &ArcRwLockWriteGuard<RawRwLock, RegionInner<A>>) -> bool {
        tracing::trace!("exclusive: [can write: {}, writers: {}] [can buffered read: {}, buffered readers: {}] [can physical read: {}, physical readers: {}]", self.can_write, guard.writers, self.can_buffered_read, guard.buffered_readers, self.can_physical_read, guard.physical_readers);
        (self.can_write || guard.writers == 0)
            && (self.can_buffered_read || guard.buffered_readers == 0)
            && (self.can_physical_read || guard.physical_readers == 0)
    }
}

impl<A: BufferAllocator> Future for ExclusiveFuture<A> {
    type Output = ExclusiveGuard<A>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.clone();
        let mut guard = inner.write_arc();
        let is_ready = self.is_ready(&guard);
        if is_ready {
            Poll::Ready(ExclusiveGuard { inner: guard })
        } else {
            if !self.is_waker_set {
                self.is_waker_set = true;
                guard.wakers.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}
// read & write slice

pub type DropCallback = Box<dyn Fn() + Send + Sync + 'static>;

pub struct WriteSlice {
    slice: SliceMut,
    region_id: RegionId,
    version: Version,
    offset: usize,
    callback: DropCallback,
}

impl Debug for WriteSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSlice")
            .field("slice", &self.slice)
            .field("region_id", &self.region_id)
            .field("version", &self.version)
            .field("offset", &self.offset)
            .finish()
    }
}

impl WriteSlice {
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn len(&self) -> usize {
        self.slice.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AsRef<[u8]> for WriteSlice {
    fn as_ref(&self) -> &[u8] {
        self.slice.as_ref()
    }
}

impl AsMut<[u8]> for WriteSlice {
    fn as_mut(&mut self) -> &mut [u8] {
        self.slice.as_mut()
    }
}

impl Drop for WriteSlice {
    fn drop(&mut self) {
        (self.callback)();
    }
}

pub enum ReadSlice<A>
where
    A: BufferAllocator,
{
    Slice {
        slice: Slice,
        allocator: Option<A>,
        callback: DropCallback,
    },
    Owned {
        buf: Option<Vec<u8, A>>,
        callback: DropCallback,
    },
}

impl<A> Debug for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Slice {
                slice, allocator, ..
            } => f
                .debug_struct("Slice")
                .field("slice", slice)
                .field("allocator", allocator)
                .finish(),
            Self::Owned { buf, .. } => f.debug_struct("Owned").field("buf", buf).finish(),
        }
    }
}

impl<A> ReadSlice<A>
where
    A: BufferAllocator,
{
    pub fn len(&self) -> usize {
        match self {
            Self::Slice { slice, .. } => slice.len(),
            Self::Owned { buf, .. } => buf.as_ref().unwrap().len(),
        }
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
        match self {
            Self::Slice { slice, .. } => slice.as_ref(),
            Self::Owned { buf, .. } => buf.as_ref().unwrap(),
        }
    }
}

impl<A> Drop for ReadSlice<A>
where
    A: BufferAllocator,
{
    fn drop(&mut self) {
        match self {
            Self::Slice { callback, .. } => (callback)(),
            Self::Owned { callback, .. } => (callback)(),
        }
    }
}