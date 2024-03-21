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

pub mod allocator;
pub mod error;
pub mod fs;

use std::{alloc::Allocator, fmt::Debug};

use error::DeviceResult;
use foyer_common::range::RangeBoundsExt;
use futures::Future;

use crate::region::RegionId;

pub trait BufferAllocator = Allocator + Clone + Send + Sync + 'static + Debug;
pub trait IoBuf = AsRef<[u8]> + Send + Sync + 'static + Debug;
pub trait IoBufMut = AsRef<[u8]> + AsMut<[u8]> + Send + Sync + 'static + Debug;
pub trait IoRange = RangeBoundsExt<usize> + Sized + Send + Sync + 'static + Debug;

pub trait Device: Sized + Clone + Send + Sync + 'static + Debug {
    type IoBufferAllocator: BufferAllocator;
    type Config: Send + Debug + Clone;

    #[must_use]
    fn open(config: Self::Config) -> impl Future<Output = DeviceResult<Self>> + Send;

    #[must_use]
    fn write<B>(
        &self,
        buf: B,
        range: impl IoRange,
        region: RegionId,
        offset: usize,
    ) -> impl Future<Output = (DeviceResult<usize>, B)> + Send
    where
        B: IoBuf;

    #[must_use]
    fn read<B>(
        &self,
        buf: B,
        range: impl IoRange,
        region: RegionId,
        offset: usize,
    ) -> impl Future<Output = (DeviceResult<usize>, B)> + Send
    where
        B: IoBufMut;

    #[must_use]
    fn flush(&self) -> impl Future<Output = DeviceResult<()>> + Send;

    fn capacity(&self) -> usize;

    fn regions(&self) -> usize;

    /// must be power of 2
    fn align(&self) -> usize;

    /// optimized io size
    fn io_size(&self) -> usize;

    fn io_buffer_allocator(&self) -> &Self::IoBufferAllocator;

    fn io_buffer(&self, len: usize, capacity: usize) -> Vec<u8, Self::IoBufferAllocator>;

    fn region_size(&self) -> usize {
        debug_assert!(self.capacity() % self.regions() == 0);
        self.capacity() / self.regions()
    }
}

pub trait DeviceExt: Device {
    #[must_use]
    fn load(
        &self,
        region: RegionId,
        range: impl IoRange,
    ) -> impl Future<Output = DeviceResult<Vec<u8, Self::IoBufferAllocator>>> + Send {
        async move {
            let range = range.bounds(0..self.region_size());
            let size = range.size().unwrap();
            debug_assert_eq!(size & (self.align() - 1), 0);

            let mut buf = self.io_buffer(size, size);
            let mut offset = 0;

            while range.start + offset < range.end {
                let len = std::cmp::min(self.io_size(), size - offset);
                let (res, b) = self.read(buf, offset..offset + len, region, range.start + offset).await;
                let bytes = res?;
                offset += bytes;
                buf = b;
                if bytes != len {
                    break;
                }
            }

            unsafe { buf.set_len(offset) };

            Ok(buf)
        }
    }
}

impl<D: Device> DeviceExt for D {}

#[cfg(not(madsim))]
#[tracing::instrument(level = "trace", skip(f))]
async fn asyncify<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

#[cfg(madsim)]
#[tracing::instrument(level = "trace", skip(f))]
async fn asyncify<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    f()
}

#[cfg(test)]
pub mod tests {
    use super::{allocator::AlignedAllocator, *};

    #[derive(Debug, Clone)]
    pub struct NullDevice(AlignedAllocator);

    impl NullDevice {
        pub fn new(align: usize) -> Self {
            Self(AlignedAllocator::new(align))
        }
    }

    impl Device for NullDevice {
        type Config = usize;
        type IoBufferAllocator = AlignedAllocator;

        async fn open(config: usize) -> DeviceResult<Self> {
            Ok(Self::new(config))
        }

        async fn write<B>(
            &self,
            buf: B,
            _range: impl IoRange,
            _region: RegionId,
            _offset: usize,
        ) -> (DeviceResult<usize>, B)
        where
            B: IoBuf,
        {
            (Ok(0), buf)
        }

        async fn read<B>(
            &self,
            buf: B,
            _range: impl IoRange,
            _region: RegionId,
            _offset: usize,
        ) -> (DeviceResult<usize>, B)
        where
            B: IoBufMut,
        {
            (Ok(0), buf)
        }

        async fn flush(&self) -> DeviceResult<()> {
            Ok(())
        }

        fn capacity(&self) -> usize {
            usize::MAX
        }

        fn regions(&self) -> usize {
            4096
        }

        fn align(&self) -> usize {
            4096
        }

        fn io_size(&self) -> usize {
            4096
        }

        fn io_buffer_allocator(&self) -> &Self::IoBufferAllocator {
            &self.0
        }

        fn io_buffer(&self, len: usize, capacity: usize) -> Vec<u8, Self::IoBufferAllocator> {
            let mut buf = Vec::with_capacity_in(capacity, self.0);
            unsafe { buf.set_len(len) };
            buf
        }
    }
}
