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
    fs::{create_dir_all, File, OpenOptions},
    os::fd::{AsRawFd, RawFd},
    path::PathBuf,
    sync::Arc,
};

use crate::region::RegionId;

use super::{
    allocator::AlignedAllocator,
    asyncify,
    error::{Error, Result},
    Device, IoBuf, IoBufMut,
};
use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct FsDeviceConfig {
    /// base dir path
    pub dir: PathBuf,

    /// must be multipliers of `align` and `file_capacity`
    pub capacity: usize,

    /// must be multipliers of `align`
    pub file_capacity: usize,

    /// io block alignment, must be pow of 2
    pub align: usize,

    /// recommended optimized io block size
    pub io_size: usize,
}

impl FsDeviceConfig {
    pub fn verify(&self) {
        assert!(self.align.is_power_of_two());
        assert_eq!(self.file_capacity % self.align, 0);
        assert_eq!(self.capacity % self.file_capacity, 0);
    }
}

#[derive(Debug)]
struct FsDeviceInner {
    config: FsDeviceConfig,

    #[allow(unused)]
    dir: File,

    files: Vec<File>,

    io_buffer_allocator: AlignedAllocator,
}

#[derive(Debug, Clone)]
pub struct FsDevice {
    inner: Arc<FsDeviceInner>,
}

#[async_trait]
impl Device for FsDevice {
    type Config = FsDeviceConfig;
    type IoBufferAllocator = AlignedAllocator;

    async fn open(config: FsDeviceConfig) -> Result<Self> {
        Self::open(config).await
    }

    #[tracing::instrument(level = "trace", skip(self, buf))]
    async fn write(
        &self,
        buf: impl IoBuf,
        region: RegionId,
        offset: u64,
        len: usize,
    ) -> Result<usize> {
        assert!(offset as usize + len <= self.inner.config.file_capacity);

        let fd = self.fd(region);

        let res = asyncify(move || {
            let res = nix::sys::uio::pwrite(fd, &buf.as_ref()[..len], offset as i64)?;
            Ok(res)
        })
        .await?;

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self, buf))]
    async fn read(
        &self,
        mut buf: impl IoBufMut,
        region: RegionId,
        offset: u64,
        len: usize,
    ) -> Result<usize> {
        assert!(offset as usize + len <= self.inner.config.file_capacity);

        let fd = self.fd(region);

        let res = asyncify(move || {
            let res = nix::sys::uio::pread(fd, &mut buf.as_mut()[..len], offset as i64)?;
            Ok(res)
        })
        .await?;

        Ok(res)
    }

    #[cfg(target_os = "linux")]
    async fn flush(&self) -> Result<()> {
        let fd = self.inner.dir.as_raw_fd();
        // Commit fs cache to disk. Linux waits for I/O completions.
        //
        // See also [syncfs(2)](https://man7.org/linux/man-pages/man2/sync.2.html)
        asyncify(move || {
            nix::unistd::syncfs(fd)?;
            Ok(())
        })
        .await?;

        // TODO(MrCroxx): track dirty files and call fsync(2) on them on other target os.

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    async fn flush(&self) -> Result<()> {
        // TODO(MrCroxx): track dirty files and call fsync(2) on them on other target os.

        Ok(())
    }

    fn capacity(&self) -> usize {
        self.inner.config.capacity
    }

    fn regions(&self) -> usize {
        self.inner.files.len()
    }

    fn align(&self) -> usize {
        self.inner.config.align
    }

    fn io_size(&self) -> usize {
        self.inner.config.io_size
    }

    fn io_buffer_allocator(&self) -> &Self::IoBufferAllocator {
        &self.inner.io_buffer_allocator
    }

    fn io_buffer(&self, len: usize, capacity: usize) -> Vec<u8, Self::IoBufferAllocator> {
        assert!(len <= capacity);
        let mut buf = Vec::with_capacity_in(capacity, self.inner.io_buffer_allocator);
        unsafe { buf.set_len(len) };
        buf
    }
}

impl FsDevice {
    pub async fn open(config: FsDeviceConfig) -> Result<Self> {
        config.verify();

        // TODO(MrCroxx): write and read config to a manifest file for pinning

        let regions = config.capacity / config.file_capacity;

        let path = config.dir.clone();
        let dir = asyncify(move || {
            create_dir_all(&path)?;
            let dir = File::open(&path)?;
            Ok(dir)
        })
        .await?;

        let futures = (0..regions)
            .map(|i| {
                let path = config.dir.clone().join(Self::filename(i as RegionId));
                async move {
                    #[cfg(target_os = "linux")]
                    use std::os::unix::prelude::OpenOptionsExt;

                    let mut opts = OpenOptions::new();
                    opts.create(true);
                    opts.write(true);
                    opts.read(true);
                    #[cfg(target_os = "linux")]
                    opts.custom_flags(libc::O_DIRECT);

                    let file = opts.open(path).map_err(Error::io)?;

                    Ok::<File, Error>(file)
                }
            })
            .collect_vec();
        let files = try_join_all(futures).await?;

        let io_buffer_allocator = AlignedAllocator::new(config.align);

        let inner = FsDeviceInner {
            config,
            dir,
            files,
            io_buffer_allocator,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn fd(&self, region: RegionId) -> RawFd {
        self.inner.files[region as usize].as_raw_fd()
    }

    fn filename(region: RegionId) -> String {
        format!("foyer-cache-{:08}", region)
    }
}

#[cfg(test)]
mod tests {

    use bytes::BufMut;

    use crate::slice::{Slice, SliceMut};

    use super::*;

    const FILES: usize = 8;
    const FILE_CAPACITY: usize = 8 * 1024; // 8 KiB
    const CAPACITY: usize = FILES * FILE_CAPACITY; // 64 KiB
    const ALIGN: usize = 4 * 1024;

    #[tokio::test]
    async fn test_fs_device_simple() {
        let dir = tempfile::tempdir().unwrap();
        let config = FsDeviceConfig {
            dir: PathBuf::from(dir.path()),
            capacity: CAPACITY,
            file_capacity: FILE_CAPACITY,
            align: ALIGN,
            io_size: ALIGN,
        };
        let dev = FsDevice::open(config).await.unwrap();

        let mut wbuffer = dev.io_buffer(ALIGN, ALIGN);
        (&mut wbuffer[..]).put_slice(&[b'x'; ALIGN]);
        let mut rbuffer = dev.io_buffer(ALIGN, ALIGN);
        (&mut rbuffer[..]).put_slice(&[0; ALIGN]);

        let wbuf = unsafe { Slice::new(&wbuffer) };
        let rbuf = unsafe { SliceMut::new(&mut rbuffer) };

        dev.write(wbuf, 0, 0, ALIGN).await.unwrap();
        dev.read(rbuf, 0, 0, ALIGN).await.unwrap();

        assert_eq!(&wbuffer, &rbuffer);

        drop(wbuffer);
        drop(rbuffer);
    }
}
