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

use std::{
    fs::{create_dir_all, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use allocator_api2::vec::Vec as VecA;
use foyer_common::{fs::freespace, range::RangeBoundsExt};
use futures::future::try_join_all;
use itertools::Itertools;

use super::{allocator::AlignedAllocator, asyncify, Device, DeviceError, DeviceResult, IoBuf, IoBufMut, IoRange};
use crate::region::RegionId;

#[derive(Debug)]
pub struct FsDeviceConfigBuilder {
    pub dir: PathBuf,
    pub capacity: Option<usize>,
    pub file_size: Option<usize>,
    pub align: Option<usize>,
    pub io_size: Option<usize>,
}

impl FsDeviceConfigBuilder {
    const DEFAULT_ALIGN: usize = 4096;
    const DEFAULT_IO_SIZE: usize = 16 * 1024;
    const DEFAULT_FILE_SIZE: usize = 64 * 1024 * 1024;

    pub fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref().into();
        Self {
            dir,
            capacity: None,
            file_size: None,
            align: None,
            io_size: None,
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = Some(capacity);
        self
    }

    pub fn with_file_size(mut self, file_size: usize) -> Self {
        self.file_size = Some(file_size);
        self
    }

    pub fn with_align(mut self, align: usize) -> Self {
        self.align = Some(align);
        self
    }

    pub fn with_io_size(mut self, io_size: usize) -> Self {
        self.io_size = Some(io_size);
        self
    }

    pub fn build(self) -> FsDeviceConfig {
        let align_v = |value: usize, align: usize| value - value % align;

        let dir = self.dir;

        let align = self.align.unwrap_or(Self::DEFAULT_ALIGN);

        let capacity = self.capacity.unwrap_or({
            // Create an empty directory before to get freespace.
            create_dir_all(&dir).unwrap();
            freespace(&dir).unwrap() / 10 * 8
        });
        let capacity = align_v(capacity, align);

        let file_size = self.file_size.unwrap_or(Self::DEFAULT_FILE_SIZE).clamp(align, capacity);
        let file_size = align_v(file_size, align);

        let capacity = align_v(capacity, file_size);

        let io_size = self.io_size.unwrap_or(Self::DEFAULT_IO_SIZE).max(align);
        let io_size = align_v(io_size, align);

        FsDeviceConfig {
            dir,
            capacity,
            file_size,
            align,
            io_size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FsDeviceConfig {
    /// base dir path
    pub dir: PathBuf,

    /// must be multipliers of `align` and `file_capacity`
    pub capacity: usize,

    /// must be multipliers of `align`
    pub file_size: usize,

    /// io block alignment, must be pow of 2
    pub align: usize,

    /// recommended optimized io block size
    pub io_size: usize,
}

impl FsDeviceConfig {
    pub fn assert(&self) {
        assert!(self.align.is_power_of_two());
        assert_eq!(self.file_size % self.align, 0);
        assert_eq!(self.capacity % self.file_size, 0);
    }
}

#[derive(Debug)]
struct FsDeviceInner {
    config: FsDeviceConfig,

    _dir: File,

    files: Vec<Arc<File>>,

    io_buffer_allocator: AlignedAllocator,
}

#[derive(Debug, Clone)]
pub struct FsDevice {
    inner: Arc<FsDeviceInner>,
}

impl Device for FsDevice {
    type Config = FsDeviceConfig;
    type IoBufferAllocator = AlignedAllocator;

    async fn open(config: FsDeviceConfig) -> DeviceResult<Self> {
        Self::open(config).await
    }

    async fn write<B>(&self, buf: B, range: impl IoRange, region: RegionId, offset: usize) -> (DeviceResult<usize>, B)
    where
        B: IoBuf,
    {
        let file_capacity = self.inner.config.file_size;

        let range = range.bounds(0..buf.as_ref().len());
        let len = RangeBoundsExt::size(&range).unwrap();

        assert!(
            offset + len <= file_capacity,
            "offset ({offset}) + len ({len}) = {} <= file capacity ({file_capacity})",
            offset + len
        );

        let file = self.file(region).clone();
        asyncify(move || {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs::FileExt;

            #[cfg(target_family = "windows")]
            use std::os::windows::fs::FileExt;

            let res = file
                .write_at(&buf.as_ref()[range], offset as u64)
                .map_err(DeviceError::from);
            (res, buf)
        })
        .await
    }

    async fn read<B>(
        &self,
        mut buf: B,
        range: impl IoRange,
        region: RegionId,
        offset: usize,
    ) -> (DeviceResult<usize>, B)
    where
        B: IoBufMut,
    {
        let file_capacity = self.inner.config.file_size;

        let range = range.bounds(0..buf.as_ref().len());
        let len = RangeBoundsExt::size(&range).unwrap();

        assert!(
            offset + len <= file_capacity,
            "offset ({offset}) + len ({len}) <= file capacity ({file_capacity})"
        );

        let file = self.file(region).clone();
        asyncify(move || {
            #[cfg(target_family = "unix")]
            use std::os::unix::fs::FileExt;

            #[cfg(target_family = "windows")]
            use std::os::windows::fs::FileExt;

            let res = file
                .read_at(&mut buf.as_mut()[range], offset as u64)
                .map_err(DeviceError::from);
            (res, buf)
        })
        .await
    }

    async fn flush_region(&self, region: RegionId) -> DeviceResult<()> {
        let file = self.file(region).clone();
        asyncify(move || file.sync_all().map_err(DeviceError::from)).await
    }

    async fn flush(&self) -> DeviceResult<()> {
        let futures = (0..self.regions() as RegionId).map(|region| self.flush_region(region));
        try_join_all(futures).await.map(|_| ())
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

    fn io_buffer(&self, len: usize, capacity: usize) -> VecA<u8, Self::IoBufferAllocator> {
        assert!(len <= capacity);
        let mut buf = VecA::with_capacity_in(capacity, self.inner.io_buffer_allocator);
        unsafe { buf.set_len(len) };
        buf
    }
}

impl FsDevice {
    pub const PREFIX: &'static str = "foyer-cache-";

    pub async fn open(config: FsDeviceConfig) -> DeviceResult<Self> {
        config.assert();

        // TODO(MrCroxx): write and read config to a manifest file for pinning

        let regions = config.capacity / config.file_size;

        let path = config.dir.clone();
        let dir = asyncify(move || {
            create_dir_all(&path)?;
            File::open(&path).map_err(DeviceError::from)
        })
        .await?;

        let futures = (0..regions)
            .map(|i| {
                let path = config.dir.clone().join(Self::filename(i as RegionId));
                async move {
                    let mut opts = OpenOptions::new();

                    opts.create(true).write(true).read(true);

                    #[cfg(target_os = "linux")]
                    {
                        use std::os::unix::fs::OpenOptionsExt;
                        opts.custom_flags(libc::O_DIRECT);
                    }

                    let file = opts.open(path)?;
                    let file = Arc::new(file);

                    Ok::<_, DeviceError>(file)
                }
            })
            .collect_vec();
        let files = try_join_all(futures).await?;

        let io_buffer_allocator = AlignedAllocator::new(config.align);

        let inner = FsDeviceInner {
            config,
            _dir: dir,
            files,
            io_buffer_allocator,
        };

        Ok(Self { inner: Arc::new(inner) })
    }

    fn file(&self, region: RegionId) -> &Arc<File> {
        &self.inner.files[region as usize]
    }

    fn filename(region: RegionId) -> String {
        format!("{}{:08}", Self::PREFIX, region)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

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
            file_size: FILE_CAPACITY,
            align: ALIGN,
            io_size: ALIGN,
        };
        let dev = FsDevice::open(config).await.unwrap();

        let mut wbuffer = dev.io_buffer(ALIGN, ALIGN);
        (&mut wbuffer[..]).put_slice(&[b'x'; ALIGN]);
        let mut rbuffer = dev.io_buffer(ALIGN, ALIGN);
        (&mut rbuffer[..]).put_slice(&[0; ALIGN]);

        let (res, wbuffer) = dev.write(wbuffer, .., 0, 0).await;
        res.unwrap();
        let (res, rbuffer) = dev.read(rbuffer, .., 0, 0).await;
        res.unwrap();

        assert_eq!(&wbuffer, &rbuffer);

        drop(wbuffer);
        drop(rbuffer);
    }

    #[test]
    fn test_config_builder() {
        let dir = tempfile::tempdir().unwrap();

        let config = FsDeviceConfigBuilder::new(dir.path()).build();

        println!("{config:?}");

        config.assert();
    }

    #[test]
    fn test_config_builder_noent() {
        let dir = tempfile::tempdir().unwrap();

        let config = FsDeviceConfigBuilder::new(dir.path().join("noent")).build();

        println!("{config:?}");

        config.assert();
    }
}
