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

use std::fmt::Debug;

use foyer_common::{
    bits::{align_up, is_aligned},
    code::{Cursor, Key, Value},
};

use crate::{
    compress::Compression,
    device::{error::DeviceError, Device},
    flusher::Entry,
    generic::{checksum, EntryHeader},
    region::{RegionHeader, RegionId, Version, REGION_MAGIC},
};

#[derive(thiserror::Error, Debug)]
pub enum BufferError<R>
where
    R: Send + Sync + 'static + Debug,
{
    #[error("need rotate and retry {0}")]
    NeedRotate(Box<R>),
    #[error("device error: {0}")]
    Device(#[from] DeviceError),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type BufferResult<T, R> = core::result::Result<T, BufferError<R>>;

#[derive(Debug)]
pub struct PositionedEntry<K, V>
where
    K: Key,
    V: Value,
{
    pub entry: Entry<K, V>,
    pub region: RegionId,
    pub offset: usize,
    pub len: usize,
}

#[derive(Debug)]
pub struct FlushBuffer<K, V, D>
where
    K: Key,
    V: Value,
    D: Device,
{
    // TODO(MrCroxx): optimize buffer allocation
    /// io buffer
    buffer: Vec<u8, D::IoBufferAllocator>,

    /// current writing region
    region: Option<RegionId>,

    /// current buffer offset of current writing region
    offset: usize,

    /// entries in io buffer waiting for flush
    entries: Vec<PositionedEntry<K, V>>,

    // underlying device
    device: D,

    default_buffer_capacity: usize,
}

impl<K, V, D> FlushBuffer<K, V, D>
where
    K: Key,
    V: Value,
    D: Device,
{
    pub fn new(device: D) -> Self {
        let default_buffer_capacity = align_up(device.align(), device.io_size() + device.io_size() / 2);
        let buffer = device.io_buffer(0, default_buffer_capacity);
        Self {
            buffer,
            region: None,
            offset: 0,
            entries: vec![],
            device,
            default_buffer_capacity,
        }
    }

    pub fn region(&self) -> Option<RegionId> {
        self.region
    }

    pub fn remaining(&self) -> usize {
        if self.region.is_none() {
            0
        } else {
            self.device
                .region_size()
                .saturating_sub(self.offset + self.buffer.len())
        }
    }

    /// Flush io buffer if necessary, and reset io buffer to a new region.
    ///
    /// Returns fully flushed entries.
    pub async fn rotate(&mut self, region: RegionId) -> BufferResult<Vec<PositionedEntry<K, V>>, Entry<K, V>> {
        let entries = self.flush().await?;
        debug_assert!(self.buffer.is_empty());
        self.region = Some(region);
        self.offset = 0;

        // write region header
        unsafe { self.buffer.set_len(self.device.align()) };
        let header = RegionHeader {
            magic: REGION_MAGIC,
            version: Version::latest(),
        };
        header.write(&mut self.buffer[..]);
        debug_assert_eq!(self.buffer.len(), self.device.align());

        Ok(entries)
    }

    /// Flush io buffer and move the io buffer to the next position.
    ///
    /// The io buffer will be cleared after flush.
    ///
    /// Returns fully flushed entries.
    pub async fn flush(&mut self) -> BufferResult<Vec<PositionedEntry<K, V>>, Entry<K, V>> {
        let Some(region) = self.region else {
            debug_assert!(self.entries.is_empty());
            return Ok(vec![]);
        };

        // align io buffer
        let len = align_up(self.device.align(), self.buffer.len());
        debug_assert!(len <= self.buffer.capacity());
        unsafe { self.buffer.set_len(len) };
        debug_assert!(self.offset + self.buffer.len() <= self.device.region_size());

        // flush and clear buffer
        let mut buf = self.device.io_buffer(0, self.default_buffer_capacity);
        std::mem::swap(&mut self.buffer, &mut buf);

        let (res, _buf) = self.device.write(buf, .., region, self.offset).await;
        res?;

        // advance io buffer
        self.offset += len;
        if self.offset == self.device.region_size() {
            self.region = None;
        }

        let mut entries = vec![];
        std::mem::swap(&mut self.entries, &mut entries);
        Ok(entries)
    }

    /// Write entry to io buffer.
    ///
    /// The io buffer may be flushed if buffer size equals or exceeds device io size.
    ///
    /// Returns fully flushed entries if there is enough space in the current region.
    /// Otherwise, returns `NotEnough` error with the given `entry`.
    ///
    /// # Format
    ///
    /// | header | value (compressed) | key | <padding> |
    #[expect(clippy::uninit_vec)]
    pub async fn write(
        &mut self,
        Entry {
            key,
            value,
            sequence,
            compression,
        }: Entry<K, V>,
    ) -> BufferResult<Vec<PositionedEntry<K, V>>, Entry<K, V>> {
        // Notify caller to rotate buffer if there is not enough space for the entry.
        //
        // NOTICE:
        //
        // Buffer remaining size is not compared here because the compressed entry size can be
        // either larger (rarely) or smaller than the uncompressed size and it can not be determined
        // before compression. So we first try to compress it and rollback if it exceeds region size.
        //
        // P.S. About rollback, see (*).
        if self.region.is_none() {
            return Err(BufferError::NeedRotate(Box::new(Entry {
                key,
                value,
                sequence,
                compression,
            })));
        }

        let old = self.buffer.len();
        debug_assert!(is_aligned(self.device.align(), old));

        // reserve underlying buffer to reduce reallocation
        let uncompressed = align_up(
            self.device.align(),
            EntryHeader::serialized_len() + key.serialized_len() + value.serialized_len(),
        );
        self.buffer.reserve(old + uncompressed);

        let mut cursor = old;
        // reserve space for header
        cursor += EntryHeader::serialized_len();
        unsafe { self.buffer.set_len(cursor) };

        // write value
        let mut vcursor = value.into_cursor();
        match compression {
            Compression::None => {
                std::io::copy(&mut vcursor, &mut self.buffer).map_err(DeviceError::from)?;
            }
            Compression::Zstd => {
                zstd::stream::copy_encode(&mut vcursor, &mut self.buffer, 0).map_err(DeviceError::from)?;
            }
            Compression::Lz4 => {
                let mut encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .build(&mut self.buffer)
                    .map_err(DeviceError::from)?;
                std::io::copy(&mut vcursor, &mut encoder).map_err(DeviceError::from)?;
                let (_w, res) = encoder.finish();
                res.map_err(DeviceError::from)?;
            }
        }
        let compressed_value_len = self.buffer.len() - cursor;
        cursor = self.buffer.len();

        // write key
        let mut kcursor = key.into_cursor();
        std::io::copy(&mut kcursor, &mut self.buffer).map_err(DeviceError::from)?;
        let encoded_key_len = self.buffer.len() - cursor;
        cursor = self.buffer.len();

        // calculate checksum
        cursor -= compressed_value_len + encoded_key_len;
        let checksum = checksum(&self.buffer[cursor..cursor + compressed_value_len + encoded_key_len]);

        // write entry header
        cursor -= EntryHeader::serialized_len();
        let header = EntryHeader {
            key_len: encoded_key_len as u32,
            value_len: compressed_value_len as u32,
            sequence,
            compression,
            checksum,
        };
        header.write(&mut self.buffer[cursor..cursor + EntryHeader::serialized_len()]);

        // (*) if size exceeds region limit, rollback write and return
        if self.offset + self.buffer.len() > self.device.region_size() {
            unsafe { self.buffer.set_len(old) };
            let key = kcursor.into_inner();
            let value = vcursor.into_inner();
            return Err(BufferError::NeedRotate(Box::new(Entry {
                key,
                value,
                sequence,
                compression,
            })));
        }

        // 3. align buffer size
        let target = align_up(self.device.align(), self.buffer.len());
        self.buffer.reserve(target - self.buffer.len());
        unsafe { self.buffer.set_len(target) }

        let key = kcursor.into_inner();
        let value = vcursor.into_inner();

        self.entries.push(PositionedEntry {
            entry: Entry {
                key,
                value,
                sequence,
                compression,
            },
            region: self.region.unwrap(),
            offset: self.offset + old,
            len: self.buffer.len() - old,
        });

        // flush if buffer equals or exceeds device io size
        let entries = if self.buffer.len() >= self.device.io_size() || self.remaining() == 0 {
            self.flush().await?
        } else {
            vec![]
        };

        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::device::fs::{FsDevice, FsDeviceConfig};

    fn ent(size: usize) -> Entry<(), Vec<u8>> {
        Entry {
            key: (),
            value: vec![b'x'; size],
            compression: Compression::None,
            sequence: 0,
        }
    }

    #[tokio::test]
    async fn test_flush_buffer() {
        let tempdir = tempdir().unwrap();

        let device = FsDevice::open(FsDeviceConfig {
            dir: tempdir.path().into(),
            capacity: 256 * 1024,     // 256 KiB
            file_capacity: 64 * 1024, // 64 KiB
            align: 4 * 1024,          // 4 KiB
            io_size: 16 * 1024,       // 16 KiB
        })
        .await
        .unwrap();

        let mut buffer = FlushBuffer::new(device.clone());
        assert_eq!(buffer.region(), None);

        const HEADER: usize = EntryHeader::serialized_len();

        {
            let entry = ent(5 * 1024 - 128); // ~ 5 KiB

            let res = buffer.write(entry).await;
            let entry = match res {
                Err(BufferError::NeedRotate(entry)) => Box::into_inner(entry),
                _ => panic!("should be not enough error"),
            };

            let entries = buffer.rotate(0).await.unwrap();
            assert!(entries.is_empty());

            // 4 ~ 12 KiB
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert!(entries.is_empty());
            // 12 ~ 20 KiB
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].offset, 4 * 1024);
            assert_eq!(entries[1].offset, 12 * 1024);

            // 20 ~ 28 KiB
            let entries = buffer.write(entry.clone()).await.unwrap();
            assert!(entries.is_empty());
            let entries = buffer.flush().await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 20 * 1024);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 0, 0).await;
            res.unwrap();
            assert_eq!(
                &buf[HEADER + 4 * 1024..HEADER + 9 * 1024 - 128],
                &[b'x'; 5 * 1024 - 128]
            );
            assert_eq!(
                &buf[HEADER + 12 * 1024..HEADER + 17 * 1024 - 128],
                &[b'x'; 5 * 1024 - 128]
            );
            assert_eq!(
                &buf[HEADER + 20 * 1024..HEADER + 25 * 1024 - 128],
                &[b'x'; 5 * 1024 - 128]
            );

            assert!(buffer.entries.is_empty());
        }

        {
            let entry = ent(54 * 1024 - 128); // ~ 54 KiB

            let res = buffer.write(entry).await;
            let entry = match res {
                Err(BufferError::NeedRotate(entry)) => Box::into_inner(entry),
                _ => panic!("should be not enough error"),
            };

            let entries = buffer.rotate(1).await.unwrap();
            assert!(entries.is_empty());

            // 4 ~ 60 KiB
            let entries = buffer.write(entry).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 4 * 1024);

            let entry = ent(3 * 1024 - 128); // ~ 3 KiB

            // 60 ~ 64 KiB
            let entries = buffer.write(entry).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].offset, 60 * 1024);

            let buf = device.io_buffer(64 * 1024, 64 * 1024);
            let (res, buf) = device.read(buf, .., 1, 0).await;
            res.unwrap();
            assert_eq!(
                &buf[HEADER + 4 * 1024..HEADER + 58 * 1024 - 128],
                &[b'x'; 54 * 1024 - 128]
            );
            assert_eq!(
                &buf[HEADER + 60 * 1024..HEADER + 63 * 1024 - 128],
                &[b'x'; 3 * 1024 - 128]
            );

            assert!(buffer.entries.is_empty());
        }
    }
}
