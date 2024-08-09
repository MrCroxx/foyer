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
    collections::HashSet,
    fmt::Debug,
    ops::{Deref, DerefMut, Range},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Buf;
use foyer_common::code::{StorageKey, StorageValue};

use crate::{
    error::Result,
    serde::{Checksummer, EntryDeserializer},
    IoBytes, IoBytesMut,
};

use super::serde::EntryHeader;

pub type SetId = u64;

#[derive(Debug)]
pub struct Set {
    storage: Arc<SetStorage>,
}

impl Deref for Set {
    type Target = SetStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl Set {
    pub fn new(storage: Arc<SetStorage>) -> Self {
        Self { storage }
    }
}

#[derive(Debug)]
pub struct SetMut {
    storage: SetStorage,
}

impl Deref for SetMut {
    type Target = SetStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl DerefMut for SetMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

impl SetMut {
    pub fn new(storage: SetStorage) -> Self {
        Self { storage }
    }

    pub fn into_storage(self) -> SetStorage {
        self.storage
    }
}

/// # Format
///
/// ```plain
/// | checksum (4B) | timestamp (8B) | len (4B) |
/// ```
pub struct SetStorage {
    /// Set checksum.
    checksum: u32,

    /// Set written data length.
    len: usize,
    /// Set data length capacity.
    capacity: usize,
    /// Set size.
    size: usize,
    /// Set last updated timestamp.
    timestamp: u64,

    buffer: IoBytesMut,
}

impl Debug for SetStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetStorage")
            .field("checksum", &self.checksum)
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .field("size", &self.size)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl SetStorage {
    pub const SET_HEADER_SIZE: usize = 16;

    pub fn load(buffer: IoBytesMut) -> Self {
        let checksum = (&buffer[0..4]).get_u32();
        let timestamp = (&buffer[4..12]).get_u64();
        let len = (&buffer[12..16]).get_u32() as usize;

        let mut this = Self {
            checksum,
            len,
            capacity: buffer.len() - Self::SET_HEADER_SIZE,
            size: buffer.len(),
            timestamp,
            buffer,
        };

        let c = Checksummer::checksum32(&this.buffer[4..]);
        if c != checksum {
            // Do not report checksum mismiatch. Clear the set directly.
            this.clear();
        }

        this
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn freeze(mut self) -> IoBytes {
        self.update();
        self.buffer.freeze()
    }

    pub fn apply(&mut self, deletions: &HashSet<u64>, insertions: &[u8]) {
        assert!(insertions.len() < self.capacity);

        self.deletes(deletions);
        self.append(insertions);
    }

    fn deletes(&mut self, deletes: &HashSet<u64>) {
        if deletes.is_empty() {
            return;
        }

        let mut rcursor = 0;
        let mut wcursor = 0;

        while rcursor < self.len {
            let header = EntryHeader::read(
                &self.buffer
                    [Self::SET_HEADER_SIZE + rcursor..Self::SET_HEADER_SIZE + rcursor + EntryHeader::ENTRY_HEADER_SIZE],
            );

            if !deletes.contains(&header.hash()) {
                if rcursor != wcursor {
                    self.buffer.copy_within(
                        Self::SET_HEADER_SIZE + rcursor..Self::SET_HEADER_SIZE + header.entry_len(),
                        wcursor,
                    );
                }
                wcursor += header.entry_len();
            }

            rcursor += header.entry_len();
        }
    }

    fn append(&mut self, buffer: &[u8]) {
        if buffer.is_empty() {
            return;
        }

        self.reserve(buffer.len());
        (&mut self.buffer[Self::SET_HEADER_SIZE + self.len..Self::SET_HEADER_SIZE + self.len + buffer.len()])
            .copy_from_slice(buffer);
        self.len += buffer.len();
    }

    pub fn get<K, V>(&self, hash: u64) -> Result<Option<(K, V)>>
    where
        K: StorageKey,
        V: StorageValue,
    {
        for entry in self.iter() {
            if hash == entry.hash {
                let k = EntryDeserializer::deserialize_key::<K>(&entry.key)?;
                let v = EntryDeserializer::deserialize_value::<V>(&entry.value, crate::Compression::None)?;
                return Ok(Some((k, v)));
            }
        }
        Ok(None)
    }

    /// from:
    ///
    /// ```plain
    /// 0        wipe          len       capacity
    /// |_________|ooooooooooooo|___________|
    /// ```
    ///
    /// to:
    ///
    /// ```plain
    /// 0     new len = len - wipe       capacity
    /// |ooooooooooooo|_____________________|
    /// ```
    fn reserve(&mut self, required: usize) {
        let remains = self.capacity - self.len;
        if remains < required {
            let mut wipe = 0;
            for entry in self.iter() {
                wipe += entry.len();
                if remains + wipe >= required {
                    break;
                }
            }
            self.buffer.copy_within(
                Self::SET_HEADER_SIZE + wipe..Self::SET_HEADER_SIZE + self.len,
                Self::SET_HEADER_SIZE,
            );
            self.len -= wipe;
            assert!(self.capacity - self.len >= required);
        }
    }

    fn iter(&self) -> SetIter<'_> {
        SetIter::open(self)
    }

    fn data(&self) -> &[u8] {
        &self.buffer[Self::SET_HEADER_SIZE..self.size]
    }

    fn update(&mut self) {
        self.update_timestamp();
        self.update_checksum();
    }

    fn update_timestamp(&mut self) {
        self.timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }

    fn update_checksum(&mut self) {
        self.checksum = Checksummer::checksum32(&self.buffer[4..self.size]);
    }
}

pub struct SetEntry<'a> {
    offset: usize,
    pub hash: u64,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> SetEntry<'a> {
    /// Length of the entry with header, key and value included.
    pub fn len(&self) -> usize {
        EntryHeader::ENTRY_HEADER_SIZE + self.key.len() + self.value.len()
    }

    /// Range of the entry in the set data.
    pub fn range(&self) -> Range<usize> {
        self.offset..self.offset + self.len()
    }
}

pub struct SetIter<'a> {
    set: &'a SetStorage,
    offset: usize,
}

impl<'a> SetIter<'a> {
    fn open(set: &'a SetStorage) -> Self {
        Self { set, offset: 0 }
    }

    fn is_valid(&self) -> bool {
        self.offset < self.set.len as usize
    }

    fn next(&mut self) -> Option<SetEntry<'a>> {
        if !self.is_valid() {
            return None;
        }
        let mut cursor = self.offset;
        let header = EntryHeader::read(&self.set.data()[cursor..cursor + EntryHeader::ENTRY_HEADER_SIZE]);
        cursor += EntryHeader::ENTRY_HEADER_SIZE;
        let key = &self.set.data()[cursor..cursor + header.key_len()];
        cursor += header.key_len();
        let value = &self.set.data()[cursor..cursor + header.value_len()];
        let entry = SetEntry {
            offset: self.offset,
            hash: header.hash(),
            key,
            value,
        };
        self.offset += entry.len();
        Some(entry)
    }
}

impl<'a> Iterator for SetIter<'a> {
    type Item = SetEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_storage_basic() {}
}
