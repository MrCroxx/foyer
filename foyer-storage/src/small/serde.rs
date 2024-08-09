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

use std::u8;

use bytes::{Buf, BufMut};
use foyer_common::strict_assert;

/// max key/value len: `64 KiB - 1`
///
/// # Format
///
/// ```plain
/// | hash 64b |
/// | key len low 8b | value len low 8b | key len high 4b | value len high 4b |
/// ```
#[derive(Debug, PartialEq, Eq)]
pub struct EntryHeader {
    hash: u64,
    key_len: u16,
    value_len: u16,
}

impl EntryHeader {
    pub const ENTRY_HEADER_SIZE: usize = (12 + 12 + 64) / 8;

    pub fn new(hash: u64, key_len: usize, value_len: usize) -> Self {
        strict_assert!(key_len < (1 << 12));
        strict_assert!(value_len < (1 << 12));
        Self {
            hash,
            key_len: key_len as _,
            value_len: value_len as _,
        }
    }

    #[inline]
    pub fn hash(&self) -> u64 {
        self.hash
    }

    #[inline]
    pub fn key_len(&self) -> usize {
        self.key_len as _
    }

    #[inline]
    pub fn value_len(&self) -> usize {
        self.value_len as _
    }

    #[inline]
    pub fn entry_len(&self) -> usize {
        Self::ENTRY_HEADER_SIZE + self.key_len() + self.value_len()
    }

    pub fn write(&self, mut buf: impl BufMut) {
        buf.put_u64(self.hash);
        buf.put_u8(self.key_len as u8);
        buf.put_u8(self.value_len as u8);
        let v = ((self.key_len >> 4) as u8 & 0b_1111_0000) | (self.value_len >> 8) as u8;
        buf.put_u8(v);
    }

    pub fn read(mut buf: impl Buf) -> Self {
        let hash = buf.get_u64();
        let mut key_len = buf.get_u8() as u16;
        let mut value_len = buf.get_u8() as u16;
        let v = buf.get_u8() as u16;
        key_len |= (v & 0b_1111_0000) << 8;
        value_len |= (v & 0b_0000_1111) << 8;
        Self {
            hash,
            key_len,
            value_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::IoBytesMut;

    use super::*;

    #[test]
    fn test_entry_header_serde() {
        let header = EntryHeader {
            hash: 114514,
            key_len: 114,
            value_len: 514,
        };
        let mut buf = IoBytesMut::new();
        header.write(&mut buf);
        let h = EntryHeader::read(&buf[..]);
        assert_eq!(header, h);
    }
}
