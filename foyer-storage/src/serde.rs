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

use std::{fmt::Debug, hash::Hasher};
use twox_hash::{XxHash32, XxHash64};

use foyer_common::code::{StorageKey, StorageValue};

use crate::{
    compress::Compression,
    error::{Error, Result},
    IoBytesMut,
};

#[derive(Debug)]
pub struct Checksummer;

impl Checksummer {
    pub fn checksum64(buf: &[u8]) -> u64 {
        let mut hasher = XxHash64::with_seed(0);
        hasher.write(buf);
        hasher.finish()
    }

    pub fn checksum32(buf: &[u8]) -> u32 {
        let mut hasher = XxHash32::with_seed(0);
        hasher.write(buf);
        hasher.finish() as u32
    }
}

#[derive(Debug)]
pub struct KvInfo {
    pub key_len: usize,
    pub value_len: usize,
}

#[derive(Debug)]
pub struct EntrySerializer;

impl EntrySerializer {
    #[allow(clippy::needless_borrows_for_generic_args)]
    pub fn serialize<'a, K, V>(
        key: &'a K,
        value: &'a V,
        compression: &'a Compression,
        mut buffer: &'a mut IoBytesMut,
    ) -> Result<KvInfo>
    where
        K: StorageKey,
        V: StorageValue,
    {
        let mut cursor = buffer.len();

        // serialize value
        match compression {
            Compression::None => {
                bincode::serialize_into(&mut buffer, &value).map_err(Error::from)?;
            }
            Compression::Zstd => {
                let encoder = zstd::Encoder::new(&mut buffer, 0).map_err(Error::from)?.auto_finish();
                bincode::serialize_into(encoder, &value).map_err(Error::from)?;
            }

            Compression::Lz4 => {
                let encoder = lz4::EncoderBuilder::new()
                    .checksum(lz4::ContentChecksum::NoChecksum)
                    .auto_flush(true)
                    .build(&mut buffer)
                    .map_err(Error::from)?;
                bincode::serialize_into(encoder, &value).map_err(Error::from)?;
            }
        }

        let value_len = buffer.len() - cursor;
        cursor = buffer.len();

        // serialize key
        bincode::serialize_into(&mut buffer, &key).map_err(Error::from)?;
        let key_len = buffer.len() - cursor;

        Ok(KvInfo { key_len, value_len })
    }
}

#[derive(Debug)]
pub struct EntryDeserializer;

impl EntryDeserializer {
    pub fn deserialize<K, V>(
        buffer: &[u8],
        ken_len: usize,
        value_len: usize,
        compression: Compression,
        checksum: Option<u64>,
    ) -> Result<(K, V)>
    where
        K: StorageKey,
        V: StorageValue,
    {
        // deserialize value
        let buf = &buffer[..value_len];
        let value = Self::deserialize_value(buf, compression)?;

        // deserialize key
        let buf = &buffer[value_len..value_len + ken_len];
        let key = Self::deserialize_key(buf)?;

        // calculate checksum if needed
        if let Some(expected) = checksum {
            let get = Checksummer::checksum64(&buffer[..value_len + ken_len]);
            if expected != get {
                return Err(Error::ChecksumMismatch { expected, get });
            }
        }

        Ok((key, value))
    }

    pub fn deserialize_key<K>(buf: &[u8]) -> Result<K>
    where
        K: StorageKey,
    {
        bincode::deserialize_from(buf).map_err(Error::from)
    }

    pub fn deserialize_value<V>(buf: &[u8], compression: Compression) -> Result<V>
    where
        V: StorageValue,
    {
        match compression {
            Compression::None => bincode::deserialize_from(buf).map_err(Error::from),
            Compression::Zstd => {
                let decoder = zstd::Decoder::new(buf).map_err(Error::from)?;
                bincode::deserialize_from(decoder).map_err(Error::from)
            }
            Compression::Lz4 => {
                let decoder = lz4::Decoder::new(buf).map_err(Error::from)?;
                bincode::deserialize_from(decoder).map_err(Error::from)
            }
        }
    }
}
