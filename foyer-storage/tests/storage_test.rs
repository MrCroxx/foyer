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

// TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
#![allow(clippy::identity_op)]

use std::{path::Path, sync::Arc};

use ahash::RandomState;
use foyer_memory::{Cache, CacheBuilder, FifoConfig};
use foyer_storage::{
    test_utils::JudgeRecorder, Compression, DirectFsDeviceOptionsBuilder, RuntimeConfigBuilder, Storage, StoreBuilder,
};

const KB: usize = 1024;
const MB: usize = 1024 * 1024;

const INSERTS: usize = 100;
const LOOPS: usize = 10;

async fn test_store(
    memory: Cache<u64, Vec<u8>>,
    builder: impl Fn(&Cache<u64, Vec<u8>>) -> StoreBuilder<u64, Vec<u8>, RandomState>,
    recorder: Arc<JudgeRecorder<u64>>,
) {
    let store = builder(&memory).build().await.unwrap();

    let mut index = 0;

    for _ in 0..INSERTS as u64 {
        index += 1;
        let e = memory.insert(index, vec![index as u8; KB]);
        store.enqueue(e).await.unwrap();
    }

    store.close().await.unwrap();

    let remains = recorder.remains();

    for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
        let value = match store.load(&i).await.unwrap() {
            Some((k, v)) => {
                if k == i {
                    Some(v)
                } else {
                    None
                }
            }
            None => None,
        };
        if remains.contains(&i) {
            assert_eq!(value, Some(vec![i as u8; 1 * KB]));
        } else {
            assert!(value.is_none());
        }
    }

    drop(store);

    for _ in 0..LOOPS {
        let store = builder(&memory).build().await.unwrap();

        let remains = recorder.remains();

        for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
            let value = match store.load(&i).await.unwrap() {
                Some((k, v)) => {
                    if k == i {
                        Some(v)
                    } else {
                        None
                    }
                }
                None => None,
            };
            if remains.contains(&i) {
                assert_eq!(value, Some(vec![i as u8; 1 * KB]));
            } else {
                assert!(value.is_none());
            }
        }

        for _ in 0..INSERTS as u64 {
            index += 1;
            let e = memory.insert(index, vec![index as u8; KB]);
            store.enqueue(e).await.unwrap();
        }

        store.close().await.unwrap();

        let remains = recorder.remains();

        for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
            let value = match store.load(&i).await.unwrap() {
                Some((k, v)) => {
                    if k == i {
                        Some(v)
                    } else {
                        None
                    }
                }
                None => None,
            };
            if remains.contains(&i) {
                assert_eq!(value, Some(vec![i as u8; 1 * KB]));
            } else {
                assert!(value.is_none());
            }
        }

        drop(store);
    }
}

fn basic(
    memory: &Cache<u64, Vec<u8>>,
    path: impl AsRef<Path>,
    recorder: &Arc<JudgeRecorder<u64>>,
) -> StoreBuilder<u64, Vec<u8>> {
    StoreBuilder::new(memory.clone())
        .with_device_config(
            DirectFsDeviceOptionsBuilder::new(path)
                .with_capacity(4 * MB)
                .with_file_size(MB)
                .build(),
        )
        .with_indexer_shards(4)
        .with_admission_picker(recorder.clone())
        .with_reinsertion_picker(recorder.clone())
        .with_recover_concurrency(2)
        .with_flush(true)
}

#[tokio::test]
async fn test_direct_fs_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let memory = CacheBuilder::new(1).with_eviction_config(FifoConfig::default()).build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>>| basic(memory, tempdir.path(), &r);
    test_store(memory, builder, recorder).await;
}

#[tokio::test]
async fn test_direct_fs_store_zstd() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let memory = CacheBuilder::new(1).with_eviction_config(FifoConfig::default()).build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>>| basic(memory, tempdir.path(), &r).with_compression(Compression::Zstd);
    test_store(memory, builder, recorder).await;
}

#[tokio::test]
async fn test_direct_fs_store_lz4() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let memory = CacheBuilder::new(1).with_eviction_config(FifoConfig::default()).build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>>| basic(memory, tempdir.path(), &r).with_compression(Compression::Lz4);
    test_store(memory, builder, recorder).await;
}

#[tokio::test]
async fn test_runtime_fs_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let memory = CacheBuilder::new(1).with_eviction_config(FifoConfig::default()).build();
    let r = recorder.clone();
    let builder = |memory: &Cache<u64, Vec<u8>>| {
        basic(memory, tempdir.path(), &r).with_runtime_config(RuntimeConfigBuilder::new().build())
    };
    test_store(memory, builder, recorder).await;
}
