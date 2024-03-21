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

#![feature(lint_reasons)]
#![expect(clippy::identity_op)]

use std::{path::PathBuf, sync::Arc, time::Duration};

use foyer_intrusive::eviction::fifo::FifoConfig;
use foyer_storage::{
    compress::Compression,
    device::fs::FsDeviceConfig,
    lazy::LazyStore,
    runtime::{RuntimeConfig, RuntimeLazyStore, RuntimeStorageConfig, RuntimeStore},
    storage::{Storage, StorageExt},
    store::{FifoFsStoreConfig, Store},
    test_utils::JudgeRecorder,
};

const KB: usize = 1024;
const MB: usize = 1024 * 1024;

const INSERTS: usize = 100;
const LOOPS: usize = 10;

async fn test_storage<S>(config: S::Config, recorder: Arc<JudgeRecorder<u64, Vec<u8>>>)
where
    S: Storage<Key = u64, Value = Vec<u8>>,
{
    let store = S::open(config.clone()).await.unwrap();
    while !store.is_ready() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut index = 0;

    for _ in 0..INSERTS as u64 {
        index += 1;
        store.insert(index, vec![index as u8; 1 * KB]).await.unwrap();
    }

    store.close().await.unwrap();

    let remains = recorder.remains();

    for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
        if remains.contains(&i) {
            assert_eq!(store.lookup(&i).await.unwrap().unwrap(), vec![i as u8; 1 * KB],);
        } else {
            assert!(store.lookup(&i).await.unwrap().is_none());
        }
    }

    drop(store);

    for _ in 0..LOOPS {
        let store = S::open(config.clone()).await.unwrap();
        while !store.is_ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let remains = recorder.remains();

        for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
            if remains.contains(&i) {
                assert_eq!(store.lookup(&i).await.unwrap().unwrap(), vec![i as u8; 1 * KB],);
            } else {
                assert!(store.lookup(&i).await.unwrap().is_none());
            }
        }

        for _ in 0..INSERTS as u64 {
            index += 1;
            store.insert(index, vec![index as u8; 1 * KB]).await.unwrap();
        }

        store.close().await.unwrap();

        let remains = recorder.remains();

        for i in 0..INSERTS as u64 * (LOOPS + 1) as u64 {
            if remains.contains(&i) {
                assert_eq!(store.lookup(&i).await.unwrap().unwrap(), vec![i as u8; 1 * KB],);
            } else {
                assert!(store.lookup(&i).await.unwrap().is_none());
            }
        }

        drop(store);
    }
}

#[tokio::test]
async fn test_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let config = FifoFsStoreConfig {
        name: "".to_string(),
        eviction_config: FifoConfig,
        device_config: FsDeviceConfig {
            dir: PathBuf::from(tempdir.path()),
            capacity: 4 * MB,
            file_capacity: 1 * MB,
            align: 4 * KB,
            io_size: 4 * KB,
        },
        catalog_bits: 1,
        admissions: vec![recorder.clone()],
        reinsertions: vec![recorder.clone()],
        flushers: 1,
        reclaimers: 1,
        clean_region_threshold: 1,
        recover_concurrency: 2,
        compression: Compression::None,
    };

    test_storage::<Store<_, _>>(config.into(), recorder).await;
}

#[tokio::test]
async fn test_store_zstd() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let config = FifoFsStoreConfig {
        name: "".to_string(),
        eviction_config: FifoConfig,
        device_config: FsDeviceConfig {
            dir: PathBuf::from(tempdir.path()),
            capacity: 4 * MB,
            file_capacity: 1 * MB,
            align: 4 * KB,
            io_size: 4 * KB,
        },
        catalog_bits: 1,
        admissions: vec![recorder.clone()],
        reinsertions: vec![recorder.clone()],
        flushers: 1,
        reclaimers: 1,
        clean_region_threshold: 1,
        recover_concurrency: 2,
        compression: Compression::Zstd,
    };

    test_storage::<Store<_, _>>(config.into(), recorder).await;
}

#[tokio::test]
async fn test_store_lz4() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let config = FifoFsStoreConfig {
        name: "".to_string(),
        eviction_config: FifoConfig,
        device_config: FsDeviceConfig {
            dir: PathBuf::from(tempdir.path()),
            capacity: 4 * MB,
            file_capacity: 1 * MB,
            align: 4 * KB,
            io_size: 4 * KB,
        },
        catalog_bits: 1,
        admissions: vec![recorder.clone()],
        reinsertions: vec![recorder.clone()],
        flushers: 1,
        reclaimers: 1,
        clean_region_threshold: 1,
        recover_concurrency: 2,
        compression: Compression::Lz4,
    };

    test_storage::<Store<_, _>>(config.into(), recorder).await;
}

#[tokio::test]
async fn test_lazy_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let config = FifoFsStoreConfig {
        name: "".to_string(),
        eviction_config: FifoConfig,
        device_config: FsDeviceConfig {
            dir: PathBuf::from(tempdir.path()),
            capacity: 4 * MB,
            file_capacity: 1 * MB,
            align: 4 * KB,
            io_size: 4 * KB,
        },
        catalog_bits: 1,
        admissions: vec![recorder.clone()],
        reinsertions: vec![recorder.clone()],
        flushers: 1,
        reclaimers: 1,
        clean_region_threshold: 1,
        recover_concurrency: 2,
        compression: Compression::None,
    };

    test_storage::<LazyStore<_, _>>(config.into(), recorder).await;
}

#[tokio::test]
async fn test_runtime_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let config = RuntimeStorageConfig {
        store: FifoFsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 4 * MB,
                file_capacity: 1 * MB,
                align: 4 * KB,
                io_size: 4 * KB,
            },
            catalog_bits: 1,
            admissions: vec![recorder.clone()],
            reinsertions: vec![recorder.clone()],
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            recover_concurrency: 2,
            compression: Compression::None,
        }
        .into(),
        runtime: RuntimeConfig {
            worker_threads: None,
            thread_name: None,
        },
    };

    test_storage::<RuntimeStore<_, _>>(config, recorder).await;
}

#[tokio::test]
async fn test_runtime_lazy_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let recorder = Arc::new(JudgeRecorder::default());
    let config = RuntimeStorageConfig {
        store: FifoFsStoreConfig {
            name: "".to_string(),
            eviction_config: FifoConfig,
            device_config: FsDeviceConfig {
                dir: PathBuf::from(tempdir.path()),
                capacity: 4 * MB,
                file_capacity: 1 * MB,
                align: 4 * KB,
                io_size: 4 * KB,
            },
            catalog_bits: 1,
            admissions: vec![recorder.clone()],
            reinsertions: vec![recorder.clone()],
            flushers: 1,
            reclaimers: 1,
            clean_region_threshold: 1,
            recover_concurrency: 2,
            compression: Compression::None,
        }
        .into(),
        runtime: RuntimeConfig {
            worker_threads: None,
            thread_name: None,
        },
    };

    test_storage::<RuntimeLazyStore<_, _>>(config, recorder).await;
}
