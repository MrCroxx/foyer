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

pub use crate::{
    cache::{Cache, CacheBuilder, CacheEntry, Entry, EntryState, EvictionConfig},
    context::CacheContext,
    eviction::{fifo::FifoConfig, lfu::LfuConfig, lru::LruConfig, s3fifo::S3FifoConfig},
    generic::Weighter,
    listener::{CacheEventListener, DefaultCacheEventListener},
    metrics::Metrics,
};
pub use ahash::RandomState;
