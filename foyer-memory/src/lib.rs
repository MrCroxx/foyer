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

//! This crate provides a concurrent in-memory cache component that supports replaceable eviction algorithm.
//!
//! # Motivation
//!
//! There are a few goals to achieve with the crate:
//!
//! 1. Pluggable eviction algorithm with the same abstraction.
//! 2. Tracking the real memory usage by the cache. Including both holding by the cache and by the external users.
//! 3. Reduce the concurrent read-through requests into one.
//!
//! To achieve them, the crate needs to combine the advantages of the implementations of RocksDB and CacheLib.
//!
//! # Design
//!
//! The cache is mainly compused of the following components:
//! 1. handle             : Carries the cached entry, reference count, pointer links in the eviction container, etc.
//! 2. indexer            : Indexes cached keys to the handles.
//! 3. eviction container : Defines the order of eviction. Usually implemented with intrusive data structures.
//!
//! Because a handle needs to be referenced and mutated by both the indexer and the eviction container in the same
//! thread, it is hard to implement in 100% safe Rust without overhead. So, the APIs of the indexer and the eviciton
//! container are defined with `NonNull` pointers of the handles.
//!
//! When some entry is inserted into the cache, the associated handle should be transmuted into pointer without
//! dropping. When some entry is removed from the cache, the pointer of the associated handle should be transmuted into
//! an owned data structure.
//!
//! # Handle Lifetime
//!
//! The handle is created during a new entry is being inserted, and then inserted into both the indexer and the eviction
//! container.
//!
//! The handle is return if the entry is retrieved from the cache. The handle will track the count of the external
//! owners to decide the time to reclaim.
//!
//! When a key is removed or updated, the original handle will be removed from the indexer and the eviction container,
//! and waits to be released by all the external owners before reclamation.
//!
//! When the cache is full and being inserted, a handle will be evicted from the eviction container based on the
//! eviction algorithm. The evicted handle will NOT be removed from the indexer immediately because it still occupies
//! memory and can be used by queries followed up.
//!
//! After the handle is released by all the external owners, the eviction container will update its order or evict it
//! based on the eviction algorithm. If it doesn't appear in the eviction container, it may be reinserted if it still in
//! the indexer and there is enough space. Otherwise, it will be removed from both the indexer and the eviction
//! container.
//!
//! The handle that does not appear in either the indexer or the eviction container, and has no external owner, will be
//! destroyed.

#![warn(missing_docs)]

mod cache;
mod cleaner;
mod context;
mod eviction;
mod generic;
mod handle;
mod indexer;

mod prelude;
pub use prelude::*;
