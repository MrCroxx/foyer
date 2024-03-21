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

use std::sync::atomic::AtomicUsize;

#[derive(Debug, Default)]
pub struct Metrics {
    /// successful inserts without replaces
    pub insert: AtomicUsize,
    /// successful replaces
    pub replace: AtomicUsize,

    /// get hits
    pub hit: AtomicUsize,
    /// get misses
    pub miss: AtomicUsize,

    /// fetches after cache miss with `entry` interface
    pub fetch: AtomicUsize,
    /// deduped fetches after cache miss with `entry` interface
    pub queue: AtomicUsize,

    /// successful removes
    pub remove: AtomicUsize,

    /// evicts from the eviction container
    pub evict: AtomicUsize,
    /// successful reinserts, only counts successful reinserts after evicted
    pub reinsert: AtomicUsize,

    /// released handles
    pub release: AtomicUsize,
}
