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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheContext {
    /// The default context shared by all eviction container implementations.
    Default,
    LruPriorityLow,
}

impl Default for CacheContext {
    fn default() -> Self {
        Self::Default
    }
}

/// The overhead of `Context` itself and the conversion should be light.
pub trait Context: From<CacheContext> + Into<CacheContext> + Send + Sync + 'static + Clone {}

impl<T> Context for T where T: From<CacheContext> + Into<CacheContext> + Send + Sync + 'static + Clone {}
