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

use foyer_common::code::{HashBuilder, Key, Value};

use crate::CacheEntryNoReferenceHandle;

/// Trait for the customized event listener.

pub trait EventListener: Send + Sync + 'static {
    /// Associated key type.
    type Key;
    /// Associated value type.
    type Value;
    /// Associated hash builder type.
    type HashBuilder;

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(unused_variables)]
    /// Called when a cache entry is released from the in-memory cache.
    unsafe fn on_no_reference(
        &self,
        handle: &CacheEntryNoReferenceHandle<'_, Self::Key, Self::Value, Self::HashBuilder>,
    ) where
        Self::Key: Key,
        Self::Value: Value,
        Self::HashBuilder: HashBuilder,
    {
    }

    // TODO(MrCroxx): use `expect` after `lint_reasons` is stable.
    #[allow(unused_variables)]
    /// Called when a cache entry is released from the in-memory cache.
    fn on_memory_release(&self, key: Self::Key, value: Self::Value)
    where
        Self::Key: Key,
        Self::Value: Value,
    {
    }
}
