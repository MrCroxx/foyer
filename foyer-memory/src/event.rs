//  Copyright 2024 MrCroxx
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

use std::marker::PhantomData;

use crate::{Key, Value};

pub trait CacheEventListener: Send + Sync + 'static {
    type Key: Key;
    type Value: Value;
    type Context: Send + Sync + 'static;

    /// The function is called when an entry is released by the cache and all external users.
    ///
    /// The arguments includes the key, value and context with ownership.
    fn on_release(&self, key: Self::Key, value: Self::Value, context: Self::Context, charges: usize);
}

pub struct DefaultCacheEventListener<K, V, C>(PhantomData<(K, V, C)>)
where
    K: Key,
    V: Value,
    C: Send + Sync + 'static;

impl<K, V, C> Default for DefaultCacheEventListener<K, V, C>
where
    K: Key,
    V: Value,
    C: Send + Sync + 'static,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K, V, C> CacheEventListener for DefaultCacheEventListener<K, V, C>
where
    K: Key,
    V: Value,
    C: Send + Sync + 'static,
{
    type Key = K;
    type Value = V;
    type Context = C;

    fn on_release(&self, _key: Self::Key, _value: Self::Value, _context: Self::Context, _charges: usize) {}
}
