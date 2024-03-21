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

use std::marker::PhantomData;

use crate::{CacheContext, Key, Value};

pub trait CacheEventListener<K, V>: Send + Sync + 'static
where
    K: Key,
    V: Value,
{
    /// The function is called when an entry is released by the cache and all external users.
    ///
    /// The arguments includes the key and value with ownership.
    fn on_release(&self, key: K, value: V, context: CacheContext, charges: usize);
}

pub struct DefaultCacheEventListener<K, V>(PhantomData<(K, V)>)
where
    K: Key,
    V: Value;

impl<K, V> Default for DefaultCacheEventListener<K, V>
where
    K: Key,
    V: Value,
{
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K, V> CacheEventListener<K, V> for DefaultCacheEventListener<K, V>
where
    K: Key,
    V: Value,
{
    fn on_release(&self, _key: K, _value: V, _context: CacheContext, _charges: usize) {}
}
