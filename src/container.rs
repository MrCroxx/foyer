//  Copyright 2023 MrCroxx
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

use std::collections::BTreeMap;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ptr::NonNull;

use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard};
use twox_hash::XxHash64;

use crate::policies::{Handle, Policy};
use crate::store::Store;
use crate::{Data, Index};

pub struct Config<I, P, H, T, W, S>
where
    I: Index,
    P: Policy<I = I, H = H>,
    H: Handle<I = I>,
    W: Fn(&T) -> usize + Send + Sync,
    S: Store<I = I, D = T>,
{
    capacity: usize,

    pool_count_bits: usize,

    policy_config: P::C,

    weighter: W,

    store: S,

    _marker: PhantomData<(I, H, T)>,
}

#[allow(clippy::type_complexity)]
pub struct Container<I, P, H, D, W, S>
where
    I: Index,
    P: Policy<I = I, H = H>,
    H: Handle<I = I>,
    D: Data,
    W: Fn(&D) -> usize + Send + Sync,
    S: Store<I = I, D = D>,
{
    pool_count_bits: usize,
    pools: Vec<Mutex<Pool<I, P, H, D, S>>>,

    weighter: W,
}

impl<I, P, H, D, W, S> Container<I, P, H, D, W, S>
where
    I: Index,
    P: Policy<I = I, H = H>,
    H: Handle<I = I>,
    D: Data,
    W: Fn(&D) -> usize + Send + Sync,
    S: Store<I = I, D = D>,
{
    pub fn new(config: Config<I, P, H, D, W, S>) -> Self {
        let pool_count = 1 << config.pool_count_bits;
        let capacity = config.capacity >> config.pool_count_bits;
        let pools = (0..pool_count)
            .map(|id| Pool {
                id,
                policy: P::new(config.policy_config.clone()),
                capacity,
                size: 0,
                handles: BTreeMap::new(),
                store: config.store.clone(),
            })
            .map(Mutex::new)
            .collect_vec();

        Self {
            pool_count_bits: config.pool_count_bits,
            pools,

            weighter: config.weighter,
        }
    }

    pub fn insert(&self, index: I, data: D) -> bool {
        let mut pool = self.pool(&index);

        if pool.handles.get(&index).is_some() {
            // already in cache
            return false;
        }

        let weight = (self.weighter)(&data);

        pool.make_room(weight);

        pool.insert(index, weight, data);

        true
    }

    pub fn remove(&self, index: &I) -> bool {
        let mut pool = self.pool(index);

        if pool.handles.get(index).is_none() {
            // not in cache
            return false;
        }

        pool.remove(index);

        true
    }

    pub fn get(&self, index: &I) -> Option<D> {
        let mut pool = self.pool(index);

        pool.get(index)
    }

    // TODO(MrCroxx): optimize this
    pub fn size(&self) -> usize {
        self.pools.iter().map(|pool| pool.lock().size).sum()
    }

    fn pool(&self, index: &I) -> MutexGuard<'_, Pool<I, P, H, D, S>> {
        let mut hasher = XxHash64::default();
        index.hash(&mut hasher);
        let id = hasher.finish() as usize & ((1 << self.pool_count_bits) - 1);

        self.pools[id].lock()
    }
}

struct Pool<I, P, H, D, S>
where
    I: Index,
    P: Policy<I = I, H = H>,
    H: Handle<I = I>,
    D: Data,
    S: Store<I = I, D = D>,
{
    #[allow(unused)]
    id: usize,

    policy: P,

    capacity: usize,

    size: usize,

    handles: BTreeMap<I, PoolHandle<I, H>>,

    store: S,
}

impl<I, P, H, D, S> Pool<I, P, H, D, S>
where
    I: Index,
    P: Policy<I = I, H = H>,
    H: Handle<I = I>,
    D: Data,
    S: Store<I = I, D = D>,
{
    fn make_room(&mut self, weight: usize) {
        let mut handles = vec![];
        for index in self.policy.eviction_iter() {
            if self.size + weight <= self.capacity || self.size == 0 {
                break;
            }
            let PoolHandle { weight, handle } = self.handles.remove(index).unwrap();
            self.size -= weight;
            handles.push(handle);
        }
        for handle in handles {
            assert!(self.policy.remove(handle));
            unsafe { drop(Box::from_raw(handle.as_ptr())) };
        }
    }

    fn insert(&mut self, index: I, weight: usize, data: D) {
        let handle = Box::new(H::new(index.clone()));
        let handle = unsafe { NonNull::new_unchecked(Box::leak(handle)) };

        assert!(self.policy.insert(handle));
        self.handles
            .insert(index.clone(), PoolHandle { weight, handle });
        self.size += weight;

        self.store.store(index, data);
    }

    fn remove(&mut self, index: &I) {
        let PoolHandle { weight, handle } = self.handles.remove(index).unwrap();
        assert!(self.policy.remove(handle));
        self.size -= weight;

        self.store.delete(index);
    }

    fn get(&mut self, index: &I) -> Option<D> {
        match self.handles.get(index) {
            Some(PoolHandle { weight: _, handle }) => {
                self.policy.access(*handle);
                self.store.load(index)
            }
            None => None,
        }
    }
}

struct PoolHandle<I, H>
where
    I: Index,
    H: Handle<I = I>,
{
    weight: usize,
    handle: NonNull<H>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::policies::lru::{Config as LruConfig, Handle as LruHandle, Lru};
    use crate::store::tests::MemoryStore;

    #[test]
    fn test_container_simple() {
        let policy_config = LruConfig {
            update_on_write: true,
            update_on_read: true,
            lru_insertion_point_fraction: 0.0,
        };

        let config = Config {
            capacity: 100,
            pool_count_bits: 0,
            policy_config,
            weighter: |data: &Vec<u8>| data.len(),
            store: MemoryStore::new(),
            _marker: PhantomData,
        };

        let container: Container<u64, Lru<_>, LruHandle<_>, Vec<u8>, _, MemoryStore<_, _>> =
            Container::new(config);

        assert!(container.insert(1, vec![b'x'; 40]));
        assert!(!container.insert(1, vec![b'x'; 40]));
        assert_eq!(container.get(&1), Some(vec![b'x'; 40]));

        assert!(container.insert(2, vec![b'x'; 60]));
        assert!(!container.insert(2, vec![b'x'; 60]));
        assert_eq!(container.get(&2), Some(vec![b'x'; 60]));

        assert!(container.insert(3, vec![b'x'; 50]));
        assert_eq!(container.get(&3), Some(vec![b'x'; 50]));
        assert_eq!(container.get(&1), None);
        assert_eq!(container.get(&2), None);

        assert!(container.remove(&3));

        assert_eq!(container.size(), 0);
    }
}
