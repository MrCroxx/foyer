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

use std::{fmt::Debug, ptr::NonNull};

use foyer_intrusive::{
    collections::dlist::{Dlist, DlistLink},
    core::adapter::Link,
    intrusive_adapter,
};

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    CacheContext, Key, Value,
};

#[derive(Debug, Clone)]
pub struct LruConfig {
    /// The ratio of the high priority pool occupied.
    ///
    /// [`Lru`] guarantees that the high priority charges are always as larger as
    /// but no larger that the capacity * high priority pool ratio.
    ///
    /// # Panic
    ///
    /// Panics if the value is not in [0, 1.0].
    pub high_priority_pool_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LruContext {
    HighPriority,
    LowPriority,
}

impl From<CacheContext> for LruContext {
    fn from(value: CacheContext) -> Self {
        match value {
            CacheContext::Default => Self::HighPriority,
            CacheContext::LruPriorityLow => Self::LowPriority,
        }
    }
}

impl From<LruContext> for CacheContext {
    fn from(value: LruContext) -> Self {
        match value {
            LruContext::HighPriority => CacheContext::Default,
            LruContext::LowPriority => CacheContext::LruPriorityLow,
        }
    }
}

pub struct LruHandle<K, V>
where
    K: Key,
    V: Value,
{
    link: DlistLink,
    base: BaseHandle<K, V, LruContext>,
    in_high_priority_pool: bool,
}

impl<K, V> Debug for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruHandle").finish()
    }
}

intrusive_adapter! { LruHandleDlistAdapter<K, V> = NonNull<LruHandle<K, V>>: LruHandle<K, V> { link: DlistLink } where K: Key, V: Value }

impl<K, V> Handle for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = LruContext;

    fn new() -> Self {
        Self {
            link: DlistLink::default(),
            base: BaseHandle::new(),
            in_high_priority_pool: false,
        }
    }

    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context) {
        self.base.init(hash, key, value, charge, context)
    }

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context> {
        &mut self.base
    }
}

unsafe impl<K, V> Send for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for LruHandle<K, V>
where
    K: Key,
    V: Value,
{
}

pub struct Lru<K, V>
where
    K: Key,
    V: Value,
{
    high_priority_list: Dlist<LruHandleDlistAdapter<K, V>>,
    list: Dlist<LruHandleDlistAdapter<K, V>>,

    high_priority_charges: usize,
    high_priority_charges_capacity: usize,
}

impl<K, V> Lru<K, V>
where
    K: Key,
    V: Value,
{
    unsafe fn may_overflow_high_priority_pool(&mut self) {
        while self.high_priority_charges > self.high_priority_charges_capacity {
            debug_assert!(!self.high_priority_list.is_empty());

            // overflow last entry in high priority pool to low priority pool
            let mut ptr = self.high_priority_list.pop_front().unwrap_unchecked();
            ptr.as_mut().in_high_priority_pool = false;
            self.high_priority_charges -= ptr.as_ref().base().charge();
            self.list.push_back(ptr);
        }
    }
}

impl<K, V> Eviction for Lru<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = LruHandle<K, V>;
    type Config = LruConfig;

    unsafe fn new(capacity: usize, config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        assert!(
            config.high_priority_pool_ratio >= 0.0 && config.high_priority_pool_ratio <= 1.0,
            "high_priority_pool_ratio_percentage must be in [0, 100], given: {}",
            config.high_priority_pool_ratio
        );

        let high_priority_charges_capacity = (capacity as f64 * config.high_priority_pool_ratio) as usize;

        Self {
            high_priority_list: Dlist::new(),
            list: Dlist::new(),
            high_priority_charges: 0,
            high_priority_charges_capacity,
        }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        debug_assert!(!handle.link.is_linked());

        match handle.base().context() {
            LruContext::HighPriority => {
                handle.in_high_priority_pool = true;
                self.high_priority_charges += handle.base().charge();
                self.high_priority_list.push_back(ptr);

                self.may_overflow_high_priority_pool();
            }
            LruContext::LowPriority => {
                handle.in_high_priority_pool = false;
                self.list.push_back(ptr);
            }
        }

        handle.base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        let mut ptr = self.list.pop_front().or_else(|| self.high_priority_list.pop_front())?;

        let handle = ptr.as_mut();
        debug_assert!(!handle.link.is_linked());

        if handle.in_high_priority_pool {
            self.high_priority_charges -= handle.base().charge();
        }

        handle.base_mut().set_in_eviction(false);

        Some(ptr)
    }

    unsafe fn access(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn reinsert(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();

        if handle.base().is_in_eviction() {
            debug_assert!(handle.link.is_linked());
            self.remove(ptr);
            self.push(ptr);
        } else {
            debug_assert!(!handle.link.is_linked());
            self.push(ptr);
        }
    }

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let handle = ptr.as_mut();
        debug_assert!(handle.link.is_linked());

        if handle.in_high_priority_pool {
            self.high_priority_charges -= handle.base.charge();
            self.high_priority_list.remove_raw(handle.link.raw());
        } else {
            self.list.remove_raw(handle.link.raw());
        }

        handle.base_mut().set_in_eviction(false);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());

        while !self.list.is_empty() {
            let mut ptr = self.list.pop_front().unwrap_unchecked();
            ptr.as_mut().base_mut().set_in_eviction(false);
            res.push(ptr);
        }

        while !self.high_priority_list.is_empty() {
            let mut ptr = self.high_priority_list.pop_front().unwrap_unchecked();
            ptr.as_mut().base_mut().set_in_eviction(false);
            self.high_priority_charges -= ptr.as_ref().base().charge();
            res.push(ptr);
        }

        debug_assert_eq!(self.high_priority_charges, 0);

        res
    }

    unsafe fn len(&self) -> usize {
        self.high_priority_list.len() + self.list.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl<K, V> Send for Lru<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for Lru<K, V>
where
    K: Key,
    V: Value,
{
}

#[cfg(test)]
pub mod tests {

    use foyer_intrusive::core::pointer::Pointer;
    use itertools::Itertools;

    use super::*;
    use crate::eviction::test_utils::TestEviction;

    impl<K, V> TestEviction for Lru<K, V>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        fn dump(&self) -> Vec<(<Self::Handle as Handle>::Key, <Self::Handle as Handle>::Value)> {
            self.list
                .iter()
                .chain(self.high_priority_list.iter())
                .map(|handle| (handle.base().key().clone(), handle.base().value().clone()))
                .collect_vec()
        }
    }

    type TestLruHandle = LruHandle<u64, u64>;
    type TestLru = Lru<u64, u64>;

    unsafe fn new_test_lru_handle_ptr(key: u64, value: u64, context: LruContext) -> NonNull<TestLruHandle> {
        let mut handle = Box::new(TestLruHandle::new());
        handle.init(0, key, value, 1, context);
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_lru_handle_ptr(ptr: NonNull<TestLruHandle>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    unsafe fn dump_test_lru(lru: &TestLru) -> (Vec<NonNull<TestLruHandle>>, Vec<NonNull<TestLruHandle>>) {
        (
            lru.list
                .iter()
                .map(|handle| NonNull::new_unchecked(handle.as_ptr() as *mut _))
                .collect_vec(),
            lru.high_priority_list
                .iter()
                .map(|handle| NonNull::new_unchecked(handle.as_ptr() as *mut _))
                .collect_vec(),
        )
    }

    #[test]
    fn test_lru() {
        unsafe {
            let ptrs = (0..20)
                .map(|i| {
                    new_test_lru_handle_ptr(
                        i,
                        i,
                        if i < 10 {
                            LruContext::HighPriority
                        } else {
                            LruContext::LowPriority
                        },
                    )
                })
                .collect_vec();

            let config = LruConfig {
                high_priority_pool_ratio: 0.5,
            };
            let mut lru = TestLru::new(8, &config);

            assert_eq!(lru.high_priority_charges_capacity, 4);

            // [0, 1, 2, 3]
            lru.push(ptrs[0]);
            lru.push(ptrs[1]);
            lru.push(ptrs[2]);
            lru.push(ptrs[3]);
            assert_eq!(lru.len(), 4);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.high_priority_list.len(), 4);
            assert_eq!(dump_test_lru(&lru), (vec![], vec![ptrs[0], ptrs[1], ptrs[2], ptrs[3]]));

            // 0, [1, 2, 3, 4]
            lru.push(ptrs[4]);
            assert_eq!(lru.len(), 5);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.high_priority_list.len(), 4);
            assert_eq!(
                dump_test_lru(&lru),
                (vec![ptrs[0]], vec![ptrs[1], ptrs[2], ptrs[3], ptrs[4]])
            );

            // 0, 10, [1, 2, 3, 4]
            lru.push(ptrs[10]);
            assert_eq!(lru.len(), 6);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.high_priority_list.len(), 4);
            assert_eq!(
                dump_test_lru(&lru),
                (vec![ptrs[0], ptrs[10]], vec![ptrs[1], ptrs[2], ptrs[3], ptrs[4]])
            );

            // 10, [1, 2, 3, 4]
            let p0 = lru.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(lru.len(), 5);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.high_priority_list.len(), 4);
            assert_eq!(
                dump_test_lru(&lru),
                (vec![ptrs[10]], vec![ptrs[1], ptrs[2], ptrs[3], ptrs[4]])
            );

            // 10, [1, 3, 4]
            lru.remove(ptrs[2]);
            assert_eq!(lru.len(), 4);
            assert_eq!(lru.high_priority_charges, 3);
            assert_eq!(lru.high_priority_list.len(), 3);
            assert_eq!(dump_test_lru(&lru), (vec![ptrs[10]], vec![ptrs[1], ptrs[3], ptrs[4]]));

            // 10, 11, [1, 3, 4]
            lru.push(ptrs[11]);
            assert_eq!(lru.len(), 5);
            assert_eq!(lru.high_priority_charges, 3);
            assert_eq!(lru.high_priority_list.len(), 3);
            assert_eq!(
                dump_test_lru(&lru),
                (vec![ptrs[10], ptrs[11]], vec![ptrs[1], ptrs[3], ptrs[4]])
            );

            // 10, 11, 1, [3, 4, 5, 6]
            lru.push(ptrs[5]);
            lru.push(ptrs[6]);
            assert_eq!(lru.len(), 7);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.high_priority_list.len(), 4);
            assert_eq!(
                dump_test_lru(&lru),
                (
                    vec![ptrs[10], ptrs[11], ptrs[1]],
                    vec![ptrs[3], ptrs[4], ptrs[5], ptrs[6]]
                )
            );

            // 10, 11, 1, 3, [4, 5, 6, 0]
            lru.push(ptrs[0]);
            assert_eq!(lru.len(), 8);
            assert_eq!(lru.high_priority_charges, 4);
            assert_eq!(lru.high_priority_list.len(), 4);
            assert_eq!(
                dump_test_lru(&lru),
                (
                    vec![ptrs[10], ptrs[11], ptrs[1], ptrs[3]],
                    vec![ptrs[4], ptrs[5], ptrs[6], ptrs[0]]
                )
            );

            let ps = lru.clear();
            assert_eq!(ps, [10, 11, 1, 3, 4, 5, 6, 0].map(|i| ptrs[i]));

            for ptr in ptrs {
                del_test_lru_handle_ptr(ptr);
            }
        }
    }
}
