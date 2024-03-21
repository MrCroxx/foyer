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
    intrusive_adapter,
};

use crate::{
    eviction::Eviction,
    handle::{BaseHandle, Handle},
    CacheContext, Key, Value,
};

#[derive(Debug, Clone)]
pub struct FifoContext;

impl From<CacheContext> for FifoContext {
    fn from(_: CacheContext) -> Self {
        Self
    }
}

impl From<FifoContext> for CacheContext {
    fn from(_: FifoContext) -> Self {
        CacheContext::Default
    }
}

pub struct FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    link: DlistLink,
    base: BaseHandle<K, V, FifoContext>,
}

impl<K, V> Debug for FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FifoHandle").finish()
    }
}

intrusive_adapter! { FifoHandleDlistAdapter<K, V> = NonNull<FifoHandle<K, V>>: FifoHandle<K, V> { link: DlistLink } where K: Key, V: Value }

impl<K, V> Handle for FifoHandle<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;
    type Context = FifoContext;

    fn new() -> Self {
        Self {
            link: DlistLink::default(),
            base: BaseHandle::new(),
        }
    }

    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context) {
        self.base.init(hash, key, value, charge, context);
    }

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context> {
        &self.base
    }

    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context> {
        &mut self.base
    }
}

#[derive(Debug, Clone)]
pub struct FifoConfig {}

pub struct Fifo<K, V>
where
    K: Key,
    V: Value,
{
    queue: Dlist<FifoHandleDlistAdapter<K, V>>,
}

impl<K, V> Eviction for Fifo<K, V>
where
    K: Key,
    V: Value,
{
    type Handle = FifoHandle<K, V>;
    type Config = FifoConfig;

    unsafe fn new(_capacity: usize, _config: &Self::Config) -> Self
    where
        Self: Sized,
    {
        Self { queue: Dlist::new() }
    }

    unsafe fn push(&mut self, mut ptr: NonNull<Self::Handle>) {
        self.queue.push_back(ptr);
        ptr.as_mut().base_mut().set_in_eviction(true);
    }

    unsafe fn pop(&mut self) -> Option<NonNull<Self::Handle>> {
        self.queue.pop_front().map(|mut ptr| {
            ptr.as_mut().base_mut().set_in_eviction(false);
            ptr
        })
    }

    unsafe fn reinsert(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn access(&mut self, _: NonNull<Self::Handle>) {}

    unsafe fn remove(&mut self, mut ptr: NonNull<Self::Handle>) {
        let p = self.queue.iter_mut_from_raw(ptr.as_mut().link.raw()).remove().unwrap();
        assert_eq!(p, ptr);
        ptr.as_mut().base_mut().set_in_eviction(false);
    }

    unsafe fn clear(&mut self) -> Vec<NonNull<Self::Handle>> {
        let mut res = Vec::with_capacity(self.len());
        while let Some(mut ptr) = self.queue.pop_front() {
            ptr.as_mut().base_mut().set_in_eviction(false);
            res.push(ptr);
        }
        res
    }

    unsafe fn len(&self) -> usize {
        self.queue.len()
    }

    unsafe fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl<K, V> Send for Fifo<K, V>
where
    K: Key,
    V: Value,
{
}
unsafe impl<K, V> Sync for Fifo<K, V>
where
    K: Key,
    V: Value,
{
}

#[cfg(test)]
pub mod tests {

    use itertools::Itertools;

    use super::*;
    use crate::eviction::test_utils::TestEviction;

    impl<K, V> TestEviction for Fifo<K, V>
    where
        K: Key + Clone,
        V: Value + Clone,
    {
        fn dump(&self) -> Vec<(<Self::Handle as Handle>::Key, <Self::Handle as Handle>::Value)> {
            self.queue
                .iter()
                .map(|handle| (handle.base().key().clone(), handle.base().value().clone()))
                .collect_vec()
        }
    }

    type TestFifoHandle = FifoHandle<u64, u64>;
    type TestFifo = Fifo<u64, u64>;

    unsafe fn new_test_fifo_handle_ptr(key: u64, value: u64) -> NonNull<TestFifoHandle> {
        let mut handle = Box::new(TestFifoHandle::new());
        handle.init(0, key, value, 1, FifoContext);
        NonNull::new_unchecked(Box::into_raw(handle))
    }

    unsafe fn del_test_fifo_handle_ptr(ptr: NonNull<TestFifoHandle>) {
        let _ = Box::from_raw(ptr.as_ptr());
    }

    #[test]
    fn test_fifo() {
        unsafe {
            let ptrs = (0..8).map(|i| new_test_fifo_handle_ptr(i, i)).collect_vec();

            let mut fifo = TestFifo::new(100, &FifoConfig {});

            // 0, 1, 2, 3
            fifo.push(ptrs[0]);
            fifo.push(ptrs[1]);
            fifo.push(ptrs[2]);
            fifo.push(ptrs[3]);

            // 2, 3
            let p0 = fifo.pop().unwrap();
            let p1 = fifo.pop().unwrap();
            assert_eq!(ptrs[0], p0);
            assert_eq!(ptrs[1], p1);

            // 2, 3, 4, 5, 6
            fifo.push(ptrs[4]);
            fifo.push(ptrs[5]);
            fifo.push(ptrs[6]);

            // 2, 6
            fifo.remove(ptrs[3]);
            fifo.remove(ptrs[4]);
            fifo.remove(ptrs[5]);

            assert_eq!(fifo.clear(), vec![ptrs[2], ptrs[6]]);

            for ptr in ptrs {
                del_test_fifo_handle_ptr(ptr);
            }
        }
    }
}
