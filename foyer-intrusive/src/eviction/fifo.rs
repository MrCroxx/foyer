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

//  Copyright (c) Meta Platforms, Inc. and affiliates.
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

use std::{mem::ManuallyDrop, ptr::NonNull};

use super::EvictionPolicy;
use crate::{
    collections::dlist::{Dlist, DlistIter, DlistLink},
    core::{
        adapter::{Adapter, Link},
        pointer::Pointer,
    },
    intrusive_adapter,
};

#[derive(Debug, Clone)]
pub struct FifoConfig;

#[derive(Debug, Default)]
pub struct FifoLink {
    link: DlistLink,
}

impl FifoLink {
    fn raw(&self) -> NonNull<Self> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

impl Link for FifoLink {
    fn is_linked(&self) -> bool {
        self.link.is_linked()
    }
}

intrusive_adapter! { FifoLinkAdapter = NonNull<FifoLink>: FifoLink { link: DlistLink } }

/// FIFO policy
#[derive(Debug)]
pub struct Fifo<A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    queue: Dlist<FifoLinkAdapter>,

    _config: FifoConfig,

    len: usize,

    adapter: A,
}

impl<A> Drop for Fifo<A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    fn drop(&mut self) {
        let mut to_remove = vec![];
        for ptr in self.iter() {
            to_remove.push(ptr.clone());
        }
        for ptr in to_remove {
            self.remove(&ptr);
        }
    }
}

impl<A> Fifo<A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    pub fn new(config: FifoConfig) -> Self {
        Self {
            queue: Dlist::new(),

            _config: config,

            len: 0,

            adapter: A::new(),
        }
    }

    fn insert(&mut self, ptr: A::Pointer) {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::into_ptr(ptr) as *mut _);
            let link = self.adapter.item2link(item);

            assert!(!link.as_ref().is_linked());

            self.queue.push_back(link);

            self.len += 1;
        }
    }

    fn remove(&mut self, ptr: &A::Pointer) -> A::Pointer {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::as_ptr(ptr) as *mut _);
            let link = self.adapter.item2link(item);

            assert!(link.as_ref().is_linked());

            self.queue.iter_mut_from_raw(link.as_ref().link.raw()).remove().unwrap();

            self.len -= 1;

            A::Pointer::from_ptr(item.as_ptr())
        }
    }

    fn access(&mut self, _ptr: &A::Pointer) {}

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> FifoIter<A> {
        let mut iter = self.queue.iter();
        iter.front();

        FifoIter {
            fifo: self,
            iter,

            ptr: ManuallyDrop::new(None),
        }
    }
}

pub struct FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    fifo: &'a Fifo<A>,
    iter: DlistIter<'a, FifoLinkAdapter>,

    ptr: ManuallyDrop<Option<<A as Adapter>::Pointer>>,
}

impl<'a, A> FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    unsafe fn update_ptr(&mut self, link: NonNull<FifoLink>) {
        std::mem::forget(self.ptr.take());

        let item = self.fifo.adapter.link2item(link);
        let ptr = A::Pointer::from_ptr(item.as_ptr());
        self.ptr = ManuallyDrop::new(Some(ptr));
    }

    unsafe fn ptr(&self) -> Option<&'a <A as Adapter>::Pointer> {
        if self.ptr.is_none() {
            return None;
        }
        let ptr = self.ptr.as_ref().unwrap();
        let raw = ptr as *const <A as Adapter>::Pointer;
        Some(&*raw)
    }
}

impl<'a, A> Iterator for FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    type Item = &'a A::Pointer;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let link = match self.iter.get() {
                Some(link) => {
                    let link = link.raw();
                    self.iter.next();
                    link
                }
                None => return None,
            };
            self.update_ptr(link);
            self.ptr()
        }
    }
}

// unsafe impl `Send + Sync` for structs with `NonNull` usage

unsafe impl<A> Send for Fifo<A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl<A> Sync for Fifo<A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl Send for FifoLink {}

unsafe impl Sync for FifoLink {}

unsafe impl<'a, A> Send for FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl<'a, A> Sync for FifoIter<'a, A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
}

impl<A> EvictionPolicy for Fifo<A>
where
    A: Adapter<Link = FifoLink>,
    <A as Adapter>::Pointer: Clone,
{
    type Adapter = A;
    type Config = FifoConfig;

    fn new(config: Self::Config) -> Self {
        Self::new(config)
    }

    fn insert(&mut self, ptr: A::Pointer) {
        self.insert(ptr)
    }

    fn remove(&mut self, ptr: &A::Pointer) -> A::Pointer {
        self.remove(ptr)
    }

    fn access(&mut self, ptr: &A::Pointer) {
        self.access(ptr)
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn iter(&self) -> impl Iterator<Item = &'_ A::Pointer> {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::eviction::EvictionPolicyExt;

    #[derive(Debug)]
    struct FifoItem {
        link: FifoLink,
        key: u64,
    }

    impl FifoItem {
        fn new(key: u64) -> Self {
            Self {
                link: FifoLink::default(),
                key,
            }
        }
    }

    intrusive_adapter! { FifoItemAdapter = Arc<FifoItem>: FifoItem { link: FifoLink } }

    #[test]
    fn test_fifo_simple() {
        let config = FifoConfig;
        let mut fifo = Fifo::<FifoItemAdapter>::new(config);

        let mut items = vec![];

        for key in 0..10 {
            let item = Arc::new(FifoItem::new(key));
            fifo.push(item.clone());
            items.push(item);
        }
        let v = fifo.iter().map(|item| item.key).collect_vec();
        assert_eq!(v, (0..10).collect_vec());

        let v = (0..5).map(|_| fifo.pop().unwrap()).map(|item| item.key).collect_vec();
        assert_eq!(v, (0..5).collect_vec());

        let v = fifo.iter().map(|item| item.key).collect_vec();
        assert_eq!(v, (5..10).collect_vec());

        drop(fifo);

        for item in items {
            assert_eq!(Arc::strong_count(&item), 1);
        }
    }
}
