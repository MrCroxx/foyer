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

#[derive(Clone, Debug)]
pub struct LruConfig {
    /// Insertion point of the new entry, between 0 and 1.
    pub lru_insertion_point_fraction: f64,
}

#[derive(Debug, Default)]
pub struct LruLink {
    link_lru: DlistLink,

    is_in_tail: bool,
}

impl LruLink {
    fn raw(&self) -> NonNull<Self> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

impl Link for LruLink {
    fn is_linked(&self) -> bool {
        self.link_lru.is_linked()
    }
}

intrusive_adapter! { LruLinkAdapter = NonNull<LruLink>: LruLink { link_lru: DlistLink } }

#[derive(Debug)]
pub struct Lru<A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
    /// lru list
    lru: Dlist<LruLinkAdapter>,

    /// insertion point
    insertion_point: Option<NonNull<LruLink>>,

    /// length of tail after insertion point
    tail_len: usize,

    len: usize,

    config: LruConfig,

    adapter: A,
}

impl<A> Drop for Lru<A>
where
    A: Adapter<Link = LruLink>,
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

impl<A> Lru<A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
    fn new(config: LruConfig) -> Self {
        Self {
            lru: Dlist::new(),

            insertion_point: None,

            tail_len: 0,

            len: 0,

            config,

            adapter: A::new(),
        }
    }

    fn insert(&mut self, ptr: A::Pointer) {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::into_ptr(ptr) as *mut _);
            let link = self.adapter.item2link(item);

            assert!(!link.as_ref().is_linked());

            self.insert_lru(link);

            self.update_lru_insertion_point();

            self.len += 1;
        }
    }

    fn remove(&mut self, ptr: &A::Pointer) -> A::Pointer {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::as_ptr(ptr) as *mut _);
            let mut link = self.adapter.item2link(item);

            assert!(link.as_ref().is_linked());

            self.ensure_not_insertion_point(link);
            self.lru
                .iter_mut_from_raw(link.as_ref().link_lru.raw())
                .remove()
                .unwrap();
            if link.as_ref().is_in_tail {
                link.as_mut().is_in_tail = false;
                self.tail_len -= 1;
            }

            self.len -= 1;

            A::Pointer::from_ptr(item.as_ptr())
        }
    }

    fn access(&mut self, ptr: &A::Pointer) {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::as_ptr(ptr) as *mut _);
            let mut link = self.adapter.item2link(item);

            assert!(link.as_ref().is_linked());

            self.ensure_not_insertion_point(link);

            self.move_to_lru_front(link);

            if link.as_ref().is_in_tail {
                link.as_mut().is_in_tail = false;
                self.tail_len -= 1;
                self.update_lru_insertion_point();
            }
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> LruIter<'_, A> {
        let mut iter = self.lru.iter();
        iter.back();
        LruIter {
            iter,
            lru: self,
            ptr: ManuallyDrop::new(None),
        }
    }

    fn update_lru_insertion_point(&mut self) {
        unsafe {
            if self.config.lru_insertion_point_fraction == 0.0 {
                return;
            }

            if self.insertion_point.is_none() {
                self.insertion_point = self.lru.back().map(LruLink::raw);
                self.tail_len = 0;
                if let Some(insertion_point) = &mut self.insertion_point {
                    insertion_point.as_mut().is_in_tail = true;
                    self.tail_len += 1;
                }
            }

            if self.lru.len() <= 1 {
                return;
            }

            assert!(self.insertion_point.is_some());

            let expected_tail_len = (self.lru.len() as f64 * (1.0 - self.config.lru_insertion_point_fraction)) as usize;

            let mut curr = self.insertion_point.unwrap();
            while self.tail_len < expected_tail_len && Some(curr) != self.lru.front().map(LruLink::raw) {
                curr = self.lru_prev(curr).unwrap();
                curr.as_mut().is_in_tail = true;
                self.tail_len += 1;
            }
            while self.tail_len > expected_tail_len && Some(curr) != self.lru.back().map(LruLink::raw) {
                curr.as_mut().is_in_tail = false;
                self.tail_len -= 1;
                curr = self.lru_next(curr).unwrap();
            }

            self.insertion_point = Some(curr);
        }
    }

    unsafe fn ensure_not_insertion_point(&mut self, link: NonNull<LruLink>) {
        if Some(link) == self.insertion_point {
            self.insertion_point = self.lru_prev(link);
            match &mut self.insertion_point {
                Some(insertion_point) => {
                    self.tail_len += 1;
                    insertion_point.as_mut().is_in_tail = true;
                }
                // TODO(MrCroxx): think ?
                None => assert_eq!(self.lru.len(), 1),
            }
        }
    }

    unsafe fn insert_lru(&mut self, link: NonNull<LruLink>) {
        match self.insertion_point {
            Some(insertion_point) => self
                .lru
                .iter_mut_from_raw(insertion_point.as_ref().link_lru.raw())
                .insert_before(link),
            None => self.lru.push_front(link),
        }
    }

    unsafe fn move_to_lru_front(&mut self, link: NonNull<LruLink>) {
        self.lru
            .iter_mut_from_raw(link.as_ref().link_lru.raw())
            .remove()
            .unwrap();
        self.lru.push_front(link);
    }

    unsafe fn lru_prev(&self, link: NonNull<LruLink>) -> Option<NonNull<LruLink>> {
        let mut iter = self.lru.iter_from_raw(link.as_ref().link_lru.raw());
        iter.prev();
        iter.get().map(LruLink::raw)
    }

    unsafe fn lru_next(&self, link: NonNull<LruLink>) -> Option<NonNull<LruLink>> {
        let mut iter = self.lru.iter_from_raw(link.as_ref().link_lru.raw());
        iter.next();
        iter.get().map(LruLink::raw)
    }
}

pub struct LruIter<'a, A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
    lru: &'a Lru<A>,
    iter: DlistIter<'a, LruLinkAdapter>,

    ptr: ManuallyDrop<Option<<A as Adapter>::Pointer>>,
}

impl<'a, A> LruIter<'a, A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
    unsafe fn update_ptr(&mut self, link: NonNull<LruLink>) {
        std::mem::forget(self.ptr.take());

        let item = self.lru.adapter.link2item(link);
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

impl<'a, A> Iterator for LruIter<'a, A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
    type Item = &'a A::Pointer;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let link = match self.iter.get() {
                Some(link) => {
                    let link = link.raw();
                    self.iter.prev();
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

unsafe impl<A> Send for Lru<A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl<A> Sync for Lru<A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl Send for LruLink {}

unsafe impl Sync for LruLink {}

unsafe impl<'a, A> Send for LruIter<'a, A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl<'a, A> Sync for LruIter<'a, A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
}

impl<A> EvictionPolicy for Lru<A>
where
    A: Adapter<Link = LruLink>,
    <A as Adapter>::Pointer: Clone,
{
    type Adapter = A;
    type Config = LruConfig;

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
    use crate::key_adapter;

    #[derive(Debug)]
    struct LruItem {
        link: LruLink,
        key: u64,
    }

    impl LruItem {
        fn new(key: u64) -> Self {
            Self {
                link: LruLink::default(),
                key,
            }
        }
    }

    intrusive_adapter! { LruItemAdapter = Arc<LruItem>: LruItem { link: LruLink } }
    key_adapter! { LruItemAdapter = LruItem { key: u64 } }

    #[test]
    fn test_lru_simple() {
        let config = LruConfig {
            lru_insertion_point_fraction: 0.0,
        };
        let mut lru = Lru::<LruItemAdapter>::new(config);

        let handles = vec![
            Arc::new(LruItem::new(0)),
            Arc::new(LruItem::new(1)),
            Arc::new(LruItem::new(2)),
        ];

        lru.insert(handles[0].clone());
        lru.insert(handles[1].clone());
        lru.insert(handles[2].clone());

        assert_eq!(vec![0, 1, 2], lru.iter().map(|item| item.key).collect_vec());

        lru.access(&handles[1]);

        assert_eq!(vec![0, 2, 1], lru.iter().map(|item| item.key).collect_vec());

        lru.remove(&handles[2]);

        assert_eq!(vec![0, 1], lru.iter().map(|item| item.key).collect_vec());

        drop(lru);

        for handle in handles {
            assert_eq!(Arc::strong_count(&handle), 1);
        }
    }
}
