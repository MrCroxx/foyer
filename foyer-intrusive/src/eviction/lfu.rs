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

use std::{
    hash::{Hash, Hasher},
    mem::ManuallyDrop,
    ptr::NonNull,
};

use cmsketch::CMSketchUsize;
use twox_hash::XxHash64;

use super::EvictionPolicy;
use crate::{
    collections::dlist::{Dlist, DlistIter, DlistLink},
    core::{
        adapter::{Adapter, KeyAdapter, Link},
        pointer::Pointer,
    },
    intrusive_adapter,
};

const MIN_CAPACITY: usize = 100;
const ERROR_THRESHOLD: f64 = 5.0;
const HASH_COUNT: usize = 4;
const DECAY_FACTOR: f64 = 0.5;

#[derive(Debug, Clone)]
pub struct LfuConfig {
    /// The multiplier for window len given the cache size.
    pub window_to_cache_size_ratio: usize,

    /// The ratio of tiny lru capacity to overall capacity.
    pub tiny_lru_capacity_ratio: f64,
}

#[derive(PartialEq, Eq, Debug)]
enum LruType {
    Tiny,
    Main,

    None,
}

#[derive(Debug, Default)]
pub struct LfuLink {
    link_tiny: DlistLink,
    link_main: DlistLink,
}

impl Link for LfuLink {
    fn is_linked(&self) -> bool {
        self.link_tiny.is_linked() || self.link_main.is_linked()
    }
}

impl LfuLink {
    fn lru_type(&self) -> LruType {
        match (self.link_tiny.is_linked(), self.link_main.is_linked()) {
            (true, true) => unreachable!(),
            (true, false) => LruType::Tiny,
            (false, true) => LruType::Main,
            (false, false) => LruType::None,
        }
    }

    fn raw(&self) -> NonNull<Self> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

intrusive_adapter! { LfuLinkTinyDlistAdapter = NonNull<LfuLink>: LfuLink { link_tiny: DlistLink } }
intrusive_adapter! { LfuLinkMainDlistAdapter = NonNull<LfuLink>: LfuLink { link_main: DlistLink } }

/// Implements the W-TinyLFU cache eviction policy as described in -
///
/// https://arxiv.org/pdf/1512.00727.pdf
///
/// The cache is split into 2 parts, the main cache and the tiny cache.
/// The tiny cache is typically sized to be 1% of the total cache with
/// the main cache being the rest 99%. Both caches are implemented using
/// LRUs. New items land in tiny cache. During eviction, the tail item
/// from the tiny cache is promoted to main cache if its frequency is
/// higher than the tail item of of main cache, and the tail of main
/// cache is evicted. This gives the frequency based admission into main
/// cache. Hits in each cache simply move the item to the head of each
/// LRU cache.
/// The frequency counts are maintained in count-min-sketch approximate
/// counters -
///
/// Counter Overhead:
/// The window_to_cache_size_ratio determines the size of counters.
/// The default value is 32 which means the counting window size is
/// 32 times the cache size. After every 32 X cache capacity number
/// of items, the counts are halved to weigh frequency by recency.
/// The function counter_size() returns the size of the counters
/// in bytes. See maybe_grow_access_counters() implementation for
/// how the size is computed.
///
/// Tiny cache size:
/// This default to 1%. There's no need to tune this parameter.
#[derive(Debug)]
pub struct Lfu<A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
    /// tiny lru list
    lru_tiny: Dlist<LfuLinkTinyDlistAdapter>,

    /// main lru list
    lru_main: Dlist<LfuLinkMainDlistAdapter>,

    /// the window length counter
    window_size: usize,

    /// maximum value of window length which when hit the counters are halved
    max_window_size: usize,

    /// the capacity for which the counters are sized
    capacity: usize,

    /// approximate streaming frequency counters
    ///
    /// the counts are halved every time the max_window_len is hit
    frequencies: CMSketchUsize,

    len: usize,

    config: LfuConfig,

    adapter: A,
}

impl<A> Drop for Lfu<A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
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

impl<A> Lfu<A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
    pub fn new(config: LfuConfig) -> Self {
        let mut res = Self {
            lru_tiny: Dlist::new(),
            lru_main: Dlist::new(),

            window_size: 0,
            max_window_size: 0,
            capacity: 0,

            // A dummy size, will be updated later.
            frequencies: CMSketchUsize::new_with_size(1, 1),

            len: 0,

            config,

            adapter: A::new(),
        };
        res.maybe_grow_access_counters();
        res
    }

    fn insert(&mut self, ptr: A::Pointer) {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::into_ptr(ptr) as *mut _);
            let link = self.adapter.item2link(item);

            assert!(!link.as_ref().is_linked());

            self.lru_tiny.push_front(link);

            // Initialize the frequency count for this link.
            self.update_frequencies(link);

            // If tiny cache is full, unconditionally promote tail to main cache.
            let expected_tiny_len =
                (self.config.tiny_lru_capacity_ratio * (self.lru_tiny.len() + self.lru_main.len()) as f64) as usize;
            if self.lru_tiny.len() > expected_tiny_len {
                let raw = self.lru_tiny.back().unwrap().raw();
                self.switch_to_lru_front(raw);
            } else {
                self.maybe_promote_tail();
            }

            // If the number of counters are too small for the cache size, double them.
            self.maybe_grow_access_counters();

            self.len += 1;
        }
    }

    fn remove(&mut self, ptr: &A::Pointer) -> A::Pointer {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::as_ptr(ptr) as *mut _);
            let link = self.adapter.item2link(item);

            assert!(link.as_ref().is_linked());

            self.remove_from_lru(link);

            self.len -= 1;

            A::Pointer::from_ptr(item.as_ptr())
        }
    }

    fn access(&mut self, ptr: &A::Pointer) {
        unsafe {
            let item = NonNull::new_unchecked(A::Pointer::as_ptr(ptr) as *mut _);
            let link = self.adapter.item2link(item);

            assert!(link.as_ref().is_linked());

            self.move_to_lru_front(link);

            self.update_frequencies(link);
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> LfuIter<A> {
        let mut iter_main = self.lru_main.iter();
        let mut iter_tiny = self.lru_tiny.iter();

        iter_main.back();
        iter_tiny.back();

        LfuIter {
            lfu: self,
            iter_main,
            iter_tiny,

            ptr: ManuallyDrop::new(None),
        }
    }

    fn maybe_grow_access_counters(&mut self) {
        let capacity = self.lru_tiny.len() + self.lru_main.len();

        // If the new capacity ask is more than double the current size,
        // recreate the approximate frequency counters.
        if 2 * self.capacity > capacity {
            return;
        }

        self.capacity = std::cmp::max(capacity, MIN_CAPACITY);

        // The window counter that's incremented on every fetch.
        self.window_size = 0;

        // The frequency counters are halved every `max_window_size` fetches to decay the frequency counts.
        self.max_window_size = self.capacity * self.config.window_to_cache_size_ratio;

        // Number of frequency counters - roughly equal to the window size divided by error tolerance.
        let num_counters = (1f64.exp() * self.max_window_size as f64 / ERROR_THRESHOLD) as usize;
        let num_counters = num_counters.next_power_of_two();

        self.frequencies = CMSketchUsize::new_with_size(num_counters, HASH_COUNT);
    }

    unsafe fn update_frequencies(&mut self, link: NonNull<LfuLink>) {
        self.frequencies.record(self.hash_link(link));
        self.window_size += 1;

        // Decay counts every `max_window_size`. This avoids having items that were
        // accessed frequently (were hot) but aren't being accessed anymore (are cold)
        // from staying in cache forever.
        if self.window_size == self.max_window_size {
            self.window_size >>= 1;
            self.frequencies.decay(DECAY_FACTOR);
        }
    }

    fn maybe_promote_tail(&mut self) {
        unsafe {
            let link_main = match self.lru_main.back() {
                Some(link) => link.raw(),
                None => return,
            };
            let link_tiny = match self.lru_tiny.back() {
                Some(link) => link.raw(),
                None => return,
            };

            if self.admit_to_main(link_main, link_tiny) {
                self.switch_to_lru_front(link_main);
                self.switch_to_lru_front(link_tiny);
                return;
            }

            // A node with high frequency at the tail of main cache might prevent
            // promotions from tiny cache from happening for a long time. Relocate
            // the tail of main cache to prevent this.
            self.move_to_lru_front(link_main);
        }
    }

    fn admit_to_main(&self, link_main: NonNull<LfuLink>, link_tiny: NonNull<LfuLink>) -> bool {
        unsafe {
            assert_eq!(link_main.as_ref().lru_type(), LruType::Main);
            assert_eq!(link_tiny.as_ref().lru_type(), LruType::Tiny);

            let frequent_main = self.frequencies.count(self.hash_link(link_main));
            let frequent_tiny = self.frequencies.count(self.hash_link(link_tiny));

            frequent_main <= frequent_tiny
        }
    }

    unsafe fn move_to_lru_front(&mut self, link: NonNull<LfuLink>) {
        match link.as_ref().lru_type() {
            LruType::Tiny => {
                let raw = link.as_ref().link_tiny.raw();
                let ptr = self.lru_tiny.iter_mut_from_raw(raw).remove().unwrap();
                self.lru_tiny.push_front(ptr);
            }
            LruType::Main => {
                let raw = link.as_ref().link_main.raw();
                let ptr = self.lru_main.iter_mut_from_raw(raw).remove().unwrap();
                self.lru_main.push_front(ptr);
            }
            LruType::None => unreachable!(),
        }
    }

    unsafe fn switch_to_lru_front(&mut self, link: NonNull<LfuLink>) {
        match link.as_ref().lru_type() {
            LruType::Tiny => {
                let raw = link.as_ref().link_tiny.raw();
                let ptr = self.lru_tiny.iter_mut_from_raw(raw).remove().unwrap();
                self.lru_main.push_front(ptr);
            }
            LruType::Main => {
                let raw = link.as_ref().link_main.raw();
                let ptr = self.lru_main.iter_mut_from_raw(raw).remove().unwrap();
                self.lru_tiny.push_front(ptr);
            }
            LruType::None => unreachable!(),
        }
    }

    unsafe fn remove_from_lru(&mut self, link: NonNull<LfuLink>) {
        match link.as_ref().lru_type() {
            LruType::Tiny => {
                let raw = link.as_ref().link_tiny.raw();
                self.lru_tiny.iter_mut_from_raw(raw).remove().unwrap();
            }
            LruType::Main => {
                let raw = link.as_ref().link_main.raw();
                self.lru_main.iter_mut_from_raw(raw).remove().unwrap();
            }
            LruType::None => unreachable!(),
        }
    }

    fn hash_link(&self, link: NonNull<LfuLink>) -> u64 {
        let mut hasher = XxHash64::default();
        let key = unsafe {
            let item = self.adapter.link2item(link);
            let key = self.adapter.item2key(item);
            key.as_ref()
        };
        key.hash(&mut hasher);
        hasher.finish()
    }
}

pub struct LfuIter<'a, A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
    lfu: &'a Lfu<A>,
    iter_tiny: DlistIter<'a, LfuLinkTinyDlistAdapter>,
    iter_main: DlistIter<'a, LfuLinkMainDlistAdapter>,

    ptr: ManuallyDrop<Option<<A as Adapter>::Pointer>>,
}

impl<'a, A> LfuIter<'a, A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
    unsafe fn update_ptr(&mut self, link: NonNull<LfuLink>) {
        std::mem::forget(self.ptr.take());

        let item = self.lfu.adapter.link2item(link);
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

impl<'a, A> Iterator for LfuIter<'a, A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
    type Item = &'a A::Pointer;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let link_main = self.iter_main.get();
            let link_tiny = self.iter_tiny.get();

            let link = match (link_main, link_tiny) {
                (None, None) => return None,
                (Some(link_main), None) => {
                    let link = link_main.raw();
                    self.iter_main.prev();
                    link
                }
                (None, Some(link_tiny)) => {
                    let link = link_tiny.raw();
                    self.iter_tiny.prev();
                    link
                }
                (Some(link_main), Some(link_tiny)) => {
                    // Eviction from tiny or main depending on whether the tiny handle woould be
                    // admitted to main cachce. If it would be, evict from main cache, otherwise
                    // from tiny cache.
                    if self.lfu.admit_to_main(link_main.raw(), link_tiny.raw()) {
                        let link = link_main.raw();
                        self.iter_main.prev();
                        link
                    } else {
                        let link = link_tiny.raw();
                        self.iter_tiny.prev();
                        link
                    }
                }
            };
            self.update_ptr(link);
            self.ptr()
        }
    }
}

// unsafe impl `Send + Sync` for structs with `NonNull` usage

unsafe impl<A> Send for Lfu<A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
}
unsafe impl<A> Sync for Lfu<A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
}

unsafe impl Send for LfuLink {}
unsafe impl Sync for LfuLink {}

unsafe impl<'a, A> Send for LfuIter<'a, A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,

    <A as Adapter>::Pointer: Clone,
{
}
unsafe impl<'a, A> Sync for LfuIter<'a, A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
}

impl<A> EvictionPolicy for Lfu<A>
where
    A: Adapter<Link = LfuLink> + KeyAdapter<Link = LfuLink>,
    <A as Adapter>::Pointer: Clone,
{
    type Adapter = A;
    type Config = LfuConfig;

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
    struct LfuItem {
        link: LfuLink,
        key: u64,
    }

    impl LfuItem {
        fn new(key: u64) -> Self {
            Self {
                link: LfuLink::default(),
                key,
            }
        }
    }

    intrusive_adapter! { LfuItemAdapter = Arc<LfuItem>: LfuItem { link: LfuLink } }
    key_adapter! { LfuItemAdapter = LfuItem { key: u64 } }

    #[test]
    fn test_lfu_simple() {
        let config = LfuConfig {
            window_to_cache_size_ratio: 10,
            tiny_lru_capacity_ratio: 0.01,
        };
        let mut lfu = Lfu::<LfuItemAdapter>::new(config);

        let items = (0..101).map(LfuItem::new).map(Arc::new).collect_vec();
        for item in items.iter().take(100) {
            lfu.insert(item.clone());
        }

        assert_eq!(99, lfu.lru_main.len());
        assert_eq!(1, lfu.lru_tiny.len());
        assert_eq!(items[0].link.lru_type(), LruType::Tiny);

        // 0 will be evicted at last because it is on tiny lru but its frequency equals to others
        assert_eq!(
            (1..100).chain([0].into_iter()).collect_vec(),
            lfu.iter().map(|item| item.key).collect_vec()
        );

        for item in items.iter().take(100) {
            lfu.access(item);
        }
        lfu.access(&items[0]);
        lfu.insert(items[100].clone());

        assert_eq!(items[0].link.lru_type(), LruType::Main);
        assert_eq!(items[100].link.lru_type(), LruType::Tiny);

        assert_eq!(
            [100].into_iter().chain(1..100).chain([0].into_iter()).collect_vec(),
            lfu.iter().map(|item| item.key).collect_vec()
        );

        drop(lfu);

        for item in items {
            assert_eq!(Arc::strong_count(&item), 1);
        }
    }
}
