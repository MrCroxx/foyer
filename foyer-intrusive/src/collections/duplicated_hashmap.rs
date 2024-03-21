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

use std::{fmt::Debug, hash::Hasher, marker::PhantomData, ptr::NonNull};

use foyer_common::code::{Key, Value};
use twox_hash::XxHash64;

use super::dlist::{Dlist, DlistIter, DlistIterMut, DlistLink};
use crate::{
    core::{
        adapter::{Adapter, KeyAdapter, Link},
        pointer::Pointer,
    },
    intrusive_adapter,
};

pub struct DuplicatedHashMapLink {
    slot_link: DlistLink,
    group_link: DlistLink,
    group: Dlist<DuplicatedHashMapLinkGroupAdapter>,
}

impl Default for DuplicatedHashMapLink {
    fn default() -> Self {
        Self {
            slot_link: DlistLink::default(),
            group_link: DlistLink::default(),
            group: Dlist::new(),
        }
    }
}

impl Debug for DuplicatedHashMapLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuplicatedHashMapLink")
            .field("slot_link", &self.slot_link)
            .field("group_link", &self.group_link)
            .finish()
    }
}

intrusive_adapter! { DuplicatedHashMapLinkSlotAdapter = NonNull<DuplicatedHashMapLink>: DuplicatedHashMapLink { slot_link: DlistLink } }
intrusive_adapter! { DuplicatedHashMapLinkGroupAdapter = NonNull<DuplicatedHashMapLink>: DuplicatedHashMapLink { group_link: DlistLink } }

impl DuplicatedHashMapLink {
    pub fn raw(&self) -> NonNull<DuplicatedHashMapLink> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }
}

unsafe impl Send for DuplicatedHashMapLink {}
unsafe impl Sync for DuplicatedHashMapLink {}

impl Link for DuplicatedHashMapLink {
    fn is_linked(&self) -> bool {
        self.group_link.is_linked()
    }
}

pub struct DuplicatedHashMap<K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = DuplicatedHashMapLink>,
{
    slots: Vec<Dlist<DuplicatedHashMapLinkSlotAdapter>>,

    len: usize,

    adapter: A,

    _marker: PhantomData<V>,
}

impl<K, V, A> Drop for DuplicatedHashMap<K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = DuplicatedHashMapLink>,
{
    fn drop(&mut self) {
        unsafe {
            for slot in self.slots.iter_mut() {
                let mut iter_slot = slot.iter_mut();
                iter_slot.front();
                while iter_slot.is_valid() {
                    let mut link_slot = iter_slot.remove().unwrap();
                    let mut iter_group = link_slot.as_mut().group.iter_mut();
                    iter_group.front();
                    while iter_group.is_valid() {
                        let link_group = iter_group.remove().unwrap();
                        let item = self.adapter.link2item(link_group);
                        let _ = A::Pointer::from_ptr(item.as_ptr());
                    }
                }
            }
        }
    }
}

impl<K, V, A> DuplicatedHashMap<K, V, A>
where
    K: Key,
    V: Value,
    A: KeyAdapter<Key = K, Link = DuplicatedHashMapLink>,
{
    pub fn new(bits: usize) -> Self {
        let mut slots = Vec::with_capacity(1 << bits);
        for _ in 0..(1 << bits) {
            slots.push(Dlist::new());
        }
        Self {
            slots,
            len: 0,
            adapter: A::new(),
            _marker: PhantomData,
        }
    }

    pub fn insert(&mut self, ptr: A::Pointer) {
        unsafe {
            let item_new = NonNull::new_unchecked(A::Pointer::into_ptr(ptr) as *mut _);
            let mut link_new = self.adapter.item2link(item_new);

            assert!(link_new.as_ref().group.is_empty());

            let key_new = self.adapter.item2key(item_new).as_ref();
            let hash = self.hash_key(key_new);
            let slot = (self.slots.len() - 1) & hash as usize;

            match self.lookup_inner_mut(key_new, slot) {
                Some(mut iter) => {
                    let link = iter.get_mut().unwrap();
                    link.group.push_back(link_new);
                }
                None => {
                    self.slots[slot].push_front(link_new);
                    link_new.as_mut().group.push_back(link_new);
                }
            }

            self.len += 1;
        }
    }

    pub fn remove(&mut self, key: &K) -> Vec<A::Pointer> {
        unsafe {
            let hash = self.hash_key(key);
            let slot = (self.slots.len() - 1) & hash as usize;

            match self.lookup_inner_mut(key, slot) {
                Some(mut iter) => {
                    let mut link = iter.remove().unwrap();
                    let mut res = Vec::with_capacity(link.as_ref().group.len());
                    let mut iter = link.as_mut().group.iter_mut();
                    iter.front();
                    while iter.is_valid() {
                        let link = iter.remove().unwrap();
                        debug_assert!(!link.as_ref().is_linked());
                        let item = self.adapter.link2item(link);
                        let ptr = A::Pointer::from_ptr(item.as_ptr());
                        res.push(ptr);
                    }
                    debug_assert!(link.as_ref().group.is_empty());
                    self.len -= res.len();
                    res
                }
                None => vec![],
            }
        }
    }

    pub fn lookup(&self, key: &K) -> Vec<&<A::Pointer as Pointer>::Item> {
        unsafe {
            let hash = self.hash_key(key);
            let slot = (self.slots.len() - 1) & hash as usize;

            match self.lookup_inner(key, slot) {
                Some(iter) => {
                    let link = iter.get().unwrap();
                    let mut res = Vec::with_capacity(link.group.len());
                    let mut iter = link.group.iter();
                    iter.front();
                    while iter.is_valid() {
                        let link = iter.get().unwrap();
                        let item = self.adapter.link2item(link.raw()).as_ref();
                        res.push(item);
                        iter.next();
                    }
                    res
                }
                None => vec![],
            }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    ///
    /// `link` MUST be in this [`HashMap`].
    pub unsafe fn remove_in_place(&mut self, mut link: NonNull<DuplicatedHashMapLink>) -> A::Pointer {
        assert!(link.as_ref().is_linked());
        let item = self.adapter.link2item(link);
        let key = self.adapter.item2key(item).as_ref();
        let hash = self.hash_key(key);
        let slot = (self.slots.len() - 1) & hash as usize;

        if link.as_ref().slot_link.is_linked() {
            // the removed item is the group header

            // remove from slot list
            self.slots[slot]
                .iter_mut_from_raw(link.as_ref().slot_link.raw())
                .remove();

            // remove from group list
            link.as_mut()
                .group
                .iter_mut_from_raw(link.as_ref().group_link.raw())
                .remove();

            // transfer group list and relink slot list if necessary
            if let Some(header_new) = link.as_mut().group.front_mut() {
                self.slots[slot].push_front(header_new.raw());
                header_new.group.replace_with(&mut link.as_mut().group);
            }
        } else {
            // the removed item is NOT the group header

            // find header link
            let header_link = {
                let mut header = &link.as_ref().group_link;
                while header.prev().is_some() {
                    header = &*header.prev().unwrap().as_ptr();
                }
                let adapter = DuplicatedHashMapLinkGroupAdapter::new();
                adapter.link2item(header.raw()).as_mut()
            };
            debug_assert!(header_link.slot_link.is_linked());

            // remove from group link
            header_link
                .group
                .iter_mut_from_raw(link.as_ref().group_link.raw())
                .remove();
        }

        self.len -= 1;

        debug_assert!(!link.as_ref().slot_link.is_linked());
        debug_assert!(!link.as_ref().group_link.is_linked());
        debug_assert!(link.as_ref().group.is_empty());

        A::Pointer::from_ptr(item.as_ptr())
    }

    /// # Safety
    ///
    /// there must be at most one matches in the slot
    unsafe fn lookup_inner_mut(
        &mut self,
        key: &K,
        slot: usize,
    ) -> Option<DlistIterMut<'_, DuplicatedHashMapLinkSlotAdapter>> {
        let mut iter = self.slots[slot].iter_mut();
        iter.front();
        while iter.is_valid() {
            let item = self.adapter.link2item(iter.get().unwrap().raw());
            let ikey = self.adapter.item2key(item).as_ref();
            if ikey == key {
                return Some(iter);
            }
            iter.next();
        }
        None
    }

    /// # Safety
    ///
    /// there must be at most one matches in the slot
    unsafe fn lookup_inner(&self, key: &K, slot: usize) -> Option<DlistIter<'_, DuplicatedHashMapLinkSlotAdapter>> {
        let mut iter = self.slots[slot].iter();
        iter.front();
        while iter.is_valid() {
            let item = self.adapter.link2item(iter.get().unwrap().raw());
            let ikey = self.adapter.item2key(item).as_ref();
            if ikey == key {
                return Some(iter);
            }
            iter.next();
        }
        None
    }

    fn hash_key(&self, key: &K) -> u64 {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

// TODO(MrCroxx): Need more tests.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::{intrusive_adapter, key_adapter};

    #[derive(Debug)]
    struct DuplicatedHashMapItem {
        link: DuplicatedHashMapLink,

        key: u64,
        value: u64,
    }

    intrusive_adapter! { HashMapItemAdapter = Arc<DuplicatedHashMapItem>: DuplicatedHashMapItem { link: DuplicatedHashMapLink } }
    key_adapter! { HashMapItemAdapter = DuplicatedHashMapItem { key: u64 } }

    #[test]
    fn test_duplicated_hashmap_simple() {
        // 2 slots
        let mut map = DuplicatedHashMap::<u64, u64, HashMapItemAdapter>::new(1);

        // 8 * 8 items
        let items = (0..8)
            .map(|key| {
                (0..8)
                    .map(|value| {
                        Arc::new(DuplicatedHashMapItem {
                            link: DuplicatedHashMapLink::default(),
                            key,
                            value,
                        })
                    })
                    .collect_vec()
            })
            .collect_vec();

        // 8 * 8
        for item in items.iter().flatten() {
            map.insert(item.clone());
        }
        for item in items.iter().flatten() {
            assert_eq!(Arc::strong_count(item), 2);
        }
        for key in 0..8 {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            assert_eq!(vs, (0..8).collect_vec());
        }

        // { 0 2 4 6 } * 8
        for key in (0..8).skip(1).step_by(2) {
            let res = map.remove(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            assert_eq!(vs, (0..8).collect_vec());
        }
        for key in (0..8).step_by(2) {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            assert_eq!(vs, (0..8).collect_vec());
        }

        // 8 * 8
        for item in items.iter().skip(1).step_by(2).flatten() {
            map.insert(item.clone());
        }
        for key in 0..8 {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            assert_eq!(vs, (0..8).collect_vec());
        }

        // remove group member in place
        unsafe { map.remove_in_place(items[4][4].link.raw()) };
        for key in 0..8 {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            if key == 4 {
                assert_eq!(vs, vec![0, 1, 2, 3, 5, 6, 7])
            } else {
                assert_eq!(vs, (0..8).collect_vec());
            }
        }
        // 8 * 8
        map.insert(items[4][4].clone());
        for key in 0..8 {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            assert_eq!(vs, (0..8).collect_vec());
        }

        // remove group head in place
        unsafe { map.remove_in_place(items[4][0].link.raw()) };
        for key in 0..8 {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            if key == 4 {
                assert_eq!(vs, vec![1, 2, 3, 4, 5, 6, 7])
            } else {
                assert_eq!(vs, (0..8).collect_vec());
            }
        }
        // 8 * 8
        map.insert(items[4][0].clone());
        for key in 0..8 {
            let res = map.lookup(&key);
            let mut vs = res.iter().map(|item| item.value).collect_vec();
            vs.sort();
            assert_eq!(vs, (0..8).collect_vec());
        }

        drop(map);

        for item in items.into_iter().flatten() {
            assert_eq!(Arc::strong_count(&item), 1);
        }
    }
}
