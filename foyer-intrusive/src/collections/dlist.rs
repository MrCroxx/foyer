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

use std::ptr::NonNull;

use crate::core::{
    adapter::{Adapter, Link},
    pointer::Pointer,
};

#[derive(Debug, Default)]
pub struct DlistLink {
    prev: Option<NonNull<DlistLink>>,
    next: Option<NonNull<DlistLink>>,
    is_linked: bool,
}

impl DlistLink {
    pub fn raw(&self) -> NonNull<DlistLink> {
        unsafe { NonNull::new_unchecked(self as *const _ as *mut _) }
    }

    pub fn prev(&self) -> Option<NonNull<DlistLink>> {
        self.prev
    }

    pub fn next(&self) -> Option<NonNull<DlistLink>> {
        self.next
    }
}

unsafe impl Send for DlistLink {}
unsafe impl Sync for DlistLink {}

impl Link for DlistLink {
    fn is_linked(&self) -> bool {
        self.is_linked
    }
}

#[derive(Debug)]
pub struct Dlist<A>
where
    A: Adapter<Link = DlistLink>,
{
    head: Option<NonNull<DlistLink>>,
    tail: Option<NonNull<DlistLink>>,

    len: usize,

    adapter: A,
}

impl<A> Drop for Dlist<A>
where
    A: Adapter<Link = DlistLink>,
{
    fn drop(&mut self) {
        let mut iter = self.iter_mut();
        iter.front();
        while iter.is_valid() {
            iter.remove();
        }
        assert!(self.is_empty());
    }
}

impl<A> Dlist<A>
where
    A: Adapter<Link = DlistLink>,
{
    pub fn new() -> Self {
        Self {
            head: None,
            tail: None,
            len: 0,

            adapter: A::new(),
        }
    }

    pub fn front(&self) -> Option<&<A::Pointer as Pointer>::Item> {
        unsafe {
            self.head
                .map(|link| self.adapter.link2item(link))
                .map(|link| link.as_ref())
        }
    }

    pub fn back(&self) -> Option<&<A::Pointer as Pointer>::Item> {
        unsafe {
            self.tail
                .map(|link| self.adapter.link2item(link))
                .map(|link| link.as_ref())
        }
    }

    pub fn front_mut(&mut self) -> Option<&mut <A::Pointer as Pointer>::Item> {
        unsafe {
            self.head
                .map(|link| self.adapter.link2item(link))
                .map(|mut link| link.as_mut())
        }
    }

    pub fn back_mut(&mut self) -> Option<&mut <A::Pointer as Pointer>::Item> {
        unsafe {
            self.tail
                .map(|link| self.adapter.link2item(link))
                .map(|mut link| link.as_mut())
        }
    }

    pub fn push_front(&mut self, ptr: A::Pointer) {
        self.iter_mut().insert_after(ptr);
    }

    pub fn push_back(&mut self, ptr: A::Pointer) {
        self.iter_mut().insert_before(ptr);
    }

    pub fn pop_front(&mut self) -> Option<A::Pointer> {
        let mut iter = self.iter_mut();
        iter.next();
        iter.remove()
    }

    pub fn pop_back(&mut self) -> Option<A::Pointer> {
        let mut iter = self.iter_mut();
        iter.prev();
        iter.remove()
    }

    pub fn iter(&self) -> DlistIter<'_, A> {
        DlistIter {
            link: None,
            dlist: self,
        }
    }

    pub fn iter_mut(&mut self) -> DlistIterMut<'_, A> {
        DlistIterMut {
            link: None,
            dlist: self,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the prev element of the given `link`.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn prev_of_raw(&self, link: NonNull<DlistLink>) -> Option<&<A::Pointer as Pointer>::Item> {
        link.as_ref().prev().map(|link| self.adapter.link2item(link).as_ref())
    }

    /// Get the mutable prev element of the given `link`.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn prev_mut_of_raw(&self, link: NonNull<DlistLink>) -> Option<&mut <A::Pointer as Pointer>::Item> {
        link.as_ref().prev().map(|link| self.adapter.link2item(link).as_mut())
    }

    /// Get the next element of the given `link`.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn next_of_raw(&self, link: NonNull<DlistLink>) -> Option<&<A::Pointer as Pointer>::Item> {
        link.as_ref().next().map(|link| self.adapter.link2item(link).as_ref())
    }

    /// Get the mutable next element of the given `link`.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn next_mut_of_raw(&self, link: NonNull<DlistLink>) -> Option<&mut <A::Pointer as Pointer>::Item> {
        link.as_ref().next().map(|link| self.adapter.link2item(link).as_mut())
    }

    /// Remove an node that holds the given raw link.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn remove_raw(&mut self, link: NonNull<DlistLink>) -> A::Pointer {
        let mut iter = self.iter_mut_from_raw(link);
        debug_assert!(iter.is_valid());
        iter.remove().unwrap_unchecked()
    }

    /// Create mutable iterator directly on raw link.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn iter_mut_from_raw(&mut self, link: NonNull<DlistLink>) -> DlistIterMut<'_, A> {
        DlistIterMut {
            link: Some(link),
            dlist: self,
        }
    }

    /// Create immutable iterator directly on raw link.
    ///
    /// # Safety
    ///
    /// `link` MUST be in this [`Dlist`].
    pub unsafe fn iter_from_raw(&self, link: NonNull<DlistLink>) -> DlistIter<'_, A> {
        DlistIter {
            link: Some(link),
            dlist: self,
        }
    }

    /// # Safety
    ///
    /// `self` must be empty. `src` will be set empty after operation.
    pub unsafe fn replace_with(&mut self, src: &mut Dlist<A>) {
        debug_assert!(self.head.is_none());
        debug_assert!(self.tail.is_none());
        debug_assert_eq!(self.len, 0);

        self.head = src.head;
        self.tail = src.tail;
        self.len = src.len;

        src.head = None;
        src.tail = None;
        src.len = 0;
    }
}

pub struct DlistIter<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    link: Option<NonNull<A::Link>>,
    dlist: &'a Dlist<A>,
}

impl<'a, A> DlistIter<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    pub fn is_valid(&self) -> bool {
        self.link.is_some()
    }

    pub fn get(&self) -> Option<&<A::Pointer as Pointer>::Item> {
        self.link
            .map(|link| unsafe { self.dlist.adapter.link2item(link).as_ref() })
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().next,
                None => self.link = self.dlist.head,
            }
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().prev,
                None => self.link = self.dlist.tail,
            }
        }
    }

    /// Move to head.
    pub fn front(&mut self) {
        self.link = self.dlist.head;
    }

    /// Move to head.
    pub fn back(&mut self) {
        self.link = self.dlist.tail;
    }

    pub fn is_front(&self) -> bool {
        self.link == self.dlist.head
    }

    pub fn is_back(&self) -> bool {
        self.link == self.dlist.tail
    }
}

pub struct DlistIterMut<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    link: Option<NonNull<A::Link>>,
    dlist: &'a mut Dlist<A>,
}

impl<'a, A> DlistIterMut<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    pub fn is_valid(&self) -> bool {
        self.link.is_some()
    }

    pub fn get(&self) -> Option<&<A::Pointer as Pointer>::Item> {
        self.link
            .map(|link| unsafe { self.dlist.adapter.link2item(link).as_ref() })
    }

    pub fn get_mut(&mut self) -> Option<&mut <A::Pointer as Pointer>::Item> {
        self.link
            .map(|link| unsafe { self.dlist.adapter.link2item(link).as_mut() })
    }

    /// Move to next.
    ///
    /// If iter is on tail, move to null.
    /// If iter is on null, move to head.
    pub fn next(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().next,
                None => self.link = self.dlist.head,
            }
        }
    }

    /// Move to prev.
    ///
    /// If iter is on head, move to null.
    /// If iter is on null, move to tail.
    pub fn prev(&mut self) {
        unsafe {
            match self.link {
                Some(link) => self.link = link.as_ref().prev,
                None => self.link = self.dlist.tail,
            }
        }
    }

    /// Move to front.
    pub fn front(&mut self) {
        self.link = self.dlist.head;
    }

    /// Move to back.
    pub fn back(&mut self) {
        self.link = self.dlist.tail;
    }

    /// Removes the current item from [`Dlist`] and move next.
    pub fn remove(&mut self) -> Option<A::Pointer> {
        unsafe {
            if !self.is_valid() {
                return None;
            }

            debug_assert!(self.is_valid());
            let mut link = self.link.unwrap_unchecked();

            let item = self.dlist.adapter.link2item(link);
            let ptr = A::Pointer::from_ptr(item.as_ptr());

            // fix head and tail if node is either of that
            let mut prev = link.as_ref().prev;
            let mut next = link.as_ref().next;
            if Some(link) == self.dlist.head {
                self.dlist.head = next;
            }
            if Some(link) == self.dlist.tail {
                self.dlist.tail = prev;
            }

            // fix the next and prev ptrs of the node before and after this
            if let Some(prev) = &mut prev {
                prev.as_mut().next = next;
            }
            if let Some(next) = &mut next {
                next.as_mut().prev = prev;
            }

            link.as_mut().next = None;
            link.as_mut().prev = None;
            link.as_mut().is_linked = false;

            self.dlist.len -= 1;

            self.link = next;

            Some(ptr)
        }
    }

    /// Link a new ptr before the current one.
    ///
    /// If iter is on null, link to tail.
    pub fn insert_before(&mut self, ptr: A::Pointer) {
        unsafe {
            let item_new = NonNull::new_unchecked(A::Pointer::into_ptr(ptr) as *mut _);
            let mut link_new = self.dlist.adapter.item2link(item_new);
            assert!(!link_new.as_ref().is_linked());

            match self.link {
                Some(link) => self.link_before(link_new, link),
                None => {
                    self.link_between(link_new, self.dlist.tail, None);
                    self.dlist.tail = Some(link_new);
                }
            }

            if self.dlist.head == self.link {
                self.dlist.head = Some(link_new);
            }

            link_new.as_mut().is_linked = true;

            self.dlist.len += 1;
        }
    }

    /// Link a new ptr after the current one.
    ///
    /// If iter is on null, link to head.
    pub fn insert_after(&mut self, ptr: A::Pointer) {
        unsafe {
            let item_new = NonNull::new_unchecked(A::Pointer::into_ptr(ptr) as *mut _);
            let mut link_new = self.dlist.adapter.item2link(item_new);
            assert!(!link_new.as_ref().is_linked());

            match self.link {
                Some(link) => self.link_after(link_new, link),
                None => {
                    self.link_between(link_new, None, self.dlist.head);
                    self.dlist.head = Some(link_new);
                }
            }

            if self.dlist.tail == self.link {
                self.dlist.tail = Some(link_new);
            }

            link_new.as_mut().is_linked = true;

            self.dlist.len += 1;
        }
    }

    unsafe fn link_before(&mut self, link: NonNull<A::Link>, next: NonNull<A::Link>) {
        self.link_between(link, next.as_ref().prev, Some(next));
    }

    unsafe fn link_after(&mut self, link: NonNull<A::Link>, prev: NonNull<A::Link>) {
        self.link_between(link, Some(prev), prev.as_ref().next);
    }

    unsafe fn link_between(
        &mut self,
        mut link: NonNull<A::Link>,
        mut prev: Option<NonNull<A::Link>>,
        mut next: Option<NonNull<A::Link>>,
    ) {
        if let Some(prev) = &mut prev {
            prev.as_mut().next = Some(link);
        }
        if let Some(next) = &mut next {
            next.as_mut().prev = Some(link);
        }
        link.as_mut().prev = prev;
        link.as_mut().next = next;
    }

    pub fn is_front(&self) -> bool {
        self.link == self.dlist.head
    }

    pub fn is_back(&self) -> bool {
        self.link == self.dlist.tail
    }
}

impl<'a, A> Iterator for DlistIter<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    type Item = &'a <A::Pointer as Pointer>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        match self.link {
            Some(link) => Some(unsafe { self.dlist.adapter.link2item(link).as_ref() }),
            None => None,
        }
    }
}

impl<'a, A> Iterator for DlistIterMut<'a, A>
where
    A: Adapter<Link = DlistLink>,
{
    type Item = &'a mut <A::Pointer as Pointer>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.next();
        match self.link {
            Some(link) => Some(unsafe { self.dlist.adapter.link2item(link).as_mut() }),
            None => None,
        }
    }
}

// TODO(MrCroxx): Need more tests.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use super::*;
    use crate::intrusive_adapter;

    #[derive(Debug)]
    struct DlistItem {
        link: DlistLink,
        val: u64,
    }

    impl DlistItem {
        fn new(val: u64) -> Self {
            Self {
                link: DlistLink::default(),
                val,
            }
        }
    }

    #[derive(Debug, Default)]
    struct DlistAdapter;

    unsafe impl Adapter for DlistAdapter {
        type Pointer = Box<DlistItem>;

        type Link = DlistLink;

        fn new() -> Self {
            Self
        }

        unsafe fn link2item(&self, link: NonNull<Self::Link>) -> NonNull<<Self::Pointer as Pointer>::Item> {
            NonNull::new_unchecked(crate::container_of!(link.as_ptr(), DlistItem, link) as *mut _)
        }

        unsafe fn item2link(&self, item: NonNull<<Self::Pointer as Pointer>::Item>) -> NonNull<Self::Link> {
            NonNull::new_unchecked((item.as_ptr() as *const u8).add(crate::offset_of!(DlistItem, link)) as *mut _)
        }
    }

    intrusive_adapter! { DlistArcAdapter = Arc<DlistItem>: DlistItem { link: DlistLink } }

    #[test]
    fn test_dlist_simple() {
        let mut l = Dlist::<DlistAdapter>::new();

        l.push_back(Box::new(DlistItem::new(2)));
        l.push_front(Box::new(DlistItem::new(1)));
        l.push_back(Box::new(DlistItem::new(3)));

        let v = l.iter_mut().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1, 2, 3]);
        assert_eq!(l.len(), 3);

        let mut iter = l.iter_mut();
        iter.next();
        iter.next();
        assert_eq!(iter.get().unwrap().val, 2);
        let i2 = iter.remove();
        assert_eq!(i2.unwrap().val, 2);
        assert_eq!(iter.get().unwrap().val, 3);
        let v = l.iter_mut().map(|item| item.val).collect_vec();
        assert_eq!(v, vec![1, 3]);
        assert_eq!(l.len(), 2);

        let i3 = l.pop_back();
        assert_eq!(i3.unwrap().val, 3);
        let i1 = l.pop_front();
        assert_eq!(i1.unwrap().val, 1);
        assert!(l.pop_front().is_none());
        assert_eq!(l.len(), 0);
    }

    #[test]
    fn test_arc_drop() {
        let mut l = Dlist::<DlistArcAdapter>::new();

        let items = (0..10).map(|i| Arc::new(DlistItem::new(i))).collect_vec();
        for item in items.iter() {
            l.push_back(item.clone());
        }
        for item in items.iter() {
            assert_eq!(Arc::strong_count(item), 2);
        }
        drop(l);
        for item in items.iter() {
            assert_eq!(Arc::strong_count(item), 1);
        }
    }
}
