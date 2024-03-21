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

use bitflags::bitflags;

use crate::{context::Context, Key, Value};

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct BaseHandleFlags: u8 {
        const IN_INDEXER = 0b00000001;
        const IN_EVICTION = 0b00000010;
    }
}

pub trait Handle: Send + Sync + 'static {
    type Key: Key;
    type Value: Value;
    type Context: Context;

    fn new() -> Self;
    fn init(&mut self, hash: u64, key: Self::Key, value: Self::Value, charge: usize, context: Self::Context);

    fn base(&self) -> &BaseHandle<Self::Key, Self::Value, Self::Context>;
    fn base_mut(&mut self) -> &mut BaseHandle<Self::Key, Self::Value, Self::Context>;
}

#[derive(Debug)]
pub struct BaseHandle<K, V, C>
where
    K: Key,
    V: Value,
{
    /// key, value, context
    entry: Option<(K, V, C)>,
    /// key hash
    hash: u64,
    /// entry charge
    charge: usize,
    /// external reference count
    refs: usize,
    /// flags that used by the general cache abstraction
    flags: BaseHandleFlags,
}

impl<K, V, C> Default for BaseHandle<K, V, C>
where
    K: Key,
    V: Value,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, C> BaseHandle<K, V, C>
where
    K: Key,
    V: Value,
{
    /// Create a uninited handle.
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            entry: None,
            hash: 0,
            charge: 0,
            refs: 0,
            flags: BaseHandleFlags::empty(),
        }
    }

    /// Init handle with args.
    #[inline(always)]
    pub fn init(&mut self, hash: u64, key: K, value: V, charge: usize, context: C) {
        debug_assert!(self.entry.is_none());
        self.hash = hash;
        self.entry = Some((key, value, context));
        self.charge = charge;
        self.refs = 0;
        self.flags = BaseHandleFlags::empty();
    }

    /// Take key and value from the handle and reset it to the uninited state.
    #[inline(always)]
    pub fn take(&mut self) -> (K, V, C, usize) {
        debug_assert!(self.entry.is_some());
        unsafe {
            self.entry
                .take()
                .map(|(key, value, context)| (key, value, context, self.charge))
                .unwrap_unchecked()
        }
    }

    /// Return `true` if the handle is inited.
    #[inline(always)]
    pub fn is_inited(&self) -> bool {
        self.entry.is_some()
    }

    /// Get key hash.
    ///
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Get key reference.
    ///  
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn key(&self) -> &K {
        debug_assert!(self.entry.is_some());
        unsafe { self.entry.as_ref().map(|entry| &entry.0).unwrap_unchecked() }
    }

    /// Get value reference.
    ///  
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn value(&self) -> &V {
        debug_assert!(self.entry.is_some());
        unsafe { self.entry.as_ref().map(|entry| &entry.1).unwrap_unchecked() }
    }

    /// Get context reference.
    ///  
    /// # Panics
    ///
    /// Panics if the handle is uninited.
    #[inline(always)]
    pub fn context(&self) -> &C {
        debug_assert!(self.entry.is_some());
        unsafe { self.entry.as_ref().map(|entry| &entry.2).unwrap_unchecked() }
    }

    /// Get the charge of the handle.
    #[inline(always)]
    pub fn charge(&self) -> usize {
        self.charge
    }

    /// Increase the external reference count of the handle, returns the new reference count.
    #[inline(always)]
    pub fn inc_refs(&mut self) -> usize {
        self.inc_refs_by(1)
    }

    /// Increase the external reference count of the handle, returns the new reference count.
    #[inline(always)]
    pub fn inc_refs_by(&mut self, val: usize) -> usize {
        self.refs += val;
        self.refs
    }

    /// Decrease the external reference count of the handle, returns the new reference count.
    #[inline(always)]
    pub fn dec_refs(&mut self) -> usize {
        self.refs -= 1;
        self.refs
    }

    /// Get the external reference count of the handle.
    #[inline(always)]
    pub fn refs(&self) -> usize {
        self.refs
    }

    /// Return `true` if there are external references.
    #[inline(always)]
    pub fn has_refs(&self) -> bool {
        self.refs() > 0
    }

    #[inline(always)]
    pub fn set_in_indexer(&mut self, in_cache: bool) {
        if in_cache {
            self.flags |= BaseHandleFlags::IN_INDEXER;
        } else {
            self.flags -= BaseHandleFlags::IN_INDEXER;
        }
    }

    #[inline(always)]
    pub fn is_in_indexer(&self) -> bool {
        !(self.flags & BaseHandleFlags::IN_INDEXER).is_empty()
    }

    #[inline(always)]
    pub fn set_in_eviction(&mut self, in_eviction: bool) {
        if in_eviction {
            self.flags |= BaseHandleFlags::IN_EVICTION;
        } else {
            self.flags -= BaseHandleFlags::IN_EVICTION;
        }
    }

    #[inline(always)]
    pub fn is_in_eviction(&self) -> bool {
        !(self.flags & BaseHandleFlags::IN_EVICTION).is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_handle_basic() {
        let mut h = BaseHandle::<(), (), ()>::new();
        assert!(!h.is_in_indexer());
        assert!(!h.is_in_eviction());

        h.set_in_indexer(true);
        h.set_in_eviction(true);
        assert!(h.is_in_indexer());
        assert!(h.is_in_eviction());

        h.set_in_indexer(false);
        h.set_in_eviction(false);
        assert!(!h.is_in_indexer());
        assert!(!h.is_in_eviction());
    }
}
