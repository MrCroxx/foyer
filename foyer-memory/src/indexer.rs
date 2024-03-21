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

use hashbrown::hash_table::{Entry as HashTableEntry, HashTable};

use crate::{handle::Handle, Key};

pub trait Indexer: Send + Sync + 'static {
    type Key: Key;
    type Handle: Handle<Key = Self::Key>;

    fn new() -> Self;
    unsafe fn insert(&mut self, handle: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>>;
    unsafe fn get(&self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>>;
    unsafe fn remove(&mut self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>>;
    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>>;
}

pub struct HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
    table: HashTable<NonNull<H>>,
}

unsafe impl<K, H> Send for HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
}

unsafe impl<K, H> Sync for HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
}

impl<K, H> Indexer for HashTableIndexer<K, H>
where
    K: Key,
    H: Handle<Key = K>,
{
    type Key = K;
    type Handle = H;

    fn new() -> Self {
        Self {
            table: HashTable::new(),
        }
    }

    unsafe fn insert(&mut self, mut ptr: NonNull<Self::Handle>) -> Option<NonNull<Self::Handle>> {
        let base = ptr.as_mut().base_mut();

        debug_assert!(!base.is_in_indexer());
        base.set_in_indexer(true);

        match self.table.entry(
            base.hash(),
            |p| p.as_ref().base().key() == base.key(),
            |p| p.as_ref().base().hash(),
        ) {
            HashTableEntry::Occupied(mut o) => {
                std::mem::swap(o.get_mut(), &mut ptr);
                let b = ptr.as_mut().base_mut();
                debug_assert!(b.is_in_indexer());
                b.set_in_indexer(false);
                Some(ptr)
            }
            HashTableEntry::Vacant(v) => {
                v.insert(ptr);
                None
            }
        }
    }

    unsafe fn get(&self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>> {
        self.table.find(hash, |p| p.as_ref().base().key() == key).copied()
    }

    unsafe fn remove(&mut self, hash: u64, key: &Self::Key) -> Option<NonNull<Self::Handle>> {
        match self
            .table
            .entry(hash, |p| p.as_ref().base().key() == key, |p| p.as_ref().base().hash())
        {
            HashTableEntry::Occupied(o) => {
                let (mut p, _) = o.remove();
                let b = p.as_mut().base_mut();
                debug_assert!(b.is_in_indexer());
                b.set_in_indexer(false);
                Some(p)
            }
            HashTableEntry::Vacant(_) => None,
        }
    }

    unsafe fn drain(&mut self) -> impl Iterator<Item = NonNull<Self::Handle>> {
        self.table.drain().map(|mut ptr| {
            ptr.as_mut().base_mut().set_in_indexer(false);
            ptr
        })
    }
}
