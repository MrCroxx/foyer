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

use std::fmt::Debug;

use crate::core::adapter::Adapter;

pub trait Config = Send + Sync + 'static + Debug + Clone;

pub trait EvictionPolicy: Send + Sync + Debug + 'static {
    type Adapter: Adapter;
    type Config: Config;

    fn new(config: Self::Config) -> Self;

    fn insert(&mut self, ptr: <Self::Adapter as Adapter>::Pointer);

    fn remove(&mut self, ptr: &<Self::Adapter as Adapter>::Pointer) -> <Self::Adapter as Adapter>::Pointer;

    fn access(&mut self, ptr: &<Self::Adapter as Adapter>::Pointer);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> impl Iterator<Item = &'_ <Self::Adapter as Adapter>::Pointer> + '_;
}

pub trait EvictionPolicyExt: EvictionPolicy {
    fn push(&mut self, ptr: <Self::Adapter as Adapter>::Pointer);

    fn pop(&mut self) -> Option<<Self::Adapter as Adapter>::Pointer>;

    fn peek(&self) -> Option<&<Self::Adapter as Adapter>::Pointer>;
}

impl<E: EvictionPolicy> EvictionPolicyExt for E
where
    <E::Adapter as Adapter>::Pointer: Clone,
{
    fn push(&mut self, ptr: <Self::Adapter as Adapter>::Pointer) {
        self.insert(ptr)
    }

    fn pop(&mut self) -> Option<<E::Adapter as Adapter>::Pointer> {
        let ptr = {
            let mut iter = self.iter();
            let ptr = iter.next();
            ptr.cloned()
        };
        ptr.map(|ptr| self.remove(&ptr))
    }

    fn peek(&self) -> Option<&<Self::Adapter as Adapter>::Pointer> {
        self.iter().next()
    }
}

pub mod fifo;
pub mod lfu;
pub mod lru;
pub mod sfifo;
