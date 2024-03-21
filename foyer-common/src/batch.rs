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

use parking_lot::Mutex;
use tokio::sync::oneshot;

const DEFAULT_CAPACITY: usize = 16;

pub enum Identity<T> {
    Leader(oneshot::Receiver<T>),
    Follower(oneshot::Receiver<T>),
}

#[derive(Debug)]
pub struct Item<A, T> {
    pub arg: A,
    pub tx: oneshot::Sender<T>,
}

#[derive(Debug)]
pub struct Batch<A, T> {
    queue: Mutex<Vec<Item<A, T>>>,
}

impl<A, T> Default for Batch<A, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A, T> Batch<A, T> {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            queue: Mutex::new(Vec::with_capacity(capacity)),
        }
    }

    pub fn push(&self, arg: A) -> Identity<T> {
        let (tx, rx) = oneshot::channel();
        let item = Item { arg, tx };
        let mut queue = self.queue.lock();
        let is_leader = queue.is_empty();
        queue.push(item);
        if is_leader {
            Identity::Leader(rx)
        } else {
            Identity::Follower(rx)
        }
    }

    pub fn rotate(&self) -> Vec<Item<A, T>> {
        let mut queue = self.queue.lock();
        let mut q = Vec::with_capacity(queue.capacity());
        std::mem::swap(&mut *queue, &mut q);
        q
    }
}
