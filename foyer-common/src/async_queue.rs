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

use std::{collections::VecDeque, fmt::Debug};

use parking_lot::Mutex;
use tokio::sync::{watch, Notify};

#[derive(Debug)]
pub struct AsyncQueue<T> {
    queue: Mutex<VecDeque<T>>,
    notified: Notify,
    watch_tx: watch::Sender<usize>,
    watch_rx: watch::Receiver<usize>,
}

impl<T: Debug> Default for AsyncQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Debug> AsyncQueue<T> {
    pub fn new() -> Self {
        let (watch_tx, watch_rx) = watch::channel(0);
        Self {
            queue: Mutex::new(VecDeque::default()),
            notified: Notify::new(),
            watch_tx,
            watch_rx,
        }
    }

    pub fn try_acquire(&self) -> Option<T> {
        let mut guard = self.queue.lock();
        if let Some(item) = guard.pop_front() {
            if !guard.is_empty() {
                // Since in `release` we use `notify_one`, not all waiters
                // will be waken up. Therefore if we figure out that the queue is not empty,
                // we call `notify_one` to awake the next pending `acquire`.
                self.notified.notify_one();
            }
            self.watch_tx.send(guard.len()).unwrap();
            Some(item)
        } else {
            None
        }
    }

    pub async fn acquire(&self) -> T {
        loop {
            let notified = self.notified.notified();
            {
                let mut guard = self.queue.lock();
                if let Some(item) = guard.pop_front() {
                    if !guard.is_empty() {
                        // Since in `release` we use `notify_one`, not all waiters
                        // will be waken up. Therefore if we figure out that the queue is not empty,
                        // we call `notify_one` to awake the next pending `acquire`.
                        self.notified.notify_one();
                    }
                    self.watch_tx.send(guard.len()).unwrap();
                    break item;
                }
            }
            notified.await;
        }
    }

    pub fn release(&self, item: T) {
        let mut guard = self.queue.lock();
        guard.push_back(item);
        self.watch_tx.send(guard.len()).unwrap();
        self.notified.notify_one();
    }

    pub fn len(&self) -> usize {
        *self.watch_rx.borrow()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn watch(&self) -> watch::Receiver<usize> {
        self.watch_rx.clone()
    }

    pub fn flash(&self) {
        self.watch_tx.send(self.len()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::{poll_fn, Future},
        pin::pin,
        task::{Poll, Poll::Pending},
    };

    use crate::async_queue::AsyncQueue;

    #[tokio::test]
    async fn test_basic() {
        let queue = AsyncQueue::new();
        queue.release(1);
        assert_eq!(1, queue.acquire().await);
    }

    #[tokio::test]
    async fn test_multiple_reader() {
        let queue = AsyncQueue::new();
        let mut read_future1 = pin!(queue.acquire());
        let mut read_future2 = pin!(queue.acquire());
        assert_eq!(Pending, poll_fn(|cx| Poll::Ready(read_future1.as_mut().poll(cx))).await);
        assert_eq!(Pending, poll_fn(|cx| Poll::Ready(read_future2.as_mut().poll(cx))).await);
        queue.release(1);
        queue.release(2);
        assert_eq!(1, read_future1.await);
        assert_eq!(2, read_future2.await);
    }
}
