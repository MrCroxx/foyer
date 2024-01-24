//  Copyright 2024 MrCroxx
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

use std::sync::Arc;

use crossbeam::queue::SegQueue;
use parking_lot::RwLock;

use crate::notify::{Notified, Notify};

#[derive(Debug)]
pub struct Sender<T> {
    batch: Arc<RwLock<SegQueue<T>>>,
    notify: Arc<Notify>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            batch: self.batch.clone(),
            notify: self.notify.clone(),
        }
    }
}

impl<T> Default for Sender<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Sender<T> {
    pub fn new() -> Self {
        Self {
            batch: Arc::new(RwLock::new(SegQueue::default())),
            notify: Arc::new(Notify::default()),
        }
    }

    /// Note: [`Receiver`] must be created in the target thread for only once.
    pub fn subscribe(&self) -> Receiver<T> {
        Receiver::new(self)
    }

    pub fn send(&self, msg: T) {
        self.batch.read().push(msg);
        self.notify.notify();
    }

    pub fn notify(&self) {
        self.notify.notify();
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    batch: Arc<RwLock<SegQueue<T>>>,
    notified: Notified,
}

impl<T> Receiver<T> {
    fn new(sender: &Sender<T>) -> Self {
        Self {
            batch: sender.batch.clone(),
            notified: sender.notify.notified(),
        }
    }

    pub fn recv(&self) -> SegQueue<T> {
        self.notified.wait();
        std::mem::take(&mut *self.batch.write())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_batch() {
        const N: usize = 10000;

        let tx = Sender::default();

        let t = tx.clone();
        let handle = std::thread::spawn(move || {
            let rx = t.subscribe();

            loop {
                let batch = rx.recv();
                for i in batch {
                    if i == N - 1 {
                        return;
                    }
                }
            }
        });

        std::thread::sleep(Duration::from_millis(100));
        for i in 0..N {
            tx.send(i);
        }
        handle.join().unwrap();
    }
}
