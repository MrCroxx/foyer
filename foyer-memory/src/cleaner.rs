//  Copyright 2024 Foyer Project Authors
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

use foyer_common::{
    code::{Key, Value},
    event::EventListener,
};

/// [`EventListener`] implementation for [`Cleaner`].
///
/// Send all entries to the cleaner via channel.
pub struct CleanerEventListener<K, V> {
    tx: flume::Sender<(K, V)>,
}

impl<K, V> CleanerEventListener<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new(threads: usize, nested: Option<Arc<dyn EventListener<Key = K, Value = V>>>) -> Self {
        let (tx, rx) = flume::unbounded();

        for _ in 0..threads {
            let cleaner = Cleaner {
                rx: rx.clone(),
                nested: nested.clone(),
            };
            std::thread::Builder::new()
                .name("foyer-cleaner".to_string())
                .spawn(move || cleaner.run())
                .unwrap();
        }

        Self { tx }
    }
}

impl<K, V> EventListener for CleanerEventListener<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;
    type Value = V;

    fn on_memory_release(&self, key: Self::Key, value: Self::Value) {
        let _ = self.tx.send((key, value));
    }
}

/// [`Cleaner`] receives entries and drop them in its own thread.
struct Cleaner<K, V> {
    rx: flume::Receiver<(K, V)>,
    nested: Option<Arc<dyn EventListener<Key = K, Value = V>>>,
}

impl<K, V> Cleaner<K, V>
where
    K: Key,
    V: Value,
{
    /// Run the cleaner.
    fn run(self) {
        while let Ok(kv) = self.rx.recv() {
            let kvs = self.rx.drain();
            if let Some(l) = self.nested.as_ref() {
                std::iter::once(kv)
                    .chain(kvs)
                    .for_each(|(k, v)| l.on_memory_release(k, v))
            }
        }
    }
}
