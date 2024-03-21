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
//  limitations under the License.s

use std::{collections::HashSet, marker::PhantomData};

use foyer_common::code::{Key, Value};
use parking_lot::Mutex;

use crate::{admission::AdmissionPolicy, reinsertion::ReinsertionPolicy};

#[derive(Debug, Clone)]
pub enum Record<K: Key> {
    Admit(K),
    Evict(K),
}

#[derive(Debug)]
pub struct JudgeRecorder<K, V>
where
    K: Key,
    V: Value,
{
    records: Mutex<Vec<Record<K>>>,
    _marker: PhantomData<V>,
}

impl<K, V> JudgeRecorder<K, V>
where
    K: Key,
    V: Value,
{
    pub fn dump(&self) -> Vec<Record<K>> {
        self.records.lock().clone()
    }

    pub fn remains(&self) -> HashSet<K> {
        let records = self.dump();
        let mut res = HashSet::default();
        for record in records {
            match record {
                Record::Admit(key) => {
                    res.insert(key);
                }
                Record::Evict(key) => {
                    res.remove(&key);
                }
            }
        }
        res
    }
}

impl<K, V> Default for JudgeRecorder<K, V>
where
    K: Key,
    V: Value,
{
    fn default() -> Self {
        Self {
            records: Mutex::new(Vec::default()),
            _marker: PhantomData,
        }
    }
}

impl<K, V> AdmissionPolicy for JudgeRecorder<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;

    type Value = V;

    fn judge(&self, key: &K, _weight: usize) -> bool {
        self.records.lock().push(Record::Admit(key.clone()));
        true
    }

    fn on_insert(&self, _key: &K, _weight: usize, _judge: bool) {}

    fn on_drop(&self, _key: &K, _weight: usize, _judge: bool) {}
}

impl<K, V> ReinsertionPolicy for JudgeRecorder<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;

    type Value = V;

    fn judge(&self, key: &K, _weight: usize) -> bool {
        self.records.lock().push(Record::Evict(key.clone()));
        false
    }

    fn on_insert(&self, _key: &Self::Key, _weight: usize, _judge: bool) {}

    fn on_drop(&self, _key: &Self::Key, _weight: usize, _judge: bool) {}
}
