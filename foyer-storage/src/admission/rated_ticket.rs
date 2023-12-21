//  Copyright 2023 MrCroxx
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

use std::{
    fmt::Debug,
    marker::PhantomData,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
};

use foyer_common::{
    code::{Key, Value},
    rated_ticket::RatedTicket,
};

use super::{AdmissionContext, AdmissionPolicy};

#[derive(Debug)]
pub struct RatedTicketAdmissionPolicy<K, V>
where
    K: Key,
    V: Value,
{
    inner: RatedTicket,

    last: AtomicUsize,

    context: OnceLock<AdmissionContext<K, V>>,

    _marker: PhantomData<(K, V)>,
}

impl<K, V> RatedTicketAdmissionPolicy<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new(rate: usize) -> Self {
        Self {
            inner: RatedTicket::new(rate as f64),
            last: AtomicUsize::default(),
            context: OnceLock::new(),
            _marker: PhantomData,
        }
    }
}

impl<K, V> AdmissionPolicy for RatedTicketAdmissionPolicy<K, V>
where
    K: Key,
    V: Value,
{
    type Key = K;

    type Value = V;

    fn init(&self, context: AdmissionContext<Self::Key, Self::Value>) {
        self.context.set(context).unwrap();
    }

    fn judge(&self, _key: &Self::Key, _weight: usize) -> bool {
        let res = self.inner.probe();

        let statistics = self.context.get().unwrap().statistics.as_ref();
        let current = statistics.written_bytes.load(Ordering::Relaxed);
        let last = self.last.load(Ordering::Relaxed);
        let delta = current.saturating_sub(last);

        if delta > 0 {
            self.last.store(current, Ordering::Relaxed);
            self.inner.reduce(delta as f64);
        }

        res
    }

    fn on_insert(&self, _key: &Self::Key, _weight: usize, _judge: bool) {}

    fn on_drop(&self, _key: &Self::Key, _weight: usize, _judge: bool) {}
}
