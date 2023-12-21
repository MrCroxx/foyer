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

use std::{fmt::Debug, sync::Arc};

use foyer_common::code::{Key, Value};

use crate::{catalog::Catalog, statistics::Statistics};

#[derive(Debug)]
pub struct AdmissionContext<K, V>
where
    K: Key,
    V: Value,
{
    pub catalog: Arc<Catalog<K, V>>,
    pub statistics: Arc<Statistics>,
}

impl<K, V> Clone for AdmissionContext<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            statistics: self.statistics.clone(),
        }
    }
}

#[expect(unused_variables)]
pub trait AdmissionPolicy: Send + Sync + 'static + Debug {
    type Key: Key;
    type Value: Value;

    fn init(&self, context: AdmissionContext<Self::Key, Self::Value>) {}

    fn judge(&self, key: &Self::Key, weight: usize) -> bool;

    fn on_insert(&self, key: &Self::Key, weight: usize, judge: bool);

    fn on_drop(&self, key: &Self::Key, weight: usize, judge: bool);
}

pub mod rated_ticket;
