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

use std::sync::Arc;

use parking_lot::{lock_api::ArcRwLockWriteGuard, RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait ErwLockInner {
    type R;
    fn is_exclusive(&self, require: &Self::R) -> bool;
}

#[derive(Debug)]
pub struct ErwLock<T: ErwLockInner> {
    inner: Arc<RwLock<T>>,
}

impl<T: ErwLockInner> Clone for ErwLock<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ErwLockInner> ErwLock<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.inner.write()
    }

    pub async fn exclusive(&self, require: &T::R) -> ArcRwLockWriteGuard<RawRwLock, T> {
        loop {
            {
                let guard = self.inner.clone().write_arc();
                if guard.is_exclusive(require) {
                    return guard;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
