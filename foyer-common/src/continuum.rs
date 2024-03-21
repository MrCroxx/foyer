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

use std::{
    ops::Range,
    sync::atomic::{AtomicU16, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering},
};

use itertools::Itertools;

macro_rules! def_continuum {
    ($( { $name:ident, $uint:ty, $atomic:ty }, )*) => {
        $(
            #[derive(Debug)]
            pub struct $name {
                slots: Vec<$atomic>,
                capacity: $uint,
                continuum: $atomic,
            }

            impl $name {
                pub fn new(capacity: $uint) -> Self {
                    let slots = (0..capacity).map(|_| <$atomic>::default()).collect_vec();
                    let continuum = <$atomic>::default();
                    Self {
                        slots,
                        capacity,
                        continuum,
                    }
                }

                pub fn is_occupied(&self, start: $uint) -> bool {
                    !self.is_vacant(start)
                }

                pub fn is_vacant(&self, start: $uint) -> bool {
                    let continuum = self.continuum.load(Ordering::Acquire);
                    if continuum + self.capacity > start {
                        return true;
                    }

                    self.advance_until(|_, _| false, 0);

                    let continuum = self.continuum.load(Ordering::Acquire);
                    continuum + self.capacity > start
                }

                /// Submit a range.
                pub fn submit(&self, range: Range<$uint>) {
                    debug_assert!(range.start < range.end);

                    self.slots[self.slot(range.start)].store(range.end, Ordering::SeqCst);
                }

                /// Submit a range, may advance continuum till the given range.
                ///
                /// Return `true` if advanced, else `false`.
                pub fn submit_advance(&self, range: Range<$uint>) -> bool {
                    debug_assert!(range.start < range.end);

                    let continuum = self.continuum.load(Ordering::Acquire);

                    debug_assert!(continuum <= range.start, "assert continuum <= range.start failed: {} <= {}", continuum, range.start);

                    if continuum == range.start {
                        // continuum can be advanced directly and exclusively
                        self.continuum.store(range.end, Ordering::Release);
                        true
                    } else {
                        let slot = &self.slots[self.slot(range.start)];
                        slot.store(range.end, Ordering::Release);
                        let stop = move |current: $uint, _next: $uint| {
                            current > range.start
                        };
                        self.advance_until(stop, 1)
                    }
                }

                pub fn advance(&self) -> bool {
                    self.advance_until(|_, _| false, 0)
                }

                pub fn continuum(&self) -> $uint {
                    self.continuum.load(Ordering::Acquire)
                }

                fn slot(&self, position: $uint) -> usize {
                    (position % self.capacity) as usize
                }

                /// `stop: Fn(continuum, next) -> bool`.
                fn advance_until<P>(&self, stop: P, retry: usize) -> bool
                where
                    P: Fn($uint, $uint) -> bool + Send + Sync + 'static,
                {
                    let mut continuum = self.continuum.load(Ordering::Acquire);
                    let mut start = continuum;

                    let mut times = 0;
                    loop {
                        let slot = &self.slots[self.slot(continuum)];

                        let next = slot.load(Ordering::Acquire);

                        if next >= continuum + self.capacity {
                            // case 1: `range` >= `capacity`
                            // case 2: continuum has rotated before `next` is loaded
                            continuum = self.continuum.load(Ordering::Acquire);
                            if continuum != start {
                                start = continuum;
                                continue;
                            }
                        }

                        if next <= continuum || stop(continuum, next) {
                            // nothing to advance
                            return false;
                        }

                        // make sure `continuum` can be modified exclusively and lock
                        if let Ok(_) = slot.compare_exchange(next, continuum, Ordering::AcqRel, Ordering::Relaxed) {
                            // If this thread is scheduled for a long time after `continuum` is loaded,
                            // the `slot` may refer to a rotated slot with actual index `continuum + N * capacity`,
                            // and the loaded `continuum` lags. `continuum` needs to be checked if it is still behind
                            // the slot.
                            continuum = self.continuum.load(Ordering::Acquire);
                            if continuum == start {
                                // exclusive
                                continuum = next;
                                break;
                            }
                        }

                        // prepare for the next retry
                        times += 1;
                        if times > retry {
                            return false;
                        }

                        continuum = self.continuum.load(Ordering::Acquire);
                        if continuum == start {
                            return false;
                        }
                        start = continuum;
                    }

                    loop {
                        let next = self.slots[self.slot(continuum)].load(Ordering::Relaxed);

                        if next <= continuum || stop(continuum, next) {
                            break;
                        }

                        continuum = next;
                    }

                    debug_assert_eq!(start, self.continuum.load(Ordering::Acquire));

                    // modify continuum exclusively and unlock
                    self.continuum.store(continuum, Ordering::Release);

                    if continuum == start {
                        return false;
                    }
                    return true;
                }
            }
        )*
    }
}

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro! {
            { ContinuumU8, u8, AtomicU8 },
            { ContinuumU16, u16, AtomicU16 },
            { ContinuumU32, u32, AtomicU32 },
            { ContinuumU64, u64, AtomicU64 },
            { ContinuumUsize, usize, AtomicUsize },
        }
    };
}

for_all_primitives! { def_continuum }

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rand::{rngs::OsRng, Rng};
    use tokio::sync::Semaphore;

    use super::*;

    #[ignore]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_continuum_fuzzy() {
        const CAPACITY: u64 = 4096;
        const CURRENCY: usize = 16;
        const UNIT: u64 = 16;
        const LOOP: usize = 1000;

        let s = Arc::new(Semaphore::new(CAPACITY as usize));
        let c = Arc::new(ContinuumU64::new(CAPACITY));
        let v = Arc::new(AtomicU64::new(0));

        let tasks = (0..CURRENCY)
            .map(|_| {
                let s = s.clone();
                let c = c.clone();
                let v = v.clone();
                async move {
                    for _ in 0..LOOP {
                        let unit = OsRng.gen_range(1..UNIT);
                        let start = v.fetch_add(unit, Ordering::Relaxed);
                        let end = start + unit;

                        let permit = s.acquire_many(unit as u32).await.unwrap();

                        let sleep = OsRng.gen_range(0..10);
                        tokio::time::sleep(Duration::from_millis(sleep)).await;
                        c.submit(start..end);
                        c.advance();

                        drop(permit);
                    }
                }
            })
            .collect_vec();

        let handles = tasks.into_iter().map(tokio::spawn).collect_vec();
        for handle in handles {
            handle.await.unwrap();
        }

        c.advance();

        assert_eq!(v.load(Ordering::Relaxed), c.continuum());
    }
}
