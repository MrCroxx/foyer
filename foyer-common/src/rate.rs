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

use std::time::{Duration, Instant};

use parking_lot::Mutex;

#[derive(Debug)]
pub struct RateLimiter {
    inner: Mutex<Inner>,
    rate: f64,
}

#[derive(Debug)]
struct Inner {
    quota: f64,

    last: Instant,
}

impl RateLimiter {
    pub fn new(rate: f64) -> Self {
        let inner = Inner {
            quota: rate,
            last: Instant::now(),
        };
        Self {
            rate,
            inner: Mutex::new(inner),
        }
    }

    pub fn consume(&self, weight: f64) -> Option<Duration> {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        let refill = now.duration_since(inner.last).as_secs_f64() * self.rate;
        inner.last = now;
        inner.quota = f64::min(inner.quota + refill, self.rate);
        inner.quota -= weight;
        if inner.quota >= 0.0 {
            return None;
        }
        let wait = Duration::from_secs_f64((-inner.quota) / self.rate);
        Some(wait)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use rand::{thread_rng, Rng};

    use super::*;

    const ERATIO: f64 = 0.05;
    const THREADS: usize = 8;
    const RATE: usize = 1000;
    const DURATION: Duration = Duration::from_secs(10);

    #[ignore]
    #[test]
    fn test_rate_limiter() {
        let v = Arc::new(AtomicUsize::new(0));
        let limiter = Arc::new(RateLimiter::new(RATE as f64));
        let task = |rate: usize, v: Arc<AtomicUsize>, limiter: Arc<RateLimiter>| {
            let start = Instant::now();
            loop {
                if start.elapsed() >= DURATION {
                    break;
                }
                if let Some(dur) = limiter.consume(rate as f64) {
                    std::thread::sleep(dur);
                }
                v.fetch_add(rate, Ordering::Relaxed);
            }
        };
        let mut handles = vec![];
        let mut rng = thread_rng();
        for _ in 0..THREADS {
            let rate = rng.gen_range(10..20);
            let handle = std::thread::spawn({
                let v = v.clone();
                let limiter = limiter.clone();
                move || task(rate, v, limiter)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let error = (v.load(Ordering::Relaxed) as isize
            - RATE as isize * DURATION.as_secs() as isize)
            .unsigned_abs();
        let eratio = error as f64 / (RATE as f64 * DURATION.as_secs_f64());
        assert!(eratio < ERATIO, "eratio: {}, target: {}", eratio, ERATIO);
    }
}
