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

use std::time::Instant;

use parking_lot::Mutex;

#[derive(Debug)]
pub struct RatedTicket {
    inner: Mutex<Inner>,
    rate: f64,
}

#[derive(Debug)]
struct Inner {
    quota: f64,

    last: Instant,
}

impl RatedTicket {
    pub fn new(rate: f64) -> Self {
        let inner = Inner {
            quota: 0.0,
            last: Instant::now(),
        };
        Self {
            rate,
            inner: Mutex::new(inner),
        }
    }

    pub fn probe(&self) -> bool {
        let mut inner = self.inner.lock();

        let now = Instant::now();
        let refill = now.duration_since(inner.last).as_secs_f64() * self.rate;
        inner.last = now;
        inner.quota = f64::min(inner.quota + refill, self.rate);

        inner.quota > 0.0
    }

    pub fn reduce(&self, weight: f64) {
        self.inner.lock().quota -= weight;
    }

    pub fn consume(&self, weight: f64) -> bool {
        let mut inner = self.inner.lock();

        let now = Instant::now();
        let refill = now.duration_since(inner.last).as_secs_f64() * self.rate;
        inner.last = now;
        inner.quota = f64::min(inner.quota + refill, self.rate);

        if inner.quota <= 0.0 {
            return false;
        }

        inner.quota -= weight;

        true
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use itertools::Itertools;
    use rand::{thread_rng, Rng};

    use super::*;

    #[ignore]
    #[test]
    fn test_rated_ticket_consume() {
        test(consume)
    }

    #[ignore]
    #[test]
    fn test_rated_ticket_probe_reduce() {
        test(probe_reduce)
    }

    fn test<F>(f: F)
    where
        F: Fn(usize, &Arc<AtomicUsize>, &Arc<RatedTicket>) + Send + Sync + Copy + 'static,
    {
        const CASES: usize = 10;
        const ERATIO: f64 = 0.05;

        let handles = (0..CASES).map(|_| std::thread::spawn(move || case(f))).collect_vec();
        let mut eratios = vec![];
        for handle in handles {
            let eratio = handle.join().unwrap();
            assert!(eratio < ERATIO, "eratio: {} < ERATIO: {}", eratio, ERATIO);
            eratios.push(eratio);
        }
        println!("========== RatedTicket error ratio begin ==========");
        for eratio in eratios {
            println!("eratio: {eratio}");
        }
        println!("=========== RatedTicket error ratio end ===========");
    }

    fn consume(weight: usize, v: &Arc<AtomicUsize>, limiter: &Arc<RatedTicket>) {
        if limiter.consume(weight as f64) {
            v.fetch_add(weight, Ordering::Relaxed);
        }
    }

    fn probe_reduce(weight: usize, v: &Arc<AtomicUsize>, limiter: &Arc<RatedTicket>) {
        if limiter.probe() {
            limiter.reduce(weight as f64);
            v.fetch_add(weight, Ordering::Relaxed);
        }
    }

    fn case<F>(f: F) -> f64
    where
        F: Fn(usize, &Arc<AtomicUsize>, &Arc<RatedTicket>) + Send + Sync + Copy + 'static,
    {
        const THREADS: usize = 8;
        const RATE: usize = 1000;
        const DURATION: Duration = Duration::from_secs(10);

        let v = Arc::new(AtomicUsize::new(0));
        let limiter = Arc::new(RatedTicket::new(RATE as f64));
        let task = |rate: usize, v: Arc<AtomicUsize>, limiter: Arc<RatedTicket>, f: F| {
            let start = Instant::now();
            let mut rng = thread_rng();
            loop {
                if start.elapsed() >= DURATION {
                    break;
                }
                std::thread::sleep(Duration::from_millis(rng.gen_range(1..100)));
                f(rate, &v, &limiter)
            }
        };
        let mut handles = vec![];
        let mut rng = thread_rng();
        for _ in 0..THREADS {
            let rate = rng.gen_range(10..20);
            let handle = std::thread::spawn({
                let v = v.clone();
                let limiter = limiter.clone();
                move || task(rate, v, limiter, f)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let error = (v.load(Ordering::Relaxed) as isize - RATE as isize * DURATION.as_secs() as isize).unsigned_abs();
        error as f64 / (RATE as f64 * DURATION.as_secs_f64())
    }
}
