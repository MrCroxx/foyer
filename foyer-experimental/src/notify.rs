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

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
    thread::{current, park, Thread},
};

use crossbeam::utils::Backoff;

/// A multi-producer-single-consumer blocking notify implementation.
#[derive(Debug, Default, Clone)]
pub struct Notify {
    ready: Arc<AtomicBool>,
    thread: Arc<OnceLock<Thread>>,
}

impl Notify {
    /// Note: `notified` must be called in the target thread.
    pub fn notified(&self) -> Notified {
        self.thread
            .set(current())
            .expect("`notified` can only be called once on the same `Notify`.");
        Notified::new(self.ready.clone())
    }

    pub fn notify(&self) {
        self.ready.store(true, Ordering::SeqCst);
        self.thread
            .get()
            .expect("`notified` must be called before `notify`")
            .unpark();
    }
}

#[derive(Debug)]
pub struct Notified {
    ready: Arc<AtomicBool>,
    backoff: Backoff,
}

impl Notified {
    fn new(ready: Arc<AtomicBool>) -> Self {
        Self {
            ready,
            backoff: Backoff::default(),
        }
    }

    pub fn wait(&self) {
        while !self.ready.load(Ordering::SeqCst) {
            if self.backoff.is_completed() {
                park();
            } else {
                self.backoff.snooze();
            }
        }
        self.ready.store(false, Ordering::SeqCst);
        self.backoff.reset();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        thread::{sleep, spawn},
        time::Duration,
    };

    use super::*;

    #[test]
    fn assert_notify_send_sync_static() {
        fn is_send_sync_static<T: Send + Sync + 'static>() {}
        is_send_sync_static::<Notify>()
    }

    #[test]
    fn test_notify_buffer_one() {
        let notify = Notify::default();

        let notified = notify.notified();

        notify.notify();

        notified.wait();
    }

    #[test]
    fn test_notify_buffer_one_block() {
        let handle = spawn(|| {
            let notify = Notify::default();

            let notified = notify.notified();

            notify.notify();

            notified.wait();
            notified.wait();
        });

        sleep(Duration::from_secs(1));

        assert!(!handle.is_finished());
    }

    #[test]
    fn test_notify() {
        let notify = Arc::new(Notify::default());
        let n = notify.clone();
        let handle = spawn(move || {
            let notified = n.notified();

            notified.wait();
        });
        sleep(Duration::from_secs(1));
        notify.notify();
        handle.join().unwrap();
    }
}
