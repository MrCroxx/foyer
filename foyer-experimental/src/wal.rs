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
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Instant,
};

use bytes::{Buf, BufMut};
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::{
    asyncify, batch,
    buf::{BufExt, BufMutExt},
    error::Result,
    metrics::Metrics,
};

pub trait HashValue: Send + Sync + 'static + Eq + std::fmt::Debug {
    fn size() -> usize;
    fn write(&self, buf: impl BufMut);
    fn read(buf: impl Buf) -> Self;
}

macro_rules! for_all_primitives {
    ($macro:ident) => {
        $macro! {
            u8, u16, u32, u64, usize,
            i8, i16, i32, i64, isize,
        }
    };
}

macro_rules! impl_hash_value {
    ($( $type:ty, )*) => {
        paste::paste! {
            $(
                impl HashValue for $type {
                    fn size() -> usize {
                        std::mem::size_of::<$type>()
                    }

                    fn write(&self, mut buf: impl BufMut) {
                        buf.[<put_ $type>](*self);
                    }

                    fn read(mut buf: impl Buf) -> Self {
                        buf.[<get_ $type>]()
                    }
                }
            )*
        }
    };
}

for_all_primitives! { impl_hash_value }

#[derive(Debug)]
pub struct Tombstone<H: HashValue> {
    hash: H,
    pos: u32,
}

impl<H: HashValue> Tombstone<H> {
    pub fn size() -> usize {
        H::size() + 8
    }

    pub fn new(hash: H, pos: u32) -> Self {
        Self { hash, pos }
    }

    pub fn write(&self, mut buf: impl BufMut) {
        self.hash.write(&mut buf);
        buf.put_u32(self.pos);
    }

    pub fn read(mut buf: impl Buf) -> Self {
        let hash = H::read(&mut buf);
        let pos = buf.get_u32();
        Self { hash, pos }
    }
}

#[derive(Debug)]
pub struct TombstoneLogConfig {
    pub id: usize,
    pub dir: PathBuf,
    pub notifiers: usize,
    pub metrics: Arc<Metrics>,
}

#[derive(Debug)]
struct InflightTombstone<H: HashValue> {
    tombstone: Tombstone<H>,
    tx: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
pub struct TombstoneLog<H: HashValue> {
    tx: batch::Sender<InflightTombstone<H>>,

    metrics: Arc<Metrics>,

    stopped: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<JoinHandle<Result<()>>>>>,
}

impl<H: HashValue> Clone for TombstoneLog<H> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            metrics: self.metrics.clone(),
            stopped: self.stopped.clone(),
            handles: self.handles.clone(),
        }
    }
}

impl<H: HashValue> TombstoneLog<H> {
    pub async fn open(config: TombstoneLogConfig) -> Result<Self> {
        let mut path = config.dir;

        path.push(format!("tombstone-{:08X}", config.id));

        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path)?;

        let txs = (0..config.notifiers)
            .map(|_| batch::Sender::default())
            .collect_vec();

        let stopped = Arc::new(AtomicBool::new(false));
        let metrics = config.metrics.clone();

        let tx = batch::Sender::default();

        // Create and run the flusher.
        let handle = {
            let tx = tx.clone();
            let txs = txs.clone();
            let metrics = metrics.clone();
            let stopped = stopped.clone();

            std::thread::spawn(move || {
                let flusher = TombstoneLogFlusher {
                    file,
                    rx: tx.subscribe(),
                    txs: txs,
                    metrics,
                    stopped,
                };
                flusher.run()
            })
        };

        // Create and run the notifiers.
        let handles = txs.into_iter().map(|tx| {
            let metrics = metrics.clone();
            let stopped = stopped.clone();

            std::thread::spawn(move || {
                let notifier = TombstoneLogFlushNotifier {
                    rx: tx.subscribe(),
                    metrics,
                    stopped,
                };
                notifier.run()
            })
        });

        let handles = std::iter::once(handle).chain(handles).collect_vec();

        let handles = Arc::new(Mutex::new(handles));

        Ok(Self {
            tx,

            metrics,

            stopped,
            handles,
        })
    }

    pub async fn close(&self) -> Result<()> {
        let handles = std::mem::take(&mut *self.handles.lock());
        if !handles.is_empty() {
            self.stopped.store(true, Ordering::Release);
            self.tx.notify();
            for handle in handles {
                asyncify(move || handle.join().unwrap()).await?;
            }
        }
        Ok(())
    }

    pub async fn append(&self, tombstone: Tombstone<H>) -> Result<()> {
        let now = Instant::now();

        let (tx, rx) = oneshot::channel();
        self.tx.send(InflightTombstone { tombstone, tx });

        self.metrics
            .inner_op_duration_wal_submit
            .observe(now.elapsed().as_secs_f64());

        let res = rx.await.unwrap();

        self.metrics
            .inner_op_duration_wal_append
            .observe(now.elapsed().as_secs_f64());

        res
    }
}

#[derive(Debug)]
struct TombstoneLogFlusher<H: HashValue> {
    file: File,

    rx: batch::Receiver<InflightTombstone<H>>,

    txs: Vec<batch::Sender<FlushNotifyTask>>,

    metrics: Arc<Metrics>,

    stopped: Arc<AtomicBool>,
}

impl<H: HashValue> TombstoneLogFlusher<H> {
    fn run(mut self) -> Result<()> {
        let mut index = 0;

        loop {
            let inflights = { self.rx.recv() };

            let timer = self.metrics.inner_op_duration_wal_flush.start_timer();

            let mut buffer = Vec::with_capacity(Tombstone::<H>::size() * inflights.len());
            let mut txs = Vec::with_capacity(inflights.len());

            for inflight in inflights {
                inflight.tombstone.write(&mut buffer);
                txs.push(inflight.tx);
            }

            let timer_write = self.metrics.inner_op_duration_wal_write.start_timer();
            match self.file.write_all(&buffer) {
                Ok(()) => {}
                Err(e) => {
                    self.txs[index % self.txs.len()].send(FlushNotifyTask {
                        txs,
                        io_result: Err(e),
                    });
                    index += 1;
                    continue;
                }
            }
            drop(timer_write);

            let timer_sync = self.metrics.inner_op_duration_wal_sync.start_timer();
            match self.file.sync_data() {
                Ok(()) => {}
                Err(e) => {
                    self.txs[index % self.txs.len()].send(FlushNotifyTask {
                        txs,
                        io_result: Err(e),
                    });
                    index += 1;
                    continue;
                }
            }
            drop(timer_sync);

            self.txs[index % self.txs.len()].send(FlushNotifyTask {
                txs,
                io_result: Ok(()),
            });
            index += 1;

            drop(timer);

            if self.stopped.load(Ordering::Relaxed) {
                self.txs.iter().for_each(|tx| tx.notify());
                return Ok(());
            }
        }
    }
}

#[derive(Debug)]
struct FlushNotifyTask {
    txs: Vec<oneshot::Sender<Result<()>>>,
    io_result: std::io::Result<()>,
}

#[derive(Debug)]
struct TombstoneLogFlushNotifier {
    rx: batch::Receiver<FlushNotifyTask>,

    metrics: Arc<Metrics>,

    stopped: Arc<AtomicBool>,
}

impl TombstoneLogFlushNotifier {
    fn run(self) -> Result<()> {
        loop {
            let timer = self.metrics.inner_op_duration_wal_notify.start_timer();

            let tasks = self.rx.recv();
            let mut count = 0;

            for task in tasks {
                count += task.txs.len();
                for tx in task.txs {
                    let res = match &task.io_result {
                        Ok(()) => Ok(()),
                        Err(e) => Err(std::io::Error::from(e.kind()).into()),
                    };
                    tx.send(res).unwrap();
                }
            }

            self.metrics
                .inner_op_objects_distribution_wal_notify
                .observe(count as f64);

            drop(timer);

            if self.stopped.load(Ordering::Relaxed) {
                return Ok(());
            }
        }
    }
}
