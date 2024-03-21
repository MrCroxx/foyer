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
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
};

use bytes::{Buf, BufMut};
use crossbeam::channel;
use parking_lot::{Condvar, Mutex};
use tokio::sync::oneshot;

use crate::{
    asyncify,
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
    pub metrics: Arc<Metrics>,
}

#[derive(Debug)]
struct InflightTombstone<H: HashValue> {
    tombstone: Tombstone<H>,
    tx: oneshot::Sender<Result<()>>,
}

#[derive(Debug)]
struct TombstoneLogInner<H: HashValue> {
    inflights: Mutex<Vec<InflightTombstone<H>>>,
    condvar: Condvar,
}

#[derive(Debug)]
pub struct TombstoneLog<H: HashValue> {
    inner: Arc<TombstoneLogInner<H>>,

    metrics: Arc<Metrics>,

    stopped: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<JoinHandle<Result<()>>>>>,
}

impl<H: HashValue> Clone for TombstoneLog<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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

        let file = OpenOptions::new().write(true).read(true).create(true).open(path)?;

        let inner = Arc::new(TombstoneLogInner {
            inflights: Mutex::new(vec![]),
            condvar: Condvar::new(),
        });

        let (task_tx, task_rx) = channel::unbounded();
        let stopped = Arc::new(AtomicBool::new(false));
        let metrics = config.metrics.clone();

        let flusher = TombstoneLogFlusher {
            file,
            inner: inner.clone(),
            task_tx,
            metrics: metrics.clone(),
            stopped: stopped.clone(),
        };

        let notifier = TombstoneLogFlushNotifier {
            task_rx,
            metrics: metrics.clone(),
        };

        let handles = vec![
            std::thread::spawn(move || flusher.run()),
            std::thread::spawn(move || notifier.run()),
        ];
        let handles = Arc::new(Mutex::new(handles));

        Ok(Self {
            inner,

            metrics,

            stopped,
            handles,
        })
    }

    pub async fn close(&self) -> Result<()> {
        let handles = std::mem::take(&mut *self.handles.lock());
        if !handles.is_empty() {
            self.stopped.store(true, Ordering::Release);
            self.inner.condvar.notify_one();
            for handle in handles {
                asyncify(move || handle.join().unwrap()).await?;
            }
        }
        Ok(())
    }

    pub async fn append(&self, tombstone: Tombstone<H>) -> Result<()> {
        let timer = self.metrics.inner_op_duration_wal_append.start_timer();

        let rx = {
            let mut inflights = self.inner.inflights.lock();
            let (tx, rx) = oneshot::channel();
            inflights.push(InflightTombstone { tombstone, tx });
            rx
        };
        self.inner.condvar.notify_one();
        let res = rx.await.unwrap();

        drop(timer);

        res
    }
}

#[derive(Debug)]
struct TombstoneLogFlusher<H: HashValue> {
    file: File,

    inner: Arc<TombstoneLogInner<H>>,

    task_tx: channel::Sender<FlushNotifyTask>,

    metrics: Arc<Metrics>,

    stopped: Arc<AtomicBool>,
}

impl<H: HashValue> TombstoneLogFlusher<H> {
    fn run(mut self) -> Result<()> {
        loop {
            let inflights = {
                let mut inflights = self.inner.inflights.lock();
                if self.stopped.load(Ordering::Relaxed) {
                    return Ok(());
                }
                if inflights.is_empty() {
                    self.inner.condvar.wait_while(&mut inflights, |inflights| {
                        inflights.is_empty() && !self.stopped.load(Ordering::Relaxed)
                    });
                }
                if self.stopped.load(Ordering::Relaxed) {
                    return Ok(());
                }
                std::mem::take(&mut *inflights)
            };

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
                    self.task_tx.send(FlushNotifyTask { txs, io_result: Err(e) }).unwrap();
                    continue;
                }
            }
            drop(timer_write);

            let timer_sync = self.metrics.inner_op_duration_wal_sync.start_timer();
            match self.file.sync_data() {
                Ok(()) => {}
                Err(e) => {
                    self.task_tx.send(FlushNotifyTask { txs, io_result: Err(e) }).unwrap();
                    continue;
                }
            }
            drop(timer_sync);

            self.task_tx.send(FlushNotifyTask { txs, io_result: Ok(()) }).unwrap();

            drop(timer);
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
    task_rx: channel::Receiver<FlushNotifyTask>,

    metrics: Arc<Metrics>,
}

impl TombstoneLogFlushNotifier {
    fn run(self) -> Result<()> {
        while let Ok(task) = self.task_rx.recv() {
            let timer = self.metrics.inner_op_duration_wal_notify.start_timer();
            for tx in task.txs {
                let res = match &task.io_result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(std::io::Error::from(e.kind()).into()),
                };
                tx.send(res).unwrap();
            }
            drop(timer);
        }
        Ok(())
    }
}
