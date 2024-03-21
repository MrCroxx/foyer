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
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use foyer_common::runtime::BackgroundShutdownRuntime;
use foyer_experimental::{
    metrics::METRICS,
    wal::{Tombstone, TombstoneLog, TombstoneLogConfig},
};
use foyer_experimental_bench::{export::MetricsExporter, init_logger, io_monitor, IoMonitorConfig};
use itertools::Itertools;
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
pub struct Args {
    /// dir for cache data
    #[arg(short, long)]
    dir: String,

    /// writer concurrency
    #[arg(short, long, default_value_t = 1024)]
    concurrency: usize,

    /// time (s)
    #[arg(short, long, default_value_t = 60)]
    time: usize,

    /// (s)
    #[arg(long, default_value_t = 2)]
    report_interval: u64,

    #[arg(long, default_value_t = false)]
    metrics: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("{:#?}", args);

    init_logger();

    if args.metrics {
        MetricsExporter::init("0.0.0.0:19970".parse().unwrap());
    }

    let guard = io_monitor(IoMonitorConfig {
        dir: PathBuf::from(&args.dir),
        interval: Duration::from_secs(args.report_interval),
        total_secs: args.time,
    });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let rt = BackgroundShutdownRuntime::from(rt);
    let rt = Arc::new(rt);

    let config = TombstoneLogConfig {
        id: 0,
        dir: args.dir.clone().into(),
        metrics: Arc::new(METRICS.metrics("wal-bench")),
    };
    let log = TombstoneLog::open(config).await.unwrap();

    let handles = (0..args.concurrency)
        .map(|_| {
            let log = log.clone();
            let args = args.clone();
            let rt = rt.clone();
            tokio::spawn(async move {
                write(log.clone(), args, rt).await;
            })
        })
        .collect_vec();

    for handle in handles {
        handle.await.unwrap();
    }

    drop(guard);

    log.close().await.unwrap();
}

async fn write(log: TombstoneLog<u64>, args: Args, rt: Arc<BackgroundShutdownRuntime>) {
    let start = Instant::now();
    let mut log = log;

    let mut rng = StdRng::seed_from_u64(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as _);
    loop {
        if start.elapsed() >= Duration::from_secs(args.time as _) {
            return;
        }

        let tombstone = Tombstone::new(rng.gen(), rng.gen());
        log = rt
            .spawn(async move {
                log.append(tombstone).await.unwrap();
                log
            })
            .await
            .unwrap();
    }
}
