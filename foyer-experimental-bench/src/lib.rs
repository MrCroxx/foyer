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

pub mod analyze;
pub mod export;
pub mod rate;
pub mod text;
pub mod utils;

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use analyze::{analyze, Metrics, MetricsDump};
use tokio::task::JoinHandle;
use utils::{iostat, IoStat};

#[cfg(feature = "tokio-console")]
pub fn init_logger() {
    console_subscriber::init();
}

#[cfg(feature = "trace")]
pub fn init_logger() {
    use opentelemetry_sdk::{
        trace::{BatchConfigBuilder, Config},
        Resource,
    };
    use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
    use tracing::Level;
    use tracing_subscriber::{filter::Targets, prelude::*};

    let trace_config = Config::default().with_resource(Resource::new(vec![opentelemetry::KeyValue::new(
        SERVICE_NAME,
        "foyer-storage-bench",
    )]));
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(1048576)
        .with_max_export_batch_size(4096)
        .with_max_concurrent_exports(4)
        .build();

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .with_trace_config(trace_config)
        .with_batch_config(batch_config)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .unwrap();
    let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(
            Targets::new()
                .with_target("foyer_storage", Level::DEBUG)
                .with_target("foyer_common", Level::DEBUG)
                .with_target("foyer_intrusive", Level::DEBUG)
                .with_target("foyer_storage_bench", Level::DEBUG),
        )
        .with(opentelemetry_layer)
        .init();
}

#[cfg(not(any(feature = "tokio-console", feature = "trace")))]
pub fn init_logger() {
    use tracing_subscriber::{prelude::*, EnvFilter};

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                // .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_line_number(true),
        )
        .with(EnvFilter::from_default_env())
        .init();
}

#[derive(Debug, Clone)]
pub struct IoMonitorConfig {
    pub dir: PathBuf,
    pub interval: Duration,
    pub total_secs: usize,
}

#[cfg(not(target_os = "linux"))]
pub fn io_monitor(_: IoMonitorConfig) -> IoMonitorGuard {
    panic!("bench tool io monitor only supports linux")
}

#[cfg(target_os = "linux")]
pub fn io_monitor(config: IoMonitorConfig) -> IoMonitorGuard {
    use utils::{detect_fs_type, io_stat_path, FsType};

    let fs_type = detect_fs_type(&config.dir);
    let io_stat_path = match fs_type {
        FsType::Xfs | FsType::Ext4 => io_stat_path(&config.dir),
        _ => panic!("bench tool io monitor only supports ext4 and xfs"),
    };

    let metrics = Metrics::default();

    let start_io_stat = iostat(&io_stat_path);
    let start_metrics_dump = metrics.dump();
    let start = Instant::now();

    let handle = {
        let metrics = metrics.clone();
        let config = config.clone();
        let io_stat_path = io_stat_path.clone();
        tokio::spawn(async move {
            let mut last_io_stat = start_io_stat;
            let mut last_metrics_dump = start_metrics_dump;
            loop {
                let now = Instant::now();
                tokio::time::sleep(config.interval).await;
                let new_io_stat = iostat(&io_stat_path);
                let new_metrics_dump = metrics.dump();
                let analysis = analyze(
                    now.elapsed(),
                    &last_io_stat,
                    &new_io_stat,
                    &last_metrics_dump,
                    &new_metrics_dump,
                );
                println!("Report [{}s/{}s]", start.elapsed().as_secs(), config.total_secs);
                println!("{}", analysis);
                last_io_stat = new_io_stat;
                last_metrics_dump = new_metrics_dump;
            }
        })
    };

    IoMonitorGuard {
        config,
        io_stat_path,

        metrics,

        start,
        start_io_stat,
        start_metrics_dump,

        handle,
    }
}

#[allow(dead_code)]
#[must_use]
pub struct IoMonitorGuard {
    config: IoMonitorConfig,
    io_stat_path: PathBuf,

    metrics: Metrics,

    start: Instant,
    start_io_stat: IoStat,
    start_metrics_dump: MetricsDump,

    handle: JoinHandle<()>,
}

impl Drop for IoMonitorGuard {
    fn drop(&mut self) {
        self.handle.abort();
        let end_io_stat = iostat(&self.io_stat_path);
        let end_metrics_dump = self.metrics.dump();
        let analysis = analyze(
            self.start.elapsed(),
            &self.start_io_stat,
            &end_io_stat,
            &self.start_metrics_dump,
            &end_metrics_dump,
        );
        let elapsed = self.start.elapsed();
        println!("Summary [{}s]", elapsed.as_secs());
        println!("{}", analysis);
    }
}
