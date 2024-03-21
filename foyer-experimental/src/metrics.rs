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

use std::sync::{LazyLock, OnceLock};

use prometheus::{
    core::{AtomicU64, GenericGauge, GenericGaugeVec},
    exponential_buckets, opts, register_histogram_vec_with_registry, register_int_counter_vec_with_registry, Histogram,
    HistogramVec, IntCounter, IntCounterVec, Registry,
};

type UintGaugeVec = GenericGaugeVec<AtomicU64>;
type UintGauge = GenericGauge<AtomicU64>;

macro_rules! register_gauge_vec {
    ($TYPE:ident, $OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        let gauge_vec = $TYPE::new($OPTS, $LABELS_NAMES).unwrap();
        $REGISTRY.register(Box::new(gauge_vec.clone())).map(|_| gauge_vec)
    }};
}

macro_rules! register_uint_gauge_vec_with_registry {
    ($OPTS:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_gauge_vec!(UintGaugeVec, $OPTS, $LABELS_NAMES, $REGISTRY)
    }};

    ($NAME:expr, $HELP:expr, $LABELS_NAMES:expr, $REGISTRY:expr $(,)?) => {{
        register_uint_gauge_vec_with_registry!(opts!($NAME, $HELP), $LABELS_NAMES, $REGISTRY)
    }};
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// Set metrics registry for `foyer`.
///
/// Metrics registry must be set before `open`.
///
/// Return `true` if set succeeds.
pub fn set_metrics_registry(registry: Registry) -> bool {
    REGISTRY.set(registry).is_ok()
}

pub fn get_metrics_registry() -> &'static Registry {
    REGISTRY.get_or_init(|| prometheus::default_registry().clone())
}

pub static METRICS: LazyLock<GlobalMetrics> = LazyLock::new(GlobalMetrics::default);

#[derive(Debug)]
pub struct GlobalMetrics {
    pub op_duration: HistogramVec,
    pub op_bytes: IntCounterVec,

    pub total_bytes: UintGaugeVec,

    pub inner_op_duration: HistogramVec,
    pub inner_op_bytes: IntCounterVec,
    pub inner_op_bytes_distribution: HistogramVec,
}

impl Default for GlobalMetrics {
    fn default() -> Self {
        Self::new(get_metrics_registry())
    }
}

impl GlobalMetrics {
    pub fn new(registry: &Registry) -> Self {
        let op_duration = register_histogram_vec_with_registry!(
            "foyer_storage_op_duration",
            "foyer storage op duration",
            &["foyer", "op", "extra"],
            exponential_buckets(0.0001, 2.0, 16).unwrap(), // 100 us ~ 6.55 s
            registry,
        )
        .unwrap();
        let op_bytes = register_int_counter_vec_with_registry!(
            "foyer_storage_op_bytes",
            "foyer storage op bytes",
            &["foyer", "op", "extra"],
            registry,
        )
        .unwrap();

        let total_bytes = register_uint_gauge_vec_with_registry!(
            "foyer_storage_total_bytes",
            "foyer storage total bytes",
            &["foyer", "total", "extra"],
            registry,
        )
        .unwrap();

        let inner_op_duration = register_histogram_vec_with_registry!(
            "foyer_storage_inner_op_duration",
            "foyer storage inner op duration",
            &["foyer", "op", "extra"],
            exponential_buckets(0.0001, 2.0, 16).unwrap(), // 1 us ~ 0.655 s
            registry,
        )
        .unwrap();
        let inner_op_bytes = register_int_counter_vec_with_registry!(
            "foyer_storage_inner_op_bytes",
            "foyer storage inner op bytes",
            &["foyer", "op", "extra"],
            registry,
        )
        .unwrap();
        let inner_op_bytes_distribution = register_histogram_vec_with_registry!(
            "foyer_storage_inner_op_bytes_distribution",
            "foyer storage inner op bytes distribution",
            &["foyer", "op", "extra"],
            exponential_buckets(0.0001, 2.0, 16).unwrap(), // 1 us ~ 0.655 s
            registry,
        )
        .unwrap();

        Self {
            op_duration,
            op_bytes,
            total_bytes,
            inner_op_duration,
            inner_op_bytes,
            inner_op_bytes_distribution,
        }
    }

    pub fn metrics(&self, foyer: &str) -> Metrics {
        Metrics::new(self, foyer)
    }
}

#[derive(Debug)]
pub struct Metrics {
    pub inner_op_duration_wal_flush: Histogram,
    pub inner_op_bytes_wal_flush: IntCounter,
    pub inner_op_bytes_distribution_wal_flush: Histogram,

    pub inner_op_duration_wal_write: Histogram,
    pub inner_op_duration_wal_sync: Histogram,

    pub inner_op_duration_wal_append: Histogram,
    pub inner_op_duration_wal_notify: Histogram,

    pub _total_bytes: UintGauge,
}

impl Metrics {
    pub fn new(global: &GlobalMetrics, foyer: &str) -> Self {
        let inner_op_duration_wal_flush = global.inner_op_duration.with_label_values(&[foyer, "wal", "flush"]);
        let inner_op_bytes_wal_flush = global.inner_op_bytes.with_label_values(&[foyer, "wal", "flush"]);
        let inner_op_bytes_distribution_wal_flush = global
            .inner_op_bytes_distribution
            .with_label_values(&[foyer, "wal", "flush"]);

        let inner_op_duration_wal_write = global.inner_op_duration.with_label_values(&[foyer, "wal", "write"]);

        let inner_op_duration_wal_sync = global.inner_op_duration.with_label_values(&[foyer, "wal", "sync"]);

        let inner_op_duration_wal_append = global.inner_op_duration.with_label_values(&[foyer, "wal", "append"]);
        let inner_op_duration_wal_notify = global.inner_op_duration.with_label_values(&[foyer, "wal", "notify"]);

        let total_bytes = global.total_bytes.with_label_values(&[foyer, "", ""]);

        Self {
            inner_op_duration_wal_flush,
            inner_op_bytes_wal_flush,
            inner_op_bytes_distribution_wal_flush,

            inner_op_duration_wal_write,
            inner_op_duration_wal_sync,

            inner_op_duration_wal_append,
            inner_op_duration_wal_notify,

            _total_bytes: total_bytes,
        }
    }
}
