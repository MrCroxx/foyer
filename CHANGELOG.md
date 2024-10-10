---
title: Changelog
description: Changelog for foyer.
authors: mrcroxx
date: 2023-05-12T11:02:09+08:00
---

# Changelog

<!-- truncate -->

## 2024-10-11

### Releases

| crate | version |
| - | - |
| foyer | 0.12.2|
| foyer-common | 0.12.2 |
| foyer-intrusive | 0.12.2 |
| foyer-memory | 0.12.2 |
| foyer-storage | 0.12.2 |
| foyer-bench | 0.12.2 |

### Changes

- Revert "Scale shards to 1 when there is not enough capacity". It would be useful sometimes. Just raise the warning.

## 2024-10-10

### Releases

| crate | version |
| - | - |
| foyer | 0.12.1 |
| foyer-common | 0.12.1 |
| foyer-intrusive | 0.12.1 |
| foyer-memory | 0.12.1 |
| foyer-storage | 0.12.1 |
| foyer-bench | 0.12.1 |

### Changes

- Downgrade hashbrown to 0.14 to fix build on nightly for projects using hashbrown < 0.15.
- Fix build with madsim.
- Refine small object disk cache.
- Scale shards to 1 when there is not enough capacity.

## 2024-10-09

### Releases

| crate | version |
| - | - |
| foyer | 0.12.0 |
| foyer-common | 0.12.0 |
| foyer-intrusive | 0.12.0 |
| foyer-memory | 0.12.0 |
| foyer-storage | 0.12.0 |
| foyer-bench | 0.12.0 |

### Changes

- Align the versions of all components to the same. 📣
- Introduce small object disk cache. 🎉
- Introduce mixed/large/small storage engine.
- Refactor builders for the hybrid cache.
- Introduce submit queue size threshold to prevent from channel piling up.
- Support `jeprof` for foyer-bench.
- Rename feature "mtrace" to "tracing".

## 2024-09-25

### Releases

| crate | version |
| - | - |
| foyer | 0.11.5 |
| foyer-common | 0.9.5 |
| foyer-intrusive | 0.9.5 |
| foyer-memory | 0.7.5 |
| foyer-storage | 0.10.5 |
| foyer-bench | 0.3.5 |

### Changes

- Fix panic on dropping the hybrid cache. #736

## 2024-09-24

### Releases

| crate | version |
| - | - |
| foyer | 0.11.4 |
| foyer-common | 0.9.4 |
| foyer-intrusive | 0.9.4 |
| foyer-memory | 0.7.4 |
| foyer-storage | 0.10.4 |
| foyer-bench | 0.3.4 |

### Changes

- Revert pre-serialization design. The insert latency and memory usage would be better for most cases.
- Rename `with_buffer_threshold` to `with_buffer_pool_size`. The old method is kept but marked as deprecated.
- Raise a warn when using `DirectFileDevice` on within a file system.

## 2024-09-20

### Releases

| crate | version |
| - | - |
| foyer | 0.11.3 |
| foyer-common | 0.9.3 |
| foyer-intrusive | 0.9.3 |
| foyer-memory | 0.7.3 |
| foyer-storage | 0.10.3 |
| foyer-bench | 0.3.3 |

### Changes

- Fix panicked by io buffer pool alignment issue.

## 2024-09-12

### Releases

| crate | version |
| - | - |
| foyer | 0.11.2 |
| foyer-common | 0.9.2 |
| foyer-intrusive | 0.9.2 |
| foyer-memory | 0.7.2 |
| foyer-storage | 0.10.2 |
| foyer-bench | 0.3.2 |

### Changes

- Support windows (for `foyer` only).
- Bump rust toolchain to `1.81.0`.
- Expose in-memory cache builder and cache entry.
- Reduce page fault and vec growth overhead.
- Use bytes size for `foyer-bench`.
- Fix install deps script.

## 2024-08-31

### Releases

| crate | version |
| - | - |
| foyer | 0.11.1 |
| foyer-common | 0.9.1 |
| foyer-intrusive | 0.9.1 |
| foyer-memory | 0.7.1 |
| foyer-storage | 0.10.1 |
| foyer-bench | 0.3.1 |

### Changes

- Add metrics for serde.
- Refine `fetch` runtime usage.
- Fix unhandled low-layer errors. #674 #677 #679
- Implement `Default` for `TokioRuntimeConfig`.
- Fix typos and format code with unstable features.

## 2024-08-21

### Releases

| crate | version |
| - | - |
| foyer | 0.11.0 |
| foyer-common | 0.9.0 |
| foyer-intrusive | 0.9.0 |
| foyer-memory | 0.7.0 |
| foyer-storage | 0.10.0 |
| foyer-bench | 0.3.0 |

### Changes

- Support disk cache on raw block device.
- Support fine-grained storage engine runtime configuration.
- Enhance performance via reducing page fault.
- Refine storage engine framework for future features.
- Expose `Weighter` trait.
- Support `serde` for more configurations.
- Update `foyer-bench` with more fine-grained configurations.
- Fix panics with `None` recover mode.

## 2024-08-15

### Releases

| crate | version |
| - | - |
| foyer | 0.10.4 |
| foyer-storage | 0.9.3 |
| foyer-bench | 0.2.3 |

### Changes

- Support serde for recover mode configuration.

## 2024-08-14

### Releases

| crate | version |
| - | - |
| foyer | 0.10.2 |
| foyer-storage | 0.9.2 |
| foyer-bench | 0.2.2 |

### Changes

- Fix panic with "none" recovery mode.

## 2024-07-08

### Releases

| crate | version |
| - | - |
| foyer | 0.10.1 |
| foyer-common | 0.8.1 |
| foyer-intrusive | 0.8.1 |
| foyer-memory | 0.6.1 |
| foyer-storage | 0.9.1 |
| foyer-bench | 0.2.1 |

### Changes

- Refine write model, make flush buffer threshold configurable to mitigate memory usage spike and OOM.

## 2024-07-02

### Releases

| crate | version |
| - | - |
| foyer | 0.10.0 |
| foyer-common | 0.8.0 |
| foyer-intrusive | 0.8.0 |
| foyer-memory | 0.6.0 |
| foyer-storage | 0.9.0 |
| foyer-bench | 0.2.0 |

### Changes

- Introduce tail-based tracing framework with [minitrace](https://github.com/tikv/minitrace-rust). [Tail-based Tracing Example](https://github.com/foyer-rs/foyer/tree/main/examples/tail_based_tracing.rs).
- Fix `fetch()` disk cache refill on in-memory cache miss.
- Publish *foyer* logo! 

<img src="https://raw.githubusercontent.com/foyer-rs/foyer/main/etc/logo/slogan.min.svg" style="width: 200px"/>

## 2024-06-14

### Releases

| crate | version |
| - | - |
| foyer | 0.9.4 |
| foyer-storage | 0.8.5 |
| foyer-bench | 0.1.4 |

### Changes

- Fix phantom entries after foyer storage recovery. [#560](https://github.com/foyer-rs/foyer/pull/560)
- Fix hybrid cache hit metrics with `fetch()` interface. [#563](https://github.com/foyer-rs/foyer/pull/563)

## 2024-06-05

### Releases

| crate | version |
| - | - |
| foyer | 0.9.3 |
| foyer-common | 0.7.3 |
| foyer-intrusive | 0.7.2 |
| foyer-memory | 0.5.2 |
| foyer-storage | 0.8.4 |
| foyer-bench | 0.1.3 |

### Changes

- Hybrid cache `fetch()` use the dedicated runtime by default if enabled.
- Separate `fetch()` and `fetch_with_runtime()` interface for in-memory cache.

## 2024-06-04

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.8.3 |

### Changes

- Fix "invalid argument (code: 22)" on target aarch64.

## 2024-06-03

### Releases

| crate | version |
| - | - |
| foyer | 0.9.2 |
| foyer-common | 0.7.2 |
| foyer-intrusive | 0.7.1 |
| foyer-memory | 0.5.1 |
| foyer-storage | 0.8.2 |
| foyer-bench | 0.1.2 |

### Changes

- Support customized cache event listener.

## 2024-05-31

### Releases

| crate | version |
| - | - |
| foyer | 0.9.1 |
| foyer-common | 0.7.1 |
| foyer-intrusive | 0.7.0 |
| foyer-memory | 0.5.0 |
| foyer-storage | 0.8.1 |
| foyer-bench | 0.1.1 |

### Changes

- Fix "attempt to subtract with overflow" panic after cloning cache entry. [#543](https://github.com/foyer-rs/foyer/issues/543).
- Fix "assertion failed: base.is_in_indexer()" panic after replacing deposit entry. [#547](https://github.com/foyer-rs/foyer/issues/547).
- Fix setting name for in-memory cache.
- Add region related metrics.
- Introduce `strict_assertions` and `sanity` feature for debugging.
- `foyer-bench` support setting eviction algorithm.
- Upgrade `metrics` to `0.23`.
- Remove `pop()` related interface from the in-memory cache.
- Refine intrusive data structure implementation.

## 2024-05-27

### Releases

| crate | version |
| - | - |
| foyer | 0.9.0 |
| foyer-common | 0.7.0 |
| foyer-intrusive | 0.6.0 |
| foyer-memory | 0.4.0 |
| foyer-storage | 0.8.0 |
| foyer-bench | 0.1.0 |

### Changes

- Refine the storage engine to reduce the overhead and boost the performance.
  - Hybrid cache memory overhead reduced by ~50%.
  - Hybrid cache operation latency reduced by ~80%.
- Replace `prometheus` with `metrics` to support more flexible metrics collection.
- Introduce `foyer-bench` to replace the original `foyer-storage-bench` for better benchmarking.
- Fulfill rust docs for public APIs.
- Introduce tombstone log for updatable persistent cache.
- Reduce unnecessary dependencies.
- More details: [foyer - Development Roadmap](https://github.com/orgs/foyer-rs/projects/2).

## 2024-04-28

### Releases

| crate | version |
| - | - |
| foyer | 0.8.9 |
| foyer-common | 0.6.4 |
| foyer-memory | 0.3.6 |
| foyer-storage | 0.7.6 |
| foyer-storage-bench | 0.7.5 |

### Changes

- feat: Add config to control the recover mode.
- feat: Add config to enable/disable direct i/o. (Enabled by default for large entries optimization.)

## 2024-04-28

### Releases

| crate | version |
| - | - |
| foyer | 0.8.8 |
| foyer-memory | 0.3.5 |
| foyer-storage | 0.7.5 |
| foyer-storage-bench | 0.7.4 |

### Changes

- feat: Impl `Debug` for `HybridCache`.
- feat: Impl `serde`, `Default` for eviction configs.
- refactor: Add internal trait `EvictionConfig` to bound eviction algorithm configs.

## 2024-04-27

### Releases

| crate | version |
| - | - |
| foyer | 0.8.7 |

### Changes

- Make `HybridCache` cloneable.

## 2024-04-27

### Releases

| crate | version |
| - | - |
| foyer-memory | 0.3.4 |

### Changes

- Fix S3FIFO ghost queue.

## 2024-04-26

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.7.4 |

### Changes

- Fix `FsDeviceBuilder` on a non-exist directory without capacity given.

## 2024-04-26

### Releases

| crate | version |
| - | - |
| foyer | 0.8.6 |
| foyer-common | 0.6.3 |
| foyer-intrusive | 0.5.3 |
| foyer-memory | 0.3.3 |
| foyer-storage | 0.7.3 |
| foyer-storage-bench | 0.7.3 |

### Changes

- Remove unused dependencies.
- Remove hakari workspace hack.

## 2024-04-26

### Releases

| crate | version |
| - | - |
| foyer | 0.8.5 |

### Changes

- Expose `EntryState`, `HybridEntry`.
- Expose `StorageWriter`, `Metrics`, `get_metrics_registry`, `set_metrics_registry`.
- Expose `RangeBoundsExt`, `BufExt`, `BufMutExt`.
- Re-export `ahash::RandomState`.
- Loose `entry()` args trait bounds.

## 2024-04-25

### Releases

| crate | version |
| - | - |
| foyer | 0.8.4 |

### Changes

- Expose `HybridCacheEntry`.

## 2024-04-25

### Releases

| crate | version |
| - | - |
| foyer | 0.8.3 |

### Changes

- Expose `Key`, `Value`, `StorageKey`, `StorageValue` traits.

## 2024-04-24

### Releases

| crate | version |
| - | - |
| foyer | 0.8.2 |
| foyer-common | 0.6.2 |
| foyer-intrusive | 0.5.2 |
| foyer-memory | 0.3.2 |
| foyer-storage | 0.7.2 |
| foyer-storage-bench | 0.7.2 |
| foyer-workspace-hack | 0.5.2 |

### Changes

- Add `nightly` feature to make it compatible with night toolchain.

## 2024-04-24

### Releases

| crate | version |
| - | - |
| foyer | 0.8.1 |
| foyer-common | 0.6.1 |
| foyer-intrusive | 0.5.1 |
| foyer-memory | 0.3.1 |
| foyer-storage | 0.7.1 |
| foyer-storage-bench | 0.7.1 |
| foyer-workspace-hack | 0.5.1 |

### Changes

- Add `with_flush` to enable flush for each io.
- Loose MSRV to 1.76 .
- Flush the device on store close.

## 2024-04-23

### Releases

| crate | version |
| - | - |
| foyer | 0.8.0 |
| foyer-common | 0.6.0 |
| foyer-intrusive | 0.5.0 |
| foyer-memory | 0.3.0 |
| foyer-storage | 0.7.0 |
| foyer-storage-bench | 0.7.0 |
| foyer-workspace-hack | 0.5.0 |

### Changes

- Combine in-memory cache and disk cache into `HybridCache`.
- Refine APIs, make them more user-friendly.
- Refine `Key`, `Value`, `StorageKey`, `StorageValue` traits.
- Support `serde` for storage key and value serialization and deserialization.
- Loose trait bounds for key and value.
- Add configurable ghost queue for S3FIFO.
- Fix S3FIFO eviction bugs.
- Add more examples.

## 2024-04-11

### Releases

| crate | version |
| - | - |
| foyer | 0.7.0 |
| foyer-common | 0.5.0 |
| foyer-intrusive | 0.4.0 |
| foyer-memory | 0.2.0 |
| foyer-storage | 0.6.0 |
| foyer-storage-bench | 0.6.0 |
| foyer-workspace-hack | 0.4.0 |

### Changes

- Make `foyer` compatible with rust stable toolchain (MSRV = 1.77.2). 🎉

## 2024-04-09

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.5.1 |
| foyer-memory | 0.1.4 |

### Changes

- fix: Fix panics on `state()` for s3fifo entry.
- fix: Enable `offset_of` feature for `foyer-storage`.

## 2024-04-08

### Releases

| crate | version |
| - | - |
| foyer-intrusive | 0.3.1 |
| foyer-memory | 0.1.3 |

### Changes

- feat: Introduce s3fifo to `foyer-memory`.
- fix: Fix doctest for `foyer-intrusive`.

## 2024-03-21

### Releases

| crate | version |
| - | - |
| foyer-memory | 0.1.2 |

### Changes

- fix: `foyer-memory` export `DefaultCacheEventListener`.

## 2024-03-14

### Releases

| crate | version |
| - | - |
| foyer-memory | 0.1.1 |

### Changes

- Make eviction config cloneable.

## 2024-03-13

### Releases

| crate | version |
| - | - |
| foyer-storage-bench | 0.5.1 |

### Changes

- Fix `foyer-storage-bench` build with `trace` feature.

## 2024-03-12

### Releases

| crate | version |
| - | - |
| foyer | 0.6.0 |
| foyer-common | 0.4.0 |
| foyer-intrusive | 0.3.0 |
| foyer-memory | 0.1.0 |
| foyer-storage | 0.5.0 |
| foyer-storage-bench | 0.5.0 |
| foyer-workspace-hack | 0.3.0 |

### Changes

- Release foyer in-memory cache as crate `foyer-memory`.
- Bump other components with changes.

## 2023-12-28

### Releases

| crate | version |
| - | - |
| foyer | 0.5.0 |
| foyer-common | 0.3.0 |
| foyer-intrusive | 0.2.0 |
| foyer-storage | 0.4.0 |
| foyer-storage-bench | 0.4.0 |
| foyer-workspace-hack | 0.2.0 |

### Changes

- Bump rust-toolchain to "nightly-2023-12-26".
- Introduce time-series distribution args to bench tool. [#253](https://github.com/foyer-rs/foyer/pull/253)
- Fix duplicated insert drop metrics.

## 2023-12-22

### Releases

| crate | version |
| - | - |
| foyer | 0.4.0 |
| foyer-storage | 0.3.0 |
| foyer-storage-bench | 0.3.0 |
| foyer-workspace-hack | 0.1.1 |

### Changes

- Remove config `flusher_buffer_capacity`.
- Fix benchmark tool cache miss ratio.

## 2023-12-20

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.2.2 |

### Changes

- Fix metrics for writer dropping.
- Add interface `insert_async_with_callback` and `insert_if_not_exists_async_with_callback` for callers to get the insert result.

## 2023-12-18

### Releases

| crate | version |
| - | - |
| foyer-storage | 0.2.1 |

### Changes

- Introduce the entry size histogram, update metrics.

## 2023-12-18

### Releases

| crate | version |
| - | - |
| foyer | 0.3.0 |
| foyer-common | 0.2.0 |
| foyer-storage | 0.2.0 |
| foyer-storage-bench | 0.2.0 |

### Changes

- Introduce the associated type `Cursor` for trait `Key` and `Value` to reduce unnecessary buffer copy if possible.
- Remove the ring buffer and continuum tracker for they are no longer needed.
- Update the configuration of the storage engine and the benchmark tool.

## 2023-11-29

### Releases

| crate | version |
| - | - |
| foyer | 0.2.0 |
| foyer-common | 0.1.0 |
| foyer-intrusive | 0.1.0 |
| foyer-storage | 0.1.0 |
| foyer-storage-bench | 0.1.0 |
| foyer-workspace-hack | 0.1.0 |

### Changes

The first version that can be used as file cache.

The write model and the design of storage engine has been switched from CacheLib navy version to the ring buffer version (which was highly inspired by MySQL 8.0 link_buf).

Introduces `Store`, `RuntimeStore`, `LazyStore` to simplify usage. In most cases, `RuntimeStore` is preferred to use a dedicated tokio runtime to serve **foyer** to avoid the influence to the user's runtime. If lazy-load is needed, use `RuntimeLazyStore` instead.

The implementation of **foyer** is separated into multiple crates. But importing `foyer` is enough for it re-exports the crates that **foyer**'s user needs.

Brief description about the subcrates:

- foyer-common: Provide basic data structures and algorithms.
- foyer-intrusive: Provide intrusive containers for implementing eviction lists and collections. Intrusive data structures provide the ability to implement low-cost multi-index data structures, which will be used for the memory cache in future.
- foyer-storage: Provide the file cache storage engine and wrappers to simplify the code.
- foyer-storage-bench: Runnable benchmark tool for the file cache storage engine.
- foyer-workspace-hack: Generated by [hakari](https://crates.io/crates/hakari) to prevent building each crate from triggering building from scratch.

## 2023-05-12

### Releases

| crate | version |
| - | - |
| foyer | 0.1.0 |

### Changes

Initial version with just basic interfaces.
