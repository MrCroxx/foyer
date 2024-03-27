use std::{hash::BuildHasher, sync::Arc};

use ahash::RandomState;
use foyer_memory::{
    cache::{LfuCache, LruCache},
    eviction::Eviction,
    indexer::Indexer,
    CacheEventListener, DefaultCacheEventListener, FifoCache, FifoCacheConfig, FifoConfig, GenericCache, Handle,
    LfuCacheConfig, LfuConfig, LruCacheConfig, LruConfig,
};
use rand::{distributions::Distribution, thread_rng};

type CacheKey = String;
type CacheValue = ();

const ITEMS: usize = 10_000;
const ITERATIONS: usize = 5_000_000;

const SHARDS: usize = 1;
const OBJECT_POOL_CAPACITY: usize = 16;
/*
inspired by pingora/tinyufo/benches/bench_hit_ratio.rs
cargo bench --bench bench_hit_ratio

zif_exp, cache_size             fifo            lru             lfu             moka
0.90, 0.005                     16.26%          19.22%          32.38%          33.46%
0.90, 0.01                      22.55%          26.21%          38.55%          37.93%
0.90, 0.05                      41.10%          45.60%          55.45%          55.26%
0.90,  0.1                      51.11%          55.72%          63.83%          64.21%
0.90, 0.25                      66.81%          71.17%          76.21%          77.14%
1.00, 0.005                     26.64%          31.08%          44.14%          45.61%
1.00, 0.01                      34.37%          39.13%          50.60%          50.67%
1.00, 0.05                      54.06%          58.79%          66.79%          66.99%
1.00,  0.1                      63.12%          67.58%          73.92%          74.38%
1.00, 0.25                      76.14%          79.92%          83.61%          84.33%
1.05, 0.005                     32.63%          37.68%          50.24%          51.77%
1.05, 0.01                      40.95%          46.09%          56.75%          57.15%
1.05, 0.05                      60.47%          65.06%          72.07%          72.36%
1.05,  0.1                      68.96%          73.15%          78.52%          78.96%
1.05, 0.25                      80.42%          83.76%          86.79%          87.42%
1.10, 0.005                     39.02%          44.52%          56.29%          57.90%
1.10, 0.01                      47.66%          52.99%          62.64%          63.23%
1.10, 0.05                      66.60%          70.95%          76.94%          77.25%
1.10,  0.1                      74.26%          78.09%          82.56%          82.93%
1.10, 0.25                      84.15%          87.06%          89.54%          90.05%
1.50, 0.005                     81.19%          85.28%          88.91%          89.94%
1.50, 0.01                      86.91%          89.87%          92.24%          92.78%
1.50, 0.05                      94.75%          96.04%          96.95%          97.07%
1.50,  0.1                      96.65%          97.51%          98.06%          98.15%
1.50, 0.25                      98.35%          98.81%          99.04%          99.09%
*/
fn cache_hit<H, E, I, L, S>(
    cache: Arc<GenericCache<CacheKey, CacheValue, H, E, I, L, S>>,
    keys: Arc<Vec<CacheKey>>,
) -> f64
where
    H: Handle<Key = CacheKey, Value = CacheValue>,
    E: Eviction<Handle = H>,
    I: Indexer<Key = CacheKey, Handle = H>,
    L: CacheEventListener<CacheKey, CacheValue>,
    S: BuildHasher + Send + Sync + 'static,
{
    let mut hit = 0;
    for key in keys.iter() {
        let value = cache.get(key);
        if value.is_some() {
            hit += 1;
        } else {
            cache.insert(key.clone(), (), 1);
        }
    }
    hit as f64 / ITERATIONS as f64
}

fn moka_cache_hit(cache: &moka::sync::Cache<CacheKey, CacheValue>, keys: &Vec<String>) -> f64 {
    let mut hit = 0;
    for key in keys.iter() {
        let value = cache.get(key);
        if value.is_some() {
            hit += 1;
        } else {
            cache.insert(key.clone(), ());
        }
    }
    hit as f64 / ITERATIONS as f64
}

fn new_fifo_cache(capacity: usize) -> Arc<FifoCache<CacheKey, CacheValue>> {
    let config = FifoCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: FifoConfig {},
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(FifoCache::<CacheKey, CacheValue>::new(config))
}

fn new_lru_cache(capacity: usize) -> Arc<LruCache<CacheKey, CacheValue>> {
    let config = LruCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: LruConfig {
            high_priority_pool_ratio: 0.1,
        },
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(LruCache::<CacheKey, CacheValue>::new(config))
}

fn new_lfu_cache(capacity: usize) -> Arc<LfuCache<CacheKey, CacheValue>> {
    let config = LfuCacheConfig {
        capacity,
        shards: SHARDS,
        eviction_config: LfuConfig {
            window_capacity_ratio: 0.1,
            protected_capacity_ratio: 0.8,
            cmsketch_eps: 0.001,
            cmsketch_confidence: 0.9,
        },
        object_pool_capacity: OBJECT_POOL_CAPACITY,
        hash_builder: RandomState::default(),
        event_listener: DefaultCacheEventListener::default(),
    };

    Arc::new(LfuCache::<CacheKey, CacheValue>::new(config))
}

fn bench_one(zif_exp: f64, cache_size_percent: f64) {
    print!("{zif_exp:.2}, {cache_size_percent:4}\t\t\t");
    let mut rng = thread_rng();
    let zipf = zipf::ZipfDistribution::new(ITEMS, zif_exp).unwrap();

    let cache_size = (ITEMS as f64 * cache_size_percent) as usize;

    let fifo_cache = new_fifo_cache(cache_size);
    let lru_cache = new_lru_cache(cache_size);
    let lfu_cache = new_lfu_cache(cache_size);
    let moka_cache = moka::sync::Cache::new(cache_size as u64);

    let mut keys = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let key = zipf.sample(&mut rng).to_string();
        keys.push(key.clone());
    }

    let keys = Arc::new(keys);

    // Use multiple threads to simulate concurrent read-through requests.
    let fifo_cache_hit_handle = std::thread::spawn({
        let cache = fifo_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let lru_cache_hit_handle = std::thread::spawn({
        let cache = lru_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let lfu_cache_hit_handle = std::thread::spawn({
        let cache = lfu_cache.clone();
        let keys = keys.clone();
        move || cache_hit(cache, keys)
    });

    let moka_cache_hit_handle = std::thread::spawn({
        let cache = moka_cache.clone();
        let keys = keys.clone();
        move || moka_cache_hit(&cache, &keys)
    });

    let fifo_hit_ratio = fifo_cache_hit_handle.join().unwrap();
    let lru_hit_ratio = lru_cache_hit_handle.join().unwrap();
    let lfu_hit_ratio = lfu_cache_hit_handle.join().unwrap();
    let moka_hit_ratio = moka_cache_hit_handle.join().unwrap();

    print!("{:.2}%\t\t", fifo_hit_ratio * 100.0);
    print!("{:.2}%\t\t", lru_hit_ratio * 100.0);
    print!("{:.2}%\t\t", lfu_hit_ratio * 100.0);
    print!("{:.2}%\n", moka_hit_ratio * 100.0);
}

fn bench_zipf_hit() {
    println!("zif_exp, cache_size\t\tfifo\t\tlru\t\tlfu\t\tmoka");
    for zif_exp in [0.9, 1.0, 1.05, 1.1, 1.5] {
        for cache_capacity in [0.005, 0.01, 0.05, 0.1, 0.25] {
            bench_one(zif_exp, cache_capacity);
        }
    }
}

fn main() {
    bench_zipf_hit();
}
