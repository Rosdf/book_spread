use std::collections::HashMap;
use rustc_hash::FxBuildHasher;

pub type InternalHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

pub const fn new_internal_map<K, V>() -> InternalHashMap<K, V> {
    HashMap::with_hasher(FxBuildHasher)
}

pub fn new_internal_map_with_capacity<K, V>(capacity: usize) -> InternalHashMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, FxBuildHasher)
}