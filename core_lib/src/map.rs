use hashbrown::{HashMap, HashSet};
use rustc_hash::FxBuildHasher;

pub type InternalHashMap<K, V> = HashMap<K, V, FxBuildHasher>;
pub type InternalHashSet<T> = HashSet<T, FxBuildHasher>;

pub const fn new_internal_map<K, V>() -> InternalHashMap<K, V> {
    HashMap::with_hasher(FxBuildHasher)
}

pub const fn new_internal_set<T>() -> InternalHashSet<T> {
    HashSet::with_hasher(FxBuildHasher)
}

pub fn new_internal_map_with_capacity<K, V>(capacity: usize) -> InternalHashMap<K, V> {
    HashMap::with_capacity_and_hasher(capacity, FxBuildHasher)
}
