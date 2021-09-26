//! Thread-safe key/value cache.

use std::collections::hash_map::{Entry, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock, LockResult};

/// Cache that remembers the result for each key.
#[derive(Debug, Default)]
pub struct Cache<K, V> {
    // todo! This is an example cache type. Build your own cache type that satisfies the
    // specification for `get_or_insert_with`.
    inner: Mutex<HashMap<K, Arc<RwLock<Option<V>>>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    /// Retrieve the value or insert a new one created by `f`.
    ///
    /// An invocation to this function should not block another invocation with a different key.
    /// For example, if a thread calls `get_or_insert_with(key1, f1)` and another thread calls
    /// `get_or_insert_with(key2, f2)` (`key1≠key2`, `key1,key2∉cache`) concurrently, `f1` and `f2`
    /// should run concurrently.
    ///
    /// On the other hand, since `f` may consume a lot of resource (= money), it's desirable not to
    /// duplicate the work. That is, `f` should be run only once for each key. Specifically, even
    /// for the concurrent invocations of `get_or_insert_with(key, f)`, `f` is called only once.
    pub fn get_or_insert_with<F: FnOnce(K) -> V>(&self, key: K, f: F) -> V {
        use std::collections::hash_map::Entry;

        let mut map = self.inner.lock().unwrap();

        let (found, has_inserted) = match map.entry(key.clone()) {
            Entry::Occupied(o) => (o.into_mut().clone(), false),
            Entry::Vacant(v) => {
                let placeholder = Arc::new(RwLock::new(None));
                let p = v.insert(placeholder);
                (p.clone(), true)
            }
        };
        let first_write_lock = if has_inserted { Some(found.write().unwrap()) } else { None };
        drop(map);

        match first_write_lock {
            Some(mut write_guard) => *write_guard = Some(f(key)),
            None => (),
        }
        // first_write_lock moved out.

        let ret = found.read().unwrap().clone().unwrap();

        ret
    }
}
