//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::ptr::null;
use crossbeam_epoch::{Atomic, CompareExchangeError, Guard, Owned, Pointer, Shared};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

fn get_top_bit(n: usize) -> usize {
    let mut a: usize = 1;
    while n >= a {
        a <<= 1;
    }
    a >>= 1;
    a
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        unsafe {
            loop {
                let sentinel = self.buckets.get(index, guard);
                let sentinel_read = sentinel.load(Ordering::Acquire, guard);

                if !sentinel_read.is_null() {
                    return Cursor::from_raw(sentinel, sentinel_read.as_raw());
                } else {
                    let mut cursor = if index == 0 {
                        self.list.head(guard)
                    } else {
                        let prev_bucket_ind = index - get_top_bit(index);
                        self.lookup_bucket(prev_bucket_ind, guard)
                    };

                    let new_bucket_key = index.reverse_bits();
                    let new_bucket = Owned::new(Node::new(new_bucket_key, None::<V>));

                    cursor.find_harris(&new_bucket_key, guard);
                    if let Err(_) = cursor.insert(new_bucket, guard) {
                        continue;
                    }

                    match sentinel.compare_exchange(
                        Shared::null(), cursor.curr(), Ordering::Release, Ordering::Relaxed, guard) {
                        Ok(_) => {}
                        Err(_) => { cursor.delete(guard); }
                    }
                }
            }
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let size = self.size.load(Ordering::Acquire);
        let index = key % size;
        let mut cursor = self.lookup_bucket(index, guard);

        let found =
            cursor.find_harris(&(key.reverse_bits() + 1), guard).unwrap();

        (size, found, cursor)
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if found {
            let a = cursor.lookup();
            match a {
                None => { None }
                Some(dd) => {
                    match dd {
                        None => { None }
                        Some(dd) => { Some(dd) }
                    }
                }
            }
        } else {
            None
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let (size, found, mut cursor) = self.find(key, guard);
        if found {
            Err(value)
        } else {
            let prev_count = self.count.fetch_add(1, Ordering::AcqRel);
            cursor.insert(Owned::new(Node::new(key.reverse_bits() + 1, Some(value))), guard).unwrap();
            if prev_count + 1 > size * Self::LOAD_FACTOR {
                self.size.compare_exchange(size, size * 2, Ordering::Release, Ordering::Relaxed);
            }

            Ok(())
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);

        let (_, found, mut cursor) = self.find(key, guard);
        if found {
            self.count.fetch_sub(1, Ordering::AcqRel);
            let ret = cursor.delete(guard);

            match ret {
                Ok(op) => {
                    match op {
                        None => { Err(()) }
                        Some(v) => { Ok(v) }
                    }
                }
                Err(_) => { Err(()) }
            }
        } else {
            Err(())
        }
    }
}
