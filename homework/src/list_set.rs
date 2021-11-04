#![allow(clippy::mutex_atomic)]

use std::cmp;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}

unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}

unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        loop {
            let node_p = *self.0;
            if node_p.is_null() {
                return false;
            }

            let data = unsafe { &(*node_p).data };
            if data == key {
                return true;
            } else if data > key {
                return false;
            } else {
                self.0 = unsafe { (*node_p).next.lock().unwrap() };
            }
        }
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let mut cursor = Cursor(self.head.lock().unwrap());
        (cursor.find(key), cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let (b, _) = self.find(key);
        b
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        match self.find(&key) {
            (true, _) => Err(key),
            (false, Cursor(mut guard)) => {
                let next_node = *guard;
                let new_node = Node::new(key, next_node);
                (*guard) = new_node;
                Ok(())
            }
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        match self.find(&key) {
            (true, Cursor(mut guard)) => {
                assert!(!(*guard).is_null());
                assert!(unsafe { (**guard).data.eq(key) });
                let next_node = unsafe { *(**guard).next.lock().unwrap() };
                let data = unsafe { Box::from_raw(*guard).data };
                *guard = next_node;
                Ok(data)
            }
            (false, _) => Err(())
        }
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.0 {
            Some(guard) => {
                let node_p = **guard;
                if unsafe { node_p.is_null() } {
                    return None;
                }

                let data = unsafe { &(*node_p).data };

                self.0 = unsafe { Some((*node_p).next.lock().unwrap()) };

                Some(data)
            }
            None => None
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let guard = self.head.lock().unwrap();
        if (*guard).is_null() {
            return;
        }

        let mut curr_node = unsafe { Box::from_raw(*guard) };

        loop {
            let next_guard = curr_node.next.lock().unwrap();
            if (*next_guard).is_null() {
                return;
            }

            let next_node = unsafe { Box::from_raw(*next_guard) };

            drop(next_guard);
            curr_node = next_node;
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
