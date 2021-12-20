use core::marker::PhantomData;
use core::ptr::{self, NonNull};
use std::collections::HashSet;
use std::fmt;

#[cfg(not(feature = "check-loom"))]
use core::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::ops::Deref;
use std::ptr::null;
#[cfg(feature = "check-loom")]
use loom::sync::atomic::{fence, AtomicBool, AtomicPtr, AtomicUsize, Ordering};

use super::HAZARDS;

/// Represents the ownership of a hazard pointer slot.
pub struct Shield<T> {
    slot: NonNull<HazardSlot>,
    _marker: PhantomData<*const T>, // !Send + !Sync
}

impl<T> Shield<T> {
    /// Creates a new shield for hazard pointer.
    pub fn new(hazards: &HazardBag) -> Self {
        let slot = hazards.acquire_slot();
        Self {
            slot: slot.into(),
            _marker: PhantomData,
        }
    }

    /// Try protecting the pointer `*pointer`.
    /// 1. Store `*pointer` to the hazard slot.
    /// 2. Check if `src` still points to `*pointer` (validation) and update `pointer` to the
    ///    latest value.
    /// 3. If validated, return true. Otherwise, clear the slot (store 0) and return false.
    pub fn try_protect(&self, pointer: &mut *const T, src: &AtomicPtr<T>) -> bool {
        unsafe {
            let ptr = (*pointer);
            let slot = self.slot.as_ref();
            slot.hazard.store(ptr as usize, Ordering::Release);
            let loaded = src.load(Ordering::Acquire) as * const T;
            if loaded == ptr {
                true
            }
            else {
                *pointer = loaded;
                slot.hazard.store(0, Ordering::Release);
                false
            }
        }
    }

    /// Get a protected pointer from `src`.
    pub fn protect(&self, src: &AtomicPtr<T>) -> *const T {
        let mut pointer = src.load(Ordering::Relaxed) as *const T;
        while !self.try_protect(&mut pointer, src) {
            #[cfg(feature = "check-loom")]
                loom::sync::atomic::spin_loop_hint();
        }
        pointer
    }
}

impl<T> Default for Shield<T> {
    fn default() -> Self {
        Self::new(&HAZARDS)
    }
}

impl<T> Drop for Shield<T> {
    /// Clear and release the ownership of the hazard slot.
    fn drop(&mut self) {
        todo!()
    }
}

impl<T> fmt::Debug for Shield<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shield")
            .field("slot address", &self.slot)
            .field("slot data", unsafe { self.slot.as_ref() })
            .finish()
    }
}

/// Global bag (multiset) of hazards pointers.
/// `HazardBag.head` and `HazardSlot.next` form a grow-only list of all hazard slots. Slots are
/// never removed from this list. Instead, it gets deactivated and recycled for other `Shield`s.
#[derive(Debug)]
pub struct HazardBag {
    head: AtomicPtr<HazardSlot>,
}

/// See `HazardBag`
#[derive(Debug)]
struct HazardSlot {
    // Whether this slot is occupied by a `Shield`.
    active: AtomicBool,
    // Machine representation of the hazard pointer.
    hazard: AtomicUsize,
    // Immutable pointer to the next slot in the bag.
    next: *const HazardSlot,
}

impl HazardSlot {
    fn new(next: *const HazardSlot) -> Self {
        Self {
            active: AtomicBool::new(false),
            hazard: AtomicUsize::new(0),
            next,
        }
    }
}

impl HazardBag {
    #[cfg(not(feature = "check-loom"))]
    /// Creates a new global hazard set.
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[cfg(feature = "check-loom")]
    /// Creates a new global hazard set.
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Acquires a slot in the hazard set, either by recycling an inactive slot or allocating a new
    /// slot.
    fn acquire_slot(&self) -> &HazardSlot {
        unsafe {
            // try recycling an inactive slot
            if let Some(slot) = self.try_acquire_inactive() {
                return slot;
            }

            // allocate a new slot and return
            loop {
                let head = self.head.load(Ordering::Acquire);
                let new_slot = Box::new(HazardSlot::new(head));
                new_slot.active.store(true, Ordering::Release);
                let new_slot_raw = Box::into_raw(new_slot);
                match self.head.compare_exchange(head, new_slot_raw, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => { return &*new_slot_raw; }
                    Err(e) => { drop(Box::from_raw(e)) }
                }
            }
        }
    }

    /// Find an inactive slot and activate it.
    fn try_acquire_inactive(&self) -> Option<&HazardSlot> {
        unsafe {
            let mut curr_p: *const HazardSlot = self.head.load(Ordering::Acquire);
            while !curr_p.is_null() {
                let curr = &*curr_p;
                match curr.active.compare_exchange(false, true, Ordering::Release, Ordering::Relaxed) {
                    Ok(_) => {
                        return Some(curr);
                    }
                    Err(_) => {}
                }
                curr_p = curr.next;
            };
            None
        }
    }

    /// Returns all the hazards in the set.
    pub fn all_hazards(&self) -> HashSet<usize> {
        unsafe {
            let mut ret = HashSet::<usize>::new();

            let mut curr_p: *const HazardSlot = self.head.load(Ordering::Acquire);
            while !curr_p.is_null() {
                let curr = &*curr_p;
                if curr.active.load(Ordering::Acquire) == true {
                    let hazard = curr.hazard.load(Ordering::Acquire);  // Todo:
                    ret.insert(hazard);
                }
            };
            ret
        }
    }
}

impl Drop for HazardBag {
    fn drop(&mut self) {
        todo!()
    }
}

unsafe impl Send for HazardSlot {}

unsafe impl Sync for HazardSlot {}

#[cfg(all(test, not(feature = "check-loom")))]
mod tests {
    use super::{HazardBag, Shield};
    use std::collections::HashSet;
    use std::mem;
    use std::ops::Range;
    use std::sync::{atomic::AtomicPtr, Arc};
    use std::thread;

    const THREADS: usize = 8;
    const VALUES: Range<usize> = 1..1024;

    // `all_hazards` should return hazards protected by shield(s).
    #[test]
    fn all_hazards_protected() {
        let hazard_bag = Arc::new(HazardBag::new());
        let _ = (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        shield.protect(&src);
                        // leak the shield so that
                        mem::forget(shield);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|th| th.join().unwrap())
            .collect::<Vec<_>>();
        let all = hazard_bag.all_hazards();
        let values = VALUES.collect();
        assert!(all.is_superset(&values))
    }

    // `all_hazards` should not return values that are no longer protected.
    #[test]
    fn all_hazards_unprotected() {
        let hazard_bag = Arc::new(HazardBag::new());
        let _ = (0..THREADS)
            .map(|_| {
                let hazard_bag = hazard_bag.clone();
                thread::spawn(move || {
                    for data in VALUES {
                        let src = AtomicPtr::new(data as *mut ());
                        let shield = Shield::new(&hazard_bag);
                        shield.protect(&src);
                    }
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|th| th.join().unwrap())
            .collect::<Vec<_>>();
        let all = hazard_bag.all_hazards();
        let values = VALUES.collect();
        let intersection: HashSet<_> = all.intersection(&values).collect();
        assert!(intersection.is_empty())
    }

    // `acquire_slot` should recycle existing slots.
    #[test]
    fn recycle_slots() {
        let hazard_bag = HazardBag::new();
        // allocate slots
        let shields = (0..1024)
            .map(|_| Shield::<()>::new(&hazard_bag))
            .collect::<Vec<_>>();
        // slot addresses
        let old_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();
        // release the slots
        drop(shields);

        let shields = (0..128)
            .map(|_| Shield::<()>::new(&hazard_bag))
            .collect::<Vec<_>>();
        let new_slots = shields
            .iter()
            .map(|s| s.slot.as_ptr() as usize)
            .collect::<HashSet<_>>();

        // no new slots should've been created
        assert!(new_slots.is_subset(&old_slots));
    }
}
