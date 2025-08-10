use parking_lot::Mutex;
use std::collections::VecDeque;
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, Ordering},
    Arc, Weak,
};

pub struct WeakCell<T> {
    ptr: AtomicPtr<T>,
}

unsafe impl<T> Send for WeakCell<T> {}
unsafe impl<T> Sync for WeakCell<T> {}

impl<T> Drop for WeakCell<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T> WeakCell<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Self { ptr: AtomicPtr::new(ptr::null_mut()) }
    }

    #[inline(always)]
    pub fn exists(&self) -> bool {
        self.ptr.load(Ordering::Acquire) != ptr::null_mut()
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<Arc<T>> {
        if self.ptr.load(Ordering::Acquire) == ptr::null_mut() {
            return None;
        }
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            return unsafe { Weak::from_raw(ptr) }.upgrade();
        } else {
            None
        }
    }

    pub fn clear(&self) {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            // Convert into Weak and drop
            let _ = unsafe { Weak::from_raw(ptr) };
        }
    }

    #[inline(always)]
    pub fn put(&self, item: Weak<T>) {
        let old_ptr = self.ptr.swap(item.into_raw() as *mut T, Ordering::SeqCst);
        if old_ptr != ptr::null_mut() {
            let _ = unsafe { Weak::from_raw(old_ptr) };
        }
    }
}

pub struct LockedQueue<T> {
    empty: AtomicBool,
    queue: Mutex<VecDeque<T>>,
}

impl<T> LockedQueue<T> {
    #[inline]
    pub fn new(cap: usize) -> Self {
        Self { empty: AtomicBool::new(true), queue: Mutex::new(VecDeque::with_capacity(cap)) }
    }

    #[inline(always)]
    pub fn push(&self, msg: T) {
        let mut guard = self.queue.lock();
        if guard.is_empty() {
            self.empty.store(false, Ordering::Release);
        }
        guard.push_back(msg);
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        if self.empty.load(Ordering::Acquire) {
            return None;
        }
        let mut guard = self.queue.lock();
        if let Some(item) = guard.pop_front() {
            if guard.len() == 0 {
                self.empty.store(true, Ordering::Release);
            }
            Some(item)
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let guard = self.queue.lock();
        guard.len()
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn exists(&self) -> bool {
        !self.empty.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_weak_cell() {
        let cell = WeakCell::new();
        assert!(!cell.exists());
        let item = Arc::new(1);
        cell.put(Arc::downgrade(&item));
        assert!(cell.exists());
        let _item = cell.pop().unwrap();
        assert!(!cell.exists());
        assert!(Arc::ptr_eq(&item, &_item));
        cell.put(Arc::downgrade(&item));
        assert!(cell.exists());
        cell.clear();
        assert!(!cell.exists());
        drop(_item);
        assert_eq!(Arc::strong_count(&item), 1);
        assert_eq!(Arc::weak_count(&item), 0);
    }

    #[test]
    fn test_locked_queue() {
        let queue = LockedQueue::new(2);
        assert_eq!(queue.len(), 0);
        assert!(!queue.exists());
        queue.push(1);
        assert_eq!(queue.len(), 1);
        assert!(queue.exists());
        queue.push(2);
        assert_eq!(queue.len(), 2);
        // exceeding len
        queue.push(3);
        assert_eq!(queue.len(), 3);
        assert!(queue.exists());
        assert_eq!(queue.pop().unwrap(), 1);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.pop().unwrap(), 2);
        assert_eq!(queue.len(), 1);
        assert!(queue.exists());
        assert_eq!(queue.pop().unwrap(), 3);
        assert_eq!(queue.len(), 0);
        assert!(!queue.exists());
    }
}
