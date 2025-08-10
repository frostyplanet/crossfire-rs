use std::ptr;
use std::sync::{
    atomic::{AtomicPtr, Ordering},
    Arc, Weak,
};

pub struct ArcCell<T> {
    ptr: AtomicPtr<T>,
}

impl<T> Drop for ArcCell<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

unsafe impl<T> Send for ArcCell<T> {}
unsafe impl<T> Sync for ArcCell<T> {}

impl<T> ArcCell<T> {
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
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            return Some(unsafe { Arc::from_raw(ptr) });
        } else {
            None
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn clear(&self) {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::SeqCst);
        if ptr != ptr::null_mut() {
            // Convert into Weak and drop
            let _ = unsafe { Arc::from_raw(ptr) };
        }
    }

    #[inline(always)]
    pub fn try_put(&self, item: Arc<T>) {
        let item_ptr = Arc::into_raw(item) as *mut T;
        match self.ptr.compare_exchange(
            ptr::null_mut(),
            item_ptr,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => {}
            Err(_) => {
                let _ = unsafe { Arc::from_raw(item_ptr) };
            }
        }
    }
}

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
        if self.ptr.load(Ordering::SeqCst) == ptr::null_mut() {
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
}
