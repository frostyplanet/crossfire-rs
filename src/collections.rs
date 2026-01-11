use std::ptr;
use std::sync::{
    atomic::{AtomicPtr, Ordering},
    Arc, Weak,
};

pub struct ArcCell<T> {
    ptr: AtomicPtr<T>,
}

impl<T> Drop for ArcCell<T> {
    #[inline]
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

#[allow(dead_code)]
pub struct WeakCell<T> {
    ptr: AtomicPtr<T>,
}

unsafe impl<T> Send for WeakCell<T> {}
unsafe impl<T> Sync for WeakCell<T> {}

impl<T> Drop for WeakCell<T> {
    #[inline]
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
    pub fn is_empty(&self) -> bool {
        self.ptr.load(Ordering::SeqCst).is_null()
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<Arc<T>> {
        let mut v = self.ptr.load(Ordering::SeqCst);
        if v == ptr::null_mut() {
            return None;
        }
        loop {
            match self.ptr.compare_exchange(v, ptr::null_mut(), Ordering::SeqCst, Ordering::Acquire)
            {
                Ok(_) => return unsafe { Weak::from_raw(v) }.upgrade(),
                Err(_v) => {
                    if _v == ptr::null_mut() {
                        return None;
                    }
                    v = _v;
                }
            }
        }
    }

    //// it is allow to fail, with only one shot and weak Ops
    #[inline(always)]
    pub fn clear(&self) -> bool {
        // Don't need acurate, it's optional
        let v = self.ptr.load(Ordering::Acquire);
        if v == ptr::null_mut() {
            return false;
        }
        match self.ptr.compare_exchange(v, ptr::null_mut(), Ordering::Release, Ordering::Relaxed) {
            Ok(_) => {
                let _ = unsafe { Weak::from_raw(v) };
                return true;
            }
            Err(_v) => {
                // We don't really have to clear this on spurious failure
                return false;
            }
        }
    }

    #[inline(always)]
    pub fn replace(&self, item: Weak<T>) {
        let old_ptr = self.ptr.swap(item.into_raw() as *mut T, Ordering::SeqCst);
        if old_ptr != ptr::null_mut() {
            let _ = unsafe { Weak::from_raw(old_ptr) };
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_weak_cell() {
        use super::*;
        use std::sync::Arc;
        let cell = WeakCell::new();
        assert!(cell.is_empty());
        let item = Arc::new(1);
        cell.replace(Arc::downgrade(&item));
        assert!(!cell.is_empty());
        let _item = cell.pop().unwrap();
        assert!(cell.is_empty());
        assert!(Arc::ptr_eq(&item, &_item));
        cell.replace(Arc::downgrade(&item));
        assert!(!cell.is_empty());
        // it is allow to fail under miri
        println!("clear");
        while !cell.clear() {
            assert!(!cell.is_empty());
            println!("try clear again");
        }
        assert!(cell.is_empty());
        drop(_item);
        assert_eq!(Arc::strong_count(&item), 1);
        assert_eq!(Arc::weak_count(&item), 0);
    }
}
