use super::waker::{ThinWaker, WakeResult, WakerItem};
use embed_collections::{
    Pointer,
    dlist::{DLinkedList, DListItem, DListNode},
};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct WakerList(DLinkedList<NonNull<WakerSeg>, ()>);

impl WakerList {
    fn push(&mut self, seq: u32, waker: ThinWaker) -> WakerSegRef {
        if let Some(seg_p) = self.0.get_front() {
            let seg = unsafe { seg_p.as_mut() };
            if !seg.is_full() {
                return seg.push(WakerItem::new(seq, waker));
            }
        }
        let mut seg = WakerSeg::new();
        let seg_ref = seg.push(WakerItem::new(seq, waker));
        self.0.push_back(NonNull::from(seg_ref.as_ref()));
        let _ = Box::leak(seg);
        seg_ref
    }

    fn wake(&mut self) -> WakeResult {}

    fn cancel(&mut self, waker_ref: WakerSegRef) -> Result<(), u8> {
        todo!();
    }
}

pub struct WakerSegRef {
    seg: NonNull<WakerSeg>,
    idx: usize,
}

impl Deref for WakerSegRef {
    type Target = WakerItem;
    fn deref(&self) -> &Self::Target {
        let seg = unsafe { self.seg.as_ref() };
        unsafe { seg.items[self.idx].assume_init_ref() }
    }
}

// 256B
struct WakerSeg {
    node: UnsafeCell<DListNode<Self, ()>>,
    start: usize,
    end: usize,
    left: usize,
    ref_count: AtomicUsize,
    items: [MaybeUninit<WakerItem>; Self::MAX_ITEMS],
}

unsafe impl DListItem<()> for WakerSeg {
    fn get_node(&self) -> &mut DListNode<Self, ()> {
        unsafe { &mut *self.node.get() }
    }
}

impl WakerSeg {
    const MAX_ITEMS: usize = 9;
    #[inline]
    pub fn new() -> Box<Self> {
        Box::new(WakerSeg {
            start: 0,
            end: 0,
            left: 4,
            ref_count: AtomicUsize::new(1),
            node: UnsafeCell::new(DListNode::default()),
        })
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.end == 4
    }

    #[inline(always)]
    pub fn push(&mut self, item: WakerItem) -> WakerSegRef {
        let idx = self.end;
        debug_assert!(idx != 4);
        self.end = idx + 1;
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        unsafe { self.items[idx].write(item) };
        todo!();
    }

    #[inline(always)]
    pub fn wake(&mut self) -> Result<WakeResult, ()> {
        let idx = self.start;
        if idx == 4 {
            return Err(());
        }
        if idx == self.end {
            return Ok(WakeResult::Skip);
        }
        let item = unsafe { self.items[idx].assume_init_ref() };
        self.start = idx + 1;
        Ok(item.wake());
    }

    pub fn cancel(&mut self, waker_ref: &WakerSegRef) -> Result<(), u8> {
        let seg = unsafe { waker_ref.seg.as_mut() };
        if let Err(state) = waker_ref.abandon() {
            return Err(state);
        }
        if seg.end == waker_ref.idx + 1 {
            seg.end -= 1;
            return Ok(());
        } else if seg.start == waker_ref.idx {
            // advance
            seg.start += 1;
            if seg.start == 4 {
                // remove node
            }
        }
        return Ok(());
    }
}
