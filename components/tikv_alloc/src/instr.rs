use std::alloc::{GlobalAlloc, Layout};

use super::REAL_GLOBAL_ALLOC;

pub unsafe fn alloc(layout: Layout) -> *mut u8 {
    REAL_GLOBAL_ALLOC.alloc(layout)
}

pub unsafe fn dealloc(ptr: *mut u8, layout: Layout) {
    REAL_GLOBAL_ALLOC.dealloc(ptr, layout)
}

pub unsafe fn alloc_zeroed(layout: Layout) -> *mut u8 {
    REAL_GLOBAL_ALLOC.alloc_zeroed(layout)
}

pub unsafe fn realloc(
    ptr: *mut u8, 
    layout: Layout, 
    new_size: usize
) -> *mut u8 {
    REAL_GLOBAL_ALLOC.realloc(ptr, layout, new_size)
}


