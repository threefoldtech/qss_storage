use std::mem;

/// Size of a `usize` in bytes
pub const PTR_SIZE: usize = mem::size_of::<usize>();
