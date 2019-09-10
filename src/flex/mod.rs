//! # Flexible Cache
//!
//! This mod provides the flexible cache, or `Flex`, which uses a
//! specified amount of memory, wraps a file, and trades off nicely
//! between memory consumption and disk IO.
//!
//! Flex feels like an array with holes, and it can evict unneeded
//! pages automatically. It aggressively prefetches, caches, and
//! writes back data.  Also, it wraps the underlying file completely,
//! in order to provide a transparent interface.  It can be considered
//! an advanced replacement of `BufReader` and/or `BufWriter`.
//!
//! This mod is intentionally named "flex" instead of the more general
//! "cache" to avoid confusion.

#![allow(unused)]

mod interval_tree;

use std::io::{Read, Write, Seek};
use std::io::{SeekFrom, Result};

/// Default flex size.  This value is big enough to cover many common
/// cases.
pub const DEFAULT_FLEX_SIZE: usize = 64 * 1024 * 1024;

/// The Flexible cache.  The cache represent the latest view of the file.
///
/// It wraps a `File`, which should be readable and writable.
pub struct Flex<F>
where F: Read + Write + Seek
{
    file: F,

    /// Maximum number of bytes that the cache data can make use of.
    /// This capacity only counts the bytes used for data.  It doesn't
    /// count the size of, say, `Flex` itself or other auxiliary
    /// structures.  Since they tend to be small, this should not be a
    /// problem anyway.
    capacity: usize,
}

impl<F> Flex<F>
where F: Read + Write + Seek
{
    pub fn new(file: F) -> Flex<F> {
        Flex::with_capacity(DEFAULT_FLEX_SIZE, file)
    }

    pub fn with_capacity(capacity: usize, file: F) -> Flex<F> {
        Flex {
            file,
            capacity,
        }
    }
}

impl<F> Read for Flex<F>
where F: Read + Write + Seek
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        unimplemented!()
    }
}


impl<F> Write for Flex<F>
where F: Read + Write + Seek
{
    /// The written data is aggressively cached, until (some portions
    /// of) it is evicted, or `flush()` is called.
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        unimplemented!()
    }

    /// Make sure the dirty data in the cache is flushed to the disk.
    fn flush(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl<F> Seek for Flex<F>
where F: Read + Write + Seek
{
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        unimplemented!()
    }
}

