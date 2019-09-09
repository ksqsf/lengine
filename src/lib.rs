#![feature(bufreader_seek_relative)]

pub type RowId = u32;
pub type Offset = u64;

pub mod index;
pub mod log;
pub mod engine;

pub use crate::index::Index;
pub use crate::log::Log;
pub use crate::engine::Engine;
pub use crate::engine::Transaction;

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::ErrorKind;
    use std::path::Path;

    /// Almost the same as `std::fs::remove_file`, but it doesn't
    /// panic if the target does not exist.
    pub fn ensure_nonexistent<P>(path: P)
    where
        P: AsRef<Path>
    {
        match fs::remove_file(path.as_ref()) {
            Ok(_) => (),
            Err(ref e) if e.kind() == ErrorKind::NotFound => (),
            Err(e) => Err(e).unwrap(),
        }
    }

    /// Almost the same as `std::fs::remove_dir_all`, but it doesn't
    /// panic if the target does not exist.
    pub fn ensure_dir_nonexistent<P>(path: P)
    where
        P: AsRef<Path>
    {
        match fs::remove_dir_all(path.as_ref()) {
            Ok(_) => (),
            Err(ref e) if e.kind() == ErrorKind::NotFound => (),
            Err(e) => Err(e).unwrap(),
        }
    }
}
