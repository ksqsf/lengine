//! # Index

use std::fs::{OpenOptions, File};
use std::path::Path;
use std::io::{Result, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::convert::TryInto;

use byteorder::LittleEndian;
use byteorder::{ByteOrder, ReadBytesExt};

use crate::{RowId, Offset};

/// In-memory representation of an index file.
// FIXME: The size of `map` should have an upper bound. Or just make it a cache.
pub struct Index {
    file: File,
    map: Vec<Offset>,
    next_row: RowId,
}

impl Index {
    /// This function opens an existing index file, or creates one if
    /// it does not exist yet.
    ///
    /// The file will be read to populate the in-memory index.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Index> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        // Make sure the types match.  This can generate a compile-time error.
        unsafe {
            debug_assert!(std::mem::transmute::<Offset, u64>(0) == 0);
        }

        // FIXME: Inefficient.
        let len: usize = file.seek(SeekFrom::End(0))?.try_into().unwrap();
        let n_rows = len / size_of::<Offset>();
        file.seek(SeekFrom::Start(0))?;

        let mut map = Vec::with_capacity(n_rows);
        map.resize(n_rows, Default::default());

        file.read_u64_into::<LittleEndian>(&mut map[0..n_rows])?;

        Ok(Index {
            file,
            map,
            next_row: n_rows.try_into().unwrap(),
        })
    }

    /// Look up a row ID, and get its offset in the log file.
    ///
    /// Currently, it never fails.
    pub fn get(&self, row: RowId) -> Result<Option<Offset>> {
        Ok(self.map.get(row as usize).cloned())
    }

    /// Start a new transaction to append a bunch of new RowId-Offset
    /// pairs to the index file.
    ///
    /// During a transaction, it's impossible to look up row IDs.
    pub fn transaction(&mut self) -> Transaction<'_> {
        Transaction {
            old_len: self.map.len(),
            index: self,
        }
    }

    /// Get the row ID for the next entry.
    pub fn next_row(&self) -> RowId {
        self.next_row
    }

    pub fn reset(&mut self) -> Result<()> {
        self.next_row = 0;
        Ok(())
    }

    /// Try to write the updated index into the file.
    fn sync_data(&mut self, old_len: usize) -> Result<()> {
        self.file.seek(SeekFrom::End(0))?;
        let n = self.map.len() - old_len;

        // FIXME: Inefficient.
        let mut buf = Vec::new();
        buf.resize(n * size_of::<Offset>(), Default::default());
        LittleEndian::write_u64_into(&self.map[old_len..], buf.as_mut_slice());
        self.file.write_all(buf.as_ref())?;
        self.file.sync_data()?;

        Ok(())
    }
}

/// An atomic update to Index.
pub struct Transaction<'idx> {
    index: &'idx mut Index,
    old_len: usize,
}

impl<'idx> Transaction<'idx> {
    /// Append a new offset into the index, and get a new row ID.
    pub fn append(&mut self, offset: Offset) -> RowId {
        let row = self.index.next_row;
        self.index.map.push(offset);
        self.index.next_row += 1;
        row
    }

    /// Commit the updates to the index file.
    pub fn commit(self) -> Result<()> {
        self.index.sync_data(self.old_len)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use crate::tests::*;

    #[test]
    fn open() {
        {
            ensure_nonexistent("IDX1");
            Index::open("IDX1").unwrap();
        }
        {
            Index::open("IDX1").unwrap();
        }
        fs::remove_file("IDX1").unwrap();
    }

    #[test]
    fn transaction() {
        ensure_nonexistent("IDX2");
        let row;
        {
            let mut idx = Index::open("IDX2").unwrap();
            let mut tx = idx.transaction();
            row = tx.append(12321);
            tx.commit().unwrap();
        }
        {
            let idx = Index::open("IDX2").unwrap();
            assert_eq!(idx.get(row).unwrap().unwrap(), 12321);
        }
        fs::remove_file("IDX2").unwrap();
    }

    #[test]
    fn bulk_append() {
        ensure_nonexistent("IDX3");

        // Generate data
        let n = 1_000_000;
        let mut offsets = Vec::with_capacity(n);
        for i in 0..n {
            offsets.push(i as Offset);
        }

        // Bulk append
        let mut idx = Index::open("IDX3").unwrap();
        let mut tx = idx.transaction();
        let rowids: Vec<_> = offsets.iter()
            .map(|&x| tx.append(x))
            .collect();
        tx.commit().unwrap();

        // Instantly verify
        rowids.iter().map(|&x| idx.get(x).unwrap().unwrap())
            .zip(&offsets)
            .for_each(|(a, &b)| assert_eq!(a, b));

        // Re-read and verify
        drop(idx);
        let idx = Index::open("IDX3").unwrap();
        rowids.iter().map(|&x| idx.get(x).unwrap().unwrap())
            .zip(&offsets)
            .for_each(|(a, &b)| assert_eq!(a, b));

        fs::remove_file("IDX3").unwrap();
    }
}
