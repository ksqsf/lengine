use std::io::{Result, Error, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::fs;
use std::cell::Cell;
use std::collections::HashMap;
use libc::ENOTDIR;


use crate::{Index, Log, RowId, Offset};
use crate::index::Transaction as IndexTx;
use crate::log::Transaction as LogTx;

/// The log engine.
pub struct Engine {
    index: Index,
    log: Log,

    info: HashMap<String, Vec<u8>>,

    last_row: Cell<RowId>,
    last_offset: Cell<Offset>,
}

impl Engine {
    /// Open an existing log repository, or create it if it doesn't
    /// exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Engine> {
        // Check if path exists.
        if Path::exists(path.as_ref()) {
            // Check if the target is directory.
            let metadata = fs::metadata(path.as_ref())?;
            if !metadata.is_dir() {
                return Err(Error::from_raw_os_error(ENOTDIR))
            }
        } else {
            fs::create_dir(path.as_ref())?;
        }

        // TODO: Lock the db.

        // Open the index file.
        let index_path = {
            let mut buf = PathBuf::from(path.as_ref());
            buf.push("IDX0");
            buf
        };
        let index = Index::open(&index_path)?;

        // Open the log file.
        let log_path = {
            let mut buf = PathBuf::from(path.as_ref());
            buf.push("LOG0");
            buf
        };
        let log = Log::open(&log_path)?;

        Ok(Engine {
            index,
            log,
            info: HashMap::new(),
            last_row: Cell::new(0),
            last_offset: Cell::new(0),
        })
    }

    /// Get an entry from the log file.
    ///
    /// Precisely, this method just reads the log file at a certain
    /// offset.
    pub fn get(&mut self, row: RowId, size: usize) -> Result<Option<Vec<u8>>> {
        match self.index.get(row)? {
            Some(offset) => {
                let mut buf = Vec::with_capacity(size);
                unsafe { buf.set_len(size); }

                // FIXME: `get` should not take &mut self, but seek
                // and read require that.

                // FIXME: this optimization is too simple.

                // If this read is sequential, don't seek.
                if row != self.last_row.get() + 1 {
                    self.log.seek(SeekFrom::Start(offset))?;
                }
                self.log.read(&mut buf[..])?;

                self.last_row.set(row);
                self.last_offset.set(offset);
                return Ok(Some(buf))
            }
            None => Ok(None),
        }
    }

    /// Get the next row ID.
    pub fn next_row(&self) -> RowId {
        self.index.next_row()
    }

    /// Number of existing entries.
    pub fn count(&self) -> usize {
        self.next_row() as usize
    }

    /// Start a transaction.
    pub fn transaction(&mut self) -> Result<Transaction> {
        Ok(Transaction {
            log_tx: self.log.transaction()?,
            index_tx: self.index.transaction(),
            info: &mut self.info,
            info_updates: HashMap::new(),
        })
    }

    /// Get extra info.
    pub fn info(&mut self) -> &mut HashMap<String, Vec<u8>> {
        &mut self.info
    }
}

pub struct Transaction<'a> {
    log_tx: LogTx<'a>,
    index_tx: IndexTx<'a>,
    info: &'a mut HashMap<String, Vec<u8>>,
    info_updates: HashMap<String, Vec<u8>>,
}

impl<'a> Transaction<'a> {
    pub fn append(&mut self, entry: &[u8]) -> Result<RowId> {
        Ok(self.index_tx.append(self.log_tx.append(entry)?))
    }

    pub fn put_info(&mut self, key: String, value: Vec<u8>) {
        self.info_updates.insert(key, value);
    }

    pub fn commit(self) -> Result<()> {
        self.log_tx.commit()?;
        self.index_tx.commit()?;
        for (k, v) in self.info_updates {
            self.info.insert(k, v);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;

    #[test]
    fn open() {
        ensure_dir_nonexistent("dbopen");
        {
            Engine::open("dbopen").unwrap();
        }
        {
            Engine::open("dbopen").unwrap();
        }
        fs::remove_dir_all("dbopen").unwrap();
    }

    #[test]
    fn bulk_append() {
        ensure_dir_nonexistent("db1");

        // Data
        let n = 1_000_000;
        let text = b"yuck".as_ref();
        let text_len = text.len();

        // Write data
        let mut engine = Engine::open("db1").unwrap();
        let mut tx = engine.transaction().unwrap();
        let mut rows = vec![];
        for i in 0..n {
            let row = tx.append(text).unwrap();
            rows.push(row);
            assert_eq!(row, i);
        }
        tx.commit().unwrap();

        // Instant verification
        for row in &rows {
            let data = engine.get(*row, text_len).unwrap().unwrap();
            assert_eq!(data.as_slice(), text);
        }

        // Re-read
        drop(engine);
        let mut engine = Engine::open("db1").unwrap();
        assert_eq!(engine.count(), rows.len());
        for i in 0..engine.count() {
            let data = engine.get(i as u32, text_len).unwrap().unwrap();
            assert_eq!(data.as_slice(), text);
        }

        fs::remove_dir_all("db1").unwrap();
    }
}
