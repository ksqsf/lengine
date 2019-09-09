//! # Log
//!
//! The log mod provides an abstraction over the underlying log
//! file. Its main purpose is to provide fast random reads.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Result, Seek, SeekFrom, BufReader, BufWriter};
use std::path::Path;

use crate::Offset;

const DEFAULT_READ_BUF_SIZE: usize = 1024;
const DEFAULT_WRITE_BUF_SIZE: usize = 64 * 1024 * 1024;

/// In-memory representation of a log file.
///
/// Reads and writes should not be mixed.
pub struct Log {
    /// The raw file handle.
    file: File,

    /// A buffered reader for the log file.
    reader: BufReader<File>,
}

impl Log {
    /// Open a log file if it exists, or create it if it doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Log> {
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(path)?;
        let reader = BufReader::with_capacity(DEFAULT_READ_BUF_SIZE, file.try_clone()?);
        Ok(Log {
            file,
            reader,
        })
    }

    /// Start a new transaction to append entries to the log file.
    ///
    /// Do not read the log file before the transaction is committed or cancelled.
    pub fn transaction(&mut self) -> Result<Transaction<'_>> {
        let tail = self.file.seek(SeekFrom::End(0))?;
        Ok(Transaction {
            writer: BufWriter::with_capacity(DEFAULT_WRITE_BUF_SIZE, self.file.try_clone()?),
            log: self,
            tail,
        })
    }

    pub fn seek_relative(&mut self, offset: i64) -> Result<()> {
        self.reader.seek_relative(offset)
    }

    /// Try to write the data to the log file, and make sure the
    /// writes do happen.
    fn sync_data(&mut self)  -> Result<()> {
        self.file.sync_data()
    }
}

impl Read for Log {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.reader.read(buf)
    }
}

impl Seek for Log {
    /// This method use be used with care. It will discard the reader
    /// buffer.
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.reader.seek(pos)
    }
}

/// Atomic updates to the log file.
pub struct Transaction<'a> {
    log: &'a mut Log,
    writer: BufWriter<File>,
    tail: u64,
}

impl<'a> Transaction<'a> {
    pub fn append(&mut self, entry: &[u8]) -> Result<Offset> {
        // FIXME: how should we align entries?
        // Note, `seek` will invalidate the buffer.
        let offset = self.tail;
        self.writer.write_all(entry)?;
        self.tail += entry.len() as u64;
        Ok(offset)
    }

    pub fn commit(mut self) -> Result<()> {
        self.writer.flush()?;
        self.log.sync_data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use crate::tests::*;

    #[test]
    fn open() {
        ensure_nonexistent("LOG1");
        {
            Log::open("LOG1").unwrap();
        }
        {
            Log::open("LOG1").unwrap();
        }
        fs::remove_file("LOG1").unwrap();
    }

    #[test]
    fn bulk_append() {
        let filename = "LOG_bulk_append";
        ensure_nonexistent(filename);

        // Data
        let n = 1_000_000;
        let text = b"Just a test?!\n";

        // Append
        let mut log = Log::open(filename).unwrap();
        let mut tx = log.transaction().unwrap();
        let mut offsets = Vec::with_capacity(n);
        for i in 0..n {
            let offset = tx.append(text).unwrap();
            assert_eq!(offset, (i * text.len()) as u64);
            offsets.push(offset);
        }
        tx.commit().unwrap();

        // Re-read
        drop(log);
        let mut log = Log::open(filename).unwrap();
        log.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = [0; 14];
        assert_eq!(text.len(), 14);
        for _ in 0..n {
            log.read_exact(&mut buf).unwrap();
            assert_eq!(&buf[..], &text[..]);
        }

        fs::remove_file(filename).unwrap();
    }
}
