use crate::NewEvent;

use std::collections::BTreeSet;
use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, ErrorKind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

// TODO: Next thing is to actually store the current pages (data/index) and only add a new page if needed.

const FILE_HEADER: &str = "esdb"; // TODO: Use a meta page instead of this header
const DATABASE_PATH: &str = "store.db";
const PAGE_SIZE: u64 = 256;

/// Page Header
/// ===========
///
/// |-------------------------|
/// | D len (BE) | Data page  |
/// |------------|------------|
/// | I len (BE) | Index page |
/// |-------------------------|
///
/// https://en.wikipedia.org/wiki/Type-length-value
#[derive(Debug, PartialEq)]
enum PageRef {
    Data { index: u64, length: u64 },
    Index { index: u64, length: u64 },
}

impl PageRef {
    fn len(&self) -> u64 {
        match self {
            PageRef::Data { length, .. } => *length,
            PageRef::Index { length, .. } => *length,
        }
    }
}

fn parse_page_header(input: &[u8], index: u64) -> PageRef {
    let length = u64_from_be_bytes(&input[1..9]);

    match input[0] {
        b'D' => PageRef::Data { index, length },
        b'I' => PageRef::Index { index, length },
        _ => panic!("invalid page header"),
    }
}

#[derive(Copy, Clone)]
struct Page {
    /// Where in the file the page lives
    index: u64,
    /// Where we're writing to
    head: u64,
    /// The length of the index
    len: u64,
}

#[derive(Clone)]
pub struct Storage {
    next_page_index: Arc<AtomicU64>,
    current_data_page: Arc<Mutex<Option<Page>>>,
    current_index_page: Arc<Mutex<Option<Page>>>,
    aggregate_id_aggregate_sequence_unique_index: BTreeSet<String>,
}

fn ensure_header(file: &mut File) -> io::Result<()> {
    if file.metadata()?.len() == 0 {
        // Write header
        file.write_all(FILE_HEADER.as_bytes())?;
    } else {
        // Check header
        let mut buf: [u8; FILE_HEADER.len()] = [0; FILE_HEADER.len()];
        file.read_exact(&mut buf)?;

        if buf != FILE_HEADER.as_bytes() {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid header"));
        }
    }

    Ok(())
}

fn file(database_path: &str) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(database_path)
}

impl Storage {
    fn file(&self) -> io::Result<File> {
        file(DATABASE_PATH)
    }

    #[allow(dead_code)]
    pub fn create_db() -> io::Result<Self> {
        let mut file = match file(DATABASE_PATH) {
            Ok(file) => file,
            Err(_) => OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(DATABASE_PATH)?,
        };

        ensure_header(&mut file)?;

        // TODO: Load these values from the header
        let db = Self {
            next_page_index: Arc::new(AtomicU64::new(PAGE_SIZE)),
            current_data_page: Arc::new(Mutex::new(None)),
            current_index_page: Arc::new(Mutex::new(None)),
            aggregate_id_aggregate_sequence_unique_index: BTreeSet::new(),
        };

        // TODO: We should do this in case we crashed before last time.
        // db.update_indexes_on_disk()?;

        Ok(db)
    }

    #[allow(dead_code)]
    fn pages(&mut self) -> io::Result<Vec<PageRef>> {
        let file = self.file()?;
        let mut file = BufReader::new(file);

        // Start after the meta page
        let mut pos = PAGE_SIZE; // TODO: Read this from the meta page

        let mut pages = Vec::new();

        loop {
            let mut header_buf = vec![0; 9];
            file.seek(io::SeekFrom::Start(pos))?;

            match file.read_exact(&mut header_buf) {
                Err(e) => match e.kind() {
                    ErrorKind::UnexpectedEof => break,
                    _ => return Err(e),
                },
                _ => {}
            }

            let page = parse_page_header(&header_buf, pos);
            pos += page.len();
            pages.push(page);
        }

        Ok(pages)
    }

    #[allow(dead_code)]
    pub fn append(&mut self, new_events: Vec<NewEvent>, aggregate_id: Uuid) -> io::Result<()> {
        let mut file = self.file()?;

        // Add a new page
        // TODO: Check if the page is full.
        let mut current_data_page = self.current_data_page.lock().expect("poisoned");
        if !current_data_page.is_some() {
            let index = self.next_page_index.load(Ordering::SeqCst);
            self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);

            file.seek(io::SeekFrom::Start(index))?;

            file.write_all(b"D")?; // 1
            file.write_all(&PAGE_SIZE.to_be_bytes())?; // 8

            let header_len = 1 + 8;

            *current_data_page = Some(Page {
                index: index,
                head: index + header_len,
                len: PAGE_SIZE,
            });
        }

        // Serialize events
        let mut buffer = Vec::new();
        for event in new_events.iter() {
            let buf = serde_json::to_vec(&event).expect("unable to serialize");
            let len = u16::try_from(buf.len()).expect("event size too large");
            buffer.extend_from_slice(&len.to_be_bytes());
            buffer.extend_from_slice(&buf);
        }

        // TODO: Check aggregate_id_aggregate_sequence_unique_index

        file.seek(io::SeekFrom::Start(current_data_page.unwrap().head))?;
        file.write_all(&buffer)?;

        // TODO: Are both of these needed?
        file.flush()?;
        file.sync_all()?;

        // NOTE: We have _not_ updated the data_pointer on disk (in the header) at this point, so
        // when we start the database server we will need to scan through the storage to ensure we
        // have the latest data_pointer.
        *current_data_page = current_data_page.take().map(|mut p| {
            p.head += buffer.len() as u64;
            p
        });

        for event in new_events {
            let key = format!("{}{}", aggregate_id, event.aggregate_sequence);
            self.aggregate_id_aggregate_sequence_unique_index
                .insert(key);
        }

        // TODO: Kick off background task to update the indexes + header on the disk.
        let c = self.clone();
        std::thread::spawn(move || {
            let _ = c.update_indexes_on_disk();
        });

        // Indexes we need:
        //
        //      sequence -> event_row (single)
        //      aggregate_id -> event_rows (multiple)
        //      aggregate_id + aggregate_sequence => unique index

        // For unique indexes, we can:
        //
        //  - Store the value in memory before saving, and lock on it.
        //  - Check if it's used before saving
        //  - Have a background process read new things from that in-memory store and save them
        //
        //  If we crash before saving the indexes, we can replay them when starting back up and
        //  save them to disk before allowing new writes.
        //
        //  Treat the main data store as a write ahead log.
        //
        //  TODO: Where do I put indexes?
        //  TODO: Where do I put the state of where the WAL is up to?
        //
        //      Maybe we can have "state" pages throughout the stream,
        //      they can be linked to from the header section of the stream for quick access?
        //      Or they could be linked-listed together?
        //
        //      Ok, we could have pages of some defined size with a small header before each that
        //      tells us what it is.
        //
        //          Q: What would we do for overflows?
        //
        //      Great thread: https://dba.stackexchange.com/questions/11189/how-do-databases-store-index-key-values-on-disk-for-variable-length-fields
        //
        // TODO: Ensure unique aggregate_sequence + aggregate_id
        // TODO: Ensure unique sequence
        // TODO: Ensure unique event_id
        // TODO: Update indexes?
        // TODO: Update sequence?

        Ok(())
    }

    #[allow(dead_code)]
    pub fn events(&mut self) -> io::Result<Vec<NewEvent>> {
        let file = self.file()?;
        let mut file = BufReader::new(file);

        // Start after the meta page
        let mut pos = PAGE_SIZE; // TODO: Read this from the meta page

        let mut events = Vec::new();

        loop {
            let mut header_buf = vec![0; 9];
            file.seek(io::SeekFrom::Start(pos))?;

            match file.read_exact(&mut header_buf) {
                Err(e) => match e.kind() {
                    ErrorKind::UnexpectedEof => break,
                    _ => return Err(e),
                },
                _ => {}
            }

            let page_ref = parse_page_header(&header_buf, pos);
            pos += page_ref.len();

            match page_ref {
                PageRef::Data { .. } => {
                    // Read events from the data page

                    let page_len = page_ref.len() as usize;

                    let mut buf = vec![0; page_len];
                    file.read_exact(&mut buf)?;

                    let mut i = 0;
                    loop {
                        if i >= page_len {
                            break;
                        }

                        let len = u16_from_be_bytes(&buf[i..(i + 2)]) as usize;
                        i += 2;

                        if len == 0 {
                            // There are no more events here.
                            break;
                        }

                        let event: NewEvent = serde_json::from_slice(&buf[i..(i + len)])
                            .expect("unable to deserialize");
                        i += len;
                        events.push(event);
                    }
                }
                PageRef::Index { .. } => {
                    // Noop
                }
            }
        }

        Ok(events)
    }

    // https://github.com/wspeirs/btree/blob/master/src/disk_btree.rs
    fn update_indexes_on_disk(&self) -> io::Result<()> {
        // NOTE: We don't need to do this on each write, we could batch them.

        let mut file = self.file()?;

        // Add a new page
        // TODO: Check if the page is full.
        let mut current_index_page = self.current_index_page.lock().expect("poisoned");
        if !current_index_page.is_some() {
            let index = self.next_page_index.load(Ordering::SeqCst);
            self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);

            file.seek(io::SeekFrom::Start(index))?;

            file.write_all(b"I")?; // 1
            file.write_all(&PAGE_SIZE.to_be_bytes())?; // 8

            let header_len = 1 + 8;

            *current_index_page = Some(Page {
                index: index,
                head: index + header_len,
                len: PAGE_SIZE,
            });
        }

        // TODO: Build next part of index into buffer
        let buffer = Vec::new();
        file.seek(io::SeekFrom::Start(current_index_page.unwrap().head))?;
        file.write_all(&buffer)?;

        // TODO: Are both of these needed?
        file.flush()?;
        file.sync_all()?;

        Ok(())
    }

    #[allow(dead_code)]
    fn flush(&self) -> io::Result<()> {
        self.update_indexes_on_disk()
    }
}

fn u64_from_be_bytes(input: &[u8]) -> u64 {
    u64::from_be_bytes(input.try_into().expect("8 bytes"))
}

fn u16_from_be_bytes(input: &[u8]) -> u16 {
    u16::from_be_bytes(input.try_into().expect("2 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn basic() {
        let mut storage = Storage::create_db().unwrap();

        let aggregate_id = Uuid::new_v4();
        let event = NewEvent {
            aggregate_sequence: 1,
            event_type: "foo_bar".to_string(),
            body: json!({"foo": "bar"}),
        };

        storage
            .append(vec![event.clone()], aggregate_id.clone())
            .unwrap();
        storage
            .append(vec![event.clone()], aggregate_id.clone())
            .unwrap();

        storage.flush().unwrap();

        assert_eq!(
            storage.pages().unwrap(),
            vec![
                PageRef::Data {
                    index: PAGE_SIZE,
                    length: PAGE_SIZE
                },
                PageRef::Index {
                    index: PAGE_SIZE + PAGE_SIZE,
                    length: PAGE_SIZE
                }
            ]
        );

        assert_eq!(
            storage.events().unwrap(),
            vec![event.clone(), event.clone()]
        );

        // XXX: Try writing events over a page
    }

    #[test]
    fn test_parse_page_header() {
        let mut input = b"I".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());
        input.push(b'\0');

        assert_eq!(
            parse_page_header(&input, 42),
            PageRef::Index {
                index: 42,
                length: 123
            }
        );

        let mut input = b"D".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());
        input.push(b'\0');

        assert_eq!(
            parse_page_header(&input, 42),
            PageRef::Data {
                index: 42,
                length: 123
            }
        );
    }
}
