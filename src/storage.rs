use crate::NewEvent;

use std::collections::BTreeSet;
use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use pretty_hex::*;
use uuid::Uuid;

mod page;

use page::*;

const FILE_HEADER: &str = "esdb"; // TODO: Use a meta page instead of this header
const PAGE_HEADER_LEN: u64 = 9;

// NOTES
// =====
//
// Pages
// -----
//
// Open questions:
// - Can overflow pages be used to store multiple "overflows"? If so, how are they layed out?
// - Should we CRC the pages? (https://github.com/zowens/crc32c)
// - How should we "point" to the pages?
//
// Meta Page
// ---------
//
// Open questions:
// - Should we have one?
//
// Ref:
// - https://fadden.com/tech/file-formats.html

#[derive(Clone)]
pub struct Storage {
    path: PathBuf,
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

fn file(path: &PathBuf) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

impl Storage {
    #[allow(dead_code)]
    pub fn create_db(path: PathBuf) -> io::Result<Self> {
        let mut file = match file(&path) {
            Ok(file) => file,
            Err(_) => OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?,
        };

        ensure_header(&mut file)?;

        // TODO: Load these values from the header
        let db = Self {
            path,
            next_page_index: Arc::new(AtomicU64::new(PAGE_SIZE)),
            current_data_page: Arc::new(Mutex::new(None)),
            current_index_page: Arc::new(Mutex::new(None)),
            aggregate_id_aggregate_sequence_unique_index: BTreeSet::new(),
        };

        // TODO: We should do this in case we crashed before last time.
        // db.update_indexes_on_disk()?;

        Ok(db)
    }

    fn file(&self) -> io::Result<File> {
        file(&self.path)
    }

    #[allow(dead_code)]
    fn pages(&mut self) -> io::Result<Vec<PageRef>> {
        let mut file = BufReader::new(self.file()?);

        // Start after the meta page
        let mut pos = PAGE_SIZE; // TODO: Read this from the meta page

        let mut pages = Vec::new();

        loop {
            let mut header_buf = vec![0; PAGE_HEADER_LEN as usize];
            file.seek(io::SeekFrom::Start(pos))?;

            match file.read_exact(&mut header_buf) {
                Err(e) => match e.kind() {
                    ErrorKind::UnexpectedEof => break,
                    _ => return Err(e),
                },
                _ => {}
            }

            let page = PageRef::parse_from_header(&header_buf, pos);
            pos += page.len();
            pages.push(page);
        }

        Ok(pages)
    }

    #[allow(dead_code)]
    pub fn append(&mut self, new_events: Vec<NewEvent>, aggregate_id: Uuid) -> io::Result<()> {
        let mut file = self.file()?;

        let mut current_data_page = self.current_data_page.lock().expect("poisoned");
        // If we don't have a current data page, add a new one
        if !current_data_page.is_some() {
            let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
            *current_data_page = Some(Page::add(&mut file, index, PAGE_SIZE, b'D')?);
        }

        // TODO: Check aggregate_id_aggregate_sequence_unique_index

        // Serialize events
        let mut buffer = Vec::new();
        for event in new_events.iter() {
            let buf = serde_json::to_vec(&event).expect("unable to serialize");
            let len = u16::try_from(buf.len()).expect("event size too large");
            buffer.extend_from_slice(&len.to_be_bytes());
            buffer.extend_from_slice(&buf);
        }

        // Check if the page is full.
        if buffer.len() as u64 > current_data_page.unwrap().free() {
            // TODO: Handle overflowing to multiple pages
            let overflow_page = {
                let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
                Page::add(&mut file, index, PAGE_SIZE, b'O')?
            };

            let mut part_1 = current_data_page.unwrap().free() as usize;
            part_1 -= 8; // Make space for an overflow page pointer

            // Store part 1
            file.seek(io::SeekFrom::Start(current_data_page.unwrap().head))?;
            file.write_all(&buffer[0..part_1])?;

            *current_data_page = None;

            // Store page pointer
            dbg!(&overflow_page.index.to_be_bytes());
            dbg!(overflow_page.index);
            file.write_all(&overflow_page.index.to_be_bytes())?;

            // TODO: Ensure no overflow (again)!
            file.seek(io::SeekFrom::Start(overflow_page.head))?;
            file.write_all(&buffer[part_1..])?;
        } else {
            file.seek(io::SeekFrom::Start(current_data_page.unwrap().head))?;
            file.write_all(&buffer)?;

            if let Some(p) = current_data_page.as_mut() {
                p.head += buffer.len() as u64;
            };
        }

        file.sync_data()?;

        // NOTE: We have _not_ updated the data_pointer on disk (in the header) at this point, so
        // when we start the database server we will need to scan through the storage to ensure we
        // have the latest data_pointer.

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
        let mut file = BufReader::new(self.file()?);

        // Start after the meta page
        let mut pos = PAGE_SIZE; // TODO: Read this from the meta page

        let mut events = Vec::new();

        loop {
            match Page::read(&mut file, pos)? {
                Some(page_data) => match page_data {
                    PageData::Data(data) => {
                        debug_assert_eq!(data.len(), PAGE_SIZE as usize);

                        // Read events from the data page
                        let page_len = data.len();
                        let page_header_len = PAGE_HEADER_LEN as usize;

                        let mut i = page_header_len; // TODO: This skips the header, make this nicer
                        loop {
                            if i >= page_len {
                                break;
                            }

                            let event_len = u16_from_be_bytes(&data[i..(i + 2)]) as usize;
                            i += 2;

                            if event_len == 0 {
                                // There are no more events here.
                                break;
                            }

                            // Check for overflow
                            let event: NewEvent = if i + event_len >= page_len {
                                // Read the rest of the page, minus the last 8 bytes, which contain
                                // the overflow pointer
                                let mut event_data = data[i..page_len - 8].to_vec();
                                let remaining_to_read = event_len - event_data.len();

                                // Read the index of the overflow page
                                let overflow_page_index = u64_from_be_bytes(&data[page_len - 8..]);
                                // Read the overflow page
                                let overflow_page = Page::read(&mut file, overflow_page_index)?
                                    .expect("missing overflow page")
                                    .unwrap_overflow();

                                let mut overflow_event_data =
                                    // Skip the header, then read the remaining data
                                    overflow_page[page_header_len..remaining_to_read + page_header_len].to_vec();

                                event_data.append(&mut overflow_event_data);

                                dbg!((&event_data[0..256]).hex_dump());

                                serde_json::from_slice(&event_data)
                            } else {
                                serde_json::from_slice(&data[i..(i + event_len)])
                            }
                            .expect("unable to deserialize");

                            i += event_len;

                            events.push(event);
                        }

                        pos += page_len as u64;
                    }
                    PageData::Index(data) => pos += data.len() as u64,
                    PageData::Overflow(data) => pos += data.len() as u64,
                },
                None => break,
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
            let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
            *current_index_page = Some(Page::add(&mut file, index, PAGE_SIZE, b'I')?);
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
        let temp = tempdir();
        let mut path = PathBuf::from(temp.path());
        path.push("store.db");

        let mut storage = Storage::create_db(path).unwrap();

        let aggregate_id = Uuid::new_v4();
        let event = NewEvent {
            aggregate_sequence: 1,
            event_type: "foo_bar_longer".to_string(),
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
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE
                },
            ]
        );

        assert_eq!(
            storage.events().unwrap(),
            vec![event.clone(), event.clone()]
        );

        temp.into_path();
    }

    #[test]
    fn test_overflow() {
        let temp = tempdir();
        let mut path = PathBuf::from(temp.path());
        path.push("store.db");

        let mut storage = Storage::create_db(path).unwrap();

        let aggregate_id = Uuid::new_v4();
        let event = NewEvent {
            aggregate_sequence: 1,
            event_type: String::from_utf8(vec![b'X'; PAGE_SIZE as _]).unwrap(),
            body: json!({"foo": "bar"}),
        };

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
                PageRef::Overflow {
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE
                },
                PageRef::Index {
                    index: PAGE_SIZE * 3,
                    length: PAGE_SIZE
                },
            ]
        );

        assert_eq!(storage.events().unwrap(), vec![event.clone()]);

        // Add another large event
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
                PageRef::Overflow {
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE
                },
                PageRef::Index {
                    index: PAGE_SIZE * 3,
                    length: PAGE_SIZE
                },
                PageRef::Data {
                    index: PAGE_SIZE * 4,
                    length: PAGE_SIZE
                },
                PageRef::Overflow {
                    index: PAGE_SIZE * 5,
                    length: PAGE_SIZE
                },
            ]
        );

        assert_eq!(
            storage.events().unwrap(),
            vec![event.clone(), event.clone()]
        );
    }

    fn tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("esdb.")
            .rand_bytes(8)
            .tempdir()
            .expect("unable to create tempdir")
    }
}
