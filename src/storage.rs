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

const FILE_HEADER: &str = "esdb"; // TODO: Use a meta page instead of this header
const PAGE_SIZE: u64 = 4096;

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
    Overflow { index: u64, length: u64 },
}

#[derive(Debug)]
enum PageData {
    Data(Vec<u8>),
    Index(Vec<u8>),
    Overflow(Vec<u8>),
}

impl PageData {
    fn unwrap_overflow(self) -> Vec<u8> {
        match self {
            PageData::Overflow(data) => data,
            _ => panic!("not an overflow page: {:?}", self),
        }
    }
}

impl PageRef {
    fn len(&self) -> u64 {
        match self {
            PageRef::Data { length, .. } => *length,
            PageRef::Index { length, .. } => *length,
            PageRef::Overflow { length, .. } => *length,
        }
    }
}

fn parse_page_header(input: &[u8], index: u64) -> PageRef {
    let length = u64_from_be_bytes(&input[1..9]);

    match input[0] {
        b'D' => PageRef::Data { index, length },     // 68 0x44
        b'I' => PageRef::Index { index, length },    // 73 0x49
        b'O' => PageRef::Overflow { index, length }, // 79 0x4f
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

impl Page {
    fn free(&self) -> u64 {
        let used = self.head - self.index;
        self.len - used
    }

    fn add(file: &mut File, index: u64, len: u64, ident: u8) -> io::Result<Self> {
        file.seek(io::SeekFrom::Start(index))?;

        file.write_all(&[ident])?; // 1
        file.write_all(&len.to_be_bytes())?; // 8

        let header_len = 1 + 8;

        Ok(Self {
            index,
            head: index + header_len,
            len,
        })
    }

    fn read(file: &mut BufReader<File>, pos: u64) -> io::Result<Option<PageData>> {
        let header_len = 9;
        let mut header_buf = vec![0; header_len];

        file.seek(io::SeekFrom::Start(pos))?;
        match file.read_exact(&mut header_buf) {
            Err(e) => match e.kind() {
                ErrorKind::UnexpectedEof => return Ok(None),
                _ => return Err(e),
            },
            _ => {}
        }

        let page_ref = parse_page_header(&header_buf, pos);
        let mut buf = Vec::new();
        // Start at the beginning again
        file.seek(io::SeekFrom::Start(pos))?;
        file.take(page_ref.len()).read_to_end(&mut buf)?;

        // The page isn't always full, should it be?
        // debug_assert_eq!(buf.len(), page_ref.len() as usize);

        let page_data = match page_ref {
            PageRef::Data { .. } => PageData::Data(buf),
            PageRef::Index { .. } => PageData::Index(buf),
            PageRef::Overflow { .. } => PageData::Overflow(buf),
        };

        Ok(Some(page_data))
    }
}

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

    #[allow(dead_code)]
    fn pages(&mut self) -> io::Result<Vec<PageRef>> {
        let file = file(&self.path)?;
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
        let mut file = file(&self.path)?;

        let mut current_data_page = self.current_data_page.lock().expect("poisoned");
        // If we've never had a page, add the initial data page
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
            // TODO: Split buffer
            // TODO: Add overflow page the size of the remaining space required.

            let overflow_page = {
                let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
                let p = Page::add(&mut file, index, PAGE_SIZE, b'O')?;

                // {
                //     // TODO: This is a hack to ensure there is data after the new overflow page so
                //     // we can read_exact in Page::read.
                //     let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
                //     Page::add(&mut file, index, PAGE_SIZE, b'O')?;
                // }

                p
            };

            let mut part_1 = current_data_page.unwrap().free() as usize;
            part_1 -= 8; // Make space for an overflow page pointer

            // Store part 1
            file.seek(io::SeekFrom::Start(current_data_page.unwrap().head))?;
            file.write_all(&buffer[0..part_1])?;

            // Store page pointer
            dbg!(&overflow_page.index.to_be_bytes());
            dbg!(overflow_page.index);
            file.write_all(&overflow_page.index.to_be_bytes())?;

            // TODO: Ensure no overflow (again)!
            file.seek(io::SeekFrom::Start(overflow_page.head))?;
            file.write_all(&buffer[part_1..])?;

            // file.seek(io::SeekFrom::Start(overflow_page.index))?;
            // let mut buf = Vec::new();
            // file.read_to_end(&mut buf)?;
            // dbg!(buf);

            {
                let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
                *current_data_page = Some(Page::add(&mut file, index, PAGE_SIZE, b'D')?);
            }
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
        let file = file(&self.path)?;
        let mut file = BufReader::new(file);

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

                        let mut i = 9; // TODO: This skips the header, make this nicer
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
                                    overflow_page[9..remaining_to_read + 9].to_vec();

                                // println!("{:?}", overflow_event_data.hex_dump());

                                event_data.append(&mut overflow_event_data);

                                // println!("{:?}", event_data.hex_dump());

                                serde_json::from_slice(&event_data)
                            } else {
                                serde_json::from_slice(&data[i..(i + event_len)])
                            }
                            .expect("unable to deserialize");
                            dbg!(&event);

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

        let mut file = file(&self.path)?;

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
                PageRef::Data {
                    index: PAGE_SIZE * 3,
                    length: PAGE_SIZE
                },
                PageRef::Index {
                    index: PAGE_SIZE * 4,
                    length: PAGE_SIZE
                },
            ]
        );

        assert_eq!(storage.events().unwrap(), vec![event]);
    }

    #[test]
    fn test_parse_page_header() {
        let mut input = b"I".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());

        assert_eq!(
            parse_page_header(&input, 42),
            PageRef::Index {
                index: 42,
                length: 123
            }
        );

        let mut input = b"D".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());

        assert_eq!(
            parse_page_header(&input, 42),
            PageRef::Data {
                index: 42,
                length: 123
            }
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
