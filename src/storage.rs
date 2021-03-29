use std::collections::BTreeSet;
use std::convert::{TryFrom, TryInto};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufReader, ErrorKind};
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[allow(unused_imports)]
use pretty_hex::*;

mod page;

use page::*;

const FILE_HEADER: &str = "esdb"; // TODO: Use a meta page instead of this header

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
pub struct Storage<T: Indexer + Send + Sync + Clone> {
    path: PathBuf,
    next_page_index: Arc<AtomicU64>,
    current_data_page: Arc<Mutex<Option<Page>>>,
    current_index_page: Arc<Mutex<Option<Page>>>,
    aggregate_id_aggregate_sequence_unique_index: Arc<Mutex<BTreeSet<String>>>,
    indexer: Option<Arc<Mutex<Box<T>>>>,
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

impl<T: 'static> Storage<T>
where
    T: Indexer + Send + Sync + Clone,
{
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
            aggregate_id_aggregate_sequence_unique_index: Arc::new(Mutex::new(BTreeSet::new())),
            indexer: None,
        };

        // TODO: We should do this in case we crashed before last time.
        // db.update_indexes()?;

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

            if let Err(e) = file.read_exact(&mut header_buf) {
                match e.kind() {
                    ErrorKind::UnexpectedEof => break,
                    _ => return Err(e),
                }
            }

            let page = PageRef::parse_from_header(&header_buf, pos);
            pos += page.len();
            pages.push(page);
        }

        Ok(pages)
    }

    pub fn append(&self, new_events: Vec<Vec<u8>>) -> io::Result<()> {
        let mut file = self.file()?;

        let mut current_data_page = self.current_data_page.lock().expect("poisoned");
        // If we don't have a current data page, add a new one
        if !current_data_page.is_some() {
            let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
            *current_data_page = Some(Page::add_data(&mut file, index, PAGE_SIZE)?);
        }

        // TODO: Check aggregate_id_aggregate_sequence_unique_index

        // Serialize events
        let mut buffer = Vec::new();
        for event in new_events.iter() {
            let len = u16::try_from(event.len()).expect("event size too large");
            buffer.extend_from_slice(&len.to_be_bytes());
            buffer.extend_from_slice(&event);
        }

        let mut remaining = &buffer[..];

        if let Some(page) = current_data_page.as_mut() {
            while remaining.len() as u64 > page.free() {
                let mut end = page.free() as usize;
                end -= mem::size_of::<u64>(); // Make space for an overflow page pointer

                let data = &remaining[0..end];

                remaining = &remaining[end..];

                let overflow_index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
                page.write(&mut file, &[data, &overflow_index.to_be_bytes()].concat())?;
                *page = Page::add_data_with_offset(
                    &mut file,
                    overflow_index,
                    PAGE_SIZE,
                    remaining.len() as u64,
                )?;
            }

            page.write(&mut file, &remaining)?;
        }

        file.sync_data()?;

        // NOTE: We have _not_ updated the data_pointer on disk (in the header) at this point, so
        // when we start the database server we will need to scan through the storage to ensure we
        // have the latest data_pointer.

        // for event in new_events {
        //     let key = format!("{}{}", aggregate_id, event.aggregate_sequence);
        //     self.aggregate_id_aggregate_sequence_unique_index
        //         .insert(key);
        // }

        // TODO: Kick off background task to update the indexes + header on the disk.
        let c = self.clone();
        std::thread::spawn(move || {
            let _ = c.update_indexes();
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

    pub fn reserve_key_in_index(&self, index_name: &str, key: String) -> Result<(), ()> {
        let _index = match index_name {
            "aggregate_id_aggregate_sequence_unique_index" => {}
            "event_id_unique_index" => {}
            "sequence_unique_index" => {}
            _ => panic!("invalid index name"),
        };

        // TODO: Actually use different indexes
        let mut index = self
            .aggregate_id_aggregate_sequence_unique_index
            .lock()
            .expect("poisoned");

        // If the set did not have this value present, true is returned
        if index.insert(key) {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn events(&self) -> io::Result<Vec<Vec<u8>>> {
        let mut file = BufReader::new(self.file()?);

        // Start after the meta page
        let mut pos = PAGE_SIZE; // TODO: Read this from the meta page

        let mut events = Vec::new();

        while let Some(page_data) = Page::read(&mut file, pos)? {
            match page_data {
                PageData::Data(data, offset) => {
                    // Read events from the data page
                    let page_len = data.len();
                    let page_header_len = PAGE_HEADER_LEN as usize;

                    // Skip over the header
                    let mut i = page_header_len;

                    // We might have overflow data at the beginning of the page, if so we want to
                    // skip over it
                    i += offset as usize;

                    loop {
                        if i >= page_len {
                            break;
                        }

                        // The first two bytes should contain the length of the event
                        let event_len = u16_from_be_bytes(&data[i..(i + 2)]) as usize;
                        i += 2;

                        if event_len == 0 {
                            // There are no more events here.
                            break;
                        }

                        // Check for overflow
                        let event = if event_len < (page_len - i) {
                            // We're not overflowing
                            data[i..(i + event_len)].to_vec()
                        } else {
                            // We are overflowing

                            // Read the rest of the page, minus the last 8 bytes, which contain
                            // the overflow pointer
                            let mut event_data = data[i..page_len - 8].to_vec();
                            let mut remaining_to_read = event_len - event_data.len();

                            // Read the index of the overflow page
                            let mut overflow_page_index = u64_from_be_bytes(&data[page_len - 8..]);

                            while remaining_to_read > 0 {
                                // Read the overflow page, we don't care about the offset, only the
                                // overflow portion at the beginning
                                let (overflow_data, _offset) =
                                    Page::read(&mut file, overflow_page_index)?
                                        .expect("missing overflow page")
                                        .unwrap_data();

                                let end = if remaining_to_read > overflow_data.len() {
                                    // We're overflowing again, read the next overflow pointer
                                    overflow_page_index =
                                        u64_from_be_bytes(&overflow_data[page_len - 8..]);

                                    // We want to read up to the last 8 bytes
                                    page_len - 8
                                } else {
                                    // We want to read the rest of the event
                                    remaining_to_read + page_header_len
                                };

                                // Skip the header, then read to "end"
                                event_data.extend_from_slice(&overflow_data[page_header_len..end]);

                                remaining_to_read = event_len - event_data.len();
                            }

                            event_data
                        };

                        i += event_len;

                        events.push(event);
                    }

                    pos += page_len as u64;
                }
                PageData::Index(data, _offset) => pos += data.len() as u64,
            }
        }

        Ok(events)
    }

    // https://github.com/wspeirs/btree/blob/master/src/disk_btree.rs
    fn update_indexes(&self) -> io::Result<()> {
        let mut file = self.file()?;

        // Add a new page
        let mut current_index_page = self.current_index_page.lock().expect("poisoned");
        if !current_index_page.is_some() {
            let index = self.next_page_index.fetch_add(PAGE_SIZE, Ordering::SeqCst);
            *current_index_page = Some(Page::add_index(&mut file, index, PAGE_SIZE)?);
        }

        // Find blobs that _may_ be unindexed
        // Call self.indexer.run(unindexed_blobs); to get the UniqueIndexUpdate requests
        // Update the indexes for real-real, if they're not already updated

        // TODO: Build next part of index into buffer
        let buffer = Vec::new();
        file.seek(io::SeekFrom::Start(current_index_page.unwrap().head))?;
        file.write_all(&buffer)?;

        // NOTE: We don't need to do this on each write
        file.sync_data()?;

        Ok(())
    }

    #[allow(dead_code)]
    fn flush(&self) -> io::Result<()> {
        self.update_indexes()
    }
}

/// A request to update a unique index
pub struct UniqueIndexUpdate {
    index_name: String,
    unique_key: String,
}

impl<T: Indexer + Send + Sync + Clone> Storage<T> {
    pub fn set_indexer(&mut self, indexer: T) {
        self.indexer = Some(Arc::new(Mutex::new(Box::new(indexer))));
    }
}

pub trait Indexer {
    fn run(&self, unindexed_blobs: Vec<Vec<u8>>) -> Result<Vec<UniqueIndexUpdate>, ()>;
}

fn u64_from_be_bytes(input: &[u8]) -> u64 {
    u64::from_be_bytes(input.try_into().expect("8 bytes"))
}

fn u16_from_be_bytes(input: &[u8]) -> u16 {
    u16::from_be_bytes(input.try_into().expect("2 bytes"))
}

#[allow(dead_code)]
fn hex_dump(data: &[u8]) {
    eprintln!("{:?}", data.hex_dump());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let (mut storage, _tempdir_guard) = StorageCreator::new();

        storage.append(vec![b"foo".to_vec()]).unwrap();
        storage.append(vec![b"bar".to_vec()]).unwrap();

        storage.flush().unwrap();

        assert_eq!(
            storage.pages().unwrap(),
            vec![
                PageRef::Data {
                    index: PAGE_SIZE,
                    length: PAGE_SIZE,
                    offset: 0,
                },
                PageRef::Index {
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE,
                    offset: 0,
                },
            ]
        );

        assert_eq!(
            storage.events().unwrap(),
            vec![b"foo".to_vec(), b"bar".to_vec()]
        );
    }

    #[test]
    fn test_overflow() {
        let (mut storage, _tempdir_guard) = StorageCreator::new();

        // Page size overflows because the header takes up room
        let overflow_len = PAGE_SIZE as usize;

        storage.append(vec![vec![b'X'; overflow_len]]).unwrap();
        storage.flush().unwrap();

        assert_eq!(storage.events().unwrap(), vec![vec![b'X'; overflow_len]]);
        assert_eq!(
            storage.pages().unwrap(),
            vec![
                PageRef::Data {
                    index: PAGE_SIZE,
                    length: PAGE_SIZE,
                    offset: 0,
                },
                PageRef::Data {
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE,
                    offset: 27,
                },
                PageRef::Index {
                    index: PAGE_SIZE * 3,
                    length: PAGE_SIZE,
                    offset: 0,
                },
            ]
        );

        // Add another large blob
        storage.append(vec![vec![b'Y'; overflow_len]]).unwrap();
        storage.flush().unwrap();

        assert_eq!(
            storage.events().unwrap(),
            vec![vec![b'X'; overflow_len], vec![b'Y'; overflow_len]]
        );
        assert_eq!(
            storage.pages().unwrap(),
            vec![
                PageRef::Data {
                    index: PAGE_SIZE,
                    length: PAGE_SIZE,
                    offset: 0,
                },
                PageRef::Data {
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE,
                    offset: 27,
                },
                PageRef::Index {
                    index: PAGE_SIZE * 3,
                    length: PAGE_SIZE,
                    offset: 0,
                },
                PageRef::Data {
                    index: PAGE_SIZE * 4,
                    length: PAGE_SIZE,
                    offset: 54,
                },
            ]
        );
    }

    #[test]
    fn test_overflow_multiple_pages() {
        let (mut storage, _tempdir_guard) = StorageCreator::new();

        // Page size overflows because the header takes up room
        let overflow_len = PAGE_SIZE as usize * 2;

        storage.append(vec![vec![b'X'; overflow_len]]).unwrap();
        storage.flush().unwrap();

        assert_eq!(storage.events().unwrap(), vec![vec![b'X'; overflow_len]]);
        assert_eq!(
            storage.pages().unwrap(),
            vec![
                PageRef::Data {
                    index: PAGE_SIZE,
                    length: PAGE_SIZE,
                    offset: 0,
                },
                PageRef::Data {
                    index: PAGE_SIZE * 2,
                    length: PAGE_SIZE,
                    offset: 4123,
                },
                PageRef::Data {
                    index: PAGE_SIZE * 3,
                    length: PAGE_SIZE,
                    offset: 52,
                },
                PageRef::Index {
                    index: PAGE_SIZE * 4,
                    length: PAGE_SIZE,
                    offset: 0,
                },
            ]
        );
    }

    // #[test]
    // fn test_update_indexes() {
    //     let (mut storage, _tempdir_guard) = StorageCreator::new::<MyTestIndexer>();
    //
    //     storage.append(vec![b"foo".to_vec()]).unwrap();
    //     storage.append(vec![b"bar".to_vec()]).unwrap();
    //
    //     storage.flush().unwrap();
    //
    //     assert_eq!(
    //         storage.pages().unwrap(),
    //         vec![
    //             PageRef::Data {
    //                 index: PAGE_SIZE,
    //                 length: PAGE_SIZE,
    //                 offset: 0,
    //             },
    //             PageRef::Index {
    //                 index: PAGE_SIZE * 2,
    //                 length: PAGE_SIZE,
    //                 offset: 0,
    //             },
    //         ]
    //     );
    //
    //     assert_eq!(
    //         storage.events().unwrap(),
    //         vec![b"foo".to_vec(), b"bar".to_vec()]
    //     );
    // }

    struct StorageCreator<T = TestIndexer>
    where
        T: Indexer + Send + Sync + Clone;

    impl<T> StorageCreator<T>
    where
        T: Indexer + Send + Sync + Clone,
    {
        fn new() -> (Storage<T>, tempfile::TempDir) {
            let temp = tempdir();
            let mut path = PathBuf::from(temp.path());
            path.push("store.db");

            (Storage::create_db(path).unwrap(), temp)
        }
    }

    fn tempdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("esdb.")
            .rand_bytes(8)
            .tempdir()
            .expect("unable to create tempdir")
    }

    #[derive(Clone)]
    pub struct TestIndexer;

    impl Indexer for TestIndexer {
        fn run(&self, unindexed_blobs: Vec<Vec<u8>>) -> Result<Vec<UniqueIndexUpdate>, ()> {
            Ok(Vec::new())
        }
    }
}
