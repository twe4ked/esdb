use crate::storage::u64_from_be_bytes;

use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, ErrorKind};

pub const PAGE_SIZE: u64 = 4096;
const PAGE_HEADER_LEN: u64 = 9;

/// |======================================|
/// | Page Header                          |
/// |======================================|
/// | D len (BE) | Data page     | 68 0x44 |
/// |------------|---------------|---------|
/// | I len (BE) | Index page    | 73 0x49 |
/// |------------|---------------|---------|
/// | O len (BE) | Overflow page | 79 0x4f |
/// |--------------------------------------|
///
/// https://en.wikipedia.org/wiki/Type-length-value
#[derive(Debug, PartialEq)]
pub enum PageRef {
    Data { index: u64, length: u64 },
    Index { index: u64, length: u64 },
    Overflow { index: u64, length: u64 },
}

impl PageRef {
    pub fn len(&self) -> u64 {
        match self {
            PageRef::Data { length, .. } => *length,
            PageRef::Index { length, .. } => *length,
            PageRef::Overflow { length, .. } => *length,
        }
    }

    pub fn parse_from_header(input: &[u8], index: u64) -> PageRef {
        let len = PAGE_HEADER_LEN as usize;
        debug_assert_eq!(input.len(), len);

        let length = u64_from_be_bytes(&input[1..len]);

        match input[0] {
            b'D' => PageRef::Data { index, length },     // 68 0x44
            b'I' => PageRef::Index { index, length },    // 73 0x49
            b'O' => PageRef::Overflow { index, length }, // 79 0x4f
            _ => panic!("invalid page header"),
        }
    }
}

#[derive(Debug)]
pub enum PageData {
    Data(Vec<u8>),
    Index(Vec<u8>),
    Overflow(Vec<u8>),
}

impl PageData {
    pub fn unwrap_overflow(self) -> Vec<u8> {
        match self {
            PageData::Overflow(data) => data,
            _ => panic!("not an overflow page: {:?}", self),
        }
    }
}

#[derive(Copy, Clone)]
/// Represents a page for knowing where to write to.
pub struct Page {
    /// Where in the file the page lives
    pub index: u64,
    /// Where we're writing to
    pub head: u64,
    /// The length of the index
    pub len: u64,
}

impl Page {
    pub fn free(&self) -> u64 {
        let used = self.head - self.index;
        self.len - used
    }

    pub fn add(file: &mut File, index: u64, len: u64, ident: u8) -> io::Result<Self> {
        file.seek(io::SeekFrom::Start(index))?;

        file.write_all(&[ident])?; // 1
        file.write_all(&len.to_be_bytes())?; // 8

        debug_assert_eq!(1 + 8, PAGE_HEADER_LEN);

        Ok(Self {
            index,
            head: index + PAGE_HEADER_LEN,
            len,
        })
    }

    pub fn read(file: &mut BufReader<File>, pos: u64) -> io::Result<Option<PageData>> {
        let mut header_buf = vec![0; PAGE_HEADER_LEN as usize];

        file.seek(io::SeekFrom::Start(pos))?;
        if let Err(e) = file.read_exact(&mut header_buf) {
            match e.kind() {
                ErrorKind::UnexpectedEof => return Ok(None),
                _ => return Err(e),
            }
        }

        let page_ref = PageRef::parse_from_header(&header_buf, pos);
        let mut buf = Vec::new();

        // Start at the beginning again so that we include the header in the page
        file.seek(io::SeekFrom::Start(pos))?;
        file.take(page_ref.len()).read_to_end(&mut buf)?;

        let page_data = match page_ref {
            PageRef::Data { .. } => PageData::Data(buf),
            PageRef::Index { .. } => PageData::Index(buf),
            PageRef::Overflow { .. } => PageData::Overflow(buf),
        };

        Ok(Some(page_data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_page_header() {
        let mut input = b"I".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());

        assert_eq!(
            PageRef::parse_from_header(&input, 42),
            PageRef::Index {
                index: 42,
                length: 123
            }
        );

        let mut input = b"D".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());

        assert_eq!(
            PageRef::parse_from_header(&input, 42),
            PageRef::Data {
                index: 42,
                length: 123
            }
        );
    }
}
