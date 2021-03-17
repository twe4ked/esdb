use crate::storage::u64_from_be_bytes;

use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, ErrorKind};

use pretty_hex::*;

pub const PAGE_SIZE: u64 = 4096;
pub const PAGE_HEADER_LEN: u64 = 17;

/// |======================================|
/// | Page Header                          |
/// |======================================|
/// | D len (BE) | Data page     | 68 0x44 |
/// |------------|---------------|---------|
/// | I len (BE) | Index page    | 73 0x49 |
/// |------------|---------------|---------|
///
/// https://en.wikipedia.org/wiki/Type-length-value
#[derive(Debug, PartialEq)]
pub enum PageRef {
    Data {
        index: u64,
        length: u64,
        offset: u64,
    },
    Index {
        index: u64,
        length: u64,
        offset: u64,
    },
}

impl PageRef {
    pub fn len(&self) -> u64 {
        match self {
            PageRef::Data { length, .. } => *length,
            PageRef::Index { length, .. } => *length,
        }
    }

    pub fn parse_from_header(input: &[u8], index: u64) -> PageRef {
        let len = PAGE_HEADER_LEN as usize;
        debug_assert_eq!(input.len(), len);

        let length = u64_from_be_bytes(&input[1..9]);
        let offset = u64_from_be_bytes(&input[9..17]);

        match input[0] {
            b'D' => PageRef::Data {
                index,
                length,
                offset,
            }, // 68 0x44
            b'I' => PageRef::Index {
                index,
                length,
                offset,
            }, // 73 0x49
            _ => {
                dbg!((&input).hex_dump());
                panic!("invalid page header")
            }
        }
    }
}

#[derive(Debug)]
pub enum PageData {
    Data(Vec<u8>, u64),
    Index(Vec<u8>, u64),
}

impl PageData {
    pub fn unwrap_data(self) -> (Vec<u8>, u64) {
        match self {
            PageData::Data(data, offset) => (data, offset),
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

    pub fn add_data(file: &mut File, index: u64, len: u64) -> io::Result<Self> {
        Self::add_data_with_offset(file, index, len, 0)
    }

    pub fn add_data_with_offset(
        file: &mut File,
        index: u64,
        len: u64,
        offset: u64,
    ) -> io::Result<Self> {
        Self::add(file, index, len, b'D', offset)
    }

    pub fn add_index(file: &mut File, index: u64, len: u64) -> io::Result<Self> {
        Self::add(file, index, len, b'I', 0)
    }

    fn add(file: &mut File, index: u64, len: u64, ident: u8, offset: u64) -> io::Result<Self> {
        file.seek(io::SeekFrom::Start(index))?;

        file.write_all(&[ident])?; // 1
        file.write_all(&len.to_be_bytes())?; // 8
        file.write_all(&offset.to_be_bytes())?; // 8

        debug_assert_eq!(1 + 8 + 8, PAGE_HEADER_LEN);

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
            PageRef::Data { offset, .. } => PageData::Data(buf, offset),
            PageRef::Index { offset, .. } => PageData::Index(buf, offset),
        };

        Ok(Some(page_data))
    }

    pub fn write(&mut self, file: &mut File, buffer: &[u8]) -> io::Result<()> {
        debug_assert!(
            buffer.len() as u64 <= self.free(),
            "buffer to large for page"
        );

        file.seek(io::SeekFrom::Start(self.head))?;
        file.write_all(&buffer)?;

        self.head += buffer.len() as u64;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_page_header() {
        let mut input = b"I".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());
        input.append(&mut 456_u64.to_be_bytes().to_vec());

        assert_eq!(
            PageRef::parse_from_header(&input, 42),
            PageRef::Index {
                index: 42,
                length: 123,
                offset: 456,
            }
        );

        let mut input = b"D".to_vec();
        input.append(&mut 123_u64.to_be_bytes().to_vec());
        input.append(&mut 456_u64.to_be_bytes().to_vec());

        assert_eq!(
            PageRef::parse_from_header(&input, 42),
            PageRef::Data {
                index: 42,
                length: 123,
                offset: 456,
            }
        );
    }
}
