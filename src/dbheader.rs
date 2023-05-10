//! dbheader reads the header of a database file.

use std::io::{Read, Seek, SeekFrom};

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("The magic bytes for this file are wrong.")]
    WrongMagic,
    #[error("A field value is not supported by this code, though it may be valid Sqlite format.")]
    Unsupported,
    #[error("The pagesize is not supported by this code, though it may be valid Sqlite format.")]
    UnsupportedPagesize,
    #[error("A field value specified a free list that is not supported by this code, though it may be valid Sqlite format.")]
    UnsupportedFreelistUse,
    #[error("A field value specified a schema type that is not supported by this code, though it may be valid Sqlite format.")]
    UnsupportedSchema,
    #[error("A field value is invalid per the Sqlite format spec (version 3.0.0).")]
    Invalid,
    #[error("Error reading file.")]
    ReadFailed,
}

// Code to open db files and (in the future) lock the file at the OS level.
//  It also provides a function to get the DB file headers.

// The database file header fields that we return from public interface.
#[derive(Debug, Clone)]
pub struct DbfileHeader {
    pub pagesize: u32,
    pub numpages: u32,
    pub changecnt: u32,
}

// The database file header as stored in a sqlite file.
// All the [u8; 32] fields are 32 bit big-endian unsigned integers.
#[derive(Debug, Clone)]
#[repr(C)]
struct DbfileHeaderReprC {
    // Offset	Size	Description
    // 0        16	The header string: "SQLite format 3\000"
    magic: [u8; 16],
    // 16	2	The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 meaning 65536.
    pagesize: [u8; 2],
    // 18	1	File format write version. 1 for legacy; 2 for WAL.
    ffwv: u8,
    // 19	1	File format read version. 1 for legacy; 2 for WAL.
    ffrv: u8,
    // 20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
    reserved_end: u8,
    // 21	1	Maximum embedded payload fraction. Must be 64.
    maxepf: u8,
    // 22	1	Minimum embedded payload fraction. Must be 32.
    minepf: u8,
    // 23	1	Leaf payload fraction. Must be 32.
    lpf: u8,
    // 24	4	File change counter.
    fcc: [u8; 4],
    // 28	4	Size of the database file in pages. The "in-header database size".
    numpages: [u8; 4],
    // 32	4	Page number of the first freelist trunk page.
    pnfftp: [u8; 4],
    // 36	4	Total number of freelist pages.
    nflp: [u8; 4],
    // 40	4	The schema cookie.
    sc: [u8; 4],
    // 44	4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
    sfn: [u8; 4],
    // 48	4	Default page cache size.
    dpcs: [u8; 4],
    // 52	4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    lrbpv: [u8; 4],
    // 56	4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    encoding: [u8; 4],
    // 60	4	The "user version" as read and set by the user_version pragma.
    userversion: [u8; 4],
    // 64	4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    ivm: [u8; 4],
    // 68	4	The "Application ID" set by PRAGMA application_id.
    appid: [u8; 4],
    // 72	20	Reserved for expansion. Must be zero.
    reserved: [u8; 20],
    // 92	4	The version-valid-for number.
    vvf: [u8; 4],
    // 96	4	SQLITE_VERSION_NUMBER
    sqlite_version_number: [u8; 4],
}

const SQLITE_DB_HEADER_BYTES: usize = 100;
const SQLITE3_MAGIC_STRING: &[u8] = &[
    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00,
];
const TWENTY_ZEROS: &[u8] = &[0; 20];
const SQLITE_VERSION_NUMBER: u32 = 3037000; // This is the one I'm using for generating test files.

fn bytes_identical<T: Ord>(a: &[T], b: &[T]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut iter_b = b.iter();
    for v in a {
        match iter_b.next() {
            Some(w) => {
                if v == w {
                    continue;
                } else {
                    return false;
                }
            }
            None => break,
        }
    }
    true
}

pub fn get_header_clone(f: &mut std::fs::File) -> Result<DbfileHeader, Error> {
    let mut v = [0_u8; SQLITE_DB_HEADER_BYTES];
    f.seek(SeekFrom::Start(0)).map_err(|_| Error::ReadFailed)?;
    f.read_exact(&mut v[..]).map_err(|_| Error::ReadFailed)?;
    get_header(&v)
}

pub fn get_header(h: &[u8; SQLITE_DB_HEADER_BYTES]) -> Result<DbfileHeader, Error> {
    if std::mem::size_of::<[u8; SQLITE_DB_HEADER_BYTES]>() != std::mem::size_of::<DbfileHeaderReprC>() {
        return Err(Error::ReadFailed);
    }
    let hdri = unsafe {
      std::mem::transmute::<[u8; SQLITE_DB_HEADER_BYTES], DbfileHeaderReprC>(*h)
    };
    // The header must have the magic string that identifies the file as a sqlite file.
    if !bytes_identical(&hdri.magic, SQLITE3_MAGIC_STRING) {
        return Err(Error::WrongMagic);
    }
    let pagesize: u32 = match u16::from_be_bytes(hdri.pagesize) {
        512 => 512,
        1024 => 1024,
        2048 => 2048,
        4096 => 4096,
        8192 => 8192,
        16384 => 16384,
        32768 => 32768,
        1 => 65536,
        _ => return Err(Error::UnsupportedPagesize),
    };
    if hdri.ffwv != 0x01 {
        return Err(Error::Unsupported);
    }
    if hdri.ffrv != 0x01 {
        return Err(Error::Unsupported);
    }
    if hdri.reserved_end != 0x00 {
        return Err(Error::Unsupported);
    }
    if hdri.maxepf != 0x40 {
        return Err(Error::Invalid);
    }
    if hdri.minepf != 0x20 {
        return Err(Error::Invalid);
    }
    if hdri.lpf != 0x20 {
        return Err(Error::Invalid);
    }
    let changecnt: u32 = u32::from_be_bytes(hdri.fcc);
    let numpages: u32 = u32::from_be_bytes(hdri.numpages);
    if u32::from_be_bytes(hdri.pnfftp) != 0x0 {
        return Err(Error::UnsupportedFreelistUse);
    }
    if u32::from_be_bytes(hdri.nflp) != 0x0 {
        return Err(Error::UnsupportedFreelistUse);
    }
    let _schema_cookie = u32::from_be_bytes(hdri.sc);
    if u32::from_be_bytes(hdri.sfn) != 0x4 {
        return Err(Error::UnsupportedSchema);
    }
    if u32::from_be_bytes(hdri.dpcs) != 0x0 {
        return Err(Error::Unsupported);
    }
    if u32::from_be_bytes(hdri.lrbpv) != 0x0 {
        return Err(Error::Unsupported);
    }
    if u32::from_be_bytes(hdri.encoding) != 0x1 {
        return Err(Error::Unsupported);
    }
    if u32::from_be_bytes(hdri.userversion) != 0x0 {
        return Err(Error::Unsupported);
    }
    if u32::from_be_bytes(hdri.ivm) != 0x0 {
        return Err(Error::Unsupported);
    }
    if u32::from_be_bytes(hdri.appid) != 0x0 {
        return Err(Error::Unsupported);
    }
    if !bytes_identical(&hdri.reserved, TWENTY_ZEROS) {
        return Err(Error::WrongMagic);
    }
    let _version_valid_for = u32::from_be_bytes(hdri.vvf);
    if u32::from_be_bytes(hdri.sqlite_version_number) != SQLITE_VERSION_NUMBER {
        return Err(Error::Unsupported);
    }

    Ok(DbfileHeader {
        pagesize,
        changecnt,
        numpages,
    })
}
