use byteorder::{BigEndian, ReadBytesExt};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

pub struct DbAttachment {
    pub f: std::fs::File,
}

// TODO: consider whether the Error types should be "per-architectural layer" or common to all methods in the DB.
#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("The magic bytes for this file are wrong.")]
    WrongMagic,
    #[error("A field value is not supported by this code, though it may be valid Sqlite format.")]
    Unsupported,
    #[error("A field value is invalid per the Sqlite format spec (version 3.0.0).")]
    Invalid,
    #[error("Error reading file.")]
    ReadFailed,
    #[error("Error opening db file file.")]
    OpenFailed,
}

// Code to open db files and (in the future) lock the file at the OS level.
//  It also provides a function to get the DB file headers.

// TODO: consider moving the header reader to use the pager interface so header accesses can use locks.
// That might mean that it doesn't use the Reader/BufReader interface, (uses Page) and so it won't have to return ReadFailed?

// TODO: move db file header code to vfs.rs
// The database file header.
#[derive(Debug, Clone)]
pub struct DbfileHeader {
    pub pagesize: u32,
    pub numpages: u32,
    pub changecnt: u32,
}

const SQLITE3_MAGIC_STRING: &[u8] = &[
    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74, 0x20, 0x33, 0x00,
];
const TWENTY_ZEROS: &[u8] = &[0; 20];
const SQLITE_VERSION_NUMBER: u32 = 3037000; // This is the one I'm using for generating test files.

// Sqlite supports different page sizes, but we are just going to support the default.
// TODO: consolidate multiple definitions of this constant in other modules.
const PAGESIZE: u32 = 4096;

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

fn get_header<R: Read + Seek>(f: &mut R) -> Result<DbfileHeader, Error> {
    f.seek(SeekFrom::Start(0)).unwrap();
    // Offset	Size	Description
    // 0        16	    The header string: "SQLite format 3\000"

    let mut fileid_buffer = [0; 16];
    f.read_exact(&mut fileid_buffer)
        .map_err(|_| Error::ReadFailed)?;
    if !bytes_identical(&fileid_buffer, SQLITE3_MAGIC_STRING) {
        return Err(Error::WrongMagic);
    }
    // Offset	Size	Description
    // 16	    2	    The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    let pagesize: u32 = match f.read_u16::<BigEndian>().unwrap() {
        1 => 1,
        x => x as u32,
    };
    if pagesize != PAGESIZE {
        return Err(Error::Unsupported);
    }
    // Offset	Size	Description
    // 18	    1	    File format write version. 1 for legacy; 2 for WAL.
    // 19	    1	    File format read version. 1 for legacy; 2 for WAL.
    if f.read_u8().map_err(|_| Error::ReadFailed)? != 0x01 {
        return Err(Error::Unsupported);
    }
    if f.read_u8().map_err(|_| Error::ReadFailed)? != 0x01 {
        return Err(Error::Unsupported);
    }

    // Offset	Size	Description
    // 20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
    // 21	1	Maximum embedded payload fraction. Must be 64.
    // 22	1	Minimum embedded payload fraction. Must be 32.
    // 23	1	Leaf payload fraction. Must be 32.
    if f.read_u8().map_err(|_| Error::ReadFailed)? != 0x00 {
        return Err(Error::Unsupported);
    }
    if f.read_u8().map_err(|_| Error::ReadFailed)? != 0x40 {
        return Err(Error::Invalid);
    }
    if f.read_u8().map_err(|_| Error::ReadFailed)? != 0x20 {
        return Err(Error::Invalid);
    }
    if f.read_u8().map_err(|_| Error::ReadFailed)? != 0x20 {
        return Err(Error::Invalid);
    }

    // Offset	Size	Description
    // 24	    4	    File change counter.
    // 28	    4	    Size of the database file in pages. The "in-header database size".
    let changecnt: u32 = f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)?;
    let numpages: u32 = f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)?;

    // Offset	Size	Description
    // 32	    4	    Page number of the first freelist trunk page.
    // 36	    4	    Total number of freelist pages.
    // 40	    4	    The schema cookie.
    // 44	    4	    The schema format number. Supported schema formats are 1, 2, 3, and 4.
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x1 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x4 {
        return Err(Error::Unsupported);
    }

    // Offset	Size	Description
    // 48	    4	    Default page cache size.
    // 52	    4	    The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    // 56	    4	    The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    // 60	    4	    The "user version" as read and set by the user_version pragma.
    // 64	    4	    True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    // 68	    4	    The "Application ID" set by PRAGMA application_id.
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x1 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x0 {
        return Err(Error::Unsupported);
    }

    // Offset	Size	Description
    // 72	20	Reserved for expansion. Must be zero.
    let mut reserved_buffer = [0; 20];
    f.read_exact(&mut reserved_buffer)
        .expect("Should have read the 20 byte header.");
    if !bytes_identical(&reserved_buffer, TWENTY_ZEROS) {
        return Err(Error::WrongMagic);
    }

    // Offset	Size	Description
    // 92	4	The version-valid-for number.
    // 96	4	SQLITE_VERSION_NUMBER
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != 0x1 {
        return Err(Error::Unsupported);
    }
    if f.read_u32::<BigEndian>().map_err(|_| Error::ReadFailed)? != SQLITE_VERSION_NUMBER {
        return Err(Error::Unsupported);
    }

    f.seek(SeekFrom::Start(0)).unwrap();
    Ok(DbfileHeader {
        pagesize: pagesize,
        changecnt: changecnt,
        numpages: numpages,
    })
}

impl DbAttachment {
    // TODO: allow filename to be specified.
    // Why does std::fs::File::open use AsRef<Path> trait on the filename argument?
    // TODO: return errors
    // Is it better to use the Box<dyn Error> pattern or
    // to define a new error type in this crate and map the std::fs errors to it?
    pub fn open() -> Result<DbAttachment, Error> {
        // TODO: Lock file when opening so that other processes do not also
        // open and modify it, and so that is not modified while reading.
        let result = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open("./test.db");
        match result {
            Ok(ff) => Ok(DbAttachment { f: ff }),
            Err(_) => Err(Error::OpenFailed),
        }
    }

    pub fn get_header(&mut self) -> Result<DbfileHeader, Error> {
        get_header(&mut self.f)
    }
}
