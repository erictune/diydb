// The pager owns the data in each page, and allows callers to access it for reading or writing.
// Goal is to avoid copying pages.
// Pages are loaded on demand.
// All pages have the same size.

use std::io::{Read, Seek, SeekFrom};

pub struct Pager {
    vfs: crate::vfs::DbAttachment,
    pages: Vec<Option<Vec<u8>>>,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("The page number is higher than the file contains or the code supports.")]
    PageNumberBeyondLimits,
    #[error("Error reading file.")]
    ReadFailed,
    #[error("Internal error.")]
    InternalError,
}

// Page numbers are 1-based, to match how Sqlite numbers pages.  PageNum ensures people pass something that is meant to be a page number
// to a function that expects a page number.
pub type PageNum = usize;

// TODO: support variable page sizes, using the page size specified in the DB.
const ASSUMED_PAGE_SIZE: usize = 4096;

// TODO: support databases with more on-disk pages, limiting memory usage by paging out unused pages.
const MAX_PAGE_NUM: PageNum = 10_000; // 10_000 * 4k page ~= 40MB

impl Pager {
    pub fn new(vfs: crate::vfs::DbAttachment) -> Self {
        Pager {
            vfs: vfs,
            pages: vec![],
        }
        // TODO: get the header and check that the number of pages in the DB is less than the maximum number of pages allowed.
    }

    #[allow(dead_code)]
    fn alloc_new_page(self) -> PageNum {
        // TODO: to support writes, need to allocate new pages: write to the database header to increase the page count.
        unimplemented!()
    }

    fn read_page_from_vfs(
        vfs: &mut crate::vfs::DbAttachment,
        pn: PageNum,
    ) -> Result<Vec<u8>, Error> {
        let mut v = vec![0_u8; ASSUMED_PAGE_SIZE];
        vfs.f
            .seek(SeekFrom::Start((pn - 1) as u64 * ASSUMED_PAGE_SIZE as u64))
            .unwrap();
        match vfs.f.read_exact(&mut v[..]).map_err(|_| Error::ReadFailed) {
            Ok(()) => Ok(v),
            Err(e) => Err(e),
        }
    }

    fn ensure_present(&mut self, pn: PageNum) {
        // We are increasing the capacity of what pages we cache in memory, not changing the on-disk database file.
        if pn > self.pages.len() {
            println!("Extending pager capacity to {}", pn);
            self.pages.resize(pn, None)
        }

        let need_load = match self.pages[pn - 1] {
            None => true,
            Some(_) => false,
        };
        if !need_load {
            return;
        }
        println!("Reading page {} on demand.", pn);
        let v = Self::read_page_from_vfs(&mut self.vfs, pn)
            .map_err(|_| Error::ReadFailed)
            .unwrap();
        self.pages[pn - 1] = Some(v);
    }

    // I think this says that the self object, has lifetime 'b which must be longer than the lifetime of the returned reference
    // to the vector it contains.
    pub fn get_page_ro<'a, 'b: 'a>(&'b mut self, pn: PageNum) -> Result<&'a Vec<u8>, Error> {
        if pn > MAX_PAGE_NUM {
            return Err(Error::PageNumberBeyondLimits);
        }

        self.ensure_present(pn);
        match &self.pages[pn - 1] {
            Some(v) => Ok(v.as_ref()),
            None => Err(Error::InternalError),
        }
    }

    #[allow(dead_code)]
    pub fn get_page_rw(self, _: PageNum) -> Result<Vec<u8>, Error> {
        // TODO: support writing pages. This will need reader/writer locks.
        unimplemented!("Writing not implemented")
    }
}
