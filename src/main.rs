// TODO: main should just be a REPL like the sqlite3 cli.
fn main() {
    let mut vfs = diydb::vfs::DbAttachment::open("./record.db").expect("Should have opened the DB");
    // Read db file header to confirm it is a valid file, and how many and what size pages it has.
    let dbhdr = vfs.get_header().expect("Should have gotten DB file header");
    println!("Opened DB File. {:?}", dbhdr);
    // TODO: combine pager and vfs references as interal details of DbAttachment struct.
    // DbAttachment can be in its own module.
    // A DbAttachment contains a pager [1] and any settings of the session (ro vs rw).
    // A DbAttachment offers access to the current state of the db header.
    // A DbAttachment checks magic opening the file.
    // A DbAttachment checks that fixed header values are valid/supported (like page size).
    // A vfs is an implementation detail of a DBAttachment, and only has the one implementation for us (posix locking)
    //  (maybe ":memory:" in the future.)
    // A DbAttachment gives access to modifiable header fields, using Pager to lock concurrent access to page 1.
    // [1] When we open the file, we will lock it.  So there should be only one instance of the file open
    // across all processes. (might two processess open readonly without locking?  Okay, but they have separate pagers.)
    // When diydb is used as a library, then there is only one DBAttachment to a give file in that process as well.
    // So only one pager is needed.
    // So the pager can be embedded in the db attachment.
    // That raises the question of how to make the DBAttachment threadsafe, but that is for another day.
    let mut pager = diydb::pager::Pager::new(vfs);
    println!("-----");
    println!("Printing schema table...");
    diydb::print_schema(&mut pager);
    println!("-----");
    let q = "SELECT * FROM record_test";
    println!("Doing query: {}", q);
    diydb::run_query(&mut pager, q);
}
