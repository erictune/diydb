use std::env;

fn path_to_testdata(filename: &str) -> String {
    env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_open_db() {
    let path = path_to_testdata("minimal.db");
    let mut vfs = diydb::vfs::DbAttachment::open(path.as_str()).expect("Should have opened the DB");
    // Read db file header to confirm it is a valid file, and how many and what size pages it has.
    let dbhdr = vfs.get_header().expect("Should have gotten DB file header");
    println!("Opened DB File. {:?}", dbhdr);
}

// TODO: test get_creation_sql_and_root_pagenum()

// TODO: test print_table()

// #[test]
// fn test_record_iterator_real_db() {
//     let record_iterator = new_table_leaf_cell_iterator_for_page(pgr, SCHEMA_BTREE_ROOT_PAGENUM);
//     assert_eq!(adder::add(3, 2), 5);
// }
