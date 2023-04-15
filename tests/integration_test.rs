use std::env;

fn path_to_testdata(filename: &str) -> String {
    env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_open_db() {
    let path = path_to_testdata("minimal.db");
    let _ = diydb::pager::Pager::open(path.as_str()).initialize();
}

#[test]
fn test_get_creation_sql_and_root_pagenum_using_minimal_db() {
    let path = path_to_testdata("minimal.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
    pager.initialize();
    let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, "a");
    assert!(x.is_some());
    let (pgnum, csql) = x.unwrap();
    assert_eq!(pgnum, 2);
    assert_eq!(
        csql.to_lowercase().replace('\n', " "),
        "create table a ( b int )"
    );
}

#[test]
fn test_get_creation_sql_and_root_pagenum_using_schematable_db() {
    let path = path_to_testdata("schema_table.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
    pager.initialize();
    let expected_tables = vec![
        ("t1", 2, "create table t1 (a int)"),
        ("t2", 3, "create table t2 (a int, b int)"),
        (
            "t3",
            4,
            "create table t3 (a text, b int, c text, d int, e real)",
        ),
    ];
    for expect in expected_tables {
        let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, expect.0);
        assert!(x.is_some());
        let (pgnum, csql) = x.unwrap();
        assert_eq!(pgnum, expect.1); // first page after schema_table page.
        assert_eq!(csql.to_lowercase().replace('\n', " "), expect.2);
    }
}

#[test]
fn test_record_iterator_on_minimal_db() {
    let path = path_to_testdata("minimal.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
    pager.initialize();
    let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, "a");
    let mut ri = diydb::new_table_leaf_cell_iterator_for_page(&mut pager, x.unwrap().0);
    let first_item = ri.next().clone();
    assert!(first_item.is_some());
    assert_eq!(first_item.unwrap().0, 1);
    assert!(ri.next().is_none());
}

#[test]
fn test_record_iterator_on_multipage_db() {
    // This tests iterating over the root page which is interor type.
    // The table has these rows:
    // row 1: "AAA"
    // row 2: "AAB"
    // ...
    // row 1000: "JJJ"
    //
    // The file has 4 x 4k pages:
    // Page 1: schema
    // Page 2: root of "digits" table.
    // Page 3: Index type page.
    // Page 4: first leaf page (AAA to DFA ; rows 1-351)
    // Page 5: second leaf page (DFB to GJA ; rows 352-691)
    // Page 6: third leaf page (GJB to JJJ ; 692-1000)
    let path = path_to_testdata("multipage.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
    pager.initialize();
    let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, "thousandrows");
    let pgnum = x.unwrap().0;
    assert_eq!(pgnum, 3);
    let mut ri = diydb::new_table_interior_cell_iterator_for_page(&mut pager, pgnum);
    assert_eq!(ri.next(), Some(4));
    assert_eq!(ri.next(), Some(5));
    assert_eq!(ri.next(), Some(6));
    assert_eq!(ri.next(), None);
}

#[test]
fn test_record_iterator_on_multipage_withvarious_page_sizes() {
    let dbs = vec![
        "multipage-512B-page.db",
        "multipage-1kB-page.db",
        "multipage.db", // 4k.
    ];
    for db in dbs {
        let path = path_to_testdata(db);
        println!("{}", path);
        let mut pager = diydb::pager::Pager::open(path.as_str());
        pager.initialize();
        let _ = diydb::get_creation_sql_and_root_pagenum(&mut pager, "thousandrows");
        // TODO: test queries on the table once btree table iterator support done.
    }
}

#[test]
fn test_record_iterator_on_three_level_db() {
    // This tests iterating over a btree of three levels (root, non-root interior pages, leaf pages).
    // The table has these rows:
    // row 1: 1
    // row 1000000: 1000000
    let path = path_to_testdata("threelevel.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
    pager.initialize();
    let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, "t");
    let pgnum = x.unwrap().0;

    let ri = diydb::new_table_iterator_for_page(&pager, pgnum);
    let mut last_rowid = 0;
    for e in ri.enumerate() {
        let (expected, (rowid, _)) = e;
        println!("Visiting rowid {} on iteration {}", rowid, expected);
        assert_eq!(expected+1, rowid as usize);
        last_rowid = rowid
    }
    assert_eq!(last_rowid, 100000);

    // TODO NOSUBMIT : count number of interior pages in the table?
    // let num_pages = pager.get_num_pages().unwrap();
    // let mut n_interior = 0;
    // println!("Number of pages: {}", num_pages);
    // for page_num in 1..(num_pages+1) { 
    //     print!("Page {} - ", page_num);
    //     let r = diydb::new_reader_for_page(&mut pager, page_num as usize);
    //     let h = r.check_header();
    //     match h.btree_page_type {
    //         diydb::btree::PageType::TableInterior => n_interior += 1,
    //         _ => (),
    //     }
    //     //println!("{:?}", h);    
    // }
    // Ensure the test file had a bunch of interior pages.
    // assert!(n_interior > 3);

    // TODO NOSUBMIT : ensure we can walk all 1000000 or so items in the table in order.
}
