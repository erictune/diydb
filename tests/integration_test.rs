use std::env;

fn path_to_testdata(filename: &str) -> String {
    env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_open_db() {
    let path = path_to_testdata("minimal.db");
    let _ = diydb::pager::Pager::open(path.as_str());
}

#[test]
fn test_get_creation_sql_and_root_pagenum_using_minimal_db() {
    let path = path_to_testdata("minimal.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
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
    let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, "a");
    let mut ri = diydb::new_table_leaf_cell_iterator_for_page(&mut pager, x.unwrap().0);
    let first_item = ri.next().clone();
    assert!(first_item.is_some());
    assert_eq!(first_item.unwrap().0, 1);
    assert!(ri.next().is_none());

}

#[test]
#[should_panic]
// TODO: make this not panic by supporting index pages.
fn test_record_iterator_on_multipage_db() {
    let path = path_to_testdata("multipage.db");
    let mut pager = diydb::pager::Pager::open(path.as_str());
    let x = diydb::get_creation_sql_and_root_pagenum(&mut pager, "thousandrows");
    let ri = diydb::new_table_leaf_cell_iterator_for_page(&mut pager, x.unwrap().0);
    assert_eq!(ri.count(), 1000);
    //println!("{:?}", ri.collect::<Vec<u8>>());
}
