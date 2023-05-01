fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_get_creation_sql_and_root_pagenum_using_minimal_db() {
    let path = path_to_testdata("minimal.db");
    let mut pager =
        diydb::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager.");
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
    let mut pager =
        diydb::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager");
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
fn test_run_query_on_minimal_db() {
    use diydb::sql_value::SqlValue::*;
    let path = path_to_testdata("minimal.db");
    let mut pager =
        diydb::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager.");
    let qot = diydb::run_query_no_print(&pager, "select * from a").unwrap();
    assert_eq!(qot.rows.len(), 1);
    assert_eq!(qot.rows[0].items.len(), 1);
    assert_eq!(qot.rows[0].items[0], Int(1));
}

#[test]
fn test_run_query_on_multipage_with_various_page_sizes() {
    use diydb::sql_value::SqlValue::*;

    let dbs = vec![
        "multipage-512B-page.db",
        "multipage-1kB-page.db",
        "multipage.db", // 4k.
    ];
    for db in dbs {
        let path = path_to_testdata(db);
        let mut pager =
            diydb::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
        pager.initialize().expect("Should have initialized pager.");
        let qot = diydb::run_query_no_print(&pager, "select * from thousandrows").unwrap();
        assert_eq!(qot.rows.len(), 1000);

        assert_eq!(qot.rows[0].row_id, 1);
        assert_eq!(qot.rows[0].items.len(), 3);
        assert_eq!(
            qot.rows[0].items,
            vec![
                Text(String::from("A")),
                Text(String::from("A")),
                Text(String::from("A"))
            ]
        );

        assert_eq!(qot.rows[284].row_id, 285);
        assert_eq!(qot.rows[284].items.len(), 3);
        assert_eq!(
            qot.rows[284].items,
            vec![
                Text(String::from("C")),
                Text(String::from("I")),
                Text(String::from("E"))
            ]
        );

        assert_eq!(qot.rows[999].row_id, 1000);
        assert_eq!(qot.rows[999].items.len(), 3);
        assert_eq!(
            qot.rows[999].items,
            vec![
                Text(String::from("J")),
                Text(String::from("J")),
                Text(String::from("J"))
            ]
        );
    }
}

#[cfg(test)]
fn expected_row_for_three_level_db(i: i64) -> diydb::typed_row::TypedRow {
    diydb::typed_row::TypedRow {
        row_id: i + 1,
        items: format!("{:05}", i)
            .replace("0", "A")
            .replace("1", "B")
            .replace("2", "C")
            .replace("3", "D")
            .replace("4", "E")
            .replace("5", "F")
            .replace("6", "G")
            .replace("7", "H")
            .replace("8", "I")
            .replace("9", "J")
            .chars()
            .map(|c| diydb::sql_value::SqlValue::Text(String::from(c)))
            .collect(),
    }
}

#[test]
fn test_run_query_on_three_level_db() {
    // This tests iterating over a btree of three levels (root, non-root interior pages, leaf pages).
    // The table has these rows:
    // row 1: 1
    // row 1000000: 1000000

    let path = path_to_testdata("threelevel.db");
    let mut pager =
        diydb::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager.");
    let qot = diydb::run_query_no_print(&pager, "select * from t").unwrap();

    assert_eq!(qot.rows.len(), 100000);
    for i in 0..100000 {
        assert_eq!(qot.rows[i].row_id as usize, i + 1);
        assert_eq!(qot.rows[i].items.len(), 5);
        assert_eq!(qot.rows[i], expected_row_for_three_level_db(i as i64));
    }
}
