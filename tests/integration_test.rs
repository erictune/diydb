fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

fn server_state_with_open_db_for_run_query_tests(path: &str) -> diydb::DbServerState {
    let mut ss = diydb::DbServerState::new();
    diydb::open_db(&mut ss, path)
        .expect(format!("Should have opened {}.", path).as_str());
    ss.into()
}

#[test]
fn test_run_query_on_minimal_db() {
    use diydb::sql_value::SqlValue::*;
    let path = path_to_testdata("minimal.db");
    let ss = server_state_with_open_db_for_run_query_tests(path.as_str());
    let tt = diydb::run_query_no_print(&ss, "select * from a").unwrap();
    assert_eq!(tt.rows.len(), 1);
    assert_eq!(tt.rows[0].items.len(), 1);
    assert_eq!(tt.rows[0].items[0], Int(1));
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
        let ss = server_state_with_open_db_for_run_query_tests(path.as_str());
        let tt = diydb::run_query_no_print(&ss, "select * from thousandrows").unwrap();
        assert_eq!(tt.rows.len(), 1000);

        assert_eq!(tt.rows[0].items.len(), 3);
        assert_eq!(
            tt.rows[0].items,
            vec![
                Text(String::from("A")),
                Text(String::from("A")),
                Text(String::from("A"))
            ]
        );

        assert_eq!(tt.rows[284].items.len(), 3);
        assert_eq!(
            tt.rows[284].items,
            vec![
                Text(String::from("C")),
                Text(String::from("I")),
                Text(String::from("E"))
            ]
        );

        assert_eq!(tt.rows[999].items.len(), 3);
        assert_eq!(
            tt.rows[999].items,
            vec![
                Text(String::from("J")),
                Text(String::from("J")),
                Text(String::from("J"))
            ]
        );
    }
}

#[cfg(test)]
fn expected_row_for_three_level_db(i: i64) -> diydb::typed_row::Row {
    diydb::typed_row::Row {
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
    let ss = server_state_with_open_db_for_run_query_tests(path.as_str());
    let tt = diydb::run_query_no_print(&ss, "select * from t").unwrap();

    assert_eq!(tt.rows.len(), 100000);
    for i in 0..100000 {
        assert_eq!(tt.rows[i].items.len(), 5);
        assert_eq!(tt.rows[i], expected_row_for_three_level_db(i as i64));
    }
}

#[test]
fn test_run_dbless_selects() {
    let ss = diydb::DbServerState::new();
    let tt = diydb::run_query_no_print(&ss, "select 1, 2, 3").unwrap();
    use diydb::sql_value::SqlValue;
    assert_eq!(tt.rows.len(), 1);
    assert_eq!(tt.rows[0].items.len(), 3);
    assert_eq!(
        tt.rows[0].items,
        vec![SqlValue::Int(1), SqlValue::Int(2), SqlValue::Int(3)]
    );
}

#[cfg(test)]
#[allow(non_snake_case)]
fn text_A() -> diydb::sql_value::SqlValue {
    diydb::sql_value::SqlValue::Text("A".to_string())
}
#[cfg(test)]
#[allow(non_snake_case)]
fn text_B() -> diydb::sql_value::SqlValue {
    diydb::sql_value::SqlValue::Text("B".to_string())
}

#[test]
fn test_run_selects() {
    use diydb::typed_row::Row;
    use diydb::sql_value::SqlValue::*;
    let path = path_to_testdata("for_exprs.db");
    let ss = server_state_with_open_db_for_run_query_tests(path.as_str());
    let cases = vec![
        (
            "select * from t",
            vec![
                    Row { items: vec![Int(1), Int(1), Real(1.1), Real(1.1), text_A(), text_A()] }, 
                    Row { items: vec![Int(1), Int(2), Real(1.1), Real(2.2), text_A(), text_B()] },
                    Row { items: vec![Int(2), Int(1), Real(2.2), Real(1.1), text_B(), text_A()] },
                    Row { items: vec![Int(0), Int(3), Real(0.0), Real(3.3), text_A(), text_A()] },
            ], 
        ),
        (
            "select a, c, e from t",
            vec![
                Row { items: vec![Int(1), Real(1.1), text_A()] }, 
                Row { items: vec![Int(1), Real(1.1), text_A()] },
                Row { items: vec![Int(2), Real(2.2), text_B()] },
                Row { items: vec![Int(0), Real(0.0), text_A()] },
            ]
        ),
        (
            "select 1, 2, 3",
            vec![
                Row { items: vec![Int(1), Int(2), Int(3)] }, 
            ],
        ),
        (
            "select 1, 2, 3 from t",
            vec![
                Row { items: vec![Int(1), Int(2), Int(3)] }, 
                Row { items: vec![Int(1), Int(2), Int(3)] }, 
                Row { items: vec![Int(1), Int(2), Int(3)] }, 
                Row { items: vec![Int(1), Int(2), Int(3)] }, 
            ],
        ),
        (
            "select d, 1, a, 2, c, 3 from t",
            vec![
                Row { items: vec![Real(1.1), Int(1), Int(1), Int(2), Real(1.1), Int(3)] }, 
                Row { items: vec![Real(2.2), Int(1), Int(1), Int(2), Real(1.1), Int(3)] },
                Row { items: vec![Real(1.1), Int(1), Int(2), Int(2), Real(2.2), Int(3)] },
                Row { items: vec![Real(3.3), Int(1), Int(0), Int(2), Real(0.0), Int(3)] },
            ], 
        ),
        (
            "select *, 1, * from t",
            vec![
                Row { items: vec![Int(1), Int(1), Real(1.1), Real(1.1), text_A(), text_A(), Int(1), Int(1), Int(1), Real(1.1), Real(1.1), text_A(), text_A()] }, 
                Row { items: vec![Int(1), Int(2), Real(1.1), Real(2.2), text_A(), text_B(), Int(1), Int(1), Int(2), Real(1.1), Real(2.2), text_A(), text_B()] },
                Row { items: vec![Int(2), Int(1), Real(2.2), Real(1.1), text_B(), text_A(), Int(1), Int(2), Int(1), Real(2.2), Real(1.1), text_B(), text_A()] },
                Row { items: vec![Int(0), Int(3), Real(0.0), Real(3.3), text_A(), text_A(), Int(1), Int(0), Int(3), Real(0.0), Real(3.3), text_A(), text_A()] },

            ], 

        ),
        (
            "select 1 + 1",
            vec![
                Row { items: vec![Int(2)] }, 
            ], 

        ),
        (
            "select 1 + 1 from t",
            vec![
                Row { items: vec![Int(2)] }, 
                Row { items: vec![Int(2)] }, 
                Row { items: vec![Int(2)] }, 
                Row { items: vec![Int(2)] }, 
            ], 
        ),
        (
            "select 1 + 1, a from t",
            vec![
                Row { items: vec![Int(2), Int(1),] }, 
                Row { items: vec![Int(2), Int(1),] }, 
                Row { items: vec![Int(2), Int(2),] }, 
                Row { items: vec![Int(2), Int(0),] }, 
            ], 
        ),
    ];
    for case in cases {
        println!("--------------\n");
        println!("running: {}", case.0);
        let actual = diydb::run_query_no_print(&ss, case.0);
        assert!(actual.is_ok());
        let actual = actual.unwrap();
        println!("Actual rows: {:?}", actual.rows);
        println!("Expected rows: {:?}", case.1);
        assert_eq!(actual.rows, case.1);
    }
}

#[test]
fn test_create_a_temptable() {
    let mut ss = diydb::DbServerState::new();
    diydb::run_create(&mut ss, "create temp table t (i int)").unwrap();
    // This is relying on automatic creation of a temptable.  TODO: implement CREATE and use that here.
    let tt = diydb::run_query_no_print(&ss, "select * from temp.t").unwrap();
    assert_eq!(tt.rows.len(), 0);
}
// TODO: be able to create persistent tables.

#[test]
fn test_insert_into_temptable_adds_a_row() {
    use diydb::sql_value::SqlValue::*;
    let mut ss = diydb::DbServerState::new();
    diydb::run_create(&mut ss, "create temp table t (i int)").unwrap();
    // This is relying on automatic creation of a temptable.  TODO: implement CREATE and use that here.
    let tt = diydb::run_query_no_print(&mut ss, "select * from temp.t").unwrap();
    assert_eq!(tt.rows.len(), 0);
    // Should be able to insert a row.
    diydb::run_insert(&mut ss, "insert into temp.t values (42)").expect("Should have inserted without errors");
    // After Insert, there are two rows.
    let tt = diydb::run_query_no_print(&mut ss, "select * from temp.t").unwrap();
    assert_eq!(tt.rows.len(), 1);
    assert_eq!(tt.rows[0].items.len(), 1);
    assert_eq!(tt.rows[0].items[0], Int(42));
    // Should be able to insert another row.
    diydb::run_insert(&mut ss, "insert into temp.t values (102)").expect("Should have inserted without errors");
    // After Insert, there are two rows.
    let tt = diydb::run_query_no_print(&mut ss, "select * from temp.t").unwrap();
    assert_eq!(tt.rows.len(), 2);
    assert_eq!(tt.rows[0].items.len(), 1);
    assert_eq!(tt.rows[0].items[0], Int(42));
    assert_eq!(tt.rows[1].items.len(), 1);
    assert_eq!(tt.rows[1].items[0], Int(102));
}
// TODO: insert to multiple column temp tables.
// TODO: insert multiple rows at a time.
// TODO: test all those things on persistent SQLite tables when supported.

#[test]
fn test_insert_select_on_temptable_strict_works() {
    let mut ss = diydb::DbServerState::new();

    diydb::run_create(&mut ss, "create temp table t (i int, j int) strict").expect("Should have setup test scenario.");
    diydb::run_insert(&mut ss, "insert into temp.t values (42, 27)").expect("Should have inserted without errors");
    diydb::run_insert(&mut ss, "insert into temp.t values (42, 'hello')").expect_err("Should have gotten error inserting string to int column");
    diydb::run_insert(&mut ss, "insert into temp.t values (42)").expect_err("Should have gotten error inserting short row");
    diydb::run_insert(&mut ss, "insert into temp.t values (42, 43, 44)").expect_err("Should have gotten error inserting long row");
}
