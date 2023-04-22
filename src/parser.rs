//! `parser` contains generated parsing routines for SQL and tests on them.

#[allow(unused_imports)]
use pest::Parser; // This needs to be in scope for the next statements to work.
#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SQLParser;

#[test]
fn test_parse_literals() {
    let cases = vec![
        ("1"),
        ("1000000000000"),
        ("-1000000000000"),
        ("1.01"),
        ("123456789.987654321"),
        ("'hi'"),
        ("true"),
        ("tRuE"),
        ("TRUE"),
        ("false"),
        ("fAlSe"),
        ("FALSE"),
        ("null"),
        ("nUlL"),
        ("NULL"),
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::literal, case).is_ok());
    }
}

#[test]
fn test_not_parse_invalid_literals() {
    let cases = vec![
        ("A"),
        ("\"hi\""),
        ("the quick brown fox"),
        ("NIL"),
        ("DELETE"),
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::literal, case).is_err());
    }
}

#[test]
fn test_parse_create_statements() {
    let cases = vec![
        "CREATE TABLE FOO (A INT, B INT)",
        "create table foo (a int, b int)",
        "create table foo (a int)",
        "create table foo (a int,  b int, c int, dee real)",
        "CREATE TABLE t (a int, b integer, c text, d string, e real)",
        "creaTe TaBle superlongname (superduperlongname integer)",
    ];
    for case in cases {
        println!("Case: {}", case);
        assert!(SQLParser::parse(Rule::create_stmt, case).is_ok());
    }
}

#[test]
fn test_not_parse_invalid_create_statements() {
    let cases = vec![
        "CREATE TABLE FOO (nonsense that does not have commas)",
        "create table foo a int, b int",
        "create table foo ()",
        "create table foo (,,,,,)",
        "SELECT * from T",
    ];
    for case in cases {
        assert!(SQLParser::parse(Rule::create_stmt, case).is_err());
    }
}

#[test]
fn test_parse_select_statement() {
    let cases = vec![
        ("SELECT * FROM tbl"),
        ("select a,b,c fRoM tbl"),
        ("select x, 1 from tbl"),
        ("select x, 1"), // This is invalid SQL, but this check happens after parsing.
        ("select 1.01"),
        ("select 'hi'"),
    ];

    for case in cases {
        assert!(SQLParser::parse(Rule::select_stmt, case).is_ok());
    }
}

#[test]
fn test_not_parse_invalid_select_statement() {
    let cases = vec![
        ("CREATE * FROM tbl"),
        ("FROM blahblah"),
        ("select \"hi\""), // Double quotes are invalid as literals in std SQL.
    ];

    for case in cases {
        assert!(SQLParser::parse(Rule::select_stmt, case).is_err());
    }
}
