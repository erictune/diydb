//! `parser` holds routines for parsing SQL statements.

use pest::Parser;
#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SQLParser;

pub fn parse_create_statement(c: &str) -> (String, Vec<&str>, Vec<&str>) {
    use itertools::Itertools;
    let create_stmt = SQLParser::parse(Rule::create_stmt, c)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap();

    let mut colnames = vec![];
    let mut coltypes = vec![];

    let mut table_name = String::from("");
    // Confirm it is a select statement.
    for c in create_stmt.into_inner() {
        match c.as_rule() {
            Rule::table_identifier => {
                table_name = String::from(c.as_str());
            }
            Rule::column_defs => {
                for column_def in c.into_inner() {
                    match column_def.as_rule() {
                        Rule::column_def => {
                            let (col_name, col_type) = column_def
                                .into_inner()
                                .take(2)
                                .map(|e| e.as_str())
                                .collect_tuple()
                                .unwrap();
                            colnames.push(col_name);
                            coltypes.push(col_type);
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
    (table_name, colnames, coltypes)
}

#[test]
fn test_parse_create_statement() {
    assert_eq!(
        parse_create_statement("CREATE TABLE t (a int, b integer, c text, d string, e real)"),
        (
            String::from("t"),
            vec!["a", "b", "c", "d", "e"],
            vec!["int", "integer", "text", "string", "real"]
        )
    );
    assert_eq!(
        parse_create_statement("CREATE TABLE Tbl_Two(a int,b int)"),
        (String::from("Tbl_Two"), vec!["a", "b"], vec!["int", "int"])
    );
}

// TODO: expand star into list of all column names of all tables in the input table list.
pub fn parse_select_statement(query: &str) -> (Vec<&str>, Vec<&str>) {
    let select_stmt = SQLParser::parse(Rule::select_stmt, &query)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap();

    let mut output_cols = vec![];
    let mut input_tables = vec![];
    // Confirm it is a select statement.
    for s in select_stmt.into_inner() {
        match s.as_rule() {
            Rule::table_identifier => {
                input_tables.push(s.as_str());
            }
            Rule::select_items => {
                for t in s.into_inner() {
                    output_cols.push(t.as_str());
                }
            }
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
    (input_tables, output_cols)
}

#[test]
fn test_parse_select_statement() {
    assert_eq!(
        parse_select_statement("SELECT * FROM tbl"),
        (vec!["tbl"], vec!["*"])
    );
    assert_eq!(
        parse_select_statement("select a,b,c fRoM tbl"),
        (vec!["tbl"], vec!["a", "b", "c"])
    );
}
