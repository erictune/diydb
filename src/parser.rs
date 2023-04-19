//! `parser` holds routines for parsing SQL statements.

use pest::Parser;
#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SQLParser;

pub fn parse_create_statement(c: &str) -> (String, Vec<String>, Vec<String>) {
    use itertools::Itertools;
    let create_stmt = SQLParser::parse(Rule::create_stmt, c)
        .expect("unsuccessful parse") // unwrap the parse result
        .next()
        .unwrap();

    let mut colnames = vec![];
    let mut coltypes = vec![];

    let mut tablename = String::from("");
    // Confirm it is a create statement.
    for c in create_stmt.into_inner() {
        match c.as_rule() {
            Rule::table_identifier => {
                tablename = String::from(c.as_str());
            }
            Rule::column_defs => {
                for column_def in c.into_inner() {
                    match column_def.as_rule() {
                        Rule::column_def => {
                            let (col_name, col_type) = column_def
                                .into_inner()
                                .take(2)
                                .map(|e| String::from(e.as_str()))
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
    (tablename, colnames, coltypes)
}

#[test]
fn test_parse_create_statement() {
    let cases = vec![
        (
            "CREATE TABLE t (a int, b integer, c text, d string, e real)",
            (
                "t",
                vec!["a", "b", "c", "d", "e"],
                vec!["int", "integer", "text", "string", "real"]
            )
         ),
         (
            "CREATE TABLE Tbl_Two(a int,b int)",
            (
                "Tbl_Two", vec!["a", "b"], vec!["int", "int"]
            )
         ),

    ];
    for case in cases {
        let input = case.0;
        println!("Input: {}", input);
        let actual = parse_create_statement(input);
        let expected = (
                String::from(case.1.0), 
                case.1.1.iter().map(|x| String::from(*x)).collect(), 
                case.1.2.iter().map(|x| String::from(*x)).collect()
        );
        assert_eq!(actual, expected);
    }    
}

// TODO: expand star into list of all column names of all tables in the input table list.
pub fn parse_select_statement(query: &str) -> (Vec<&str>, Vec<&str>) {
    let select_stmt = SQLParser::parse(Rule::select_stmt, query)
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
            x => panic!("Unable to parse expr:  {} ", s.as_str()),
        }
    }
    (input_tables, output_cols)
}

#[test]
fn test_parse_select_statement() {
    let cases = vec![
        (
            "SELECT * FROM tbl",
            (vec!["tbl"], vec!["*"])
    
        ),
        (
            "select a,b,c fRoM tbl",
            (vec!["tbl"], vec!["a", "b", "c"])
        ),
    ];
    
    for case in cases {
        let input = case.0;
        println!("Input: {}", input);
        let actual = parse_select_statement(input);
        let expected = case.1;
        assert_eq!(actual, expected);
    }    
}
