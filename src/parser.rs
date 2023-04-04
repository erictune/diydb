// extern crate pest;
// #[macro_use]
// extern crate pest_derive;
// use pest::Parser;
// #[derive(Parser)]
// #[grammar = "sql.pest"]


use pest::Parser;
#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SQLParser;

// TODO: figure out how to move parsing and code generation out of main into codegen.rs.
// TODO: expand star into list of all column names of all tables in the input table list.
pub fn parse_create_statement(c: &str) -> (String, Vec<&str>, Vec<&str>) {
    use itertools::Itertools;

    // TODO: get this from the schema table by looking it up.

    let create_stmt = SQLParser::parse(Rule::create_stmt, c)
    .expect("unsuccessful parse") // unwrap the parse result
    .next().unwrap();

    let mut colnames = vec![];
    let mut coltypes = vec![];

    let mut table_name = String::from("");
    // Confirm it is a select statement.
    for c in create_stmt.into_inner() {
        //println!("{:?}", s);
        match c.as_rule() {
            Rule::table_identifier => { table_name = String::from(c.as_str()); },
            Rule::column_defs => {
                for column_def in c.into_inner() {
                    match column_def.as_rule() {
                        Rule::column_def => { 
                            let (col_name, col_type) = column_def.into_inner().take(2).map(|e| e.as_str()).collect_tuple().unwrap();
                            colnames.push(col_name );
                            coltypes.push(col_type);

                        },
                        _ => unreachable!(),
                    }
                }
            },
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
    (table_name, colnames, coltypes)
}

pub fn parse_select_statement(query: &str)  -> (Vec<&str>, Vec<&str>) {
    let select_stmt = SQLParser::parse(Rule::select_stmt, &query)
    .expect("unsuccessful parse") // unwrap the parse result
    .next().unwrap();

    let mut output_cols = vec![];
    let mut input_tables = vec![];
    // Confirm it is a select statement.
    for s in select_stmt.into_inner() {
        //println!("{:?}", s);
        match s.as_rule() {
            Rule::select_item => { 
                for t in s.into_inner() {
                    //println!("--- {:?}", t);

                    match t.as_rule() {
                        Rule::column_name => { input_tables.push(t.as_str()); },
                        Rule::star => unimplemented!(),
                        _ => unreachable!(),
                     };
                }
            },
            Rule::table_identifier => { output_cols.push(s.as_str()); },
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
    (input_tables, output_cols)
}

