extern crate pest;
#[macro_use]
extern crate pest_derive;

use pest::Parser;

#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SQLParser;

// TODO: Parse the create statement from the "sql" column of the schema table to provide the schema for the second table,
// and use that schema as the column headers.
// Then cast each serial type to its schema type while reading them.


// Intent is to have code structure which models Sqlite's architecture (https://www.sqlite.org/arch.html)
// "vfs" - opens and locks database files, providing Read and Seek interfaces to them, and the header (readonly initially).
mod vfs;
// "pager" - provides an array of pages, which may or may not be present in memory (seek and load on first access).  Uses a vfs.
mod pager;
// "btree" - provides iterator (cursor) to walk over btree elements (in future could support writes.).  Uses a pager to get at pages.
mod btree;
// "parser" - parses SQL statement into a parse tree, e.g. using https://pest.rs/book/examples/ini.html
// We use pest parser generator.

// "bytecode" - makes a program from a parse tree.  Uses btree cursors to the referenced tables.  Emits rows.
// "interface" - REPL loop that accepts a sql query to do on the file, using parser and vfs, and commands to open databases, etc.
// Formats emitted rows to csv file or stdout.

mod record;
mod serial_type;

use crate::vfs::DbAttachment;
use std::io::Cursor;
// TODO: make an iterator that can walk across multiple pages.  To do that,
// the "btree iterator" needs to hold access to the pager.  This in turn requires.
// improvements to pager design, like:
// (pager object static lifetime, page interior mutability, concurrency controls)

fn new_reader_for_page(pgr: &mut pager::Pager, pgnum: usize) -> btree::PageReader {
    let page = match pgr.get_page_ro(pgnum) {
        Ok(p) => p,
        Err(e) => panic!("Error loading db page #{} : {}", pgnum, e),
    };
    let btree_start_offset = match pgnum {
        1 => 100,
        _ => 0,
    };
    btree::PageReader::new(page, btree_start_offset)
}

fn new_table_leaf_cell_iterator_for_page(pgr: &mut pager::Pager, pgnum: usize) -> btree::TableLeafCellIterator {
    let page = match pgr.get_page_ro(pgnum) {
        Ok(p) => p,
        Err(e) => panic!("Error loading db page #{} : {}", pgnum, e),
    };
    let btree_start_offset = match pgnum {
        1 => 100,
        _ => 0,
    };
    // TODO: hide btree::CellIterator.  Just have TableCellIterator, which handles both page types for table btrees.
    btree::TableLeafCellIterator::new(btree::CellIterator::new(page, btree_start_offset))
}

fn print_table(pgr: &mut pager::Pager, root_pgnum: usize, table_name: &str, column_names: Vec<&str>) {
    println!("Full Dump of Table {}", table_name);
    println!("   | {} |", column_names.iter().map(|x| format!("{:15}", x)).collect::<Vec<String>>().join(" | "));
    {
        let pr = new_reader_for_page(pgr, root_pgnum);
        let _ = pr.check_header();
        //println!("{:?}", tl.check_header());
    }
    {
        let tci = new_table_leaf_cell_iterator_for_page(pgr, root_pgnum);
        for (rowid, payload) in tci {
            // TODO: use map(typecode_to_string).join("|") or something like that.
            let rhi = record::HeaderIterator::new(payload);
            print!("{:2} |", rowid);
            for t in rhi {   
                print!(" {:15} |", serial_type::typecode_to_string(t)); 
            }
            println!("");
            print!("{:2} |", rowid);
            let hi = record::ValueIterator::new(&payload[..]);
            for (t, v) in hi {
                // TODO: map the iterator using a closure that calls to_string, and then intersperses the delimiters and then reduces into a string.
                // TODO: move cursor use into read_value_to_string, so it just uses a byte slice.
                print!(" {:15} |", serial_type::read_value_to_string(&t, &mut Cursor::new(v)));
            }
            println!("");
        }
    }
}

// TODO: figure out how to move parsing and code generation out of main into codegen.rs.
// TODO: expand star into list of all column names of all tables in the input table list.
fn parse_create_statement(c: &str) -> (String, Vec<&str>) {
    // TODO: get this from the schema table by looking it up.

    let create_stmt = SQLParser::parse(Rule::create_stmt, c)
    .expect("unsuccessful parse") // unwrap the parse result
    .next().unwrap();

    let mut colnames_and_types = vec![];
    let mut table_name = String::from("");
    // Confirm it is a select statement.
    for c in create_stmt.into_inner() {
        //println!("{:?}", s);
        match c.as_rule() {
            Rule::table_identifier => { table_name = String::from(c.as_str()); },
            Rule::column_defs => {
                for column_def in c.into_inner() {
                    match column_def.as_rule() {
                        Rule::column_def => { colnames_and_types.push(column_def.as_str()); },
                        _ => unreachable!(),
                    }
                }
            },
            Rule::EOI => (),
            _ => unreachable!(),
        }
    }
    (table_name, colnames_and_types)
}

fn parse_select_statement(query: &str)  -> (Vec<&str>, Vec<&str>) {
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

fn main() {
    // Open the database file. This is a file in sqlite3 format.
    let mut vfs = DbAttachment::open("./record.db").expect("Should have opened the DB");

    // Read db file header to confirm it is a valid file, and how many and what size pages it has.
    let dbhdr = vfs.get_header().expect("Should have gotten DB file header");
    println!("Opened DB File. {:?}", dbhdr);
    // TODO: move checking magic and reading creation-time fields (like page size) into vfs.rs.
    //       but move access to modifiable fields to use a Pager from pager.rs, since that will require locking.

    let mut pager = pager::Pager::new(vfs);

    // ----------------------------------------------------//

    // Page 1 (the first page) is always a btree page, and it is the root of the schema.  It has references
    // to the roots of other btrees.
    const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
    let schema_table_columns = vec!["type", "name", "tbl_name", "rootpage", "sql"];

    print_table(&mut pager, SCHEMA_BTREE_ROOT_PAGENUM, "sqlite_schema", schema_table_columns);

    // ----------------------------------------------------//
    // TODO: get from above table:
    let create_statement = "CREATE TABLE record_test ( a int, b int, c real, d string, e int)";
    // TODO: separate types and column names.  Just print the names.  Use the types to cast the serial_types.
    let (table_name, column_names) = parse_create_statement(create_statement);
    // TODO: make the print_table function accept a vector<T> where T is str& or String?
    // let column_names_as_str = column_names.iter().into_iter().map(|s| s.as_str()).collect();

    print_table(&mut pager, 2, table_name.as_str(), column_names);

    // ----------------------------------------------------//

    // We only handle pages of type btree.
    // Rationale:  When Sqlite files are created from sessions that use only CREATE TABLE and INSERT statements,
    // the resulting files don't appear to have other page types.
    // TODO: support non-btree pages.

    let q = "SELECT a FROM record_test";
    let (input_tables, output_cols) = parse_select_statement(q);
    println!("output_cols: {}", output_cols.join(";"));
    println!("input_tables: {}", input_tables.join(";"));

    // TODO: Generate a sequence of instruction for the above statement, like:
    // 
    // let prog = vec![
    //      OpOpenTable("a", Cursor1),  // addr 0
    //      OpBreakIfDone(Cursor1), 
    //      // Maybe one op for each column to be selected?
    //      OpReadFromCursor(Cursor1, RowReg1),
    //      OpSelect(RowReg1, SelExpr),
    //      OpWriteToOutputStream(RowReg1),
    //      OpJumpToAddr(0),
    // ];

    // Explain the program to the user.
    // println!("{}", program.explain());

    // Define a VM to run the program:
    // let cursor = get_read_cursor(prog.input_table_name());
    // vm.reset();
    // vm.load_program(prog);
    // while true {
    //     match prog.step() {
    //         StepResult::Halt => break,
    //         StepResult::Result(row) => println!(row),
    //         StepResult::Processed => _,
    //     }
    // }

    // Define interface convenience functions to run a query while formatting the output to a text table, etc.

    // Make a REPL to run queries.
}
