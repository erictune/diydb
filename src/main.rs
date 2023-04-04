// TODO: implement PrintTable using an inner method that emits rows and an outer method that formats (width, headers) and prints.
//       subsequently, the inner method could be implemented as a "do select" method that interprets a parsed select statement and emits
//       rows.

// TODO: move below comments to the readme.md under code structure.

// System Layers
//
// Intent is to have code structure which models Sqlite's architecture (https://www.sqlite.org/arch.html)
// "vfs" - opens and locks database files, providing Read and Seek interfaces to them, and the header (readonly initially).
mod vfs;
// "pager" - provides an array of pages, which may or may not be present in memory (seek and load on first access).  Uses a vfs.
mod pager;
// "btree" - provides iterator (cursor) to walk over btree elements (in future could support writes.).  Uses a pager to get at pages.
mod btree;
// "parser" - parses SQL statement into a parse tree, e.g. using https://pest.rs/book/examples/ini.html
// We use pest parser generator.
mod parser;
extern crate pest;
#[macro_use]
extern crate pest_derive;

// "bytecode" - makes a program from a parse tree.  Uses btree cursors to the referenced tables.  Emits rows.
// "interface" - REPL loop that accepts a sql query to do on the file, using parser and vfs, and commands to open databases, etc.
// Formats emitted rows to csv file or stdout.

mod record;
mod serial_type;

// We only handle pages of type btree.
// Rationale:  When Sqlite files are created from sessions that use only CREATE TABLE and INSERT statements,
// the resulting files don't appear to have other page types.
// TODO: support non-btree pages.

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

fn new_table_leaf_cell_iterator_for_page(
    pgr: &mut pager::Pager,
    pgnum: usize,
) -> btree::TableLeafCellIterator {
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

fn print_table(
    pgr: &mut pager::Pager,
    root_pgnum: usize,
    table_name: &str,
    col_names: Vec<&str>,
    col_types: Vec<&str>,
    detailed: bool,
) {
    println!("Full Dump of Table {}", table_name);
    {
        let pr = new_reader_for_page(pgr, root_pgnum);
        let hdr = pr.check_header();
        if detailed {
            println!("{:?}", hdr);
        }
    }
    println!(
        "   | {} |",
        col_names
            .iter()
            .map(|x| format!("{:15}", x))
            .collect::<Vec<String>>()
            .join(" | ")
    );
    if detailed {
        println!(
            "   | {} |",
            col_types
                .iter()
                .map(|x| format!("{:15}", x))
                .collect::<Vec<String>>()
                .join(" | ")
        );
    }

    {
        let tci = new_table_leaf_cell_iterator_for_page(pgr, root_pgnum);
        for (rowid, payload) in tci {
            let rhi = record::HeaderIterator::new(payload);
            if detailed {
                print!("{:2} |", rowid);
                for t in rhi {
                    print!(" {:15} |", serial_type::typecode_to_string(t));
                }
                println!("");
            }
            print!("{:2} |", rowid);
            let hi = record::ValueIterator::new(&payload[..]);
            for (t, v) in hi {
                // TODO: map the iterator using a closure that calls to_string, and then intersperses the delimiters and then reduces into a string.
                // TODO: move cursor use into read_value_to_string, so it just uses a byte slice.
                print!(
                    " {:15} |",
                    serial_type::read_value_to_string(&t, &mut Cursor::new(v))
                );
            }
            println!("");
        }
    }
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
    let schema_table_column_names = vec!["type", "name", "tbl_name", "rootpage", "sql"];
    let schema_table_column_types = vec!["text", "text", "text", "integer", "text"];

    print_table(
        &mut pager,
        SCHEMA_BTREE_ROOT_PAGENUM,
        "sqlite_schema",
        schema_table_column_names,
        schema_table_column_types,
        false,
    );

    // ----------------------------------------------------//
    // TODO: get from above table:
    let create_statement = "CREATE TABLE record_test ( a int, b int, c real, d string, e int)";
    // TODO: separate types and column names.  Just print the names.  Use the types to cast the serial_types.
    let (table_name, column_names, column_types) = parser::parse_create_statement(create_statement);
    // TODO: make the print_table function accept a vector<T> where T is str& or String?
    // let column_names_as_str = column_names.iter().into_iter().map(|s| s.as_str()).collect();

    print_table(
        &mut pager,
        2,
        table_name.as_str(),
        column_names,
        column_types,
        false,
    );

    // ----------------------------------------------------//

    let q = "SELECT a FROM record_test";
    let (input_tables, output_cols) = parser::parse_select_statement(q);
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
