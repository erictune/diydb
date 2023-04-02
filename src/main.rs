// TODO: save this into github, before doing more to it.  It works, so lets keep it working.
// TODO: iterator-ify the table dumping.

// Intent is to have code structure which models Sqlite's architecture (https://www.sqlite.org/arch.html)
// "vfs" - opens and locks database files, providing Read and Seek interfaces to them, and the header (readonly initially).
mod vfs;
// "pager" - provides an array of pages, which may or may not be present in memory (seek and load on first access).  Uses a vfs.
mod pager;
// "btree" - provides iterator (cursor) to walk over btree elements (in future could support writes.).  Uses a pager to get at pages.
mod btree;
// "bytecode" - makes a program from a parse tree.  Uses btree cursors to the referenced tables.  Emits rows.
// "parser" - parses SQL statement into a parse tree, e.g. using https://pest.rs/book/examples/ini.html
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
    println!("Table {}", table_name);
    println!("| {} |", column_names.join(" | "));
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
            print!("{} |", rowid);
            for t in rhi {   
                print!(" {} |", serial_type::typecode_to_string(t)); 
            }
            println!("");
            print!("{} |", rowid);
            let hi = record::ValueIterator::new(&payload[..]);
            for (t, v) in hi {
                // TODO: map the iterator using a closure that calls to_string, and then intersperses the delimiters and then reduces into a string.
                // TODO: move cursor use into read_value_to_string, so it just uses a byte slice.
                print!(" {} |", serial_type::read_value_to_string(&t, &mut Cursor::new(v)));
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
    let schema_table_columns = vec!["type", "name", "tbl_name", "rootpage", "sql"];

    print_table(&mut pager, SCHEMA_BTREE_ROOT_PAGENUM, "sqlite_schema", schema_table_columns);

    // ----------------------------------------------------//

    // TODO: Iterate over the table_names and page numbers from the schema table.
    print_table(&mut pager, 2, "TODO_get_table_name", vec![]);

    // ----------------------------------------------------//

    // We only handle pages of type btree.
    // Rationale:  When Sqlite files are created from sessions that use only CREATE TABLE and INSERT statements,
    // the resulting files don't appear to have other page types.
    // TODO: support non-btree pages.

    // For now we only support this query.
    // let q = "SELECT * FROM a";

    // TODO move to parser.rs
    // let pt = parse(q);

    // let prog = codegen(pt);
    // program {
    //   set output header to input header;
    //   a: break if input cursor done;
    //   read from input cursor to registers
    //   write reg to output cursor
    //   jump a
    //  }

    // Explain the program to the user.
    // println!("{}", program.explain());

    // Define a function to run programs (a VM):
    // let cursor = get_read_cursor(prog.input_table_name());
    // prog.give_cursor(cursor);
    // prog.reset();
    // while true {
    //     match prog.step() {
    //         StepResult::Halt => break,
    //         StepResult::Result(row) => println!(row),
    //         StepResult::Processed => _,
    //     }
    // }

    // Define interface convenience functions to run a query while formatting the output to a text table, etc.

    // REPL to run queries.
}
