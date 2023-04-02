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

// TODO: make an iterator that can walk across multiple pages.  To do that,
// the "btree iterator" needs to hold access to the pager.  This in turn requires.
// improvements to pager design, like:
// (pager object static lifetime, page interior mutability, concurrency controls)

// TODO: look into consolidating btree::PageReader and btree::CellIterator
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
fn _new_cell_iterator_for_page(pgr: &mut pager::Pager, pgnum: usize) -> btree::CellIterator {
    let page = match pgr.get_page_ro(pgnum) {
        Ok(p) => p,
        Err(e) => panic!("Error loading db page #{} : {}", pgnum, e),
    };
    let btree_start_offset = match pgnum {
        1 => 100,
        _ => 0,
    };
    btree::CellIterator::new(page, btree_start_offset)
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

    // Page 1 (the first page) is always a btree page, and it is the root of the schema.  It has references
    // to the roots of other btrees.
    const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
    let schema_table_columns = vec!["type", "name", "tbl_name", "rootpage", "sql"];

    // ----------------------------------------------------//
    println!("Schema Table");
    println!("| {} |", schema_table_columns.join(" | "));
    {
        let pr = new_reader_for_page(&mut pager, SCHEMA_BTREE_ROOT_PAGENUM);
        let _ = pr.check_header();
        //println!("{:?}", tl.check_header());

        // TODO: instead of printing the btree contents inside "get_btree_page", do using an iterator.
        //   e.g.   btree = btree::new(/* pager */ pager, /* pagenum of root */ SCHEMA_BTREE_ROOT_PAGENUM);
        //          for kv in btree.iter() { ... }
        pr.print_cell_contents();
    }

    // ----------------------------------------------------//
    // TODO: Get the table_name and page number from the schema table.
    let table_name = "TODO_get_table_name";
    let pagenum = 2;
    println!("Table {}", table_name);
    // TODO: Print the schema of this table by parsing the sql of the corresponding row of the schema table.
    {
        let pr = new_reader_for_page(&mut pager, pagenum);
        let _ = pr.check_header();
        //println!("{:?}", tl2.check_header());
        pr.print_cell_contents();
    }   

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
