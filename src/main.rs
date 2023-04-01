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

use crate::vfs::DbAttachment;

fn main() {
    // Open the database file. This is a file in sqlite3 format.
    let mut vfs = DbAttachment::open().expect("Should have opened the DB");

    // Read db file header to confirm it is a valid file, and how many and what size pages it has.
    let dbhdr = vfs.get_header().unwrap();
    println!("Opened DB File.  Interesting Header bits: {:?}", dbhdr);
    // TODO: move checking magic and reading creation-time fields (like page size) into vfs.rs.
    //       but move access to modifiable fields to use a Pager from pager.rs, since that will require locking.

    let mut pager = pager::Pager::new(vfs);

    // Page 1 (the first page) is always a btree page, and it is the root of the schema.  It has references
    // to the roots of other btrees.
    const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
    let schema_table_columns = vec!["type", "name", "tbl_name", "rootpage", "sql"];

    // Dump the the database btree.
    let pagenum = SCHEMA_BTREE_ROOT_PAGENUM;
    // TODO: consider putting this into btree.rs

    print!("| ");
    for column in schema_table_columns.iter() {
        print!(" {} |", *column);
    }
    println!("");

    let page = match pager.get_page_ro(pagenum) {
        Ok(page) => page,
        Err(e) => panic!("Error loading db page #{} : {}", pagenum, e),
    };
    let btree_start_offset = match pagenum {
        1 => 100,
        _ => 0,
    };

    // TODO: consider making this take a pager and a root pagenumber instead.
    //   e.g.   btree::get_btree_page(/* pager */ pager, /* pagenum of root */ SCHEMA_BTREE_ROOT_PAGENUM);
    match btree::get_btree_page(page, btree_start_offset) {
        Ok(ph) => {
            println!("{:?}", ph);
        }
        Err(e) => {
            panic!("Error processing btree #{} : {}", pagenum, e);
        }
    }
    // TODO: instead of printing the btree contents inside "get_btree_page", do these things:
    // - provide an iterator over btree kvs
    //   e.g.   btree = btree::new(/* pager */ pager, /* pagenum of root */ SCHEMA_BTREE_ROOT_PAGENUM);
    //          for kv in btree.iter() { ... }
    // - provide an iterator over record elements (returning the type, length, and raw bytes)
    //   e.g.   record = record::new(kv);
    // - provide a helper function to get an integer type record as an i64 integer (?)
    //   e.g.   for elem in record { print!("{}", elem.type_as_string()) }
    // - provide a helper function to record in the form of a printable string.
    //   e.g.   for elem in record { print!("{}", elem.value_as_string()) }

    // Dump a table other than than schema table.

    // TODO: instead of hardcoding that there is another table with root page 2, write a function to walk the records of the schema page,
    // to find the root pages of all "table" records, and then we can dump those.
    // TODO: for ech of those rows, parse the sql column of "sql" to extract the list of column names.  Print those as a header to this btree.
    let page2 = match pager.get_page_ro(2) {
        Ok(page) => page,
        Err(e) => panic!("Error loading db page #{} : {}", 2, e),
    };
    let btree_start_offset2 = match 2 {
        1 => 100,
        _ => 0,
    };

    match btree::get_btree_page(page2, btree_start_offset2) {
        Ok(ph) => {
            println!("{:?}", ph);
        }
        Err(e) => {
            panic!("Error processing btree #{} : {}", pagenum, e);
        }
    }

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
