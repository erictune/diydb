mod btree;
mod formatting;
// TODO: mod pager should not be public.  It should be allocated when you open a file, and be private to the DbAttachement.
pub mod pager;
pub mod parser;
mod record;
mod serial_type;
pub mod vfs;

extern crate pest;
#[macro_use]
extern crate pest_derive;

// Page 1 (the first page) is always a btree page, and it is the root page of the schema table.
// It has references to the root pages of other btrees.
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
const SCHEMA_SCHEMA: &str =
    "CREATE TABLE sqlite_schema (type text, name text, tbl_name text, rootpage integer, sql text)";
const SCHEMA_TABLE_TBL_NAME_COLIDX: usize = 2;
const SCHEMA_TABLE_ROOTPAGE_COLIDX: usize = 3;
const SCHEMA_TABLE_SQL_COLIDX: usize = 4;
const SCHEMA_TABLE_NUMCOLS: usize = 5;

/// Get the SQL CREATE statement used to create `table_name`.
fn get_creation_sql_and_root_pagenum(
    pgr: &mut pager::Pager,
    table_name: &str,
) -> Option<(pager::PageNum, String)> {
    if table_name == SCHEMA_TABLE_NAME {
        return Some((SCHEMA_BTREE_ROOT_PAGENUM, String::from(SCHEMA_SCHEMA)));
    } else {
        let record_iterator = new_table_leaf_cell_iterator_for_page(pgr, SCHEMA_BTREE_ROOT_PAGENUM);
        for (_, payload) in record_iterator {
            let vi = record::ValueIterator::new(&payload[..]);
            let mut idx = 0_usize;
            let mut root_pagenum: Option<pager::PageNum> = None;
            let mut creation_sql: Option<String> = None;
            for (t, v) in vi {
                match idx {
                    SCHEMA_TABLE_TBL_NAME_COLIDX => {
                        if serial_type::value_to_string(&t, v) != table_name {
                            continue;
                        }
                    }
                    SCHEMA_TABLE_ROOTPAGE_COLIDX => {
                        let tmp = serial_type::value_to_i64(&t, v, false).unwrap();
                        root_pagenum = Some(tmp as pager::PageNum);
                    }
                    SCHEMA_TABLE_SQL_COLIDX => {
                        creation_sql = Some(serial_type::value_to_string(&t, v));
                    }
                    _ => (),
                }
                idx += 1;
            }
            if idx != SCHEMA_TABLE_NUMCOLS {
                panic!("Invalid sqlite_schema table.")
            }
            return Some((
                root_pagenum.expect("Should have gotten root page number from schema table."),
                creation_sql.expect("Should have gotten creation sql from schema table."),
            ));
        }
    }
    None
}

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
    {
        let pr = new_reader_for_page(pgr, root_pgnum);
        let hdr = pr.check_header();
        if detailed {
            println!("{:?}", hdr);
        }
    }
    let mut tci = new_table_leaf_cell_iterator_for_page(pgr, root_pgnum);
    formatting::print_table(&mut tci, table_name, col_names, col_types, detailed);
}

/// Print the Schema table to standard output.
pub fn print_schema(pager: &mut pager::Pager) {
    let table_name = "sqlite_schema";
    let (root_pagenum, create_statement) = get_creation_sql_and_root_pagenum(pager, table_name)
        .expect(format!("Should have looked up the schema for {}.", table_name).as_str());
    let (_table_name2, column_names, column_types) =
        parser::parse_create_statement(&create_statement);

    print_table(
        pager,
        root_pagenum,
        table_name,
        column_names,
        column_types,
        false,
    );
}

pub fn run_query(pager: &mut pager::Pager, query: &str) {
    let (input_tables, output_cols) = parser::parse_select_statement(query);
    println!("output_cols: {}", output_cols.join(", "));
    println!("input_tables: {}", input_tables.join(", "));

    // Execute the query (TODO: use code generation.)
    if input_tables.len() > 1 {
        panic!("We don't support multiple table queries.")
    };
    if input_tables.len() < 1 {
        panic!("We don't support selects without FROM.")
    };
    let table_name = input_tables[0];
    if output_cols.len() != 1 || output_cols[0] != "*" {
        panic!("We don't support selecting specific columns.")
    }
    let (root_pagenum, create_statement) = get_creation_sql_and_root_pagenum(pager, table_name)
        .expect(format!("Should have looked up the schema for {}.", table_name).as_str());
    let (_table_name2, column_names, column_types) =
        parser::parse_create_statement(&create_statement);
    print_table(
        pager,
        root_pagenum,
        table_name,
        column_names,
        column_types,
        false,
    );

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
}
