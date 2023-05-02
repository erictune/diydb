mod ast;
mod ast_to_ir;
mod btree;
mod dbheader;
mod formatting;
mod ir;
mod ir_interpreter;
pub mod pager;
pub mod parser;
mod pt_to_ast;
mod record;
mod serial_type;
pub mod sql_type;
pub mod sql_value;
pub mod typed_row;
extern crate pest;
#[macro_use]
extern crate pest_derive;

use std::str::FromStr;
use sql_type::SqlType;
use typed_row::{TypedRow, RowCastingError, RawRowCaster};

// Page 1 (the first page) is always a btree page, and it is the root page of the schema table.
// It has references to the root pages of other btrees.
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
const SCHEMA_SCHEMA: &str =
    "CREATE TABLE sqlite_schema (type text, name text, tbl_name text, rootpage integer, sql text)";
const SCHEMA_TABLE_TBL_NAME_COLIDX: usize = 2;
const SCHEMA_TABLE_ROOTPAGE_COLIDX: usize = 3;
const SCHEMA_TABLE_SQL_COLIDX: usize = 4;

/// QueryOutputTable collects results into a temporary in-memory table of limited size
///
/// # Design Rationale
/// In internal code, the database avoids making copies for efficiency, since queries can process many more rows than they
/// returns (JOINs, WHEREs without indexes, etc).
/// But when a query is complete, the results are copied.  That way, the callers does not have to deal with a reference lifetimes,
/// and we can release any the page locks as soon as possible.
/// The assumption here is that the caller is an interactive user who wants a limited number of rows (thousands).
/// For non-interactive bulk use, perhaps this needs to be revisted.
pub struct QueryOutputTable {
    pub rows: Vec<typed_row::TypedRow>,
    pub column_names: Vec<String>,
    pub column_types: Vec<SqlType>,
}

/// Get the root page number for, and the SQL CREATE statement used to create `table_name`.
pub fn get_creation_sql_and_root_pagenum(
    pgr: &pager::Pager,
    table_name: &str,
) -> Option<(pager::PageNum, String)> {
    if table_name == SCHEMA_TABLE_NAME {
        return Some((SCHEMA_BTREE_ROOT_PAGENUM, String::from(SCHEMA_SCHEMA)));
    } else {
        let record_iterator: btree::table::Iterator = new_table_iterator(pgr, SCHEMA_BTREE_ROOT_PAGENUM);
        for (_, payload) in record_iterator {
            let vi = record::ValueIterator::new(payload);
            let row = vi.collect::<Vec<(i64, &[u8])>>();
            let this_table_name = serial_type::value_to_string(
                &row[SCHEMA_TABLE_TBL_NAME_COLIDX].0,
                row[SCHEMA_TABLE_TBL_NAME_COLIDX].1,
            )
            .unwrap();
            if this_table_name != table_name {
                continue;
            }
            let root_pagenum = serial_type::value_to_i64(
                &row[SCHEMA_TABLE_ROOTPAGE_COLIDX].0,
                row[SCHEMA_TABLE_ROOTPAGE_COLIDX].1,
                false,
            )
            .expect("Should have gotten root page number from schema table.")
                as pager::PageNum;
            let creation_sql = serial_type::value_to_string(
                &row[SCHEMA_TABLE_SQL_COLIDX].0,
                row[SCHEMA_TABLE_SQL_COLIDX].1,
            )
            .unwrap();
            return Some((root_pagenum, creation_sql));
        }
    }
    None
}

pub fn page_and_offset_for_pagenum(pgr: &pager::Pager, pgnum: usize) -> (&Vec<u8>, pager::PageNum) {
    let page: &Vec<u8> = match pgr.get_page_ro(pgnum) {
        Ok(p) => p,
        Err(e) => panic!("Error loading db page #{} : {}", pgnum, e),
    };
    let btree_start_offset = match pgnum {
        1 => 100,
        _ => 0,
    };
    (page, btree_start_offset)
}

pub fn new_table_iterator(pgr: &pager::Pager, pgnum: usize) -> btree::table::Iterator {
    crate::btree::table::Iterator::new(pgnum, pgr)
}

fn print_table(
    pgr: &pager::Pager,
    root_pgnum: usize,
    col_names: Vec<String>,
    col_types: Vec<String>,
    detailed: bool,
) -> anyhow::Result<()> {
    {
        let (page, offset) = page_and_offset_for_pagenum(pgr, root_pgnum);
        let hdr = btree::header::check_header(page, offset);
        if detailed {
            println!("{:?}", hdr);
        }
    }
    let mut tci = new_table_iterator(pgr, root_pgnum);
    let qot = clone_and_cast_table_iterator(&mut tci, &col_names, &col_types)?;
    // TODO: want "connection" in between these lines.
    // While we don't want copying or buffering inside the execution, it is okay to buffer lines going over the connection.
    // The execution engine can't be blocked by the printing, which might stall due to pagination, etc.  Therefore,
    // an iterator might not be right, and at the least some kind of buffer is needed.
    // There might need to be a limit to the buffer size though.
    formatting::print_table_qot(&qot, detailed)?;
    Ok(())
}

// TODO: replace this with executing a query?
/// Print the Schema table to standard output.
pub fn print_schema(pager: &pager::Pager) -> anyhow::Result<()> {
    let table_name = "sqlite_schema";
    let (root_pagenum, create_statement) = get_creation_sql_and_root_pagenum(pager, table_name)
        .unwrap_or_else(|| panic!("Should have looked up the schema for {}.", table_name));
    let (_, column_names, column_types) = pt_to_ast::parse_create_statement(&create_statement);

    print_table(pager, root_pagenum, column_names, column_types, false)?;
    Ok(())
}

pub fn run_query(pager: &pager::Pager, query: &str) -> anyhow::Result<()> {
    let qot = run_query_no_print(pager, query)?;
    crate::formatting::print_table_qot(&qot, false)?;
    Ok(())
}

pub fn run_query_no_print(
    pager: &pager::Pager,
    query: &str,
) -> anyhow::Result<crate::QueryOutputTable> {
    // Convert parse tree to AST.
    let ss: ast::SelectStatement = pt_to_ast::pt_select_statement_to_ast(query);
    // Convert the AST to IR.
    let ir: ir::Block = ast_to_ir::ast_select_statement_to_ir(&ss);
    // Execute the IR.
    let qot: crate::QueryOutputTable = ir_interpreter::run_ir(pager, &ir)?;
    Ok(qot)
}

fn clone_and_cast_table_iterator<'f>(
    ti: &'f mut crate::btree::table::Iterator<'f>,
    column_names: &Vec<String>,
    column_types: &Vec<String>,
) -> Result<crate::QueryOutputTable, anyhow::Error> {
    let column_types: Vec<SqlType> = column_types.iter().map(|s| SqlType::from_str(s.as_str()).unwrap()).collect();
    let column_types2 = column_types.clone();
    let r: Result<Vec<TypedRow>, RowCastingError> =
        RawRowCaster::new(column_types, ti).collect();
    let r = r?;
    Ok(crate::QueryOutputTable {
        // TODO: take() a limited number of rows when collect()ing them, and return error if they don't fit?
        rows: r,
        column_names: column_names.clone(),
        column_types: column_types2,
    })
}
