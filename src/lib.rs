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
mod table;
pub mod typed_row;
extern crate pest;
#[macro_use]
extern crate pest_derive;

use sql_type::SqlType;
use table::Table;

// Page 1 (the first page) is always a btree page, and it is the root page of the schema table.
// It has references to the root pages of other btrees.
const SCHEMA_TABLE_NAME: &str = "sqlite_schema";
const SCHEMA_BTREE_ROOT_PAGENUM: pager::PageNum = 1;
const SCHEMA_SCHEMA: &str =
    "CREATE TABLE sqlite_schema (type text, name text, tbl_name text, rootpage integer, sql text)";
const SCHEMA_TABLE_TBL_NAME_COLIDX: usize = 2;
const SCHEMA_TABLE_ROOTPAGE_COLIDX: usize = 3;
const SCHEMA_TABLE_SQL_COLIDX: usize = 4;

/// TempTable collects query results into a temporary in-memory table of limited size.
///
/// # Design Rationale
/// In internal code, the database avoids making copies for efficiency, since queries can process many more rows than they
/// returns (JOINs, WHEREs without indexes, etc).
/// But when a query is complete, the results are copied.  That way, the callers does not have to deal with a reference lifetimes,
/// and we can release any the page locks as soon as possible.
/// The assumption here is that the caller is an interactive user who wants a limited number of rows (thousands).
/// For non-interactive bulk use, perhaps this needs to be revisted.
pub struct TempTable {
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
        // TODO: get rid of in favor of Table::to_temp_table()
        for (_, payload) in crate::btree::table::Iterator::new(SCHEMA_BTREE_ROOT_PAGENUM, pgr) {
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

// TODO: replace this with executing a query?
/// Print the Schema table to standard output.
pub fn print_schema(pager: &pager::Pager) -> anyhow::Result<()> {
    //let column_names = SCHEMA_TABLE_COL_NAMES.iter().map(|x| String::from(*x)).collect();
    //let column_types = SCHEMA_TABLE_COL_TYPES_STR.iter().map(|x| String::from(*x)).collect();
    let tbl = Table::open_read(pager, SCHEMA_TABLE_NAME)?;
    let tt: TempTable = tbl.to_temp_table()?;
    formatting::print_table_tt(&tt, false)?;
    Ok(())
}

pub fn run_query(ps: &pager::PagerSet, query: &str) -> anyhow::Result<()> {
    let tt = run_query_no_print(ps, query)?;
    crate::formatting::print_table_tt(&tt, false)?;
    Ok(())
}

pub fn run_query_no_print(ps: &pager::PagerSet, query: &str) -> anyhow::Result<crate::TempTable> {
    // Convert parse tree to AST.
    let ss: ast::SelectStatement = pt_to_ast::pt_select_statement_to_ast(query);
    // Convert the AST to IR.
    let ir: ir::Block = ast_to_ir::ast_select_statement_to_ir(&ss);
    // Execute the IR.
    let tt: crate::TempTable = ir_interpreter::run_ir(ps, &ir)?;
    Ok(tt)
}
