use crate::ir;
use crate::pager;
use anyhow::Result;

// TODO: instead printing directly from here:
// - return output headers and a table iterator, and then
// have the caller call formatting::print_table.
// TODO: return Result<> to allow for errors to propagate up to main without panicing.
// TODOX: should we eagerly or lazily convert from payload to typed?
// - Is converting very expensive?  Probably for Text and Blobs.
// - Project can project without converting, so we should allow it to Project a Scan without converting?

pub fn run_ir(ps: &pager::PagerSet, ir: &ir::Block) -> Result<crate::QueryOutputTable> {
    match ir {
        // TODO support root Project blocks.  This requires printing rows that
        // have constant exprs, dropping rows, etc.
        // The right way to do is to have formatting::print_table accept different kinds of iterators.
        ir::Block::Project(_) => panic!("IR that uses Project not supported yet."),
        ir::Block::ConstantRow(_) => {
            // TODO: Fill and return a QOT, whether this is a root or not (it is always a leaf).
            // the types can be used to do a conversion from the payload type to the SQL column type.
            unimplemented!("IR that uses ConstantRow not supported yet.");
        }
        ir::Block::Scan(s) => {
            // Question: what happens if the IR was built based on assumptions about the schema (e.g. number and types of columns),
            // and then the schema changed?  How about storing the message digest of the creation_sql in the Scan block and verify
            // it here.
            let table_name = s.tablename.as_str();
            let pager = ps.default_pager()?;
            let (root_pagenum, create_statement) =
                crate::get_creation_sql_and_root_pagenum(pager, table_name).unwrap_or_else(|| {
                    panic!("Should have looked up the schema for {}.", table_name)
                });
            let (_, column_names, column_types) =
                crate::pt_to_ast::parse_create_statement(&create_statement);
            let mut tci = crate::btree::table::Iterator::new(root_pagenum, pager);
            // TODO: make this a convenience function int typed_row.rs.
            crate::clone_and_cast_table_iterator(&mut tci, &column_names, &column_types)
        }
    }
}
