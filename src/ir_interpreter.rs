//! executes SQL intermediate representation (IR).

use crate::ir;
use crate::pager;
use crate::TempTable;
use anyhow::Result;

use crate::pager::PagerSet;

use streaming_iterator::StreamingIterator;

use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;

use crate::ast;
use crate::table::Table;
use crate::typed_row::build_row;
use crate::typed_row::Row;

fn ast_constant_to_sql_value(c: &ast::Constant) -> SqlValue {
    match c {
        ast::Constant::Int(i) => SqlValue::Int(*i),
        ast::Constant::String(s) => SqlValue::Text(s.clone()),
        ast::Constant::Real(f) => SqlValue::Real(*f),
        ast::Constant::Bool(b) => SqlValue::Int(match b {
            true => 1,
            false => 0,
        }),
        ast::Constant::Null() => SqlValue::Null(),
    }
}

fn ast_constant_to_sql_type(c: &ast::Constant) -> SqlType {
    match c {
        ast::Constant::Int(_) => SqlType::Int,
        ast::Constant::String(_) => SqlType::Text,
        ast::Constant::Real(_) => SqlType::Real,
        ast::Constant::Bool(_) => SqlType::Int,
        ast::Constant::Null() => SqlType::Int, // Not clear what to do in this case.  Need Unknown type?
    }
}

// Explanation: why aren't ir::Block and *XB the same?
// 1) so that lifetimes don't pollute the IR code.  That could get confusing when applying transformations on it.
// 2) so that if we replace Executions Blocks with code generation, the IR does not change what it holds.

/// holds state for execution of a Project block.
struct ProjectXB {
    // TODO: the ProjectXB needs to have a list of which columns it will take from its input, and where it will
    // put them in the output.
    // let actions:  Vec<Action>;  // actions.len() == output_row.len().
    // enum Action {
    //     Take(usize), // let Take(x) ; 0 <= x < input_row.len(); take index x from input row.
    //     Constant(SqlValue), // put constant value into output row.
    //     // later: Expr(),
    // }
    // The new() will need to input's column names and types.
    // also construct the output column names and types at the same time as constructing the actions.
    // the nth element of actions tells how to construct the nth cell of the output row.
    // TODO: expand stars to the list of input.column_names.
    // TODO: check each projected column name to see if it is in the input Table's rows.
}

// TODO: rename constantrow to constanttable since it could have several rows, like in `select * from (select 1 union select 2);`
/// holds state for execution of a ConstantRow block.
struct ConstantRowXB {
    temp_table: Box<TempTable>,
}

/// holds state for execution of a Scan block.
struct ScanXB<'a> {
    table: Table<'a>,
}

/// holds any of the types of execution blocks.
enum AnyXB<'a> {
    C(ConstantRowXB),
    S(ScanXB<'a>),
    P(ProjectXB),
}

impl ConstantRowXB {
    pub fn new(temp_table: Box<TempTable>) -> ConstantRowXB {
        ConstantRowXB {
            temp_table: Box::new(*temp_table),
        }
    }
}

impl<'a, 'b> ScanXB<'a> {
    pub fn open(tablename: &'b str, ps: &'a PagerSet) -> Result<ScanXB<'a>> {
        Ok(ScanXB {
            table: Table::open_read(ps.default_pager()?, tablename)?,
        })
    }
}

/// iterates over the rows of a Project execution Block.
struct ProjectStreamingIterator {}

/// iterates over the rows of a Scan execution Block.
/// The lifetime is bound by the lifetime of the pager used in the table::Iterator.
struct ScanStreamingIterator<'p> {
    column_types: Vec<SqlType>,
    it: crate::btree::table::Iterator<'p>,
    item: Option<Row>,
}

/// There are two reasons why we chose to implement StreamingIterator instead of
/// Iterator.
/// 1) so project can build a local row to return, and allow it to be used by reference,
///    but not need to retain all computed rows.
/// 2) to limit lifetime of borrows from Scans to allow freeing/unlocking pages behind.)
///
impl StreamingIterator for ProjectStreamingIterator {
    type Item = Row;
    fn advance(&mut self) {
        // TODO: advane needs to build a local row using the steps we decided on when creating the
        // ProjectXB.
    }

    // and then return the local row on get.
    fn get(&self) -> Option<&Self::Item> {
        None
    }

    //have a list of which columns it will take from its input, and where it will
    // put them in the output.  Basically a vector length n
    // where the nth element of the vector tells how to build the nth row of the output row.
    // The how is one of "return this constant value", or "take the kth column of the input".
    // Later it will include expressions.
}

impl<'p> StreamingIterator for ScanStreamingIterator<'p> {
    type Item = Row;
    fn advance(&mut self) {
        self.item = match self.it.next() {
            Some(x) => match build_row(&self.column_types, x.1) {
                Ok(row) => Some(row),
                // TODO: figure out a way to avoid panicing when the conversion fails, like truncating,
                // returning a result, NULL value, etc.
                Err(e) => panic!("Unable to convert row to table types: {:}", e),
            },
            None => None,
        };
    }
    fn get(&self) -> Option<&Self::Item> {
        self.item.as_ref()
    }
}

/*
I've tried to this working with Traits instead of matches and enums.
But I got tripped by with the lifetime of Box<dyn Trait> references.
*/

/// provides a stream of rows, with column names and types.
///
/// A StreamedTable that the iterator is created for has a lifetime at least 'a.
/// The iterator itself is good for lifetime 'b.
/// We don't want the iterator used longer than the underlying table, so 'a: 'b.
trait StreamedTable<'a, 'b> {
  fn streaming_iterator(&'a self) -> Box<dyn StreamingIterator<Item=Row> + 'b>
  where 'a: 'b;
  fn column_names(&self) -> Vec<String>;
  fn column_types(&self) -> Vec<SqlType>;
}

impl<'a, 'b> StreamedTable<'a, 'b> for ConstantRowXB {
    fn streaming_iterator(&'a self) -> Box<dyn StreamingIterator<Item = Row> + 'b>
    where
        'a: 'b,
    {
        return Box::new(streaming_iterator::convert_ref(self.temp_table.rows.iter()));
    }
    fn column_names(&self) -> Vec<String> {
        return self.temp_table.column_names.clone();
    }
    fn column_types(&self) -> Vec<SqlType> {
        return self.temp_table.column_types.clone();
    }
}

impl<'a, 'b> StreamedTable<'a, 'b> for ScanXB<'a> {
    fn streaming_iterator(&'a self) -> Box<dyn StreamingIterator<Item = Row> + 'b>
    where
        'a: 'b,
    {
        Box::new(ScanStreamingIterator {
            column_types: self.table.column_types().clone(),
            it: self.table.iter(),
            item: None,
        })
    }
    fn column_names(&self) -> Vec<String> {
        return self.table.column_names().clone();
    }
    fn column_types(&self) -> Vec<SqlType> {
        return self.table.column_types().clone();
    }
}

impl<'a, 'b> StreamedTable<'a, 'b> for ProjectXB {
    fn streaming_iterator(&'a self) -> Box<dyn StreamingIterator<Item = Row> + 'b> {
        return Box::new(ProjectStreamingIterator {});
    }
    fn column_names(&self) -> Vec<String> {
        return vec![];
    } // TODO: do properly by reading the rows from the table, using a pager, which requires a prepare step.
    fn column_types(&self) -> Vec<SqlType> {
        return vec![];
    } // TODO: do properly, by getting the schema.
}

// TODO: in prepare_ir, for a Project Block, after having prepared its children, prepare it by getting the column_names and types -
// compute the output cols by expanding Stars.  Compute the output types based on the types of any expressions.
impl<'a, 'b, 'c> AnyXB<'a>
where
    'a: 'b,
    'a: 'c,
{
    fn into_box(self) -> Box<dyn StreamedTable<'a, 'b> + 'c> {
        match self {
            AnyXB::C(foo) => Box::new(foo),
            AnyXB::P(foo) => Box::new(foo),
            AnyXB::S(foo) => Box::new(foo),
        }
    }
}

/// Run an IR representation of a query.
pub fn run_ir(ps: &pager::PagerSet, ir: &ir::Block) -> Result<crate::TempTable> {
    let xb = match ir {
        ir::Block::Project(_) => {
            return Err(anyhow::anyhow!("IR that uses Project not supported yet."))
        }
        ir::Block::ConstantRow(cr) => AnyXB::C(ConstantRowXB::new(Box::new(TempTable {
            rows: vec![Row {
                items: cr.row.iter().map(ast_constant_to_sql_value).collect(),
            }],
            column_names: (0..cr.row.len()).map(|i| format!("_f{i}")).collect(),
            column_types: cr.row.iter().map(ast_constant_to_sql_type).collect(),
        }))),
        ir::Block::Scan(s) => {
            // TODO: lock table at this point so schema does not change.
            // TODO: if we previously loaded the schema speculatively during IR optimization, verify unchanged now, e.g. using a message hash.
            AnyXB::S(ScanXB::open(s.tablename.as_str(), ps)?)
        }
    };
    // I'd like to use Traits here but I haven't been able to get Box<dyn Trait> to do what I wanted.
    match xb {
        AnyXB::C(c) => {
            let mut it = c.streaming_iterator();
            let mut rows = vec![];
            while let Some(item) = it.next() {
                rows.push(item.clone());
                // TODO: limit the number of rows taken, and return an error result if too many.
            }
            return Ok(crate::TempTable {
                rows: rows,
                column_names: c.column_names().clone(),
                column_types: c.column_types().clone(),
            });
        }
        AnyXB::P(_) => return Err(anyhow::anyhow!("IR that uses Project not supported yet.")),
        AnyXB::S(s) => {
            let mut it = s.streaming_iterator();
            let mut rows = vec![];
            while let Some(item) = it.next() {
                rows.push(item.clone());
                // TODO: limit the number of rows taken, and return an error result if too many.
            }
            return Ok(crate::TempTable {
                rows: rows,
                column_names: s.column_names().clone(),
                column_types: s.column_types().clone(),
            });
        }
    }
}
