//! provides a temporary in-memory table of SQL data.
//! 
//! The table is implemented with rust native data structures, not SQLite-compatible structures.
//! The uses of TempTable are:
//!   - With tables `CREATE TEMP TABLE...` syntax (to be implemented).
//!   - To collect query results.
//!
//! # Design Rationale
//! In internal code, the database avoids making copies for efficiency, since queries can process many more rows than they
//! returns (JOINs, WHEREs without indexes, etc).
//! But when a query is complete, the results are copied.  That way, the callers does not have to deal with a reference lifetimes,
//! and we can release any the page locks as soon as possible.
//! The assumption here is that the caller is an interactive user who wants a limited number of rows (thousands).
//! For non-interactive bulk use, perhaps this needs to be revisted.

use crate::table_traits::TableMeta;
use crate::typed_row::Row;
use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;

use streaming_iterator::StreamingIterator;

#[derive(Debug, Clone)]
pub struct TempTable {
    pub rows: Vec<Row>,
    pub table_name: String,
    pub column_names: Vec<String>,
    pub column_types: Vec<SqlType>,
    pub strict: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Something went wrong appending: {0}")]
    AppendValidationError(#[from] crate::typed_row::Error),
}  


impl TableMeta for TempTable {
    fn column_names(&self) -> Vec<String> {
        self.column_names.clone()
    }
    fn column_types(&self) -> Vec<SqlType> {
        self.column_types.clone()
    }
    fn table_name(&self) -> String {
        self.table_name.clone()
    }
    fn strict(&self) -> bool {
        self.strict
    }
}

impl TempTable {
    pub fn streaming_iterator(&self) -> TempTableStreamingIterator {
        // Could not get streaming_iterator::convert or streaming_iterator::convert_ref to work here.
        TempTableStreamingIterator::new(self.rows.iter())
    }

    /// inserts a value in a table using the next unused rowid.
    pub fn append_row(&mut self, row: &Vec<SqlValue>) -> Result<(), Error> {
        crate::typed_row::validate_row_for_table(self, row).map_err(|e| Error::AppendValidationError(e))?;
        // TODO: store a rowid for consistency with regular Tables.
        self.rows.push(Row{ items: row.clone() });
        Ok(())
    }

    /// Printings out tables nicely.
    /// In the future, also csv output, etc.
    pub fn print(&self, detailed: bool) -> anyhow::Result<()> {
        println!(
            "   | {} |",
            self.column_names
                .iter()
                .map(|x| format!("{:15}", x))
                .collect::<Vec<String>>()
                .join(" | ")
        );
        if detailed {
            println!(
                "   | {} |",
                self.column_types
                    .iter()
                    .map(|x| format!("{:15}", x))
                    .collect::<Vec<String>>()
                    .join(" | ")
            );
        }
        {
            for tr in self.rows.iter() {
                println!(
                    "   | {} |",
                    tr.items
                        .iter()
                        .map(|x| format!("{:15}", x))
                        .collect::<Vec<String>>()
                        .join(" | ")
                );
            }
        }
        Ok(())
    }

}

/// iterates over the rows of a TempTable .
/// The lifetime is bound by the lifetime of the TempTable.
pub struct TempTableStreamingIterator<'a> {
    it: std::slice::Iter<'a, Row>,
    item: Option<Row>,
}
impl<'a> TempTableStreamingIterator<'a> {
    fn new(it: std::slice::Iter<'a, Row>) -> TempTableStreamingIterator<'a> {
        TempTableStreamingIterator { it, item: None }
    }
}

impl<'a> StreamingIterator for TempTableStreamingIterator<'a> {
    type Item = Row;

    #[inline]
    fn advance(&mut self) {
        self.item = self.it.next().map(|r| Row{ items: r.items.clone(), })
    }

    #[inline]
    fn get(&self) -> Option<&Row> {
        self.item.as_ref()
    }
}

#[test]
fn test_temp_table() {
    use crate::sql_value::SqlValue;
    let tbl = TempTable {
        rows: vec![Row {
            items: vec![SqlValue::Int(1)],
        }],
        table_name: "test".to_string(),
        column_names: vec!["b".to_string()],
        column_types: vec![SqlType::Int],
        strict: true,
    };
    assert_eq!(tbl.column_names(), vec![String::from("b")]);
    assert_eq!(tbl.column_types(), vec![SqlType::Int]);
    let mut it = tbl.streaming_iterator();
    //let mut it = &mut cvt as &dyn streaming_iterator::StreamingIterator<Item = &Row>;
    it.advance();
    assert_eq!(
        it.get(),
        Some(&Row {
            items: vec![SqlValue::Int(1)]
        })
    );
    it.advance();
    assert_eq!(it.get(), None);
}