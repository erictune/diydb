//! provides access to table records in a typed form.
//! Table records in SQLite are stored as bytes, with a serial_type code, and need to be converted
//! to Rust types before they can be used in expressions.
//! Some conversions are fallible.  Any failure to convert any element in a row is treated as a failure to convert the entire row.
//! Null values are not a conversion failure.
//! Any value could possibly be Null (NOT NULL is not handled here.)
//! Any io errors reading the row are also treated as failures to read the entire row.
//! An error returned by the underlying table iterator (such as an error in the btree structure)
//! will manifest as one row having an error, fewer rows being returned.
// TODO: It might be better to treat casting errors differently from errors in the underlying iterator.
use crate::sql_type::SqlType;
use crate::sql_value::SqlValue;
#[allow(unused_imports)] // Needed fot trait FromStr
use std::str::FromStr;

/// can hold a sequence of values of any of the SQL types (sqlite supported subset), along with a rowid.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedRow {
    pub row_id: i64,
    pub items: Vec<SqlValue>,
}

#[derive(thiserror::Error, Debug)]
pub enum RowCastingError {
    #[error("One or more rows were not castable due to, first one given.")]
    OneOrMoreRowsNotCastable(#[from] crate::serial_type::Error),
    #[error("Type array and value array length mismatch.")]
    ArrayLenMismatch,

}

fn build_typed_row(row_id: i64, column_types: &Vec<SqlType>, record: &[u8]) -> Result<TypedRow, RowCastingError> {
    use crate::record::ValueIterator;
    let mut ret: Vec<SqlValue> = vec![];
    for (i, (serty, bytes)) in ValueIterator::new(record).enumerate() {
        if i > column_types.len() { return Err(RowCastingError::ArrayLenMismatch) }
        match crate::serial_type::value_to_sql_typed_value(&serty, column_types[i], bytes) {
            Ok(v) => ret.push(v),
            Err(e) => return Err(RowCastingError::OneOrMoreRowsNotCastable(e)),
        }
    }
    Ok(TypedRow {
        row_id: row_id,
        items: ret.to_vec(),
    })
}

#[test]
fn test_build_typed_row() {
    use SqlValue::*;
    // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
    let test_record: &[u8] = &[
        0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f, 0x54,
        0x65, 0x6e,
    ];
    let column_types: Vec<SqlType> = vec![SqlType::Int, SqlType::Int, SqlType::Real, SqlType::Text, SqlType::Int];
    let tr = build_typed_row(1, &column_types, &test_record).unwrap();
    assert_eq!(tr.row_id, 1);
    assert_eq!(tr.items.len(), 5);
    assert_eq!(tr.items[0], Int(0));
    assert_eq!(tr.items[1], Int(1));
    assert_eq!(tr.items[2], Real(3.1415));
    assert_eq!(tr.items[3], Text(String::from("Ten")));
    assert_eq!(tr.items[4], Null());
}

/// provides iterator adapter to convert an iterator over raw table rows into an iterator over typed rows (Vec<SqlTypedValue>).
/// Note that the type of the rows is based on the serial type of the record.  The caller still needs to check that the
/// returned enum variant in the row matches the schema-specified value, if this matters to the caller.
pub struct RawRowCaster<'a> {
    column_types: Vec<SqlType>,
    // I tried to make this generic over type T, to hide our internal implementation of
    // a btree table iterator.  I have tried things like:
    // `raw_row_iter() -> impl Iterator<Item = &RawRow>`
    // and
    // `new(it: T)` where `T: Iterator<Item = (i64, Vec<u8>)> >`.
    // However, when I tried to do this, I got rust errors about
    // assocaited type bounds being unstable.
    // fn raw_row_iter(&self) -> &btree::table::Iterator { self.rows.iter() }
    it: &'a mut crate::btree::table::Iterator<'a>,
}

impl<'a> RawRowCaster<'a> {
    pub fn new(column_types: Vec<SqlType>, it: &'a mut crate::btree::table::Iterator<'a>) -> Self {
        Self { column_types, it }
    }
}

impl<'a> Iterator for RawRowCaster<'a> {
    type Item = Result<TypedRow, RowCastingError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next() {
            None => None,
            Some(r) => Some(build_typed_row(r.0, &self.column_types, r.1)),
        }
    }
}

#[cfg(test)]
fn path_to_testdata(filename: &str) -> String {
    std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set")
        + "/resources/test/"
        + filename
}

#[test]
fn test_raw_row_caster() {
    use SqlValue::*;
    // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
    let path = path_to_testdata("minimal.db");
    let mut pager =
        crate::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager.");
    // let tbl = Table::open_read(table_name)

    let (pgnum, csql) = crate::get_creation_sql_and_root_pagenum(&pager, "a").unwrap();
    // TODO: put this into get_creation_sql_and_root_pagenum
    let (_, _, column_types) = crate::pt_to_ast::parse_create_statement(&csql);
    let column_types: Vec<SqlType> = column_types.iter().map(|s| SqlType::from_str(s.as_str()).unwrap()).collect();
    // let ti = tbl.iter()
    let mut ti = crate::new_table_iterator(&pager, pgnum);
    let mut rrc = RawRowCaster::new(column_types, &mut ti);
    {
        let x = rrc.next();
        assert!(x.is_some());
        let x = x.unwrap();
        assert!(x.is_ok());
        let x = x.unwrap();
        assert_eq!(x.row_id, 1);
        assert_eq!(x.items.len(), 1);
        assert_eq!(x.items[0], Int(1));
    }
    assert!(rrc.next().is_none());
}
