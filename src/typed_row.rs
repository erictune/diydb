//! provides access to table records in a typed form.
//! Table records in SQLite are stored as bytes, with a serial_type code, and need to be converted
//! to Rust types before they can be used in expressions.
//! Some conversions are fallible.  Any failure to convert any element in a row is treated as a failure to convert the entire row.
//! Any io errors reading the row are also treated as failures to read the entire row.
//! An error returned by the underlying table iterator (such as an error in the btree structure)
//! will manifest as one row having an error, fewer rows being returned.
// TODO: It might be better to treat casting errors differently from errors in the underlying iterator.

#[derive(Debug, Clone, PartialEq)]
/// can hold values of any of the SQL types (sqlite supported subset).
pub enum SqlTypedValue {
    Int(i64),
    Text(String),
    Blob(Vec<u8>),
    Real(f64),
    Bool(bool),
    Null(),
}

impl std::fmt::Display for SqlTypedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlTypedValue::Int(x) => write!(f, "{}", x),
            SqlTypedValue::Text(x) => write!(f, "{}", x),
            SqlTypedValue::Blob(_) => write!(f, "<BLOB>"),
            SqlTypedValue::Real(x) => write!(f, "{}", x),
            SqlTypedValue::Bool(x) => write!(f, "{}", x),
            SqlTypedValue::Null() => write!(f, "NULL"),
        }
    }
}

/// can hold a sequence of values of any of the SQL types (sqlite supported subset), along with a rowid.
pub struct TypedRow {
    pub row_id: i64,
    pub items: Vec<SqlTypedValue>,
}

#[derive(thiserror::Error, Debug)]
pub enum RowCastingError {
    #[error("One or more rows were not castable due to, first one given.")]
    OneOrMoreRowsNotCastable(#[from] crate::serial_type::Error),
}

fn build_typed_row(row_id: i64, record: &[u8]) -> Result<TypedRow, RowCastingError> {
    use crate::record::ValueIterator;
    let mut ret: Vec<SqlTypedValue> = vec![];
    for (serty, bytes) in ValueIterator::new(record) {
        match crate::serial_type::value_to_sql_typed_value(&serty, bytes, false) {
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
    use SqlTypedValue::*;
    // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
    let test_record: &[u8] = &[
        0x06, 0x08, 0x09, 0x07, 0x13, 0x00, 0x40, 0x09, 0x21, 0xca, 0xc0, 0x83, 0x12, 0x6f, 0x54,
        0x65, 0x6e,
    ];
    let tr = build_typed_row(1, &test_record).unwrap();
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
    pub fn new(it: &'a mut crate::btree::table::Iterator<'a>) -> Self {
        Self { it }
    }
}

impl<'a> Iterator for RawRowCaster<'a> {
    type Item = Result<TypedRow, RowCastingError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.it.next() {
            None => None,
            Some(r) => Some(build_typed_row(r.0, r.1)),
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
    use SqlTypedValue::*;
    // literal 0 | literal 1 | float 3.1415 | "Ten" | NULL
    let path = path_to_testdata("minimal.db");
    let mut pager =
        crate::pager::Pager::open(path.as_str()).expect("Should have opened db with pager.");
    pager.initialize().expect("Should have initialized pager.");
    let x = crate::get_creation_sql_and_root_pagenum(&mut pager, "a");
    let mut ti = crate::new_table_iterator(&mut pager, x.unwrap().0);
    let mut rrc = RawRowCaster::new(&mut ti);
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
