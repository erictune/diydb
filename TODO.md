Current Projects Stack
----------------------
# Side Project: Minimal Writing

Goal: Support inserting rows, in a minumum way.
I don't want to go deep into writes right now, but having at least minimal write ability may make it more clear where I need to go with the pager, and maybe other iterfaces too.

Writing to existing tables which have room in their last page for new cells (no btree growth yet).
Defer tree rebalancing; defer inserting cells in some order; defer length-changing update of existing cells.

Cleanups:
- [x] in typed_row.rs and in serial_type.rs, separate basic deserialization (to only Blob, Text, Int and Null types) from Casting to non-fundamental types (Int to Real, Int to Bool, etc).  Thus, serial_type does not need to know about SqlType.
- [x] in typed_row.rs, move the serial type sizeof code into serial_type.rs.
- [x] in typed_row.rs, rename build_row() to from_serialized() -> Result<Row, Error>;
- [x] add null to SqlType, as SQLite has null as a type.  
  - [ ] consolidate methods on SqlType and SqlValue from ir_interpreter to those files.
- [ ] eliminate Cursor seeks to enable next step.
- [ ] Cleanup: see if I can wrap these three oft-used-together fields into one object.
    - (pages_bytes: &/&mut Vec<u8>, non_btree_header_bytes: usize, page_size: u32)
    - page type (header)
    - maybe also page number.
    - maybe in the future a lock.
    - First experiment to see if the page_bytes can be wrapped in a new type.
    - ... into one object that represents access to a page.
    - and which, when it is freed, releases reference counts in the Pager.


New Code:
- [x] in typed_row.rs, implement a full row writing routine.
  - [ ] Use it to fuzz test going both ways.
- [x] write function in serial_type.rs to determine the serial_type_code for a sql_value, for the purpose of determining its size, to see if it will fit.
- [x] in record.rs, write a "to_serialied(v: Vec<SqlType>)", that takes an array of SQLValues, and builds the header and payload vectors, and then can copy that into some other slice.
- [x] in typed_row.rs, add a row.serialize_to(&mut byte_slice) -> Result<(), SerializingError> : this gives an error if the target byte_slice does not have room for the serialized code.  It uses record.rs.  
- [x] extend serial_type.rs to work in the reverse.  Copying is okay.
  - [ ] fuzz testing!
- [x] extend pager to grant write access to a page.
  - [x] ref counter for now, read and write locks later.
  - [x] deny locking several pages at once, which would need a rollback log or WAL file.

- [IN_PROGRESS] add `append` method to src/btree/cell.rs to write an additional cell to a page, or error if there is no room.  
- [ ] add `append` method to src/btree/leaf.rs to support appending Cells to a leaf page specifically.  
- [ ] add `append` method to src/btree/table.rs to support appending Cells to the last item in a table.
    - [ ] for now only support appending to pages that have a leaf page as the first page (single page tables).
          To support multi-level trees, we'd need to support multiple rw locks on pages, which we can't do now due to the multiple borrow rule.
- [x] extend parser and AST to support `INSERT INTO TABLE VALUES(...)`.  This is an append operation on a btree opened to write. This uses a Seek.
- [ ] perhaps extend parser and AST to support `UPDATE ...`, which can be done as a Scan.   This is an append operation on a btree opened to write.
- [ ] add run_insert/run_update methods like run_query.  There is no IR for insert operations, I guess.  You just do them?  UPDATEs use a Scan. 

# Expressions
1. [X] Introduce Expr with only Constant member.
  - No new queries supported.
  - replace the Constant SelItem with Expr SelItem.
  - IR holds Constant when Expr is Constant.

2. [x] Introduce BinOps in queries.
  - do constant propagation after AST is built, before IR is built = complete simplification only at this time.
  - the only new queries supported is "select 1 + 1" and "select 1+1, a from t" - the others keep the same IR.
  - no change to project handling or IR.
  - detect type mismatch between constant operand and binop.

3. [ ] support colnames in expressions
  - do partial simplification of expressions that include columns.
  - build project function from Take() and BinOp().
  - This adds support for queries like "select 1 + a from t" and "select a + b + c + d + e".
  - detect type mismatch between column type and binop.

# Completed Project - parse and execute queries with basic project step
Build steel thread of parsing and execution.
- [x] Parse to Parse tree using pest.rs.
  - e.g. Start with `select 1, x from t;` and generate `Pairs<Rule>`
  - [x] test that the right things are parsed and the wrong things are not.
- [x]  Build AST from parse tree: See AST.md.
  - [x] from above PT , test we can build this AST: `Select(SelectItems(Constant(1), ColName(x)), From(TableName("t")))`
-  [x] Build IR from AST: See IR.md
    - e.g. from AST, build this IR: `Project([Constant(1), ColName("x")], Scan("t")))`
    - [x] test the above case
-  [X] Interpret IR to execute.
    - [x] interpret `Scan`
    - [x] return a row iterator from `run_ir`.
    - [x] handle `ConstantRow` by creating a TempTable.
    - [X] add returning error instead of panic from ast_to_ir.
    - [x] write hold executor blocks.
    - [x] write code that creates executor blocks from ir (prepare_ir).
    - [X] eliminate rowid from TypedRow - add back later if needed or have a flag to include it as first item?
    - [X] Rename TypedRow to Row
    - [x] Test IR evaluation using unit testing.
-  [x] end to end test of query PT/AST/IR/Execute.
-  [x] add minimal Project support
    - [x]  support constant values, star expansion and direct mention of columns.
    - [x]  ir_interpreter unit tests
    - [x] refer to source columns by index rather than by name to avoid lookup.
    - [x] Expand each star to the list of all columns in the schema.


Future Projects
----------------

# SQL Layer Projects

## Where 
- `SELECT a, b FROM t where a > b` becoming `Filter(Project(Scan))`.
  - simplify_ast_select_statement() should simplify expressions in where clause too.
  - build_where() similar to build_project().
  - build_where() to detect type mismatch in expressions.
  
## Finish Projection.
- [ ] Use alternative name provided with "AS" in projects.
- [ ] Expression trees evaluated at runtime.
- [ ] push any projections that drop columns into the Scan so they don't need to be converted from storage format before being emitted.
- [ ] push any functions on longer values (Strings, Blobs?) down to the lowest project to reduce  amount of data copied.
- [ ] Implement Table locking at query time that prevents schema update and table delete.
- [ ] Implement Page locking at Scan time that releases done-with leaf pages (and used interior pages) held as long as needed.

## Nested Select
- `SELECT a, b FROM (SELECT 1 as a, "two" as b, 3 as c)` becoming `Project(TempTable)`

## Temp table
- `CREATE TEMP table t as (SELECT 1 as a, "two" as b, 3 as c); SELECT a, b FROM t`
- No locking/ACID needed for in-memory.

- `GROUP BY`

## `JOIN`

## AST Optimization
- [X] Add binary expressions on literals and column names to pest grammar.
  - e.g.  `select 1 + 1, x + (2 + 2) from t;`
- [X] Add operators and basic expressions in `SelectClause` to `pt_to_ast.rs` and `ast.rs`.
- [X] Add `ast_optimize.rs` to do constant folding.

## Filter
- [ ] `select a from t where a > 3;`
- [ ] `WHERE` in PT.
- [ ] `WhereClause` in AST.
- [ ] `Filter` in IR.

## IR Optimization
- Maybe consolidate project, filter, and select into a single IR block operation, which is what Sqlite appears to do, if you look at `EXPLAIN QUERY PLAN` output.  This could still manifest as one or several iterators in a chain when executing it.  But moving the project closer to the lowest iterator would allow skipping serial-type conversion of unused (possibly large and even spilled) fields.

- [ ] Parse a query which can be optimized by changing a scan to a rowid or index seek.
  - e.g. `select * from t where rowid = 3`
- [ ] Post IR generation, detect that `Scan` can be replaced with  `SeekRowid`
- [ ] Execute it, and check that it was more efficient (steps executed?)
- [ ] Here is a detailed treatment with theorems, reduction rules, and some test cases: https://arxiv.org/pdf/1607.04197.pdf
- [ ] Implement SearchIterator (SeekIterator?) for Table, and support "WHERE rowid = #" queries using that.


# Small Tasks

Quick Cleanups for when you don't have a lot of time:
- [X] Have semicolon at end of sql queries.
- [X] Integration tests should run end-to-end using run_query(), checking the results.
- [ ] Replacing `panic`, `unwrap` and `expect` with returning errors (using thiserr in lower modules, and `anyhow::Result` and `bail` in main and higher modules.).
- [ ] Get full coverage of lib.rs in integration test.

# Recurring Tasks 

- Run `cargo fmt` 
- Run `cargo clippy`.
- Look for stale TODOs
- Review README.md


# B-tree Layer Projects

- Support searching for a rowid, and via an index.
- Support overflowing TEXT/BLOB types.

# Pager Layer Projects

- [X] Improve the CLI to allow opening named files.
- [ ] file system-level lock db file when opening it.
- [ ] Make a Pager::Page object that has is_present(), purpose(), start_offset(), use_read() and use_write() methods.
    - This is a precursor to supporting locking, and writes.
- [ ] Make a Pager::PageRef object that represents a read or write lock on a page and allows borrowing the page contents
    - use_read() and use_write() methods on Pager::Page can return the Pager::PageRef which the caller puts on their stack.
      Pager::PageRef will wrap the RwLock::LockResult.
- [ ] Make a Table::read_lock() and table_writelock() methods that lock the table from being redefined, and locks the schema table
  row from being modified.
- [ ] Replace panics that are likely to happen during interactive with Results<>.
- [ ] Try to Box the File in pager.rs in a temporary box, and then use it, then move it to the Box in the constructed struct,
  so that we can run the header check in open().

## Staticification

Use of the lazy_static module and macro could allow the pagerset to be declared as static,
which might reduce the complexity of dealing with lifetimes of data held in the pager.
The destructor of static resources does not run, so we would have to manually release any file system locks, flush files, etc.

## Single Thread demand paging

Lock Pages to allow for pager and queries to co-exist.
- [ ] Pre-size the pages vector at constructor time to a fixed size.  Therefore, remove the RefCell on it.
- [ ] Option is not needed since the inner vec can be zero length if it is paged out.
- [ ] Add a RwLock to every page (inner Vecs).
- [ ] The Pager can acquire the RWlock to page out aged pages.
- [ ] A table iterator can check if a page is present, and if not, get a write lock and fill the page.
    - It might be easier to have the caller put the RwLock on their stack rather than returning the LockResult type.
    - Test if write access to the pager is needed to put a write RwLock on the contained page.
    - Lock one page at a time, as you realize that you need it, in TableIterator.  Then let it be borrowed by CellIterator etc. Don't need to lock the whole vector, whose size is statically set at start time.
    - The locker may need to read lock the option, figure out it is a missing page, do a write lock, pull it in, release and reqacquire, or downgrade, and then read lock and do the read query.


## Multi-thread demand paging, multiple page readers
- The RwLock should, IIUC, allow copy-less cloning?

# Cross-Cutting Projects

## Streaming Iterators and Streaming Page Cache
Because our iterators currently return btree-references from iterators, the callers can in theory hold the references to some cells
for the entire duration of a scan over the table.  This means that the pager cannot drop any pages until a scan of a large (perhaps larger than main memory) file is complete.  To fix this, change from using the `Iterator` pattern for the btree and table iterators, to
a `StreamingIterator` pattern from the `streaming_iterator` crate.  This pattern forces the callers reference to memory to end before
advancing the iterator to the next row, while still allowing for use of familiar iterator methods and functional-style constructs.
This allows the caller to read values by references (such as when evaluating a where expression) but forces them to copy if they need values for longer (such as building an aggregation like top N).  Proof of making this work looks like scanning a large table with a "where" clause while the pager pages out pages as they are done being used, keeping memory usage within some bound such as 10 pages.

## Use of Indexes
- Generate test data.
- Implement CREATE INDEX syntax in parser and test.
- Implement index name/schema lookup in the schema table and test.
- Implement index interior and leaf page iterators and test them.
- Implement SearchIterator (SeekIterator?) for indexes, using interior and leaf iterators.
- In Optimize step, look for  "WHERE column = value" queries and then look for applicable indexes for each WHERE constraint.
- Add RangeIteraror that returns index rows from Lo to Hi (with lower / upper bounds, like btree)/
- How are Indexes updated atomically with the table?
- run queries in sqlite3 with `.eqp on` to see how it runs them, and compare to what I do (e.g. print my IR when that flag is on too).


## Sequential I/O optimization
The Scan IR Op is not required to walk the database tree in order, just visit all the pages.
The tree could be walked to determine a list of leaf pages, and those could be prefeched and visited in
the order in which they are ready, using an async framework.

## ACID

Think about ACID and what that means for implementing the database.

## Spilled Payloads.
Decide how to handle spilled payloads.  Options:

  1. Make a copy eagerly ... how to make the accessor take that memory ... lifetimes.
  1. Provide lazy access to the data through a spilled string iterator?  -- Holds locks on the spill page too?  Gets complex?
  1. Expose via enum { CompleteString, SpilledString }, complicating callers.
  1. Have the iterator own a heap allocation that contains a clone, but only when necessary. *like*
  1. Have the iterator return a text/blob iterator that knows how to iterate over the split, with string/slice-like Traits *like*

## Writing

  1. Support replacing values in existing rows (fixed size types)
    - seek the right place in the btree to modify.
    - Lock table or row.
    - Write single modified page to disk afterwards, in an consistent way.
  1. Support replacing values in existing rows 
    - For variable sized types (TEXT), requires reordering cells in page.
  1. Support inserting values in existing tables if there is room in a page.
    - find the right place in the btree to insert.
  1. Support inserting values into existing tables, allocating a new page, and growing and balancing btree if needed, and writing all of the changes.
    - Write multi-page in crash-safe way (e.g. with rollback journal or WAL)
  1. Support creating a new table with create syntax, and writing the to schema table, and then writing that and the root page.
    - Lock schema table.  order updates to btree vs schema, recover if crashing.
  1. Support deleting items.
    - delete table (would need freelist, vacuum/compaction)
    - delete or modify row
      - needs btree rebalance
      - needs page defrag and freeblock support.

# Execution Layer Ideas

- Code Generation
  - chose which indexes to use when multiple available
  - chose loop order for joins.
  - simplify code using relational-algebra-like rules
  - JIT the code for speed/fun?
    - WHERE expressions used in scans could be a jitted function.
      - Calling rust modules from within JIT-ed code: https://y.tsutsumi.io/2018/09/30/using-rust-functions-in-llvms-jit/
      - Inkwell.
    - Then an entire tree of IR could be JIT-ed?



