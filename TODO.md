Current Projects Stack
----------------------
# Main Project
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
-  [ ] add minimal Project support
    - [ ] just support constant values and direct mention of columns.
    - [ ] should work already in ast_to_ir.
    - [ ] finish Project in ir_interpreter.rs.  needs to look into the input ir block to get its output columns, so it can
          come up with a strategy.
    - [ ] generate errors when columns names don't exist at preparation time.
    - [ ] test various permutations of project-using queries in integration testing.
    - [ ] rename constantRow to ConstantTable and have it contain a TempTable.
    - [ ] add ir_interpreter unit tests?

A Goal is to minimize copying, using refs.  Esp. in deeper parts of IR tree. Idea is that parent in IR tree to decides if clone needed.  Child to offer a ref to uncloned data.  Not there yet.  Using streaming_iterator limits outstanding lifetime to 1 row.


Scope for "steel thread" is just constants (literals) and expressions.

Future Projects
----------------

# SQL Layer Projects

## Finish Projection.
- [ ] Generate temporary names for constant valued columns without "AS" in projects.
- [ ] Expression trees evaluated at runtime.
- [ ] refer to source columns by index rather than by name to avoid lookup.
- [ ] push any projections that drop columns into the Scan so they don't need to be converted from storage format before being emitted.
- [ ] push any functions on longer values (Strings, Blobs?) down to the lowest project to reduce  amount of data copied.
- [ ] Check column refs against the table schema and return error if not found. (schema hash to be confirmed at execution time).
- [ ] Expand each star to the list of all columns in the schema.
- [ ] Implement Table locking at query time that prevents schema update and table delete.
- [ ] Implement Page locking at Scan time that releases done-with leaf pages (and used interior pages) held as long as needed).

# Nested Select
- `SELECT a, b FROM (SELECT 1 as a, "two" as b, 3 as c)` becoming `Project(TempTable)`

# Temp table
- `CREATE TEMP table t as (SELECT 1 as a, "two" as b, 3 as c); SELECT a, b FROM t`

## AST Optimization
- [ ] Add binary expressions on literals and column names to pest grammar.
  - e.g.  `select 1 + 1, x + (2 + 2) from t;`
  - [ ] addition and subtraction is sufficient - avoid precedence problem for now.
  - See code in stash.
- [ ] Add operators and basic expressions in `SelectClause` to `pt_to_ast.rs` and `ast.rs`.
- [ ] Add `ast_optimize.rs` to do constant folding.
  - [ ] e.g.  `Project(["_1", "_2"], AddColumn(Constant(2 /* 1+1 */), AddColumn(ColExpr(Add(ColName(x), Constant(4))), Scan("t")))`
- [ ] test execution of such queries.

## Filter
- [ ] `select a from t where a > 3;`
- [ ] `WHERE` in PT.
- [ ] `WhereClause` in AST.
- [ ] `Filter` in IR.

## IR Optimization
- [ ] Parse a query which can be optimized by changing a scan to a rowid or index seek.
  - e.g. `select * from t where rowid = 3`
- [ ] Post IR generation, detect that `Scan` can be replaced with  `SeekRowid`
- [ ] Execute it, and check that it was more efficient (steps executed?)
- [ ] Here is a detailed treatment with theorems, reduction rules, and some test cases: https://arxiv.org/pdf/1607.04197.pdf
- [ ] Implement SearchIterator (SeekIterator?) for Table, and support "WHERE rowid = #" queries using that.


# Small Tasks

Quick Cleanups for when you don't have a lot of time:
- Have semicolon at end of sql queries.
- `.explain` by printing IR out.
- Integration tests should run end-to-end using run_query(), checking the results.
- Replacing unwrap and expect with returning errors (using thiserr in modules, and anyhow in main).
  Remaining file: lib.rs, pt_to_ast.rs, and btree/
- Using clippy.
- Improve the CLI to allow opening named files.
- Make a Pager::Page object that has is_present(), purpose(), start_offset(), use_read() and use_write() methods.
  The use_read() and use_write()s return a Pager::PageRef which the caller puts on their stack to hold a lock.
  They wrap the RwLock::LockResult().
- Make a Pager::PageRef object that represents a read or write lock on a page and allows borrowing the page contents
 via borrow() and borrow_mut() methods.
- Make a Table::read_lock() and table_writelock() methods that lock the table from being redefined, and locks the schema table
  row from being modified.
- Replace panics that are likely to happen during interactive with Results<>.
- Try to Box the File in pager.rs in a temporary box, and then use it, then move it to the Box in the constructed struct,
  so that we can run the header check in open().
- Lock db file when opening it.
- look for stale TODOs
- Get full coverage of lib.rs in integration test.


# B-tree Layer Projects
...

# Pager Layer Projects

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

## Multi-thread demand paging, multiple pag readers
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

  1. Support replacing values in existing rows (fixed size types), and then writing the page after the query.
  1. Support replacing values in existing rows (variable sized types, requires reordering cells in page).
  1. Support inserting values in existing tables if there is room in a page.
  1. Support inserting values in existing tables if there is room in a page, but allocating a page for spilled data, and writing that one too.
  1. Support inserting values into existing tables, allocating a new page, and growing and balancing btree if needed, and writing all of the changes.
  1. Support creating a new table with create syntax, and writing the to schema table, and then writing that and the root page.

- Need Table locking? (when e.g. changing the definition of a table schema)
- Need btree locking? (when growing/shrinking the btree (this might take the form of just locking certain pages?))
