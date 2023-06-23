Current Projects Stack
----------------------

# CURRENT - Pages owned by Table, not Db
  - [X] change StoredDb::pages from `Vec<_>` to `HashMap<pagenum, Page>`, while keeping public interface of StoredDb the same.
  - [ ] in btree/table.rs, in seek_leftmost_leaf(), notice how when we push an EitherIter<>, we pass a Cell::Iterator into a LeafIterator or a InteriorIterator.  Let's flip that and see if we can make the Leaf/InteriorIterator take a PageHandle, and own that PageHandle for the duration of the usage of the page.  
    - Will that make the borrow checker happy?
      If not we may need to try unrolling the loop into 3 or 4 predicated phases. 
    - Maybe try this on HEAD, and then try applying the stash "PageReadHandle almost working".
    - We tried, in the stash "PageReadHandle almost working",
      to make PageReadHandle work, and it felt close, but there is the lifetime issue where the PageReadHandle is not saved long enough (currently local temp).  We tried
      pushing it on the stack too, but I don't think rust
      understood the parallel lifetimes of the iterator and the PageReadHandle.  We need to lock them together in one object, and a page-level iterator looks like the place to do that.
  - Then it might also be classy to put type, header length, 
       offset information into the PageReadHandle.
        And it might be classy to move PageReadHandles into their own crate.
  - [ ] the above being sorted, we can then add a RwLock
        into the PageReadHandle, which will allow for concurrent writes (which would require different iterator implementations?).
  - [ ] If that works, then proceed to implement "demand paging" for the StoredDb.
  - [ ] Recall that one goal was to split StoredDb into the DB
        stuff, and the Pages stuff.  Because there are too many concerns in StoredDb right now.  Therefore:
    - [ ] factor the following members of StoredDb into a private struct in the same module called Pages:
          ```
          pub pages: Vec<Option<Vec<u8>>>, // MOVE OUT
          pub page_size: u32,  // DUPLICATE
          pub open_rw_page: Option<PageNum>, // MOVE OUT
          pub num_open_rw_pages: usize,  // MOVE OUT
          ```
        and change reference to refer to pages.pages, pages.open_rw_page, etc.
        StoredDb has a Pages called Pages.
        In StoredDb::open, do the read_exact() into the pages.pages.insert(pagenum).get_mut(pagenum).
        Refer to the stash called "trying to page btree_pager.rs"
  - Once we have demand paging, we can consider moving to a Pages-per-Table model.  Assess if this is still desirable.  A goal was to hide lifetimes from library users.
   that we wanted to have a separate Pages object for each table.  We thought that this might allow hiding lifetimes from callers, and simplify implementing write support.  Reassess if this is possible after doing the above.
   When someone asks for open_read()
    then reuse the cached Pages for that Table. (Or return a reference to a cached StoredTable instead?)
  - This should get rid of the <'_>  from StoredTable<'_>?
  - But we may still have a reference from the Table back to its Db, which the Pages uses to load pages? Groan.
  - We might want to rename a Pages as a Btree?

# Idea - TreeMap for TempTable and map interface for Table
  - Support rowid as a map (rowid, row).
  - Iterators for Table and TempTable to return both.

# Big Idea - Move Page Ownership from PagerSet/Pager into Table
  - Make Table objects lifetime be as long as the DB has been opened.
  - When you open a Db, a Db object is created.
  - The Db Object has a list of its Tables and Indexes and range of Pages and the file handle and file lock.
  - Tables (and Indexes) have a Btree in them.
  - Btree object owns its set of pages, which have numbers in them.
  - When a Btree object wants to read in a page with a known page number, it allocates memory (size = 1 page) which it owns,
    then it asks the Db object, which it has a readonly reference to, to read in page number P into its memory, which the Db can borrow
    for the duration of the load call. 
  - In future, when there is concurrency, a query thread (e.g. Scan iterator) can get read access to a Page by RWLocking the page it wants to read.
  - Expansion of the Table's pages only happens when someone has exclusive access to the table itself, which probably needs an RWlock of its own, at the
    DB level.
  - A Table need not have any pages present in it at first.  They get demand loaded.
  - A Pager and PagerSet are not passed around visibly, but a Context (with the list of Dbs, needs to pass into each function.)
  - When you try to scan or append a table, this is a member function, so hopefully the lifetimes are easier to reason about - the method lives less long
    than the table.  If ScanAndProjectAndClone() is a method of the Table (but what about iterators?) then the lifetime might be easier to reason about?
  - Limiting number of pages in core can be done separately, by requesting space allocation from a global quota counter.  LRU can be done
    using a central page LRU counter that is bumped every time a page gets accessed.

# Idea - enum dispatch
  - enum_dispatch crate
  - use for different table types (which implement trait TableMeta)
  - use for different streamable block types (which implement trait RowStream).
  - use for different inserables (which implement AppendRow).

# Side Side Project: RWLock pagers and pages.
This allows finishing "Minimal Writing"
Want non-mut ref to PagerSet to produce non-mut Pager, to produce mut Pages. using an RWLock.
  - **Have stash with partway successful version of this**
  - Think about making ReadOnly be a modal flag to PagerSet, which controls how the db file is opened, and causes runtime Errs when 
     trying to get pages for mutation, but not producing multiple types of Pagers form a PagerSet, or multiple types of Pages from a Pager.
  - Consider two approaches: 
    1. One where there is a PagerReader and PageWriter which wrap a ReadGuard and a WriteGuard respectively;
    2. another where the caller calls get_page_ro(), which returns an ReadGuard to a &page, and get_page_rw() which returns a WriteGuard
       to a &mut Page.  Thus, the called hold the *Guard on their stack, and there is only one Page type, and it has methods on both &self and &mut self, the latter not working when you only have an immutable reference.
  - Another way to attack the problem is to tackle making a Page type before trying to make the locking work, and return that &Page instead of &Vec.
    More helper functions can be put into Page to clean up callers (while being cautious to not put non-page concepts in to Page, such as Btree,Cell, etc).
  - Additionally, it could help things to flatten the btree iterators into fewer layers.
  
# Side Project: Minimal Writing
**This is in git stash right now**

Goal: Support inserting rows into Sqlite tables and writing the changed page back to disk.  Do it in a minumum way.
I don't want to go too deep into writes right now, but having at least minimal write ability may make it more clear where I need to go with the pager, and maybe other iterfaces too.

Writing to existing tables which have room in their last page for new cells (no btree growth yet).
Defer tree rebalancing; defer inserting cells in some order; defer length-changing update of existing cells.

Cleanups:
- [x] in typed_row.rs and in serial_type.rs, separate basic deserialization (to only Blob, Text, Int and Null types) from Casting to non-fundamental types (Int to Real, Int to Bool, etc).  Thus, serial_type does not need to know about SqlType.
- [x] in typed_row.rs, move the serial type sizeof code into serial_type.rs.
- [x] in typed_row.rs, rename build_row() to from_serialized() -> Result<Row, Error>;
- [x] add null to SqlType, as SQLite has null as a type.  
  - [x] consolidate methods on SqlType and SqlValue from ir_interpreter to those files.
- [ ] eliminate Cursor seeks to enable next step.
- [ ] Cleanup: see if I can wrap these three oft-used-together fields into one object.
    - (pages_bytes: &/&mut Vec<u8>, non_btree_header_bytes: usize, page_size: u32)
    - page type (header)
    - maybe also page number.
    - maybe in the future a lock.
    - First experiment to see if the page_bytes can be wrapped in a new type.
    - ... into one object that represents access to a page.
    - and which, when it is freed, releases reference counts in the Pager.

- [ ] Split Table into  ReadBtreeTable and in WriteBtreeTable, while looking for ways to reduce duplicated code. I don't like having the mut in it when just writing.  And passing in the pager as mutable into several places is going to prevent concurrency.  Not going to be able to give mutable pagers to multiple threads and have them all get (different) pages because of the mutability limitation.  Realize that `mut` doesn't really mean writable.  It means exclusive access.  So, rwlock can be used on immutable pager to return mutable page.

- [ ] Is there a trait that ReadBtreeTable, WriteBtreeTable, and TempTable should all implement, called TableMeta (name, column defs), and default implementations of helper methods?  

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
- defer support for `UPDATE ...`.  This is  done as a Scan.  It will be better with WHERE support and modifying records.
- [x] add insert support to main.rs
- [x]  in lib::run_insert(), open the first page of the table, using a helper.  Get the table schema.  Check that the ast::InsertStmt::values
      match the target table types.  
- [x] check that there is a sole page.  
-  append to that page.
  - [ ] Finish append code in btree/cell.rs.  It is a bit of mess right now.  But look at expanding get_free_space_range() into also doing the append, and get rid of existing append().
  - [ ] 
- [ ] commit the page to disk, releasing the hold on the page.
- Before committing all that code, think about how to split Table into ROTable and RWTable, with &mut only for RWTable.


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

4. [ ] support UnaryOps.
  - typeof(value)
  - logical not
  - sum() aggregation.

Recent Completed Projects
-------------------------

# Mini-Project - STRICT
  - Support and test "strict".
  - [x] parse from CREATE
  - [x] make it part of TableMeta
  - [x] use strict checks


# CREATE TEMP table
  - [x] put a schema table (temptable) in the pagerset
  - [x] allow lookups of tables to use the PagerSet, and to look for temp tables.
  - [x] in `CREATE`, etc, add parsing of db names, and treat "temp" db name specially; also detect "TEMP" (which forces the db name to be temp);

# parse and execute queries with basic project step
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

# Insert into TempTable
  - [x] Refactor TempTable to own file.
  - [x] Define TableMeta trait.
  - [x] Add append method to Temp Table.
  - [x] Support SELECT on temp table in run_ir.
  - [x] test inserting values into a TempTable using "INSERT".


Future Projects
----------------

# SQL Layer Projects

## Where 
- `SELECT a, b FROM t where a > b` becoming `Filter(Project(Scan))`.
  - simplify_ast_select_statement() should simplify expressions in where clause too.
  - build_where() similar to build_project().
  - build_where() to detect type mismatch in expressions.
  
## Nested Select
- `SELECT a, b FROM (SELECT 1 as a, "two" as b, 3 as c)` becoming `Project(TempTable)`

## `GROUP BY`

## Finish Projection.
- [ ] Use alternative name provided with "AS" in projects.
- [ ] Expression trees evaluated at runtime.
- [ ] push any projections that drop columns into the Scan so they don't need to be converted from storage format before being emitted.
- [ ] push any functions on longer values (Strings, Blobs?) down to the lowest project to reduce  amount of data copied.
- [ ] Implement Table locking at query time that prevents schema update and table delete.
- [ ] Implement Page locking at Scan time that releases done-with leaf pages (and used interior pages) held as long as needed.

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
    - First experiment to see if the page_bytes can be wrapped in a newtype.
    - Then, implement Drop for the newtype to write the page back?
    - This is a precursor to supporting writeback-when-done-with-write, and pageout-after-done-read.
    - [ ] Pre-req - eliminate Cursor seeks in btree code: use direct array access.  this will help to enable next step.
    - [ ] Put these objects into the Page (ReadPage/WritePage).
        - the actual pages_bytes: &/&mut Vec<u8>
        - the value of non_btree_header_bytes: usize
        - the page_size: u32
        - the page number, for error messages.
        - accessors to page header including:
          - page_size: u32
        - ReadGuard/WriteGuard for locks (future).
- [ ] Make a Table::read_lock() and Table::write_lock() methods that lock the table from being redefined, and locks the schema table
  row from being modified.
- [ ] Replace panics that are likely to happen during interactive with Results<>.
- [ ] Try to Box the File in pager.rs in a temporary box, and then use it, then move it to the Box in the constructed struct,
  so that we can run the header check in open().

## Idea - Staticification

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



