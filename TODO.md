# Current Project

Build steel thread of parsing and execution.
- [x] Parse to Parse tree using pest.rs.
  - e.g. Start with `select 1, x from t;` and generate `Pairs<Rule>`
  - [x] test that the right things are parsed and the wrong things are not.
- [x]  Build AST from parse tree: See AST.md.
  - [x] from above PT , test we can build this AST: `Select(SelectItems(Constant(1), ColName(x)), From(TableName("t")))`
-  [x] Build IR from AST: See IR.md
    - e.g. from AST, build this IR: `Project([Constant(1), ColName("x")], Scan("t")))`
    - [x] test the above case
-  [ ] Interpret IR to execute.
    - [x] interpret `Scan`
    - [x] return a row iterator from `run_ir`.
    - [ ] handle `ConstantRow`
    - [ ] handle `Project`.
    - [ ] connect root block to printer.
    - [ ] Goal is to minimize copying, using refs.  Esp. in deeper parts of IR tree.
      - Parent in IR tree to decides if clone needed.  Child to offer a ref.
      - How long is ref valid if page needs to go out?  Page waits until query done.  Refs last for lifetime of the IR execution (of the IR?)
    - [ ] Test IR evaluation using unit testing, with fake tables.
-  [ ] end to end test of query PT/AST/IR/Execute.
Scope for "steel thread" is just constants (literals) and expressions.

# SQL Layer Projects

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


# Small Tasks

Quick Cleanups for when you don't have a lot of time:
- Integration tests should run end-to-end using run_query(), checking the results.
- Replacing unwrap and expect with returning errors (using thiserr in modules, and anyhow in main).
  Remaining file: lib.rs, pt_to_ast.rs, and btree/*.rs.
- Using clippy.
- Improve the CLI to allow opening named files.
- Make a Pager::Page object that has is_present(), purpose(), start_offset(), and data() methods.
- Make a Btree::Page object that has btree_header() and btree_type() attributes.
- Replace panics that are likely to happen during interactive with Results<>.
- Try to Box the File in pager.rs in a temporary box, and then use it, then move it to the Box in the constructed struct,
  so that we can run the header check in open().
- Lock db file when opening it.
- look for stale TODOs
- run rustfmt
- Implement SearchIterator (SeekIterator?) for Table, and support "WHERE rowid = #" queries using that.
- Get full coverage of lib.rs in integration test.
- `.explain` by printing IR out.


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

## Use of Indexes
- Generate test data.
- Implement CREATE INDEX syntax in parser and test.
- Implement index name/schema lookup in the schema table and test.
- Implement index interior and leaf page iterators and test them.
- Implement SearchIterator (SeekIterator?) for indexes, using interior and leaf iterators.
- In Optimize step, look for  "WHERE column = value" queries and then look for applicable indexes for each WHERE constraint.
- Add RangeIteraror that returns index rows from Lo to Hi (with lower / upper bounds, like btree)/
- How are Indexes updated atomically with the table?

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