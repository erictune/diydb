Next small tasks:  Do all the things below end to end but with a current simple grammar.
- Add ast.rs from stash.  (Use vscode stash tool).  Just select, create, and constants.
- Instead of parsing to vectors of strings in parser.rs, parse to AST types.
- Generate very basic IR from that (Scan, ComputeCol, Project)
- Change Query to use the IR.  (IR is for queries only, which I guess includes inserts and deletes some day.)
- Merge in remamining stashed support for plus/minus in AST, and add basic expressions.  Add constant folding pass.
- Need AST printing out too.

Next big effort: parsing, optimization and execution.
1.  Build AST from parse tree: See AST.md.
    - e.g. `select 1 + 1;` -[parse]-> `Pairs<Rule>` -[build_ast]-> `Select(Add(Constant(1), Constant(1)))`
1.  Build IR from AST: See IR.md
    - e.g. `select a + b from t;` -[parse&build_ast]-> `Select(Add(Col("a"), Col("b")))` -> `Project("_1", AddCol(Expr(...), Scan("t")))
    - e.g. `select * from T` -> ... -> `Scan(T)`.
    - e.g. `select a from T` -> ... -> `Project([Col(a)], Scan(T))`.
1.  Interpret IR to execute.
    - testing for query execution can mostly use fake tables, which implement certain traits shared by real btree iterators.
    - testing things could get tedious otherwise.
1. Optimize IR, e.g.
    - collapse constant expressions.
    - identify when rowid can be used.
    - Later: identify when index can be used.

Quick Cleanups for when you don't have a lot of time:
- Improve the CLI - e.g. allow opening other files.
- replace panics that are likely to happen during interactive with Results<>.
- Try to Box the File in pager.rs in a temporary box, and then use it, then move it to the Box in the constructed struct,
  so that we can run the header check in open().
- Lock db file when opening it.
- look for stale TODOs
- run rustfmt
- Implement SearchIterator (SeekIterator?) for Table, and support "WHERE rowid = #" queries using that.
- Get full coverage of lib.rs in integration test.

Unknown size effort - Think about ACID and what that means for implementing the database.

A large effort for later - Use of Indexes
- Generate test data.
- Implement CREATE INDEX syntax in parser and test.
- Implement index lookup in the schema table and test.
- Implement index interior and leaf page iterators and test.
- Implement SearchIterator (SeekIterator?) for indexes, using interior and leaf iterators.
- In Optimize step, look for  "WHERE column = value" queries and then look for applicable indexes for each WHERE constraint.
- Add RangeIteraror that returns index rows from Lo to Hi (with lower / upper bounds, like btree)/

A large effort for later - spilled payloads.
- decide how to handle spilled payloads.  Options:
    1. Make a copy eagerly ... how to make the accessor take that memory ... lifetimes.
    1. Provide lazy access to the data through a spilled string iterator?  -- Holds locks on the spill page too?  Gets complex?
    1. Expose via enum { CompleteString, SpilledString }, complicating callers.
    1. Have the iterator own a heap allocation that contains a clone, but only when necessary. *like*
    1. Have the iterator return a text/blob iterator that knows how to iterate over the split, with string/slice-like Traits *like*

A large effort for later - writing
1. Support replacing values in existing rows (fixed size types), and then writing the page after the query.
1. Support replacing values in existing rows (variable sized types, requires reordering cells in page).
1. Support inserting values in existing tables if there is room in a page.
1. Support inserting values in existing tables if there is room in a page, but allocating a page for spilled data, and writing that one too.
1. Support inserting values into existing tables, allocating a new page, and growing and balancing btree if needed, and writing all of the changes.
1. Support creating a new table with create syntax, and writing the to schema table, and then writing that and the root page.

A large effort for later - demand paging and concurrent readers..
- We will eventually need all of:
    - table locking (when e.g. changing the definition of a table schema)
    - btree locking (when growing/shrinking the btree (this might take the form of just locking certain pages?))
    - single page locking (when modifying a value in a row) - several rows can be modified concurrently.
- Are Indexes children of the Table, since they need to be updated in sync with the table?

Making the pager and btree work for harder use cases:
  - SQLite does not use lock free data structures, AFAICT, where as some newer (in memory) systems do because of the high
    rates you can get when you don't do I/O.
  - I'll probably have o write my own unsafe code to use SQLite's data structures.
  - This blog goes over tree traversals: https://sachanganesh.com/programming/graph-tree-traversals-in-rust/
    They do several things:
    - They don't store the references to the data in the stack for the inorder traversal. 
      They just store the references to the page numbers.
      - Would this help us?  Need to think how transactions would work.  Do we try to lock all the pages we need for read as we go,
        and then succeed if we get them all?
    - They use an arena allocator where all the memory has a lifetime longer than the traversal (same with my pager.) 
  - how to module: https://www.sheshbabu.com/posts/rust-module-system/

