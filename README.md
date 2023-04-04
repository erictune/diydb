# DIY Database
`diydb` is a toy Database modeled after Sqlite, written in Rust as a learning exercise.

# Goals
For the author to:
- learn Rust
- re-learn b-trees
- explore basic database optimizations

# Non-goals
- Careful compatibility with SQLite
- Making a database that is useful to others, beyond learning.

# Current State

- Can open some very simple sqlite database files and dump the contents.
  - We only handle pages of type btree, not e.g. free lists.  This is still usable, since when Sqlite files are created from sessions that use only CREATE TABLE and INSERT statements, the resulting files don't have other page types.
- Doesn't support btrees that span more than 1 page (4k) yet.
- No writing yet.  Inputs are created using `sqlite3` CLI.
- very basic demand paging.

# Code Structure

The intent is to have layers  which models Sqlite's architecture (https://www.sqlite.org/arch.html), which looks like this:

* Interface   
* Parser      
* Bytecode VM 
* Btree       
* Pager       
* VFS         

Files are organized similarly:
* `main.rs` - loads a file, parses some SQL, and prints out tables.
* `parser.rs` - parses SQL statements into a parse tree, e.g. using https://pest.rs/book/examples/ini.html
* `serial_types.rs` - handles SQLite *serial types* (which can differ from row to row within a column, and are different from SQL types).
* `record.rs` - iterates over and parses row records that are stored in btree cells.
* `btree.rs` - provides iterator (cursor) to walk over btree elements (in future could support writes.).  Uses a pager to get at pages.
* `pager.rs` - provides an array of pages, which may or may not be present in memory (seek and load on first access).  Uses a vfs.  This will eventually enforce R/W locking of pages among multiple cursors.
* `vfs.rs` - opens database files. Reads the db file header. Will someday lock the files at the OS level.  
* There is no bytecode yet.  I may add a bytecode VM to execute SQL, and a code generator to emit bytecode from parsed SQL, and a query planner of sorts.
* There is no interface yet.  I may add a REPL that parses SQL and meta-commands.

TODO: Move table formatting to a separate file.

# Future Work
In no particular order.
- Data
  - Support for scanning multi-page btrees.
  - Support searching within multi-page btrees, rather than just scanning.
  - Support indexes.
  - Support blobs 
  - Support overflow
- Concurrency
  - Locking Database file when accessing.
  - Pager layer to support multiple accessors with overlapping lifetimes.
- Write support
  - inserts
    - insert (limited to single page btree per table.)
    - insert (with btree growth and rebalancing.)
    - insert (blob overflow page.)
  - deletes/modifys with size change
    - delete table (would need freelist, vacuum/compaction)
    - delete or modify row
      - needs btree rebalance 
      - needs page defrag and freeblock support.
  - persistence
    - write state to disk at exit
    - write state after single-page update completed.
    - write multi-page in crash-safe way (e.g. with journal or WAL) 
  - concurrency
    - transactions
    - locking pages
- Parsing
  - Selection of specific columns from tables.
  - `WHERE` clauses in SQL statements.
  - `JOIN`
  - `GROUP BY`
  - nested select (maybe?)
- Code Generation
  - chose which indexes to use when multiple available
  - chose loop order for joins.
  - simplify code using relational-algebra-like rules
