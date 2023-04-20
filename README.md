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
- Distributed system.

# Current State

- Can open some very simple sqlite database files and dump the contents.
  - We only handle pages of type btree, not e.g. free lists.  This is still usable, since when Sqlite files are created from sessions that use only CREATE TABLE and INSERT statements, the resulting files don't have other page types.
- No writing yet.  Inputs are created using `sqlite3` CLI.
- No demand paging.

# Code Structure

Initially the goal was to have layers which roughly model Sqlite's architecture (https://www.sqlite.org/arch.html), which looks like this:

* Interface
* Parser
* Bytecode VM
* Btree
* Pager
* VFS

At present, there is a Parser, Btree code, and a basic Pager.  VFS does not seem necessary since cross-platform support is not interesting to the author. Queries are sort of interpreted right now so there is no Bytecode VM.  The author may explore JIT as
instead of a bytecode VM (less control over fairness, but better speed?).  Some kind of command line interface seems likely.

Files are organized similarly:
* `main.rs` - loads a file, parses some SQL, and prints out tables.
* `parser.rs` - Parses SQL statements into a parse tree, e.g. using https://pest.rs/book/examples/ini.html
  * `sql.pest` - Defines grammar for generated parser.
* `serial_types.rs` - handles SQLite *serial types* (which can differ from row to row within a column, and are different from SQL types).
* `record.rs` - iterates over and parses row records that are stored in btree cells.
* `btree/*.rs` - provides iterators to walk over btree elements.  Uses a Pager to get at pages.
* `pager.rs` - `Pager` provides interface to get a page of the DB for reading.  In the future, it may or may not be present in memory  when requested.  It holds the handle to the open database file.
* `formatting.rs` - prints out tables nicely.
* `record.rs` - interprets row records as a sequence of column values.
* `serial_type.rs` - interprets column values from bytes to specific types.

# Future Work
See also [TODO.md](./TODO.md).

In no particular order.
- Data
  - Support for scanning multi-page btrees.
  - Support searching within multi-page btrees, rather than just scanning.
  - Support indexes.
  - Support blobs
  - Support overflow
- Concurrency
  - Support basic concurrency with at least Table-level locking. No plan for MVCC.
  - Pager layer to support multiple writers of different tables, which will require some unsafe rust to
    hide the locking done underneath.
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
  - Consistency
    - Write rollback journal, write the transaction, and then delete the rollback journal.
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

# Similar Projects

- https://github.com/erikgrinaker/toydb

# Notable Things I Learned

## When to use Traits vs Enums

-   Reference: https://www.possiblerust.com/guide/enum-or-trait-object
-   You use an enum when you want to be forced to handle every alternative at every usage site.
-   You use Traits when you want to have an open-ended set of types you can use.
    -   Others can extend your code by implementing a trait.  Enums are a closed set of types.
    -   If you don't need extensibility for users, you can skip using traits.  But, for testing, one may want to define mock types
        for internal use that use traits.
-   Traits are a bit slower as they use vtables Enums are a bit faster as they use branchs.
    -   We prefer faster if possible.
    -   In some cases the compiler can eliminate the need for this, I think.

## Lifetime Specifiers
It has been useful to reminding myself that the lifetime specifier is not the "places where this reference is used" (scope).
Rather it is the lifetime of the variable (referrent).  In one failed attempt at using lifetimes, I added more bounds for a
type with several references, but actually both references were to the same variable (the pager and its data).  Sometimes
adding the compilers suggestions it the right things, but other times it is not.

